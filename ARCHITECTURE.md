# Build contract

`DESIGN.md` is the specification. This file is the module layout every agent
builds against, plus the crate API signatures that were verified by compiling
them, not read from docs.

## Build commands

```bash
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR=/tmp/llmrouter-target   # the repo is on a slow share
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
cargo test
cargo fmt
```

Always set both variables. Without `CARGO_TARGET_DIR` the build writes to a
network share and takes minutes.

## Verified crate signatures

Compiled and run. Argument order differs from the published docs, so trust
these.

```rust
use switchyard_translation::{
    decode_request, encode_request,
    decode_aggregated_response, encode_aggregated_response,
    decode_stream, encode_stream, WireFormat,
};

// WireFormat::{OpenAiChat, AnthropicMessages, OpenAiResponses}

fn decode_request(wire_format: WireFormat, body: &serde_json::Value)
    -> Result<LlmRequest>;
fn encode_request(request: &LlmRequest, wire_format: WireFormat)
    -> Result<serde_json::Value>;
fn decode_aggregated_response(body: &serde_json::Value, wire_format: WireFormat)
    -> Result<AggLlmResponse>;
fn encode_aggregated_response(agg: &AggLlmResponse, wire_format: WireFormat,
                              served_model: Option<&str>)
    -> Result<serde_json::Value>;

fn decode_stream<S>(bytes: S, source: WireFormat)
    -> Result<LlmResponseStream, LlmClientError>
where S: Stream<Item = Result<Vec<u8>, LlmClientError>> + Send + 'static;

fn encode_stream(chunks: LlmResponseStream, target: WireFormat,
                 served_model: Option<String>)
    -> Result<RawEventStream, LlmClientError>;
```

Neutral types live in `switchyard_translation::llm`: `LlmRequest`, `Message`,
`InstructionBlock`, `ToolDefinition`, `ToolCall`, `ToolResult`, `ToolChoice`,
`SamplingParams`, `OutputParams`, `AggLlmResponse`, `ResponseOutput`, `Usage`,
`StopReason`, `LlmResponseChunk`, `ContentBlock`, `Role`.

Also verified: `redb` 2.6 open/insert/commit/get round trip, `blake3::hash`
returning 32 bytes.

## Modules

One agent owns one file. Never edit another module's file.

| File | Owns | DESIGN.md sections |
|---|---|---|
| `src/config.rs` | Loading `router.yml`, `providers.yml`, `models.yml`, `intelligence.yml`, `.env`. All config structs with serde defaults. | Config, Every number here is a starting point |
| `src/catalogue.rs` | `Channel`, `ChannelId`, the sorted `ArcSwap<Vec<Channel>>`, and the per-channel query with an exclusion set. | The channel list and one client request |
| `src/health.rs` | `retry_counter`, `retry_base`, `eligible_at`, the backoff maths, `Retry-After` parsing, redb persistence and the 5 minute flush. | Startup and health, Backoff |
| `src/probe.rs` | The two startup waves and the poller: keyless reachability probe, keyed `/models` probe. | Startup and health |
| `src/cache.rs` | redb `responses` table, blake3 key over the neutral request, read rules, the intelligence write rule in one transaction, ttl and `max_bytes`. | Cache |
| `src/wire.rs` | The only module that names `switchyard_translation`. Four buffered functions and two streaming ones. | Format translation |
| `src/upstream.rs` | The shared `reqwest::Client`, HTTP/2 settings, one channel call buffered or streamed, the shared `Bytes` body. | HTTP client settings, Memory over the life of one client request |
| `src/race.rs` | N channels, M hedged copies, first copy home wins, cancellation, fallback, the client-disconnect path. | Core loop, Streaming, Concurrency against one provider |
| `src/server.rs` | axum routes, request and response translation at the edges, status codes (no keepalive: the status line must not go out before the outcome is known). | Endpoints, Status codes, Streaming |
| `src/models_api.rs` | `GET /v1/models`, one row per distinct model with live channel health. | `GET /v1/models` |
| `src/admission.rs` | The RSS gate: read `/proc/self/statm`, compare to `memory_high_water`, `503` above it. | Admission: the gate is memory |
| `src/net.rs` | The two network test calls that run beside every client request. | When the network itself is down |
| `src/metrics.rs` | Prometheus text, every counter and histogram named in the design. | What it records |
| `src/main.rs` | Bootstrap: jemalloc, config, state, startup probes, axum serve, graceful shutdown. | Deployment |

## Rules every agent follows

1. **`DESIGN.md` wins.** If this file and `DESIGN.md` disagree, follow
   `DESIGN.md` and say so in your report.
2. **Read "Wrong turns already taken" in `DESIGN.md` before writing code.** It
   lists 48 approaches already rejected, with reasons.
3. **Own one file.** Never edit a module you do not own. If you need a change in
   another module, report it instead.
4. **No `todo!()`, no `unimplemented!()`, no stub that returns a fake value.**
   Finish the module or report the blocker.
5. **Tests go in a `#[cfg(test)] mod tests` block in your own file.** Test the
   logic, not the network: backoff maths, the cache write rule, the channel
   query, config parsing.
6. **`cargo clippy --all-targets -- -D warnings` must pass for your file.**
7. **British English in comments. No em dashes.**
8. **Never log key material.** The first 6 characters of a client key only.
