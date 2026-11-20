# llmrouter

A high-performance, single-container LLM gateway written in Rust that dynamically races and hedges across model providers for coding agents (Claude Code, Cline, Cursor, OpenCode, and OpenAI-compatible clients).

---

## Overview

Modern agentic coding workflows demand high throughput, ultra-low latency, and robust fault tolerance. `llmrouter` acts as an intelligent intermediary between coding tools and dozens of LLM backends:

- **Unified Multi-Protocol Gateway**: Accepts both Anthropic Messages (`POST /v1/messages`) and OpenAI Chat Completions (`POST /v1/chat/completions`), translating bidirectionally with high fidelity.
- **Speculative Hedged Racing**: Fans out requests across multiple candidate model channels in parallel, escalating channel width dynamically (`race_escalation_ms`).
- **Commit on Completion**: Every candidate stream runs until one finishes its entire response. Only then are losing streams cancelled—protecting against mid-stream disconnections and partial failures without sacrificing reliability.
- **Automatic Model Resolution**: Routes `auto` or virtual model names to the highest-performing available model based on real-time intelligence benchmarks.
- **Embedded Caching**: High-throughput response caching backed by [redb](https://github.com/cberner/redb) with canonical semantic hashing.
- **Adaptive Health Tracking & Per-Class Backoff**: Transparently isolates provider errors (e.g. rate limits, network timeouts, context overflow) with exponential backoff per failure classification.
- **Telemetry & Live Dashboard**: Exposes Prometheus metrics (`/metrics`), detailed request access logs (`/metrics/requests`), server logs (`/logs`), version metadata (`/version`), and an embedded web dashboard (`/dashboard`).

---

## Architecture & Core Concepts

```
                  ┌────────────────────────────────────────┐
                  │              Coding Agent              │
                  │   (Claude Code / Cline / Cursor / etc) │
                  └───────────────────┬────────────────────┘
                                      │
                         Anthropic / OpenAI API
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                llmrouter                                 │
│                                                                          │
│   ┌────────────────────┐    ┌──────────────────┐    ┌────────────────┐   │
│   │ Translation Layer  │───▶│ Catalogue Filter │───▶│ Response Cache │   │
│   │ (switchyard/wire)  │    │  (context/tools) │    │     (redb)     │   │
│   └────────────────────┘    └──────────────────┘    └────────────────┘   │
│                                      │                                   │
│                                      ▼                                   │
│                     ┌──────────────────────────────────┐                 │
│                     │       Hedged Channel Race        │                 │
│                     │  - Initial width: N channels     │                 │
│                     │  - Escalation: +1 channel / ms   │                 │
│                     │  - Commit on Completion          │                 │
│                     └──────────────────────────────────┘                 │
│                                      │                                   │
└──────────────────────────────────────┼───────────────────────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                              ▼
  ┌───────────┐                  ┌───────────┐                  ┌───────────┐
  │   Groq    │                  │  NVIDIA   │                  │  Mistral  │
  │ (OpenAI)  │                  │   (NIM)   │                  │ (OpenAI)  │
  └───────────┘                  └───────────┘                  └───────────┘
```

### Commit on Completion

Rather than committing to whichever stream outputs the first token (which could be a small or misbehaving model, or a stream that stalls mid-response), `llmrouter` keeps multiple channels streaming concurrently. When one stream successfully reaches its terminal event and passes format validation, it wins the race and remaining streams are cancelled. This guarantees clean, unbroken multi-turn answers.

### Thinking & Tool Use Translation Sanitization

When translating Anthropic requests to OpenAI-compatible providers, prior assistant thinking blocks (`type: "thinking"`) must not be forwarded as `reasoning` in input message objects. Strict providers (such as Groq and Mistral) enforce exact OpenAPI message schemas and reject non-standard fields with `400` or `422`. `llmrouter` automatically strips internal reasoning blocks from multi-turn input history while preserving tool calls and assistant text, ensuring uninterrupted agent iterations.

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/messages` | Anthropic Messages API (streaming & buffered) |
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions API (streaming & buffered) |
| `GET` | `/v1/models` | List all discovered & configured models |
| `GET` | `/version` | Build version, git SHA, and build timestamp |
| `GET` | `/health` | Liveness and health probe |
| `GET` | `/logs` | Recent application logs |
| `GET` | `/metrics` | Prometheus exposition endpoint |
| `GET` | `/metrics/requests` | In-memory ring buffer of recent requests and latencies |
| `GET` | `/dashboard` | Interactive operational dashboard |

---

## Configuration

Configuration lives in `config/` (or via command-line flags):

- **`config/router.yml`**: Defines gateway settings:
  ```yaml
  listen: "0.0.0.0:8080"
  streaming: true
  race_width: 2              # Initial concurrent channel trials (N)
  race_width_max: 8          # Maximum concurrent channels after escalation
  race_escalation_ms: 50     # Interval to add another channel if still racing
  max_per_channel: 1         # Redundant copies per channel (M)
  stream_stall_timeout: 60s  # Max duration without tokens before channel drops
  cache:
    enabled: true
    dir: "data"
  ```
- **`config/providers.yml`**: Upstream provider URLs, wire formats (`openai` or `anthropic`), and API key mappings.
- **`config/intelligence.yml`**: Model benchmark rankings and scores.
- **`config/models.yml`**: Specific per-model overrides (context windows, tool call support).

---

## Quickstart

### Using Cargo

```bash
# Build release binary
cargo build --release

# Run gateway
./target/release/llmrouter --config-dir config/
```

### Using Docker

```bash
docker compose up -d --build
```

### Connecting Claude Code

Point Claude Code to `llmrouter` via `~/.claude/settings.json` or environment variables:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://localhost:8080",
    "ANTHROPIC_MODEL": "auto"
  }
}
```

Or via CLI:

```bash
export ANTHROPIC_BASE_URL="http://localhost:8080"
export ANTHROPIC_MODEL="auto"
claude
```

---

## Verification, Git Hooks & CI

### Git Hooks (Pre-commit & Pre-push)

The repository includes a [`.pre-commit-config.yaml`](.pre-commit-config.yaml) enforcing code formatting, source file tracking, hygiene checks, and secret detection before commits, plus running the full verification suite before pushes.

Install hooks using [`prek`](https://github.com/j178/prek) (Rust) or [`pre-commit`](https://pre-commit.com/) (accelerated with `uv`):

```bash
# Using prek (fast Rust-native hook runner)
prek install --hook-type pre-commit --hook-type pre-push

# Or using pre-commit with uv
uv tool install --with pre-commit-uv pre-commit
pre-commit install --hook-type pre-commit --hook-type pre-push
```

### Local CI Suite

The repository maintains an 80% line and region test coverage gate with formatting, clippy, unit, integration, contract, and end-to-end smoke tests:

```bash
# Run the local CI suite
./ci.sh
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.
