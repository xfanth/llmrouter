# llmrouter — design notes

A single-container LLM gateway. One endpoint, any model, racing requests across
providers, first good answer wins. Replaces omniroute, which leaks memory and
sheds traffic.

Revision 4, 9 October 2026.

## Verdict and effort

Doable, and the performance targets were never the hard part. 1 RPS with a
10ms budget is comfortable in any compiled language. The real cost sits in tool
calling and the Anthropic message format, both of which come from the client
choice rather than the throughput numbers.

Rough effort for one person who knows Rust:

| Piece | Effort |
|---|---|
| Config, catalogue, ranking, context filtering | 1-2 days |
| Non-streamed racing and fallback | 1 day |
| `redb` cache | half a day |
| OpenAI chat-completions endpoint | 1 day |
| Anthropic `/v1/messages`, buffered and streamed | 2-3 days |
| Tool calling across both formats | 2-3 days |
| Docker, logging, metrics, hardening | 1-2 days |

Call it **a week to ten days**. Streaming is out of scope, which is what keeps
this small: racing works properly on whole responses, failures are recoverable
right up to the moment a response is returned, and the cache behaves.

## Why this exists

Omniroute does not do the job. Measured 8 October 2026 and written up in its own
`MEMORY_FINDINGS.md`: the gateway refuses traffic with its own 503 once the V8
heap reaches 952MB, which is 92% of the 1,024MB cap its image sets. Heap climbs
at 1-4MB a minute under light traffic and never falls back. A restart is the
only cure. It had been shedding every request for two hours when measured, with
84 routing attempts killed before any upstream call. The matching upstream bug
(#13621, still open) is that large Claude sessions retain request strings.

That is a garbage-collected-language failure mode. Rust does not have it. This
is the clearest argument for the stack choice, stronger than any latency number.

Three things follow for the new router:

- **Memory must be flat under load.** Bounded, measurable, no growth over days.
  Holding 4,000 tuples and a request cache in tens of megabytes, not hundreds.
- **No self-imposed refusal guard.** Omniroute sheds traffic to protect itself.
  If memory is flat there is nothing to protect against.
- **A failed upstream must not poison a channel.** Omniroute's #15672 recorded
  every self-shed 503 against the connection, locking out channels that were
  never dialled. Failures must be attributed to the attempt, not the channel.

## Decisions taken

- **Clients: coding tools.** Claude Code, Cline and Cursor. They need tool
  calling and the Anthropic `/v1/messages` format.
- **Streaming, with a hedge strategy.** `streaming: true` by default.
  `hedge_strategy: tps` buffers every stream for 1 second, then keeps the
  fastest and cancels the rest. Not a nicety: a channel call takes 34.5 minutes
  and Claude Code's `API_TIMEOUT_MS` defaults to 10, so without streaming the
  client hangs up every time. See **Streaming**.
- **Deployment: home network, no password.** Runs on TrueNAS, reachable only
  from inside the LAN. No shared secret, no rate limiting, no TLS.
- **Key handling: fall through, no quota tracking.** Per spec item 8: a failed
  channel moves immediately to the next. No per-key quota accounting, no parked
  keys, no circuit breakers. That subsystem is a large part of what makes
  omniroute heavy.
- **Providers:** everything in `providers.yml`, 15 providers and 47 keys.
- **Context windows and tool support** from `model_capabilities.csv`, joined
  against each provider's model list at startup.
- **Model naming** matched automatically against the ranking file, with an
  override file for anything matched wrongly.
- **Caching:** caller sent `auto` or no model, any saved answer for that prompt
  is reused. Caller named a model, only an answer from that same model serves.
- **Admission on memory, not a request count.** `memory_high_water: 3GB`, then
  `503`. See **Admission: the gate is memory, not a request count**.

## Targets

From `ROUTER_SPEC.md`, owner's own figures:

| Target | Figure |
|---|---|
| Concurrent requests | 30+ |
| Requests per second | ~1 |
| Added latency | under 10ms for a small prompt, about 20ms for a 1M token prompt. See **Latency, and why not to chase it**. |
| Scale | provider x model x key. 4,000 was a guess; real figure is ~20,000 before filtering, low thousands after |
| Memory | under 4GB |
| CPU | low |
| Cache | written to disk, loaded at startup, survives container recreation |

All of these are comfortable. Full expansion is about 20,000 channels: gemini
75 models x 9 keys = 675, openrouter 417 x 8 = 3,336, nvidia 106 x 7 = 742,
and so on. The tool-call filter drops 16%. The free-key filter takes openrouter
from 3,336 to roughly 120. The live pool lands in the low thousands.

Held as a pre-sorted vector at about 200 bytes each, 20,000 channels is 4MB.
Filtering is a linear scan, microseconds. 30 concurrent requests at N=3 is 90
in-flight connections, which `tokio` handles without noticing. Expected
resident is 15-30MB, not 4GB.

Nothing here is tight. With streaming out of scope, the 10ms budget is measured
on a whole request, and the work in that window is a hash, a `redb` lookup and
a slice of a pre-sorted vector.

## Correction on the Baseten Performance Client

It does not do what the brief assumes, and this changes the design, so it is
worth being precise. From their docs:

- A client instance is bound to **one base URL and one API key**. There is no
  racing across different providers, models, or keys. That part is ours to
  write.
- Its **hedging** (`hedge_delay`) sends a duplicate of the same request to the
  same endpoint. That is your M, "max requests to the same channel". It does
  cover requirement 10.
- It is a **client library, not a server**. The whole gateway, endpoints,
  translation, cache, catalogue is still ours.
- `batch_post` **does not consume server-sent events**, so streamed replies
  cannot go through it. Their own examples set `stream: false`.

What it does give us, and it is worth having: connection reuse, a Rust HTTP
core, retries on 408/409/429/5xx with exponential backoff and jitter, per
request and total timeouts, cancellation tokens, and that same-channel hedging.

So the plan is one Performance Client instance per channel, and we write the
cross-channel race over the top of them. That keeps the library doing the part
it is good at.

## Stack

**Rust, tokio, axum, reqwest.**

Going native removes the Python layer entirely. Expect a 10-15MB binary,
15-30MB resident, and routing overhead under 0.5ms. A `FROM scratch` container
image, no interpreter, no GC pauses.

The cost is honest: roughly three times the writing time of the Python version,
and a slower loop when the shape changes. At 1 RPS neither language is under
any pressure, so this is a maintenance choice rather than a performance one.

Dependencies: `tokio`, `axum` (HTTP server), `reqwest` (HTTP client, streaming
included), `serde` / `serde_json` / `serde_yaml`, `redb` (cache), `csv`, and
`sha2`. All first-tier crates.

### What the Performance Client is still worth in Rust

Less than it was in Python. In Python it brought a Rust HTTP core that the
language could not match. In Rust we already have `reqwest` and `tokio`, so
what remains is retry policy, connection pooling and hedging. Those are things
`reqwest` plus about twenty lines of `tokio::select!` already do, and the
racing across channels was never in the library anyway.

It is also batch-shaped. Its unit of work is a list of payloads sent to one
endpoint, not a single request fanned across several. Bending that into a
per-request race is more friction than writing the race directly.

Recommendation: write the race on `reqwest` and `tokio` directly, and keep the
Performance Client as a drop-in only if the hedging and retry policy turn out
to be fiddly. Hedging is roughly:

```rust
tokio::select! {
    r = first => r,
    r = async { sleep(hedge_delay).await; second.await } => r,
}
```

## Shape

```
llmrouter/
  src/main.rs       boot, config, axum router, endpoints
  src/catalogue.rs  providers, models, keys, context sizes, ranking
  src/route.rs      filter -> order -> race -> fallback
  src/translate.rs  anthropic <-> openai request/response shapes
  src/cache.rs      redb request cache
  providers.yaml    per-provider base url, auth style, wire format
  models.csv        Artificial Analysis rankings (supplied)
  config.yaml       user config
  Dockerfile
  docker-compose.yml
```

Providers differ in about four things: base URL, auth header name, auth prefix,
and whether they speak the OpenAI or Anthropic wire format. That is a YAML
table, not a class hierarchy.

## What you actually have

Two sources were checked. Omniroute's live `storage.sqlite` stores every
credential encrypted (`enc:v1:` prefix, unlocked by `STORAGE_ENCRYPTION_KEY`
in its `.env`), so nothing usable came out of it. New-api's
`config/channels.csv` and `config/freellmapi-keys.csv` hold the same
credentials in plaintext, and those are the source of truth here.

`build_providers.py` turns them into `providers.yaml`. The result is **15
providers and 47 keys**:

| Provider | Keys | Priority | Base URL |
|---|---|---|---|
| zai | 1 | 100 | `api.z.ai/api/coding/paas/v4` |
| gemini | 9 | 94 | `generativelanguage.googleapis.com/v1beta/openai` |
| hades-local | 1 | 92 | `hades-ubuntu.arpa:8021` |
| groq | 6 | 90 | `api.groq.com/openai` |
| cerebras | 6 | 89 | `api.cerebras.ai/v1` |
| mistral | 1 | 85 | `api.mistral.ai` |
| chatjimmy-local | 1 | 85 | `localhost:4100` |
| nvidia | 7 | 83 | `integrate.api.nvidia.com/v1` |
| openrouter | 8 | 80 | `openrouter.ai/api/v1` |
| cloudflare x5 | 1 each | 34-38 | `api.cloudflare.com/.../ai/v1`, five accounts |
| pollinations | 2 | 30 | `text.pollinations.ai/openai` |

Every one speaks the OpenAI chat format, so translation work is close to zero.
The Z.ai coding plan's base URL is confirmed as `/api/coding/paas/v4`, settling
the question raised earlier. New-api's own priority numbers are carried across
as a starting order, to be replaced by the intelligence ranking.

**This changes the racing argument.** 15 providers and 935 known models expand
to thousands of channels, so racing is worth doing. Openrouter alone opens up
most of the ranking file.

### What was left out

**27 more keys with no base URL.** `freellmapi-keys.csv` holds 59 keys across
20 platforms. 27 matched an existing channel and were merged in. The rest
belong to platforms new-api has no channel for, so there is no URL to call:
opencode (8), cloudflare (6, each needs its own account id), modelscope (3),
and one each for agnes, ainative, bazaarlink, cohere, github, huggingface,
llm7, reka, routeway and sealion. Supply URLs and they drop straight in.

**Roughly 60 OAuth connections.** Antigravity, grok-cli, kilocode, agy,
amazon-q, kiro, gemini-web, codex, github and zed-hosted authenticate by OAuth
rather than a static key. Each needs its own sign-in and refresh flow. None are
in `providers.yaml`, and this is the single largest piece of work if you want
them.

**Non-chat endpoints.** 18 of them: embeddings, text-to-speech, transcription
and image generation.

### Model capabilities, already solved

Omniroute's `model_capabilities` table turned out to be the most useful thing
in the whole box, and it is now exported here as `model_capabilities.csv`:
9,927 rows across 239 providers, last synced 8 October 2026, carrying
`provider`, `model_id` in the provider's own naming, `tool_call`, `reasoning`,
`structured_output`, `limit_context`, `limit_output`, `modalities_input`,
`family` and `open_weights`.

That closes three gaps at once:

- **Context windows.** 9,763 of the rows carry one, so the startup query
  against each provider's model list becomes a fallback rather than the primary
  source. `context_defaults.yaml` (65 rows) is now redundant.
- **Tool support.** 8,696 rows report `tool_call`. Claude Code sends tools on
  every request, so any model without tool support is a guaranteed failure.
  Filtering on this flag before racing removes an entire class of wasted call.
- **Provider-native model IDs.** The names are what each provider's API
  expects, which is exactly the side of the naming problem the Artificial
  Analysis file does not have.

Coverage for our 15 providers is 935 rows: openrouter 417, opencode 120, nvidia
106, huggingface 78, gemini 75, mistral 42, github 35, zai 20, cohere 17, groq
16, modelscope 7, cerebras 2. Cloudflare and pollinations are absent and need
their model lists read at startup.

## The ranking data

`models.csv` has 650 rows, 641 with an intelligence score, and a precomputed
`intelligence_rank` column. Ordering is a sort on that column. Solved.

It also carries blended cost per million tokens, time to first token, output
tokens per second, and a stack of benchmark scores (coding index, GPQA, AIME,
LiveCodeBench, Tau-bench and others). All usable later for cost-aware or
latency-aware routing without needing new data.

Two things it does not have, and both matter:

**No context window column.** This is the field requirement 3 is built on, and
it is not in the file. Decided: read it from each provider's model list at
startup, with `context_defaults.yaml`, a short hand-written file, covering any
provider that does not publish it. Gemini's model list does return input token
limits. Z.ai's coding plan may not expose a model list at all on a coding key,
in which case its models live entirely in the fallback file.

**The `slug` is an Artificial Analysis slug, not a provider model ID.** AA call
it `claude-fable-5-1`; Anthropic's API wants `claude-fable-5-1-20260901` or
similar. OpenRouter wants `anthropic/claude-fable-5.1`. The `creator` column is
who built the model, not who serves it, so it cannot be used for routing
either.

Decided: match automatically, correct by hand. Normalise both sides by
lowercasing, stripping any trailing date, and flattening dots, dashes,
underscores and slashes, then match. `model_overrides.yaml` maps provider ID
to AA slug for anything the automatic pass gets wrong or misses.

The risk this carries is a silent wrong match: `gemini-3.7-flash` matching the
row for `gemini-3.7-flash-lite` produces a plausible but incorrect ranking and
nothing complains. Mitigation is to log every automatic match at startup and
print the ones resolved only by a loose match, so a wrong pairing is visible
rather than silent.

## Building the catalogue at startup

The provider decides what exists. The file supplies the detail.

1. For each provider in `providers.yml`, call its `/models` endpoint with the
   first working key. That list is the only source that says which models that
   key can call today.
2. Join each returned model id against `model_capabilities.csv` on
   `(provider, model_id)` to get `limit_context` and `tool_call`.
3. A model the provider lists but the file does not know: take the context
   window from the provider response, and assume tool support until a call
   proves otherwise.
4. Join against the ranking to get `intelligence_rank`. Anything unmatched
   sorts to the bottom rather than being dropped.
5. Expand to tuples of (channel, model, key) and sort once by rank.

`model_capabilities.csv` is a snapshot from 8 October 2026 and goes stale.
Models appear and retire. Never treat it as the list of what exists.

Pollinations is absent from the file, so it depends on step 1 entirely.
Cloudflare is present, but under the single provider name
`cloudflare-workers-ai`, while `providers.yaml` names each account
`cloudflare-<account id>`. `llmrouter-make-config` copies those 27 rows across
under each account's own name, so the join in step 2 matches.

## Vocabulary

**Client request** = one inbound call from a coding tool to the router.

**Channel call** = one outbound call from the router to one channel. One client
request makes many channel calls.

**Channel** = one provider/model/key tuple. Not a provider. Gemini with 9 keys
and 75 models is 675 channels, not one.

**N** (`race_width`) = concurrent calls to *different* channels.

**M** (`max_per_channel`) = concurrent calls to the *same* channel. A hedged
duplicate of the identical request to the identical provider, model and key.

The M copies fire **with** the first one. `hedge_delay_ms` is 0 by default.

Set it above 0 to delay the copies instead. That saves the duplicate calls when
the first copy answers quickly, at the cost of being slower when it does not.
Default 0 means never wait.

### M and health

The M copies are one trial, not M trials. Identical request, identical provider,
model and key, same moment. The second copy tells you nothing the first did not.
Counting them would also make the backoff depend on a latency dial: at M=3 a
channel reaches the 30 day cap after 8 walks instead of 23.

- Any copy returns 2xx with content: the channel is healthy, `retry_counter`
  resets to 0. Ignore the sibling failures.
- Every copy fails: `retry_counter` goes up by 1, not by M.
- Every copy fails and some carry `Retry-After`: take the **longest** of them as
  `retry_base`. The copies hit the provider's window at different points, so the
  longest is the safe number.

### What M buys and what it costs

**Buys: latency.** The first copy to answer wins, so the channel returns its
best of M samples instead of one draw. Generation time varies a lot per call,
so this cuts the slow tail on every channel, not only a broken one. Raise M when
the client wants the lowest latency possible.

**Costs: budget.** A rate limit counts requests against one key. M multiplies
exactly that. Five copies spend five of that key's budget for one answer.

The two meet at the limit. Below it, M is close to free and buys real speed.
Near it, M pushes the channel over and the copies come back `429`.

N has no such cost, because it spreads across different keys and providers.
Where both would work, raise N first.

## Core loop

A request arrives. The router:

1. Estimates the prompt's token count (characters / 3.5 is accurate enough;
   exact tokenisation is not worth the dependency).
2. Filters the tuple list against `model_capabilities.csv`, three checks in
   one pass:
   - `limit_context` holds the prompt plus the requested output, with a margin.
   - `tool_call` is 1 when the request carries tools. Claude Code always does.
     146 of the 935 models on your channels fail this, 16%.
   - The key can call the model. OpenRouter lists 417 models and 15 free
     variants, so a free key skips about 400 of them.

   This is spec item 7's filter with two more fields. No new mechanism, no
   failure memory. It stops a request walking hundreds of dead tuples before
   the first live one.
3. Sorts what remains by `intelligence_rank`, ascending. The general
   `intelligence` score, not `coding_index`, even though the client is a coding
   tool.

   **Ties break at random.** Many channels share a rank: the same model on
   several providers, or the same provider/model on several keys. Shuffle
   within each rank group. No latency tiebreak, no preferring one provider.

   Worked example. Five channels with ranks a:1, b:2, c:2, d:2, e:3.
   - N=2 fires `a` and one of `b`, `c`, `d` chosen at random.
   - N=5 fires all five at once.

   N takes the first N of the shuffled order. It does not skip a channel for
   being the same model as one already in flight.
4. Takes the top N (`race_width`), applying M (`max_per_channel`) so a thin
   pool still fills the race by hitting one channel more than once. The M part
   is the Performance Client's `hedge_delay`; the N part is ours.
5. Fires all N concurrently as asyncio tasks sharing a cancellation token.
6. First 2xx **carrying non-empty content** wins. Cancel the token, which tears
   down the rest immediately. A 2xx with an empty body, an error object, or no
   content block counts as a failure, because an empty 200 returns faster than
   a real generation and would win every race. A model's refusal to answer is
   real content and passes through.
7. Any channel that fails is replaced straight away by the next candidate down
   the list, keeping N in flight, until success or the list is exhausted.
8. On success, write to cache keyed by the request hash.

Step 7 is the bit worth care. It is a sliding window, not a fixed batch: a
failure triggers an immediate top-up rather than waiting for the batch to
settle. The Performance Client's own retry logic is turned down for the racing
path, since moving to a better channel beats retrying a failed one.

## When the caller names a model

The default is `auto` or no model at all. A caller may still name one.

Filter the channel list to that model, then race by the same rules. Ranking
does nothing here: every surviving channel is the same model, so the only
ordering left is the random tie-break.

Example: `claude-fable-5-1` exists on openrouter with 8 keys. That is 8
channels at one rank. N=2 fires two at random. Fallback walks the rest.

No match means `404` with the reason. Never substitute a different model: the
caller asked for a specific thing.

Cache follows the rule already set. A named model serves only an answer
produced by that same model.

## The filter runs before any channel call

**No channel call happens until the filter has run.** The filter is cheap, it is
local, and it knows things that would otherwise cost a failed channel call to
find out.

Three checks, all from `model_capabilities.csv`:

1. `limit_context` holds the prompt plus the requested output.
2. `tool_call` is 1 when the client request carries tools.
3. The key can reach the model.

A client request with a 500k token prompt makes **zero** channel calls. Every
channel fails check 1, the list is empty, and the router answers at once.

The answer says which check emptied the list.

| Code | Means |
|---|---|
| `413` | No channel has a large enough context window. The body names the largest one found. |
| `504` | The list is empty for any other reason: cold start, every channel in backoff, no tool support. |

This is the difference between a fault in the client request and a fault in the
router. A 500k prompt is the caller's problem and no amount of waiting fixes it.

## Exhaustion

Spec item 8: keep making channel calls until one succeeds or the channel list
runs out. No cap on the number of channel calls. No wall-clock limit on the
client request.

`request_timeout: 60s` limits one channel call. It does not limit the client
request.

When the list runs out, return `504`. The body carries the number of channel
calls made and the last upstream error.

**The health table keeps the list short, not a cap.** A channel that failed is
in backoff, so it is not in the list. A provider/key that died is not in the
list. After a few hours of traffic the list holds the channels that work, and a
client request makes a handful of channel calls.

### Worked case: every key dead, empty health table

The channel list holds 4,000 channels. Nothing is in backoff. Every key expired
last night, and the filter cannot see that, because only a channel call finds it
out.

Client request 1 arrives. The router fires N channel calls at once, not one
after another. Each one returns `401`. As each fails, its `retry_counter` goes
to 1 and its wait goes to 1 second. The router fires the next N. It walks all
4,000 this way, N at a time.

**M fires too.** `hedge_delay_ms` is 0 by default, so the M copies of a channel
call start with the first one. Each wave is N times M calls in flight, not N.
All of them return `401`.

Client request 2 arrives a few seconds later. Every wait of 1 second already
expired, so all 4,000 channels are back in the list. The router walks all 4,000
again. Each `retry_counter` goes to 2 and each wait goes to 2 seconds.

Request 3 doubles it again, to 4 seconds. Then 8, 16, 32.

The waits grow faster than the client requests arrive, so the list empties
itself.

| Failure | Wait |
|---|---|
| 1 | 1s |
| 12 | 34 minutes |
| 17 | 18 hours |
| 20 | 6 days |
| 23 | the 30 day cap |

By failure 12 the wait is 34 minutes, which is longer than the client requests
arriving, so the list is already empty and the router answers `504` without
making a channel call.

Twelve full walks is the cost, and the router pays it once. No cap on channel
calls is needed, because the backoff is the cap.

## Cache

**`redb`, an embedded key-value store, not SQLite and not Redis.**

The brief is a key-value lookup that survives a restart. That is exactly what
`redb` is: a single-file embedded store in pure Rust, ACID, no server, no
second container, sub-microsecond point reads from the page cache.

On the two suggestions:

**SQLite is not slow.** An indexed point lookup runs at roughly 100,000 to
500,000 reads per second on a local disk. At 1 RPS it is four or five orders of
magnitude faster than needed. It would not have been the bottleneck. The better
argument against it is simply that a SQL engine is the wrong shape for a
key-value lookup, and `redb` deletes the SQL layer without adding anything.

**Redis is the wrong trade here.** It buys nothing at 1 RPS and costs a second
container, a network hop on every lookup (0.2-1ms, which is most of your 10ms
budget spent on a cache hit), and weaker durability than either embedded
option. Its default persistence snapshots periodically, so a container kill can
drop recent writes. The only case for Redis is sharing a cache between several
router instances, which is not in the brief.

If the cache ever needs to be shared across machines, Redis becomes correct.
Until then it is an extra moving part.

### Layout

Two tables in one `redb` file.

**`responses`**, the prompt and completion cache. One entry per prompt.

| Field | Type | Bytes | What it is |
|---|---|---|---|
| `key` | `[u8; 32]` | 32 | BLAKE3 of the neutral request |
| `schema_version` | `u8` | 1 | Bump it to invalidate every entry |
| `created_at` | `u64` | 8 | Unix seconds, for the 7 day check |
| `model_id` | `String` | ~30 | Which model produced this answer |
| `intelligence` | `u16` | 2 | That model's score, so a write can compare |
| `channel_id` | `u64` | 8 | Hash of provider+key+model, for logging |
| `provider` | `String` | ~12 | For logging |
| `body` | `Vec<u8>` | ~125KB | The neutral `AggLlmResponse` |

**`health`**: `thing -> (retry_counter, retry_base, eligible_at)`, where `thing`
is a channel, a provider/key pair or a provider. Flushed every 5 minutes and on
shutdown, loaded at startup. See **Startup and health**.

### The key

**BLAKE3, 32 bytes, over the neutral request.** Not over the raw body.

An Anthropic client and an OpenAI client send the same conversation as
completely different JSON:

```json
// /v1/messages
{"system": [{"type": "text", "text": "You are helpful"}],
 "messages": [{"role": "user",
               "content": [{"type": "text", "text": "fix this bug"}]}]}

// /v1/chat/completions
{"messages": [{"role": "system", "content": "You are helpful"},
              {"role": "user",   "content": "fix this bug"}]}
```

Hash the raw bytes and those become two keys, so each format runs its own cache
and neither ever finds the other's answers. Hash the `LlmRequest` after
translation and both land on the same key.

**In the key**: messages in order, system prompt, tool definitions sorted by
name, `temperature`, `top_p`, `max_tokens`, stop sequences, response format.

**Not in the key**: the model, the client's API key, headers, request ids,
timestamps, the streaming flag.

Serialise canonically before hashing: object keys sorted, no whitespace, one
fixed number format. Otherwise the same request with fields in a different
order hashes differently.

**BLAKE3, not SHA-256.** Hashing happens on every client request, over a 3.8MB
prompt.

| Algorithm | Throughput | Time for 3.8MB |
|---|---|---|
| SHA-256, no hardware support | 0.4 GB/s | 9.3ms |
| SHA-256, with SHA-NI or ARM crypto | 1.5 GB/s | 2.5ms |
| BLAKE3, single core | 5 GB/s | 0.7ms |
| BLAKE3, four cores | 15 GB/s | 0.2ms |

SHA-256 without hardware support eats the whole 10ms budget. Both give 32 bytes.

### Reading

- **Caller sent `auto`, or no model**: return the entry. Any model will do.
- **Caller named a model**: return it only if `model_id` matches. Otherwise
  treat it as a miss and go upstream.
- **Entry older than the 7 day limit**: a miss. The next write overwrites it.

### Writing: the most intelligent answer wins

**Cancel every other channel call the moment the winner arrives.** No grace
period, no waiting for a smarter model.

The winner is the first to finish, so at that moment nothing else has finished.
A smarter model is usually a slower one, and the gap is minutes:

| Generation | Time for 32k tokens |
|---|---|
| 1,000 tok/s | 32s |
| 500 tok/s | 64s |
| 100 tok/s | 5.3 min |
| 30 tok/s | 17.8 min |

Waiting long enough to catch a slower model means holding 31 channel calls open
for up to 17 minutes after the client already has its answer. That costs 0.99
million output tokens per client request, 3 billion a day, to improve a cache
that rarely gets a hit.

### The cancellation race

Cancellation is not instant. A channel call can return a good answer in the gap
between the winner landing and the cancel reaching it.

**Do not throw those away.** They are already paid for. Offer each one to the
cache under the same rule as the winner.

### The write rule

Every candidate, the winner and any late arrival, goes through the same check:

| Stored entry | Action |
|---|---|
| None | Write the candidate. |
| Older than the 7 day ttl | Overwrite. |
| `intelligence` lower than the candidate | Overwrite. |
| `intelligence` equal or higher | Keep it. Discard the candidate. |

So a late arrival from a smarter model replaces the winner in the cache, and one
from a weaker model does not. The client keeps the fast answer either way.

The same rule guards against an entry from an earlier client request: a fast
weak model cannot overwrite a slow strong one that answered the same prompt
yesterday.

`intelligence` comes from the in-memory ranking, looked up by `model_id`. A
model absent from the ranking scores 0, so it never displaces a ranked one.

**Writes must be atomic.** Several late arrivals can reach the cache at once.
Do the read, the compare and the write in one `redb` transaction, or two
candidates both read the old entry and the weaker one wins.

### One entry per prompt, by design

A flat key means a second answer to the same prompt replaces the first, subject
to the rule above.

**Worked example.** A client sends a prompt with `auto`. `glm-5.3` wins and is
cached. Later the same prompt arrives naming `claude-fable-5-1`. `model_id` does
not match, so it is a miss and goes upstream. `fable` answers. If `fable` ranks
higher than `glm-5.3`, it overwrites. If it ranks lower, the `glm-5.3` entry
stays and the `fable` answer is discarded.

**Losing the `fable` answer is acceptable.** The router is used with `auto`.
A named model is the rare case, and it stays correct: it never serves a wrong
model's answer, it just misses.

**A composite key `(prompt, model)` was considered and rejected.** It would hold
one entry per model per prompt and fix the overwrite. It is unbounded: your
providers carry 848 distinct model ids, 705 of them tool-capable, so one prompt
could hold 848 entries at 104MB.

| Models answering one prompt | GB/day | 100GB holds |
|---|---|---|
| 1, flat key, normal operation | 10 | 9.70 days |
| 2 | 21 | 4.85 days |
| 10 | 103 | 0.97 days |
| 32 | 330 | 0.30 days |
| 705, every tool-capable model | 7,268 | 0.01 days |
| 848, every model | 8,743 | 0.01 days |

Capping it would need a `max_models_per_prompt` setting and an eviction scan per
write. That is machinery for a case that does not happen under `auto`.

### Size and expiry

| Setting | Value |
|---|---|
| `ttl` | 7 days, checked on read, no background sweep |
| `max_bytes` | 100GB, checked on write |

Over the cap, delete the oldest 10% by `created_at` in one transaction, then
write. Reading the file size is one `stat`.

At 10.3GB a day the 7 day limit binds first and the cap never fires. The cap is
a backstop.

`redb` does not shrink its file after a delete. It stays at its high-water mark
and reuses the freed space, which is why the cap is on bytes.

**Never cache a failure.** Only a 2xx with non-empty content is written, the
same test the race uses to pick a winner. A cached `503` would serve for a week.

**The cache lives on disk only.** No in-memory copy of entries. A hit is rare,
because each Claude Code turn carries a longer conversation than the last, so
prompts almost never repeat. A `redb` read is tens of microseconds against a
10ms budget. RAM is the scarce resource on a 32GB box shared with other
containers, and disk is not. Cap `redb`'s own page cache so it cannot grow into
the 4GB budget.

## `GET /v1/models`

One row per distinct `model_id`, not per channel. A model served by 9 Gemini
keys is one row, not nine. Your providers carry 848 distinct model ids across
roughly 4,000 channels.

Sorted by `intelligence`, highest first. `auto` sits at the top, because it is
what a client should pick.

### Extra fields are fine

OpenAI documents `id`, `object`, `created`, `owned_by` and `shutdown_date`.
Nothing says that is the whole set, and in practice nobody treats it as one.

OpenRouter's live `/api/v1/models` returns 458 models. Of OpenAI's documented
fields it includes only `id` and `created`. It drops `object` and `owned_by`,
and adds 16 of its own: `context_length`, `pricing`, `architecture`,
`supported_parameters`, `top_provider`, `reasoning`, `name`, `description`,
`canonical_slug`, `hugging_face_id`, `default_parameters`, `knowledge_cutoff`,
`expiration_date`, `links`, `per_request_limits`, `supported_voices`.

Every coding tool reads that endpoint and copes. So put everything useful in.

### The response

```json
{"object": "list", "data": [
  {
    "id": "claude-fable-5-1",
    "object": "model",
    "owned_by": "openrouter",
    "created": 1760140800,

    "intelligence": 71,
    "intelligence_rank": 3,
    "context_length": 200000,
    "max_output": 64000,
    "supports_tools": true,
    "supports_vision": true,
    "supports_reasoning": true,

    "channels": {
      "total": 9,
      "healthy": 7,
      "in_backoff": 2,
      "providers": ["openrouter", "zai"],
      "next_eligible": "2026-10-11T03:14:22Z"
    }
  }
]}
```

Standard fields first, so a client that only knows OpenAI's four finds them at
the top. Everything else after.

### Where each field comes from

| Field | Source |
|---|---|
| `id`, `context_length`, `max_output`, `supports_tools`, `supports_vision`, `supports_reasoning` | `model_capabilities.csv`, joined on `(provider, model_id)` |
| `intelligence`, `intelligence_rank` | `models.csv`, the Artificial Analysis ranking |
| `owned_by` | The provider of the highest-ranked healthy channel for this model |
| `created` | The provider's own value where it gives one, else the file's release date |
| `channels` | The live in-memory channel list and health table |

### `channels` is the debugging view

Nine channels, seven healthy, two in backoff, and when the next one comes back.
That answers "why is this model not being used" without an admin endpoint, a
control surface or saved state. See **Applying a change** for why there is no
admin endpoint.

`next_eligible` is the soonest `eligible_at` across that model's channels. Null
when one is healthy now.

### Which models appear

**All of them, with their status.** Every model the router knows a channel for,
healthy or not.

A model with 9 channels all in backoff appears with `"healthy": 0` and a
`next_eligible` timestamp. A model on a provider that failed its reachability
probe appears the same way.

Filtering by health would delete the view of what is broken, which is half the
job of this endpoint. A client that wants only working models reads
`channels.healthy`.

Only one thing keeps a model out: the router has never seen a channel for it. A
model listed in `model_capabilities.csv` for a provider you do not hold a key
for never enters the channel list, so it never appears here.

## Streaming

`streaming: true` in `router.yml`. Off gives the behaviour in **Core loop**:
wait for a complete answer, first 2xx with content wins.

### Why it matters more than it looks

**It keeps the client connected.** Claude Code's `API_TIMEOUT_MS` defaults to
10 minutes. A channel call takes 34.5 minutes: 1,000s of prefill on a 1M token
prompt, then 1,067s of generation at 30 tokens/second. Without streaming the
client hangs up at 10 minutes, every time.

Streamed, a token arrives every 33ms at 30 tokens/second, so the client's read
timer keeps resetting and never fires.

**Streaming alone is not enough.** Nothing is sent during prefill, and prefill
on a 1M token prompt runs past the timeout on its own:

| Prefill | First token | Against a 10 min timeout |
|---|---|---|
| 1,000 tok/s | 16.7 min | over |
| 5,000 tok/s | 3.3 min | under |
| 10,000 tok/s | 1.7 min | under |
| 50,000 tok/s | 0.3 min | under |

That is what the keepalive below is for.

**It lets the router pick a fast channel.** Generation speed across your
providers spans 30 to 1,000 tokens/second, which is 17 minutes of difference on
one answer.

| Generation | Total | Client waits |
|---|---|---|
| 30 tok/s | 2,067s | 34.4 min |
| 100 tok/s | 1,320s | 22.0 min |
| 500 tok/s | 1,064s | 17.7 min |
| 1,000 tok/s | 1,032s | 17.2 min |

Without streaming the router cannot see that difference until the answer lands,
and by then the choice is made.

### Two separate settings

**`streaming` in `router.yml` is about the providers. `stream` in the client's
request is about the client.** They are independent, and a mismatch is fine.

| Client asks for | Router does |
|---|---|
| `stream: true` | Forwards the winning stream as it arrives. |
| `stream: false`, or no field | Buffers the winning stream, sends one piece. |

**Always stream from the providers.** The hedge strategy needs a stream to
measure, and giving that up because the client did not ask for streaming would
throw away the 17 minute saving for nothing.

**Buffering costs nothing.** 125KB per client request, 42.7MB at 350 concurrent,
and the router already holds that response to write it to the cache.

**Never reject a mismatch.** Returning `400` because the client's mode differs
from the router's breaks every tool for no gain.

### `hedge_strategy: ttft`

Time to first token. The first channel to emit a token wins. Cancel the other 31
immediately.

Simple and fastest to a first token. It picks whichever channel got through
prefill first, which is not the channel that will finish first.

### `hedge_strategy: tps`

Tokens per second. The default.

1. `t0` is the moment the first channel emits its first token.
2. Buffer every stream. Send nothing to the client yet.
3. Wait `hedge_window_ms`, default 1000.
4. At `t0 + 1s`: one channel streaming means it wins. Several streaming means
   measure each one's tokens per second across that window, and the highest
   wins.
5. Cancel every other channel call. Flush the winner's buffer to the client,
   then forward the rest of its stream.

**Cost: 1 second.** **Gain: up to 17 minutes**, when it picks a 1,000 tok/s
channel over a 30 tok/s one.

**Buffering is cheap.** 32 streams for 1 second holds 125KB per client request
at 1,000 tokens/second each, 12KB at 100.

### Keepalive

**The router sends a keepalive from the moment it accepts a client request until
the first real byte of the answer.** Every `keepalive_interval: 15s`.

Do not tell the user to raise `API_TIMEOUT_MS`. A router that only works after
the client is reconfigured is a broken router.

| Client | Keepalive |
|---|---|
| `stream: true` | An SSE comment line, `: ping`, which the spec says a client must ignore. Anthropic's own API sends `ping` events for the same reason. |
| `stream: false` | Send the `200` and its headers at once, then one space every 15 seconds, then the JSON body. JSON parsers skip leading whitespace, so the body still parses. |

Both keep bytes moving, so the client's read timer resets and never fires.

The space trick is ugly. It is also the only thing that keeps a non-streaming
client alive through 16.7 minutes of prefill, and every JSON parser accepts it.

Stop the keepalive the moment the first real byte goes out. For a streaming
client that is the winner's first token. For a non-streaming client it is the
opening brace of the body.

### Commitment

Once a token reaches the client, the router is committed to that channel. There
is no taking it back.

That is why `tps` buffers rather than forwarding the first token it sees. `ttft`
commits at the first token and accepts the consequence.

### When the winning stream dies

`stream_stall_timeout: 60s`. No token for 60 seconds means the channel failed.
So does a dropped connection or a mid-stream error.

**Before the winner is picked**, a dead channel drops out and the race carries
on. Nothing reached the client, so there is nothing to undo.

**After the winner is picked, end the client request.** The stream stops. The
channel is marked unhealthy and backs off as usual.

Carrying on from another channel was considered and rejected. The client already
has half an answer on screen from one model. Splicing a second model's output
onto it produces a reply that changes voice and loses the thread mid-sentence,
and no client can tell that happened.

Ending it is honest. Claude Code retries from the start, and the retry skips the
dead channel because it is now in backoff.

### Cancellation and the cache

Cancel every losing channel call the moment the winner is picked, same as the
non-streamed path.

**Only a complete stream goes in the cache.** A cancelled stream is a partial
answer, and a partial answer is wrong. This removes the late-arrival rule that
applies without streaming: a losing stream has no complete body to offer.

A cached entry replays to a streaming client as a stream. The router holds the
whole answer, so it chunks it out.

### The client disconnects

Claude Code hanging up is a cancellation like any other. Drop all 64 channel
calls at once.

Without this, a client that gives up at 10 minutes leaves 64 calls running for
another 24 minutes, burning quota on an answer nobody reads.

Watch for it on the response body sink: `axum` surfaces a dropped connection as
a send error.

### Config

```yaml
streaming: true
hedge_strategy: tps        # tps | ttft
hedge_window_ms: 1000      # tps only: how long to watch before choosing
stream_stall_timeout: 60s  # no token for this long means the channel failed
keepalive_interval: 15s    # until the first real byte reaches the client
```

### Translation

`switchyard-translation` handles streams in both directions: `decode_stream`
turns provider SSE bytes into normalised events, `encode_stream` turns them into
the target format's events. Same two-edges rule as the buffered path. See
**Format translation**.

## Status codes

Five outcomes, one code each. Every failure says which of four different things
went wrong, and each one needs a different response from the caller.

| Code | Means | Whose fault | Retry? |
|---|---|---|---|
| `200` | A channel answered, or the cache did. | | |
| `413` | The filter emptied the channel list: no model has a context window big enough for this prompt. Body names the largest found. | The caller's | No. Shorten the prompt. |
| `502` | The router cannot reach the internet. Both network test calls failed. | The local link | Yes, once the link is back. |
| `503` | RSS is above `memory_high_water`, so the request was not admitted. Carries `Retry-After` and the current RSS. | The router's | Yes. It is transient. |
| `504` | No channel could serve it: none eligible, or every channel call failed. | The upstreams' | Probably not. |

### Why `503` and `504` are separate

They were one code in an earlier draft. They are opposite problems.

**`503` means the router is full and healthy.** Every channel works, there is
just no memory to start another client request. Waiting fixes it.

**`504` means the router is idle and the providers are not answering.** There is
plenty of memory. Waiting probably does not fix it, because the cause is a cold
start, every channel in backoff, or every channel call failing.

Sharing a code would hide which of those is happening, and they call for
opposite reactions.

### What each body carries

| Code | Body |
|---|---|
| `413` | The prompt's token count, and the largest `limit_context` found among the channels. |
| `502` | Which network test calls failed. |
| `503` | Current RSS, `memory_high_water`, and `Retry-After`. |
| `504` | Channel calls made, and the last upstream error. |

### `/metrics` mirrors it

`router_requests_total{outcome}` takes one value per code: `hit_cache`,
`success`, `too_large`, `no_network`, `not_admitted`, `no_channel`.

A rise in `not_admitted` means buy memory or lower `http2_stream_window`. A rise
in `no_channel` means the providers are broken. The two need opposite actions,
which is the whole reason for the split.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/messages` | Anthropic format. Claude Code uses this. Primary path. |
| `POST /v1/chat/completions` | OpenAI format. Cline, Cursor, scripts. |
| `GET /v1/models` | Merged catalogue, ranked, with live channel health. Clients fill a picker from it; you read it to see what is broken. See **`GET /v1/models`**. |
| `GET /healthz` | Docker health check. |
| `GET /metrics` | Prometheus text. Omniroute died silently; this is why. |

Not built:

- `POST /v1/responses`. OpenAI's newer stateful API. Different shape, no
  channel speaks it, no named client needs it.
- `POST /v1/completions`. Legacy, no tool calling, nothing uses it.

Any client API key is accepted. The gateway holds the real keys and listens on
the LAN only.

Internally everything normalises to the Anthropic message shape, which is the
stricter of the two. Translating down to OpenAI is lossless in the direction
that matters.

## Startup and health

**Startup never blocks on an upstream.** The router binds its port and serves
immediately, with zero channels. Everything is unhealthy until proved otherwise.
`/healthz` and `/metrics` are up from the first moment. A container that starts
and reports its problem beats one that crash-loops, which is what you are
replacing.

**No channels available means `504`.** That covers the first second of a cold
start and it covers every channel being in backoff. Boot takes a few seconds, so
in practice the catalogue is usually full before a request arrives. Do not hold
a request waiting for one.

The catalogue then fills itself in the background, in two waves.

### The unit of health

**The provider/key pair, not the provider.** You hold 47 keys across 15
providers. One expired Gemini key must not disable the other eight.

| Level | Unhealthy when |
|---|---|
| Channel (provider/model/key) | A live call fails in a way that will repeat. |
| Provider/key | Its keyed probe fails, **or** every channel on that pair is unhealthy. |
| Provider | Its reachability probe fails, **or** every key on that provider is unhealthy. |

### The two probes

1. **Reachability, no key.** Call the provider's `/models` with no
   `Authorization` header. Any HTTP status back, `401` included, means the host
   is alive. Only a connection error, a DNS failure or a timeout is a fail. If
   the endpoint will not answer without a key, it will not answer with one, so
   there is no point trying nine of them.
2. **Keyed.** Call `/models` with one key. A 2xx gives that pair's model list,
   and every model on it becomes a channel. Anything else marks the pair
   unhealthy.

**A `/models` endpoint is required.** No list, no channels. Anything other than
a 2xx leaves the pair unhealthy, and do not probe with a completion call
instead. The single exception is a provider that has no model-listing endpoint
at all: it answers `404` or `405`, and if it carries a `models` list in
`providers.yml` the probe uses that list instead. Cloudflare Workers AI is the
case this exists for. See the "Config" section for the shape.
A `401`, a `403`, a `429` or a `5xx` is still a failed probe, so a dead key
never produces channels.

Every other model fact the router holds came from a `/models` endpoint in the
first place. `model_capabilities.csv` is 9,927 rows built that way. A provider
that answers `/models` but lies about it cannot be corrected from config.

**Do not test the providers by hand before building this.** The router probes
all 15 on every startup. That is the test. A provider whose `/models` probe
fails marks itself unhealthy, costs one keyless call a month, and is reported on
`/healthz`. There is nothing to decide in advance, nothing to prune from
`providers.yaml`, and no list to hand-maintain.

The same rule covers every other "let me check the providers first" idea. The
probes are the source of truth and they run without you.

Wave 2 only runs against providers wave 1 proved healthy. Everything inside a
wave runs at once.

```
15 providers          probe 1, parallel          ~15 calls
  └─ healthy ones     probe 2, one per key       ~47 calls
       └─ every model listed becomes a channel
```

Two waves, about 62 calls, a few seconds. The catalogue is full.

### No model probe

**Do not test each model at startup.** Testing 4,000 tuples means 4,000
completion calls. Openrouter alone is 8 keys times 417 models, 3,336 of them.
Groq's free tier allows roughly 30 requests a minute, so the probes would be
rate limited into uselessness and would spend the per-minute budget the first
real request needs.

Live traffic gives the same information for free. Every request fires the top N
channels by intelligence. A channel that fails in a way that repeats is marked
unhealthy there and then. The channels that matter, the most intelligent ones,
get tested first and constantly. The ones at the bottom of the ranking are never
tested because they are never reached, which is the correct amount of effort to
spend on them.

The pre-filter already removes the obviously dead tuples before any call:
context window too small, `tool_call` is 0, the key cannot reach the model.

### Channel health from live traffic

**Health is binary and uses the same test the race uses to pick a winner.**

- Healthy: a 2xx carrying non-empty content.
- Unhealthy: anything else. A `4xx`, a `5xx`, a `429`, a timeout, a connection
  error, an empty 200, an error object in a 200 body.

No special cases, no classifying errors into kinds. A channel that did not
answer properly is a channel we would rather not call next time.

**One channel, one count.** M hedged copies of the same channel call are one
channel. If every copy fails, `retry_counter` goes up by 1, not by M. If any
copy succeeds, the channel is healthy and the counter resets.

**Cancelled is not unhealthy.** A channel that loses the race is cancelled
mid-flight. It did not fail, it was not given the chance to finish. It keeps its
current state.

When the **last** healthy channel on a provider/key goes unhealthy, the pair is
marked unhealthy and handed to the poller. When the last healthy key on a
provider goes unhealthy, so is the provider.

### Backoff

Every unhealthy thing carries two numbers, and the same maths applies to a
channel, a provider/key pair and a provider alike.

| Field | Meaning |
|---|---|
| `retry_counter` | Consecutive failures. 1, 2, 3, and so on. |
| `retry_base` | Seconds. 1 by default. |

The cap is **30 days**, not an hour.

```
wait_seconds = min(2_592_000, retry_base * 2.pow(retry_counter - 1))
```

On a failure, add 1 to `retry_counter` and wait that long before the thing is
eligible again.

**`Retry-After` sets `retry_base`.** A response carrying the header replaces the
default 1 with the number of seconds the provider named. If the header holds a
date rather than a number, use the seconds between now and that date. Everything
else is unchanged: the same doubling applies on top.

So a provider saying 60 seconds gives 60s, then 120s, then 240s on the third
consecutive failure, capped at 30 days.

**Why the cap is 30 days and not an hour.** A monthly quota is the normal way a
free tier runs out, and the provider says so: `Retry-After` carries a figure in
days. An hourly cap would ignore that and retry a channel 24 times a day for the
rest of the month, failing every time, burning rate limit and keeping a dead
channel at the top of the ranking. 30 days matches how the quotas actually
reset.

**On a success, reset both.** `retry_counter` back to 0 and `retry_base` back to
1, so the next failure is failure number one again and waits 1 second. A success
anywhere resets the thing that succeeded: the channel, its pair, or its
provider.

That handles your Z.ai flat plan without any quota accounting. An exhausted
monthly allowance returns `429` with a long `Retry-After`, and the channel goes
quiet for exactly that long instead of wasting a slot in N on every request.

### When the network itself is down

A dropped link would otherwise mark every channel unhealthy in one second, for a
fault that is nothing to do with any provider.

**Every client request fires a network test alongside its upstream calls**, in
the same `tokio::select!`, not after a failure. `GET
https://www.google.com/generate_204` and `GET
https://cloudflare.com/cdn-cgi/trace`, 2 second timeout, two operators so one
outage does not fool it. One extra request at 1 RPS costs nothing, and the
answer arrives at the same time as the failures rather than after them.

If the test fails while channels are in flight:

- Do not touch `retry_counter` on any channel, in flight or already failed. The
  fault is local.
- Cancel the rest of the walk. Do not try the next channel.
- Return **`502`**, network unavailable. Distinct from `504`, which means the
  network is fine and no channel is eligible.

Requests fail while the link is down. That is correct and unavoidable. When it
returns, the first channel is eligible 1 second later, because nothing inflated
its counter while the fault was local.

| Code | Means | Whose fault |
|---|---|---|
| `413` | The filter emptied the channel list: no model has a large enough context window for this prompt. The body names the largest found. | The caller's. No wait helps. |
| `502` | The router cannot reach the internet. Both network test calls failed. | The local link. |
| `503` | RSS is above `memory_high_water`, so the request was not admitted. Carries `Retry-After` and the current RSS. | The router's. Transient. |
| `504` | No channel could serve it: none eligible, or every channel call failed. | The upstreams'. |

`503` and `504` were one code in an earlier draft. They are opposite problems.
A `503` means the router is full and healthy, so retrying later works. A `504`
means the router is idle and the providers are not answering, so retrying later
probably does not.

### The poller

Only unhealthy things get polled. Each unhealthy thing carries its own backoff.

| Unhealthy | Poll with | On success |
|---|---|---|
| Provider | Probe 1, keyless | Mark healthy, run probe 2 on all its keys. |
| Provider/key | Probe 2, keyed | Mark healthy, refresh its model list, re-add its channels. |
| Channel | Nothing | It rejoins the pool when its backoff expires. The next real request that reaches it is the test. |

A channel differs only in that nothing polls it. It is hidden from the channel
list until its backoff expires, then it goes back in. If the next real request
that reaches it fails again, its backoff doubles and it drops out again. A
permanently dead channel settles at one attempt a month, carried by traffic that
was happening anyway.

A dead provider costs one keyless call a month, not nine keyed ones, because it
stays on probe 1 until its host answers.

### Swapping the list while serving

Both the poller and the startup waves change the channel list while requests
hold a reference to it. Hold the list in an `ArcSwap<Vec<Channel>>`. A request
clones the `Arc` once at the start and uses that snapshot to the end. A writer
builds a new list and swaps the pointer. No locks, no torn reads, and a request
in flight finishes against the list it started with.

This is why config reloads still need a restart: the poller rebuilds only from
the files already loaded, and never re-reads them.

**Health state survives a restart.** Hold it in memory while running, flush to
`redb` on shutdown, load it back at startup. Same store and same pattern as the
prompt cache, a second table in the same file.

Store an absolute deadline rather than a remaining wait, so a deadline already
past at load time means the thing is eligible at once. A container that was down
for an hour comes back with everything eligible, which is right.

**Flush every 5 minutes from a background task**, not only on shutdown. A kill
then loses at most 5 minutes of state instead of all of it.

Losing the table is expensive, which is why the flush is periodic. Perhaps half
of several thousand tuples are permanently dead. Starting them all at
`retry_counter = 0` means the router walks hundreds of known-dead channels on
every request until it relearns them, and that is exactly the behaviour this
design exists to remove.

`GET /healthz` returns 200 from the first second, with the live channel count,
the unhealthy providers and pairs, and when each is next due.

## The channel list and one client request

### The channel record

One row per provider/model/key tuple. Everything the router needs is on the row,
so the filter is one pass over a vector with no lookups.

| Group | Fields |
|---|---|
| Identity | `id` (hash of provider+key+model), `provider`, `model_id`, `key_id` |
| Capability | `limit_context`, `limit_output`, `tool_call`, `reasoning`, `modalities_input` |
| Ranking | `intelligence`, `intelligence_rank` |
| Health | `enabled`, `retry_counter`, `retry_base`, `eligible_at`, `in_flight` |
| Telemetry | `calls`, `successes`, `failures`, `last_latency_ms` |

**The list is always sorted by `intelligence`, highest first.** Sorted once when
it is built, and again whenever the health poller rebuilds it. Never sorted per
client request.

### Why the channel list is not in `redb`

The health table lives in `redb` for durability. The channel list does not live
there at all. Two reasons.

**The write rate.** `in_flight` changes twice per channel call. `retry_counter`
changes on every failure. At 64 channel calls a second that is 128 disk
transactions a second, each with an fsync, to track something that only matters
in memory.

**The scan.** The router asks for the first row in intelligence order that
passes the predicate. `redb` would deserialise every row it steps over.

| Store | Per row | 32 rows | 8,002,000 rows |
|---|---|---|---|
| `Vec` in memory | ~2ns | 0.0ms | 16ms |
| `redb` read plus deserialise | ~2us | 0.1ms | 16,004ms |

The normal case survives either way. The doomed walk goes from 16 milliseconds
to 16 seconds.

**So: memory for the hot path, `redb` for durability.** Health flushes every 5
minutes. The channel list is never persisted, because startup rebuilds it from
the providers' `/models` endpoints in a few seconds.

### Channel id

A channel's id is a hash of `provider` + `key` + `model`. Deterministic and
unique, so it survives the list being rebuilt and means the same thing in a log
a week later. Never use a list index to name a channel.

### One client request

1. **Start an empty `excluded` set** of channel ids.
2. **Ask the list for N channels in one query.** In SQL for clarity:

   ```sql
   SELECT * FROM channels
   WHERE state = 'active' AND context_size >= C AND tool_usage = true
     AND id NOT IN (excluded_channel_ids)
   LIMIT 32
   ```

   In Rust: one scan down the sorted vector, collecting rows that match until
   it has N of them. No `ORDER BY` is needed, because the vector is already
   sorted by `intelligence`, highest first.
3. **Add their ids to `excluded` and call all N**, M hedged copies each.
4. **When a channel call fails, run the same query with `LIMIT 1`** and fire the
   one channel it returns. Do not wait for the others. N stays in flight. This
   is spec item 8: move to the next immediately.
5. **First 2xx with non-empty content wins.** Cancel the token, which tears
   down every other call in flight.
6. **The query returns nothing**: `504`, with the number of channel calls made
   and the last upstream error.

**A sliding window of N, not waves of N.** At any moment N channel calls are
running, M copies each. One finishes, one starts.

### Why query each time, not filter once

**Decision: option B.** Two options were costed in full. The working is in
`scan_cost.py`, which reproduces every number below.

**Option A, snapshot once.** At the start of a client request, do one pass over
all 4,000 channels and copy the matches into a local list. Walk that list to the
end. No further queries.

**Option B, query the list.** Keep a set of channel ids already tried. Ask the
list for N channels to start, then ask for 1 more each time a channel call
fails. This is what the router does.

#### What each option costs in memory

Three separate things, which an earlier draft ran together.

1. **The master channel list.** 4,000 rows at roughly 200 bytes, 781KB. Router
   state. It exists either way and argues for neither option.
2. **What A adds per client request.** A filtered copy. Copying references into
   the master list costs 8 bytes each. A holds the `Arc`, which keeps the master
   list alive even if the poller rebuilds it, so references are safe. Copying
   whole rows instead would cost up to 781KB per client request, 23MB at 30
   concurrent. The figures below use references, which is A at its best.
3. **What B adds per client request.** Nothing is copied. B reads the master
   list directly and keeps only the ids it has tried, 16 bytes each in a hash
   set.

#### The formula

One client request issues one opening query with `LIMIT N`, then one query with
`LIMIT 1` for every channel call that fails.

The opening query is a single scan. It reads about `N / eligible_share` rows and
returns all N channels at once.

Each replacement query scans from the top again. Tried channels are exactly the
highest-ranked eligible rows, so they sit at the top, and query number `j` steps
over them before it finds a new one.

Total row checks for `k` channels tried is `k(k+1)/2`, minus the `N(N-1)/2` the
opening query saves by reading its N rows in one pass, scaled up by the share of
rows the predicate rejects.

**N and M do not change the total meaningfully.** A larger N moves work into the
opening query, which is cheaper, but it does not change how many channels get
tried.

#### The numbers

| Scenario | Channels tried | A rows | A queries | A time | A RAM | B rows | B queries | B time | B RAM |
|---|---|---|---|---|---|---|---|---|---|
| 1 Everything working | 32 | 4,000 | 1 | 0.012ms | 31.2KB | 32 | 1 | 0.000ms | 0.5KB |
| 2 Working, 200k prompt | 32 | 4,000 | 1 | 0.010ms | 15.6KB | 58 | 1 | 0.001ms | 0.5KB |
| 3 Best channels rate limited | 500 | 4,000 | 1 | 0.010ms | 15.6KB | 256,949 | 469 | 2.1ms | 7.8KB |
| 4 Every key dead, big prompt | 1,973 | 4,000 | 1 | 0.010ms | 15.6KB | 3,988,346 | 1,943 | 33ms | 30.8KB |
| 5 Every key dead, cold start | 4,000 | 4,000 | 1 | 0.012ms | 31.2KB | 8,001,504 | 3,969 | 120ms | 62.5KB |

At 30 concurrent client requests:

| Scenario | A RAM | B RAM |
|---|---|---|
| 1 Everything working | 937.5KB | 15.0KB |
| 2 Working, 200k prompt | 468.8KB | 15.0KB |
| 3 Best channels rate limited | 468.8KB | 234.4KB |
| 4 Every key dead, big prompt | 468.8KB | 924.8KB |
| 5 Every key dead, cold start | 937.5KB | 1,875.0KB |

Costed at 2ns for a row that fails an integer compare, 15ns for an eligible row
that also needs the `excluded` lookup, and 1ns to push a reference. Those three
are estimates, not measured. The row and query counts are exact: `scan_cost.py`
walks a concrete list rather than applying the formula.

Against a 4GB budget, the largest figure in either table is under 2MB. RAM does
not decide this.

#### What each scenario means

**1. Everything working.** Normal day. Short prompt, so every model's context
window can hold it. Every channel enabled, nothing in backoff. The router needs
32 channels and the first to answer wins. A reads all 4,000 rows to find them.
B reads 32 and stops.

**2. Working, 200k token prompt.** Same healthy day, long conversation. Half the
channels have a context window too small, so the filter throws them out. A still
reads all 4,000. B reads 58 to find its 32, stepping over the rejected ones.

**3. Best channels rate limited.** Sustained use. Groq and Cerebras sit at the
top of the ranking and return `429`. The router replaces each failed channel one
at a time, working down the list, until it reaches a provider with capacity. 500
channels before one works.

**4. Every key dead, big prompt.** Every key expired overnight. The long prompt
means only 1,973 channels qualify. All are called, all return `401`, the client
gets `504`.

**5. Every key dead, cold start.** The router restarted, so the health table is
empty and nothing is in backoff. The short prompt means all 4,000 channels
qualify and all are called. `504`. This happens once: every channel picks up a
`retry_counter` on the way through, so the next client request finds a shorter
list. See the worked case in **Startup and health**.

#### Verdict

Scenarios 1 and 2 are every normal client request. B is 10 to 20 times faster
there and uses 60 times less memory.

Scenarios 3, 4 and 5 favour A on paper. They do not in practice. Those walks
last 25 seconds or more, measured below, so B's extra milliseconds are
invisible, and its extra megabyte sits inside a 4GB budget.

**The deciding argument is not the numbers.** A reads health once, at the start.
It cannot see a channel recover or die during the walk. B reads health at the
moment the channel is needed.

Wall clock of the walk itself, at N=32 channel calls in flight, 4,000 channels:

| Failure mode | Rounds | Per channel call | Total |
|---|---|---|---|
| Fast `401` | 125 | 200ms | 25s |
| Timeout | 125 | 60s | 7,500s |

**No cursor.** The `excluded` set does the job, and a list index means nothing
after a rebuild. Tried channels cluster at the top of the scan and cost one hash
lookup each to skip.

Two earlier drafts were wrong here. One said 16 million lookups, which was `k`
squared rather than `k(k+1)/2`, double the real figure. The other had the router
issue N separate `LIMIT 1` queries to fill the window, which cost 528 row checks
in the normal case instead of 32.

### When the list changes underneath

Health changes are fields on an existing row. Mutate in place. No rebuild, no
swap.

Only one thing changes membership: a provider's model list refreshed after it
recovers, which adds or removes rows. That is rare. Hold the list in an
`ArcSwap<Vec<Channel>>` so a rebuild is a pointer swap, and a query in progress
finishes against the vector it started reading.

A client request already running keeps querying, so it picks up the new list on
its next query. The `excluded` set still works, because ids are hashes and not
indices.

### What N=32 means for the answer

**The winner is the fastest of the top 32, not the most intelligent.** First 2xx
with content wins, and all 32 start at the same moment. Ranking decides which 32
channels enter the race. It does not decide which one wins.

That is the intended trade: 32 of your best channels, whichever answers first.
A slow call to the single best model loses to a fast call to the 20th best.

Lower N to bias towards intelligence. N=1 always uses the best channel and waits
for it. N=32 is a latency choice.

## Concurrency against one provider

**Decision: no limit.** No semaphore, no permit to take before a channel call,
no per-provider or per-key cap. Every client request fires its channel calls
immediately.

### The numbers

Defaults are N=32 and M=2, so one client request puts **64 channel calls** in
flight. Target is 30 concurrent client requests, which is **1,920 channel calls**
at once. They land on the same few providers, because the ranking puts the same
best models at the top of every list. Some come back `429`.

1,920 open sockets is nothing for tokio. Memory is not the constraint here. An
earlier draft claimed it was and that was wrong.

N=32 does spread the load, which helps: 32 different channels means 32 different
provider/model/key tuples, and the keys differ across most of them.

### Why no limit

**The limit is a guess.** `max_inflight_per_key: 4` is a made-up number. Groq,
Gemini, openrouter and Cerebras have different limits, on different windows,
and they change without notice. One number across 47 keys is wrong for all of
them. Set it too low and the router refuses to use channels that work. Set it
too high and it does nothing.

**Hitting the limit is how the router learns where it is.** A provider's `429`
with `Retry-After` is better information than any number in a config file, and
it arrives for free.

**A `429` usually costs no latency.** N channel calls run at once. One returns
`429` while another returns a real answer, and the client never sees the
failure. Latency grows only when every channel in the wave fails, and then the
next wave is the fallback spec item 8 already calls for.

**It is less code.** A semaphore map needs permits acquired on every entry path
and released on every exit path, including cancellation, which is the path that
leaks. The race cancels channel calls constantly.

### Self-inflicted 429

One rule is needed, because a burst must not poison the channel list.

Health is binary, so a `429` marks the channel unhealthy and backs it off. With
`Retry-After` that backoff is long. Thirty concurrent client requests would push
the best channels out of the list for minutes, and the damage outlives the
burst: the next single client request finds the top of the ranking empty.

The router knows its own in-flight count per channel, so it can tell the two
apart.

| Condition | Meaning | Action |
|---|---|---|
| `429`, and this channel already had other calls in flight | The router caused it | Fail this channel call. Move to the next channel. Leave `retry_counter` and `retry_base` alone. |
| `429`, and this channel had no other calls in flight | A real limit | Back off normally. `retry_counter` + 1. `Retry-After` becomes `retry_base`. |

The in-flight count is a per-channel counter, incremented when a channel call
starts and decremented when it ends, cancellation included. It is read once, at
the moment the `429` arrives.

M hedged copies of the same channel call do not count as "other calls in
flight". They are one trial. See **M and health**.

### What this does not protect against

A provider that bans a key for sustained abuse, rather than rate limiting it.
No evidence any of your 15 do this. If one does, the key goes unhealthy and the
poller finds out when it recovers.

## Client authentication

Spec item 6: the client API key does not matter. Read the `Authorization`
header, ignore it, never forward it.

Claude Code refuses to start without a key set, so the header always arrives.
The router accepts any value, junk included, and drops it. The upstream key
comes from the channel. Forwarding the client key would leak it to 15 providers
and break every channel at once.

Bind to `0.0.0.0`. Network security sits elsewhere on the box, not here. No
password, no allowlist, no token check.

**Later:** a single shared token is the obvious next step, as `require_token` in
`router.yml`, empty by default and `401` on a mismatch. Not built now. Writing
the header handling so the value is read and discarded, rather than never
parsed, keeps that change to about ten lines.

Log the first 6 characters of the client key with each request. Not for
authentication: for telling two tools apart when both are running.

## Format translation

Claude Code speaks Anthropic. Every channel speaks OpenAI. Do not write the
translation by hand, and do not do it per channel.

Use **`switchyard-translation`** (NVIDIA NeMo Switchyard, Apache-2.0, 0.3.0
published 22 September 2026, 27,492 downloads,
<https://github.com/NVIDIA-NeMo/Switchyard>). It is the exact shape already
decided here: provider JSON in, a neutral type in the middle, provider JSON out.

```rust
use switchyard_translation::{decode_request, encode_request,
    decode_aggregated_response, encode_aggregated_response, WireFormat};

// edge in: whichever endpoint the client called
let req = decode_request(WireFormat::AnthropicMessages, body)?;

// edge out: every channel, one format
let upstream = encode_request(WireFormat::OpenAiChat, &req)?;

// the winner comes back
let agg = decode_aggregated_response(WireFormat::OpenAiChat, resp)?;
let out = encode_aggregated_response(WireFormat::AnthropicMessages, &agg)?;
```

`WireFormat` has three variants: `OpenAiChat`, `AnthropicMessages`,
`OpenAiResponses`. The third one makes `POST /v1/responses` nearly free later.

The neutral type `LlmRequest` carries `Message`, `InstructionBlock` (the system
prompt), `ToolDefinition`, `ToolCall`, `ToolResult`, `ToolChoice`,
`SamplingParams` and `OutputParams`. `AggLlmResponse` carries `ResponseOutput`,
`Usage` and `StopReason`. Buffered functions exist alongside the streaming ones,
so the no-streaming decision costs nothing.

The crate ships `normalize_anthropic_tool_use_ids` and
`sanitize_anthropic_tool_use_id`. Those two names are the proof it already
handles the hardest part: tool-call ids surviving the round trip.

Translation runs twice per request, at the two edges. The racing code only ever
sees `LlmRequest`. Add a provider and nothing in the translation path changes.

**Decision: Switchyard first, `llm-bridge-core` second.** Build against
`switchyard-translation`. Switchyard is pre-alpha and its README says so, so if
it does not hold up in the tool-calling tests, swap to `llm-bridge-core`
(Apache-2.0, Anthropic and OpenAI only, 1,248 downloads,
<https://github.com/TokenFleet-AI/llm-bridge-rust>). Hand-rolling the six
mappings below is the third option and only if both crates fail. Pin the
version of whichever one ships.

Keep the swap cheap: put every call to the crate behind one module, `wire.rs`,
with four functions matching the four calls above. Nothing else in the router
names the crate.

| What moves | Anthropic | OpenAI |
|---|---|---|
| System prompt | top-level `system`, an array of blocks | first message, `role: system` |
| Tool definition | `input_schema` | `{"type":"function","function":{...}}` |
| Tool call | `tool_use` block, `input` is an object | `tool_calls`, `arguments` is a JSON string |
| Tool result | `role: user` with `tool_result` | `role: tool` with `tool_call_id` |
| Stop reason | `end_turn`, `tool_use`, `max_tokens` | `stop`, `tool_calls`, `length` |
| Content | array of blocks | one string |

## What it records

Omniroute died quietly. These four metrics are the reason `/metrics` exists.

| Metric | Why |
|---|---|
| `router_requests_total{outcome}` | One per code: `hit_cache`, `success`, `too_large` (413), `no_network` (502), `not_admitted` (503), `no_channel` (504). |
| `router_rss_bytes` against `memory_high_water` | How close the gate is to firing. The one graph to watch. |
| `router_attempts_total{provider,model,outcome}` | `success`, `http_error`, `timeout`, `cancelled`. A provider and model that flips to all `http_error` overnight is dead. Labelled by provider and model, not by channel id: a channel id is per key, which would put tens of thousands of series on the page. |
| `router_latency_seconds` | Histogram, split into time to the winning response and time added by the router. The second one proves the latency target. |
| `router_rss_bytes`, `router_cache_entries` | Memory growth killed omniroute and nobody watched. A flat line here is the proof it is not repeating. |

One log line per request at info: outcome, winning channel, attempts, total
milliseconds. No prompts, no responses, no keys. At 1 RPS that is 86,000 lines a
day, which Docker can hold.

Failed attempts log at debug, off by default. Turn it on to find out why a
provider stopped answering.

## Deployment

Spec item 4: lightweight, via docker compose. One service, no sidecar.

### The image

`debian:bookworm-slim`, not `scratch` and not Alpine. About 125MB.

```dockerfile
FROM rust:slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl procps iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/llmrouter /usr/local/bin/llmrouter
EXPOSE 8080
ENTRYPOINT ["llmrouter"]
```

### Why Debian and not scratch or Alpine

**A shell is worth having.** When a provider starts failing, `curl`, `ss` and
`ps` inside the container answer in seconds what reading logs answers in
minutes. A `scratch` image gives none of that.

**musl's allocator is slow under this workload.** Alpine uses musl libc, whose
`malloc` performs poorly with multithreaded allocation-heavy work. This router
parses a 3.8MB JSON document per client request across many threads, which is
exactly that pattern. glibc handles it better.

**Size does not matter here.** Compressed base layers, amd64:

| Base | Compressed | With the binary and debug tools |
|---|---|---|
| `scratch` | 0 MB | ~15 MB |
| `alpine:latest` | 3.7 MB | ~25 MB |
| `debian:bookworm-slim` | 26.9 MB | ~125 MB |

110MB of difference on a box with a 100GB cache pool is noise.

**Use jemalloc explicitly**, through `tikv-jemallocator`, whatever the base:

```rust
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

The 15% fragmentation figure in **Resource budget** assumes jemalloc. glibc's
`malloc` fragments more on this allocation pattern, and that slack is what sits
between the 3GB gate and the 4GB limit.

### The compose file

```yaml
services:
  llmrouter:
    # No `image:` tag: Compose names the image after the project, so two
    # checkouts never share one global tag.
    build: .
    restart: unless-stopped
    ports:
      - "0.0.0.0:8080:8080"
    mem_limit: 4g
    ulimits:
      nofile: 65536
    volumes:
      - /mnt/shuttle/share/app-data/llmrouter/config:/config:ro
      - /mnt/shuttle/share/app-data/llmrouter/data:/data
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:8080/healthz"]
      interval: 30s
      start_period: 10s
```

### Why each line is there

**`mem_limit: 4g` makes the budget real.** Without it the process grows into the
32GB the box shares with Frigate, Plex and the rest. With it, passing 4GB kills
the container and Docker restarts it. `memory_high_water: 3GB` exists so that
never happens: the router refuses work before the kernel refuses it for him.

**`ulimits.nofile: 65536`.** The Linux default is 1,024. At 90 connections the
router is nowhere near either number, but a provider stuck on HTTP/1.1 pushes
the count up fast, and running out of descriptors fails in confusing ways.

**`restart: unless-stopped`.** The health table flushes every 5 minutes, so a
restart costs at most 5 minutes of learned health. The cache is on the pool and
survives.

**`0.0.0.0:8080`.** Network security sits elsewhere on the box. See **Client
authentication**.

### The two volumes

| Host path | Container path | Mode | Holds |
|---|---|---|---|
| `/mnt/shuttle/share/app-data/llmrouter/config` | `/config` | read-only | `.env`, `providers.yml`, `models.yml`, `intelligence.yml`, `router.yml`, `model_capabilities.csv` |
| `/mnt/shuttle/share/app-data/llmrouter/data` | `/data` | read-write | `cache.redb`, `spill/` |

Read-only on `/config` means a bug cannot rewrite your keys.

`/data` sits on the ZFS pool, so the cache and the health table survive the
container being deleted and recreated. That is the requirement in the brief.

### The health check

`curl` is in the image, so the check is the obvious one:

```yaml
test: ["CMD", "curl", "-fsS", "http://localhost:8080/healthz"]
```

`start_period: 10s` covers the two startup probe waves. The router serves from
the first second, so this only stops Docker marking it unhealthy while the
catalogue fills.

### Applying a config change

Edit the file under `/mnt/shuttle/share/app-data/llmrouter/config`, then
`docker compose restart llmrouter`. Boot re-reads every file and re-probes the
providers. See **Config**.

## Every number here is a starting point

**Nothing in `router.yml` is a commitment.** N, M, the backoff cap, the stream
window, the memory high water, the cache size: all of them are config. Edit the
file, restart the container, done in two seconds.

The job of this document is to pick a defensible starting value for each one and
say what it costs, so the first change is informed rather than a guess.

| Setting | Start at | Change it when |
|---|---|---|
| `race_width` (N) | 32 | Provider quotas run dry, or the answer is good enough from fewer channels. |
| `max_per_channel` (M) | 2 | Latency is fine without the hedge, or a key keeps hitting its rate limit. |
| `backoff_max` | 30d | Channels come back sooner than a monthly reset. |
| `http2_stream_window` | 131072 | `router_rss_bytes` approaches `memory_high_water`. |
| `memory_high_water` | 3GB | The container memory limit changes. |
| `cache.max_bytes` | 100GB | The pool fills, or 7 days of entries never gets close. |
| `request_timeout` | 3600s | Models are faster than 30 tokens/second. |

### What N=32 and M=2 cost

At 3,000 client requests a day with 1M token prompts:

| Setting | Calls per request | Input tokens/day | Per key, across 47 | Against 3B used |
|---|---|---|---|---|
| N=32, M=2 | 64 | 192 billion | 4.1 billion | 64x |
| N=8, M=1 | 8 | 24 billion | 0.5 billion | 8x |
| N=4, M=1 | 4 | 12 billion | 0.3 billion | 4x |
| N=2, M=1 | 2 | 6 billion | 0.1 billion | 2x |
| N=1, M=1 | 1 | 3 billion | 0.1 billion | 1x |

189 billion tokens a day buy answers that get cancelled and thrown away.

**Starting at N=32, M=2.** The trade is the best model slowly against the
twentieth best model fast, and 32 picks the second. The 30 day backoff cap makes
the failure mode clean: a channel that exhausts its monthly quota says so
through `Retry-After`, goes quiet for the right length of time, and the router
walks down the ranking to channels with quota left.

Watch `router_attempts_total{provider,model,outcome}` on `/metrics`. If the top of the
ranking sits permanently in backoff, lower N.

## Config

Split by concern, not one file. Restart to apply any change.

| File | Holds |
|---|---|
| `.env` | Secrets. API keys, one variable per channel. Never committed. |
| `providers.yml` | Channels: name, base URL, wire format, which `.env` keys it uses. |
| `models.yml` | Per-model overrides: context window, tool support, name mapping. |
| `intelligence.yml` | The ranking. Built from `models.csv`. |
| `router.yml` | N, M, streaming and hedge strategy, timeouts, health and backoff, memory gate, cache path and size. |
| `model_capabilities.csv` | Context window, max output and tool support per `(provider, model_id)`. Missing rows mean defaults, so a missing file silently widens the race. |

`providers.yml` names its keys instead of holding them, so the file is safe to
read and share:

```yaml
providers:
  - name: gemini
    wire: openai
    base_url: https://generativelanguage.googleapis.com/v1beta/openai
    keys_env: GEMINI_KEYS        # comma separated in .env
  - name: openrouter
    wire: openai
    base_url: https://openrouter.ai/api/v1
    keys_env: OPENROUTER_KEYS
  - name: cloudflare-b4b050cc
    wire: openai
    base_url: https://api.cloudflare.com/client/v4/accounts/b4b050cc/ai/v1
    keys_env: CLOUDFLARE_B4B050CC_KEYS
    models:                      # used only when the probe gets 404 or 405
      - '@cf/meta/llama-3.1-8b-instruct'
```

A provider with no model-listing endpoint answers `404` or `405` on the
`/models` probe. Cloudflare Workers AI is one: the OpenAI-compatible path takes
`POST` only. Such a provider lists its models under `models`, and the probe
uses that list in place of the one it could not fetch. Any other status, `401`
included, is still a failed probe, so a dead key never produces channels.
Set `models` on such a provider and nothing else: a base URL missing its version
segment also answers `404`, and a `models` list would turn that into a healthy
provider whose every chat call fails. `llmrouter-make-config` therefore copies
`known_models` across for Cloudflare only.

`router.yml`:

```yaml
race_width: 32           # N: concurrent channel calls, different channels
max_per_channel: 2       # M: hedged copies of one channel call
hedge_delay_ms: 0        # 0 = the M copies fire with the first one
streaming: true
hedge_strategy: tps           # tps | ttft
hedge_window_ms: 1000         # tps only: how long to watch before choosing
stream_stall_timeout: 60s     # no token for this long means the channel failed
keepalive_interval: 15s       # until the first real byte reaches the client
request_timeout: 3600s        # one channel call; the client request has no cap
connect_timeout: 5s       # handshake only; a dead host fails fast
spill_above_concurrent: 100
spill_path: /data/spill
memory_high_water: 3GB        # stop admitting client requests above this
http2_stream_window: 131072   # receive buffer per request slot
http2_connection_window: 1048576
health_poll:
  probe_timeout: 5s      # per probe, every wave
  retry_base: 1s         # default base; Retry-After overrides it per failure
  backoff_max: 30d       # wait = min(backoff_max, retry_base * 2^(counter-1))
  network_test_timeout: 2s  # runs beside every request's upstream calls
cache:
  enabled: true
  path: /data/cache.redb
  max_bytes: 100GB       # backstop; the 7 day ttl normally binds first
  flush_interval: 5m     # background flush of both tables
  ttl: 168h
```

### Applying a change

Edit the file, restart the container. Boot reads every file, queries each
provider's model list, joins against the ranking, sorts, and serves. One to two
seconds. At 1 RPS a restart costs one request.

No file watcher. The health poller already swaps the channel list safely through
an `ArcSwap`, so the mechanism exists, but a file watcher would re-read config
and keys from disk mid-flight, which is a different and worse problem. No admin endpoint: that means a control surface,
saved state, and auth, on a box with 47 live provider keys and no password.

Spec item 3 says re-sort on any add or remove. A restart does exactly that, and
cannot leave a half-updated list.

## HTTP client settings

One `reqwest::Client` for the whole process. It pools connections per host
internally, and a clone shares the pool, so there is no reason to build one per
provider.

**"Request slot" means one HTTP/2 stream**, which is one request and its
response multiplexed onto a shared connection. It has nothing to do with token
streaming, which is a separate thing: see **Streaming**.

| Setting | Value | Why |
|---|---|---|
| `http2_prior_knowledge` | on where the provider allows it | One connection carries 100-250 request slots. Without it, 62,010 client requests would need 930,150 connections instead of 15,875. |
| `http2_initial_stream_window_size` | 131072 (128KB) | The receive buffer per request slot. This is the dominant memory term. |
| `http2_initial_connection_window_size` | 1048576 (1MB) | Shared across the slots on one connection. |
| `http2_adaptive_window` | off | It grows windows from measured bandwidth, which spends memory to speed up a receive path worth 0.00% of a call. |
| `pool_max_idle_per_host` | 64 | One client request can fire 32 channel calls at one provider. |
| `pool_idle_timeout` | 90s | Most providers close an idle connection near 2 minutes. Go first. |
| `tcp_keepalive` | 30s | Stops a NAT table dropping an idle connection silently. |
| `timeout` | `request_timeout`, 3600s | Per channel call. |
| `connect_timeout` | 5s | Handshake only. A dead host fails fast. |

### Why 128KB, and what changing it costs

A reply is 125KB. The receive window caps how fast it arrives: throughput is
window divided by round-trip time.

| Window | Reply transfer | Share of a 2,067s call | Request slots in 2.5GB | Sustains |
|---|---|---|---|---|
| 128 KB | 0.05s | 0.00% | 20,480 | 0.15 req/s |
| 64 KB | 0.10s | 0.00% | 40,960 | 0.31 req/s |
| 32 KB | 0.20s | 0.01% | 81,920 | 0.62 req/s |
| 16 KB | 0.39s | 0.02% | 163,840 | 1.24 req/s |
| 8 KB | 0.78s | 0.04% | 327,680 | 2.48 req/s |
| 4 KB | 1.56s | 0.08% | 655,360 | 4.95 req/s |

At 128KB and the real load of 3,000 client requests a day, 4,593 request slots
hold 0.56GB and add 0.05s. That is the right default.

Lower it only to trade latency for capacity, and only after `router_rss_bytes`
on `/metrics` shows the limit being hit.

**No dynamic tiering.** Halving the window as load rises was considered and
rejected. `reqwest` cannot change the window on a live connection, so it would
mean one `Client` per tier, picking a tier from current memory, plus hysteresis
to stop it flapping. That is custom machinery for a load this box cannot reach
anyway.

### Head-of-line blocking

32 channel calls to one provider share one TCP connection, each uploading
3.8MB. One slow slot can hold up the others at the transport layer.

If it shows up, cap request slots per connection and let the pool open more
connections. Do not pre-empt it.

## Memory over the life of one client request

A channel call is slow, and the prompt is only needed at the start. Holding it
for the whole call is what breaks the budget.

### How long a channel call takes

| Step | Rate | Time |
|---|---|---|
| Upload 3.8MB, 64 calls sharing the line | 5 Gbit/s | 0.4s |
| Prefill 1,000,000 tokens | 1,000 tok/s | 1,000s |
| Generate 32,000 tokens | 30 tok/s | 1,067s |
| **Total** | | **2,067s = 34.4 min** |

**The prompt is needed for 0.4s of 2,067s. That is 0.019% of the request.**

### Concurrency is not a choice

Concurrency is arrival rate times service time. Little's Law. An earlier draft
used "30 concurrent" as an assumption, which was wrong.

| Arrival rate | Concurrent client requests | RAM, naive |
|---|---|---|
| 3,000/day, actual usage | 72 | 1.9 GB |
| 6,828/day | 163 | 4.0 GB |
| 1/s, stated peak | 2,067 | 47.6 GB |

### Where the 47.6GB goes

| Part | MB per request | At 2,067 concurrent | Free it |
|---|---|---|---|
| Inbound raw body | 3.8 | 7.7 GB | After the parse |
| Parsed structure | 9.5 | 19.2 GB | After the encode |
| Outbound body | 3.8 | 7.7 GB | When the client has its answer |
| Sockets, 64 at 100KB | 6.2 | 12.6 GB | Never, held all 2,067s |

### The four rules

**1. Free the parsed structure after the encode.** It exists only to translate
Anthropic into OpenAI. Once the outbound body is built, keep the 32 byte cache
key and drop the rest. Saves 19.2GB at peak.

**2. Free the inbound raw body after the parse.** Saves 7.7GB at peak.

**3. Hold the outbound body in RAM, spill to a scratch file above a threshold.**
A fallback channel call needs the body again, possibly 20 minutes later, so it
cannot simply be dropped.

```yaml
spill_above_concurrent: 100   # below this, outbound bodies stay in RAM
spill_path: /data/spill
```

At 72 concurrent the bodies are 274MB, less than the `redb` page cache. Nothing
touches disk. Above 100 concurrent, write each new outbound body to a scratch
file and read it back when a fallback fires. Delete the file the moment the
client has its answer. A 3.8MB SSD read is a few milliseconds against a 34
minute request.

**4. Insist on HTTP/2.** It decides whether the peak is reachable at all.

| Connections per client request | MB per request | At 2,067 concurrent |
|---|---|---|
| 64, HTTP/1.1, one per channel call | 6.25 | 12.6 GB |
| ~20, HTTP/2 multiplexed | 1.25 | 2.5 GB |
| ~15, one per provider | 0.94 | 1.9 GB |

HTTP/2 puts the 32 calls to one provider on one connection, so 64 channel calls
become roughly one connection per distinct provider in the race. A provider that
only speaks HTTP/1.1 climbs back towards 64. The memory gate absorbs that
without anyone changing a setting, because it measures rather than predicts.

### Do not store the prompt in the cache

Putting the prompt in the cache entry, so it doubles as the spill, was
considered and rejected.

**Nothing ever reads it back.** On a cache hit the client just sent you the
prompt, and a hit means no upstream call. On a miss there is nothing stored
under that key.

| | Scratch file, deleted when done | Prompt in the cache, kept 7 days |
|---|---|---|
| Bytes per cache entry | 125 KB | 3.9 MB, 31x more |
| Cache growth at 3,000/day | 0.4 GB/day | 11.5 GB/day |
| Cache growth at 1/s | 10.3 GB/day | 330.9 GB/day |
| 100GB holds, at 3,000/day | 279 days | 8.7 days |
| 100GB holds, at 1/s | 9.7 days | 0.3 days |
| Stored prompt ever read | n/a | never |

### Admission: the gate is memory, not a request count

**No limit on requests per second. No limit on concurrent requests. One gate,
and it is RSS.**

Before admitting a client request, read the process RSS from
`/proc/self/statm`. Over `memory_high_water: 3GB`, return `503`. Under it,
admit.

Read it on every admission decision, not on a timer. One open, one read, one
parse: a few microseconds against a request that lasts 2,067 seconds. A cached
sample would let requests slip in between reads, and buy nothing.

**Why not a request count.** The memory a client request holds depends on
figures nobody has measured: how many connections the race opens, what a
connection costs, whether a provider speaks HTTP/1.1. A count that assumes
those is wrong the moment one of them changes.

| Connections per request | KB per socket | Count that fills 3.75GB |
|---|---|---|
| 15 | 64 | 2,944 |
| 20 | 128 | 1,339 |
| 32 | 128 | 879 |
| 64 | 256 | 235 |

235 to 2,944, a spread of 12.6x. Any number written into a config file there is
a guess.

**The memory gate needs no guess.** Whatever a request actually costs, the
router admits fewer when it costs more. If sockets turn out to be 256KB instead
of 64KB, it adapts without anyone editing a file.

**What a `503` means here.** The router is holding as much as it can and is
refusing new work rather than being killed. That is the failure mode omniroute
did not have: it grew until the heap cap shed traffic silently.

Reject, do not queue. A slot opens every 34 minutes, so a queue is a hang with
extra steps. `503` with `Retry-After` lets the client decide.

**At the stated usage nothing is rejected.** 3,000 client requests a day is
0.035 req/s, which is 4.9x inside the 0.17 req/s the memory gate allows.

### Why the high water is 3GB and not 4GB

One thing sits between RSS and the budget: **allocator fragmentation**. The heap
holds more than the program uses. 15% is typical for jemalloc, which is 461MB at
a 3GB high water.

```
high x 1.15 = 4GB   ->   max safe high water = 3.48GB
```

3GB sits 480MB below that. The margin is for the 15% being wrong: at 25%
fragmentation the ceiling drops to 3.2GB and 3GB still holds.

**There is no sampling overshoot term.** An earlier draft read RSS on a 100ms
timer and reserved 80MB for requests admitted between reads. Reading per
admission removes the gap and the reserve.

| Term | Size |
|---|---|
| Allocator fragmentation, 15% of 3GB | 461 MB |
| Sampling overshoot | none |
| Slack available | 1,024 MB |

### What the 503 does not mean

It is not a channel failure. The three codes stay distinct:

| Code | Means | Whose fault |
|---|---|---|
| `413` | The filter emptied the channel list: no model has a large enough context window for this prompt. The body names the largest found. | The caller's. No wait helps. |
| `502` | The router cannot reach the internet. Both network test calls failed. | The local link. |
| `503` | RSS is above `memory_high_water`, so the request was not admitted. Carries `Retry-After` and the current RSS. | The router's. Transient. |
| `504` | No channel could serve it: none eligible, or every channel call failed. | The upstreams'. |

`503` and `504` were one code in an earlier draft. They are opposite problems.
A `503` means the router is full and healthy, so retrying later works. A `504`
means the router is idle and the providers are not answering, so retrying later
probably does not.

The body says which. A `503` from the memory gate carries `Retry-After` and
names the current RSS, so the cause is obvious from the client side.

`/metrics` separates them: `router_requests_total{outcome}` counts the `503`
as `not_admitted` and the `504` as `no_channel`, alongside `hit_cache`,
`success`, `too_large` and `no_network`.

## Capacity: what this box actually serves

Everything below comes from `load_model.py`.

### The chain

```
4GB budget, admit below 3GB, 257MB fixed        ->  2.75GB for traffic
per request slot = 128KB window + 0.5KB conn share  =  128.5KB
2.75GB / 128.5KB                                 =  22,430 request slots
22,430 / 64 channel calls                        =  350 client requests
22,430 / 250 slots per connection                =  90 connections
```

**350 concurrent client requests.** That is the hard number the 4GB budget buys
at a 128KB window.

### Throughput

Throughput is concurrency divided by service time, and service time is set by
how fast the model generates.

| Generation | Service time | Max req/s | Req/day | Client tokens/day | Upstream |
|---|---|---|---|---|---|
| 30 tps | 2,067s | 0.17 | 14,652 | 15 billion | 0.35 Gbit/s |
| 100 tps | 1,320s | 0.27 | 22,940 | 24 billion | 0.54 Gbit/s |
| 500 tps | 1,064s | 0.33 | 28,459 | 29 billion | 0.67 Gbit/s |
| 1,000 tps | 1,032s | 0.34 | 29,342 | 30 billion | 0.70 Gbit/s |

**Stated usage is 3,000 client requests a day. The floor here is 14,652.**
Roughly 5x headroom, on the slowest models.

The table flattens at 0.34 req/s because prefill is a floor: 1M tokens at
1,000 tok/s is 1,000 seconds however fast generation gets.

### Which ceiling binds

| Ceiling | Limit | Binds? |
|---|---|---|
| Memory, 3GB at a 128KB window | 0.17 req/s at 30 tps | **yes** |
| Bandwidth, 5,000 Mbit/s line | 2.44 req/s | no |
| File descriptors, 90 connections | far above | no |
| Provider token quota | far below either | the real limit |

Memory binds first. Lowering the window to 16KB would give 1.4 req/s before
bandwidth took over, at the cost of 0.39s per reply. See **HTTP client
settings**.

### What does not fit

**1 client request/second.** 2,067 concurrent, needing 16.5GB. The router
holds whatever fits under 3GB, around 350, and returns `503` to the rest. No
setting changes this: each answer takes 34 minutes.

**30 client requests/second.** 62,012 concurrent, needing 486.7GB. Not a tuning
problem.

**Provider tokens, at any rate.** 64 channel calls x 1M input tokens is 64
million input tokens per client request. At the stated 3,000 requests a day that
is 192 billion input tokens a day, against the 3 billion actually used. N and M
are what decide this, not the box.

## Resource budget

### Latency, and why not to chase it

`switchyard-translation` uses `serde_json`. So does `llm-bridge-core`. The
parser cannot be swapped from outside: `sonic-rs` is a different crate name, so
a Cargo patch will not substitute it, and the `serde_json` calls sit inside
switchyard's own code.

Work before the first byte reaches a provider, on a 3.8MB prompt:

| Step | `serde_json` | SIMD parser |
|---|---|---|
| Parse the prompt | 12.42 ms | 2.48 ms |
| BLAKE3 the neutral request | 0.75 ms | 0.75 ms |
| Encode the outbound body | 6.21 ms | 2.48 ms |
| **Total** | **19.37 ms** | **5.71 ms** |

The 10ms budget is achievable, but not with switchyard as it ships.

**Take switchyard as it ships anyway.** 13.7ms of difference sits against an
upstream call of 10 to 60 seconds for a 1M token prompt. Forking a pre-alpha
NVIDIA crate to save 0.1% of the answer time is the wrong trade.

**Prefer a maintained library over custom code.** `switchyard-translation`
handles tool-call ids, stop-reason mapping and content-block shapes that a
hand-written translator would get wrong for months. Speed is not worth
correctness here.

If that 20ms ever matters, the fix is known and costed: fork onto `sonic-rs`,
or write the six mappings yourself.

### The restated target

| Prompt | Router overhead |
|---|---|
| Small, a few KB | under 10ms |
| 1M tokens, 3.8MB | about 20ms, nearly all JSON parsing |

The original "under 10ms" was set before the prompt size was known. The fixed
overhead is small. The rest scales with prompt size, and no router avoids it.


Worked in `load_model.py`, which reproduces every number here.

### The workload

| | |
|---|---|
| Prompt | 1,000,000 tokens, every client request |
| Completion | 32,000 tokens |
| Rate | 1 client request/second, sustained |
| N, M | 32 and 2, so 64 channel calls per client request |
| Concurrent client requests | 30 |

### The box

| | |
|---|---|
| Internet | 5,000 Mbit/s |
| LAN | 10GbE |
| Cache pool | 100GB SSD, ZFS |
| RAM budget | 4GB, on a 32GB box shared with other containers |

### Three estimates everything rests on

| Estimate | Value | Why |
|---|---|---|
| Bytes per token | 4 | English text inside JSON |
| Parsed structure vs raw JSON | 2.5x | A typed tree costs more than the bytes it came from |
| Socket buffers per connection | 100KB | Kernel plus TLS |

Nothing else is guessed. Measure these three and the rest follows.

### RAM, per client request

The client sends the prompt. The router receives it, parses it once to translate
Anthropic into OpenAI, and re-encodes one outbound body. **All 64 channel calls
share that one body through a reference-counted handle.**

| Part | RAM |
|---|---|
| Received from the client | 3.8 MB |
| Parsed once, to translate the wire format | 9.5 MB |
| Re-encoded once, shared by all 64 calls | 3.8 MB |
| **Peak per client request** | **17.2 MB** |

### RAM, whole process

| Part | RAM |
|---|---|
| 30 client requests at 17.2MB | 515.0 MB |
| Socket buffers, 1,920 sockets at 100KB | 187.5 MB |
| Replies in flight, including losers that finished | 117.2 MB |
| Channel list, 4,000 rows at 200B | 0.8 MB |
| Health table | 0.2 MB |
| `redb` page cache, capped | 256.0 MB |
| **Total** | **1.05 GB** |

Inside the 4GB budget with room to spare.

Replies in flight is the worst case: all 32 distinct models finish before
cancellation lands, so 30 client requests hold 32 bodies of 125KB each. Normally
two or three finish.

### The mistake that breaks it

Giving each channel call its own copy of the body costs
`30 x 64 x 4MB = 7.15GB`. Eight times the working design, and double the budget.

Nothing warns you. It is what the obvious code does, it works fine with small
prompts, and it falls over at 1M tokens, which is the normal case here.

**The outbound body must be encoded once per client request and shared.** This
is a requirement, not an optimisation.

The 64 bodies are not byte-identical, because each names a different model. So
send the body as two chunks: a per-channel prefix of about 100 bytes holding the
model name and the per-call settings, then the shared 4MB tail. Cloning the
handle to the tail copies 16 bytes.

### Network

| | |
|---|---|
| Upstream per client request | 64 x 4MB = 244 MB |
| Upstream at 1 req/s | 2.05 Gbit/s |
| Share of the 5,000 Mbit/s line | 41% |
| Share of 10GbE LAN | 20% |
| Upstream per day | 20.1 TB |
| Downstream | 3.1 Mbit/s |

Downstream is nothing, because only the winner is read in full. The rest are
cancelled.

### Disk, the cache

| | |
|---|---|
| One entry, reply plus 95 bytes | 125.1 KB |
| Writes at 1 req/s | 10.3 GB/day |
| 7 days | 72 GB |
| `max_bytes: 100GB` holds | 9.7 days |
| Write throughput | 125 KB/s |

### Throughput

| | |
|---|---|
| Inbound client requests | 1/s |
| Outbound channel calls | 64/s |
| Channel calls per day | 5,529,600 |
| Cache lookups | 1/s, one hash and one `redb` read |

### Provider tokens, the one thing that does not fit

| | |
|---|---|
| Input tokens per client request | 64 million |
| Input tokens per day | 5.5 trillion |
| Against the 3 billion/day actually used | 1,843x higher |

RAM, disk, network and throughput all fit. Provider tokens miss by three orders
of magnitude.

This is the constraint that decides N and M, and it has nothing to do with the
box. Racing 64 channel calls spends 64 million input tokens to get one answer
back.

## Wrong turns already taken

Every item below was proposed during the design and rejected. They are written
down because they are the obvious-looking answers, and anyone picking this up
will reach for them again.

**Do not test providers by hand.** The startup probes do it on every boot and
report on `/healthz`. There is no list to curate, no provider to prune, no
spreadsheet of which endpoint works. See **Startup and health**.

**Do not add a cap on channel calls per client request.** Spec item 8 says walk
until success or the list runs out. The exponential backoff is the cap: a
channel that keeps failing reaches 34 minutes after 12 failures and the 30 day
cap after 23, so it leaves the list on its own. A fixed `max_attempts` was proposed and removed.

**Do not add a wall-clock deadline for the client request.** Same reason.
`request_timeout` limits one channel call and nothing limits the client request.

**Do not probe each model at startup.** That is one completion call per tuple,
roughly 4,000 of them, and openrouter alone is 3,336. Groq's free tier allows
about 30 requests a minute, so the probes get rate limited and spend the budget
the first client request needs. Live traffic tests the channels that matter,
for free, because the ranking fires the best ones first.

**Do not classify failures into kinds.** Health is binary and uses the same test
the race uses to pick a winner: 2xx with non-empty content, or unhealthy. An
earlier draft treated `429`, `5xx` and timeouts as "load, not a fault" and
marked nothing. That left an exhausted monthly allowance returning `429` forever
while sitting at the top of the ranking.

**Do not hold a client request waiting for the catalogue to fill.** No channels
means `504`. A `startup_grace` was proposed and removed.

**Do not check the network only after a failure.** The network test runs beside
every client request's channel calls, so the answer arrives with the failures
rather than after them. A lazy check with a cached answer was proposed and
removed.

**Do not let health state die with the process.** Losing the table puts
thousands of known-dead channels back in the list at `retry_counter` 0. That is
the exact behaviour this design exists to remove. Flush to `redb` every 5
minutes.

**Do not give the client's API key to a provider.** Read it, ignore it, drop it.
The upstream key comes from the channel.

**Do not translate wire formats per channel.** Once at each edge, through a
neutral type. See **Format translation**.

**Do not count M hedged copies as M failures.** They are one trial. Counting
them would make the backoff depend on a latency dial.

**Do not add a per-provider concurrency limit.** A semaphore with
`max_inflight_per_key` was proposed and rejected. The number is a guess, it is
wrong across 47 keys with different limits, and a `429` costs no latency when
the other N-1 channel calls in the wave are still running. See **Concurrency
against one provider**.

**Do not filter the channel list into a snapshot at the start of a client
request.** Query the list each time a channel is needed, excluding the ids
already tried. A snapshot cannot see a channel recover or die mid-walk, and it
does 4,000 rows of work for a client request that usually needs five channels.
See **Why query each time, not filter once**.

**Do not name a channel by its list index.** Use the id, a hash of
provider+key+model. An index means nothing after a rebuild.

**Do not use a composite cache key `(prompt, model)`.** It is unbounded: 848
distinct model ids means one prompt could hold 848 entries at 104MB. A flat key
with the intelligence comparison on write gives the right answer under `auto`,
which is how the router is used. See **One entry per prompt, by design**.

**Do not hash the raw request body for the cache key.** Hash the neutral request
after translation, or an Anthropic client and an OpenAI client asking the same
question get two different keys and two caches that never meet.

**Do not use SHA-256 for the cache key.** Without hardware support it costs
9.3ms on a 3.8MB prompt, which is the whole latency budget. BLAKE3 gives the
same 32 bytes about ten times faster.

**Do not throw away a channel call that finishes during cancellation.** It is
already paid for. Offer it to the cache: if its model is smarter than the stored
entry, it wins.

**Do not wait for slower channel calls to improve the cache.** The winner is
the first to finish, a smarter model is usually minutes behind, and holding 31
calls open costs 3 billion output tokens a day for a cache that rarely hits.

**Do not write custom code where a maintained library exists.** A library is
faster to ship and less buggy than a hand-written version. This decided the
wire-format translation: `switchyard-translation` is slower than a hand-written
SIMD translator by 13.7ms on a 3.8MB prompt, and it still wins, because it
already handles tool-call ids, stop-reason mapping and content-block shapes.

**Do not fork `switchyard-translation` to swap `serde_json` for `sonic-rs`.**
It saves 13.7ms against an upstream call of 10 to 60 seconds, and costs a
maintained fork of a pre-alpha crate.

**Do not set a short `request_timeout`.** A channel call is prefill plus
generation: 1M tokens at 1,000 tok/s plus 32k at 30 tok/s is 2,067 seconds.
Streaming fixes the client's timeout, not the provider's generation time. A 60s timeout needs 32,000 tok/s to finish, and every timeout
marks an honest slow model unhealthy. 3,600s.

**Do not assume a fixed number of concurrent client requests.** It is arrival
rate times service time, and nothing else. At 2,067s per request, 1 req/s means
2,067 in flight.

**Do not hold the prompt for the life of the channel call.** It is needed for
0.4s of 2,067s. Free the parse, free the inbound body, spill the outbound body
above a threshold.

**Do not store the prompt in the cache.** Nothing ever reads it back, and it is
31x the bytes of a response entry.

**Do not size the HTTP/2 receive window dynamically.** `reqwest` cannot change
it on a live connection, so tiering means one `Client` per window size, a
selector driven by memory, and hysteresis. 128KB from config, and the user
lowers it if they ever need to.

**Do not turn on `http2_adaptive_window`.** It grows windows from measured
bandwidth. Receiving a 125KB reply is 0.00% of a 2,067s call, so it spends
memory on the one thing that does not matter.

**Do not count replies as long-lived memory.** A reply is held from arrival
until it is sent to the client and written to the cache: milliseconds. At 30
client requests a second that is under 1MB, not the 22GB an earlier draft
claimed.

**Do not assume one connection pool per client request.** Connections are
shared across every client request, and HTTP/2 carries 100-250 request slots on
one connection. An earlier draft counted 930,150 connections where 15,875 was
right.

**Do not use Alpine or musl.** musl's `malloc` is slow under multithreaded
allocation-heavy load, which is exactly what parsing a 3.8MB JSON document per
client request is. `debian:bookworm-slim` with glibc, plus jemalloc as the
global allocator.

**Do not ship a `scratch` image to save 110MB.** A shell, `curl`, `ss` and `ps`
inside the container answer in seconds what log reading answers in minutes. The
size difference is noise next to a 100GB cache pool.

**Do not omit `mem_limit` from the compose file.** Without it the 4GB budget is
a comment, and the process grows into memory Frigate and Plex need.

**Do not forward the first token you see when `hedge_strategy` is `tps`.**
Sending a token commits the router to that channel. Buffer for
`hedge_window_ms`, pick the fastest stream, then flush.

**Do not expect the client to raise its timeout.** Claude Code's
`API_TIMEOUT_MS` defaults to 10 minutes and prefill on a 1M token prompt takes
16.7 minutes at 1,000 tok/s. The router sends a keepalive every 15 seconds until
the first real byte: an SSE comment for a streaming client, a leading space for
a non-streaming one. A router that only works after the client is reconfigured
is a broken router.

**Do not reject a client whose streaming mode differs from the router's.**
`streaming` in `router.yml` governs the providers. `stream` in the request
governs the client. Buffer the stream for a client that wants one piece: it
costs 42.7MB at 350 concurrent, and that response is already held for the cache
write.

**Do not splice a second channel onto a broken stream.** Once the client has
part of an answer, the router is committed. A stream that dies after that ends
the client request. Joining another model's output mid-sentence produces a reply
nobody can trust and no client can detect.

**Do not cache a cancelled stream.** It is a partial answer, and a partial
answer is wrong.

**Do not keep channel calls running after the client hangs up.** 64 calls for
another 24 minutes, for an answer nobody reads.

**Do not filter `GET /v1/models` by health.** It lists every model the router
knows a channel for, with its live status. Hiding the broken ones deletes the
view of what is broken.

**Do not restrict `GET /v1/models` to OpenAI's documented fields.** OpenRouter
returns 458 models carrying 16 non-standard fields and drops two standard ones,
and every coding tool copes. Put the intelligence score, context window,
capability flags and live channel health in.

**Do not return one status code for "no capacity" and "no channel".** They are
opposite problems: `503` means the router is full and healthy, `504` means it is
idle and the upstreams are failing. They call for opposite reactions. See
**Status codes**.

**Do not read RSS on a timer.** Read it on every admission decision. One open,
one read, one parse is microseconds against a 2,067s request, and a cached
sample lets requests slip in between reads for no gain.

**Do not limit on a request count, or on requests per second.** The gate is
RSS against `memory_high_water`. A count depends on per-connection costs that
vary by 12.6x across plausible values, so any number is a guess. See
**Admission: the gate is memory, not a request count**.

**Do not use `coding_index` for ranking.** `intelligence`, even though the
client is a coding tool. Settled.

**Do not break ranking ties on latency.** Shuffle at random within a rank group.
Measuring latency to choose a channel means calling the channels first, which is
the thing being chosen.

## What this deliberately does not do

- No load balancing or weighted routing. Order is intelligence, descending.
- No retry with backoff on the racing path. Failure moves to the next channel.
- No admin UI. Config is a YAML file; the container reloads.
- No per-user quotas, billing, or multi-tenancy.
- No semantic cache. Exact match only. Fuzzy matching needs embeddings, which
  needs a model call, which defeats the point.

## Open problems

**Tool calling is now mandatory.** Coding tools do not work without it.
Translation between Anthropic and OpenAI tool schemas is mechanical but has
sharp edges, and tool results have to survive the round trip intact. This is
real work, not a footnote.

**The cache will rarely hit for coding tools.** Each turn in a Claude Code
session carries the whole growing conversation, so two requests are almost
never byte-identical. Requirement 12 is sound, but against this traffic the hit
rate will be near zero. It earns its place for repeated scripted prompts, not
for interactive coding.

**Anthropic format is a first-class path, not a shim.** Claude Code speaks
`/v1/messages`. Every provider in `providers.yml` speaks OpenAI. So the
Anthropic-to-OpenAI translation sits on the hot path for your main client, in
both directions, and now in both buffered and streamed form. The streamed path
is the harder half: `decode_stream` and `encode_stream` must keep tool-call ids
intact across SSE chunk boundaries.

**Z.ai coding plan economics.** It is a flat subscription, not metered tokens.
Racing or hedging against it multiplies usage against whatever allowance the
plan carries, which risks a limit or the plan's terms rather than a bill.
Needs a decision before racing is enabled on that provider.

**Key rotation across a pool.** Gemini has 9 keys, openrouter 8, nvidia 7,
groq and cerebras 6 each. Each carries its own free-tier limit. Decided: no
quota accounting, just fall through to the next tuple on failure, per spec item
8. The cost is a wasted call on every exhausted key, every request. If that
proves noisy, parking a key for an hour after a 429 is the smallest fix and
does not need the counting machinery omniroute carries.

**OAuth providers.** Roughly 60 connections authenticate by OAuth and are not
exported. Supporting them means a refresh flow per provider, which is a
sizeable piece of work and the main thing that would make this project big
rather than small.

**Silent wrong name matches.** Automatic matching can pair a model with the
wrong ranking row and nothing will complain. Startup logging of loose matches
is the mitigation, not a fix.

**Images.** Not needed by coding tools, and format differences between
providers are wider here than anywhere else. Out of scope for v1.
