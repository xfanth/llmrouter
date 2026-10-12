# Tests

Three tiers. Each is its own cargo test target (`tests/<tier>/main.rs`), so
`cargo test --test contract` runs one tier on its own.

## tests/contract

The wire formats, and nothing else. No sockets, no upstreams, no storage.
These pin the bytes we accept from a client and the bytes we hand back: an
OpenAI chat request and reply, an Anthropic messages request and reply, SSE
framing in both directions, and tool calls round-tripping with their ids
intact. Snapshots are held by `insta`, so a change to the wire output shows up
as a snapshot diff rather than a hand-written assertion. Table-driven cases use
`rstest`.

## tests/integration

The real router against fake upstreams. `wiremock` stands in for the provider
HTTP APIs, `tempfile` gives each test a throwaway directory for the redb cache
and the spill files, and `reqwest` drives the router over a real socket where a
test needs the socket to be real. This tier covers the race, fallback when a
channel fails, the cache, the health table and backoff, and the status codes we
return. Time-dependent cases use tokio's `start_paused` clock rather than real
sleeps.

## tests/smoke

The built image. `docker compose up`, then `curl` against `/healthz`,
`/metrics` and `/v1/models`, then `docker compose down`. Docker is not
available on every development machine, so this tier checks for a working
daemon first and skips with a message when there is none. CI runs it for real
on an ubuntu runner.

## Fixtures

Never put real key material in a fixture. Use an obvious fake such as
`sk-test-not-a-real-key`.
