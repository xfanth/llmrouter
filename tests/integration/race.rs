//! The race, end to end: a real [`Racer`] over wiremock upstreams.
//!
//! Everything here drives `Racer::run` itself rather than a helper beneath
//! it. The upstreams are wiremock servers on loopback and the network test
//! points at a wiremock server too, so no test in this file touches the
//! internet and none of them sleeps on its own account: where a test needs an
//! upstream to be slow, the upstream is slow, and the assertion is that the
//! race came back without waiting for it.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use llmrouter::catalogue::ChannelTelemetry;
use llmrouter::config::{Config, HedgeStrategy, RouterConfig, WireName};
use llmrouter::server::PromptBody;
use llmrouter::{
    Catalogue, Channel, ChannelFilter, ChannelHealth, ChannelId, HealthKey, HealthTable,
    NeutralResponse, RaceInput, RaceOutcome, Racer,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// An upstream that answers slowly enough that waiting for it would be
/// obvious. No test may spend this long: a test that does has found a race
/// that failed to cancel its losers.
const TOO_SLOW: Duration = Duration::from_secs(10);

/// The ceiling every "the losers were cancelled" assertion is made against.
/// Far below [`TOO_SLOW`] and far above anything loopback needs.
const PROMPT: Duration = Duration::from_secs(3);

/// One channel pointed at a wiremock server. `rank` fixes the order the
/// catalogue scans in, so no test depends on the tie-break shuffle.
fn channel(provider: &str, base_url: &str, rank: u32) -> Channel {
    let model = format!("{provider}-model");
    Channel {
        id: ChannelId::new(provider, "key-fake", &model),
        provider: provider.into(),
        model_id: model.as_str().into(),
        key_id: "key-fake".into(),
        // An obvious fake. Never put real key material in a fixture.
        key: "sk-test-not-a-real-key".into(),
        base_url: base_url.into(),
        wire: WireName::OpenAi,
        limit_context: 128_000,
        limit_output: 8_192,
        tool_call: true,
        reasoning: false,
        structured_output: false,
        modalities_input: vec!["text".to_string()],
        intelligence: 100,
        intelligence_rank: rank,
        created: None,
        health: ChannelHealth::live(),
        telemetry: ChannelTelemetry::default(),
    }
}

/// What the network test should find. The race asks it on every walk, and the
/// answer decides whether a drained walk is the providers' fault or the local
/// link's.
enum Link {
    /// A local server stands in for the two operator endpoints.
    Up,
    /// Both calls go to a closed port, so both fail to connect.
    Down,
}

/// A racer over a throwaway redb file, a catalogue of the given channels and
/// a network test that answers the way the test asked for.
struct Harness {
    racer: Racer,
    health: Arc<HealthTable>,
    catalogue: Arc<Catalogue>,
    /// Held so the server outlives the race that calls it.
    _link: Option<MockServer>,
    _dir: TempDir,
}

impl Harness {
    async fn new(router: RouterConfig, channels: Vec<Channel>, link: Link) -> Self {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let config = Arc::new(Config {
            router,
            providers: Vec::new(),
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: HashMap::new(),
            capabilities: HashMap::new(),
        });

        let db = Arc::new(
            redb::Database::create(dir.path().join("state.redb")).expect("the store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                config.router.health_poll.retry_base,
                config.router.health_poll.backoff_max,
            )
            .expect("the health table opens"),
        );

        let catalogue = Arc::new(Catalogue::new());
        catalogue.replace(channels);

        let upstream = llmrouter::Upstream::new(&config.router).expect("the client builds");

        let (link_server, urls) = match link {
            Link::Up => {
                let server = MockServer::start().await;
                Mock::given(method("GET"))
                    .respond_with(ResponseTemplate::new(204))
                    .mount(&server)
                    .await;
                let urls = vec![
                    format!("{}/generate_204", server.uri()),
                    format!("{}/cdn-cgi/trace", server.uri()),
                ];
                (Some(server), urls)
            }
            // Port 1 is never listening, so both calls fail to connect at
            // once and no timer is involved.
            Link::Down => (
                None,
                vec![
                    "http://127.0.0.1:1/generate_204".to_string(),
                    "http://127.0.0.1:1/cdn-cgi/trace".to_string(),
                ],
            ),
        };
        let net = llmrouter::NetTest::with_urls(
            upstream.client().clone(),
            config.router.health_poll.network_test_timeout,
            urls,
        );

        let racer = Racer::new(
            Arc::clone(&config),
            Arc::clone(&catalogue),
            Arc::clone(&health),
            upstream,
            net,
            crate::test_metrics(),
        );

        Self {
            racer,
            health,
            catalogue,
            _link: link_server,
            _dir: dir,
        }
    }

    /// Runs one walk with a client that never hangs up, and reports how long
    /// the walk took.
    async fn run(&self, input: RaceInput) -> (RaceOutcome, Duration) {
        let started = Instant::now();
        let outcome = self.racer.run(input, CancellationToken::new()).await;
        (outcome, started.elapsed())
    }

    /// The live catalogue, so a test can read the backoff the walk wrote onto
    /// a row.
    fn rows(&self) -> Arc<Vec<Channel>> {
        self.catalogue.snapshot()
    }
}

/// The live row for one channel id.
fn row_for(rows: &[Channel], id: ChannelId) -> &Channel {
    rows.iter()
        .find(|c| c.id == id)
        .expect("the channel is still in the catalogue")
}

/// A router config with the race dials set and everything else default.
fn router_config(width: usize, copies: usize, dir: &std::path::Path) -> RouterConfig {
    RouterConfig {
        race_width: width,
        max_per_channel: copies,
        hedge_delay_ms: 0,
        spill_path: dir.to_path_buf(),
        ..RouterConfig::default()
    }
}

/// The input every walk here starts from: no model named, a prompt small
/// enough for every row, and the provider call buffered or streamed.
fn input(streaming: bool) -> RaceInput {
    RaceInput {
        // No `model` field: the upstream module adds each channel's own.
        outbound: Arc::new(PromptBody::Memory(Bytes::from_static(
            br#"{"messages":[{"role":"user","content":"ping"}],"max_tokens":16}"#,
        ))),
        filter: ChannelFilter {
            min_context: 1,
            needs_tools: false,
            named_model: None,
        },
        streaming,
        named_model: None,
    }
}

/// A whole OpenAI chat reply.
fn reply(text: &str) -> Value {
    json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion",
        "model": "fake",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text },
            "finish_reason": "stop",
        }],
    })
}

/// One SSE content delta, framed.
fn delta_frame(text: &str) -> String {
    let frame = json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "model": "fake",
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": Value::Null }],
    });
    format!("data: {frame}\n\n")
}

/// `count` content deltas followed by a terminal frame, as one SSE body.
fn sse(text: &str, count: usize) -> String {
    let mut body = String::new();
    for _ in 0..count {
        body.push_str(&delta_frame(text));
    }
    let done = json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "model": "fake",
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }],
    });
    body.push_str(&format!("data: {done}\n\ndata: [DONE]\n\n"));
    body
}

/// A streamed SSE reply template.
fn stream_body(text: &str, frames: usize) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(sse(text, frames))
}

/// A provider that opens a stream, sends one token, and then drops the socket
/// with the body unfinished. wiremock cannot cut a connection mid-answer, so
/// this is a raw listener: it is the smallest thing that gives the client a
/// stream error rather than a clean end.
///
/// Returns the base URL and the task serving it, which the caller aborts.
async fn stream_that_dies_mid_answer() -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is free");
    let addr = listener.local_addr().expect("the port is known");
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                // The request is read and thrown away: every path gets the
                // same answer.
                let mut scratch = [0u8; 4096];
                let _ = socket.read(&mut scratch).await;
                let frame = delta_frame("half");
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{frame}\r\n",
                    frame.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.flush().await;
                // No terminating chunk and no shutdown: the socket is dropped
                // here, so the client sees the body break off.
            });
        }
    });
    (format!("http://{addr}"), task)
}

/// A provider that answers `POST /chat/completions` with one template.
async fn upstream(template: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(template)
        .mount(&server)
        .await;
    server
}

/// The assistant text of a reply, read back through the same encoder the
/// server uses at the client edge.
fn text_of(response: &NeutralResponse) -> String {
    let encoded =
        llmrouter::wire::encode_response(response, llmrouter::wire::Format::OpenAiChat, None)
            .expect("the winning reply re-encodes");
    encoded["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// How many requests a mock server actually received.
async fn calls(server: &MockServer) -> usize {
    server
        .received_requests()
        .await
        .expect("the mock records its requests")
        .len()
}

/// A readable name for an outcome, so a failing match says what it got.
fn name_of(outcome: &RaceOutcome) -> &'static str {
    match outcome {
        RaceOutcome::Buffered { .. } => "a buffered win",
        RaceOutcome::Streamed { .. } => "a streamed win",
        RaceOutcome::NoNetwork { .. } => "a dead local link",
        RaceOutcome::Exhausted { .. } => "an exhausted walk",
        RaceOutcome::ClientGone => "the client gone",
    }
}

/// Two channels go out together and the one that answers first is the one the
/// client gets. The other is still mid-call, so the race has to tear it down
/// rather than wait for it: if it waited, this test would take ten seconds.
#[tokio::test]
async fn the_fastest_of_two_channels_wins_and_the_loser_is_cancelled() {
    let fast = upstream(ResponseTemplate::new(200).set_body_json(reply("fast"))).await;
    let slow = upstream(
        ResponseTemplate::new(200)
            .set_body_json(reply("slow"))
            .set_delay(TOO_SLOW),
    )
    .await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let fast_channel = channel("fast", &fast.uri(), 0);
    let slow_channel = channel("slow", &slow.uri(), 1);
    let fast_id = fast_channel.id;
    let slow_id = slow_channel.id;

    let harness = Harness::new(
        router_config(2, 1, dir.path()),
        vec![fast_channel, slow_channel],
        Link::Up,
    )
    .await;

    let (outcome, elapsed) = harness.run(input(false)).await;

    match outcome {
        RaceOutcome::Buffered {
            winner,
            also_finished,
            attempts,
        } => {
            assert_eq!(winner.channel_id, fast_id, "the first answer home wins");
            assert_eq!(text_of(&winner.response), "fast");
            assert!(
                also_finished.is_empty(),
                "the slow channel had not finished, so nothing else is paid for"
            );
            assert_eq!(attempts, 2, "both channels were called once each");
        }
        other => panic!("expected a buffered win, got {}", name_of(&other)),
    }

    assert!(
        elapsed < PROMPT,
        "the losing call must be cancelled, not waited on: the walk took {elapsed:?}"
    );
    assert_eq!(calls(&slow).await, 1, "the loser really was called");

    let rows = harness.rows();
    let loser = row_for(&rows, slow_id);
    assert_eq!(
        loser.health.in_flight.load(Ordering::Relaxed),
        0,
        "the cancelled call released its in-flight slot"
    );
    assert_eq!(
        loser.health.retry_counter.load(Ordering::Relaxed),
        0,
        "cancelled is not unhealthy"
    );
}

/// A sliding window, not waves. The first channel fails straight away and the
/// third has to start then and there, beside the second, rather than waiting
/// for the second to finish. The second takes ten seconds, so a wave-shaped
/// walk would take ten seconds; this one answers off the third.
#[tokio::test]
async fn a_failed_channel_is_replaced_at_once_rather_than_at_the_end_of_a_wave() {
    let broken = upstream(ResponseTemplate::new(500).set_body_string("upstream on fire")).await;
    let crawling = upstream(
        ResponseTemplate::new(200)
            .set_body_json(reply("crawling"))
            .set_delay(TOO_SLOW),
    )
    .await;
    let third = upstream(ResponseTemplate::new(200).set_body_json(reply("third"))).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let third_channel = channel("third", &third.uri(), 2);
    let third_id = third_channel.id;

    let harness = Harness::new(
        // Two in flight, so the third row only enters when one of the first
        // two leaves.
        router_config(2, 1, dir.path()),
        vec![
            channel("broken", &broken.uri(), 0),
            channel("crawling", &crawling.uri(), 1),
            third_channel,
        ],
        Link::Up,
    )
    .await;

    let (outcome, elapsed) = harness.run(input(false)).await;

    match outcome {
        RaceOutcome::Buffered {
            winner, attempts, ..
        } => {
            assert_eq!(winner.channel_id, third_id);
            assert_eq!(text_of(&winner.response), "third");
            assert_eq!(attempts, 3, "all three channels were called");
        }
        other => panic!("expected the third channel to win, got {}", name_of(&other)),
    }

    assert!(
        elapsed < PROMPT,
        "the replacement must start beside the slow channel, not after it: \
         the walk took {elapsed:?}"
    );
    assert_eq!(calls(&third).await, 1);
    assert_eq!(
        calls(&crawling).await,
        1,
        "the slow channel stayed in flight throughout"
    );
}

/// M hedged copies of one channel are one trial. Every copy of the broken
/// channel fails, and the channel is owed exactly one failure for it: three
/// would push it into a backoff four times longer than it earned.
#[tokio::test]
async fn three_hedged_copies_failing_count_as_one_failure_against_the_channel() {
    let broken = upstream(ResponseTemplate::new(503).set_body_string("no capacity")).await;
    // Slow enough that the broken channel's three copies have all failed
    // before this one answers, so the failure is settled rather than
    // cancelled.
    let working = upstream(
        ResponseTemplate::new(200)
            .set_body_json(reply("working"))
            .set_delay(Duration::from_millis(300)),
    )
    .await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let broken_channel = channel("broken", &broken.uri(), 0);
    let broken_id = broken_channel.id;

    let harness = Harness::new(
        router_config(2, 3, dir.path()),
        vec![broken_channel, channel("working", &working.uri(), 1)],
        Link::Up,
    )
    .await;

    let (outcome, _) = harness.run(input(false)).await;

    match outcome {
        RaceOutcome::Buffered {
            winner, attempts, ..
        } => {
            assert_eq!(text_of(&winner.response), "working");
            assert_eq!(attempts, 6, "two channels, three copies each");
        }
        other => panic!(
            "expected the working channel to win, got {}",
            name_of(&other)
        ),
    }

    assert_eq!(
        calls(&broken).await,
        3,
        "all three hedged copies really were sent"
    );

    let entry = harness
        .health
        .get(&HealthKey::Channel(broken_id))
        .expect("the failed channel has a health row");
    assert_eq!(
        entry.retry_counter, 1,
        "three failed copies are one failed trial, not three"
    );

    let rows = harness.rows();
    assert_eq!(
        row_for(&rows, broken_id)
            .health
            .retry_counter
            .load(Ordering::Relaxed),
        1,
        "the channel row mirrors the one failure"
    );
}

/// `tps` buffers every live stream and then picks on tokens per second, not on
/// who got there first. Both streams open together, so time to first token
/// cannot separate them: the one that produced more tokens wins.
#[tokio::test]
async fn the_tps_window_picks_the_stream_that_produced_the_most_tokens() {
    let wordy = upstream(stream_body("f", 12)).await;
    let terse = upstream(stream_body("s", 2)).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    // The terse one is scanned first, so winning on tokens means winning
    // against the catalogue order rather than with it.
    let wordy_channel = channel("wordy", &wordy.uri(), 1);
    let wordy_id = wordy_channel.id;

    let harness = Harness::new(
        RouterConfig {
            hedge_strategy: HedgeStrategy::Tps,
            ..router_config(2, 1, dir.path())
        },
        vec![wordy_channel, channel("terse", &terse.uri(), 0)],
        Link::Up,
    )
    .await;

    let (outcome, _) = harness.run(input(true)).await;

    let winner = match outcome {
        RaceOutcome::Streamed { winner, attempts } => {
            assert_eq!(attempts, 2, "both streams were opened and judged");
            winner
        }
        other => panic!("expected a streamed win, got {}", name_of(&other)),
    };
    assert_eq!(
        winner.channel_id, wordy_id,
        "the faster stream wins the window"
    );

    let aggregated = llmrouter::wire::aggregate_stream(winner.chunks)
        .await
        .expect("the winning stream folds into one answer");
    assert_eq!(
        text_of(&aggregated),
        "f".repeat(12),
        "the client gets the winner's buffered opening and the rest of its stream"
    );
}

/// `ttft` commits to the first channel to emit a token and never reopens the
/// question. The second channel would out-token it comfortably, and under
/// `tps` would join the window and win it; under `ttft` it is cancelled
/// unheard.
#[tokio::test]
async fn ttft_commits_to_the_first_token_even_when_a_wordier_stream_follows() {
    let first = upstream(stream_body("a", 2)).await;
    let wordier = upstream(stream_body("b", 40).set_delay(Duration::from_millis(400))).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let first_channel = channel("first", &first.uri(), 0);
    let first_id = first_channel.id;

    let harness = Harness::new(
        RouterConfig {
            hedge_strategy: HedgeStrategy::Ttft,
            ..router_config(2, 1, dir.path())
        },
        vec![first_channel, channel("wordier", &wordier.uri(), 1)],
        Link::Up,
    )
    .await;

    let (outcome, _) = harness.run(input(true)).await;

    let winner = match outcome {
        RaceOutcome::Streamed { winner, .. } => winner,
        other => panic!("expected a streamed win, got {}", name_of(&other)),
    };
    assert_eq!(
        winner.channel_id, first_id,
        "the first token takes it, whatever arrives afterwards"
    );

    let aggregated = llmrouter::wire::aggregate_stream(winner.chunks)
        .await
        .expect("the winning stream folds into one answer");
    assert_eq!(text_of(&aggregated), "aa");
}

/// The query comes back empty before a single call is made, which is the
/// `504` with nothing attempted.
#[tokio::test]
async fn a_filter_no_channel_satisfies_ends_the_walk_before_any_call() {
    let never = upstream(ResponseTemplate::new(200).set_body_json(reply("unreachable"))).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let harness = Harness::new(
        router_config(2, 1, dir.path()),
        vec![channel("never", &never.uri(), 0)],
        Link::Up,
    )
    .await;

    let mut input = input(false);
    // Larger than the row's 128k context, so nothing matches.
    input.filter.min_context = 1_000_000;

    let (outcome, _) = harness.run(input).await;

    match outcome {
        RaceOutcome::Exhausted {
            attempts,
            last_error,
        } => {
            assert_eq!(attempts, 0, "no channel was called");
            assert_eq!(last_error, None, "there is no upstream error to report");
        }
        other => panic!("expected an exhausted walk, got {}", name_of(&other)),
    }
    assert_eq!(calls(&never).await, 0);
}

/// Every channel fails and the link is up, so the fault is the providers' and
/// the walk is exhausted: the `504`, carrying the last upstream message rather
/// than a bare status.
#[tokio::test]
async fn running_out_of_channels_with_the_link_up_is_an_exhausted_walk() {
    let body = json!({ "error": { "message": "insufficient quota" } });
    let first = upstream(ResponseTemplate::new(429).set_body_json(body.clone())).await;
    let second = upstream(ResponseTemplate::new(500).set_body_json(body)).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let first_channel = channel("first", &first.uri(), 0);
    let first_id = first_channel.id;

    let harness = Harness::new(
        router_config(2, 1, dir.path()),
        vec![first_channel, channel("second", &second.uri(), 1)],
        Link::Up,
    )
    .await;

    let (outcome, _) = harness.run(input(false)).await;

    match outcome {
        RaceOutcome::Exhausted {
            attempts,
            last_error,
        } => {
            assert_eq!(attempts, 2, "both channels were called before giving up");
            let reported = last_error.expect("the last upstream error is carried back");
            assert!(
                reported.contains("insufficient quota"),
                "the provider's own message reaches the 504 body, got {reported:?}"
            );
        }
        other => panic!("expected an exhausted walk, got {}", name_of(&other)),
    }

    assert_eq!(
        harness
            .health
            .get(&HealthKey::Channel(first_id))
            .expect("the failed channel backed off")
            .retry_counter,
        1,
        "a live link means the failures are the channels' own"
    );
}

/// Both network test calls fail, so the fault is local. That is a `502`, and
/// the whole point of it is that no provider is blamed: nothing may be written
/// against any channel's backoff.
#[tokio::test]
async fn a_dead_local_link_gives_no_network_and_moves_no_channel_health() {
    let broken = upstream(ResponseTemplate::new(500).set_body_string("unreachable")).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let broken_channel = channel("broken", &broken.uri(), 0);
    let broken_id = broken_channel.id;

    let harness = Harness::new(
        router_config(2, 1, dir.path()),
        vec![broken_channel],
        Link::Down,
    )
    .await;

    let (outcome, elapsed) = harness.run(input(false)).await;

    match outcome {
        RaceOutcome::NoNetwork { failed } => {
            assert_eq!(failed.len(), 2, "both test calls are reported");
            assert!(
                failed.iter().all(|line| line.contains("127.0.0.1:1")),
                "each line names the URL it tried, got {failed:?}"
            );
        }
        other => panic!("expected a dead link, got {}", name_of(&other)),
    }

    assert!(
        elapsed < PROMPT,
        "a refused connection decides at once, the walk took {elapsed:?}"
    );
    assert!(
        harness.health.get(&HealthKey::Channel(broken_id)).is_none(),
        "a local fault is nobody's channel's fault"
    );
    let rows = harness.rows();
    assert_eq!(
        row_for(&rows, broken_id)
            .health
            .retry_counter
            .load(Ordering::Relaxed),
        0
    );
}

/// The client hangs up mid-walk. Everything in flight is torn down and the
/// walk says so rather than carrying on paying for calls nobody will read.
#[tokio::test]
async fn a_client_that_hangs_up_cancels_the_walk() {
    let slow = upstream(
        ResponseTemplate::new(200)
            .set_body_json(reply("nobody reads this"))
            .set_delay(TOO_SLOW),
    )
    .await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let slow_channel = channel("slow", &slow.uri(), 0);
    let slow_id = slow_channel.id;

    let harness = Harness::new(
        router_config(2, 1, dir.path()),
        vec![slow_channel],
        Link::Up,
    )
    .await;

    let cancel = CancellationToken::new();
    let hang_up = cancel.clone();
    tokio::spawn(async move {
        // The client's own disconnect, which the race cannot predict. This is
        // the one wait in the file and it is the event under test, not a way
        // of letting something else settle.
        tokio::time::sleep(Duration::from_millis(50)).await;
        hang_up.cancel();
    });

    let started = Instant::now();
    let outcome = harness.racer.run(input(false), cancel).await;
    let elapsed = started.elapsed();

    assert!(
        matches!(outcome, RaceOutcome::ClientGone),
        "expected the walk to report the client gone, got {}",
        name_of(&outcome)
    );
    assert!(
        elapsed < PROMPT,
        "the walk must stop with the client, the walk took {elapsed:?}"
    );
    assert!(
        harness.health.get(&HealthKey::Channel(slow_id)).is_none(),
        "a client hanging up is not the channel's failure"
    );
}

/// A stream that breaks inside the `tps` window is an ordinary failed trial:
/// nothing reached the client, so the walk replaces the dead channel and
/// carries on. Width is 1, so the broken channel is the whole in-flight set
/// when it dies, and the only way the client sees an answer is the walk
/// pulling in the untried channel behind it.
#[tokio::test]
async fn every_stream_breaking_in_the_window_replaces_the_channels_rather_than_ending_the_walk() {
    let (broken_url, broken_task) = stream_that_dies_mid_answer().await;
    let sound = upstream(stream_body("ok", 4)).await;

    let dir = tempfile::tempdir().expect("a scratch directory");
    let sound_channel = channel("sound", &sound.uri(), 1);
    let sound_id = sound_channel.id;

    let harness = Harness::new(
        RouterConfig {
            hedge_strategy: HedgeStrategy::Tps,
            ..router_config(1, 1, dir.path())
        },
        // The broken one is scanned first, so it opens the window alone.
        vec![channel("broken", &broken_url, 0), sound_channel],
        Link::Up,
    )
    .await;

    let (outcome, elapsed) = harness.run(input(true)).await;

    let winner = match outcome {
        RaceOutcome::Streamed { winner, attempts } => {
            assert_eq!(attempts, 2, "the broken channel and its replacement");
            winner
        }
        other => panic!("expected a streamed win, got {}", name_of(&other)),
    };
    assert_eq!(
        winner.channel_id, sound_id,
        "the untried channel behind the broken one serves the request"
    );
    assert_eq!(
        calls(&sound).await,
        1,
        "the replacement channel was actually called"
    );
    assert!(elapsed < PROMPT, "the walk did not sit out a second window");

    let aggregated = llmrouter::wire::aggregate_stream(winner.chunks)
        .await
        .expect("the winning stream folds into one answer");
    assert_eq!(text_of(&aggregated), "ok".repeat(4));

    broken_task.abort();
}
