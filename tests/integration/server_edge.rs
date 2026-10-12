//! The server edge: the axum router driven end to end against wiremock
//! upstreams.
//!
//! Everything here goes through `llmrouter::server::router`, so the handlers,
//! the keepalive, the race invocation and the winner-to-response conversion all
//! run for real. Nothing talks to the internet: the network test is pointed at
//! a loopback server through `NetTest::with_urls`, so each test says whether
//! the link is up or down, which is what decides between a `502` and a `504`.
//!
//! Keys in here are obvious fakes.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use llmrouter::catalogue::{Channel, ChannelHealth, ChannelId, ChannelTelemetry};
use llmrouter::server::AppState;
use llmrouter::{
    Admission, Cache, CacheConfig, Catalogue, Config, HealthTable, NetTest, Racer, RouterConfig,
    Upstream, WireName,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// An obvious fake. Never put real key material in a fixture.
const FAKE_KEY: &str = "sk-test-not-a-real-key";

/// What the network test should find. The race runs it beside every walk, and
/// the answer decides whether a drained walk is the providers' fault or the
/// local link's.
enum Link {
    /// A loopback server stands in for the two operator endpoints, so a drained
    /// walk is the providers' fault and comes back as `504`.
    Up,
    /// Both calls go to a closed port, so the fault is local and a drained walk
    /// comes back as `502`.
    Down,
    /// As `Up`, but the test answers only after the delay, so a failure that
    /// lands before it is held.
    SlowUp(Duration),
}

/// Per network test call. Only reached on the `Down` path, where the connect is
/// refused long before it.
const NET_TEST_TIMEOUT: Duration = Duration::from_secs(2);

/// The network test pointed at loopback, plus the server it needs kept alive.
async fn net_test(link: Link) -> (NetTest, Vec<String>, Option<MockServer>) {
    let client = reqwest::Client::new();
    match link {
        Link::Up | Link::SlowUp(_) => {
            let delay = match link {
                Link::SlowUp(delay) => delay,
                _ => Duration::ZERO,
            };
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(204).set_delay(delay))
                .mount(&server)
                .await;
            let urls = vec![
                format!("{}/generate_204", server.uri()),
                format!("{}/cdn-cgi/trace", server.uri()),
            ];
            let net = NetTest::with_urls(client, NET_TEST_TIMEOUT, urls.clone());
            (net, urls, Some(server))
        }
        Link::Down => {
            // Port 1 needs root to bind, so nothing in the test run can ever
            // listen on it and both connects are refused every time. An
            // ephemeral port would go straight back into the pool on drop and
            // a wiremock server started elsewhere in the run could claim it.
            let urls = vec![
                "http://127.0.0.1:1/generate_204".to_string(),
                "http://127.0.0.1:1/cdn-cgi/trace".to_string(),
            ];
            let net = NetTest::with_urls(client, NET_TEST_TIMEOUT, urls.clone());
            (net, urls, None)
        }
    }
}

/// One channel pointing at a wiremock server.
fn channel(base_url: &str, model_id: &str, key: &str, limit_context: u32) -> Channel {
    Channel {
        id: ChannelId::new("fakeprovider", key, model_id),
        provider: "fakeprovider".into(),
        model_id: model_id.into(),
        key_id: "key-0".into(),
        key: key.into(),
        base_url: base_url.into(),
        wire: WireName::OpenAi,
        limit_context,
        limit_output: 4_096,
        tool_call: true,
        reasoning: false,
        structured_output: false,
        modalities_input: vec!["text".to_owned()],
        intelligence: 500,
        intelligence_rank: 1,
        created: Some(1_700_000_000),
        health: ChannelHealth::live(),
        telemetry: ChannelTelemetry::default(),
    }
}

/// Everything one test needs: the state the handlers read, the throwaway
/// directory the cache and the spill files live in, and the catalogue so a
/// test can read a channel's live counters back.
struct Edge {
    state: Arc<AppState>,
    /// The URLs the network test was given, so a `502` body can be checked
    /// against them.
    net_urls: Vec<String>,
    /// Held so the loopback stand-in outlives the races that call it.
    _link: Option<MockServer>,
    _dir: TempDir,
}

impl Edge {
    /// Builds the whole state the way `main::build_state` does, with the
    /// catalogue pre-filled rather than probed.
    async fn new(mut router: RouterConfig, channels: Vec<Channel>, link: Link) -> Self {
        let (net, net_urls, link) = net_test(link).await;
        let dir = tempfile::tempdir().expect("tempdir");
        router.cache.path = dir.path().join("cache.redb");
        router.cache.page_cache_bytes = 1_000_000;
        router.spill_path = dir.path().join("spill");
        std::fs::create_dir_all(&router.spill_path).expect("the spill directory is created");

        let config = Arc::new(Config {
            router,
            providers: Vec::new(),
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: HashMap::new(),
            capabilities: HashMap::new(),
        });
        let router = &config.router;

        let db =
            Arc::new(llmrouter::cache::open_database(&router.cache).expect("the redb file opens"));
        let health = Arc::new(
            HealthTable::open(
                Arc::clone(&db),
                router.health_poll.retry_base,
                router.health_poll.backoff_max,
            )
            .expect("the health table opens"),
        );
        let cache = router
            .cache
            .enabled
            .then(|| Arc::new(Cache::open(db, &router.cache).expect("the cache opens")));

        let catalogue = Arc::new(Catalogue::new());
        catalogue.replace(channels);

        let metrics = crate::test_metrics();
        let upstream = Upstream::new(router).expect("the http client builds");
        let racer = Arc::new(Racer::new(
            Arc::clone(&config),
            Arc::clone(&catalogue),
            Arc::clone(&health),
            upstream,
            net,
            Arc::clone(&metrics),
        ));

        let state = Arc::new(AppState {
            admission: Admission::new(router.memory_high_water, Duration::from_secs(30)),
            config: Arc::clone(&config),
            catalogue,
            health,
            cache,
            racer,
            metrics,
            started_at: Instant::now(),
        });
        Self {
            state,
            net_urls,
            _link: link,
            _dir: dir,
        }
    }

    /// The real axum router. Building it is a per-request cost inside `call`,
    /// so a test that times a request builds it once up front and starts its
    /// clock after the build rather than around it.
    fn service(&self) -> axum::Router {
        llmrouter::server::router(Arc::clone(&self.state))
    }

    /// Drives one request through the real axum router.
    async fn call(&self, request: Request<Body>) -> axum::response::Response {
        self.service()
            .oneshot(request)
            .await
            .expect("the router is infallible")
    }

    /// The `router_requests_total{outcome}` value on this router's own
    /// counters. Each `Edge` owns its recorder, so the number counts this
    /// test's requests and nothing else's, which is what pins the
    /// outcome-to-status mapping in DESIGN.md.
    async fn outcome_count(&self, outcome: &str) -> f64 {
        let text = text_body(self.get("/metrics").await).await;
        series_value(
            &text,
            &format!("router_requests_total{{outcome=\"{outcome}\"}}"),
        )
    }

    /// `POST /v1/chat/completions` with a JSON body.
    async fn post_chat(&self, body: &Value) -> axum::response::Response {
        self.call(chat_post(body)).await
    }

    /// `POST /v1/messages` with a JSON body, in Anthropic shape.
    async fn post_messages(&self, body: &Value) -> axum::response::Response {
        self.call(messages_post(body)).await
    }

    /// A `GET` on one of the probe routes.
    async fn get(&self, uri: &str) -> axum::response::Response {
        self.call(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("the request builds"),
        )
        .await
    }
}

/// A `POST /v1/chat/completions` request with its body already serialised.
/// Kept apart from `Edge::post_chat` so a timing test can build and encode the
/// request before it starts its clock.
fn chat_post(body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {FAKE_KEY}"))
        .body(Body::from(serde_json::to_vec(body).expect("body encodes")))
        .expect("the request builds")
}

/// A `POST /v1/messages` request in Anthropic shape, body already serialised.
/// Same reason as `chat_post`: the encode happens before a timing test starts
/// its clock.
fn messages_post(body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("content-type", "application/json")
        .header("x-api-key", FAKE_KEY)
        .body(Body::from(serde_json::to_vec(body).expect("body encodes")))
        .expect("the request builds")
}

/// A router config with the race narrowed to something a test can count, the
/// providers buffered rather than streamed, and the keepalive off.
fn plain_config() -> RouterConfig {
    RouterConfig {
        race_width: 2,
        max_per_channel: 1,
        streaming: false,
        keepalive_interval: Duration::ZERO,
        request_timeout: Duration::from_secs(30),
        ..RouterConfig::default()
    }
}

/// Unix seconds now, for a backoff deadline a test wants in the future.
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs()
}

/// A client request in OpenAI chat shape.
fn chat_request(prompt: &str, stream: bool) -> Value {
    json!({
        "model": "auto",
        "max_tokens": 16,
        "stream": stream,
        "messages": [{ "role": "user", "content": prompt }],
    })
}

/// A provider's buffered reply.
fn provider_reply(content: &str) -> Value {
    json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "model": "fake-model",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content },
            "finish_reason": "stop",
        }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 },
    })
}

/// A router config on the shipped default: the providers are asked to stream.
fn streaming_config() -> RouterConfig {
    RouterConfig {
        streaming: true,
        ..plain_config()
    }
}

/// `count` content deltas followed by a terminal frame, as one SSE body.
fn sse(text: &str, count: usize) -> String {
    let mut body = String::new();
    for _ in 0..count {
        let frame = json!({
            "id": "chatcmpl-test",
            "object": "chat.completion.chunk",
            "model": "fake-model",
            "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": Value::Null }],
        });
        body.push_str(&format!("data: {frame}\n\n"));
    }
    let done = json!({
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "model": "fake-model",
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }],
    });
    body.push_str(&format!("data: {done}\n\ndata: [DONE]\n\n"));
    body
}

/// A provider answering with server-sent events rather than one whole body.
fn stream_template(text: &str, frames: usize) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "text/event-stream")
        .set_body_string(sse(text, frames))
}

/// The whole body of a response, as JSON.
async fn json_body(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body reads");
    serde_json::from_slice(&bytes).unwrap_or_else(|err| {
        panic!(
            "the body is not JSON ({err}): {}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

/// The whole body of a response, as text.
async fn text_body(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body reads");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// A wiremock server answering `POST /v1/chat/completions` with `template`.
async fn upstream_serving(template: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(template)
        .mount(&server)
        .await;
    server
}

/// The base URL a channel needs for a wiremock server, so that
/// `upstream::completion_url` lands on `/v1/chat/completions`.
fn base_url(server: &MockServer) -> String {
    format!("{}/v1", server.uri())
}

// ---------------------------------------------------------------------------
// The four status codes, and the body each one carries.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_prompt_larger_than_every_context_window_is_413_and_reports_both_figures() {
    let server =
        upstream_serving(ResponseTemplate::new(200).set_body_json(provider_reply("hi"))).await;
    // 64 tokens of context against a prompt of several thousand.
    let edge = Edge::new(
        plain_config(),
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 64)],
        Link::Up,
    )
    .await;

    let response = edge
        .post_chat(&chat_request(&"word ".repeat(2_000), false))
        .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = json_body(response).await;
    assert_eq!(body["error"], "too_large");
    assert_eq!(body["detail"]["largest_context"], 64);
    let prompt_tokens = body["detail"]["prompt_tokens"]
        .as_u64()
        .expect("prompt_tokens is a number");
    assert!(
        prompt_tokens > 64,
        "the estimate must exceed the window it was rejected against, got {prompt_tokens}"
    );
    // The decision is made before any channel call, so nothing reached the
    // provider.
    assert!(
        server
            .received_requests()
            .await
            .expect("the log is on")
            .is_empty(),
        "a 413 must be decided without calling a provider"
    );
    assert_eq!(
        edge.outcome_count("too_large").await,
        1.0,
        "a 413 is counted as too_large and as nothing else"
    );
    assert_eq!(edge.outcome_count("success").await, 0.0);
}

#[tokio::test]
async fn both_network_test_calls_failing_is_502_and_names_the_urls_that_failed() {
    // The provider never answers, so the only thing that can end this race is
    // the network test, and both of its calls are refused at once.
    let server =
        upstream_serving(ResponseTemplate::new(200).set_delay(Duration::from_secs(600))).await;
    let edge = Edge::new(
        plain_config(),
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Down,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = json_body(response).await;
    assert_eq!(body["error"], "no_network");
    let failed = body["detail"]["failed"]
        .as_array()
        .expect("failed is a list")
        .iter()
        .map(|v| v.as_str().expect("each reason is a string").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 2, "both operators are reported: {failed:?}");
    for url in &edge.net_urls {
        assert!(
            failed.iter().any(|f| f.contains(url)),
            "the body names {url} as one of the calls that failed: {failed:?}"
        );
    }
    assert!(
        failed.iter().all(|f| !f.contains(FAKE_KEY)),
        "the network test carries no key, and neither does the body: {failed:?}"
    );
    // A local fault is nobody's backoff.
    assert!(
        edge.state.health.in_backoff(u64::MAX).is_empty(),
        "a dead link must not move any channel's health"
    );
    assert_eq!(
        edge.outcome_count("no_network").await,
        1.0,
        "a 502 is counted as no_network and as nothing else"
    );
    assert_eq!(edge.outcome_count("success").await, 0.0);
}

#[tokio::test]
async fn rss_over_the_high_water_mark_is_503_with_a_retry_after_header() {
    let server =
        upstream_serving(ResponseTemplate::new(200).set_body_json(provider_reply("hi"))).await;
    let config = RouterConfig {
        // Every live process is over zero bytes resident.
        memory_high_water: 0,
        ..plain_config()
    };
    let edge = Edge::new(
        config,
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry_after: u64 = response
        .headers()
        .get("retry-after")
        .expect("a 503 carries Retry-After")
        .to_str()
        .expect("the header is ascii")
        .parse()
        .expect("the header is a count of seconds");
    let body = json_body(response).await;
    assert_eq!(body["error"], "not_admitted");
    assert_eq!(body["detail"]["memory_high_water"], 0);
    assert_eq!(body["detail"]["retry_after_secs"], retry_after);
    assert!(
        body["detail"]["rss"].as_u64().expect("rss is a number") > 0,
        "the body reports the sample the gate rejected on"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("the log is on")
            .is_empty(),
        "the gate runs before the body is parsed, let alone sent anywhere"
    );
    assert_eq!(
        edge.outcome_count("not_admitted").await,
        1.0,
        "a 503 is counted as not_admitted and as nothing else"
    );
    assert_eq!(edge.outcome_count("success").await, 0.0);
}

#[tokio::test]
async fn a_catalogue_with_every_channel_in_backoff_is_504_and_names_no_attempts() {
    // A 504 blames the providers, so the race only returns one once the link
    // has been proven up or no channel call was made at all. This is the second
    // case: the query empties the list before a single call, so the network
    // test never runs.
    let waiting = |key: &str| {
        let channel = channel("https://example.invalid/v1", "fake-model", key, 200_000);
        channel
            .health
            .eligible_at
            .store(now_unix() + 3_600, Ordering::Relaxed);
        channel
    };
    let edge = Edge::new(
        plain_config(),
        vec![waiting("sk-test-key-one"), waiting("sk-test-key-two")],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body = json_body(response).await;
    assert_eq!(body["error"], "no_channel");
    assert_eq!(
        body["detail"]["attempts"], 0,
        "nothing was eligible, so no channel call was made"
    );
    assert_eq!(
        body["detail"]["last_error"],
        Value::Null,
        "there is no upstream error to report when no upstream was called"
    );
    assert!(
        body["message"]
            .as_str()
            .expect("the message is a sentence")
            .contains("no channel"),
        "the body says in words what went wrong: {body}"
    );
    assert_eq!(
        edge.outcome_count("no_channel").await,
        1.0,
        "a 504 is counted as no_channel and as nothing else"
    );
    assert_eq!(edge.outcome_count("success").await, 0.0);
}

#[tokio::test]
async fn every_channel_call_failing_with_the_link_up_is_504_and_counts_the_calls() {
    let server =
        upstream_serving(ResponseTemplate::new(500).set_body_string("upstream broke")).await;
    let url = base_url(&server);
    let edge = Edge::new(
        plain_config(),
        vec![
            channel(&url, "fake-model", "sk-test-key-one", 200_000),
            channel(&url, "fake-model", "sk-test-key-two", 200_000),
        ],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;

    assert_eq!(
        response.status(),
        StatusCode::GATEWAY_TIMEOUT,
        "the link is up, so a drained walk is the providers' fault and not a 502"
    );
    let body = json_body(response).await;
    assert_eq!(body["error"], "no_channel");
    assert_eq!(
        body["detail"]["attempts"], 2,
        "two channels, one copy each, so two channel calls"
    );
    let last_error = body["detail"]["last_error"]
        .as_str()
        .expect("the last upstream error is reported");
    assert!(
        last_error.contains("500"),
        "the body names the status the provider returned: {last_error}"
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("the log is on")
            .len(),
        2,
        "both channels were really called"
    );
    // The link was proven up, so the failures really are the channels' own.
    // Both keys sit behind the same provider, so the escalation puts the
    // provider and the provider-key in backoff beside the two channels.
    let backed_off: Vec<llmrouter::HealthKey> = edge
        .state
        .health
        .in_backoff(now_unix())
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    for key in ["sk-test-key-one", "sk-test-key-two"] {
        let id = ChannelId::new("fakeprovider", key, "fake-model");
        assert!(
            backed_off.contains(&llmrouter::HealthKey::Channel(id)),
            "the channel on {key} is backed off once the link is known to be up: {backed_off:?}"
        );
    }
    assert_eq!(
        edge.outcome_count("no_channel").await,
        1.0,
        "a drained walk is counted as no_channel and as nothing else"
    );
    assert_eq!(edge.outcome_count("success").await, 0.0);
}

// ---------------------------------------------------------------------------
// The keepalive.
// ---------------------------------------------------------------------------

/// The route Claude Code actually calls. Without this, nothing in the repo
/// sends a valid Anthropic body to the router: a one-word slip in
/// `post_messages` that passed `Format::OpenAiChat` would still leave every
/// test green while every real request came back in the wrong shape.
#[tokio::test]
async fn post_messages_reads_an_anthropic_body_and_answers_in_anthropic_shape() {
    let server =
        upstream_serving(ResponseTemplate::new(200).set_body_json(provider_reply("pong"))).await;
    let edge = Edge::new(
        plain_config(),
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    // `system` as blocks and a required `max_tokens`: both Anthropic-only
    // spellings, so an OpenAI decode cannot read this body correctly.
    let response = edge
        .post_messages(&json!({
            "model": "auto",
            "max_tokens": 16,
            "system": [{ "type": "text", "text": "be terse" }],
            "messages": [{
                "role": "user",
                "content": [{ "type": "text", "text": "ping" }],
            }],
        }))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["type"], "message", "an Anthropic reply: {body}");
    assert_eq!(body["role"], "assistant");
    assert_eq!(body["content"][0]["type"], "text");
    assert_eq!(body["content"][0]["text"], "pong");
    assert!(
        body["choices"].is_null(),
        "an OpenAI-shaped reply came back instead: {body}"
    );

    // The system block survived the decode and reached the provider.
    let sent = server
        .received_requests()
        .await
        .expect("the log is on")
        .into_iter()
        .next()
        .expect("the winning channel was called");
    let sent: Value = serde_json::from_slice(&sent.body).expect("the upstream body is JSON");
    assert!(
        sent.to_string().contains("be terse"),
        "the system prompt reached the provider: {sent}"
    );
}

#[tokio::test]
async fn the_keepalive_sends_spaces_until_the_answer_and_nothing_after_it() {
    // The one case in this file that cannot use a paused clock. Tokio's
    // auto-advance only looks at timers, not at sockets, so a paused clock
    // walks straight past `connect_timeout` while the real TCP connect to
    // wiremock is still in progress and every channel call dies of a timeout
    // that never happened. So this runs on the real clock with both figures
    // scaled down: a keepalive every 20ms against a provider that takes 300ms,
    // which is the same ratio as 15s against four minutes.
    let server = upstream_serving(
        ResponseTemplate::new(200)
            .set_body_json(provider_reply("late but correct"))
            .set_delay(Duration::from_millis(300)),
    )
    .await;
    let config = RouterConfig {
        keepalive_interval: Duration::from_millis(20),
        ..plain_config()
    };
    let edge = Edge::new(
        config,
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;

    // The status line goes out before the body, so a committed response is a
    // 200 whatever the outcome turns out to be.
    assert_eq!(response.status(), StatusCode::OK);
    let mut frames = response.into_body().into_data_stream();
    let mut keepalives = 0usize;
    let mut answer: Option<Value> = None;
    while let Some(frame) = frames.next().await {
        let bytes = frame.expect("the body streams without error");
        assert!(
            answer.is_none(),
            "nothing follows the answer, but another frame arrived"
        );
        if bytes.as_ref() == b" " {
            keepalives += 1;
        } else {
            answer = Some(serde_json::from_slice(&bytes).expect("the final frame is the answer"));
        }
    }

    assert!(
        keepalives > 0,
        "the keepalive must reach the client before the first real byte"
    );
    let answer = answer.expect("the answer arrives after the keepalives");
    assert_eq!(
        answer["choices"][0]["message"]["content"],
        "late but correct"
    );
}

// ---------------------------------------------------------------------------
// Client disconnect.
// ---------------------------------------------------------------------------

/// Answers very slowly and tells the test the moment it was called, so the
/// test can hang up at a known point rather than guessing with a sleep.
struct SignalThenHang(tokio::sync::mpsc::UnboundedSender<()>);

impl Respond for SignalThenHang {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        let _ = self.0.send(());
        ResponseTemplate::new(200).set_delay(Duration::from_secs(3600))
    }
}

#[tokio::test]
async fn a_client_hanging_up_tears_down_every_channel_call_in_flight() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(SignalThenHang(tx))
        .mount(&server)
        .await;

    let url = base_url(&server);
    let edge = Edge::new(
        plain_config(),
        vec![
            channel(&url, "fake-model", "sk-test-key-one", 200_000),
            channel(&url, "fake-model", "sk-test-key-two", 200_000),
        ],
        Link::Up,
    )
    .await;

    let router = llmrouter::server::router(Arc::clone(&edge.state));
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&chat_request("ping", false)).expect("body encodes"),
        ))
        .expect("the request builds");
    let client = tokio::spawn(async move { router.oneshot(request).await });

    // Both channels are now talking to a provider that will never answer.
    rx.recv().await.expect("the first channel call arrives");
    rx.recv().await.expect("the second channel call arrives");
    let in_flight = |edge: &Edge| -> u32 {
        edge.state
            .catalogue
            .snapshot()
            .iter()
            .map(|c| c.health.in_flight.load(Ordering::Relaxed))
            .sum()
    };
    assert_eq!(in_flight(&edge), 2, "both channel calls are open");

    // The client goes away. axum drops the handler future, which fires the
    // cancellation token the race hung every channel call off.
    client.abort();
    assert!(
        client
            .await
            .expect_err("the task was aborted")
            .is_cancelled(),
        "the handler future was dropped, not run to completion"
    );

    // The walk runs as its own task, so it tears the calls down on its next
    // poll rather than inside the drop.
    let deadline = Instant::now() + Duration::from_secs(5);
    while in_flight(&edge) != 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        in_flight(&edge),
        0,
        "a disconnect must tear down every channel call, not leave them running"
    );
}

/// The real server, not the racer driven by hand: a client hanging up drops
/// the handler, and the walk must still settle the failure it held while the
/// network test was out. Were the walk dropped with the handler, the broken
/// channel would get no backoff and the next request would race it again.
#[tokio::test]
async fn a_client_hanging_up_on_the_server_still_settles_failures_already_held() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let broken = upstream_serving(ResponseTemplate::new(401).set_body_string("bad key")).await;
    let slow = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(SignalThenHang(tx))
        .mount(&slow)
        .await;

    let broken_channel = channel(&base_url(&broken), "fake-model", "sk-test-key-one", 200_000);
    let broken_id = broken_channel.id;
    let edge = Edge::new(
        plain_config(),
        vec![
            broken_channel,
            channel(&base_url(&slow), "fake-model", "sk-test-key-two", 200_000),
        ],
        Link::SlowUp(Duration::from_millis(500)),
    )
    .await;

    let client = tokio::spawn(
        edge.service()
            .oneshot(chat_post(&chat_request("ping", false))),
    );
    rx.recv().await.expect("the slow channel call arrives");
    // Long enough for the 401 on loopback, well short of the network test.
    tokio::time::sleep(Duration::from_millis(150)).await;
    client.abort();
    assert!(
        client
            .await
            .expect_err("the task was aborted")
            .is_cancelled(),
        "the handler future was dropped, not run to completion"
    );

    let key = llmrouter::HealthKey::Channel(broken_id);
    let deadline = Instant::now() + Duration::from_secs(5);
    while edge.state.health.get(&key).is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry = edge
        .state
        .health
        .get(&key)
        .expect("the failure held before the hang up is written");
    assert_eq!(entry.retry_counter, 1);
}

// ---------------------------------------------------------------------------
// One router, two shapes of client.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_router_serves_a_streaming_client_and_a_plain_client_from_the_same_answer() {
    let server = upstream_serving(
        ResponseTemplate::new(200).set_body_json(provider_reply("same answer either way")),
    )
    .await;
    let edge = Edge::new(
        plain_config(),
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let plain = edge.post_chat(&chat_request("ping", false)).await;
    assert_eq!(plain.status(), StatusCode::OK);
    assert_eq!(
        plain
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        "application/json"
    );
    let plain = json_body(plain).await;
    assert_eq!(
        plain["choices"][0]["message"]["content"],
        "same answer either way"
    );

    let streamed = edge.post_chat(&chat_request("ping", true)).await;
    assert_eq!(streamed.status(), StatusCode::OK);
    let content_type = streamed
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        content_type.starts_with("text/event-stream"),
        "a streaming client gets server-sent events, got {content_type}"
    );
    let body = text_body(streamed).await;
    assert!(
        body.contains("same answer either way"),
        "the stream carries the same answer: {body}"
    );
    assert!(
        body.trim_end().ends_with("data: [DONE]"),
        "an OpenAI stream ends with the done marker: {body}"
    );
}

/// The default production path: `streaming: true`, a provider serving SSE, a
/// streaming client. The race wins a stream, the server forwards it and tees a
/// copy into the cache, and the folded copy is what the next client is served.
#[tokio::test]
async fn a_streamed_win_is_forwarded_to_the_client_and_teed_into_the_cache() {
    let server = upstream_serving(stream_template("tick", 3)).await;
    let edge = Edge::new(
        streaming_config(),
        vec![channel(&base_url(&server), "fake-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let streamed = edge.post_chat(&chat_request("ping", true)).await;
    assert_eq!(streamed.status(), StatusCode::OK);
    let content_type = streamed
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    assert!(
        content_type.starts_with("text/event-stream"),
        "a streamed win reaches a streaming client as events, got {content_type}"
    );
    let body = text_body(streamed).await;
    assert_eq!(
        body.matches("tick").count(),
        3,
        "every delta the provider sent is forwarded: {body}"
    );
    assert!(
        body.trim_end().ends_with("data: [DONE]"),
        "an OpenAI stream ends with the done marker: {body}"
    );

    // The tee folds the copy on a spawned task, so the entry lands a moment
    // after the client's last frame. Wait on the entry appearing, not on a
    // duration, with the same two second ceiling the race tests settled on: a
    // loaded runner can take a while to schedule the task, fold the stream and
    // commit the redb transaction.
    let cache = edge.state.cache.clone().expect("the cache is on");
    for _ in 0..200 {
        if cache.entry_count().expect("the cache counts") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        cache.entry_count().expect("the cache counts"),
        1,
        "the streamed answer was written to the cache"
    );

    // Served from that entry, which proves the tee stored the folded answer
    // rather than the empty placeholder it starts from.
    let plain = edge.post_chat(&chat_request("ping", false)).await;
    assert_eq!(plain.status(), StatusCode::OK);
    let plain = json_body(plain).await;
    assert_eq!(
        plain["choices"][0]["message"]["content"], "tickticktick",
        "the cached reply carries the folded stream"
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("the mock records its requests")
            .len(),
        1,
        "the second client was served from the cache, not from the provider"
    );
    // One request served by a channel, one served from the cache, and the two
    // are counted apart: DESIGN.md gives them the same status but separate
    // series, and a success rate that counts cache hits as channel wins hides
    // a dead provider.
    assert_eq!(
        edge.outcome_count("success").await,
        1.0,
        "only the streamed win is a success"
    );
    assert_eq!(
        edge.outcome_count("hit_cache").await,
        1.0,
        "the second request is a cache hit"
    );
    for quiet in ["too_large", "no_network", "not_admitted", "no_channel"] {
        assert_eq!(
            edge.outcome_count(quiet).await,
            0.0,
            "nothing failed, so {quiet} stays at zero"
        );
    }
}

// ---------------------------------------------------------------------------
// The three probe routes.
// ---------------------------------------------------------------------------

/// A state with no upstream at all. The probe routes never call one.
async fn probes_only(channels: Vec<Channel>) -> Edge {
    Edge::new(plain_config(), channels, Link::Up).await
}

#[tokio::test]
async fn get_models_lists_every_model_with_its_channel_counts() {
    let backing_off = channel(
        "https://example.invalid/v1",
        "slow-model",
        "sk-test-b",
        8_000,
    );
    // In backoff for longer than any date can express. `eligible_at` is a
    // plain `u64` and the backoff arithmetic saturates, so this is a value the
    // field really can hold, and formatting it as a date used to panic inside
    // the handler.
    backing_off
        .health
        .eligible_at
        .store(u64::MAX, Ordering::Relaxed);
    let edge = probes_only(vec![
        channel(
            "https://example.invalid/v1",
            "fast-model",
            "sk-test-a",
            200_000,
        ),
        backing_off,
    ])
    .await;

    let response = edge.get("/v1/models").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;

    assert_eq!(body["object"], "list");
    let rows = body["data"].as_array().expect("data is a list");
    assert_eq!(rows[0]["id"], "auto", "the auto row leads the list");
    assert_eq!(rows[0]["owned_by"], "router");

    let find = |id: &str| {
        rows.iter()
            .find(|row| row["id"] == id)
            .unwrap_or_else(|| panic!("{id} is missing from {rows:#?}"))
    };

    let fast = find("fast-model");
    assert_eq!(fast["object"], "model");
    assert_eq!(fast["owned_by"], "fakeprovider");
    assert_eq!(fast["context_length"], 200_000);
    assert_eq!(fast["max_output"], 4_096);
    assert_eq!(fast["supports_tools"], true);
    assert_eq!(fast["channels"]["total"], 1);
    assert_eq!(fast["channels"]["healthy"], 1);
    assert_eq!(fast["channels"]["in_backoff"], 0);
    assert_eq!(fast["channels"]["providers"], json!(["fakeprovider"]));
    assert_eq!(
        fast["channels"]["next_eligible"],
        Value::Null,
        "a model with a live channel has nothing to wait for"
    );

    let slow = find("slow-model");
    assert_eq!(slow["channels"]["healthy"], 0);
    assert_eq!(slow["channels"]["in_backoff"], 1);
    assert_eq!(
        slow["channels"]["next_eligible"], "9999-12-31T23:59:59Z",
        "a model with only backed-off channels says when it returns, clamped to          the furthest date rather than panicking the handler"
    );

    // The auto row carries the whole catalogue.
    assert_eq!(rows[0]["channels"]["total"], 2);
    assert_eq!(rows[0]["channels"]["healthy"], 1);
}

#[tokio::test]
async fn healthz_answers_from_the_first_second_with_an_empty_catalogue() {
    // A cold start: the port is listening before a single provider has
    // answered anything.
    let edge = probes_only(Vec::new()).await;

    let response = edge.get("/healthz").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;

    assert_eq!(body["status"], "ok");
    assert_eq!(body["channels"]["total"], 0);
    assert_eq!(body["channels"]["eligible"], 0);
    assert_eq!(body["unhealthy"], json!([]));
    assert_eq!(
        body["cache_entries"], 0,
        "the cache is on and empty, so the count is a number rather than null"
    );
    assert!(
        body["uptime_seconds"].as_u64().expect("uptime is a number") < 60,
        "a freshly built state reports an uptime of seconds, not minutes"
    );
}

#[tokio::test]
async fn healthz_reports_a_null_cache_count_when_the_cache_is_off() {
    let config = RouterConfig {
        cache: CacheConfig {
            enabled: false,
            ..CacheConfig::default()
        },
        ..plain_config()
    };
    let edge = Edge::new(config, Vec::new(), Link::Up).await;

    let body = json_body(edge.get("/healthz").await).await;
    assert_eq!(
        body["cache_entries"],
        Value::Null,
        "no cache means no count, not a zero that looks like an empty cache"
    );
}

#[tokio::test]
async fn healthz_counts_only_the_channels_that_can_be_called_right_now() {
    let backing_off = channel(
        "https://example.invalid/v1",
        "slow-model",
        "sk-test-b",
        8_000,
    );
    backing_off
        .health
        .eligible_at
        .store(u64::MAX, Ordering::Relaxed);
    let edge = probes_only(vec![
        channel(
            "https://example.invalid/v1",
            "fast-model",
            "sk-test-a",
            200_000,
        ),
        backing_off,
    ])
    .await;

    let body = json_body(edge.get("/healthz").await).await;
    assert_eq!(body["channels"]["total"], 2);
    assert_eq!(
        body["channels"]["eligible"], 1,
        "a channel waiting out a backoff is counted but not eligible"
    );
}

#[tokio::test]
async fn metrics_names_every_series_the_design_lists() {
    let edge = probes_only(Vec::new()).await;

    let response = edge.get("/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        "text/plain; version=0.0.4",
        "prometheus refuses a scrape it cannot identify"
    );
    let text = text_body(response).await;

    for name in [
        "router_requests_total",
        "router_rss_bytes",
        "router_cache_entries",
    ] {
        assert!(
            text.contains(&format!("# TYPE {name}")),
            "{name} is missing from /metrics:\n{text}"
        );
    }
    // Every outcome is seeded, so an alert sits on a zero rather than on
    // missing data until the first failure of that kind.
    for outcome in [
        "hit_cache",
        "success",
        "too_large",
        "no_network",
        "not_admitted",
        "no_channel",
    ] {
        assert!(
            text.contains(&format!("router_requests_total{{outcome=\"{outcome}\"}}")),
            "the {outcome} series is missing from /metrics:\n{text}"
        );
    }
    // The handler reads RSS on every scrape, so the gauge is the live figure
    // rather than the zero it was seeded with.
    let rss = text
        .lines()
        .find_map(|line| line.strip_prefix("router_rss_bytes "))
        .and_then(|value| value.parse::<f64>().ok())
        .expect("router_rss_bytes carries a value");
    assert!(rss > 0.0, "the scrape reads this process's real RSS");
}

/// The value of one Prometheus series in a scrape, or zero when it is absent.
fn series_value(text: &str, series: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(series))
        .and_then(|rest| rest.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

/// The last two series DESIGN.md lists only appear once a request has run:
/// `router_attempts_total` carries a provider and model unknown at startup, and
/// a histogram has no series until something has been recorded into it.
///
/// This router owns its recorder, so the counts are this test's own. The
/// histogram is still read before and after the request, which keeps the
/// assertion on growth rather than on presence.
#[tokio::test]
async fn metrics_gains_the_latency_histogram_and_the_attempt_counter_once_a_request_has_run() {
    let server =
        upstream_serving(ResponseTemplate::new(200).set_body_json(provider_reply("counted"))).await;
    // No other test uses this model id, so the channel id it hashes into is
    // written by this request and by nothing else.
    let model_id = "metrics-only-model";
    let edge = Edge::new(
        plain_config(),
        vec![channel(&base_url(&server), model_id, FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let stages = ["router", "upstream"];
    let before = text_body(edge.get("/metrics").await).await;
    let counts: Vec<f64> = stages
        .iter()
        .map(|stage| {
            series_value(
                &before,
                &format!("router_latency_seconds_count{{stage=\"{stage}\"}}"),
            )
        })
        .collect();

    let response = edge.post_chat(&chat_request("ping", false)).await;
    assert_eq!(response.status(), StatusCode::OK);

    let text = text_body(edge.get("/metrics").await).await;
    assert!(
        text.contains("# TYPE router_latency_seconds histogram"),
        "the latency histogram is missing from /metrics:\n{text}"
    );
    // This router owns its recorder and ran one request, so each stage gains
    // exactly one reading. Both call sites that write it sit in the completion
    // paths, so dropping either one still fails this.
    for (stage, was) in stages.iter().zip(counts) {
        let now = series_value(
            &text,
            &format!("router_latency_seconds_count{{stage=\"{stage}\"}}"),
        );
        assert_eq!(
            now,
            was + 1.0,
            "the {stage} half of the latency split did not gain exactly this \
             request's reading ({was} before, {now} after):\n{text}"
        );
        assert!(
            text.contains(&format!("router_latency_seconds_sum{{stage=\"{stage}\"}}")),
            "the {stage} half of the latency split is missing from /metrics:\n{text}"
        );
    }
    let line = attempt_series(&text, "success");
    assert!(
        line.contains("provider=\"fakeprovider\"") && line.contains(&format!("model=\"{model_id}\"")),
        "this request's one winning channel call is counted against its provider and model:\n{text}"
    );
    assert!(line.ends_with(" 1"), "{line}");
    assert!(
        !text.contains(FAKE_KEY),
        "the key never reaches a metric label"
    );
}

/// Two keys on the same provider and model are two channels, but one series.
///
/// The attempt counter used to carry the channel id, which is per
/// provider/model/key: 4,807 channels times four outcomes is tens of thousands
/// of series on a page scraped every few seconds. Both channels here fail, so
/// both are counted, and the scrape must hold one `router_attempts_total`
/// series rather than one per key.
#[tokio::test]
async fn attempts_are_counted_per_provider_and_model_not_per_key() {
    let server =
        upstream_serving(ResponseTemplate::new(500).set_body_string("upstream down")).await;
    let model_id = "cardinality-model";
    let edge = Edge::new(
        plain_config(),
        vec![
            channel(&base_url(&server), model_id, "sk-test-key-one", 200_000),
            channel(&base_url(&server), model_id, "sk-test-key-two", 200_000),
        ],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("ping", false)).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

    let text = text_body(edge.get("/metrics").await).await;
    let series: Vec<&str> = text
        .lines()
        .filter(|line| line.starts_with("router_attempts_total{"))
        .collect();
    assert_eq!(
        series.len(),
        1,
        "two keys on one provider and model must share one series:\n{text}"
    );
    let line = attempt_series(&text, "http_error");
    assert!(
        line.contains("provider=\"fakeprovider\"")
            && line.contains(&format!("model=\"{model_id}\"")),
        "the one series is labelled by provider and model:\n{text}"
    );
    assert!(
        line.ends_with(" 2"),
        "both failed channel calls land on the one series:\n{line}"
    );
}

/// The one `router_attempts_total` line carrying `outcome`. The exporter picks
/// the label order, so a series is matched by its parts, not by one rendered
/// string.
fn attempt_series<'a>(text: &'a str, outcome: &str) -> &'a str {
    text.lines()
        .find(|line| {
            line.starts_with("router_attempts_total{")
                && line.contains(&format!("outcome=\"{outcome}\""))
        })
        .unwrap_or_else(|| panic!("no {outcome} attempt series in:\n{text}"))
}

/// Serving a client that did not ask for a stream means folding the whole
/// winning stream first, so the fold is part of the request's own time. The
/// duration used to be sampled before the fold, which called time to first
/// token the whole request and left the fold out of both halves of
/// `router_latency_seconds`.
///
/// This router owns its recorder, so the two halves of the histogram sum to
/// this one request's accounted time, and that is checked against the wall
/// clock the test keeps around the very same request.
#[tokio::test]
async fn folding_a_stream_for_a_plain_client_is_counted_in_the_time_the_request_took() {
    let server = upstream_serving(stream_template("some tokens ", 10_000)).await;
    let edge = Edge::new(
        streaming_config(),
        vec![channel(
            &base_url(&server),
            "fold-timing-model",
            FAKE_KEY,
            200_000,
        )],
        Link::Up,
    )
    .await;

    // The clock goes around the handler and as little else as possible. The
    // router build and the body encode are per-request work that happens
    // outside the server's own span, so doing them here rather than inside the
    // measured window keeps the unaccounted slice down to the tower plumbing.
    // Otherwise a single scheduler preemption in that slice is enough to push
    // the accounted share under the floor with nothing wrong in the router.
    let service = edge.service();
    let request = chat_post(&chat_request("fold timed", false));
    let started = Instant::now();
    let response = service
        .oneshot(request)
        .await
        .expect("the router is infallible");
    let wall = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);

    let text = text_body(edge.get("/metrics").await).await;
    let accounted: f64 = ["router", "upstream"]
        .iter()
        .map(|stage| {
            series_value(
                &text,
                &format!("router_latency_seconds_sum{{stage=\"{stage}\"}}"),
            )
        })
        .sum();
    assert!(
        accounted >= wall.as_secs_f64() * 0.8,
        "the request took {}ms, the fold of 10,000 frames included, and the two \
         halves of router_latency_seconds account for {}ms:\n{text}",
        wall.as_millis(),
        accounted * 1000.0
    );
}

/// The buffered arm has the same job as the fold: the reply still has to be
/// translated back into the client's format, and that encode is the largest
/// piece of work the router does after the race. The duration used to be
/// sampled before it, so the router half of `router_latency_seconds` left out
/// the one step the half exists to measure.
///
/// Same check as the fold above: this router owns its recorder, so the two
/// halves of the histogram are this one request's accounted time, and a
/// multi-megabyte reply makes the encode far too large to hide inside the
/// slack.
#[tokio::test]
async fn encoding_a_large_reply_for_the_client_is_counted_as_router_overhead() {
    let huge = "lorem ipsum dolor sit amet ".repeat(160_000);
    let server =
        upstream_serving(ResponseTemplate::new(200).set_body_json(provider_reply(&huge))).await;
    let edge = Edge::new(
        plain_config(),
        vec![channel(
            &base_url(&server),
            "encode-timing-model",
            FAKE_KEY,
            200_000,
        )],
        Link::Up,
    )
    .await;

    // Anthropic in, OpenAI upstream: the reply is genuinely rebuilt on the way
    // out rather than passed through.
    //
    // As in the fold test above, the router build and the body encode happen
    // before the clock starts, so the measured window is the handler's own
    // window plus the tower plumbing and nothing else.
    let service = edge.service();
    let request = messages_post(&json!({
        "model": "auto",
        "max_tokens": 16,
        "messages": [{ "role": "user", "content": "encode timed" }],
    }));
    let started = Instant::now();
    let response = service
        .oneshot(request)
        .await
        .expect("the router is infallible");
    let wall = started.elapsed();
    assert_eq!(response.status(), StatusCode::OK);

    let text = text_body(edge.get("/metrics").await).await;
    let accounted: f64 = ["router", "upstream"]
        .iter()
        .map(|stage| {
            series_value(
                &text,
                &format!("router_latency_seconds_sum{{stage=\"{stage}\"}}"),
            )
        })
        .sum();
    assert!(
        accounted >= wall.as_secs_f64() * 0.8,
        "the request took {}ms, translating a {}MB reply back into the client's \
         format included, and the two halves of router_latency_seconds account \
         for {}ms:\n{text}",
        wall.as_millis(),
        huge.len() / 1_000_000,
        accounted * 1000.0
    );
}

/// A provider that sends its first token, then goes on generating for `pause`
/// before it sends the rest. wiremock cannot pause part way through a body, so
/// this is a raw listener.
async fn stream_that_pauses_after_the_first_token(
    pause: Duration,
) -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is free");
    let addr = listener.local_addr().expect("the port is known");
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut scratch = [0u8; 65_536];
                let _ = socket.read(&mut scratch).await;
                let first = sse("first ", 1);
                let first = &first[..first.find("\n\n").expect("a frame ends") + 2];
                let rest = sse("rest ", 3);
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{first}\r\n",
                    first.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.flush().await;
                tokio::time::sleep(pause).await;
                let tail = format!("{:x}\r\n{rest}\r\n0\r\n\r\n", rest.len());
                let _ = socket.write_all(tail.as_bytes()).await;
                let _ = socket.flush().await;
            });
        }
    });
    (format!("http://{addr}"), task)
}

/// A plain client on a streaming router waits for the fold, and the fold waits
/// for the provider to finish generating. That generation is the provider's
/// time, not the router's. It used to land in the router half of
/// `router_latency_seconds`, so a long answer read as minutes of router
/// overhead on the one graph meant to prove the router adds under 10ms.
#[tokio::test]
async fn generation_after_the_first_token_is_upstream_time_for_a_plain_client() {
    const PAUSE: Duration = Duration::from_millis(2_000);
    let (url, task) = stream_that_pauses_after_the_first_token(PAUSE).await;
    let edge = Edge::new(
        streaming_config(),
        vec![channel(&url, "generation-timing-model", FAKE_KEY, 200_000)],
        Link::Up,
    )
    .await;

    let response = edge.post_chat(&chat_request("generate", false)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let text = text_body(edge.get("/metrics").await).await;
    task.abort();

    let sum = |stage: &str| {
        series_value(
            &text,
            &format!("router_latency_seconds_sum{{stage=\"{stage}\"}}"),
        )
    };
    assert!(
        sum("upstream") >= PAUSE.as_secs_f64() * 0.9,
        "the provider generated for {}ms after its first token, and the upstream \
         half only holds {}ms:\n{text}",
        PAUSE.as_millis(),
        sum("upstream") * 1000.0
    );
    assert!(
        sum("router") < 0.5,
        "the router half holds {}ms, which is the provider's generation time:\n{text}",
        sum("router") * 1000.0
    );
}
