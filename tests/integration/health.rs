//! Health and backoff, driven through the real race engine.
//!
//! Every test here builds a [`Racer`] over a real redb file and a set of
//! wiremock upstreams, runs a walk, and then reads the health table. Nothing
//! calls the health functions directly: the point of this tier is that the
//! orchestration writes what the unit tests say the arithmetic should be.
//!
//! Two things need explaining.
//!
//! The network test. A walk holds a failed channel's backoff until it knows
//! whether the fault was the channel's or the local link's, so every harness
//! here points `NetTest` at its own wiremock server answering 204. The link is
//! then provably up without touching the real internet.
//!
//! Ordering. The race cancels everything the moment a channel answers, and a
//! cancelled trial moves no health. So the failing channels answer at once and
//! the winning one is given a delay, which makes the failures land first.
//!
//! Keys are obvious fakes throughout.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use llmrouter::catalogue::{ChannelHealth, ChannelTelemetry};
use llmrouter::config::WireName;
use llmrouter::server::PromptBody;
use llmrouter::{
    Catalogue, Channel, ChannelFilter, ChannelId, Config, HealthEntry, HealthKey, HealthTable,
    NetTest, RaceInput, RaceOutcome, Racer, RouterConfig, Upstream,
};
use rstest::rstest;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// How long the winning channel waits before answering. This is the margin the
/// failing channels' whole round trip has to fit inside, not just their
/// dispatch: the race cancels every other channel the moment one wins, and a
/// cancelled trial moves no health, so a row only exists if the failing call
/// was sent, answered and recorded before the winner replies. Matches
/// LET_BOTH_CALLS_GO_OUT in tests/integration/race.rs.
const WINNER_DELAY: Duration = Duration::from_millis(250);

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970")
        .as_secs()
}

/// A stand-in for the two operator endpoints: one local server answering 204,
/// so the link is provably up and no test reaches the real internet.
async fn link_up() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/generate_204"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    server
}

fn config(dir: &Path) -> Config {
    Config {
        router: RouterConfig {
            // One copy per channel, so "the whole trial counts once" is not
            // what any of these tests is leaning on.
            max_per_channel: 1,
            race_width: 8,
            spill_path: dir.to_path_buf(),
            ..RouterConfig::default()
        },
        providers: Vec::new(),
        models: Vec::new(),
        intelligence: Vec::new(),
        keys: HashMap::new(),
        capabilities: HashMap::new(),
    }
}

/// One channel row pointing at a wiremock server. `rank` orders the walk;
/// lower is tried first.
fn channel(provider: &str, key_id: &str, model_id: &str, base_url: &str, rank: u32) -> Channel {
    // An obvious fake. Never real key material in a fixture.
    let key = format!("sk-test-not-a-real-key-{provider}-{key_id}");
    Channel {
        id: ChannelId::new(provider, &key, model_id),
        provider: provider.into(),
        model_id: model_id.into(),
        key_id: key_id.into(),
        key: key.as_str().into(),
        base_url: base_url.into(),
        wire: WireName::OpenAi,
        limit_context: 128_000,
        limit_output: 8_192,
        tool_call: true,
        reasoning: false,
        structured_output: false,
        modalities_input: vec!["text".to_string()],
        intelligence: 1_000u16.saturating_sub(rank as u16),
        intelligence_rank: rank,
        created: None,
        health: ChannelHealth::live(),
        telemetry: ChannelTelemetry::default(),
    }
}

/// The router, assembled over a real redb file and whatever channels a test
/// hands it.
struct Harness {
    health: Arc<HealthTable>,
    catalogue: Arc<Catalogue>,
    racer: Racer,
    _link: MockServer,
}

impl Harness {
    async fn open(dir: &Path, channels: Vec<Channel>) -> Self {
        let config = config(dir);
        let db = Arc::new(
            redb::Database::create(dir.join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                config.router.health_poll.retry_base,
                config.router.health_poll.backoff_max,
                llmrouter::metrics::Metrics::new().expect("the recorder builds"),
            )
            .expect("the health table opens"),
        );
        let catalogue = Arc::new(Catalogue::new());
        catalogue.replace(channels);

        let upstream = Upstream::new(&config.router).expect("the outbound client builds");
        let link = link_up().await;
        let net = NetTest::with_urls(
            upstream.client().clone(),
            config.router.health_poll.network_test_timeout,
            vec![format!("{}/generate_204", link.uri())],
        );
        let racer = Racer::new(
            Arc::new(config),
            Arc::clone(&catalogue),
            Arc::clone(&health),
            upstream,
            net,
            crate::test_metrics(),
        );
        Self {
            health,
            catalogue,
            racer,
            _link: link,
        }
    }

    /// One client request's whole walk, on the buffered path.
    async fn race(&self) -> RaceOutcome {
        self.race_filtered(ChannelFilter {
            min_context: 0,
            needs_tools: false,
            named_model: None,
        })
        .await
    }

    /// The same walk against a filter the test chooses, so a test can leave a
    /// channel eligible and still out of this request's reach.
    async fn race_filtered(&self, filter: ChannelFilter) -> RaceOutcome {
        let body = PromptBody::Memory(Bytes::from_static(br#"{"messages":[]}"#));
        self.racer
            .run(
                RaceInput {
                    outbound: Arc::new(body),
                    filter,
                    streaming: false,
                    named_model: None,
                    exclude: std::collections::HashSet::new(),
                },
                CancellationToken::new(),
            )
            .await
    }

    fn entry(&self, key: &HealthKey) -> Option<HealthEntry> {
        self.health.get(key)
    }

    fn channel_entry(&self, id: ChannelId) -> Option<HealthEntry> {
        self.entry(&HealthKey::Channel(id))
    }

    /// Finds a row by its id and clears the backoff the catalogue mirrors, so
    /// the next walk picks it up again. The health table is left alone: this
    /// stands in for the backoff deadline passing, not for a recovery.
    /// Stands in for every deadline on the row passing.
    ///
    /// A row carries one deadline per level, and a failure that took the
    /// channel down usually took its pair and its provider with it, so
    /// clearing the channel's own deadline alone leaves the row held by the
    /// two above it and the walk never calls it.
    fn reopen_row(&self, id: ChannelId) {
        let snapshot = self.catalogue.snapshot();
        let row = snapshot
            .iter()
            .find(|c| c.id == id)
            .expect("the row is in the catalogue");
        row.health.eligible_at.store(0, AtomicOrdering::Relaxed);
        row.health
            .pair_eligible_at
            .store(0, AtomicOrdering::Relaxed);
        row.health
            .provider_eligible_at
            .store(0, AtomicOrdering::Relaxed);
    }

    fn row_counters(&self, id: ChannelId) -> (u32, u64) {
        let snapshot = self.catalogue.snapshot();
        let row = snapshot
            .iter()
            .find(|c| c.id == id)
            .expect("the row is in the catalogue");
        (
            row.health.retry_counter.load(AtomicOrdering::Relaxed),
            row.health.eligible_at.load(AtomicOrdering::Relaxed),
        )
    }

    fn failures(&self, id: ChannelId) -> u64 {
        let snapshot = self.catalogue.snapshot();
        let row = snapshot
            .iter()
            .find(|c| c.id == id)
            .expect("the row is in the catalogue");
        row.telemetry.failures.load(AtomicOrdering::Relaxed)
    }

    /// The channel's open-call counter, which is shared across every client
    /// request hitting that row.
    fn in_flight(&self, id: ChannelId) -> Arc<AtomicU32> {
        let snapshot = self.catalogue.snapshot();
        let row = snapshot
            .iter()
            .find(|c| c.id == id)
            .expect("the row is in the catalogue");
        Arc::clone(&row.health.in_flight)
    }
}

/// A wiremock server answering every completion call with `template`.
async fn upstream_serving(template: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    mount(&server, template).await;
    server
}

async fn mount(server: &MockServer, template: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(template)
        .mount(server)
        .await;
}

/// A complete OpenAI chat reply, which is what the race needs before it will
/// call a channel healthy: 2xx carrying non-empty content.
fn good_reply() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .set_body_json(serde_json::json!({
            "id": "chatcmpl-integration",
            "object": "chat.completion",
            "created": 1_700_000_000,
            "model": "winner-model",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "pong" },
                "finish_reason": "stop",
            }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 },
        }))
        .set_delay(WINNER_DELAY)
}

fn rejected(status: u16, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(serde_json::json!({
        "error": { "message": message, "type": "invalid_request_error" },
    }))
}

fn won(outcome: &RaceOutcome) -> ChannelId {
    match outcome {
        RaceOutcome::Buffered { winner, .. } => winner.channel_id,
        RaceOutcome::Exhausted { last_error, .. } => {
            panic!("the walk drained instead of winning: {last_error:?}")
        }
        RaceOutcome::NoNetwork { failed } => {
            panic!("the network test settled, which these tests rely on it not doing: {failed:?}")
        }
        _ => panic!("the walk neither won nor drained"),
    }
}

/// The deadline a failure written between `before` and `after` must carry.
fn deadline_is(entry: HealthEntry, before: u64, after: u64, wait: u64) {
    assert!(
        entry.eligible_at >= before + wait && entry.eligible_at <= after + wait,
        "a {wait}s backoff written between {before} and {after} should be eligible in that \
         window plus {wait}, got {}",
        entry.eligible_at
    );
}

#[tokio::test]
async fn a_401_marks_the_channel_unhealthy_and_the_walk_carries_on() {
    let dir = TempDir::new().expect("tempdir");
    let dead = upstream_serving(rejected(401, "invalid api key")).await;
    let good = upstream_serving(good_reply()).await;

    let expired = channel("zai", "key-a", "glm-5", &format!("{}/", dead.uri()), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let (expired_id, alive_id) = (expired.id, alive.id);

    let h = Harness::open(dir.path(), vec![expired, alive]).await;
    let before = now_unix();
    let outcome = h.race().await;
    let after = now_unix();

    assert_eq!(
        won(&outcome),
        alive_id,
        "the healthy channel served the reply"
    );

    let entry = h
        .channel_entry(expired_id)
        .expect("the 401 put the channel into backoff");
    assert_eq!(entry.retry_counter, 1);
    // A dead key is an account failure, which starts at an hour: retrying
    // it every second only spends requests on a key that is not coming back.
    // The group's start is not stored as the base, which stays the default.
    assert_eq!(
        entry.retry_base_secs, 1,
        "no Retry-After, so the default base"
    );
    deadline_is(entry, before, after, 3600);

    // The row the next query reads carries the same deadline, so the channel
    // is out of the list without waiting for anything to be rebuilt.
    let (counter, eligible_at) = h.row_counters(expired_id);
    assert_eq!(counter, 1);
    assert_eq!(eligible_at, entry.eligible_at);

    // And the channel that answered is untouched.
    assert_eq!(h.channel_entry(alive_id), None);
}

#[tokio::test]
async fn consecutive_failures_double_the_wait_from_the_group_start() {
    let dir = TempDir::new().expect("tempdir");
    let dead = upstream_serving(rejected(500, "upstream exploded")).await;
    let good = upstream_serving(good_reply()).await;

    let broken = channel("zai", "key-a", "glm-5", &dead.uri(), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let broken_id = broken.id;

    let h = Harness::open(dir.path(), vec![broken, alive]).await;

    let mut waits = Vec::new();
    for _ in 0..3 {
        // The backoff from the previous walk would hide the row, so stand in
        // for its deadline passing.
        h.reopen_row(broken_id);
        let before = now_unix();
        let outcome = h.race().await;
        let after = now_unix();
        assert_ne!(won(&outcome), broken_id);
        let entry = h.channel_entry(broken_id).expect("the failure is recorded");
        waits.push((entry, before, after));
    }

    for (index, (entry, before, after)) in waits.into_iter().enumerate() {
        let counter = index as u32 + 1;
        assert_eq!(entry.retry_counter, counter);
        // A 500 is the busy group: 30 seconds, then 60, then 120.
        deadline_is(entry, before, after, 30 << index);
    }
}

/// Which form of `Retry-After` the provider sends. RFC 9110 allows both and
/// the router has to honour either.
#[derive(Debug, Clone, Copy)]
enum RetryAfterForm {
    Seconds,
    HttpDate,
}

#[rstest]
#[case::seconds_form(RetryAfterForm::Seconds)]
#[case::http_date_form(RetryAfterForm::HttpDate)]
#[tokio::test]
async fn retry_after_sets_the_base_in_either_form(#[case] form: RetryAfterForm) {
    const AHEAD: u64 = 90;

    let dir = TempDir::new().expect("tempdir");
    let header = match form {
        RetryAfterForm::Seconds => AHEAD.to_string(),
        RetryAfterForm::HttpDate => {
            httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(AHEAD))
        }
    };
    let limited = upstream_serving(
        rejected(429, "rate limited").insert_header("retry-after", header.as_str()),
    )
    .await;
    let good = upstream_serving(good_reply()).await;

    let throttled = channel("zai", "key-a", "glm-5", &limited.uri(), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let throttled_id = throttled.id;

    let h = Harness::open(dir.path(), vec![throttled, alive]).await;
    let before = now_unix();
    assert_ne!(won(&h.race().await), throttled_id);
    let after = now_unix();

    let entry = h
        .channel_entry(throttled_id)
        .expect("the 429 put the channel into backoff");
    // The date form is resolved against the clock and truncated twice: once
    // when the header is formatted to whole seconds, once when the remaining
    // gap is read back as whole seconds. Real time passes between the two, so
    // allow a few seconds of slack.
    assert!(
        (AHEAD - 3..=AHEAD).contains(&entry.retry_base_secs),
        "the provider asked for {AHEAD}s, the base became {}",
        entry.retry_base_secs
    );
    assert_eq!(entry.retry_counter, 1);
    deadline_is(entry, before, after, entry.retry_base_secs);
    assert!(
        entry.retry_base_secs > 1,
        "the header, not the configured base of 1 second, decided the wait"
    );
}

#[tokio::test]
async fn a_success_clears_the_counter_and_the_row() {
    let dir = TempDir::new().expect("tempdir");
    let flaky = MockServer::start().await;
    mount(&flaky, rejected(503, "briefly unavailable")).await;
    let good = upstream_serving(good_reply()).await;

    let recovering = channel("zai", "key-a", "glm-5", &flaky.uri(), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let recovering_id = recovering.id;

    let h = Harness::open(dir.path(), vec![recovering, alive]).await;
    assert_ne!(won(&h.race().await), recovering_id);
    assert_eq!(
        h.channel_entry(recovering_id)
            .expect("the 503 is recorded")
            .retry_counter,
        1
    );

    // The provider comes back. Its deadline passing is stood in for, and this
    // time it answers first, so it wins outright.
    flaky.reset().await;
    mount(&flaky, good_reply().set_delay(Duration::ZERO)).await;
    h.reopen_row(recovering_id);

    assert_eq!(won(&h.race().await), recovering_id);
    assert_eq!(
        h.channel_entry(recovering_id),
        None,
        "a success drops the entry, so the next failure starts from the base again"
    );
    assert_eq!(h.row_counters(recovering_id), (0, 0));
}

#[tokio::test]
async fn a_429_counts_on_its_own_but_not_while_our_own_calls_are_in_flight() {
    let dir = TempDir::new().expect("tempdir");
    let limited = upstream_serving(rejected(429, "too many requests")).await;
    let good = upstream_serving(good_reply()).await;

    let busy = channel("zai", "key-a", "glm-5", &limited.uri(), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let busy_id = busy.id;

    let h = Harness::open(dir.path(), vec![busy, alive]).await;

    // Nothing else of ours is open against the channel, so the 429 is the
    // provider rationing us and it counts.
    assert_ne!(won(&h.race().await), busy_id);
    assert_eq!(
        h.channel_entry(busy_id)
            .expect("a lone 429 is an ordinary failure")
            .retry_counter,
        1
    );

    // Back to healthy, then another client request is left open against the
    // same channel for the whole of the next walk. The counter is the one the
    // race reads, so holding it up is what a second request in flight looks
    // like from inside the rule, without a second walk and the timing it would
    // need to overlap reliably.
    h.health.record_success(&HealthKey::Channel(busy_id));
    h.reopen_row(busy_id);
    let other_request = h.in_flight(busy_id);
    other_request.fetch_add(1, AtomicOrdering::Relaxed);
    let failures_before = h.failures(busy_id);

    assert_ne!(won(&h.race().await), busy_id);
    other_request.fetch_sub(1, AtomicOrdering::Relaxed);

    assert_eq!(
        h.failures(busy_id) - failures_before,
        1,
        "the walk really did call the channel and really did get a 429"
    );
    assert_eq!(
        h.channel_entry(busy_id),
        None,
        "a 429 the router's own concurrency caused must not push the channel out of the list"
    );
    assert_eq!(
        h.row_counters(busy_id).1,
        0,
        "and the row keeps its place in the list, with no new deadline on it"
    );
}

#[tokio::test]
async fn a_failure_escalates_to_the_pair_and_then_to_the_provider() {
    let dir = TempDir::new().expect("tempdir");
    let dead = upstream_serving(rejected(401, "invalid api key")).await;
    let good = upstream_serving(good_reply()).await;

    // One provider, two keys, one channel each. Both keys are dead, so the
    // failures should walk all the way up to the provider.
    let first = channel("zai", "key-a", "glm-5", &dead.uri(), 0);
    let second = channel("zai", "key-b", "glm-5", &dead.uri(), 1);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 2);
    let (first_id, second_id, alive_id) = (first.id, second.id, alive.id);

    let h = Harness::open(dir.path(), vec![first, second, alive]).await;
    assert_eq!(won(&h.race().await), alive_id);

    for id in [first_id, second_id] {
        assert_eq!(
            h.channel_entry(id)
                .expect("the channel backed off")
                .retry_counter,
            1
        );
    }
    for key_id in ["key-a", "key-b"] {
        let pair = HealthKey::ProviderKey {
            provider: "zai".to_string(),
            key_id: key_id.to_string(),
        };
        assert_eq!(
            h.entry(&pair)
                .unwrap_or_else(|| panic!(
                    "no channel on zai/{key_id} is callable, so the pair \
                                           goes unhealthy and the poller re-probes it"
                ))
                .retry_counter,
            1
        );
    }
    assert_eq!(
        h.entry(&HealthKey::Provider("zai".to_string()))
            .expect("every key on zai is down, so the provider is down")
            .retry_counter,
        1
    );

    // Nothing above the channel that answered moved.
    assert_eq!(h.entry(&HealthKey::Provider("groq".to_string())), None);
    assert_eq!(
        h.entry(&HealthKey::ProviderKey {
            provider: "groq".to_string(),
            key_id: "key-g".to_string(),
        }),
        None
    );
}

/// A `402` on one model is not a verdict on the key. Health is binary and
/// failures are not classified by status, so the provider/key pair goes down
/// only when every channel on it is unhealthy (DESIGN.md "The unit of health").
///
/// A free OpenRouter key gets `402` on a paid model while its free models
/// carry on serving. Marking the pair would flap `/healthz` on every request
/// and spend a keyed probe each poll on a key that was never dead.
#[tokio::test]
async fn a_402_on_one_model_leaves_the_pair_healthy_while_a_sibling_is_eligible() {
    let dir = TempDir::new().expect("tempdir");
    let broke = upstream_serving(rejected(402, "insufficient credits")).await;
    let good = upstream_serving(good_reply()).await;

    // One openrouter key carrying two channels. Only the first is asked for:
    // the second takes no tools and this request needs them, so it is never
    // tried and stays eligible throughout.
    let asked = channel("openrouter", "key-a", "glm-5", &broke.uri(), 0);
    let mut untried = channel("openrouter", "key-a", "kimi-k2", &good.uri(), 1);
    untried.tool_call = false;
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 2);
    let (asked_id, untried_id, alive_id) = (asked.id, untried.id, alive.id);

    let h = Harness::open(dir.path(), vec![asked, untried, alive]).await;
    let outcome = h
        .race_filtered(ChannelFilter {
            min_context: 0,
            needs_tools: true,
            named_model: None,
        })
        .await;
    assert_eq!(won(&outcome), alive_id);

    assert_eq!(
        h.channel_entry(asked_id)
            .expect("the channel that took the 402 backed off")
            .retry_counter,
        1
    );
    assert_eq!(h.channel_entry(untried_id), None);
    assert_eq!(
        h.entry(&HealthKey::ProviderKey {
            provider: "openrouter".to_string(),
            key_id: "key-a".to_string(),
        }),
        None,
        "a sibling channel on the key is still eligible, so the pair stays healthy"
    );
    assert_eq!(
        h.entry(&HealthKey::Provider("openrouter".to_string())),
        None
    );
}

#[tokio::test]
async fn the_health_table_survives_a_restart_through_redb() {
    let dir = TempDir::new().expect("tempdir");
    let dead = upstream_serving(rejected(401, "invalid api key")).await;
    let good = upstream_serving(good_reply()).await;

    let expired = channel("zai", "key-a", "glm-5", &dead.uri(), 0);
    let alive = channel("groq", "key-g", "llama-4", &good.uri(), 1);
    let (expired_id, alive_id) = (expired.id, alive.id);

    let written = {
        let h = Harness::open(dir.path(), vec![expired, alive]).await;
        assert_ne!(won(&h.race().await), expired_id);
        let entry = h.channel_entry(expired_id).expect("the 401 is recorded");
        h.health.flush().expect("the table writes to redb");
        entry
        // The racer, the table and the redb handle all go out of scope here,
        // which is what lets the file be opened again below.
    };

    let db = Arc::new(redb::Database::create(dir.path().join("state.redb")).expect("redb reopens"));
    let reloaded = HealthTable::open(
        db,
        Duration::from_secs(1),
        Duration::from_secs(3_600),
        llmrouter::metrics::Metrics::new().expect("the recorder builds"),
    )
    .expect("the health table loads");

    assert_eq!(
        reloaded.get(&HealthKey::Channel(expired_id)),
        Some(written),
        "the backoff a live request wrote is still there after a restart"
    );
    // The deadline is absolute, so one long past means eligible at once rather
    // than a fresh wait on every boot.
    assert!(!reloaded.eligible(&HealthKey::Channel(expired_id), written.eligible_at - 1));
    assert!(reloaded.eligible(&HealthKey::Channel(expired_id), written.eligible_at));

    // The channel that answered is still absent, so the restart did not invent
    // rows. The three that did survive are the failed channel and the pair and
    // provider its failure escalated to.
    assert_eq!(reloaded.get(&HealthKey::Channel(alive_id)), None);
    assert_eq!(reloaded.in_backoff(0).len(), 3);
}
