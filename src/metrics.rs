//! Prometheus text for `GET /metrics`.
//!
//! Omniroute died quietly and nobody was watching. These are the counters that
//! would have shown it.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::Unit;
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    with_local_recorder,
};
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};

/// One value per status code, as `router_requests_total{outcome}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Served from the cache.
    HitCache,
    /// A channel answered. `200`.
    Success,
    /// No channel has a large enough context window. `413`.
    TooLarge,
    /// Both network test calls failed. `502`.
    NoNetwork,
    /// RSS over `memory_high_water`. `503`.
    NotAdmitted,
    /// No channel could serve it. `504`.
    NoChannel,
}

impl Outcome {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            Outcome::HitCache => "hit_cache",
            Outcome::Success => "success",
            Outcome::TooLarge => "too_large",
            Outcome::NoNetwork => "no_network",
            Outcome::NotAdmitted => "not_admitted",
            Outcome::NoChannel => "no_channel",
        }
    }

    /// The HTTP status this outcome returns.
    pub fn status(self) -> u16 {
        match self {
            Outcome::HitCache | Outcome::Success => 200,
            Outcome::TooLarge => 413,
            Outcome::NoNetwork => 502,
            Outcome::NotAdmitted => 503,
            Outcome::NoChannel => 504,
        }
    }
}

/// How one channel call ended, as `router_attempts_total{provider,model,outcome}`.
/// A provider/model pair that flips to all `http_error` overnight is dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// 2xx with non-empty content.
    Success,
    /// Any other status.
    HttpError,
    /// Passed `request_timeout`.
    Timeout,
    /// Lost the race and was torn down. Not a failure; health is untouched.
    Cancelled,
}

impl AttemptOutcome {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            AttemptOutcome::Success => "success",
            AttemptOutcome::HttpError => "http_error",
            AttemptOutcome::Timeout => "timeout",
            AttemptOutcome::Cancelled => "cancelled",
        }
    }
}

/// How one hedged copy of a channel call ended, as
/// `router_copies_total{provider,outcome}`.
///
/// `AttemptOutcome` is the trial's verdict, one per channel whatever M is.
/// This is the copy's own, M per channel. The pair that was missing before:
/// a live run showed 150 `http_error` attempts against 92 trials and no way
/// to tell which number counted what.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyOutcome {
    /// This copy produced the answer its trial returned.
    Success,
    /// Any status that is not a 2xx with non-empty content.
    HttpError,
    /// Passed `request_timeout`.
    Timeout,
    /// A sibling copy of the same channel won, so this one was torn down.
    /// The hole in the old counters: nothing recorded it at all.
    Superseded,
    /// Another channel won the race, so the whole trial was torn down.
    Cancelled,
    /// A `429` this router's own sibling copy caused. Health is untouched.
    SelfInflicted429,
    /// A 2xx whose body would not parse or decode.
    BodyError,
}

impl CopyOutcome {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            CopyOutcome::Success => "success",
            CopyOutcome::HttpError => "http_error",
            CopyOutcome::Timeout => "timeout",
            CopyOutcome::Superseded => "superseded",
            CopyOutcome::Cancelled => "cancelled",
            CopyOutcome::SelfInflicted429 => "self_inflicted_429",
            CopyOutcome::BodyError => "body_error",
        }
    }
}

/// How one channel trial ended, as `router_trials_total{provider,outcome}`.
/// One per channel the walk reached, whatever M is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrialOutcomeLabel {
    /// This channel produced the winning answer.
    Won,
    /// Every copy failed.
    Failed,
    /// Another channel won first.
    Cancelled,
}

impl TrialOutcomeLabel {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            TrialOutcomeLabel::Won => "won",
            TrialOutcomeLabel::Failed => "failed",
            TrialOutcomeLabel::Cancelled => "cancelled",
        }
    }
}

/// What a cache read did, as `router_cache_lookups_total{result}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLookup {
    /// Served from the cache.
    Hit,
    /// No entry under this key.
    MissAbsent,
    /// An entry existed and was past `ttl`.
    MissExpired,
    /// An entry existed but the caller named a different model.
    MissModelMismatch,
    /// An entry existed under an older `SCHEMA_VERSION`.
    MissSchema,
    /// `cache.enabled` is off, so no read happened.
    Disabled,
    /// redb refused the read, so there is no answer either way. Counted so
    /// the lookups total stays the number of reads: without it a redb outage
    /// leaves every cache series flat and reads as a quiet cache.
    Error,
}

impl CacheLookup {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            CacheLookup::Hit => "hit",
            CacheLookup::MissAbsent => "miss_absent",
            CacheLookup::MissExpired => "miss_expired",
            CacheLookup::MissModelMismatch => "miss_model_mismatch",
            CacheLookup::MissSchema => "miss_schema",
            CacheLookup::Disabled => "disabled",
            CacheLookup::Error => "error",
        }
    }
}

/// What a cache write did, as `router_cache_writes_total{result}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheWrite {
    /// Written: absent, expired, or higher intelligence than what was there.
    Stored,
    /// A better answer was already stored.
    SkippedLowerIntelligence,
    /// A cancelled or stalled stream. A partial answer is a wrong answer.
    SkippedPartialStream,
    /// redb refused the write.
    Error,
}

impl CacheWrite {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            CacheWrite::Stored => "stored",
            CacheWrite::SkippedLowerIntelligence => "skipped_lower_intelligence",
            CacheWrite::SkippedPartialStream => "skipped_partial_stream",
            CacheWrite::Error => "error",
        }
    }
}

/// Why a channel call failed, as `router_errors_total{provider,class}`.
///
/// Deliberately not labelled by model: this answers "what is this provider
/// doing to us", and provider times class is 14 times 13 rather than 222
/// models times 13.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// `401`. The key is dead.
    Unauthorized,
    /// `403`. The key is alive and not allowed this.
    Forbidden,
    /// `404`. The provider does not have this model.
    NotFound,
    /// `429` the provider imposed.
    RateLimited,
    /// `429` this router's own sibling copy caused.
    SelfInflicted429,
    /// `402`. The account is out of credit.
    PaymentRequired,
    /// `408`. The provider gave up waiting for the request.
    RequestTimeout,
    /// `400`, `413`, `422`, or any other 4xx. Usually the request's fault,
    /// not the channel's.
    OtherClientError,
    /// Any 5xx.
    ServerError,
    /// Passed `request_timeout`.
    Timeout,
    /// The connection never opened.
    Connect,
    /// A stream went quiet for `stream_stall_timeout`.
    StreamStalled,
    /// The provider reported a failure inside an otherwise healthy stream, as
    /// an error frame part way through an answer.
    StreamError,
    /// The provider closed the body part way through an answer. The transport
    /// saw a clean end of stream and the decoder is the first thing that knows
    /// there was no terminal event.
    StreamTruncated,
    /// The socket itself broke part way through an answer. Distinct from
    /// `connect`, which is a connection that never opened, and from
    /// `stream_truncated`, which is a clean close with the answer unfinished.
    /// Nobody said anything and the bytes stopped.
    StreamTransport,
    /// The socket broke after a 2xx but before a single token arrived, so the
    /// answer never started. Distinct from `connect`, where the socket never
    /// opened, and from `stream_transport`, where it broke part way through
    /// an answer that had begun.
    ///
    /// It has its own class because it stopped being rare: on a live run it
    /// was 70 of 140 failures, the largest group on the page, and the
    /// catch-all it shared with two other cases could not say so.
    PrefillTransport,
    /// A 2xx whose body was not JSON.
    BodyUnparseable,
    /// A 2xx whose JSON did not decode to a response.
    BodyUndecodable,
    /// A 2xx that never produced an answer: no content, or an error event
    /// where the opening of the answer should be. It has its own class for
    /// the same reason `prefill_transport` does: on the same live run that
    /// earned `prefill_transport` its class, this was the real content of the
    /// `other` bin, hidden behind a name that was meant for the rare
    /// leftover.
    NotAnAnswer,
    /// Anything else a class list does not name.
    Other,
}

impl ErrorClass {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            ErrorClass::Unauthorized => "unauthorized",
            ErrorClass::Forbidden => "forbidden",
            ErrorClass::NotFound => "not_found",
            ErrorClass::RateLimited => "rate_limited",
            ErrorClass::SelfInflicted429 => "self_inflicted_429",
            ErrorClass::PaymentRequired => "payment_required",
            ErrorClass::RequestTimeout => "request_timeout",
            ErrorClass::OtherClientError => "other_client_error",
            ErrorClass::ServerError => "server_error",
            ErrorClass::Timeout => "timeout",
            ErrorClass::Connect => "connect",
            ErrorClass::StreamStalled => "stream_stalled",
            ErrorClass::StreamError => "stream_error",
            ErrorClass::StreamTruncated => "stream_truncated",
            ErrorClass::StreamTransport => "stream_transport",
            ErrorClass::PrefillTransport => "prefill_transport",
            ErrorClass::BodyUnparseable => "body_unparseable",
            ErrorClass::BodyUndecodable => "body_undecodable",
            ErrorClass::NotAnAnswer => "not_an_answer",
            ErrorClass::Other => "other",
        }
    }

    /// The class an HTTP status falls into. `self_inflicted` separates a
    /// `429` the router caused from one the provider imposed, because only
    /// the second is the provider telling us to slow down.
    pub fn from_status(status: u16, self_inflicted: bool) -> Self {
        match status {
            401 => ErrorClass::Unauthorized,
            403 => ErrorClass::Forbidden,
            402 => ErrorClass::PaymentRequired,
            404 => ErrorClass::NotFound,
            408 => ErrorClass::RequestTimeout,
            429 if self_inflicted => ErrorClass::SelfInflicted429,
            429 => ErrorClass::RateLimited,
            400..=499 => ErrorClass::OtherClientError,
            500..=599 => ErrorClass::ServerError,
            _ => ErrorClass::Other,
        }
    }

    /// The backoff schedule a failure of this class puts the channel on.
    /// `other` is a call that went wrong in a way nothing names, which looks
    /// most like one that never reached an answer.
    ///
    /// `None` is a failure that does not bench the channel at all: a 4xx such
    /// as `400`, `413` or `422` is nearly always the request's fault. One bad
    /// request fails on every channel it reaches, and benching each of them
    /// would take whole keys and providers down with it. Alex's call, 22 Oct
    /// 2026.
    pub fn group(self) -> Option<crate::config::FailureGroup> {
        use crate::config::FailureGroup;
        Some(match self {
            ErrorClass::OtherClientError => return None,
            ErrorClass::Unauthorized | ErrorClass::Forbidden | ErrorClass::PaymentRequired => {
                FailureGroup::Account
            }
            ErrorClass::NotFound => FailureGroup::ModelMissing,
            ErrorClass::RateLimited | ErrorClass::SelfInflicted429 | ErrorClass::ServerError => {
                FailureGroup::Busy
            }
            ErrorClass::Connect
            | ErrorClass::Timeout
            | ErrorClass::RequestTimeout
            | ErrorClass::PrefillTransport
            | ErrorClass::Other => FailureGroup::Unreachable,
            ErrorClass::NotAnAnswer
            | ErrorClass::BodyUnparseable
            | ErrorClass::BodyUndecodable
            | ErrorClass::StreamError => FailureGroup::BadAnswer,
            ErrorClass::StreamStalled
            | ErrorClass::StreamTruncated
            | ErrorClass::StreamTransport => FailureGroup::DiedMidAnswer,
        })
    }
}

/// Which health table a backoff entry belongs to, as the `kind` label on
/// `router_backoff_entries_total`, `router_backoff_resets_total` and
/// `router_in_backoff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffKind {
    /// One provider, model and key.
    Channel,
    /// One provider and key, so one dead key does not disable its siblings.
    ProviderKey,
    /// The whole provider.
    Provider,
}

impl BackoffKind {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            BackoffKind::Channel => "channel",
            BackoffKind::ProviderKey => "provider_key",
            BackoffKind::Provider => "provider",
        }
    }
}

/// Which half of the usage block a token count came from, as the `kind` label
/// on `router_tokens_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// Sent to the provider.
    Prompt,
    /// Returned as content.
    Completion,
    /// Returned as reasoning. Billed, usually not shown, and on the live run
    /// it was 12 times the content tokens, so it is counted apart.
    Reasoning,
}

impl TokenKind {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            TokenKind::Prompt => "prompt",
            TokenKind::Completion => "completion",
            TokenKind::Reasoning => "reasoning",
        }
    }
}

/// What the hedge bought on a won trial, as
/// `router_hedge_outcome_total{provider,result}`.
///
/// The counter that was missing. `router_hedge_wins_total` says which copy
/// index won, and on its own that is a coin flip: two copies race, one wins,
/// and a live run came back 5 against 5. `router_copy_latency_seconds` cannot
/// settle it either, because a superseded copy is killed at the instant its
/// sibling wins, so its recorded latency equals the winner's by construction.
///
/// This asks the question that decides M instead: when this trial won, had
/// its siblings already failed, or were they still running?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeOutcome {
    /// A hedge copy won and the first copy had already failed. Without the
    /// hedge the whole trial would have failed and the walk would have moved
    /// to the next channel. This is M earning its keep.
    Rescued,
    /// A hedge copy won while the first copy was still running, so it beat
    /// the first copy on speed. M saved time here. How much is not knowable
    /// without letting the loser finish, which the race kills on purpose.
    Faster,
    /// The first copy won. Every other copy was a second call against the
    /// key's rate budget that changed nothing, because the copy that would
    /// have run at M=1 is the one that answered.
    Redundant,
    /// There was no hedge, because `max_per_channel` is 1.
    NoHedge,
}

impl HedgeOutcome {
    /// The label value written to `/metrics`.
    pub fn label(self) -> &'static str {
        match self {
            HedgeOutcome::Rescued => "rescued",
            HedgeOutcome::Faster => "faster",
            HedgeOutcome::Redundant => "redundant",
            HedgeOutcome::NoHedge => "no_hedge",
        }
    }
}

/// What the N-race bought for one walk that ends with an answer.
///
/// The M-hedge asks whether M copies of one channel call helped that call;
/// `router_hedge_outcome_total` answers that question. This asks the other:
/// did firing `race_width` channels at once pay, compared with trying them
/// one at a time? Two hedges, two knobs, two sets of stats.
///
/// Classified by completion order: a late-fired trial that failed before an
/// early slot won also counts as `rescued`. The per-copy lines in the log
/// carry the exact timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaceStat {
    /// The first channel fired won the walk. The race bought nothing a
    /// sequential walk would not have.
    FirstWon,
    /// A later channel won with no other trial failing first, so the race
    /// bought speed: the first channel had not answered yet.
    LaterWon,
    /// A later channel won after another trial had already failed, so a
    /// sequential walk would have burned its wait on the failure first.
    Rescued,
}

impl RaceStat {
    /// The label value written to `/metrics` under `result`.
    pub fn label(self) -> &'static str {
        match self {
            RaceStat::FirstWon => "first_won",
            RaceStat::LaterWon => "later_won",
            RaceStat::Rescued => "rescued",
        }
    }
}
/// Histogram buckets for `router_latency_seconds`, in seconds.
///
/// One metric name carries both stages, so Prometheus forces one bucket set
/// across both. It has to cover router overhead, which the targets put under
/// 10ms, and upstream time, which on a long prompt runs to tens of minutes.
/// That is why the low end goes down to half a millisecond and the top end
/// goes up to half an hour.
const LATENCY_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0, 120.0, 300.0, 600.0, 1800.0,
];

/// Histogram buckets for `router_ttft_seconds`, in seconds.
///
/// Same shape as `LATENCY_BUCKETS`: on the live run the median time to first
/// token was 730 seconds, so the top end has to reach half an hour, and a
/// cache hit lands at 11 milliseconds, so the bottom end has to resolve
/// single-digit milliseconds.
const TTFT_BUCKETS: &[f64] = LATENCY_BUCKETS;

/// Histogram buckets for `router_copy_latency_seconds`, in seconds.
///
/// One copy of one channel call. A failing nvidia call took 40 to 120
/// seconds on the live run, which is the number this histogram exists to
/// show, so the buckets are dense through that range.
const COPY_LATENCY_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 300.0,
    600.0, 1800.0, 3600.0,
];

/// Histogram buckets for `router_trials_per_request` and
/// `router_copies_per_request`.
///
/// The live run needed 8 to 11 channel trials per request. The design's worst
/// case walks every channel, which was 4,807 on the last live config, so the
/// top bucket has to be above that.
const WALK_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0,
];

/// Histogram buckets for `router_output_tokens_per_second`.
const TPS_BUCKETS: &[f64] = &[
    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0,
];

/// Histogram buckets for `router_cache_age_seconds`. The ttl is 7 days, so
/// the top bucket sits just past it.
const CACHE_AGE_BUCKETS: &[f64] = &[
    1.0, 10.0, 60.0, 300.0, 3600.0, 21600.0, 86400.0, 259200.0, 604800.0,
];

/// Histogram buckets for `router_backoff_seconds`. The backoff doubles from
/// 1 second to the 1 hour cap, so the buckets follow the same doubling.
const BACKOFF_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 3600.0,
];

/// Every outcome, so the counters can be seeded at zero.
const ALL_OUTCOMES: &[Outcome] = &[
    Outcome::HitCache,
    Outcome::Success,
    Outcome::TooLarge,
    Outcome::NoNetwork,
    Outcome::NotAdmitted,
    Outcome::NoChannel,
];

/// Every cache lookup result, so the counters can be seeded at zero.
const ALL_CACHE_LOOKUPS: &[CacheLookup] = &[
    CacheLookup::Hit,
    CacheLookup::MissAbsent,
    CacheLookup::MissExpired,
    CacheLookup::MissModelMismatch,
    CacheLookup::MissSchema,
    CacheLookup::Disabled,
    CacheLookup::Error,
];

/// Every cache write result, so the counters can be seeded at zero.
const ALL_CACHE_WRITES: &[CacheWrite] = &[
    CacheWrite::Stored,
    CacheWrite::SkippedLowerIntelligence,
    CacheWrite::SkippedPartialStream,
    CacheWrite::Error,
];

/// Every backoff kind, so the counters and gauges can be seeded at zero.
const ALL_BACKOFF_KINDS: &[BackoffKind] = &[
    BackoffKind::Channel,
    BackoffKind::ProviderKey,
    BackoffKind::Provider,
];

/// Every race stat, so the counter is seeded at zero.
const ALL_RACE_STATS: &[RaceStat] = &[RaceStat::FirstWon, RaceStat::LaterWon, RaceStat::Rescued];

/// The two values of the `state` label on `router_channels_total`.
const CHANNEL_STATES: &[&str] = &["eligible", "in_backoff"];

/// Copy indexes that get their own series on `router_hedge_wins_total`.
/// Anything higher collapses into the last one, so M can be raised without
/// the label set growing with it.
const HEDGE_INDEX_LABELS: &[&str] = &["0", "1", "2", "3+"];

/// Label value for the part of `router_latency_seconds` spent waiting on the
/// winning channel.
const STAGE_UPSTREAM: &str = "upstream";

/// Label value for the part of `router_latency_seconds` the router itself
/// added. This is the one that proves the latency target.
const STAGE_ROUTER: &str = "router";

/// The recorder and its render handle.
///
/// The recorder is owned, not installed as the process-wide one. Nothing else
/// in the crate writes through the `metrics` macros, so a global recorder
/// would buy nothing and would cap the process at one `Metrics`, which is what
/// made the counters untestable: every test shared one set of numbers.
#[derive(Debug, Clone)]
pub struct Metrics {
    recorder: Arc<PrometheusRecorder>,
    handle: PrometheusHandle,
    /// Counter values a previous run saved, applied when `render` runs.
    counter_offsets: Arc<Mutex<HashMap<String, u64>>>,
}

impl Metrics {
    /// Builds the Prometheus recorder and describes every metric, so the
    /// names appear on `/metrics` from the first scrape even at zero.
    pub fn new() -> crate::Result<Self> {
        let mut builder = PrometheusBuilder::new();
        for (name, buckets) in [
            ("router_latency_seconds", LATENCY_BUCKETS),
            ("router_ttft_seconds", TTFT_BUCKETS),
            ("router_copy_latency_seconds", COPY_LATENCY_BUCKETS),
            ("router_trials_per_request", WALK_BUCKETS),
            ("router_copies_per_request", WALK_BUCKETS),
            ("router_output_tokens_per_second", TPS_BUCKETS),
            ("router_cache_age_seconds", CACHE_AGE_BUCKETS),
            ("router_backoff_seconds", BACKOFF_BUCKETS),
        ] {
            builder = builder
                .set_buckets_for_metric(Matcher::Full(name.to_owned()), buckets)
                .map_err(|e| crate::RouterError::Config(format!("metrics buckets {name}: {e}")))?;
        }
        let recorder = builder.build_recorder();
        let metrics = Self {
            handle: recorder.handle(),
            counter_offsets: Arc::new(Mutex::new(HashMap::new())),
            recorder: Arc::new(recorder),
        };

        metrics.on_recorder(|| {
            describe_everything();
            seed_zero();
        });

        Ok(metrics)
    }

    /// Runs `f` with this instance's recorder in scope, so the `metrics`
    /// macros write here rather than to whatever is installed globally.
    fn on_recorder<T>(&self, f: impl FnOnce() -> T) -> T {
        with_local_recorder(self.recorder.as_ref(), f)
    }

    /// One client request finished. `router_requests_total{outcome}`.
    pub fn record_request(&self, outcome: Outcome) {
        self.on_recorder(|| {
            counter!("router_requests_total", "outcome" => outcome.label()).increment(1);
        });
    }

    /// One channel call finished.
    /// `router_attempts_total{provider,model,outcome}`.
    ///
    /// Labelled by provider and model, not by channel id. A channel id is per
    /// provider/model/key, so with 4,807 channels and four outcomes the label
    /// set runs to tens of thousands of series. Provider and model give the
    /// same diagnosis at a fraction of that, and the per-key detail stays in
    /// the request log where it is not multiplied by a scrape interval.
    pub fn record_attempt(&self, provider: &str, model_id: &str, outcome: AttemptOutcome) {
        self.on_recorder(|| {
            counter!(
                "router_attempts_total",
                "provider" => provider.to_owned(),
                "model" => model_id.to_owned(),
                "outcome" => outcome.label(),
            )
            .increment(1);
        });
    }

    /// One hedged copy of one channel call finished.
    /// `router_copies_total{provider,outcome}`.
    ///
    /// Called M times per channel trial, against [`record_attempt`]'s once.
    /// `Superseded` is the one that was missing: before this, a copy whose
    /// sibling won was dropped without a trace, so no counter could say
    /// whether hedging ever won a race it would otherwise have lost.
    ///
    /// [`record_attempt`]: Self::record_attempt
    pub fn record_copy(&self, provider: &str, outcome: CopyOutcome) {
        self.on_recorder(|| {
            counter!(
                "router_copies_total",
                "provider" => provider.to_owned(),
                "outcome" => outcome.label(),
            )
            .increment(1);
        });
    }

    /// How long one copy ran before it won or failed.
    /// `router_copy_latency_seconds{outcome}`.
    ///
    /// The number that explains a slow walk. On the live run the failures
    /// were taking 40 to 120 seconds each, so 9 channels in sequence at N=2
    /// came to 12 minutes, and nothing on `/metrics` said so.
    pub fn record_copy_latency(&self, outcome: CopyOutcome, elapsed: Duration) {
        self.on_recorder(|| {
            histogram!("router_copy_latency_seconds", "outcome" => outcome.label())
                .record(elapsed.as_secs_f64());
        });
    }

    /// Which copy of a channel trial produced the answer.
    /// `router_hedge_wins_total{provider,copy}`.
    ///
    /// `copy` is the index the copy was fired at, so `0` is the first and
    /// anything above it is a hedge that beat it. Wins on `0` only means M is
    /// costing a second call per channel and buying nothing.
    pub fn record_hedge_win(&self, provider: &str, copy_index: usize) {
        let label = HEDGE_INDEX_LABELS
            .get(copy_index)
            .copied()
            .unwrap_or_else(|| HEDGE_INDEX_LABELS[HEDGE_INDEX_LABELS.len() - 1]);
        self.on_recorder(|| {
            counter!(
                "router_hedge_wins_total",
                "provider" => provider.to_owned(),
                "copy" => label,
            )
            .increment(1);
        });
    }

    /// What the hedge bought on one won trial.
    /// `router_hedge_outcome_total{provider,result}`.
    ///
    /// Read this against `router_copies_total{outcome="self_inflicted_429"}`,
    /// which is what M costs. Mostly `redundant` with self-inflicted 429s
    /// climbing means `max_per_channel` should come down, or
    /// `hedge_delay_ms` should come up off 0.
    pub fn record_hedge_outcome(&self, provider: &str, result: HedgeOutcome) {
        self.on_recorder(|| {
            counter!(
                "router_hedge_outcome_total",
                "provider" => provider.to_owned(),
                "result" => result.label(),
            )
            .increment(1);
        });
    }

    /// What the N-race bought for one walk that ends with an answer.
    /// `router_race_outcome_total{result}`.
    ///
    /// Read beside the M-hedge numbers: `first_won` climbing means the race
    /// mostly fires N calls for one answer, which is what the race costs.
    /// `later_won` and `rescued` climbing means the race pays for itself.
    pub fn record_race_outcome(&self, result: RaceStat) {
        self.on_recorder(|| {
            counter!("router_race_outcome_total", "result" => result.label()).increment(1);
        });
    }

    /// Loads the counters a previous run saved, as Prometheus-form series
    /// keys (`name{k="v"}`) and their values. The values ride on top of
    /// whatever the fresh recorder has once `/metrics` renders.
    ///
    /// Only counters are restored: a histogram cannot be replayed faithfully
    /// into the recorder, and the history backend already keeps percentiles
    /// over time. Gauges are current readings, not history.
    pub fn set_counter_offsets(&self, saved: &[(String, u64)]) {
        let mut map = self.counter_offsets.lock().expect("counter offsets");
        map.clear();
        for (key, value) in saved {
            if *value > 0 {
                map.insert(key.clone(), *value);
            }
        }
    }

    /// One channel trial finished. `router_trials_total{provider,outcome}`.
    /// One per channel the walk reached, whatever M is.
    pub fn record_trial(&self, provider: &str, outcome: TrialOutcomeLabel) {
        self.on_recorder(|| {
            counter!(
                "router_trials_total",
                "provider" => provider.to_owned(),
                "outcome" => outcome.label(),
            )
            .increment(1);
        });
    }

    /// How deep one client request had to walk.
    /// `router_trials_per_request` and `router_copies_per_request`.
    ///
    /// The gap between the two is what M cost. The live run recorded 9.2
    /// trials and 18.4 copies per request, which is the whole of M=2's
    /// expense sitting next to a hedge-win count of zero on copy 1.
    pub fn record_walk(&self, trials: u32, copies: u32) {
        self.on_recorder(|| {
            histogram!("router_trials_per_request").record(f64::from(trials));
            histogram!("router_copies_per_request").record(f64::from(copies));
        });
    }

    /// Channel trials in flight right now. `router_trials_inflight`.
    ///
    /// Across every client request at once, not per request: one `Racer` is
    /// shared by all of them, so a per-walk reading would have each walk
    /// publishing its own N over the others and the gauge would hold its last
    /// non-zero value forever once traffic stopped. Against `race_width`
    /// times the concurrent requests, this is what says whether N binds.
    pub fn set_trials_inflight(&self, trials: u64) {
        self.on_recorder(|| gauge!("router_trials_inflight").set(trials as f64));
    }

    /// One cache read. `router_cache_lookups_total{result}`.
    ///
    /// The miss reasons matter more than the hit rate: a cache missing on
    /// `miss_model_mismatch` all day is a client naming models, not a cache
    /// that is too small.
    pub fn record_cache_lookup(&self, result: CacheLookup) {
        self.on_recorder(|| {
            counter!("router_cache_lookups_total", "result" => result.label()).increment(1);
        });
    }

    /// How old the entry a hit served was. `router_cache_age_seconds`.
    pub fn record_cache_age(&self, age: Duration) {
        self.on_recorder(|| {
            histogram!("router_cache_age_seconds").record(age.as_secs_f64());
        });
    }

    /// One cache write. `router_cache_writes_total{result}`.
    pub fn record_cache_write(&self, result: CacheWrite) {
        self.on_recorder(|| {
            counter!("router_cache_writes_total", "result" => result.label()).increment(1);
        });
    }

    /// `router_cache_bytes`, beside `router_cache_entries`. Entries alone
    /// cannot say whether the cache is near `max_bytes`.
    pub fn set_cache_bytes(&self, bytes: u64) {
        self.on_recorder(|| gauge!("router_cache_bytes").set(bytes as f64));
    }

    /// Tokens a finished request moved. `router_tokens_total{provider,kind}`.
    ///
    /// Reasoning is counted apart from completion because it is billed and
    /// usually not shown: the live run returned 42 content tokens against 529
    /// total, and a throughput figure that mixes the two says nothing useful
    /// about either.
    pub fn record_tokens(&self, provider: &str, kind: TokenKind, count: u64) {
        if count == 0 {
            return;
        }
        self.on_recorder(|| {
            counter!(
                "router_tokens_total",
                "provider" => provider.to_owned(),
                "kind" => kind.label(),
            )
            .increment(count);
        });
    }

    /// Output tokens per second for one finished request.
    /// `router_output_tokens_per_second`.
    pub fn record_output_rate(&self, tokens_per_second: f64) {
        self.on_recorder(|| {
            histogram!("router_output_tokens_per_second").record(tokens_per_second);
        });
    }

    /// Time from accepting the client request to its first content token.
    /// `router_ttft_seconds`.
    ///
    /// Separate from `router_latency_seconds` because the two diverge: a
    /// streamed answer can take 13 minutes to start and 1.3 seconds to
    /// finish, and only this metric shows which half the client waited on.
    pub fn record_ttft(&self, elapsed: Duration) {
        self.on_recorder(|| {
            histogram!("router_ttft_seconds").record(elapsed.as_secs_f64());
        });
    }

    /// Why one channel call failed. `router_errors_total{provider,class}`.
    ///
    /// Beside `router_attempts_total{outcome="http_error"}`, which says a
    /// call failed but not why. A provider on `unauthorized` needs a new key;
    /// the same provider on `rate_limited` needs fewer calls; on
    /// `self_inflicted_429` it needs a smaller M. Three different jobs behind
    /// one number before this.
    pub fn record_error(&self, provider: &str, class: ErrorClass) {
        self.on_recorder(|| {
            counter!(
                "router_errors_total",
                "provider" => provider.to_owned(),
                "class" => class.label(),
            )
            .increment(1);
        });
    }

    /// A channel, key or provider went into backoff.
    /// `router_backoff_entries_total{kind}`, plus the wait that was applied
    /// on `router_backoff_seconds`.
    pub fn record_backoff(&self, kind: BackoffKind, wait: Duration) {
        self.on_recorder(|| {
            counter!("router_backoff_entries_total", "kind" => kind.label()).increment(1);
            histogram!("router_backoff_seconds").record(wait.as_secs_f64());
        });
    }

    /// A channel, key or provider answered, so its counter reset to zero.
    /// `router_backoff_resets_total{kind}`.
    ///
    /// Against `router_backoff_entries_total` this is the churn rate. Entries
    /// climbing with resets flat is a list going dead; the two climbing
    /// together is a provider flapping.
    pub fn record_backoff_reset(&self, kind: BackoffKind) {
        self.on_recorder(|| {
            counter!("router_backoff_resets_total", "kind" => kind.label()).increment(1);
        });
    }

    /// How many entries of each kind are in backoff right now.
    /// `router_in_backoff{kind}`.
    pub fn set_in_backoff(&self, kind: BackoffKind, count: u64) {
        self.on_recorder(|| {
            gauge!("router_in_backoff", "kind" => kind.label()).set(count as f64);
        });
    }

    /// The channel list split by state. `router_channels_total{state}`.
    ///
    /// `eligible` falling while the total holds is the list going dead under
    /// backoff, which is the shape that preceded a `504`.
    pub fn set_channel_states(&self, eligible: u64, in_backoff: u64) {
        self.on_recorder(|| {
            gauge!("router_channels_total", "state" => "eligible").set(eligible as f64);
            gauge!("router_channels_total", "state" => "in_backoff").set(in_backoff as f64);
        });
    }

    /// `router_latency_seconds`, split two ways: the time the winning channel
    /// took, and the time the router itself added. The second one is what
    /// proves the latency target.
    pub fn record_latency(&self, upstream: Duration, router_overhead: Duration) {
        self.on_recorder(|| {
            histogram!("router_latency_seconds", "stage" => STAGE_UPSTREAM)
                .record(upstream.as_secs_f64());
            histogram!("router_latency_seconds", "stage" => STAGE_ROUTER)
                .record(router_overhead.as_secs_f64());
        });
    }

    /// `router_rss_bytes`. The one graph to watch against `memory_high_water`.
    pub fn set_rss(&self, bytes: u64) {
        self.on_recorder(|| gauge!("router_rss_bytes").set(bytes as f64));
    }

    /// `router_cache_entries`. A flat line proves memory growth is not
    /// repeating.
    pub fn set_cache_entries(&self, entries: u64) {
        self.on_recorder(|| gauge!("router_cache_entries").set(entries as f64));
    }

    /// The Prometheus text body for `GET /metrics`, with the counters a
    /// previous run saved added back on.
    pub fn render(&self) -> String {
        let offsets = self.counter_offsets.lock().expect("counter offsets");
        if offsets.is_empty() {
            return self.handle.render();
        }
        apply_counter_offsets(&self.handle.render(), &offsets)
    }
}

/// HELP and TYPE lines for every metric the router writes.
fn describe_everything() {
    describe_counter!(
        "router_requests_total",
        "Client requests finished, by outcome: one outcome per status code."
    );
    describe_counter!(
        "router_attempts_total",
        "Channel calls finished, by provider, model and outcome."
    );
    describe_histogram!(
        "router_latency_seconds",
        Unit::Seconds,
        "Request time in seconds, split into upstream time and router overhead."
    );
    describe_gauge!(
        "router_rss_bytes",
        Unit::Bytes,
        "Resident set size in bytes, to watch against memory_high_water."
    );
    describe_gauge!(
        "router_cache_entries",
        "Entries currently in the response cache."
    );
    describe_counter!(
        "router_copies_total",
        "Hedged copies of channel calls finished, by provider and outcome. M per trial."
    );
    describe_histogram!(
        "router_copy_latency_seconds",
        Unit::Seconds,
        "How long one copy of a channel call ran before it won, failed or was \
         stopped. A superseded or cancelled copy is stopped at the instant \
         the race ends, so those two outcomes measure when the race ended, \
         not how long the copy would have taken."
    );
    describe_counter!(
        "router_hedge_wins_total",
        "Channel trials won, by provider and the index of the copy that won. \
         On its own this is a coin flip and says nothing about whether \
         hedging helped: read router_hedge_outcome_total for that."
    );
    describe_counter!(
        "router_hedge_outcome_total",
        "Won trials by what the hedge bought: rescued when a hedge copy won \
         after the first copy failed, faster when a hedge copy beat a first \
         copy still running, redundant when the first copy won anyway."
    );
    describe_counter!(
        "router_race_outcome_total",
        "Walks that ended with an answer, by what the N-way race bought: \
         first_won when the first channel fired won, later_won when a later \
         channel beat the first, rescued when a later channel won after \
         another trial had failed."
    );
    describe_counter!(
        "router_trials_total",
        "Channel trials finished, by provider and outcome. One per channel, whatever M is. \
         `won` means the trial produced a usable answer, which under the `tps` hedge strategy \
         more than one channel can do for a single client request."
    );
    describe_histogram!(
        "router_trials_per_request",
        "Channel trials one client request needed."
    );
    describe_histogram!(
        "router_copies_per_request",
        "Hedged copies one client request fired."
    );
    describe_gauge!(
        "router_trials_inflight",
        "Channel trials running right now, across every client request at once."
    );
    describe_counter!(
        "router_cache_lookups_total",
        "Cache reads, by result: a hit, or the reason it missed."
    );
    describe_histogram!(
        "router_cache_age_seconds",
        Unit::Seconds,
        "Age of the entry a cache hit served."
    );
    describe_counter!(
        "router_cache_writes_total",
        "Cache writes, by result: stored, or the reason it was not."
    );
    describe_gauge!(
        "router_cache_bytes",
        Unit::Bytes,
        "Bytes the response cache holds, to watch against cache.max_bytes."
    );
    describe_counter!(
        "router_tokens_total",
        "Tokens moved by finished requests, by provider and kind."
    );
    describe_histogram!(
        "router_output_tokens_per_second",
        "Output tokens per second of the winning stream over its own generation \
         window, not the whole request."
    );
    describe_histogram!(
        "router_ttft_seconds",
        Unit::Seconds,
        "Time from accepting a client request to its first content token."
    );
    describe_counter!(
        "router_errors_total",
        "Failed channel calls, by provider and the class of the failure."
    );
    describe_counter!(
        "router_backoff_entries_total",
        "Channels, keys and providers put into backoff."
    );
    describe_counter!(
        "router_backoff_resets_total",
        "Channels, keys and providers whose retry counter reset on a success."
    );
    describe_histogram!(
        "router_backoff_seconds",
        Unit::Seconds,
        "The wait applied when something went into backoff."
    );
    describe_gauge!(
        "router_in_backoff",
        "Channels, keys and providers in backoff right now."
    );
    describe_gauge!(
        "router_channels_total",
        "The channel list split into eligible and in backoff."
    );
}

/// Touches every series whose labels are known up front, so an alert on a
/// never-seen outcome sits on a zero rather than on missing data until the
/// first failure. Providers and models are not known at startup, so
/// `router_attempts_total` cannot be seeded and appears on first use.
fn seed_zero() {
    for outcome in ALL_OUTCOMES {
        counter!("router_requests_total", "outcome" => outcome.label()).increment(0);
    }
    for result in ALL_CACHE_LOOKUPS {
        counter!("router_cache_lookups_total", "result" => result.label()).increment(0);
    }
    for result in ALL_CACHE_WRITES {
        counter!("router_cache_writes_total", "result" => result.label()).increment(0);
    }
    for result in ALL_RACE_STATS {
        counter!("router_race_outcome_total", "result" => result.label()).increment(0);
    }
    for kind in ALL_BACKOFF_KINDS {
        counter!("router_backoff_entries_total", "kind" => kind.label()).increment(0);
        counter!("router_backoff_resets_total", "kind" => kind.label()).increment(0);
        gauge!("router_in_backoff", "kind" => kind.label()).set(0.0);
    }
    for state in CHANNEL_STATES {
        gauge!("router_channels_total", "state" => *state).set(0.0);
    }
    gauge!("router_rss_bytes").set(0.0);
    gauge!("router_cache_entries").set(0.0);
    gauge!("router_cache_bytes").set(0.0);
    gauge!("router_trials_inflight").set(0.0);
}

/// Splits a Prometheus-form series key `name{k="v",k2="v2"}` into the metric
/// name and its label pairs. A key without a brace part is name-only.
fn parse_series_key(key: &str) -> Option<(String, Vec<(String, String)>)> {
    let (name, labels_part) = match key.split_once('{') {
        Some((name, rest)) => (name, rest.strip_suffix('}')?),
        None => (key, ""),
    };
    let mut labels = Vec::new();
    if !labels_part.is_empty() {
        for pair in labels_part.split(',') {
            let (k, v) = pair.split_once('=')?;
            let v = v.strip_prefix('"')?.strip_suffix('"')?;
            labels.push((k.to_owned(), v.to_owned()));
        }
    }
    Some((name.to_owned(), labels))
}

/// The stable key of one series: `name{k="v",...}` with the label pairs
/// sorted by key, so the same series makes the same key on both sides of a
/// save and restore.
pub(crate) fn build_series_key(name: &str, labels: &[(String, String)]) -> String {
    let mut sorted = labels.to_vec();
    sorted.sort();
    if sorted.is_empty() {
        return name.to_owned();
    }
    let pairs: Vec<String> = sorted.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
    format!("{}{{{}}}", name, pairs.join(","))
}

/// Adds the counters a previous run saved onto a fresh scrape. A line whose
/// series the save carries gains the saved value; a series the save carries
/// but the scrape does not is appended at the end.
fn apply_counter_offsets(scrape: &str, offsets: &HashMap<String, u64>) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for line in scrape.lines() {
        if line.starts_with('#') || line.is_empty() {
            out.push(line.to_owned());
            continue;
        }
        let Some((key_part, value_part)) = line.rsplit_once(' ') else {
            out.push(line.to_owned());
            continue;
        };
        let norm = match parse_series_key(key_part) {
            Some((name, pairs)) => build_series_key(&name, &pairs),
            None => key_part.to_owned(),
        };
        match offsets.get(&norm) {
            Some(offset) => {
                seen.insert(norm);
                let value = value_part.parse::<f64>().unwrap_or(0.0) + *offset as f64;
                out.push(format!("{key_part} {}", format_value(value)));
            }
            None => out.push(line.to_owned()),
        }
    }
    for (key, value) in offsets {
        if !seen.contains(key) {
            out.push(format!("{key} {value}"));
        }
    }
    let mut joined = out.join("\n");
    joined.push('\n');
    joined
}

/// Renders a counter value the way the exporter would: a whole number with no
/// decimal part.
fn format_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as u64)
    } else {
        format!("{value}")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_parse::{Sample, Scrape, Value};

    /// A fresh set of counters. Each `Metrics` owns its recorder, so tests in
    /// one process do not share numbers.
    fn local() -> Metrics {
        Metrics::new().expect("the recorder builds")
    }

    #[test]
    fn series_keys_survive_a_save_and_restore() {
        let saved: Vec<(String, u64)> = vec![
            (
                build_series_key(
                    "router_attempts_total",
                    &[
                        ("provider".to_owned(), "nvidia".to_owned()),
                        ("model".to_owned(), "m".to_owned()),
                    ],
                ),
                7,
            ),
            ("router_requests_total".to_owned(), 3),
        ];
        let metrics = local();
        metrics.set_counter_offsets(&saved);

        let text = metrics.render();
        // The restored series the recorder has not touched yet land at the
        // end of the body with their saved values.
        assert!(text.contains("router_attempts_total{model=\"m\",provider=\"nvidia\"} 7"));
        assert!(text.contains("router_requests_total 3"));
    }

    #[test]
    fn offsets_add_onto_a_recorder_that_restarted_at_zero() {
        let metrics = local();
        let saved: Vec<(String, u64)> = vec![(
            build_series_key(
                "router_requests_total",
                &[("outcome".to_owned(), "success".to_owned())],
            ),
            9,
        )];
        metrics.set_counter_offsets(&saved);
        metrics.record_request(Outcome::Success);

        let text = metrics.render();
        // The seed zero plus the saved 9 plus the one new request.
        assert!(text.contains("router_requests_total{outcome=\"success\"} 10"));
    }

    #[test]
    fn every_outcome_maps_to_one_status_and_one_label() {
        // The design names six outcomes over five codes: hit_cache and success
        // share 200, the other four are one apiece.
        let labels: Vec<&str> = ALL_OUTCOMES.iter().map(|o| o.label()).collect();
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "labels must be distinct");

        assert_eq!(Outcome::HitCache.status(), 200);
        assert_eq!(Outcome::Success.status(), 200);
        assert_eq!(Outcome::TooLarge.status(), 413);
        assert_eq!(Outcome::NoNetwork.status(), 502);
        assert_eq!(Outcome::NotAdmitted.status(), 503);
        assert_eq!(Outcome::NoChannel.status(), 504);
    }

    #[test]
    fn design_names_only_real_request_outcomes() {
        // A dashboard built from DESIGN.md on an outcome this module never
        // emits sits on missing data forever, so every label the design pairs
        // with router_requests_total must be one we seed.
        let labels: Vec<&str> = ALL_OUTCOMES.iter().map(|o| o.label()).collect();
        let design = include_str!("../DESIGN.md");
        // A prose paragraph is one unit; a table row is its own unit, so the
        // other metrics in the same table do not count.
        let units = design.split("\n\n").flat_map(|p| {
            if p.starts_with('|') {
                p.lines().collect()
            } else {
                vec![p]
            }
        });
        for para in units.filter(|p| p.contains("router_requests_total{outcome}")) {
            for name in para.split('`').skip(1).step_by(2) {
                if name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                    assert!(
                        labels.contains(&name),
                        "DESIGN.md names outcome `{name}`:\n{para}"
                    );
                }
            }
        }
        // A table with two header rows renders wrongly.
        let lines: Vec<&str> = design.lines().collect();
        for (i, pair) in lines.windows(3).enumerate() {
            let sep = |l: &str| l.starts_with("|---");
            assert!(
                !(sep(pair[0]) && sep(pair[2])),
                "two table headers at DESIGN.md line {}",
                i + 1
            );
        }
    }

    #[test]
    fn attempt_labels_match_the_design() {
        assert_eq!(AttemptOutcome::Success.label(), "success");
        assert_eq!(AttemptOutcome::HttpError.label(), "http_error");
        assert_eq!(AttemptOutcome::Timeout.label(), "timeout");
        assert_eq!(AttemptOutcome::Cancelled.label(), "cancelled");
    }

    #[test]
    fn buckets_rise_and_cover_both_ends() {
        assert!(LATENCY_BUCKETS.windows(2).all(|w| w[0] < w[1]));
        // Under the 10ms added-latency target, so overhead lands in a bucket
        // rather than piling into the first one.
        assert!(LATENCY_BUCKETS[0] < 0.01);
        // A 2,067s request still has buckets above it in the tail.
        assert!(*LATENCY_BUCKETS.last().expect("non-empty") >= 1800.0);
    }

    /// The scrape read into samples, so a test compares exact values rather
    /// than matching a prefix of the text, where `2` would also match `20`.
    fn scrape(metrics: &Metrics) -> Vec<Sample> {
        let text = metrics.render();
        Scrape::parse(text.lines().map(|l| Ok(l.to_owned())))
            .expect("the scrape parses")
            .samples
    }

    /// The one sample of `metric` whose labels are exactly `labels`.
    fn sample<'a>(samples: &'a [Sample], metric: &str, labels: &[(&str, &str)]) -> &'a Value {
        let found: Vec<&Sample> = samples
            .iter()
            .filter(|s| {
                s.metric == metric
                    && s.labels.len() == labels.len()
                    && labels.iter().all(|(k, v)| s.labels.get(k) == Some(*v))
            })
            .collect();
        assert_eq!(found.len(), 1, "{metric} {labels:?} in {samples:#?}");
        &found[0].value
    }

    /// A counter, gauge or untyped value.
    fn value(samples: &[Sample], metric: &str, labels: &[(&str, &str)]) -> f64 {
        match sample(samples, metric, labels) {
            Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => *v,
            other => panic!("{metric} is not a single value: {other:?}"),
        }
    }

    /// The count in one bucket of `router_latency_seconds`.
    fn bucket(samples: &[Sample], stage: &str, le: f64) -> f64 {
        let Value::Histogram(counts) =
            sample(samples, "router_latency_seconds", &[("stage", stage)])
        else {
            panic!("router_latency_seconds is not a histogram");
        };
        counts
            .iter()
            .find(|c| c.less_than == le)
            .unwrap_or_else(|| panic!("no le={le} bucket for {stage}"))
            .count
    }

    #[test]
    fn first_scrape_carries_every_request_outcome_at_zero() {
        let s = scrape(&local());

        for outcome in ALL_OUTCOMES {
            let labels = [("outcome", outcome.label())];
            assert_eq!(value(&s, "router_requests_total", &labels), 0.0);
        }
        assert_eq!(value(&s, "router_rss_bytes", &[]), 0.0);
        assert_eq!(value(&s, "router_cache_entries", &[]), 0.0);
    }

    #[test]
    fn counters_add_up_per_label() {
        let metrics = local();
        metrics.record_request(Outcome::Success);
        metrics.record_request(Outcome::Success);
        metrics.record_request(Outcome::NoChannel);
        metrics.record_attempt("groq", "llama-4", AttemptOutcome::Timeout);
        metrics.record_attempt("groq", "llama-4", AttemptOutcome::Cancelled);
        let s = scrape(&metrics);

        let requests = |outcome| value(&s, "router_requests_total", &[("outcome", outcome)]);
        assert_eq!(requests("success"), 2.0);
        assert_eq!(requests("no_channel"), 1.0);
        // The label set must match exactly, so a stray `channel` label fails.
        for outcome in ["timeout", "cancelled"] {
            let labels = [
                ("provider", "groq"),
                ("model", "llama-4"),
                ("outcome", outcome),
            ];
            assert_eq!(value(&s, "router_attempts_total", &labels), 1.0);
        }
    }

    #[test]
    fn a_count_that_only_starts_with_the_expected_digits_does_not_match() {
        // Twenty successes render as `} 20`, which a substring check for `} 2`
        // accepted. The parsed value must not.
        let metrics = local();
        for _ in 0..20 {
            metrics.record_request(Outcome::Success);
        }
        let s = scrape(&metrics);
        assert_ne!(
            value(&s, "router_requests_total", &[("outcome", "success")]),
            2.0
        );
    }

    #[test]
    fn latency_splits_into_two_stages() {
        let metrics = local();
        metrics.record_latency(Duration::from_millis(1500), Duration::from_micros(800));
        let s = scrape(&metrics);

        // Router overhead of 0.8ms sits above the 0.0005 bucket and inside 0.001.
        assert_eq!(bucket(&s, "router", 0.001), 1.0);
        assert_eq!(bucket(&s, "router", 0.0005), 0.0);
        // Upstream 1.5s sits above 1 and inside 2.5.
        assert_eq!(bucket(&s, "upstream", 1.0), 0.0);
        assert_eq!(bucket(&s, "upstream", 2.5), 1.0);
        let upstream = [("stage", "upstream")];
        assert_eq!(value(&s, "router_latency_seconds_sum", &upstream), 1.5);
    }

    #[test]
    fn gauges_hold_the_last_value_not_a_running_total() {
        let metrics = local();
        metrics.set_rss(100);
        metrics.set_rss(250);
        metrics.set_cache_entries(7);
        metrics.set_cache_entries(4);
        let s = scrape(&metrics);

        assert_eq!(value(&s, "router_rss_bytes", &[]), 250.0);
        assert_eq!(value(&s, "router_cache_entries", &[]), 4.0);
    }
}
