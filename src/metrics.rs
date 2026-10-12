//! Prometheus text for `GET /metrics`.
//!
//! Omniroute died quietly and nobody was watching. These are the counters that
//! would have shown it.

use std::sync::Arc;
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

/// Every outcome, so the counters can be seeded at zero.
const ALL_OUTCOMES: &[Outcome] = &[
    Outcome::HitCache,
    Outcome::Success,
    Outcome::TooLarge,
    Outcome::NoNetwork,
    Outcome::NotAdmitted,
    Outcome::NoChannel,
];

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
}

impl Metrics {
    /// Builds the Prometheus recorder and describes every metric, so the
    /// names appear on `/metrics` from the first scrape even at zero.
    pub fn new() -> crate::Result<Self> {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("router_latency_seconds".to_owned()),
                LATENCY_BUCKETS,
            )
            .map_err(|e| crate::RouterError::Config(format!("metrics buckets: {e}")))?
            .build_recorder();
        let metrics = Self {
            handle: recorder.handle(),
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

    /// The Prometheus text body for `GET /metrics`.
    pub fn render(&self) -> String {
        self.handle.render()
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
}

/// Touches every series whose labels are known up front, so an alert on a
/// never-seen outcome sits on a zero rather than on missing data until the
/// first failure. Providers and models are not known at startup, so
/// `router_attempts_total` cannot be seeded and appears on first use.
fn seed_zero() {
    for outcome in ALL_OUTCOMES {
        counter!("router_requests_total", "outcome" => outcome.label()).increment(0);
    }
    gauge!("router_rss_bytes").set(0.0);
    gauge!("router_cache_entries").set(0.0);
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
