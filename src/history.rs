//! Backend recorded metric history for the dashboard.
//!
//! The dashboard keeps its rolling charts in browser JavaScript memory, so a
//! refresh throws the lines away. This module samples the router's own
//! metrics every five seconds into a ring of up to 720 points, writes the
//! ring through to redb beside the cache every fifteen seconds, and serves
//! the ring back as JSON on `GET /metrics/history`.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::response::IntoResponse;
use prometheus_parse::{Sample, Scrape, Value};
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// How often the sampler records one point.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// How often the ring is written through to redb.
pub const FLUSH_EVERY: Duration = Duration::from_secs(15);

/// The ring's cap.
// ponytail: one hour at 5s a sample; raise MAX_SAMPLES if the dashboard
// wants a longer window, nothing else has to change.
pub const MAX_SAMPLES: usize = 720;

/// The redb table holding the ring. One row, the whole ring as one JSON blob,
/// which at 720 small points stays a cheap write.
const HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("history");

/// The ring's single row key.
const POINTS_KEY: &[u8] = b"points";

/// The redb table holding the saved counters. One row, the whole map as one
/// JSON blob: series key to value.
const COUNTERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("counters");

/// The counters table's single row key.
const COUNTERS_KEY: &[u8] = b"counters";

/// The counter series a save keeps: every counter metric except the `_count`,
/// `_sum` and `_bucket` companions histograms carry. Histograms are not
/// restored (they cannot be replayed faithfully into the recorder; the
/// history ring already keeps their percentiles over time) and gauges are
/// current readings, not history.
fn counters_from_samples(samples: &[Sample]) -> Vec<(String, u64)> {
    samples
        .iter()
        .filter(|s| {
            matches!(s.value, Value::Counter(_))
                && !s.metric.ends_with("_count")
                && !s.metric.ends_with("_sum")
                && !s.metric.ends_with("_bucket")
        })
        .filter_map(|s| {
            let pairs: Vec<(String, String)> = s
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let value = match &s.value {
                Value::Counter(v) => *v as u64,
                _ => return None,
            };
            Some((crate::metrics::build_series_key(&s.metric, &pairs), value))
        })
        .collect()
}
fn store_err(e: impl std::fmt::Display) -> crate::RouterError {
    crate::RouterError::Store(e.to_string())
}

/// One recorded sample of the router's own metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    /// Unix time in milliseconds.
    pub t: u64,
    /// Client requests finished, every outcome summed.
    pub requests: u64,
    /// Requests per minute since the previous point. `None` on the first
    /// sample after a start, and whenever the gap is unknown.
    pub rate: Option<f64>,
    /// Median upstream latency, seconds.
    pub up50: Option<f64>,
    /// 90th percentile upstream latency, seconds.
    pub up90: Option<f64>,
    /// 90th percentile whole request time, seconds.
    pub rt90: Option<f64>,
    /// 90th percentile time to first token, seconds.
    pub ttft90: Option<f64>,
    /// Mean output tokens per second across the finished requests.
    pub tps: Option<f64>,
    /// Resident set size, bytes.
    pub rss: u64,
    /// Bytes the response cache holds.
    pub cache: u64,
    /// Entries in the response cache.
    pub entries: u64,
}

/// The in-memory ring, optionally backed by a redb file.
#[derive(Debug)]
pub struct History {
    points: Mutex<Vec<Point>>,
    db: Option<Database>,
}

impl History {
    /// Opens (creating if needed) the history file and seeds the ring from
    /// whatever the last run flushed. A file that will not open, a table that
    /// will not create, or a blob that will not parse is a warning and an
    /// empty ring: telemetry must never take the router down.
    pub fn open(path: &Path) -> Self {
        let memory_only = Self {
            points: Mutex::new(Vec::new()),
            db: None,
        };
        let db = match Database::create(path) {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %path.display(),
                    "history will run in memory only"
                );
                return memory_only;
            }
        };
        // Opening the table inside a write transaction creates it on a fresh
        // file, so every later read finds it.
        let created = (|| -> crate::Result<()> {
            let write = db.begin_write().map_err(store_err)?;
            write.open_table(HISTORY).map_err(store_err)?;
            write.commit().map_err(store_err)?;
            Ok(())
        })();
        if let Err(e) = created {
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "history will run in memory only"
            );
            return memory_only;
        }
        let mut seeded = match load(&db) {
            Ok(points) => points,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    path = %path.display(),
                    "history load failed, starting empty"
                );
                Vec::new()
            }
        };
        // An older, larger cap leaves the ring longer than today's cap, so
        // trim here as well as on push.
        if seeded.len() > MAX_SAMPLES {
            seeded.drain(..seeded.len() - MAX_SAMPLES);
        }
        Self {
            points: Mutex::new(seeded),
            db: Some(db),
        }
    }

    /// Renders the metrics, reads one point off them and pushes it onto the
    /// ring, dropping the oldest once the cap is past.
    pub fn sample_once(&self, metrics: &crate::metrics::Metrics, now_unix_ms: u64) {
        let text = metrics.render();
        let scrape = match Scrape::parse(text.lines().map(|l| Ok(l.to_owned()))) {
            Ok(scrape) => scrape,
            Err(e) => {
                tracing::warn!(error = %e, "history sample skipped: the scrape did not parse");
                return;
            }
        };
        let mut ring = self.points.lock().expect("history ring");
        let previous = ring.last().cloned();
        let point = build_point(&scrape.samples, now_unix_ms, previous);
        ring.push(point);
        if ring.len() > MAX_SAMPLES {
            ring.remove(0);
        }
    }

    /// Writes the whole ring as one JSON blob in a write transaction. Cheap:
    /// 720 small points.
    pub fn flush(&self) -> crate::Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        let blob = serde_json::to_vec(&self.points()).map_err(store_err)?;
        let write = db.begin_write().map_err(store_err)?;
        {
            let mut table = write.open_table(HISTORY).map_err(store_err)?;
            table
                .insert(POINTS_KEY, blob.as_slice())
                .map_err(store_err)?;
        }
        write.commit().map_err(store_err)?;
        Ok(())
    }

    /// Writes the counter map through to redb, replacing whatever a previous
    /// flush wrote. Called on the same cadence as the ring's flush.
    pub fn save_counters(&self, saved: &[(String, u64)]) -> crate::Result<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        let blob = serde_json::to_vec(saved).map_err(store_err)?;
        let write = db.begin_write().map_err(store_err)?;
        {
            let mut table = write.open_table(COUNTERS).map_err(store_err)?;
            table
                .insert(COUNTERS_KEY, blob.as_slice())
                .map_err(store_err)?;
        }
        write.commit().map_err(store_err)?;
        Ok(())
    }

    /// The counters the previous run saved. Empty on a fresh file.
    pub fn load_counters(&self) -> Vec<(String, u64)> {
        let Some(db) = self.db.as_ref() else {
            return Vec::new();
        };
        let read = match db.begin_read() {
            Ok(read) => read,
            Err(e) => {
                tracing::warn!(error = %e, "counters load failed, starting empty");
                return Vec::new();
            }
        };
        let table = match read.open_table(COUNTERS) {
            Ok(table) => table,
            Err(_) => return Vec::new(),
        };
        let Some(blob) = (match table.get(COUNTERS_KEY) {
            Ok(Some(blob)) => Some(blob),
            _ => None,
        }) else {
            return Vec::new();
        };
        match serde_json::from_slice(blob.value()) {
            Ok(saved) => saved,
            Err(e) => {
                tracing::warn!(error = %e, "counters load failed, starting empty");
                Vec::new()
            }
        }
    }

    /// Renders the metrics, picks the counter series out and saves them. The
    /// sampler calls this beside the ring's flush.
    pub fn save_counters_from(&self, metrics: &crate::metrics::Metrics) {
        let text = metrics.render();
        let scrape = match Scrape::parse(text.lines().map(|l| Ok(l.to_owned()))) {
            Ok(scrape) => scrape,
            Err(e) => {
                tracing::warn!(error = %e, "counter save skipped: the scrape did not parse");
                return;
            }
        };
        let saved = counters_from_samples(&scrape.samples);
        if let Err(e) = self.save_counters(&saved) {
            tracing::warn!(error = %e, "counter save failed");
        }
    }
    /// A copy of the ring, oldest first.
    pub fn points(&self) -> Vec<Point> {
        self.points.lock().expect("history ring").clone()
    }

    /// Samples every [`SAMPLE_INTERVAL`] and flushes every [`FLUSH_EVERY`],
    /// until `shutdown` cancels.
    pub async fn sampler(
        self: Arc<Self>,
        metrics: crate::metrics::Metrics,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
        // Skip a stalled schedule rather than burst a catch-up run of samples
        // carrying near identical timestamps.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick fires at once; skip it, there is nothing new to sample.
        ticker.tick().await;
        // FLUSH_EVERY is a whole number of ticks: three at 5s.
        let flush_every = (FLUSH_EVERY.as_secs() / SAMPLE_INTERVAL.as_secs()).max(1);
        let mut ticks: u64 = 0;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.sample_once(&metrics, now_unix_ms());
                    ticks += 1;
                    if ticks % flush_every == 0 {
                        if let Err(e) = self.flush() {
                            tracing::warn!(error = %e, "history flush failed");
                        }
                        self.save_counters_from(&metrics);
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    }
}

/// The ring the file holds. Empty on a fresh file.
fn load(db: &Database) -> crate::Result<Vec<Point>> {
    let read = db.begin_read().map_err(store_err)?;
    let table = read.open_table(HISTORY).map_err(store_err)?;
    let Some(blob) = table.get(POINTS_KEY).map_err(store_err)? else {
        return Ok(Vec::new());
    };
    serde_json::from_slice(blob.value()).map_err(store_err)
}

/// `GET /metrics/history`. The ring, oldest first, as JSON.
pub async fn endpoint(
    axum::extract::State(state): axum::extract::State<Arc<crate::server::AppState>>,
) -> axum::response::Response {
    axum::Json(state.history.points()).into_response()
}

/// One point off the scrape, with the rate against the previous point.
fn build_point(samples: &[Sample], now: u64, previous: Option<Point>) -> Point {
    let requests = total(samples, "router_requests_total", None) as u64;
    let upstream = merged_buckets(
        samples,
        "router_latency_seconds",
        Some(("stage", "upstream")),
    );
    let upstream_count = total(
        samples,
        "router_latency_seconds_count",
        Some(("stage", "upstream")),
    );
    let router = merged_buckets(samples, "router_latency_seconds", Some(("stage", "router")));
    let router_count = total(
        samples,
        "router_latency_seconds_count",
        Some(("stage", "router")),
    );
    let ttft = merged_buckets(samples, "router_ttft_seconds", None);
    let ttft_count = total(samples, "router_ttft_seconds_count", None);
    let tps_count = total(samples, "router_output_tokens_per_second_count", None);
    let tps_sum = total(samples, "router_output_tokens_per_second_sum", None);

    Point {
        t: now,
        requests,
        rate: previous.and_then(|previous| {
            let dt_secs = now.saturating_sub(previous.t) as f64 / 1000.0;
            (dt_secs > 0.0)
                .then_some(requests.saturating_sub(previous.requests) as f64 / dt_secs * 60.0)
        }),
        up50: percentile(&upstream, upstream_count, 0.5),
        up90: percentile(&upstream, upstream_count, 0.9),
        rt90: percentile(&router, router_count, 0.9),
        ttft90: percentile(&ttft, ttft_count, 0.9),
        tps: (tps_count > 0.0).then(|| tps_sum / tps_count),
        rss: total(samples, "router_rss_bytes", None) as u64,
        cache: total(samples, "router_cache_bytes", None) as u64,
        entries: total(samples, "router_cache_entries", None) as u64,
    }
}

/// The sum of one series across every label set, optionally narrowed to one
/// label pair. Counters, gauges and the `_count` and `_sum` companions of a
/// histogram all read through here.
fn total(samples: &[Sample], metric: &str, label: Option<(&str, &str)>) -> f64 {
    samples
        .iter()
        .filter(|s| s.metric == metric && carries(s, label))
        .filter_map(|s| scalar(&s.value))
        .sum()
}

/// True when the sample carries the label pair, or `label` narrows nothing.
fn carries(sample: &Sample, label: Option<(&str, &str)>) -> bool {
    match label {
        Some((key, value)) => sample.labels.get(key) == Some(value),
        None => true,
    }
}

/// The scalar of one sample that is not a histogram.
fn scalar(value: &Value) -> Option<f64> {
    match value {
        Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => Some(*v),
        _ => None,
    }
}

/// The cumulative bucket counts of every label set of one histogram, merged
/// by `le`. The `+Inf` bucket is dropped: it carries no usable bound, and the
/// `_count` series says the same thing finitely.
fn merged_buckets(
    samples: &[Sample],
    metric: &str,
    label: Option<(&str, &str)>,
) -> Vec<(f64, f64)> {
    let mut buckets: Vec<(f64, f64)> = Vec::new();
    for sample in samples
        .iter()
        .filter(|s| s.metric == metric && carries(s, label))
    {
        let Value::Histogram(counts) = &sample.value else {
            continue;
        };
        for bucket in counts {
            if !bucket.less_than.is_finite() {
                continue;
            }
            match buckets.iter_mut().find(|(le, _)| *le == bucket.less_than) {
                Some(slot) => slot.1 += bucket.count,
                None => buckets.push((bucket.less_than, bucket.count)),
            }
        }
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    buckets
}

/// The smallest merged bucket bound whose cumulative count reaches `q` of the
/// merged total. `None` when nothing was recorded.
fn percentile(buckets: &[(f64, f64)], total: f64, q: f64) -> Option<f64> {
    if total == 0.0 {
        return None;
    }
    let target = q * total;
    buckets
        .iter()
        .find(|(_, cumulative)| *cumulative >= target)
        .map(|(le, _)| *le)
}

/// Unix time in milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{Metrics, Outcome};
    use std::time::Duration;

    /// A fresh set of counters. Each `Metrics` owns its recorder, so tests in
    /// one process do not share numbers.
    fn metrics() -> Metrics {
        Metrics::new().expect("the recorder builds")
    }

    /// A ring with no file behind it.
    fn memory_history() -> History {
        History {
            points: Mutex::new(Vec::new()),
            db: None,
        }
    }

    #[test]
    fn one_sample_computes_the_fields() {
        let h = memory_history();
        let m = metrics();
        m.record_request(Outcome::Success);
        m.record_request(Outcome::Success);
        m.record_latency(Duration::from_secs(1), Duration::from_millis(10));
        m.record_ttft(Duration::from_millis(500));
        m.record_output_rate(140.0);
        m.set_rss(1 << 30);
        m.set_cache_bytes(1 << 20);
        m.set_cache_entries(7);

        h.sample_once(&m, 1_000);
        let point = &h.points()[0];
        assert_eq!(point.requests, 2);
        // One 1.0s upstream latency: the smallest bucket bound at or past the
        // 0.5 and 0.9 quantiles is 1.0 itself.
        assert_eq!(point.up50, Some(1.0));
        assert_eq!(point.up90, Some(1.0));
        assert_eq!(point.rt90, Some(0.01));
        assert_eq!(point.ttft90, Some(0.5));
        assert_eq!(point.tps, Some(140.0));
        assert_eq!(point.rss, 1 << 30);
        assert_eq!(point.cache, 1 << 20);
        assert_eq!(point.entries, 7);
        assert_eq!(point.rate, None);
    }

    #[test]
    fn rate_counts_the_gap_between_samples() {
        let h = memory_history();
        let m = metrics();
        m.record_request(Outcome::Success);
        m.record_request(Outcome::Success);
        h.sample_once(&m, 1_000);
        m.record_request(Outcome::Success);
        m.record_request(Outcome::Success);
        h.sample_once(&m, 11_000);
        let points = h.points();
        assert_eq!(points[0].rate, None);
        // Two more requests across ten seconds is twelve a minute.
        assert_eq!(points[1].rate, Some(12.0));
    }

    #[test]
    fn the_ring_keeps_only_the_newest() {
        let h = memory_history();
        let m = metrics();
        for i in 0..=MAX_SAMPLES as u64 {
            h.sample_once(&m, i * 5_000);
        }
        let points = h.points();
        assert_eq!(points.len(), MAX_SAMPLES);
        // The point that would have been dropped is gone: the ring starts at
        // the second sample and ends on the last one.
        assert_eq!(points.first().map(|p| p.t), Some(5_000));
        assert_eq!(points.last().map(|p| p.t), Some(MAX_SAMPLES as u64 * 5_000));
    }

    #[test]
    fn counters_round_trip_through_redb() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("history.redb");
        let m = metrics();
        {
            let h = History::open(&path);
            m.record_request(Outcome::Success);
            m.record_request(Outcome::Success);
            m.record_output_rate(140.0);
            h.save_counters_from(&m);
        }
        let h = History::open(&path);
        let saved = h.load_counters();
        let requests = saved
            .iter()
            .find(|(key, _)| key.contains("router_requests_total{outcome=\"success\"}"))
            .map(|(_, v)| *v);
        // Only counters are saved: histogram and gauge series stay out.
        assert_eq!(requests, Some(2));
        assert!(saved.iter().all(|(key, _)| {
            !key.contains("_count") && !key.contains("_sum") && !key.contains("router_rss_bytes")
        }));
    }

    #[test]
    fn saved_counters_restore_onto_a_fresh_recorder() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("history.redb");
        let m = metrics();
        {
            let h = History::open(&path);
            m.record_request(Outcome::Success);
            h.save_counters_from(&m);
        }
        let h = History::open(&path);
        let saved = h.load_counters();
        let fresh = metrics();
        fresh.set_counter_offsets(&saved);
        let text = fresh.render();
        assert!(text.contains("router_requests_total{outcome=\"success\"} 1"));
    }

    #[test]
    fn history_round_trips_through_redb() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("history.redb");
        let m = metrics();
        {
            let h = History::open(&path);
            m.record_request(Outcome::Success);
            m.record_request(Outcome::Success);
            m.record_request(Outcome::Success);
            h.sample_once(&m, 1_000);
            m.record_request(Outcome::Success);
            m.record_request(Outcome::Success);
            h.sample_once(&m, 6_000);
            h.flush().expect("the flush writes");
        }
        let h = History::open(&path);
        let points = h.points();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].t, 1_000);
        assert_eq!(points[0].requests, 3);
        assert_eq!(points[0].rate, None);
        assert_eq!(points[1].t, 6_000);
        assert_eq!(points[1].requests, 5);
        // Two more requests across five seconds is twenty four a minute.
        assert_eq!(points[1].rate, Some(24.0));
    }

    #[test]
    fn an_empty_histogram_is_none() {
        // A fresh recorder describes the histograms but has recorded nothing
        // into them, so every percentile is None rather than a bucket bound.
        let h = memory_history();
        let m = metrics();
        h.sample_once(&m, 0);
        let point = &h.points()[0];
        assert_eq!(point.up50, None);
        assert_eq!(point.rt90, None);
        assert_eq!(point.ttft90, None);
        assert_eq!(point.tps, None);
    }
}
