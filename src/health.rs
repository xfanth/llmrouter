//! Health and backoff, at three levels: the channel, the provider/key pair and
//! the provider.
//!
//! `wait = min(backoff_max, retry_base * 2^(counter - 1))`. A `Retry-After`
//! header on a failure replaces `retry_base`; the doubling still applies on
//! top. A success resets the counter to 0 and the base to the configured
//! `health_poll.retry_base`.
//!
//! Health is binary and uses the same test the race uses to pick a winner: a
//! 2xx carrying non-empty content is healthy, everything else is not. No
//! classifying failures into kinds.
//!
//! The table lives in memory while running, flushes to redb every 5 minutes
//! and on shutdown, and loads back at startup. Deadlines are stored absolute,
//! so one already past at load time means eligible at once.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use dashmap::DashMap;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::catalogue::ChannelId;
use crate::RouterError;

/// The redb table holding the health rows. Key and value are both JSON, which
/// keeps the stored shape readable and costs nothing at this row count: a few
/// thousand rows written once every 5 minutes.
const HEALTH: TableDefinition<&str, &[u8]> = TableDefinition::new("health");

fn store_err(e: impl std::fmt::Display) -> RouterError {
    RouterError::Store(e.to_string())
}

/// The three things that carry their own backoff.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HealthKey {
    /// One provider/model/key tuple. Nothing polls it; it rejoins the list
    /// when its backoff expires and the next real request is the test.
    Channel(ChannelId),
    /// One provider/key pair. Polled with the keyed `/models` probe.
    ProviderKey {
        /// Provider name.
        provider: String,
        /// Stable key label, never the key itself.
        key_id: String,
    },
    /// One provider. Polled with the keyless reachability probe.
    Provider(String),
}

/// The three numbers every unhealthy thing carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthEntry {
    /// Consecutive failures. 0 means healthy.
    pub retry_counter: u32,
    /// Seconds. The configured `health_poll.retry_base` by default;
    /// `Retry-After` replaces it.
    pub retry_base_secs: u64,
    /// Absolute unix second from which the thing is eligible again.
    pub eligible_at: u64,
}

impl HealthEntry {
    /// A healthy entry starting from the configured base.
    pub fn new(retry_base_secs: u64) -> Self {
        Self {
            retry_counter: 0,
            retry_base_secs,
            eligible_at: 0,
        }
    }
}

/// The in-memory health table, backed by a redb table in the same file as the
/// response cache.
#[derive(Debug)]
pub struct HealthTable {
    entries: DashMap<HealthKey, HealthEntry>,
    db: Arc<Database>,
    retry_base: Duration,
    backoff_max: Duration,
}

impl HealthTable {
    /// Opens the `health` table in `db` and loads every row into memory.
    pub fn open(
        db: Arc<Database>,
        retry_base: Duration,
        backoff_max: Duration,
    ) -> crate::Result<Self> {
        // Opening the table inside a write transaction creates it on a fresh
        // file, so every later read finds it.
        let write = db.begin_write().map_err(store_err)?;
        write.open_table(HEALTH).map_err(store_err)?;
        write.commit().map_err(store_err)?;

        let entries = DashMap::new();
        {
            let read = db.begin_read().map_err(store_err)?;
            let table = read.open_table(HEALTH).map_err(store_err)?;
            for row in table.iter().map_err(store_err)? {
                let (k, v) = row.map_err(store_err)?;
                // A row that will not parse came from an older shape. Drop it
                // rather than refuse to start: the worst case is one thing
                // treated as healthy until it fails again.
                let Ok(key) = serde_json::from_str::<HealthKey>(k.value()) else {
                    continue;
                };
                let Ok(entry) = serde_json::from_slice::<HealthEntry>(v.value()) else {
                    continue;
                };
                entries.insert(key, entry);
            }
        }

        Ok(Self {
            entries,
            db,
            retry_base,
            backoff_max,
        })
    }

    /// The configured default base, for callers mirroring health elsewhere.
    /// Floored at 1 second: a base of 0 would never back off.
    pub fn retry_base_secs(&self) -> u64 {
        self.retry_base.as_secs().max(1)
    }

    /// True when the thing may be called now. An absent entry is healthy.
    pub fn eligible(&self, key: &HealthKey, now_unix: u64) -> bool {
        match self.entries.get(key) {
            Some(e) => e.eligible_at <= now_unix,
            None => true,
        }
    }

    /// The entry as it stands, for `/healthz` and `/v1/models`.
    pub fn get(&self, key: &HealthKey) -> Option<HealthEntry> {
        self.entries.get(key).map(|e| *e)
    }

    /// Records one failed trial. M hedged copies of the same channel call are
    /// one trial, so this is called once however many copies failed.
    /// `retry_after_secs` is the longest of the copies' `Retry-After` values.
    pub fn record_failure(
        &self,
        key: &HealthKey,
        retry_after_secs: Option<u64>,
        now_unix: u64,
    ) -> HealthEntry {
        let mut entry = self
            .entries
            .entry(key.clone())
            .or_insert_with(|| HealthEntry::new(self.retry_base_secs()));
        entry.retry_counter = entry.retry_counter.saturating_add(1);
        if let Some(secs) = retry_after_secs {
            // A date already past parses to 0. Floor the base at 1 second, or
            // the thing would be eligible again at once and never back off.
            entry.retry_base_secs = secs.max(1);
        }
        let wait = wait_seconds(
            entry.retry_counter,
            entry.retry_base_secs,
            self.backoff_max.as_secs(),
        );
        entry.eligible_at = now_unix.saturating_add(wait);
        *entry
    }

    /// Records a success. Drops the entry, so the next failure starts from
    /// the configured base again.
    pub fn record_success(&self, key: &HealthKey) {
        self.entries.remove(key);
    }

    /// Every entry that is still in backoff, for `/healthz`.
    pub fn in_backoff(&self, now_unix: u64) -> Vec<(HealthKey, HealthEntry)> {
        let mut out: Vec<(HealthKey, HealthEntry)> = self
            .entries
            .iter()
            .filter(|e| e.value().eligible_at > now_unix)
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        // Soonest due first, so `/healthz` reads in the order things recover.
        out.sort_by_key(|(_, e)| e.eligible_at);
        out
    }

    /// Writes the whole table to redb in one transaction.
    pub fn flush(&self) -> crate::Result<()> {
        let rows: Vec<(String, Vec<u8>)> = self
            .entries
            .iter()
            .filter_map(|e| {
                let k = serde_json::to_string(e.key()).ok()?;
                let v = serde_json::to_vec(e.value()).ok()?;
                Some((k, v))
            })
            .collect();

        let write = self.db.begin_write().map_err(store_err)?;
        // Replace rather than merge, so a thing that recovered leaves the
        // stored table as well as the in-memory one.
        write.delete_table(HEALTH).map_err(store_err)?;
        {
            let mut table = write.open_table(HEALTH).map_err(store_err)?;
            for (k, v) in &rows {
                table.insert(k.as_str(), v.as_slice()).map_err(store_err)?;
            }
        }
        write.commit().map_err(store_err)?;
        Ok(())
    }

    /// Flushes every `interval` until `shutdown` fires, then flushes once more.
    pub async fn flush_loop(
        self: Arc<Self>,
        interval: Duration,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        // Floored at a second: `tokio::time::interval` panics on a period of
        // zero, and a zero in `router.yml` means "flush as often as possible",
        // not "kill health persistence".
        let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
        // The first tick fires at once; skip it, there is nothing new to write.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.flush() {
                        tracing::warn!(error = %e, "health flush failed");
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
        if let Err(e) = self.flush() {
            tracing::warn!(error = %e, "final health flush failed");
        }
    }
}

/// `wait = min(backoff_max, retry_base * 2^(counter - 1))`. A counter of 0
/// means healthy and waits nothing.
pub fn wait_seconds(retry_counter: u32, retry_base_secs: u64, backoff_max_secs: u64) -> u64 {
    if retry_counter == 0 {
        return 0;
    }
    let shift = retry_counter - 1;
    let wait = if shift >= 63 {
        u64::MAX
    } else {
        retry_base_secs.saturating_mul(1u64 << shift)
    };
    wait.min(backoff_max_secs)
}

/// Parses a `Retry-After` header. Handles both forms: a count of seconds, and
/// an HTTP date, which becomes the seconds between `now` and that date. A date
/// already past gives 0. `httpdate` covers all three date forms RFC 9110
/// requires a recipient to accept.
pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<u64> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs);
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(at.duration_since(now).unwrap_or_default().as_secs())
}

/// The winner test, used both by the race and by health: a 2xx carrying
/// non-empty content. An empty 200, or a 200 holding an error object, fails.
pub fn is_healthy_response(status: u16, body: &[u8]) -> bool {
    if !(200..300).contains(&status) {
        return false;
    }
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    // An error object in a 200 body is a failure dressed as a success.
    if json.get("error").is_some_and(|e| !e.is_null()) {
        return false;
    }
    has_content(&json)
}

/// What a streamed reply has shown so far, from the server-sent-event frames
/// read up to now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamVerdict {
    /// Nothing decisive yet: comments, keep-alives, or openers with no content.
    Pending,
    /// At least one frame carried non-empty content, so this is an answer.
    Content,
    /// A frame carried an error object, so the 2xx is a failure in disguise.
    Failed,
}

/// The streaming twin of [`is_healthy_response`], over the raw event bytes
/// read so far.
///
/// A byte count decides nothing. An SSE comment such as
/// `: OPENROUTER PROCESSING` is non-empty bytes and no content at all, and a
/// `data:` frame holding an error object is a rate limit wearing a 200. Both
/// arrive sooner than a real first token, so either would win every race.
pub fn stream_verdict(sse: &[u8]) -> StreamVerdict {
    // Whole lines only. A half-read frame is scanned again on the next pass.
    let Some(last_newline) = sse.iter().rposition(|b| *b == b'\n') else {
        return StreamVerdict::Pending;
    };
    let text = String::from_utf8_lossy(&sse[..=last_newline]);

    let mut verdict = StreamVerdict::Pending;
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) else {
            continue;
        };
        if json.get("error").is_some_and(|e| !e.is_null()) {
            return StreamVerdict::Failed;
        }
        if stream_event_has_content(&json) {
            verdict = StreamVerdict::Content;
        }
    }
    verdict
}

/// One decoded event carries something the client would call an answer.
fn stream_event_has_content(json: &serde_json::Value) -> bool {
    // OpenAI chat chunks arrive as `choices[].delta`, which `has_content`
    // already reads.
    if has_content(json) {
        return true;
    }
    // Anthropic message events carry the block on the event itself.
    json.get("delta").is_some_and(block_is_non_empty)
        || json.get("content_block").is_some_and(block_is_non_empty)
}

/// True when the body carries at least one non-empty content block, in either
/// of the two wire shapes the router speaks.
fn has_content(json: &serde_json::Value) -> bool {
    // Anthropic messages: a top-level `content` array of blocks.
    if let Some(blocks) = json.get("content").and_then(|c| c.as_array()) {
        if blocks.iter().any(block_is_non_empty) {
            return true;
        }
    }
    // OpenAI chat completions: `choices[].message`.
    if let Some(choices) = json.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            let Some(message) = choice.get("message").or_else(|| choice.get("delta")) else {
                continue;
            };
            if message
                .get("tool_calls")
                .and_then(|t| t.as_array())
                .is_some_and(|t| !t.is_empty())
            {
                return true;
            }
            match message.get("content") {
                Some(serde_json::Value::String(s)) if !s.trim().is_empty() => return true,
                Some(serde_json::Value::Array(blocks)) if blocks.iter().any(block_is_non_empty) => {
                    return true
                }
                _ => {}
            }
        }
    }
    false
}

/// One content block carries something: text with characters in it, or any
/// structured block such as a tool call.
fn block_is_non_empty(block: &serde_json::Value) -> bool {
    match block {
        serde_json::Value::String(s) => !s.trim().is_empty(),
        serde_json::Value::Object(map) => match map.get("text") {
            Some(serde_json::Value::String(s)) => !s.trim().is_empty(),
            // A tool_use, image or thinking block carries no `text` field but
            // is still content.
            Some(_) => true,
            None => map.contains_key("type"),
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600;

    #[test]
    fn healthy_waits_nothing() {
        assert_eq!(wait_seconds(0, 1, HOUR), 0);
        assert_eq!(wait_seconds(0, 60, HOUR), 0);
    }

    #[test]
    fn doubling_from_a_base_of_one() {
        let waits: Vec<u64> = (1..=5).map(|c| wait_seconds(c, 1, HOUR)).collect();
        assert_eq!(waits, vec![1, 2, 4, 8, 16]);
    }

    #[test]
    fn retry_after_sets_the_base_and_the_doubling_still_applies() {
        // The design's worked case: a provider saying 60 seconds gives 60,
        // then 120, then 240.
        assert_eq!(wait_seconds(1, 60, HOUR), 60);
        assert_eq!(wait_seconds(2, 60, HOUR), 120);
        assert_eq!(wait_seconds(3, 60, HOUR), 240);
    }

    #[test]
    fn capped_at_an_hour() {
        // Twelve failures from a base of 1 reaches the cap.
        assert_eq!(wait_seconds(12, 1, HOUR), 2_048);
        assert_eq!(wait_seconds(13, 1, HOUR), HOUR);
        assert_eq!(wait_seconds(40, 1, HOUR), HOUR);
        // And no overflow at an absurd counter or base.
        assert_eq!(wait_seconds(u32::MAX, u64::MAX, HOUR), HOUR);
        assert_eq!(wait_seconds(64, 1, HOUR), HOUR);
    }

    #[test]
    fn retry_after_seconds_form() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(parse_retry_after("120", now), Some(120));
        assert_eq!(parse_retry_after("  120 ", now), Some(120));
        assert_eq!(parse_retry_after("0", now), Some(0));
    }

    #[test]
    fn retry_after_date_form() {
        // 6 Nov 1994 08:49:37 GMT is unix 784_111_777.
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777 - 90);
        // The same instant in all three forms RFC 9110 section 5.6.7 lists:
        // IMF-fixdate, the obsolete RFC 850 form, and asctime.
        for date in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
        ] {
            assert_eq!(parse_retry_after(date, now), Some(90), "{date}");
        }
    }

    #[test]
    fn a_date_already_past_gives_zero() {
        let date = "Sun, 06 Nov 1994 08:49:37 GMT";
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777 + 500);
        assert_eq!(parse_retry_after(date, now), Some(0));
    }

    #[test]
    fn rubbish_retry_after_is_none() {
        let now = SystemTime::UNIX_EPOCH;
        assert_eq!(parse_retry_after("soon", now), None);
        assert_eq!(parse_retry_after("", now), None);
        assert_eq!(parse_retry_after("-5", now), None);
        assert_eq!(
            parse_retry_after("Mon, 32 Xxx 1994 08:49:37 GMT", now),
            None
        );
        assert_eq!(
            parse_retry_after("Sun, 06 Nov 1994 08:49:37 UTC", now),
            None
        );
    }

    #[test]
    fn leap_year_dates() {
        let epoch = SystemTime::UNIX_EPOCH;
        // 29 Feb 2024 00:00:00 GMT is unix 1_709_164_800.
        assert_eq!(
            parse_retry_after("Thu, 29 Feb 2024 00:00:00 GMT", epoch),
            Some(1_709_164_800)
        );
        // 1 Mar 2000 00:00:00 GMT, the 400 year rule.
        assert_eq!(
            parse_retry_after("Wed, 01 Mar 2000 00:00:00 GMT", epoch),
            Some(951_868_800)
        );
    }

    #[test]
    fn two_hundred_with_text_is_healthy() {
        let openai = br#"{"choices":[{"message":{"content":"hello"}}]}"#;
        assert!(is_healthy_response(200, openai));
        let anthropic = br#"{"content":[{"type":"text","text":"hello"}]}"#;
        assert!(is_healthy_response(200, anthropic));
    }

    #[test]
    fn two_hundred_with_a_tool_call_only_is_healthy() {
        let openai = br#"{"choices":[{"message":{"content":null,"tool_calls":[{"id":"1"}]}}]}"#;
        assert!(is_healthy_response(200, openai));
        let anthropic = br#"{"content":[{"type":"tool_use","id":"1","name":"bash"}]}"#;
        assert!(is_healthy_response(200, anthropic));
    }

    #[test]
    fn an_empty_two_hundred_is_not_healthy() {
        assert!(!is_healthy_response(200, b""));
        assert!(!is_healthy_response(200, b"{}"));
        assert!(!is_healthy_response(200, br#"{"choices":[]}"#));
        assert!(!is_healthy_response(200, br#"{"content":[]}"#));
        assert!(!is_healthy_response(
            200,
            br#"{"choices":[{"message":{"content":""}}]}"#
        ));
        assert!(!is_healthy_response(
            200,
            br#"{"content":[{"type":"text","text":"   "}]}"#
        ));
    }

    #[test]
    fn an_error_object_in_a_two_hundred_is_not_healthy() {
        let body =
            br#"{"error":{"message":"rate limited"},"content":[{"type":"text","text":"x"}]}"#;
        assert!(!is_healthy_response(200, body));
    }

    #[test]
    fn a_keep_alive_comment_is_not_content() {
        assert_eq!(
            stream_verdict(b": OPENROUTER PROCESSING\n\n"),
            StreamVerdict::Pending
        );
        assert_eq!(stream_verdict(b"\n\n"), StreamVerdict::Pending);
        assert_eq!(stream_verdict(b"data: [DONE]\n\n"), StreamVerdict::Pending);
    }

    #[test]
    fn an_error_frame_fails_whatever_else_it_carries() {
        let sse = b"data: {\"error\":{\"code\":429,\"message\":\"rate limited\"}}\n\n";
        assert_eq!(stream_verdict(sse), StreamVerdict::Failed);
    }

    #[test]
    fn a_token_in_either_wire_shape_is_content() {
        let openai = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
        assert_eq!(stream_verdict(openai), StreamVerdict::Content);
        let anthropic = b"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n";
        assert_eq!(stream_verdict(anthropic), StreamVerdict::Content);
    }

    #[test]
    fn openers_and_empty_deltas_are_not_yet_content() {
        let start = b"data: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\n";
        assert_eq!(stream_verdict(start), StreamVerdict::Pending);
        let block = b"data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n";
        assert_eq!(stream_verdict(block), StreamVerdict::Pending);
        let role =
            b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
        assert_eq!(stream_verdict(role), StreamVerdict::Pending);
    }

    #[test]
    fn a_half_read_frame_waits_for_its_newline() {
        let partial = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"";
        assert_eq!(stream_verdict(partial), StreamVerdict::Pending);
        let whole = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n";
        assert_eq!(stream_verdict(whole), StreamVerdict::Content);
    }

    #[test]
    fn a_tool_call_delta_is_content() {
        let openai =
            b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"1\"}]}}]}\n\n";
        assert_eq!(stream_verdict(openai), StreamVerdict::Content);
        let anthropic = b"data: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"id\":\"1\",\"name\":\"bash\"}}\n\n";
        assert_eq!(stream_verdict(anthropic), StreamVerdict::Content);
    }

    #[test]
    fn non_two_xx_is_never_healthy() {
        let good = br#"{"content":[{"type":"text","text":"hello"}]}"#;
        for status in [199u16, 301, 400, 401, 429, 500, 502, 504] {
            assert!(!is_healthy_response(status, good));
        }
        assert!(is_healthy_response(299, good));
    }

    // The table itself, against a throwaway redb file.

    fn table(backoff_max_secs: u64) -> (HealthTable, tempfile::TempDir) {
        table_with_base(1, backoff_max_secs)
    }

    fn table_with_base(
        retry_base_secs: u64,
        backoff_max_secs: u64,
    ) -> (HealthTable, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("scratch dir");
        let db = Database::create(dir.path().join("health.redb")).unwrap();
        let t = HealthTable::open(
            Arc::new(db),
            Duration::from_secs(retry_base_secs),
            Duration::from_secs(backoff_max_secs),
        )
        .unwrap();
        (t, dir)
    }

    #[test]
    fn the_configured_retry_base_is_the_default_base() {
        let (t, _d) = table_with_base(30, HOUR);
        let k = provider("groq");
        // No Retry-After header, so the configured 30 seconds applies and
        // doubles from there.
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 30);
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 60);
        // A header still overrides it for that thing.
        assert_eq!(t.record_failure(&k, Some(5), 0).eligible_at, 20);
        // And a success puts the base back to the configured default.
        t.record_success(&k);
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 30);
    }

    #[test]
    fn a_zero_configured_base_still_backs_off_one_second() {
        let (t, _d) = table_with_base(0, HOUR);
        assert_eq!(t.record_failure(&provider("groq"), None, 0).eligible_at, 1);
    }

    fn provider(name: &str) -> HealthKey {
        HealthKey::Provider(name.to_string())
    }

    #[test]
    fn an_absent_entry_is_healthy() {
        let (t, _d) = table(HOUR);
        assert!(t.eligible(&provider("groq"), 1_000));
        assert_eq!(t.get(&provider("groq")), None);
    }

    #[test]
    fn a_failure_sets_a_deadline_and_a_success_clears_it() {
        let (t, _d) = table(HOUR);
        let k = provider("groq");
        let e = t.record_failure(&k, None, 1_000);
        assert_eq!(e.retry_counter, 1);
        assert_eq!(e.retry_base_secs, 1);
        assert_eq!(e.eligible_at, 1_001);
        assert!(!t.eligible(&k, 1_000));
        assert!(t.eligible(&k, 1_001));

        t.record_success(&k);
        assert_eq!(t.get(&k), None);
        assert!(t.eligible(&k, 0));
    }

    #[test]
    fn consecutive_failures_double_the_wait() {
        let (t, _d) = table(HOUR);
        let k = provider("groq");
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 1);
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 2);
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 4);
        assert_eq!(t.get(&k).unwrap().retry_counter, 3);
    }

    #[test]
    fn retry_after_replaces_the_base_on_the_entry() {
        let (t, _d) = table(HOUR);
        let k = provider("zai");
        assert_eq!(t.record_failure(&k, Some(60), 0).eligible_at, 60);
        assert_eq!(t.record_failure(&k, Some(60), 0).eligible_at, 120);
        // The base sticks when a later failure carries no header.
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 240);
        // A success puts the base back to 1.
        t.record_success(&k);
        assert_eq!(t.record_failure(&k, None, 0).eligible_at, 1);
    }

    #[test]
    fn a_zero_retry_after_still_backs_off_one_second() {
        let (t, _d) = table(HOUR);
        let k = provider("groq");
        assert_eq!(t.record_failure(&k, Some(0), 0).eligible_at, 1);
    }

    #[test]
    fn in_backoff_lists_only_the_still_waiting_soonest_first() {
        let (t, _d) = table(HOUR);
        let slow = provider("slow");
        let soon = provider("soon");
        t.record_failure(&slow, Some(600), 0);
        t.record_failure(&soon, Some(10), 0);
        let rows = t.in_backoff(0);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, soon);
        assert_eq!(rows[1].0, slow);
        // Past the first deadline only the slow one is left.
        let rows = t.in_backoff(100);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, slow);
        assert!(t.in_backoff(10_000).is_empty());
    }

    #[test]
    fn the_table_survives_a_restart_and_deadlines_stay_absolute() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("health.redb");
        let k = HealthKey::ProviderKey {
            provider: "gemini".to_string(),
            key_id: "key-3".to_string(),
        };

        {
            let db = Arc::new(Database::create(&path).unwrap());
            let t =
                HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap();
            t.record_failure(&k, Some(600), 1_000);
            t.record_failure(&provider("dead"), None, 1_000);
            t.record_success(&provider("dead"));
            t.flush().unwrap();
        }

        let db = Arc::new(Database::create(&path).unwrap());
        let t = HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap();
        let e = t.get(&k).unwrap();
        assert_eq!(e.retry_counter, 1);
        assert_eq!(e.retry_base_secs, 600);
        assert_eq!(e.eligible_at, 1_600);
        // Still waiting just before the deadline, eligible after it.
        assert!(!t.eligible(&k, 1_599));
        assert!(t.eligible(&k, 1_600));
        // A thing that recovered before the flush is not stored.
        assert_eq!(t.get(&provider("dead")), None);
        // A deadline long past means eligible at once, which is what a
        // container down for an hour should see.
        assert!(t.eligible(&k, 9_999_999));
    }

    #[test]
    fn a_channel_key_round_trips_through_redb() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("health.redb");
        let k = HealthKey::Channel(ChannelId(0xdead_beef_u64));
        {
            let db = Arc::new(Database::create(&path).unwrap());
            let t =
                HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap();
            t.record_failure(&k, None, 500);
            t.flush().unwrap();
        }
        let db = Arc::new(Database::create(&path).unwrap());
        let t = HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap();
        assert_eq!(t.get(&k).unwrap().eligible_at, 501);
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_flush_interval_does_not_kill_the_flush_loop() {
        // `flush_interval: 0s` in `router.yml` used to panic the loop on its
        // first line, which on a `panic = "abort"` build takes the process
        // down at boot and otherwise loses health persistence in silence.
        let dir = tempfile::tempdir().expect("scratch dir");
        let path = dir.path().join("health.redb");
        let k = HealthKey::Channel(ChannelId(1));

        let db = Arc::new(Database::create(&path).unwrap());
        let t = Arc::new(
            HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap(),
        );
        t.record_failure(&k, None, 500);

        let shutdown = tokio_util::sync::CancellationToken::new();
        let loop_task = tokio::spawn(Arc::clone(&t).flush_loop(Duration::ZERO, shutdown.clone()));
        tokio::time::sleep(Duration::from_secs(3)).await;
        shutdown.cancel();
        loop_task
            .await
            .expect("the flush loop survives a zero interval");

        // And the row really reached redb.
        drop(t);
        let db = Arc::new(Database::create(&path).unwrap());
        let reopened =
            HealthTable::open(db, Duration::from_secs(1), Duration::from_secs(HOUR)).unwrap();
        assert_eq!(reopened.get(&k).unwrap().eligible_at, 501);
    }
}
