//! The request log behind the dashboard's bottom table.
//!
//! One entry per client request that reached a completion endpoint: when,
//! who, what the model was asked and answered, how long each stage took and
//! how it ended. The ring is memory only and deliberately small — the last
//! few hundred requests, with the payloads cut to a snippet — because this
//! exists so a human can see what the router did, not to rebuild anything.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::response::IntoResponse;
use serde::Serialize;

/// How many entries the ring keeps.
const CAP: usize = 256;

/// How much of each payload one entry holds, in characters of JSON or text.
/// A snippet is for recognising the request, not for replaying it.
const SNIPPET_CHARS: usize = 2_000;

/// One finished client request.
#[derive(Debug, Clone, Serialize)]
pub struct Entry {
    /// Unix time in milliseconds, when the request finished.
    pub ts: u64,
    /// The first six characters of the client's key, as the log lines carry.
    pub client: String,
    /// The endpoint the request came in on, for example `/v1/messages`.
    pub endpoint: &'static str,
    /// The model the caller named, if any.
    pub model: Option<String>,
    /// The model that actually answered.
    pub served: Option<String>,
    /// The provider that answered.
    pub provider: Option<String>,
    /// The HTTP status the client got.
    pub status: u16,
    /// The short outcome tag, matching the `/metrics` outcome labels.
    pub outcome: String,
    /// Whether the client asked for a stream.
    pub streamed: bool,
    /// Channel calls the walk made.
    pub attempts: u32,
    /// Milliseconds from accepting the request to its first content.
    pub ttft_ms: Option<u64>,
    /// Milliseconds from accepting the request to its end.
    pub total_ms: u64,
    /// Output tokens per second of the winning stream over its own
    /// generation window.
    pub tps: Option<f64>,
    /// Prompt tokens the provider reported.
    pub tokens_in: Option<u64>,
    /// Completion tokens the provider reported.
    pub tokens_out: Option<u64>,
    /// The first part of the request body, as the client sent it.
    pub request: String,
    /// The first part of the answer text.
    pub response: String,
}

impl Entry {
    /// An entry for a request that ended before a channel was chosen. The
    /// payload snippets are filled in by the caller that still holds them.
    #[allow(clippy::too_many_arguments)] // one entry, one row, every field earns its place
    pub fn finished(
        endpoint: &'static str,
        model: Option<String>,
        streamed: bool,
        status: u16,
        outcome: &str,
    ) -> Self {
        Self {
            ts: now_unix_ms(),
            client: String::new(),
            endpoint,
            model,
            served: None,
            provider: None,
            status,
            outcome: outcome.to_owned(),
            streamed,
            attempts: 0,
            ttft_ms: None,
            total_ms: 0,
            tps: None,
            tokens_in: None,
            tokens_out: None,
            request: String::new(),
            response: String::new(),
        }
    }

    /// Cuts a payload down to the snippet cap, on character boundaries.
    pub fn snippet(text: impl AsRef<str>) -> String {
        text.as_ref().chars().take(SNIPPET_CHARS).collect()
    }
}

/// The ring of finished requests.
#[derive(Debug, Default)]
pub struct RequestLog {
    ring: Mutex<VecDeque<Entry>>,
}

impl RequestLog {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pushes one entry, dropping the oldest once the cap is past.
    pub fn record(&self, entry: Entry) {
        let mut ring = self.ring.lock().expect("request log ring");
        if ring.len() >= CAP {
            ring.pop_front();
        }
        ring.push_back(entry);
    }

    /// The ring, newest first.
    pub fn entries(&self) -> Vec<Entry> {
        let ring = self.ring.lock().expect("request log ring");
        ring.iter().rev().cloned().collect()
    }
}

/// `GET /metrics/requests`. The ring, newest first, as JSON.
pub async fn endpoint(
    axum::extract::State(state): axum::extract::State<Arc<crate::server::AppState>>,
) -> axum::response::Response {
    axum::Json(state.reqlog.entries()).into_response()
}

/// Unix time in milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ring_keeps_only_the_newest() {
        let log = RequestLog::new();
        for i in 0..(CAP + 10) {
            let mut entry = Entry::finished("/v1/messages", None, false, 200, "success");
            entry.request = format!("request {i}");
            log.record(entry);
        }
        let entries = log.entries();
        assert_eq!(entries.len(), CAP);
        // Newest first: the last one recorded leads.
        assert_eq!(entries[0].request, format!("request {}", CAP + 9));
        assert_eq!(entries[CAP - 1].request, "request 10".to_string());
    }

    #[test]
    fn snippets_cut_on_character_boundaries() {
        let long = "é".repeat(SNIPPET_CHARS + 50);
        let cut = Entry::snippet(&long);
        assert_eq!(cut.chars().count(), SNIPPET_CHARS);
        assert_eq!(cut, "é".repeat(SNIPPET_CHARS));
    }
}
