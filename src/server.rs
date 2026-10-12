//! The axum routes.
//!
//! Translation happens at the two edges only: the inbound body becomes a
//! neutral request once, and the winning reply becomes the client's format
//! once. The race only ever sees neutral types.
//!
//! The router does not stream. DESIGN.md decided whole responses only, with
//! `stream: false` upstream: that is what keeps a failure recoverable right up
//! to the moment the reply goes out, and what keeps every request on the cache
//! path. The client's own `stream` flag is read, ignored and dropped, like its
//! API key. A channel call can take minutes, so the client waits, and its
//! status code is the whole signal.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::admission::{Admission, Admit};
use crate::cache::{Cache, CacheEntry};
use crate::catalogue::{Catalogue, ChannelFilter};
use crate::config::{Config, RouterConfig};
use crate::health::HealthTable;
use crate::metrics::{Metrics, Outcome};
use crate::race::{RaceInput, RaceOutcome, Racer};
use crate::wire::Format;

/// Headroom the filter adds on top of the prompt estimate plus the requested
/// output, as a percentage. The estimate is characters divided by 3.5, which
/// is rough, so a channel is only asked for if it has a little more room than
/// the arithmetic says.
const CONTEXT_MARGIN_PERCENT: u64 = 5;

/// Client requests inside [`handle_completion`] right now. This is the number
/// `spill_above_concurrent` is compared against: concurrency is arrival rate
/// times service time, and a channel call runs for minutes, so it climbs well
/// past any request-per-second figure.
static CONCURRENT: AtomicUsize = AtomicUsize::new(0);

/// Makes a spill file name unique within this process.
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Counts one client request for as long as it is in the handler.
struct Concurrency;

impl Concurrency {
    /// Returns the guard and the count including this request.
    fn open() -> (Self, usize) {
        let now = CONCURRENT.fetch_add(1, Ordering::Relaxed) + 1;
        (Self, now)
    }
}

impl Drop for Concurrency {
    fn drop(&mut self) {
        CONCURRENT.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The outbound body, either in RAM or on disk.
///
/// A fallback channel call needs the prompt again, possibly twenty minutes
/// later, so it cannot simply be dropped after the first call. The prompt is
/// only wanted for the upload, about 0.4s of a 2,067s request, so above
/// `spill_above_concurrent` concurrent client requests it goes to a scratch
/// file and each channel call reads it back. A few milliseconds of SSD read
/// against a 34 minute request, and the 3.8MB is not resident in between.
///
/// Below the threshold nothing touches disk.
#[derive(Debug)]
pub enum PromptBody {
    /// Held in RAM for the life of the race.
    Memory(Bytes),
    /// Written to a scratch file, deleted on drop.
    Spilled(PathBuf),
}

impl PromptBody {
    /// Spills when `concurrent` is above `spill_above_concurrent`. A write
    /// that fails falls back to RAM: a full scratch disk must not fail the
    /// client request.
    pub async fn new(bytes: Bytes, concurrent: usize, config: &RouterConfig) -> Self {
        if concurrent <= config.spill_above_concurrent {
            return Self::Memory(bytes);
        }
        let name = format!(
            "{}-{}.body",
            std::process::id(),
            SPILL_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let path = config.spill_path.join(name);
        match tokio::fs::write(&path, &bytes).await {
            Ok(()) => Self::Spilled(path),
            Err(err) => {
                tracing::warn!(error = %err, path = %path.display(), "outbound body could not spill");
                Self::Memory(bytes)
            }
        }
    }

    /// The body for one channel call. The caller drops it once the upload is
    /// done, which is the whole point of spilling.
    pub async fn load(&self) -> std::io::Result<Bytes> {
        match self {
            Self::Memory(bytes) => Ok(bytes.clone()),
            Self::Spilled(path) => tokio::fs::read(path).await.map(Bytes::from),
        }
    }

    /// Where the body was spilled, for the tests.
    pub fn spill_path(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) => None,
            Self::Spilled(path) => Some(path),
        }
    }
}

impl Drop for PromptBody {
    fn drop(&mut self) {
        // The client has its answer, so the scratch file goes now rather than
        // waiting for a sweeper. An unlink is cheap enough to do inline.
        if let Self::Spilled(path) = self {
            if let Err(err) = std::fs::remove_file(&path) {
                tracing::warn!(error = %err, path = %path.display(), "spilled body could not be removed");
            }
        }
    }
}

/// Everything a handler needs. Cloned per request as one `Arc`.
#[derive(Debug)]
pub struct AppState {
    /// Read once at boot. A change needs a restart.
    pub config: Arc<Config>,
    /// The sorted channel list.
    pub catalogue: Arc<Catalogue>,
    /// Backoff state at all three levels.
    pub health: Arc<HealthTable>,
    /// `None` when `cache.enabled` is false.
    pub cache: Option<Arc<Cache>>,
    /// Runs the walk.
    pub racer: Arc<Racer>,
    /// The RSS gate.
    pub admission: Admission,
    /// Prometheus.
    pub metrics: Arc<Metrics>,
    /// For the uptime on `/healthz`.
    pub started_at: Instant,
}

/// The four routes plus the two probes.
///
/// The body limit is off. axum's default is 2 MiB, and the `Bytes` extractor
/// would refuse a 3.8MB prompt with a bare plain-text 413 before a handler
/// ever ran: no `largest_context` figure, no `too_large` counter, and a 413
/// that means something else entirely. DESIGN.md gives 413 one meaning, no
/// channel has a context window big enough, and makes the RSS gate in
/// `admission.rs` the only admission control.
pub fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/v1/messages", post(post_messages))
        .route("/v1/chat/completions", post(post_chat_completions))
        .route("/v1/models", get(crate::models_api::get_models))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state)
}

/// `POST /v1/messages`. Anthropic format. Claude Code's path.
pub async fn post_messages(
    state: axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    completion_response(state.0, Format::AnthropicMessages, &headers, body).await
}

/// `POST /v1/chat/completions`. OpenAI format.
pub async fn post_chat_completions(
    state: axum::extract::State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    completion_response(state.0, Format::OpenAiChat, &headers, body).await
}

/// `GET /healthz`. Returns 200 from the first second, with the live channel
/// count, the unhealthy providers and pairs, and when each is next due.
pub async fn healthz(state: axum::extract::State<Arc<AppState>>) -> Response {
    let now = now_unix();
    let channels = state.catalogue.snapshot();
    let eligible = channels.iter().filter(|c| c.eligible(now)).count();

    let unhealthy: Vec<Value> = state
        .health
        .in_backoff(now)
        .into_iter()
        .map(|(key, entry)| {
            let (kind, name) = describe_health_key(&key);
            json!({
                "kind": kind,
                "name": name,
                "retry_counter": entry.retry_counter,
                "retry_base_secs": entry.retry_base_secs,
                "eligible_at": entry.eligible_at,
                "eligible_in_secs": entry.eligible_at.saturating_sub(now),
            })
        })
        .collect();

    let cache_entries = state
        .cache
        .as_ref()
        .and_then(|cache| cache.entry_count().ok());

    axum::Json(json!({
        "status": "ok",
        "uptime_seconds": state.started_at.elapsed().as_secs(),
        "channels": {
            "total": channels.len(),
            "eligible": eligible,
        },
        "unhealthy": unhealthy,
        "cache_entries": cache_entries,
    }))
    .into_response()
}

/// `GET /metrics`. Prometheus text.
pub async fn metrics_endpoint(state: axum::extract::State<Arc<AppState>>) -> Response {
    if let Ok(rss) = crate::admission::read_rss_bytes() {
        state.metrics.set_rss(rss);
    }
    if let Some(cache) = state.cache.as_ref() {
        if let Ok(entries) = cache.entry_count() {
            state.metrics.set_cache_entries(entries);
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

/// Admits the request, parses the body once, then runs the shared completion
/// path and answers with whatever it decided.
///
/// Nothing is written to the client until the outcome is known. There is no
/// keepalive, and there must never be one: the status line goes out before the
/// first body byte, so any padding sent while the race is still walking would
/// commit the response to `200` and then have to deliver a `502` or a `504`
/// inside a successful body. DESIGN.md "Status codes" gives five outcomes one
/// code each, and "Why 503 and 504 are separate" needs the caller to tell them
/// apart. The `504` path is the slow one by design, so a keepalive would break
/// the contract for exactly the failure that takes longest to decide.
async fn completion_response(
    state: Arc<AppState>,
    client_format: Format,
    headers: &HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // The gate runs before the body is parsed, because parsing is where the
    // memory goes.
    if let Admit::Reject {
        rss,
        high_water,
        retry_after,
    } = state.admission.check()
    {
        state.metrics.record_request(Outcome::NotAdmitted);
        return ApiError::NotAdmitted {
            rss,
            high_water,
            retry_after_secs: retry_after.as_secs().max(1),
        }
        .into_response();
    }

    tracing::info!(
        client_key = %client_key_prefix(headers),
        format = ?client_format,
        bytes = body.len(),
        "request"
    );

    let raw_len = body.len();
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return ApiError::BadRequest {
                reason: format!("body is not valid JSON: {err}"),
            }
            .into_response()
        }
    };
    drop(body);

    handle_completion(state, client_format, parsed, raw_len).await
}

/// The shared path behind both completion endpoints: admit, hash, read the
/// cache, build the filter, race, write the cache, translate out.
async fn handle_completion(
    state: Arc<AppState>,
    client_format: Format,
    body: Value,
    raw_len: usize,
) -> Response {
    let started = Instant::now();
    let (_concurrency, concurrent) = Concurrency::open();

    let request = match crate::wire::decode_request(client_format, &body) {
        Ok(request) => request,
        Err(err) => {
            return ApiError::BadRequest {
                reason: err.to_string(),
            }
            .into_response()
        }
    };
    drop(body);

    let named_model = crate::wire::named_model(&request);
    let now = now_unix();

    // Every answer is a whole one, so every request can be served from the
    // cache.
    let key = match state.cache.as_ref() {
        Some(_) => crate::cache::cache_key(&request).ok(),
        None => None,
    };
    if let (Some(cache), Some(key)) = (state.cache.as_ref(), key) {
        match cache.get(key, named_model.as_deref(), now) {
            Ok(Some(entry)) => {
                if let Some(response) =
                    cached_response(&entry, client_format, named_model.as_deref())
                {
                    state.metrics.record_request(Outcome::HitCache);
                    tracing::info!(
                        outcome = "hit_cache",
                        model = %entry.model_id,
                        channel = %entry.channel_id,
                        ms = started.elapsed().as_millis() as u64,
                        "served"
                    );
                    return response;
                }
            }
            Ok(None) => {}
            Err(err) => tracing::warn!(error = %err, "cache read failed"),
        }
    }

    // The filter, built once and reused by every query this request makes.
    // It carries no clock: each query reads the time itself, so a channel can
    // recover mid-walk.
    let prompt_tokens = crate::catalogue::estimate_tokens(raw_len);
    let output_tokens = crate::wire::requested_output_tokens(&request);
    let filter = ChannelFilter {
        min_context: min_context(prompt_tokens, output_tokens),
        needs_tools: crate::wire::has_tools(&request),
        named_model: named_model.clone(),
    };

    if let Some(error) = pre_race_rejection(&state.catalogue, &filter, prompt_tokens) {
        if let Some(outcome) = error.outcome() {
            state.metrics.record_request(outcome);
        }
        return error.into_response();
    }

    // Encoded once and shared. Below the spill threshold it stays in RAM and
    // cloning never copies the prompt; above it the race reads it back from a
    // scratch file per channel call.
    let outbound = match outbound_body(&request) {
        Ok(bytes) => Arc::new(PromptBody::new(bytes, concurrent, &state.config.router).await),
        Err(err) => {
            return ApiError::BadRequest {
                reason: err.to_string(),
            }
            .into_response()
        }
    };
    drop(request);

    // Dropping this future, which is what axum does when the client goes
    // away, fires the token and tears down every channel call.
    let cancel = CancellationToken::new();
    let guard = cancel.clone().drop_guard();

    let outcome = state
        .racer
        .run(
            RaceInput {
                outbound,
                filter,
                // Never ask a channel for a stream: whole responses only.
                streaming: false,
                named_model: named_model.clone(),
            },
            cancel,
        )
        .await;
    drop(guard);

    match outcome {
        RaceOutcome::Buffered {
            winner,
            also_finished,
            attempts,
        } => {
            if let (Some(cache), Some(key)) = (state.cache.as_ref(), key) {
                let best = crate::race::best_for_cache(&winner, &also_finished);
                write_cache(cache, key, best, now);
            }

            let served = named_model.as_deref().unwrap_or(winner.model_id.as_str());
            let encoded =
                match crate::wire::encode_response(&winner.response, client_format, Some(served)) {
                    Ok(value) => value,
                    Err(err) => {
                        state.metrics.record_request(Outcome::NoChannel);
                        return ApiError::NoChannel {
                            attempts,
                            last_error: Some(format!("could not translate the reply: {err}")),
                        }
                        .into_response();
                    }
                };

            let total = started.elapsed();
            state
                .metrics
                .record_latency(winner.latency, total.saturating_sub(winner.latency));
            state.metrics.record_request(Outcome::Success);
            tracing::info!(
                outcome = "success",
                channel = %winner.channel_id,
                provider = %winner.provider,
                model = %winner.model_id,
                attempts,
                ms = total.as_millis() as u64,
                "served"
            );
            axum::Json(encoded).into_response()
        }

        // The race is only ever asked for buffered calls, so neither streaming
        // outcome can happen. Treat one as a routing failure rather than
        // pretend it is a reply.
        RaceOutcome::Streamed { attempts, .. } => {
            state.metrics.record_request(Outcome::NoChannel);
            tracing::error!(attempts, "the race streamed a reply nothing asked for");
            ApiError::NoChannel {
                attempts,
                last_error: Some("the race streamed a reply nothing asked for".to_string()),
            }
            .into_response()
        }

        RaceOutcome::NoNetwork { failed } => {
            state.metrics.record_request(Outcome::NoNetwork);
            tracing::info!(
                outcome = "no_network",
                ms = started.elapsed().as_millis() as u64,
                "served"
            );
            ApiError::NoNetwork { failed }.into_response()
        }

        RaceOutcome::Exhausted {
            attempts,
            last_error,
        } => {
            state.metrics.record_request(Outcome::NoChannel);
            tracing::info!(
                outcome = "no_channel",
                attempts,
                ms = started.elapsed().as_millis() as u64,
                "served"
            );
            ApiError::NoChannel {
                attempts,
                last_error,
            }
            .into_response()
        }

        RaceOutcome::StreamDied { channel_id, error } => {
            state.metrics.record_request(Outcome::NoChannel);
            tracing::error!(channel = %channel_id, error = %error, "a stream died that nothing asked for");
            ApiError::NoChannel {
                attempts: 1,
                last_error: Some(error),
            }
            .into_response()
        }

        RaceOutcome::ClientGone => {
            tracing::debug!("client went away");
            StatusCode::from_u16(499)
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
                .into_response()
        }
    }
}

/// The checks that can end a request before any channel call: a cold
/// catalogue, a model nothing serves, and a prompt nothing can hold.
fn pre_race_rejection(
    catalogue: &Catalogue,
    filter: &ChannelFilter,
    prompt_tokens: u32,
) -> Option<ApiError> {
    let channels = catalogue.snapshot();

    // A cold start has no rows at all, which is a 504 rather than a 404 or a
    // 413: nothing is known yet about any model.
    if channels.is_empty() {
        return Some(ApiError::NoChannel {
            attempts: 0,
            last_error: Some("no channels in the catalogue".to_string()),
        });
    }

    if let Some(model) = filter.named_model.as_deref() {
        // Case-insensitive, to match `Channel::matches`: a stricter gate here
        // would 404 a model the race would have served.
        if !channels
            .iter()
            .any(|c| c.model_id.eq_ignore_ascii_case(model))
        {
            return Some(ApiError::UnknownModel {
                model: model.to_string(),
            });
        }
    }

    // When the caller named a model, only that model's channels can serve the
    // request, so the 413 test and the figure it reports are taken over that
    // subset. Taking them over the whole catalogue lets an unrelated large
    // context row hide an over-long prompt, which then comes back as a 504.
    let largest = match filter.named_model.as_deref() {
        Some(model) => channels
            .iter()
            .filter(|c| c.model_id.eq_ignore_ascii_case(model))
            .map(|c| c.limit_context)
            .max()
            .unwrap_or(0),
        None => crate::catalogue::largest_context(&channels),
    };
    if filter.min_context > largest {
        return Some(ApiError::TooLarge {
            prompt_tokens,
            largest_context: largest,
        });
    }

    None
}

/// The body every channel call sends, always with `stream: false`.
///
/// DESIGN.md decided whole responses only, so a client asking for a stream
/// never turns into a streamed channel call. Encoded once, into the one format
/// `DESIGN.md` says every channel speaks.
///
/// The `model` field is removed here. The client's model string means nothing
/// to a provider, and the race decides which model is called, so each channel
/// call puts its own `model_id` in front of this body. See
/// [`crate::upstream::SharedBody`].
fn outbound_body(
    request: &crate::wire::NeutralRequest,
) -> std::result::Result<Bytes, crate::wire::WireError> {
    let mut value = crate::wire::encode_request(request, Format::OpenAiChat)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("stream".to_string(), Value::Bool(false));
        object.remove("model");
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|err| crate::wire::WireError::Translation(err.to_string()))
}

/// Turns a stored entry back into the client's format. A body that no longer
/// deserialises is treated as a miss.
fn cached_response(
    entry: &CacheEntry,
    client_format: Format,
    named_model: Option<&str>,
) -> Option<Response> {
    let response: crate::wire::NeutralResponse = serde_json::from_slice(&entry.body)
        .map_err(|err| tracing::warn!(error = %err, "cached entry could not be read back"))
        .ok()?;
    let served = named_model.unwrap_or(entry.model_id.as_str());
    let encoded = crate::wire::encode_response(&response, client_format, Some(served))
        .map_err(|err| tracing::warn!(error = %err, "cached entry could not be translated"))
        .ok()?;
    Some(axum::Json(encoded).into_response())
}

/// The bytes stored for one answer: the neutral response with the provider's
/// own copies dropped.
///
/// `preservation` holds the provider's exact reply and `extensions` its fields
/// with no neutral equivalent, so leaving them in stores the same answer twice
/// and doubles the entry on disk. These are the fields `wire::canonical_bytes`
/// already drops for the key. The served reply is re-encoded from `outputs`,
/// which is what DESIGN.md sizes the entry at.
fn cache_body(response: &crate::wire::NeutralResponse) -> serde_json::Result<Vec<u8>> {
    let mut trimmed = response.clone();
    trimmed.preservation = Default::default();
    trimmed.extensions = Default::default();
    serde_json::to_vec(&trimmed)
}

/// Serialises the cache candidate and applies the write rule. A cache failure
/// never fails the client request.
fn write_cache(cache: &Cache, key: crate::cache::CacheKey, best: &crate::race::Winner, now: u64) {
    let body = match cache_body(&best.response) {
        Ok(body) => body,
        Err(err) => {
            tracing::warn!(error = %err, "answer could not be serialised for the cache");
            return;
        }
    };
    let entry = CacheEntry {
        schema_version: crate::cache::SCHEMA_VERSION,
        created_at: now,
        model_id: best.model_id.clone(),
        intelligence: best.intelligence,
        channel_id: best.channel_id,
        provider: best.provider.clone(),
        body,
    };
    if let Err(err) = cache.put(key, entry, now) {
        tracing::warn!(error = %err, "cache write failed");
    }
}

/// Prompt plus requested output, with headroom.
fn min_context(prompt_tokens: u32, output_tokens: u32) -> u32 {
    let total = u64::from(prompt_tokens) + u64::from(output_tokens);
    let with_margin = total * (100 + CONTEXT_MARGIN_PERCENT) / 100;
    with_margin.min(u64::from(u32::MAX)) as u32
}

/// The first 6 characters of the client's key, for telling two tools apart in
/// the log. The value is never checked and never forwarded.
fn client_key_prefix(headers: &HeaderMap) -> String {
    let raw = headers
        .get(header::AUTHORIZATION)
        .or_else(|| headers.get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let value = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
    if value.is_empty() {
        return "none".to_string();
    }
    value.chars().take(6).collect()
}

/// A health key as two strings for the `/healthz` body. The key label is a
/// stable name, never the key itself.
fn describe_health_key(key: &crate::health::HealthKey) -> (&'static str, String) {
    match key {
        crate::health::HealthKey::Channel(id) => ("channel", id.to_string()),
        crate::health::HealthKey::ProviderKey { provider, key_id } => {
            ("provider_key", format!("{provider}/{key_id}"))
        }
        crate::health::HealthKey::Provider(provider) => ("provider", provider.clone()),
    }
}

/// Unix seconds now. A clock before the epoch gives 0.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The four failures, one code each. Every body says which of four different
/// things went wrong, because each needs a different response from the caller.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// `413`. The filter emptied the list because no model has a large enough
    /// context window. The caller's problem; no wait helps.
    #[error("prompt too large")]
    TooLarge {
        /// Estimated prompt tokens.
        prompt_tokens: u32,
        /// The largest `limit_context` found across the channel list.
        largest_context: u32,
    },
    /// `502`. Both network test calls failed, so the local link is down.
    #[error("no network")]
    NoNetwork {
        /// Which test URLs failed.
        failed: Vec<String>,
    },
    /// `503`. RSS is over `memory_high_water`. Transient; waiting fixes it.
    #[error("not admitted")]
    NotAdmitted {
        /// Resident set size at the decision.
        rss: u64,
        /// The configured ceiling.
        high_water: u64,
        /// Seconds for the `Retry-After` header.
        retry_after_secs: u64,
    },
    /// `504`. No channel could serve it: none eligible, or every channel call
    /// failed. The upstreams' problem.
    #[error("no channel")]
    NoChannel {
        /// Channel calls made.
        attempts: u32,
        /// The last upstream error.
        last_error: Option<String>,
    },
    /// `404`. The caller named a model no channel serves. Never substitute a
    /// different one.
    #[error("unknown model")]
    UnknownModel {
        /// What the caller asked for.
        model: String,
    },
    /// `400`. The body is not a valid request in its endpoint's format.
    #[error("bad request: {reason}")]
    BadRequest {
        /// What was wrong with it.
        reason: String,
    },
}

impl ApiError {
    /// The status this failure returns.
    pub fn status(&self) -> StatusCode {
        match self {
            ApiError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::NoNetwork { .. } => StatusCode::BAD_GATEWAY,
            ApiError::NotAdmitted { .. } => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::NoChannel { .. } => StatusCode::GATEWAY_TIMEOUT,
            ApiError::UnknownModel { .. } => StatusCode::NOT_FOUND,
            ApiError::BadRequest { .. } => StatusCode::BAD_REQUEST,
        }
    }

    /// The short tag, matching the `/metrics` outcome label where one exists.
    pub fn tag(&self) -> &'static str {
        match self {
            ApiError::TooLarge { .. } => "too_large",
            ApiError::NoNetwork { .. } => "no_network",
            ApiError::NotAdmitted { .. } => "not_admitted",
            ApiError::NoChannel { .. } => "no_channel",
            ApiError::UnknownModel { .. } => "unknown_model",
            ApiError::BadRequest { .. } => "bad_request",
        }
    }

    /// The `/metrics` outcome, for the failures that have one. A `404` and a
    /// `400` are the caller's own mistake and are not counted as routing
    /// outcomes.
    fn outcome(&self) -> Option<Outcome> {
        match self {
            ApiError::TooLarge { .. } => Some(Outcome::TooLarge),
            ApiError::NoNetwork { .. } => Some(Outcome::NoNetwork),
            ApiError::NotAdmitted { .. } => Some(Outcome::NotAdmitted),
            ApiError::NoChannel { .. } => Some(Outcome::NoChannel),
            ApiError::UnknownModel { .. } | ApiError::BadRequest { .. } => None,
        }
    }

    /// The numbers specific to this code.
    fn detail(&self) -> Value {
        match self {
            ApiError::TooLarge {
                prompt_tokens,
                largest_context,
            } => json!({ "prompt_tokens": prompt_tokens, "largest_context": largest_context }),
            ApiError::NoNetwork { failed } => json!({ "failed": failed }),
            ApiError::NotAdmitted {
                rss,
                high_water,
                retry_after_secs,
            } => json!({
                "rss": rss,
                "memory_high_water": high_water,
                "retry_after_secs": retry_after_secs,
            }),
            ApiError::NoChannel {
                attempts,
                last_error,
            } => json!({ "attempts": attempts, "last_error": last_error }),
            ApiError::UnknownModel { model } => json!({ "model": model }),
            ApiError::BadRequest { reason } => json!({ "reason": reason }),
        }
    }

    /// One sentence for a human.
    fn message(&self) -> String {
        match self {
            ApiError::TooLarge {
                prompt_tokens,
                largest_context,
            } => format!(
                "the prompt is about {prompt_tokens} tokens and the largest context window on any channel is {largest_context}"
            ),
            ApiError::NoNetwork { .. } => {
                "both network test calls failed, so the router cannot reach the internet".to_string()
            }
            ApiError::NotAdmitted {
                rss, high_water, ..
            } => format!(
                "resident memory is {rss} bytes, over the {high_water} byte ceiling, so the request was not admitted"
            ),
            ApiError::NoChannel { attempts, .. } => {
                format!("no channel could serve the request after {attempts} channel calls")
            }
            ApiError::UnknownModel { model } => {
                format!("no channel serves the model {model}")
            }
            ApiError::BadRequest { reason } => reason.clone(),
        }
    }
}

/// The JSON body each failure carries.
#[derive(Debug, Serialize)]
pub struct ErrorBody {
    /// Short machine-readable tag, matching the `/metrics` outcome label.
    pub error: String,
    /// One sentence for a human.
    pub message: String,
    /// The numbers specific to this code.
    pub detail: serde_json::Value,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let retry_after = match &self {
            ApiError::NotAdmitted {
                retry_after_secs, ..
            } => Some(*retry_after_secs),
            _ => None,
        };
        let body = ErrorBody {
            error: self.tag().to_string(),
            message: self.message(),
            detail: self.detail(),
        };

        let mut response = (status, axum::Json(body)).into_response();
        if let Some(secs) = retry_after {
            if let Ok(value) = secs.to_string().parse() {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored entry must hold the answer once. The translation crate's
    /// default policy keeps the provider's exact reply in `preservation`, so
    /// serialising the whole neutral response writes the same text twice and
    /// doubles the cache on disk.
    #[test]
    fn a_cache_entry_does_not_store_the_answer_twice() {
        let answer = "x".repeat(10_000);
        let provider_reply = json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        let response = crate::wire::decode_response(&provider_reply, Format::OpenAiChat)
            .expect("the provider reply decodes");

        let count = |bytes: &[u8]| {
            bytes
                .windows(answer.len())
                .filter(|window| *window == answer.as_bytes())
                .count()
        };
        assert_eq!(
            count(&serde_json::to_vec(&response).expect("the whole response serialises")),
            2,
            "the whole neutral response carries the answer in `outputs` and again \
             in `preservation`, which is the growth this test guards against"
        );

        let body = cache_body(&response).expect("the cache body serialises");
        assert_eq!(count(&body), 1, "the stored entry holds the answer once");

        // Stripping must not cost the answer: the entry still serves.
        let stored: crate::wire::NeutralResponse =
            serde_json::from_slice(&body).expect("the stored entry reads back");
        let served = crate::wire::encode_response(&stored, Format::OpenAiChat, Some("served"))
            .expect("the stored entry translates");
        assert_eq!(served["choices"][0]["message"]["content"], answer);
        assert_eq!(served["model"], "served");
    }

    /// A scratch directory of this test's own.
    fn spill_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("llmrouter-spill-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("the scratch directory is created");
        dir
    }

    /// The 404 gate must use the same comparison as `Channel::matches`, which
    /// ignores ASCII case. A stricter gate turns away a model the race serves.
    #[test]
    fn a_named_model_in_another_case_is_not_rejected_before_the_race() {
        use crate::catalogue::{Channel, ChannelHealth, ChannelId, ChannelTelemetry};
        use crate::WireName;

        let channel = Channel {
            id: ChannelId::new("openrouter", "sk-1", "claude-fable-5-1"),
            provider: "openrouter".into(),
            model_id: "claude-fable-5-1".into(),
            key_id: "key-0".into(),
            key: "sk-1".into(),
            base_url: "https://example.invalid/v1".into(),
            wire: WireName::OpenAi,
            limit_context: 128_000,
            limit_output: 8_192,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_string()],
            intelligence: 900,
            intelligence_rank: 1,
            health: ChannelHealth::live(),
            telemetry: ChannelTelemetry::default(),
        };

        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel]);

        let filter = ChannelFilter {
            min_context: 1_000,
            needs_tools: true,
            named_model: Some("Claude-Fable-5-1".to_string()),
        };

        assert!(
            pre_race_rejection(&catalogue, &filter, 100).is_none(),
            "a model that differs only in case is served, not 404ed"
        );

        let unknown = ChannelFilter {
            named_model: Some("no-such-model".to_string()),
            ..filter
        };
        assert!(
            matches!(
                pre_race_rejection(&catalogue, &unknown, 100),
                Some(ApiError::UnknownModel { .. })
            ),
            "a model nothing serves is still a 404"
        );
    }

    #[tokio::test]
    async fn below_the_threshold_the_outbound_body_stays_in_ram() {
        let dir = spill_dir("ram");
        let config = RouterConfig {
            spill_above_concurrent: 100,
            spill_path: dir.clone(),
            ..RouterConfig::default()
        };

        let prompt = Bytes::from_static(b"{}");
        let body = PromptBody::new(prompt.clone(), 100, &config).await;

        assert!(
            body.spill_path().is_none(),
            "nothing spills at the threshold"
        );
        assert_eq!(body.load().await.unwrap(), prompt);
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "no scratch file is written below the threshold"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn above_the_threshold_the_outbound_body_goes_to_disk_and_is_deleted() {
        let dir = spill_dir("disk");
        let config = RouterConfig {
            spill_above_concurrent: 100,
            spill_path: dir.clone(),
            ..RouterConfig::default()
        };

        let prompt = Bytes::from(vec![b'x'; 4096]);
        let body = PromptBody::new(prompt.clone(), 101, &config).await;

        let path = body
            .spill_path()
            .expect("above the threshold it spills")
            .to_path_buf();
        assert!(path.is_file(), "the body is on disk, not in RAM");
        // A fallback channel call, twenty minutes later, still gets the prompt.
        assert_eq!(body.load().await.unwrap(), prompt);

        drop(body);
        assert!(
            !path.exists(),
            "the scratch file goes when the client has its answer"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_outbound_body_carries_no_model_for_the_channel_to_overwrite() {
        // The client names its own model. Nothing in that name means anything
        // to a provider, so the shared body leaves the field out and each
        // channel call prefixes its own `model_id`.
        let request = crate::wire::decode_request(
            Format::OpenAiChat,
            &json!({
                "model": "claude-sonnet-4-5",
                "messages": [{"role": "user", "content": "hi"}],
            }),
        )
        .expect("decodes");

        let body = outbound_body(&request).expect("encodes");
        let value: Value = serde_json::from_slice(&body).expect("valid json");
        assert!(
            value.get("model").is_none(),
            "the shared body must not name the client's model: {value}"
        );
        assert_eq!(value["stream"], false);

        // What the channel actually posts.
        let sent: Value =
            serde_json::from_slice(&crate::upstream::SharedBody::new(body).joined("llama-3.3-70b"))
                .expect("valid json");
        assert_eq!(sent["model"], "llama-3.3-70b");
    }

    #[test]
    fn a_streaming_client_still_gets_a_whole_answer_from_a_whole_channel_call() {
        // Claude Code sends `stream: true` by default. DESIGN.md says whole
        // responses only, `stream: false` upstream, so the client's flag must
        // never reach a provider: a streamed channel call cannot be cached and
        // cannot fall through to the next channel once it has started.
        let request = crate::wire::decode_request(
            Format::OpenAiChat,
            &json!({
                "model": "claude-sonnet-4-5",
                "messages": [{"role": "user", "content": "fix this bug"}],
                "stream": true,
            }),
        )
        .expect("decodes");
        assert!(request.stream, "the client did ask for a stream");

        let body = outbound_body(&request).expect("the outbound body encodes");
        let value: Value = serde_json::from_slice(&body).expect("the outbound body is JSON");
        assert_eq!(value["stream"], Value::Bool(false));
    }

    #[test]
    fn min_context_adds_the_margin_and_never_overflows() {
        assert_eq!(min_context(1_000, 100), 1_155);
        assert_eq!(min_context(0, 0), 0);
        // A 500k token prompt is the case DESIGN.md names: it must stay a
        // real number so the 413 body can report it.
        assert_eq!(min_context(500_000, 32_000), 558_600);
        assert_eq!(min_context(u32::MAX, u32::MAX), u32::MAX);
    }

    #[test]
    fn each_failure_keeps_its_own_code_and_tag() {
        let cases = [
            (
                ApiError::TooLarge {
                    prompt_tokens: 1,
                    largest_context: 2,
                },
                413,
                "too_large",
            ),
            (ApiError::NoNetwork { failed: vec![] }, 502, "no_network"),
            (
                ApiError::NotAdmitted {
                    rss: 1,
                    high_water: 2,
                    retry_after_secs: 3,
                },
                503,
                "not_admitted",
            ),
            (
                ApiError::NoChannel {
                    attempts: 0,
                    last_error: None,
                },
                504,
                "no_channel",
            ),
            (
                ApiError::UnknownModel {
                    model: "x".to_string(),
                },
                404,
                "unknown_model",
            ),
            (
                ApiError::BadRequest {
                    reason: "x".to_string(),
                },
                400,
                "bad_request",
            ),
        ];
        for (error, status, tag) in cases {
            assert_eq!(error.status().as_u16(), status, "{tag}");
            assert_eq!(error.tag(), tag);
        }
    }

    #[test]
    fn the_413_body_names_both_numbers() {
        let detail = ApiError::TooLarge {
            prompt_tokens: 500_000,
            largest_context: 200_000,
        }
        .detail();
        assert_eq!(detail["prompt_tokens"], json!(500_000));
        assert_eq!(detail["largest_context"], json!(200_000));
    }

    #[test]
    fn the_502_body_names_the_failed_urls() {
        let detail = ApiError::NoNetwork {
            failed: vec![
                crate::net::TEST_URL_GOOGLE.to_string(),
                crate::net::TEST_URL_CLOUDFLARE.to_string(),
            ],
        }
        .detail();
        assert_eq!(detail["failed"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn the_504_body_names_the_attempts_and_the_last_error() {
        let detail = ApiError::NoChannel {
            attempts: 7,
            last_error: Some("401 from the key".to_string()),
        }
        .detail();
        assert_eq!(detail["attempts"], json!(7));
        assert_eq!(detail["last_error"], json!("401 from the key"));
    }

    #[test]
    fn only_the_503_carries_retry_after() {
        let response = ApiError::NotAdmitted {
            rss: 4_000_000_000,
            high_water: 3_221_225_472,
            retry_after_secs: 30,
        }
        .into_response();
        assert_eq!(response.status().as_u16(), 503);
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "30");

        let response = ApiError::NoChannel {
            attempts: 1,
            last_error: None,
        }
        .into_response();
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
    }

    #[test]
    fn the_503_body_carries_all_three_numbers() {
        let detail = ApiError::NotAdmitted {
            rss: 4_000_000_000,
            high_water: 3_221_225_472,
            retry_after_secs: 30,
        }
        .detail();
        assert_eq!(detail["rss"], json!(4_000_000_000u64));
        assert_eq!(detail["memory_high_water"], json!(3_221_225_472u64));
        assert_eq!(detail["retry_after_secs"], json!(30));
    }

    #[test]
    fn metrics_outcomes_line_up_with_the_status_codes() {
        let pairs = [
            ApiError::TooLarge {
                prompt_tokens: 0,
                largest_context: 0,
            },
            ApiError::NoNetwork { failed: vec![] },
            ApiError::NotAdmitted {
                rss: 0,
                high_water: 0,
                retry_after_secs: 1,
            },
            ApiError::NoChannel {
                attempts: 0,
                last_error: None,
            },
        ];
        for error in pairs {
            let outcome = error.outcome().expect("this failure is a routing outcome");
            assert_eq!(outcome.status(), error.status().as_u16());
            assert_eq!(outcome.label(), error.tag());
        }

        // A 404 and a 400 are the caller's own mistake, so they carry no
        // routing outcome. Counting them as no_channel would make a bad model
        // name look like broken providers.
        for error in [
            ApiError::UnknownModel {
                model: "gone".to_string(),
            },
            ApiError::BadRequest {
                reason: "nonsense".to_string(),
            },
        ] {
            assert!(
                error.outcome().is_none(),
                "{} is not a routing outcome",
                error.tag()
            );
        }
    }

    #[test]
    fn the_client_key_is_logged_six_characters_at_a_time() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sk-abcdefghijklmnop".parse().unwrap(),
        );
        assert_eq!(client_key_prefix(&headers), "sk-abc");

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "sk-ant-xyz".parse().unwrap());
        assert_eq!(client_key_prefix(&headers), "sk-ant");

        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer ab".parse().unwrap());
        assert_eq!(client_key_prefix(&headers), "ab");

        assert_eq!(client_key_prefix(&HeaderMap::new()), "none");
    }

    #[test]
    fn health_keys_describe_themselves_without_the_key() {
        let (kind, name) =
            describe_health_key(&crate::health::HealthKey::Provider("groq".to_string()));
        assert_eq!((kind, name.as_str()), ("provider", "groq"));

        let (kind, name) = describe_health_key(&crate::health::HealthKey::ProviderKey {
            provider: "gemini".to_string(),
            key_id: "key-3".to_string(),
        });
        assert_eq!((kind, name.as_str()), ("provider_key", "gemini/key-3"));

        let (kind, name) = describe_health_key(&crate::health::HealthKey::Channel(
            crate::catalogue::ChannelId(0xdead_beef),
        ));
        assert_eq!(kind, "channel");
        assert_eq!(name, "00000000deadbeef");
    }

    /// The keepalive that used to wrap this path wrote `200 OK` once
    /// `keepalive_interval` had passed, then appended the real failure to the
    /// body. Every failure must carry its own status line instead, including
    /// the slow `504`: DESIGN.md "Why 503 and 504 are separate" needs the
    /// caller to tell a full router from a dead upstream.
    #[tokio::test]
    async fn every_failure_carries_its_own_status_line() {
        let cases: Vec<(ApiError, StatusCode)> = vec![
            (
                ApiError::NoChannel {
                    attempts: 4096,
                    last_error: Some("401 invalid api key".to_string()),
                },
                StatusCode::GATEWAY_TIMEOUT,
            ),
            (
                ApiError::NoNetwork {
                    failed: vec!["https://example.invalid".to_string()],
                },
                StatusCode::BAD_GATEWAY,
            ),
            (
                ApiError::NotAdmitted {
                    rss: 4,
                    high_water: 2,
                    retry_after_secs: 5,
                },
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                ApiError::TooLarge {
                    prompt_tokens: 9,
                    largest_context: 8,
                },
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            (
                ApiError::UnknownModel {
                    model: "nope".to_string(),
                },
                StatusCode::NOT_FOUND,
            ),
            (
                ApiError::BadRequest {
                    reason: "not json".to_string(),
                },
                StatusCode::BAD_REQUEST,
            ),
        ];

        for (error, expected) in cases {
            let tag = error.tag();
            let response = error.into_response();
            assert_eq!(response.status(), expected, "{tag} keeps its own code");
            assert_ne!(
                response.status(),
                StatusCode::OK,
                "{tag} must never be delivered inside a 200"
            );
        }
    }

    /// Every key expired overnight: the walk takes far longer than any
    /// keepalive interval and still has to end as a `504` whose body names the
    /// channel calls made and the last upstream error.
    #[tokio::test]
    async fn the_slow_no_channel_outcome_is_a_504_with_attempts_and_last_error() {
        let response = ApiError::NoChannel {
            attempts: 4096,
            last_error: Some("401 invalid api key".to_string()),
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("the error body reads back");
        let body: Value = serde_json::from_slice(&bytes).expect("the body is JSON on its own");
        assert_eq!(body["error"], "no_channel");
        assert_eq!(body["detail"]["attempts"], 4096);
        assert_eq!(body["detail"]["last_error"], "401 invalid api key");
    }

    /// A cold catalogue is the same failure decided early: still a `504`.
    #[test]
    fn a_cold_catalogue_rejects_with_no_channel() {
        let filter = ChannelFilter {
            min_context: 1,
            needs_tools: false,
            named_model: None,
        };
        let error = pre_race_rejection(&Catalogue::new(), &filter, 1).expect("a cold start fails");
        assert_eq!(error.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    /// A named model whose own rows are all too small is a `413` naming that
    /// model's largest window, not a `504`. A roomier row for some other model
    /// must not hide the over-long prompt: the caller named a model, so only
    /// that model's channels can serve the request.
    #[test]
    fn a_named_model_too_small_for_the_prompt_is_too_large_not_no_channel() {
        use crate::catalogue::{Channel, ChannelHealth, ChannelId, ChannelTelemetry};
        use crate::config::WireName;

        fn row(model: &str, limit_context: u32) -> Channel {
            Channel {
                id: ChannelId::new("p", "sk-1", model),
                provider: "p".into(),
                model_id: model.into(),
                key_id: "key-0".into(),
                key: "sk-1".into(),
                base_url: "https://example.invalid/v1".into(),
                wire: WireName::OpenAi,
                limit_context,
                limit_output: 8_192,
                tool_call: true,
                reasoning: false,
                structured_output: false,
                modalities_input: vec!["text".to_string()],
                intelligence: 1_000,
                intelligence_rank: 0,
                health: ChannelHealth::live(),
                telemetry: ChannelTelemetry::default(),
            }
        }

        let catalogue = Catalogue::new();
        catalogue.replace(vec![
            row("glm-5.3-air", 128_000),
            row("gemini-3-pro", 2_000_000),
        ]);

        let filter = ChannelFilter {
            min_context: 200_000,
            needs_tools: false,
            named_model: Some("glm-5.3-air".to_string()),
        };
        let error =
            pre_race_rejection(&catalogue, &filter, 200_000).expect("the prompt is too big");
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
        match error {
            ApiError::TooLarge {
                largest_context, ..
            } => assert_eq!(largest_context, 128_000),
            other => panic!("expected a 413, got {other:?}"),
        }

        // With no model named, the roomier row does serve the request.
        let open = ChannelFilter {
            named_model: None,
            ..filter
        };
        assert!(pre_race_rejection(&catalogue, &open, 200_000).is_none());
    }

    /// A client body above axum's 2 MiB `DefaultBodyLimit` must reach the
    /// handler. DESIGN.md sizes the whole system around a 3.8MB prompt and
    /// gives `413` one meaning only: no channel has a large enough context
    /// window. Without the limit turned off in [`router`], the `Bytes`
    /// extractor refuses this request with a bare plain-text 413, the handler
    /// never runs, and nothing is counted on `/metrics`.
    #[tokio::test]
    async fn a_body_above_two_mebibytes_reaches_the_handler() {
        use tower::ServiceExt;

        let dir = spill_dir("body-limit");
        let config = Config {
            router: RouterConfig {
                spill_path: dir.clone(),
                ..RouterConfig::default()
            },
            providers: Vec::new(),
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: std::collections::HashMap::new(),
            capabilities: std::collections::HashMap::new(),
        };
        let db = Arc::new(
            redb::Database::create(dir.join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                config.router.health_poll.retry_base,
                config.router.health_poll.backoff_max,
            )
            .expect("the health table opens"),
        );
        let upstream = crate::upstream::Upstream::new(&config.router).expect("the client builds");
        let net = crate::net::NetTest::new(
            upstream.client().clone(),
            config.router.health_poll.network_test_timeout,
        );
        let catalogue = Arc::new(Catalogue::new());
        let metrics = Arc::new(Metrics::new().expect("the recorder installs once"));
        let racer = Arc::new(Racer::new(
            Arc::new(config.router.clone()),
            Arc::clone(&catalogue),
            Arc::clone(&health),
            upstream,
            net,
            Arc::clone(&metrics),
        ));
        let state = Arc::new(AppState {
            // u64::MAX, so the RSS gate admits whatever this machine is using.
            admission: Admission::new(u64::MAX, std::time::Duration::from_secs(30)),
            config: Arc::new(config),
            catalogue,
            health,
            cache: None,
            racer,
            metrics,
            started_at: Instant::now(),
        });

        // Three MiB, past the 2 MiB default and short of the 3.8MB working
        // size. Deliberately not JSON: reaching the parser is the whole point,
        // and a `400` from the parser proves the extractor let it through.
        let body = vec![b'x'; 3 * 1024 * 1024];
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body))
            .expect("the request builds");

        let response = router(state)
            .oneshot(request)
            .await
            .expect("the router answers");

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "the handler saw the body and rejected the JSON itself, rather than \
             the extractor refusing it on size"
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the error body reads back");
        let parsed: Value = serde_json::from_slice(&bytes).expect("the body is our JSON error");
        assert_eq!(parsed["error"], "bad_request");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
