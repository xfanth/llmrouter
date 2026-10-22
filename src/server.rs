//! The axum routes.
//!
//! Translation happens at the two edges only: the inbound body becomes a
//! neutral request once, and the winning reply becomes the client's format
//! once. The race only ever sees neutral types.
//!
//! Two settings decide the shape of the reply, and they are independent.
//! `streaming` in `router.yml` is about the providers: the race always asks
//! them for a stream, because the hedge strategy needs one to measure. `stream`
//! in the client's request is about the client. A mismatch is never rejected:
//! a client that asked for a stream gets the winner forwarded as server-sent
//! events, and one that did not gets the same stream folded into a single
//! piece.
//!
//! A channel call can take 34 minutes and Claude Code hangs up at 10, so this
//! module also sends the keepalive: an SSE comment for a streaming client, a
//! leading space before the JSON body for a non-streaming one, every
//! `keepalive_interval` until the first real byte.

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::admission::{Admission, Admit};
use crate::cache::{Cache, CacheEntry};
use crate::catalogue::{Catalogue, ChannelFilter};
use crate::config::{Config, RouterConfig};
use crate::health::HealthTable;
use crate::metrics::{Metrics, Outcome, TokenKind};
use crate::race::{RaceInput, RaceOutcome, Racer};
use crate::wire::{EventStream, Format};

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
    /// Written to a scratch file: random name, mode 0600, deleted on drop.
    Spilled(tempfile::TempPath),
}

impl PromptBody {
    /// Spills when `concurrent` is above `spill_above_concurrent`. A write
    /// that fails falls back to RAM: a full scratch disk must not fail the
    /// client request.
    pub async fn new(bytes: Bytes, concurrent: usize, config: &RouterConfig) -> Self {
        if concurrent <= config.spill_above_concurrent {
            return Self::Memory(bytes);
        }
        // tempfile opens with O_EXCL at 0600 under a random name, so another
        // user on the share can neither guess the path nor read the prompt.
        let dir = config.spill_path.clone();
        let spill = bytes.clone();
        // The whole write runs as one blocking task that owns the file and
        // returns the guard. A blocking task cannot be cancelled, so if the
        // client hangs up mid-write the task still finishes, and its result,
        // the guard, is dropped with it and deletes the file. Writing through
        // the open handle also means the path is never reopened after the
        // guard may already have unlinked it.
        let written = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut file = tempfile::NamedTempFile::new_in(&dir)?;
            file.write_all(&spill)?;
            Ok::<_, std::io::Error>(Self::Spilled(file.into_temp_path()))
        })
        .await;
        match written {
            Ok(Ok(body)) => body,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, path = %config.spill_path.display(), "outbound body could not spill");
                Self::Memory(bytes)
            }
            Err(err) => {
                tracing::warn!(error = %err, path = %config.spill_path.display(), "outbound body spill task failed");
                Self::Memory(bytes)
            }
        }
    }

    /// The body for one channel call. The caller drops it once the upload is
    /// done, which is the whole point of spilling.
    pub async fn load(&self) -> std::io::Result<Bytes> {
        match self {
            Self::Memory(bytes) => Ok(bytes.clone()),
            Self::Spilled(path) => tokio::fs::read(&**path).await.map(Bytes::from),
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
    /// The backend recorded metric history for the dashboard.
    pub history: Arc<crate::history::History>,
    /// The request log the dashboard's bottom table reads.
    pub reqlog: Arc<crate::reqlog::RequestLog>,
    /// For the uptime on `/healthz`.
    pub started_at: Instant,
}

/// The four routes, the probes, the version and log pages, and the two
/// browser pages.
///
/// The body limit is off. axum's default is 2 MiB, and the `Bytes` extractor
/// would refuse a 3.8MB prompt with a bare plain-text 413 before a handler
/// ever ran: no `largest_context` figure, no `too_large` counter, and a 413
/// that means something else entirely. DESIGN.md gives 413 one meaning, no
/// channel has a context window big enough, and makes the RSS gate in
/// `admission.rs` the only admission control.
pub fn router(state: Arc<AppState>) -> axum::Router {
    axum::Router::new()
        .route("/", get(crate::dashboard::index))
        .route("/dashboard", get(crate::dashboard::dashboard_page))
        .route("/docs", get(crate::dashboard::docs_page))
        .route("/openapi.json", get(crate::dashboard::openapi_json))
        .route("/v1/messages", post(post_messages))
        .route("/v1/chat/completions", post(post_chat_completions))
        .route("/v1/models", get(crate::models_api::get_models))
        .route("/healthz", get(healthz))
        .route("/version", get(version))
        .route("/logs", get(logs))
        .route("/metrics", get(metrics_endpoint))
        .route("/metrics/history", get(crate::history::endpoint))
        .route("/metrics/requests", get(crate::reqlog::endpoint))
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

/// `GET /version`. Which build is running: the crate version, and the commit
/// and build time the image was made from. Docker passes both into the build;
/// a local `cargo build` has neither and says `unknown`.
pub async fn version() -> Response {
    axum::Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "git_sha": option_env!("LLMROUTER_GIT_SHA").unwrap_or("unknown"),
        "built_at": option_env!("LLMROUTER_BUILD_DATE").unwrap_or("unknown"),
    }))
    .into_response()
}

/// `GET /logs?lines=N`.
#[derive(Debug, serde::Deserialize)]
pub struct LogsQuery {
    /// How many lines off the end, 200 by default and at most 5,000.
    lines: Option<usize>,
}

/// `GET /logs`. The end of this run's log file as plain text, so the router
/// can be read without a shell on the host.
pub async fn logs(axum::extract::Query(query): axum::extract::Query<LogsQuery>) -> Response {
    let lines = query.lines.unwrap_or(200).clamp(1, 5_000);
    let Some(path) = crate::logging::current_log() else {
        return (
            StatusCode::NOT_FOUND,
            "this process is not logging to a file\n",
        )
            .into_response();
    };
    match tokio::task::spawn_blocking(move || crate::logging::tail(path, lines)).await {
        Ok(Ok(text)) => {
            ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response()
        }
        Ok(Err(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the log file could not be read: {err}\n"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("reading the log file failed: {err}\n"),
        )
            .into_response(),
    }
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

/// Tokens and the output rate for one finished request.
///
/// Reasoning is counted apart from completion because it is billed and
/// usually never shown. On the first live run a 42 token answer carried 487
/// reasoning tokens with it, so a single "completion" figure would have
/// described neither the bill nor what the client actually read.
///
/// `rate` is the winning stream's tokens per second over its own generation
/// window: first content token to end of stream for a streamed winner, the
/// winning channel call's own wall clock for a buffered one. Dividing by the
/// whole request instead charged the hedge window and every failed channel
/// the walk got through first to a speed the provider never delivered at: a
/// 33 token answer that streamed in a quarter of a second recorded 6.7
/// tokens a second against the whole request and some 140 over its own
/// window.
fn record_usage(
    metrics: &Metrics,
    provider: &str,
    usage: &crate::wire::NeutralUsage,
    rate: Option<f64>,
) {
    let input = usage.input_tokens.unwrap_or(0);
    let output = usage.output_tokens.unwrap_or(0);
    let reasoning = usage.reasoning_tokens.unwrap_or(0);

    metrics.record_tokens(provider, TokenKind::Prompt, input);
    metrics.record_tokens(provider, TokenKind::Completion, output);
    metrics.record_tokens(provider, TokenKind::Reasoning, reasoning);

    // A measured rate is recorded as measured. An absent, zero, negative or
    // non-finite rate is left out rather than recorded: a zero would pull the
    // histogram down and read as a slow answer, when in fact nothing was
    // measured.
    if let Some(rate) = rate.filter(|r| r.is_finite() && *r > 0.0) {
        metrics.record_output_rate(rate);
    }
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
        // Read on the scrape rather than on the request path, for the same
        // reason the entry count is: both walk redb's own metadata, and a
        // scrape happens on Prometheus's schedule rather than the client's.
        if let Ok(bytes) = cache.size_bytes() {
            state.metrics.set_cache_bytes(bytes);
        }
    }
    // The same split `/healthz` reports, so the two pages cannot disagree.
    // `eligible` is the channel's own view, which already folds in its key's
    // and its provider's backoff, so it cannot be derived by counting health
    // table rows.
    let now = now_unix();
    let channels = state.catalogue.snapshot();
    let eligible = channels.iter().filter(|c| c.eligible(now)).count() as u64;
    state
        .metrics
        .set_channel_states(eligible, channels.len() as u64 - eligible);

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

/// Admits the request, parses the body once, then runs the shared completion
/// path behind the keepalive and answers with whatever it decided.
///
/// The keepalive is what DESIGN.md "Keepalive" asks for, and it costs
/// something: the status line goes out before the first body byte, so once a
/// keepalive byte has been sent the response is committed to `200` and a later
/// `502` or `504` can only be delivered inside that body. Nothing is sent for
/// the first `keepalive_interval`, so every outcome decided inside it keeps its
/// own status line, which is every outcome except a slow race. Past that the
/// choice is between a committed `200` and a client that hangs up at 10
/// minutes with nothing at all, and DESIGN.md settles it: "A router that only
/// works after the client is reconfigured is a broken router."
async fn completion_response(
    state: Arc<AppState>,
    client_format: Format,
    headers: &HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // DESIGN.md "What it records" allows one info line per request, and
    // "Client authentication" wants the client key prefix on it. The prefix
    // rides on a span rather than a line of its own, so every "served" line,
    // whichever exit writes it, says which client sent the request. The work
    // future is instrumented as well, because once the keepalive commits the
    // status line it is polled by the response body, outside this function.
    // Counts every client request this process has taken. The copies' own
    // telemetry lines group under the request span, and the counter on the
    // span is what keeps ten concurrent identical requests from blurring
    // into one in the saved log.
    static NEXT_REQUEST_ID: AtomicUsize = AtomicUsize::new(1);
    let client = client_key_prefix(headers);
    let span = tracing::info_span!(
        "request",
        client_key = %client,
        format = ?client_format,
        bytes = body.len(),
        request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
    );
    respond(state, client_format, body, client)
        .instrument(span)
        .await
}

/// The endpoint a completion request came in on, for the request log.
fn endpoint_name(format: Format) -> &'static str {
    match format {
        Format::AnthropicMessages => "/v1/messages",
        Format::OpenAiChat => "/v1/chat/completions",
        Format::OpenAiResponses => "/v1/responses",
    }
}

/// The body of [`completion_response`], run inside its request span.
async fn respond(
    state: Arc<AppState>,
    client_format: Format,
    body: axum::body::Bytes,
    client: String,
) -> Response {
    // The clock starts here rather than inside the handler. Everything from
    // this line on is latency the router adds, the admission gate included:
    // on a host without `/proc` the gate forks `ps` to read RSS, which is
    // slow enough to matter. A reading taken after the gate leaves that out
    // of the router's half of `router_latency_seconds`, and that half exists
    // to say what the router costs.
    let started = Instant::now();

    // The gate runs before the body is parsed, because parsing is where the
    // memory goes.
    if let Admit::Reject {
        rss,
        high_water,
        retry_after,
    } = state.admission.check()
    {
        state.metrics.record_request(Outcome::NotAdmitted);
        let mut entry = crate::reqlog::Entry::finished(
            endpoint_name(client_format),
            None,
            false,
            503,
            "not_admitted",
        );
        entry.client = client.clone();
        entry.tokens_in = Some(request_tokens_of(&body));
        entry.total_ms = started.elapsed().as_millis() as u64;
        state.reqlog.record(entry);
        return ApiError::NotAdmitted {
            rss,
            high_water,
            retry_after_secs: retry_after.as_secs().max(1),
        }
        .served(started);
    }

    let raw_len = body.len();
    // The request-log row wants the first part of the body as the client
    // sent it. Read off the raw bytes rather than re-serialising the parsed
    // value, so a 3.8MB prompt costs one lossy read, not a second copy.
    let request_snippet =
        crate::reqlog::Entry::snippet(String::from_utf8_lossy(&body[..body.len().min(8_000)]));
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            let mut entry = crate::reqlog::Entry::finished(
                endpoint_name(client_format),
                None,
                wants_stream_of(&body),
                400,
                "bad_request",
            );
            entry.client = client.clone();
            entry.request = request_snippet;
            entry.tokens_in = Some(request_tokens_of(&body));
            entry.total_ms = started.elapsed().as_millis() as u64;
            state.reqlog.record(entry);
            return ApiError::BadRequest {
                reason: format!("body is not valid JSON: {err}"),
            }
            .served(started);
        }
    };
    drop(body);

    // The client's own flag, read before the body is handed on. It decides the
    // shape of the reply and the shape of the keepalive, and nothing else: it
    // never reaches a provider and it is never a reason to reject anything.
    let wants_stream = parsed
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let interval = state.config.router.keepalive_interval;

    with_keepalive(
        Box::pin(
            handle_completion(
                state,
                client_format,
                parsed,
                raw_len,
                wants_stream,
                started,
                client,
                request_snippet,
            )
            .in_current_span(),
        ),
        wants_stream,
        interval,
    )
    .await
}

/// The request's size in tokens, on the raw body, for rows built before the
/// body was parsed.
fn request_tokens_of(body: &axum::body::Bytes) -> u64 {
    crate::reqlog::Entry::estimate_tokens(body.len())
}

/// Whether a not-yet-parsed body asked for a stream, for the request log's
/// row of a request the parser rejects. Only the parse failure needs this
/// before `respond` has read the flag properly.
fn wants_stream_of(body: &axum::body::Bytes) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("stream").and_then(Value::as_bool))
        .unwrap_or(false)
}

/// What the completion path decided: a whole response, or the winner's events.
enum Served {
    /// The status line and the body are both already known.
    Whole(Response),
    /// Server-sent events for a client that asked for a stream.
    Events(SseFrames),
}

impl Served {
    /// The response, when nothing has gone out to the client yet.
    fn into_response(self) -> Response {
        match self {
            Served::Whole(response) => response,
            Served::Events(frames) => Sse::new(frames).into_response(),
        }
    }
}

/// Framed server-sent events, ready to hand to axum.
type SseFrames = Pin<Box<dyn Stream<Item = std::result::Result<Event, Infallible>> + Send>>;

/// The completion path, with a keepalive byte every `interval` until the first
/// real byte of the answer.
///
/// Nothing goes out while the first interval runs, so an outcome decided in
/// that window keeps its own status code. After it the response is committed
/// to `200`: see [`completion_response`].
async fn with_keepalive(
    mut work: Pin<Box<dyn Future<Output = Served> + Send>>,
    wants_stream: bool,
    interval: Duration,
) -> Response {
    // Zero turns the keepalive off, which is the behaviour without it: every
    // outcome keeps its own status line and a slow client hangs up.
    if interval.is_zero() {
        return work.await.into_response();
    }
    match tokio::time::timeout(interval, &mut work).await {
        Ok(served) => served.into_response(),
        Err(_) => committed(work, wants_stream, interval),
    }
}

/// The slow path: `200` and its headers go out now, then a keepalive byte every
/// `interval` until the work finishes, then the answer itself.
fn committed(
    work: Pin<Box<dyn Future<Output = Served> + Send>>,
    wants_stream: bool,
    interval: Duration,
) -> Response {
    if wants_stream {
        // `: ping`, which the server-sent-events spec says a client must
        // ignore. Anthropic's own API sends ping events for the same reason.
        let frames = async_stream::stream! {
            let mut work = work;
            let mut ticker = tokio::time::interval(interval);
            let served = loop {
                // The yield happens after the select, not inside an arm, so the
                // pending work future is never held across it.
                let tick = tokio::select! {
                    _ = ticker.tick() => true,
                    served = &mut work => break served,
                };
                if tick {
                    yield Ok(Event::default().comment("ping"));
                }
            };
            match served {
                Served::Events(mut frames) => {
                    while let Some(frame) = frames.next().await {
                        yield frame;
                    }
                }
                // The status line went out as `200` a while ago, so the outcome
                // can only be delivered in the body now.
                Served::Whole(response) => {
                    let status = response.status();
                    tracing::warn!(
                        status = status.as_u16(),
                        "the keepalive had already committed the status line, so the outcome goes out as an sse error event"
                    );
                    let body = read_body(response).await;
                    yield Ok(Event::default()
                        .event("error")
                        .data(String::from_utf8_lossy(&body)));
                }
            }
        };
        Sse::new(Box::pin(frames) as SseFrames).into_response()
    } else {
        // One space every interval. JSON parsers skip leading whitespace, so
        // the body still parses when it finally arrives.
        let body = async_stream::stream! {
            let mut work = work;
            let mut ticker = tokio::time::interval(interval);
            let served = loop {
                let tick = tokio::select! {
                    _ = ticker.tick() => true,
                    served = &mut work => break served,
                };
                if tick {
                    yield Ok::<Bytes, Infallible>(Bytes::from_static(b" "));
                }
            };
            match served {
                Served::Whole(response) => {
                    let status = response.status();
                    if !status.is_success() {
                        tracing::warn!(
                            status = status.as_u16(),
                            "the keepalive had already committed the status line, so the outcome goes out inside a 200 body"
                        );
                    }
                    yield Ok(read_body(response).await);
                }
                // A client that did not ask for a stream is never handed one:
                // `handle_completion` folds it into one piece first.
                Served::Events(_) => {
                    tracing::error!("a stream was built for a client that did not ask for one");
                    yield Ok(Bytes::from_static(
                        br#"{"error":"no_channel","message":"the reply could not be delivered in one piece","detail":{}}"#,
                    ));
                }
            }
        };
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from_stream(body))
            // The builder only fails on an invalid header, and both are
            // constants, so this cannot fire.
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

/// The whole body of a response the router built itself. These are all in
/// memory already, so there is nothing to stream and no limit to apply.
async fn read_body(response: Response) -> Bytes {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_else(|err| {
            tracing::error!(error = %err, "a response body could not be read back");
            Bytes::from_static(b"{}")
        })
}

/// Frames the winner's events as server-sent events.
///
/// A stream that dies here ends the client request and nothing is spliced on:
/// the client already has part of one model's answer, and joining another
/// model's output onto it produces a reply nobody can trust. The stall timer is
/// not here but in `race.rs`, on the byte stream, which is the one place every
/// winning stream passes through and the only place that can back the channel
/// off for it.
///
/// The death is also deliberately not sent as an SSE `error` event. Claude
/// Code (checked against 2.1.308) re-sends the whole request after an abrupt
/// close only while no content block has finished. Once one has, an error
/// event and an abrupt close both keep the partial answer, and Claude Code
/// asks the model to resume it, at most three times per turn, before it stops
/// with `Connection lost mid-response`. Each resume is a fresh request, so the
/// dead channel's `died_mid_answer` backoff is what sends it to another model.
/// The channel's health move, the error class and the request-log entry name
/// the failure.
fn sse_frames(mut events: EventStream, format: Format) -> SseFrames {
    Box::pin(async_stream::stream! {
        while let Some(item) = events.next().await {
            match item {
                Ok(value) => yield Ok(sse_event(format, &value)),
                Err(err) => {
                    tracing::error!(error = %err, "the winning stream died mid-answer");
                    return;
                }
            }
        }
        // OpenAI clients read `data: [DONE]` as the end of the stream.
        // Anthropic clients read `message_stop`, which the translation already
        // emits as an event of its own.
        if !matches!(format, Format::AnthropicMessages) {
            yield Ok(Event::default().data("[DONE]"));
        }
    })
}

/// One wire event as a server-sent event. Anthropic names its events, so the
/// `type` field becomes the `event:` line; OpenAI carries the type inside the
/// data and needs no name.
///
/// The name is provider-supplied: an Anthropic-wire channel has its raw event
/// JSON replayed verbatim, so its own `type` lands here. A carriage return or
/// newline in an `event:` line cannot be framed and `Event::event` panics on
/// one, which would kill the connection task mid-answer. Such a name is
/// dropped and the event goes out unnamed, with the `type` still readable in
/// the data.
fn sse_event(format: Format, value: &Value) -> Event {
    let event = Event::default().data(value.to_string());
    match format {
        Format::AnthropicMessages => match value.get("type").and_then(Value::as_str) {
            Some(name) if !name.contains(['\r', '\n']) => event.event(name),
            Some(name) => {
                tracing::warn!(
                    length = name.len(),
                    "an event name carried a newline and was dropped"
                );
                event
            }
            None => event,
        },
        _ => event,
    }
}

/// The shared path behind both completion endpoints: admit, hash, read the
/// cache, build the filter, race, write the cache, translate out.
///
/// `client` and `request_snippet` ride along for the request log's row, which
/// every exit of this function records.
///
/// `wants_stream` is the client's own `stream` flag and decides only the shape
/// of the reply. The providers are asked for a stream whenever `streaming` is
/// on in `router.yml`, whatever the client asked for, because the hedge
/// strategy needs a stream to measure.
///
/// `started` is when the request reached the router. The caller takes it
/// before the admission gate so that the gate counts as router overhead too.
#[allow(clippy::too_many_arguments)] // the request-log row rides the same list
async fn handle_completion(
    state: Arc<AppState>,
    client_format: Format,
    body: Value,
    raw_len: usize,
    wants_stream: bool,
    started: Instant,
    client: String,
    request_snippet: String,
) -> Served {
    let (_concurrency, concurrent) = Concurrency::open();

    let request = match crate::wire::decode_request(client_format, &body) {
        Ok(request) => request,
        Err(err) => {
            let mut entry = crate::reqlog::Entry::finished(
                endpoint_name(client_format),
                None,
                wants_stream,
                400,
                "bad_request",
            );
            entry.client = client;
            entry.request = request_snippet;
            entry.total_ms = started.elapsed().as_millis() as u64;
            state.reqlog.record(entry);
            return Served::Whole(
                ApiError::BadRequest {
                    reason: err.to_string(),
                }
                .served(started),
            );
        }
    };
    drop(body);

    let named_model = route_claude_to_auto(&state.catalogue, crate::wire::named_model(&request));
    // The request's size in tokens, on the same estimate the channel filter
    // runs, so a row can show a size even when no provider reported usage.
    let request_tokens = crate::reqlog::Entry::estimate_tokens(raw_len);
    let now = now_unix();

    // Only a whole answer is ever stored, so every request can be served from
    // the cache: a hit replays to a streaming client as a stream.
    let key = match state.cache.as_ref() {
        Some(_) => crate::cache::cache_key(&request).ok(),
        None => None,
    };
    // Recorded here rather than in `cache.rs`, because a `Cache` only exists
    // when `cache.enabled` is true: from inside it the disabled case is
    // unreachable, and the counter would sit at a permanent zero that reads
    // exactly like a cache that is on and never asked.
    if state.cache.is_none() {
        state
            .metrics
            .record_cache_lookup(crate::metrics::CacheLookup::Disabled);
    }
    if let (Some(cache), Some(key)) = (state.cache.as_ref(), key) {
        match cache.get(key, named_model.as_deref(), now) {
            Ok(Some(entry)) => {
                if let Some(response) =
                    cached_response(&entry, client_format, named_model.as_deref(), wants_stream)
                {
                    state.metrics.record_request(Outcome::HitCache);
                    // A hit has its whole answer already, so the client's
                    // first token and its last arrive together. Recorded all
                    // the same: leaving hits out would make the time to first
                    // token histogram describe only the slow half of traffic.
                    state.metrics.record_ttft(started.elapsed());
                    let mut row = crate::reqlog::Entry::finished(
                        endpoint_name(client_format),
                        named_model.clone(),
                        wants_stream,
                        200,
                        "hit_cache",
                    );
                    row.client = client;
                    row.served = Some(entry.model_id.clone());
                    row.provider = Some(entry.provider.clone());
                    row.request = request_snippet;
                    row.response =
                        crate::reqlog::Entry::snippet(String::from_utf8_lossy(&entry.body));
                    row.tokens_in = Some(request_tokens);
                    row.ttft_ms = Some(started.elapsed().as_millis() as u64);
                    row.total_ms = started.elapsed().as_millis() as u64;
                    state.reqlog.record(row);
                    tracing::info!(
                        outcome = "hit_cache",
                        model = %entry.model_id,
                        channel = %entry.channel_id,
                        ms = started.elapsed().as_millis() as u64,
                        ttft_ms = started.elapsed().as_millis() as u64,
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
        let mut entry = crate::reqlog::Entry::finished(
            endpoint_name(client_format),
            named_model.clone(),
            wants_stream,
            error.status().as_u16(),
            error.tag(),
        );
        entry.client = client;
        entry.request = request_snippet;
        entry.tokens_in = Some(request_tokens);
        entry.total_ms = started.elapsed().as_millis() as u64;
        state.reqlog.record(entry);
        return Served::Whole(error.served(started));
    }

    // Encoded once and shared. Below the spill threshold it stays in RAM and
    // cloning never copies the prompt; above it the race reads it back from a
    // scratch file per channel call.
    let streaming = state.config.router.streaming;
    let outbound = match outbound_body(&request, streaming) {
        Ok(bytes) => Arc::new(PromptBody::new(bytes, concurrent, &state.config.router).await),
        Err(err) => {
            return Served::Whole(
                ApiError::BadRequest {
                    reason: err.to_string(),
                }
                .served(started),
            )
        }
    };
    drop(request);

    // Dropping this future, which is what axum does when the client goes
    // away, fires the token and tears down every channel call. The walk runs
    // as its own task so it is still polled after that: it has to see the
    // token, settle the failures it already holds and log the hang-up. Were
    // it awaited in place, it would be dropped with the handler and none of
    // that would ever run.
    let cancel = CancellationToken::new();
    let guard = cancel.clone().drop_guard();
    let racer = Arc::clone(&state.racer);
    let input = RaceInput {
        outbound: Arc::clone(&outbound),
        filter: filter.clone(),
        streaming,
        named_model: named_model.clone(),
        exclude: std::collections::HashSet::new(),
    };
    let race = tokio::spawn(
        async move {
            let outcome = racer.run(input, cancel).await;
            if matches!(outcome, RaceOutcome::ClientGone) {
                tracing::info!(
                    outcome = "client_gone",
                    ms = started.elapsed().as_millis() as u64,
                    "served"
                );
            }
            outcome
        }
        // A spawned task does not inherit the request span, so it is handed on
        // and the hang-up line still names the client.
        .instrument(tracing::Span::current()),
    );
    // The task is never aborted, so the only error is a panic, passed on as
    // if the walk had run in place.
    let outcome = race
        .await
        .unwrap_or_else(|err| std::panic::resume_unwind(err.into_panic()));
    // The winning stream is not held by this token: it moved out of its trial
    // before the race cancelled the losers.
    drop(guard);

    match outcome {
        RaceOutcome::Buffered {
            winner,
            also_finished,
            attempts,
            race_elapsed,
        } => {
            if let (Some(cache), Some(key)) = (state.cache.as_ref(), key) {
                let best = crate::race::best_for_cache(&winner, &also_finished);
                write_cache(cache, key, best, crate::cache::StreamEnd::Complete, now).await;
            }
            drop(also_finished);

            let winner = *winner;
            let served = named_model
                .clone()
                .unwrap_or_else(|| winner.model_id.clone());
            // Taken before the branch below, which hands the response on to
            // be encoded and in the streaming case moves it outright.
            let usage = winner.response.usage.clone();
            // The encoded reply feeds the request log's row below; a
            // streaming delivery of a whole answer has no single body to
            // show, so its row's payload stays empty.
            let mut reply_snippet: Option<String> = None;
            let reply = if wants_stream {
                // The client asked for a stream and the answer arrived whole,
                // which is what `streaming: false` in `router.yml` gives. The
                // whole answer is chunked back out rather than rejected.
                stream_reply(winner.response, client_format, served.clone())
            } else {
                match crate::wire::encode_response(&winner.response, client_format, Some(&served)) {
                    Ok(value) => {
                        reply_snippet = Some(crate::reqlog::Entry::snippet(value.to_string()));
                        Some(Served::Whole(axum::Json(value).into_response()))
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "the reply could not be translated");
                        None
                    }
                }
            };

            let Some(reply) = reply else {
                state.metrics.record_request(Outcome::NoChannel);
                return Served::Whole(
                    ApiError::NoChannel {
                        attempts,
                        last_error: Some("could not translate the reply".to_string()),
                    }
                    .served(started),
                );
            };

            // Sampled after the reply has been translated back into the
            // client's format, as the streamed arm does. That encode is the
            // largest piece of work the router does after the race, so a
            // reading taken earlier would leave it out of the router's half
            // of the split.
            let total = started.elapsed();

            // Split on the whole walk, not on the winning call alone.
            // `winner.latency` is that one channel's own wall clock, so every
            // failed channel the walk got through first used to land in the
            // router's half: a live run reported 829 seconds of mean router
            // overhead for work the router never did, against a target in
            // milliseconds.
            state
                .metrics
                .record_latency(race_elapsed, total.saturating_sub(race_elapsed));
            // Nothing streamed, so the client's first token is its whole
            // answer and the two timings are the same reading.
            state.metrics.record_ttft(total);
            // A buffered answer has no stream to time, so the winning
            // channel call's own wall clock is the generation window: it is
            // the whole call, connect to last byte, with none of the walk.
            let rate = usage
                .output_tokens
                .filter(|&o| o > 0)
                .map(|o| o as f64 / winner.latency.as_secs_f64());
            record_usage(&state.metrics, &winner.provider, &usage, rate);
            state.metrics.record_request(Outcome::Success);
            let mut row = crate::reqlog::Entry::finished(
                endpoint_name(client_format),
                named_model.clone(),
                wants_stream,
                200,
                "success",
            );
            row.client = client;
            row.served = Some(winner.model_id.clone());
            row.provider = Some(winner.provider.clone());
            row.request = request_snippet;
            row.response = reply_snippet.unwrap_or_default();
            row.attempts = attempts;
            // Nothing streamed on this path: the whole answer arrives at once,
            // so there is no generation window to show.
            row.ttft_ms = Some(total.as_millis() as u64);
            row.total_ms = total.as_millis() as u64;
            row.tps = rate;
            row.tokens_in = usage.input_tokens.or(Some(request_tokens));
            row.tokens_out = usage.output_tokens;
            state.reqlog.record(row);
            tracing::info!(
                outcome = "success",
                channel = %winner.channel_id,
                provider = %winner.provider,
                model = %winner.model_id,
                streamed = wants_stream,
                attempts,
                ms = total.as_millis() as u64,
                ttft_ms = total.as_millis() as u64,
                "served"
            );
            reply
        }

        RaceOutcome::Streamed {
            winner,
            attempts,
            race_elapsed,
        } => {
            // Every binding is mutable so the failover walk below can move
            // the request's identity onto whichever walk the answer finally
            // came from.
            let crate::race::StreamWinner {
                mut channel_id,
                mut provider,
                mut model_id,
                mut intelligence,
                chunks,
                mut latency,
                first_token_at,
                slot: mut stream_slot,
            } = *winner;
            let mut attempts = attempts;
            let served = named_model.clone().unwrap_or_else(|| model_id.clone());

            // A streaming client has the winning response at its first token.
            // A plain client only has it once the fold is done, so the plain
            // path below adds the time the provider spent generating after
            // that first token: it is upstream time, not time the router added.
            // The walk, not the winning call on its own. See the note on
            // the buffered arm above.
            let mut upstream = race_elapsed;
            // The first token's arrival, as a distance from the start of the
            // request. `first_token_at` is a tokio Instant and `started` is a
            // std one, so the two cannot be subtracted; going via each one's
            // own elapsed time gets there without a clock conversion.
            let first_token_elapsed = started.elapsed().saturating_sub(first_token_at.elapsed());
            let reply = if wants_stream {
                // Forwarded as it arrives, with a copy taken on the way past.
                // The copy only lands once the stream has run to the end, so a
                // cancelled or failed stream never reaches the cache.
                let (chunks, complete) = match (state.cache.as_ref(), key) {
                    (Some(cache), Some(key)) => {
                        let (forwarded, complete) = crate::wire::tee_stream(chunks);
                        let cache = Arc::clone(cache);
                        let winner = crate::race::Winner {
                            channel_id,
                            provider: provider.clone(),
                            model_id: model_id.clone(),
                            intelligence,
                            response: crate::wire::NeutralResponse::default(),
                            latency,
                            slot: stream_slot,
                        };
                        (forwarded, Some((cache, key, winner, complete)))
                    }
                    _ => (chunks, None),
                };
                if let Some((cache, key, mut winner, complete)) = complete {
                    tokio::spawn(async move {
                        let Ok(copy) = complete.await else {
                            return;
                        };
                        match crate::wire::aggregate_stream(copy).await {
                            Ok(response) => {
                                winner.response = response;
                                write_cache(
                                    &cache,
                                    key,
                                    &winner,
                                    crate::cache::StreamEnd::Complete,
                                    now,
                                )
                                .await;
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "a streamed answer could not be folded for the cache");
                            }
                        }
                    });
                }
                // Counted off the stream on its way to the client, not off
                // the cache's copy of it. Hanging the count on the cache made
                // token accounting depend on `cache.enabled`, so a streaming
                // client with the cache off counted nothing at all. Reading
                // the clock when the stream ends also makes the rate the one
                // the client felt, rather than one that includes a fold the
                // client never waited for.
                let (chunks, facts) = crate::wire::tap_stream(chunks);
                let usage_metrics = state.metrics.clone();
                let usage_provider = provider.clone();
                let reqlog = Arc::clone(&state.reqlog);
                // Everything the row needs except what the stream itself
                // produces; the task below finalises it when the stream ends,
                // the only moment the answer text and the total time are
                // known.
                let entry_model = named_model.clone();
                let entry_served = served.clone();
                let entry_provider = provider.clone();
                let entry_request = request_snippet;
                let entry_ttft = first_token_elapsed.as_millis() as u64;
                let entry_request_tokens = request_tokens;
                let entry_attempts = attempts;
                tokio::spawn(async move {
                    let facts = facts.await.ok();
                    if let Some(facts) = facts.as_ref() {
                        // The usage frame arrives with the end of the stream,
                        // so the distance from the first token to here is the
                        // provider's own generation window, with none of the
                        // walk or the hedge decision window in it.
                        let generated = first_token_at.elapsed();
                        let rate = facts
                            .usage
                            .as_ref()
                            .and_then(|usage| usage.output_tokens)
                            .filter(|&o| o > 0)
                            .map(|o| o as f64 / generated.as_secs_f64());
                        if let Some(usage) = facts.usage.as_ref() {
                            record_usage(&usage_metrics, &usage_provider, usage, rate);
                        }
                    }
                    // The status line went out as `200` at the first token,
                    // so the row's outcome is the only place that says how
                    // the answer really ended. Every one of these used to read
                    // `success`, which is how seven streams cut off mid-answer
                    // on 22 Oct 2026 went unseen on the dashboard.
                    let outcome = match facts.as_ref() {
                        Some(facts) if facts.died.is_some() => "stream_died",
                        Some(facts) if facts.complete => "success",
                        // Dropped before the answer's terminal chunk, which
                        // only the client hanging up does.
                        _ => "client_gone",
                    };
                    let mut entry = crate::reqlog::Entry::finished(
                        endpoint_name(client_format),
                        entry_model,
                        true,
                        200,
                        outcome,
                    );
                    if let Some(facts) = facts {
                        entry.error = facts.died;
                        entry.tokens_out =
                            entry
                                .tokens_out
                                .or(Some(crate::reqlog::Entry::estimate_tokens(
                                    facts.text.chars().count(),
                                )));
                        entry.response = crate::reqlog::Entry::snippet(facts.text);
                        entry.tokens_in = entry.tokens_in.or(Some(entry_request_tokens));
                        if let Some(usage) = facts.usage.as_ref() {
                            let generated = first_token_at.elapsed();
                            entry.tps = usage
                                .output_tokens
                                .filter(|&o| o > 0)
                                .map(|o| o as f64 / generated.as_secs_f64());
                        }
                    }
                    entry.served = Some(entry_served);
                    entry.provider = Some(entry_provider);
                    entry.attempts = entry_attempts;
                    entry.ttft_ms = Some(entry_ttft);
                    entry.request = entry_request;
                    entry.total_ms = started.elapsed().as_millis() as u64;
                    // From the winning channel's first token to the end of
                    // the answer: the provider generating, as against the
                    // walk before it.
                    entry.gen_ms = Some(entry.total_ms.saturating_sub(entry_ttft));
                    reqlog.record(entry);
                });

                crate::wire::encode_stream(chunks, client_format, Some(served.clone()))
                    .map(|events| Served::Events(sse_frames(events, client_format)))
                    .map_err(|err| err.to_string())
            } else {
                // The client wants one piece, so the stream is folded into one
                // here. That is the only place the whole answer exists, so it
                // is also the only streamed path that can feed the cache.
                // The fold is timed on its own rather than taken from the
                // first token. Under `hedge_strategy: tps` the race does not
                // return at the first token: it holds the decision window
                // open first, so that window is already inside
                // `race_elapsed`, and adding the distance from the first
                // token would count it a second time. With a one second
                // window that put the upstream half a second per request too
                // high and saturated the router half to zero, which is the
                // one number the split exists to prove.
                let fold_started = Instant::now();

                // A stream that dies during the fold has reached nothing: the
                // client still has nothing, so the walk is re-run on fresh
                // channels and the client never knows. The channel that died
                // is excluded from every later attempt, and so is every
                // channel a retry's own winning stream then died on.
                let mut exclude: HashSet<crate::catalogue::ChannelId> = HashSet::new();
                exclude.insert(channel_id);
                let mut folded = crate::wire::aggregate_stream(chunks).await;
                let mut retries = 0;
                while let Err(err) = folded {
                    if retries >= 2 {
                        folded = Err(err);
                        break;
                    }
                    retries += 1;
                    tracing::warn!(
                        channel = %channel_id,
                        error = %err,
                        "the winning stream died before anything reached the client;                          re-running the walk on fresh channels"
                    );
                    let input = RaceInput {
                        outbound: Arc::clone(&outbound),
                        filter: filter.clone(),
                        streaming,
                        named_model: named_model.clone(),
                        exclude: exclude.clone(),
                    };
                    let retry_cancel = CancellationToken::new();
                    let _guard = retry_cancel.clone().drop_guard();
                    match state.racer.run(input, retry_cancel).await {
                        RaceOutcome::Streamed {
                            winner,
                            attempts: more,
                            race_elapsed: more_elapsed,
                        } => {
                            attempts += more;
                            upstream += more_elapsed;
                            let w = *winner;
                            exclude.insert(w.channel_id);
                            channel_id = w.channel_id;
                            provider = w.provider;
                            model_id = w.model_id;
                            intelligence = w.intelligence;
                            latency = w.latency;
                            stream_slot = w.slot;
                            folded = crate::wire::aggregate_stream(w.chunks).await;
                        }
                        RaceOutcome::Buffered {
                            winner,
                            attempts: more,
                            race_elapsed: more_elapsed,
                            ..
                        } => {
                            attempts += more;
                            upstream += more_elapsed;
                            let w = *winner;
                            channel_id = w.channel_id;
                            provider = w.provider;
                            model_id = w.model_id;
                            intelligence = w.intelligence;
                            latency = w.latency;
                            stream_slot = w.slot;
                            folded = Ok(w.response);
                        }
                        // Nothing else came back, so the first death stands.
                        _ => {
                            folded = Err(err);
                        }
                    }
                }
                upstream += fold_started.elapsed();

                match folded {
                    Ok(response) => {
                        let winner = crate::race::Winner {
                            channel_id,
                            provider: provider.clone(),
                            model_id: model_id.clone(),
                            intelligence,
                            response,
                            latency,
                            slot: stream_slot,
                        };
                        // The fold on this path happens on the request's own
                        // task, so the usage block is in hand here rather
                        // than on the cache's spawned copy. The stream has
                        // run to its end by now, so the distance from the
                        // first token to here is the provider's generation
                        // window.
                        let generated = first_token_at.elapsed();
                        let rate = winner
                            .response
                            .usage
                            .output_tokens
                            .filter(|&o| o > 0)
                            .map(|o| o as f64 / generated.as_secs_f64());
                        record_usage(
                            &state.metrics,
                            &winner.provider,
                            &winner.response.usage,
                            rate,
                        );
                        if let (Some(cache), Some(key)) = (state.cache.as_ref(), key) {
                            write_cache(
                                cache,
                                key,
                                &winner,
                                crate::cache::StreamEnd::Complete,
                                now,
                            )
                            .await;
                        }
                        match crate::wire::encode_response(
                            &winner.response,
                            client_format,
                            Some(&served),
                        ) {
                            Ok(value) => {
                                let mut row = crate::reqlog::Entry::finished(
                                    endpoint_name(client_format),
                                    named_model.clone(),
                                    false,
                                    200,
                                    "success",
                                );
                                row.client = client;
                                row.served = Some(model_id.clone());
                                row.provider = Some(provider.clone());
                                row.request = request_snippet;
                                row.attempts = attempts;
                                // Serialise once: the snippet and the token
                                // estimate read the same encoded reply.
                                let encoded_text = value.to_string();
                                row.response = crate::reqlog::Entry::snippet(encoded_text.as_str());
                                row.tps = rate;
                                row.tokens_in =
                                    winner.response.usage.input_tokens.or(Some(request_tokens));
                                row.tokens_out = winner.response.usage.output_tokens.or(Some(
                                    crate::reqlog::Entry::estimate_tokens(encoded_text.len()),
                                ));
                                row.total_ms = started.elapsed().as_millis() as u64;
                                // The ttft is the winning channel's first
                                // token, not the moment the fold finished:
                                // the provider's own latency, which the
                                // generation window then measures against.
                                row.ttft_ms = Some(first_token_elapsed.as_millis() as u64);
                                row.gen_ms = Some(
                                    row.total_ms
                                        .saturating_sub(first_token_elapsed.as_millis() as u64),
                                );
                                state.reqlog.record(row);
                                Ok(Served::Whole(axum::Json(value).into_response()))
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "the reply could not be translated");
                                Err(err.to_string())
                            }
                        }
                    }
                    Err(error) => Err(error.to_string()),
                }
            };
            // Sampled after the branch above, not before it. On the plain path
            // the fold consumes the whole winning stream, so a reading taken
            // earlier would call time to first token the whole request and
            // leave the router's own work out of its half of the split.
            let total = started.elapsed();

            match reply {
                Ok(reply) => {
                    state
                        .metrics
                        .record_latency(upstream, total.saturating_sub(upstream));
                    // A streaming client saw its first token when the channel
                    // produced it. A plain client saw nothing until the fold
                    // was done, so for it the first token and the last are
                    // the same moment. Recording one number for both would
                    // make the histogram describe neither.
                    state.metrics.record_ttft(if wants_stream {
                        first_token_elapsed
                    } else {
                        total
                    });
                    state.metrics.record_request(Outcome::Success);
                    tracing::info!(
                        outcome = "success",
                        channel = %channel_id,
                        provider = %provider,
                        model = %model_id,
                        streamed = wants_stream,
                        attempts,
                        ms = total.as_millis() as u64,
                        ttft_ms = (if wants_stream { first_token_elapsed } else { total })
                            .as_millis() as u64,
                        "served"
                    );
                    reply
                }
                Err(error) => {
                    state.metrics.record_request(Outcome::NoChannel);
                    tracing::warn!(channel = %channel_id, error = %error, "the winning stream could not be delivered");
                    Served::Whole(
                        ApiError::NoChannel {
                            attempts,
                            last_error: Some(error),
                        }
                        .served(started),
                    )
                }
            }
        }

        RaceOutcome::NoNetwork { failed } => {
            state.metrics.record_request(Outcome::NoNetwork);
            {
                let mut row = crate::reqlog::Entry::finished(
                    endpoint_name(client_format),
                    named_model.clone(),
                    wants_stream,
                    502,
                    "no_network",
                );
                row.client = client;
                row.request = request_snippet;
                row.tokens_in = Some(request_tokens);
                row.total_ms = started.elapsed().as_millis() as u64;
                state.reqlog.record(row);
            }
            tracing::info!(
                outcome = "no_network",
                ms = started.elapsed().as_millis() as u64,
                "served"
            );
            Served::Whole(ApiError::NoNetwork { failed }.into_response())
        }

        RaceOutcome::Exhausted {
            attempts,
            last_error,
        } => {
            state.metrics.record_request(Outcome::NoChannel);
            {
                let mut row = crate::reqlog::Entry::finished(
                    endpoint_name(client_format),
                    named_model.clone(),
                    wants_stream,
                    504,
                    "no_channel",
                );
                row.client = client;
                row.request = request_snippet;
                row.tokens_in = Some(request_tokens);
                row.total_ms = started.elapsed().as_millis() as u64;
                state.reqlog.record(row);
            }
            tracing::info!(
                outcome = "no_channel",
                attempts,
                ms = started.elapsed().as_millis() as u64,
                "served"
            );
            Served::Whole(
                ApiError::NoChannel {
                    attempts,
                    last_error,
                }
                .into_response(),
            )
        }

        // Logged by the walk's task, which outlives the handler.
        RaceOutcome::ClientGone => Served::Whole(
            StatusCode::from_u16(499)
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
                .into_response(),
        ),
    }
}

/// Chunks a whole answer back out as server-sent events, for a client that
/// asked for a stream and a reply that arrived in one piece. `None` means the
/// translation failed and the caller owes the client an error instead.
fn stream_reply(
    response: crate::wire::NeutralResponse,
    client_format: Format,
    served: String,
) -> Option<Served> {
    crate::wire::encode_stream(
        crate::wire::stream_of(response),
        client_format,
        Some(served),
    )
    .map(|events| Served::Events(sse_frames(events, client_format)))
    .map_err(|err| tracing::warn!(error = %err, "a whole answer could not be chunked out"))
    .ok()
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

/// The body every channel call sends.
///
/// `streaming` is the router's own setting, not the client's: the providers are
/// asked for a stream whenever it is on, because the hedge strategy needs one
/// to measure. `stream_options` goes with it, or an OpenAI-shaped provider
/// leaves the usage figures out of the stream and the folded answer loses them.
///
/// The `model` field is removed here. The client's model string means nothing
/// to a provider, and the race decides which model is called, so each channel
/// call puts its own `model_id` in front of this body. See
/// [`crate::upstream::SharedBody`].
fn outbound_body(
    request: &crate::wire::NeutralRequest,
    streaming: bool,
) -> std::result::Result<Bytes, crate::wire::WireError> {
    let mut value = crate::wire::encode_request(request, Format::OpenAiChat)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("stream".to_string(), Value::Bool(streaming));
        if streaming {
            object.insert(
                "stream_options".to_string(),
                json!({ "include_usage": true }),
            );
        }
        object.remove("model");
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|err| crate::wire::WireError::Translation(err.to_string()))
}

/// Turns a stored entry back into the client's format, as one piece or as a
/// stream depending on what the client asked for. A body that no longer
/// deserialises is treated as a miss.
fn cached_response(
    entry: &CacheEntry,
    client_format: Format,
    named_model: Option<&str>,
    wants_stream: bool,
) -> Option<Served> {
    let response: crate::wire::NeutralResponse = serde_json::from_slice(&entry.body)
        .map_err(|err| tracing::warn!(error = %err, "cached entry could not be read back"))
        .ok()?;
    let served = named_model.unwrap_or(entry.model_id.as_str()).to_string();
    if wants_stream {
        return stream_reply(response, client_format, served);
    }
    let encoded = crate::wire::encode_response(&response, client_format, Some(&served))
        .map_err(|err| tracing::warn!(error = %err, "cached entry could not be translated"))
        .ok()?;
    Some(Served::Whole(axum::Json(encoded).into_response()))
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
/// `end` says how the answer arrived. A buffered winner is always
/// [`StreamEnd::Complete`]; a folded stream is only complete because the fold
/// succeeded, and [`Cache::put_stream`] is where that rule is enforced.
async fn write_cache(
    cache: &Arc<Cache>,
    key: crate::cache::CacheKey,
    best: &crate::race::Winner,
    end: crate::cache::StreamEnd,
    now: u64,
) {
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
    if let Err(err) = Arc::clone(cache)
        .put_stream_off_thread(key, entry, end, now)
        .await
    {
        tracing::warn!(error = %err, "cache write failed");
    }
}

/// A Claude model name that no channel serves becomes `auto`. Claude Code left
/// on its own model names then works, and it knows the real context window of
/// a Claude model, which it does not for `auto`. A channel that does serve the
/// name keeps it.
fn route_claude_to_auto(catalogue: &Catalogue, named: Option<String>) -> Option<String> {
    let name = named?;
    let claude = name
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("claude-"));
    if claude
        && !catalogue
            .snapshot()
            .iter()
            .any(|c| c.model_id.eq_ignore_ascii_case(&name))
    {
        return None;
    }
    Some(name)
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
    use headers::{authorization::Bearer, Authorization, HeaderMapExt};
    // The scheme name is case-insensitive (RFC 9110 section 11.1), which the
    // typed parser handles. Anything else falls back to the raw header.
    let bearer = headers.typed_get::<Authorization<Bearer>>();
    let raw = headers
        .get(header::AUTHORIZATION)
        .or_else(|| headers.get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let value = bearer.as_ref().map_or(raw, |auth| auth.token()).trim();
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

    /// The response for a request that ends here, with the one info line per
    /// request that DESIGN.md "What it records" asks for. `started` is when
    /// the request reached the router.
    fn served(self, started: Instant) -> Response {
        tracing::info!(
            outcome = self.tag(),
            status = self.status().as_u16(),
            ms = started.elapsed().as_millis() as u64,
            "served"
        );
        self.into_response()
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

    /// The scrape read into samples, so a test compares exact values rather
    /// than matching a prefix of the text. Mirrors the helpers in metrics.rs.
    fn scrape(metrics: &Metrics) -> Vec<prometheus_parse::Sample> {
        prometheus_parse::Scrape::parse(metrics.render().lines().map(|l| Ok(l.to_owned())))
            .expect("the scrape parses")
            .samples
    }

    /// The one unlabelled value behind `name`.
    fn plain(samples: &[prometheus_parse::Sample], name: &str) -> f64 {
        let found: Vec<_> = samples
            .iter()
            .filter(|s| s.metric == name && s.labels.is_empty())
            .collect();
        assert_eq!(found.len(), 1, "{name} in the scrape");
        match &found[0].value {
            prometheus_parse::Value::Counter(v)
            | prometheus_parse::Value::Gauge(v)
            | prometheus_parse::Value::Untyped(v) => *v,
            other => panic!("{name} is not a plain value: {other:?}"),
        }
    }

    #[test]
    fn the_output_rate_is_recorded_over_its_generation_window() {
        let metrics = Metrics::new().expect("the recorder builds");
        let usage = crate::wire::NeutralUsage {
            input_tokens: Some(17),
            output_tokens: Some(33),
            ..Default::default()
        };

        record_usage(&metrics, "zai", &usage, Some(140.0));

        let s = scrape(&metrics);
        assert_eq!(plain(&s, "router_output_tokens_per_second_sum"), 140.0);
        assert_eq!(plain(&s, "router_output_tokens_per_second_count"), 1.0);
        let tokens = s
            .iter()
            .find(|s| {
                s.metric == "router_tokens_total"
                    && s.labels.get("provider") == Some("zai")
                    && s.labels.get("kind") == Some("completion")
            })
            .expect("the completion token counter is in the scrape");
        match &tokens.value {
            prometheus_parse::Value::Counter(v) => assert_eq!(*v, 33.0),
            other => panic!("the tokens counter: {other:?}"),
        }
    }

    #[test]
    fn a_missing_zero_or_nonfinite_rate_is_left_out() {
        let metrics = Metrics::new().expect("the recorder builds");
        let usage = crate::wire::NeutralUsage {
            output_tokens: Some(33),
            ..Default::default()
        };

        record_usage(&metrics, "zai", &usage, None);
        record_usage(&metrics, "zai", &usage, Some(0.0));
        record_usage(&metrics, "zai", &usage, Some(f64::NAN));
        record_usage(&metrics, "zai", &usage, Some(f64::INFINITY));

        let s = scrape(&metrics);
        assert!(
            s.iter()
                .all(|s| s.metric != "router_output_tokens_per_second_count"),
            "no rate sample should exist"
        );
    }

    /// A stored entry must hold the answer once. `wire` decodes with
    /// preservation off so the provider's exact reply is never kept beside the
    /// normalised one, and `cache_body` blanks the field anyway for a response
    /// that reached the cache by some other route.
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
            1,
            "the decoded response carries the answer once, not again in `preservation`"
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

    /// A scratch directory of this test's own, removed on drop.
    fn spill_dir(name: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("llmrouter-spill-{name}-"))
            .tempdir()
            .expect("the scratch directory is created")
    }

    /// One live `claude-fable-5-1` row with tools and a 128k window.
    fn fable_channel() -> crate::catalogue::Channel {
        use crate::catalogue::{Channel, ChannelHealth, ChannelId, ChannelTelemetry};
        use crate::WireName;

        Channel {
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
            created: None,
            health: ChannelHealth::live(),
            telemetry: ChannelTelemetry::default(),
        }
    }

    #[test]
    fn an_unserved_claude_name_routes_to_auto_and_a_served_one_is_kept() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![fable_channel()]);
        let route = |name: &str| route_claude_to_auto(&catalogue, Some(name.to_string()));

        assert_eq!(route("claude-opus-5-5"), None, "nothing serves it, so auto");
        assert_eq!(route("Claude-Haiku-5-5"), None, "the prefix ignores case");
        assert_eq!(
            route("Claude-Fable-5-1").as_deref(),
            Some("Claude-Fable-5-1"),
            "a channel serves it, so the name is kept"
        );
        assert_eq!(
            route("no-such-model").as_deref(),
            Some("no-such-model"),
            "only Claude names fall back; others still get their 404"
        );
        assert_eq!(route_claude_to_auto(&catalogue, None), None);
    }

    /// The 404 gate must use the same comparison as `Channel::matches`, which
    /// ignores ASCII case. A stricter gate turns away a model the race serves.
    #[test]
    fn a_named_model_in_another_case_is_not_rejected_before_the_race() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![fable_channel()]);

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
            spill_path: dir.path().to_path_buf(),
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
    }

    #[tokio::test]
    async fn above_the_threshold_the_outbound_body_goes_to_disk_and_is_deleted() {
        let dir = spill_dir("disk");
        let config = RouterConfig {
            spill_above_concurrent: 100,
            spill_path: dir.path().to_path_buf(),
            ..RouterConfig::default()
        };

        let prompt = Bytes::from(vec![b'x'; 4096]);
        let body = PromptBody::new(prompt.clone(), 101, &config).await;

        let path = body
            .spill_path()
            .expect("above the threshold it spills")
            .to_path_buf();
        assert!(path.is_file(), "the body is on disk, not in RAM");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "only the router can read a spilled prompt");
        }
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            !name.starts_with(&format!("{}-", std::process::id())),
            "the name is random, not pid plus a counter"
        );
        // A fallback channel call, twenty minutes later, still gets the prompt.
        assert_eq!(body.load().await.unwrap(), prompt);

        drop(body);
        assert!(
            !path.exists(),
            "the scratch file goes when the client has its answer"
        );
    }

    #[test]
    fn a_client_hanging_up_mid_spill_leaves_no_scratch_file() {
        let dir = spill_dir("hangup");
        let config = RouterConfig {
            spill_above_concurrent: 100,
            spill_path: dir.path().to_path_buf(),
            ..RouterConfig::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let prompt = Bytes::from(vec![b'x'; 4 * 1024 * 1024]);
            let spill = Box::pin(PromptBody::new(prompt, 101, &config));
            // Give the blocking write a beat to start, then abandon the
            // handler future, as axum does when the client disconnects.
            // Whether the write finished before or after the drop is a
            // property of the disk, not of the router; the cleanup must
            // hold either way. Asserting the first poll stayed pending was
            // a timing bet that failed on a fast CI disk.
            tokio::time::sleep(Duration::from_millis(1)).await;
            drop(spill);
        });
        // Dropping the runtime waits for the blocking pool to finish, so any
        // write still in flight has landed by now.
        drop(runtime);
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "an abandoned spill must not leave the prompt on disk"
        );
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

        let body = outbound_body(&request, false).expect("encodes");
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

    /// One whole answer, as the race hands it over.
    fn whole_answer(text: &str) -> crate::wire::NeutralResponse {
        let provider_reply = json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        crate::wire::decode_response(&provider_reply, Format::OpenAiChat)
            .expect("the provider reply decodes")
    }

    /// The whole body of a response, as text.
    async fn body_text(response: Response) -> String {
        String::from_utf8(read_body(response).await.to_vec()).expect("the body is utf-8")
    }

    #[test]
    fn the_two_streaming_settings_are_independent() {
        // `stream` in the request is about the client and `streaming` in
        // `router.yml` is about the providers. Neither one is ever a reason to
        // reject the other, and the client's flag never reaches a provider.
        let asked = crate::wire::decode_request(
            Format::OpenAiChat,
            &json!({
                "model": "claude-fable-5-1",
                "messages": [{"role": "user", "content": "fix this bug"}],
                "stream": true,
            }),
        )
        .expect("decodes");
        assert!(asked.stream, "the client did ask for a stream");

        let body = outbound_body(&asked, false).expect("the outbound body encodes");
        let value: Value = serde_json::from_slice(&body).expect("the outbound body is JSON");
        assert_eq!(
            value["stream"],
            Value::Bool(false),
            "a streaming client against buffered providers still posts stream: false"
        );

        let did_not_ask = crate::wire::decode_request(
            Format::OpenAiChat,
            &json!({
                "model": "claude-fable-5-1",
                "messages": [{"role": "user", "content": "fix this bug"}],
            }),
        )
        .expect("decodes");
        assert!(!did_not_ask.stream);

        let body = outbound_body(&did_not_ask, true).expect("the outbound body encodes");
        let value: Value = serde_json::from_slice(&body).expect("the outbound body is JSON");
        assert_eq!(
            value["stream"],
            Value::Bool(true),
            "the providers are always streamed when router.yml says so, because the \
             hedge strategy needs a stream to measure"
        );
        assert_eq!(
            value["stream_options"]["include_usage"],
            Value::Bool(true),
            "without this an OpenAI-shaped provider leaves usage out of the stream"
        );
        assert!(value.get("model").is_none(), "{value}");
    }

    /// A streaming client gets an SSE comment every interval until the first
    /// real byte, and none after it.
    #[tokio::test(start_paused = true)]
    async fn a_streaming_client_is_pinged_until_the_first_real_byte() {
        let work = Box::pin(async {
            // Past three intervals, which is the prefill DESIGN.md sizes the
            // keepalive against.
            tokio::time::sleep(Duration::from_secs(50)).await;
            stream_reply(
                whole_answer("the answer"),
                Format::OpenAiChat,
                "served".to_string(),
            )
            .expect("the whole answer chunks out")
        });

        let response = with_keepalive(work, true, Duration::from_secs(15)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );

        let body = body_text(response).await;
        assert!(
            body.matches(": ping").count() >= 2,
            "the client's read timer must keep resetting: {body}"
        );
        assert!(body.contains("the answer"), "{body}");
        let first_data = body.find("data:").expect("the answer is in the body");
        assert!(
            !body[first_data..].contains(": ping"),
            "the keepalive stops at the first real byte: {body}"
        );
        assert!(body.trim_end().ends_with("data: [DONE]"), "{body}");
    }

    /// A non-streaming client gets the `200` and its headers at once, then one
    /// space every interval, then the JSON body. JSON parsers skip leading
    /// whitespace, so the body still parses.
    #[tokio::test(start_paused = true)]
    async fn a_non_streaming_client_gets_a_space_every_interval_then_the_json() {
        let work = Box::pin(async {
            tokio::time::sleep(Duration::from_secs(50)).await;
            Served::Whole(axum::Json(json!({"id": "1", "content": "the answer"})).into_response())
        });

        let response = with_keepalive(work, false, Duration::from_secs(15)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");

        let body = read_body(response).await;
        assert!(
            body.starts_with(b" "),
            "the padding goes out before the body: {:?}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            body.iter().take_while(|b| **b == b' ').count() >= 2,
            "one space per interval"
        );
        let parsed: Value =
            serde_json::from_slice(&body).expect("leading whitespace still parses as JSON");
        assert_eq!(parsed["content"], "the answer");
    }

    /// An outcome decided inside the first interval has sent nothing yet, so it
    /// keeps its own status line. That is every outcome except a slow race.
    #[tokio::test(start_paused = true)]
    async fn an_outcome_decided_before_the_first_keepalive_keeps_its_status_line() {
        for wants_stream in [false, true] {
            let work = Box::pin(async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Served::Whole(
                    ApiError::NoChannel {
                        attempts: 3,
                        last_error: Some("401 invalid api key".to_string()),
                    }
                    .into_response(),
                )
            });

            let response = with_keepalive(work, wants_stream, Duration::from_secs(15)).await;
            assert_eq!(
                response.status(),
                StatusCode::GATEWAY_TIMEOUT,
                "wants_stream = {wants_stream}"
            );
            let body: Value = serde_json::from_slice(&read_body(response).await)
                .expect("the error body is JSON on its own");
            assert_eq!(body["error"], "no_channel");
        }
    }

    /// A zero interval turns the keepalive off and nothing is committed early.
    #[tokio::test(start_paused = true)]
    async fn a_zero_interval_sends_no_keepalive_at_all() {
        let work = Box::pin(async {
            tokio::time::sleep(Duration::from_secs(600)).await;
            Served::Whole(
                ApiError::NoChannel {
                    attempts: 1,
                    last_error: None,
                }
                .into_response(),
            )
        });

        let response = with_keepalive(work, false, Duration::ZERO).await;
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    /// A reply that arrived in one piece still serves a client that asked for a
    /// stream, in that client's own event format.
    #[tokio::test]
    async fn a_whole_answer_chunks_out_to_a_streaming_client() {
        let served = stream_reply(
            whole_answer("the answer"),
            Format::OpenAiChat,
            "served".to_string(),
        )
        .expect("the whole answer chunks out");
        let body = body_text(served.into_response()).await;
        assert!(body.contains("the answer"), "{body}");
        assert!(body.contains("\"model\":\"served\""), "{body}");
        assert!(body.trim_end().ends_with("data: [DONE]"), "{body}");

        // Anthropic names each event instead, and its own `message_stop` ends
        // the stream, so there is no `[DONE]` line to add.
        let served = stream_reply(
            whole_answer("the answer"),
            Format::AnthropicMessages,
            "served".to_string(),
        )
        .expect("the whole answer chunks out");
        let body = body_text(served.into_response()).await;
        assert!(body.contains("event: message_start"), "{body}");
        assert!(body.contains("event: message_stop"), "{body}");
        assert!(!body.contains("[DONE]"), "{body}");
    }

    /// A stream that dies mid-answer must not be framed as a clean ending.
    /// `data: [DONE]` tells an OpenAI client the half answer it already has is
    /// the whole answer, so nothing retries and the user never learns the
    /// reply was cut off. It must also not carry a server `error` event: that
    /// makes agent clients such as Claude Code stop instead of retrying, and
    /// the abrupt close is the one ending the client will treat as a
    /// transport failure and re-send the whole request for.
    #[tokio::test]
    async fn a_stream_that_dies_mid_answer_gets_no_done_marker() {
        let bytes = futures::stream::iter(vec![
            Ok(b"data: {\"choices\":[{\"delta\":{\"content\":\"half an \"}}]}\n\n".to_vec()),
            Err(crate::wire::WireError::Stream(
                "the connection dropped".to_string(),
            )),
        ]);
        let chunks = crate::wire::decode_stream(bytes, Format::OpenAiChat).expect("decodes");
        let events = crate::wire::encode_stream(chunks, Format::OpenAiChat, None)
            .expect("the chunks encode");

        let body =
            body_text(Served::Events(sse_frames(events, Format::OpenAiChat)).into_response()).await;
        assert!(
            body.contains("half an "),
            "the partial answer still goes out: {body}"
        );
        assert!(
            !body.contains("[DONE]"),
            "a death is not a clean ending: {body}"
        );
        assert!(
            !body.contains("event: error"),
            "a death is a transport failure to the client, not a server error: {body}"
        );
    }

    #[tokio::test]
    async fn an_event_is_named_for_anthropic_and_unnamed_for_openai() {
        async fn framed(format: Format, value: &Value) -> String {
            let event = sse_event(format, value);
            let frames: SseFrames = Box::pin(futures::stream::once(async move { Ok(event) }));
            body_text(Sse::new(frames).into_response()).await
        }
        let value = json!({"type": "content_block_delta", "index": 0});

        let anthropic = framed(Format::AnthropicMessages, &value).await;
        assert!(
            anthropic.contains("event: content_block_delta"),
            "{anthropic}"
        );
        assert!(anthropic.contains("data: {"), "{anthropic}");

        let openai = framed(Format::OpenAiChat, &value).await;
        assert!(!openai.contains("event:"), "{openai}");
        assert!(openai.contains("data: {"), "{openai}");
    }

    /// A provider can put a newline in its own event `type`, and an
    /// Anthropic-wire channel has its raw JSON replayed verbatim, so that name
    /// reaches the framing unchecked. Naming the event with it panics, which
    /// kills the connection after the `200` has gone out.
    #[tokio::test]
    async fn an_event_name_with_a_newline_does_not_kill_the_stream() {
        let value = json!({"type": "content_block_delta\n", "index": 0});
        let event = sse_event(Format::AnthropicMessages, &value);
        let frames: SseFrames = Box::pin(futures::stream::once(async move { Ok(event) }));
        let body = body_text(Sse::new(frames).into_response()).await;
        assert!(!body.contains("event:"), "the bad name is dropped: {body}");
        assert!(body.contains("data: {"), "the event still goes out: {body}");
    }

    /// A cached entry serves both shapes: one piece for a client that asked for
    /// one, a stream for a client that did not.
    #[tokio::test]
    async fn a_cached_entry_serves_a_streaming_client_as_a_stream() {
        let entry = CacheEntry {
            schema_version: crate::cache::SCHEMA_VERSION,
            created_at: 0,
            model_id: "claude-fable-5-1".to_string(),
            intelligence: 900,
            channel_id: crate::catalogue::ChannelId::new("openrouter", "sk-1", "claude-fable-5-1"),
            provider: "openrouter".to_string(),
            body: cache_body(&whole_answer("from the cache")).expect("the entry serialises"),
        };

        let streamed = cached_response(&entry, Format::OpenAiChat, None, true)
            .expect("the entry replays as a stream");
        let body = body_text(streamed.into_response()).await;
        assert!(body.contains("from the cache"), "{body}");
        assert!(body.trim_end().ends_with("data: [DONE]"), "{body}");

        let whole = cached_response(&entry, Format::OpenAiChat, None, false)
            .expect("the entry serves in one piece");
        let response = whole.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let parsed: Value = serde_json::from_slice(&read_body(response).await).expect("JSON");
        assert_eq!(parsed["choices"][0]["message"]["content"], "from the cache");
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

        // The scheme name is case-insensitive, so a lower-case one is still
        // stripped and the key itself is logged.
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "bearer sk-lower".parse().unwrap());
        assert_eq!(client_key_prefix(&headers), "sk-low");

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
                created: None,
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
    /// A router with no providers, so the catalogue stays cold. `dir` holds
    /// its scratch store and spill files.
    fn bare_state(dir: &tempfile::TempDir) -> Arc<AppState> {
        let config = Config {
            router: RouterConfig {
                spill_path: dir.path().to_path_buf(),
                ..RouterConfig::default()
            },
            providers: Vec::new(),
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: std::collections::HashMap::new(),
            capabilities: std::collections::HashMap::new(),
        };
        let db = Arc::new(
            redb::Database::create(dir.path().join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                config.router.health_poll.retry_base,
                config.router.health_poll.backoff_max,
                crate::metrics::Metrics::new().expect("the recorder builds"),
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
        let config = Arc::new(config);
        let racer = Arc::new(Racer::new(
            Arc::clone(&config),
            Arc::clone(&catalogue),
            Arc::clone(&health),
            upstream,
            net,
            Arc::clone(&metrics),
        ));
        Arc::new(AppState {
            // u64::MAX, so the RSS gate admits whatever this machine is using.
            admission: Admission::new(u64::MAX, std::time::Duration::from_secs(30)),
            config,
            catalogue,
            health,
            cache: None,
            racer,
            metrics,
            history: Arc::new(crate::history::History::open(
                &dir.path().join("history.redb"),
            )),
            reqlog: Arc::new(crate::reqlog::RequestLog::new()),
            started_at: Instant::now(),
        })
    }

    /// DESIGN.md "What it records": one info line per request with its
    /// outcome. A request turned away before the race used to leave nothing
    /// in the log to say how it ended.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_request_rejected_before_the_race_still_logs_its_outcome() {
        use tower::ServiceExt;

        let dir = spill_dir("outcome-line");
        let mut statuses = Vec::new();
        for body in [
            "not json".to_string(),
            json!({"model": "m", "max_tokens": 8, "messages": [{"role": "user", "content": "hi"}]})
                .to_string(),
        ] {
            let request = axum::http::Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body))
                .expect("the request builds");
            let response = router(bare_state(&dir))
                .oneshot(request)
                .await
                .expect("the router answers");
            statuses.push(response.status());
        }

        assert_eq!(
            statuses,
            vec![StatusCode::BAD_REQUEST, StatusCode::GATEWAY_TIMEOUT],
            "a bad body is a 400 and a cold catalogue is a 504"
        );
        // tracing-test installs one global subscriber, so the capture does not
        // race other tests for the callsite interest cache the way a scoped
        // `with_default` subscriber did.
        for outcome in ["bad_request", "no_channel"] {
            logs_assert(|lines: &[&str]| {
                lines
                    .iter()
                    .any(|line| {
                        line.contains(" INFO ")
                            && line.contains("served")
                            && line.contains(&format!("outcome=\"{outcome}\""))
                    })
                    .then_some(())
                    .ok_or_else(|| format!("no info outcome line for {outcome}: {lines:?}"))
            });
        }
    }

    /// DESIGN.md "What it records": one info line per request, and the client
    /// key prefix from "Client authentication" on that same line, so the
    /// prefix can be matched to its outcome under concurrency.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_request_writes_one_info_line_carrying_the_client_key() {
        use tower::ServiceExt;

        let dir = spill_dir("one-line");
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-api-key", "sk-onelinetest")
            .body(axum::body::Body::from("not json"))
            .expect("the request builds");
        let response = router(bare_state(&dir))
            .oneshot(request)
            .await
            .expect("the router answers");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        logs_assert(|lines: &[&str]| {
            let info: Vec<&&str> = lines.iter().filter(|l| l.contains(" INFO ")).collect();
            match info.as_slice() {
                [line] if line.contains("served") && line.contains("sk-one") => Ok(()),
                _ => Err(format!("want one info line with outcome and key: {info:?}")),
            }
        });
    }

    #[tokio::test]
    async fn a_body_above_two_mebibytes_reaches_the_handler() {
        use tower::ServiceExt;

        let dir = spill_dir("body-limit");
        let state = bare_state(&dir);

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
    }
}
