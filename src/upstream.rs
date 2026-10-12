//! The outbound HTTP client and one channel call.
//!
//! One `reqwest::Client` for the whole process. It pools per host internally
//! and a clone shares the pool, so there is no reason to build one per
//! provider.
//!
//! The outbound body is encoded once per client request and handed to every
//! channel call as a refcounted `Bytes`. Cloning it is a refcount bump, never
//! a copy of 3.8MB.
//!
//! The bodies are not byte-identical, because each call has to name its own
//! channel's model. So the body goes out as two chunks: a per-channel prefix
//! of about 100 bytes holding the model name, then the shared tail.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::header::{
    HeaderMap, ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER,
};

use crate::catalogue::Channel;
use crate::config::{RouterConfig, WireName};

/// The version header the Anthropic Messages API requires on every call. It is
/// a protocol version, not a model version, and it is fixed for the format.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The outbound body, split so every channel call shares one copy of the
/// prompt while still naming its own model.
///
/// `tail` is the encoded request with its `model` field removed and its
/// opening brace sliced off, so one call's body is `{"model":"<id>",` followed
/// by the tail. Cloning this copies a refcount, not 3.8MB.
#[derive(Debug, Clone)]
pub struct SharedBody {
    tail: Bytes,
}

impl SharedBody {
    /// Takes the encoded request, which must be a JSON object carrying no
    /// `model` field, and keeps everything after the opening brace.
    pub fn new(encoded: Bytes) -> Self {
        let tail = if encoded.first() == Some(&b'{') {
            encoded.slice(1..)
        } else {
            encoded
        };
        Self { tail }
    }

    /// The prefix naming this channel's own model. The comma is dropped when
    /// the tail holds no other field, which would otherwise be invalid JSON.
    fn prefix(&self, model_id: &str) -> Bytes {
        let name = serde_json::Value::String(model_id.to_string());
        let separator = if self.tail.as_ref() == b"}" { "" } else { "," };
        Bytes::from(format!("{{\"model\":{name}{separator}"))
    }

    /// The two chunks for one channel, prefix first.
    fn chunks(&self, model_id: &str) -> [Bytes; 2] {
        [self.prefix(model_id), self.tail.clone()]
    }

    /// The whole body for one channel in one buffer. Tests only: a real call
    /// sends the two chunks without joining them.
    #[cfg(test)]
    pub(crate) fn joined(&self, model_id: &str) -> Bytes {
        let [prefix, tail] = self.chunks(model_id);
        let mut out = bytes::BytesMut::with_capacity(prefix.len() + tail.len());
        out.extend_from_slice(&prefix);
        out.extend_from_slice(&tail);
        out.freeze()
    }
}

/// The shared client.
#[derive(Debug, Clone)]
pub struct Upstream {
    // Behind an `Arc` only so that two handles can be proved to share one
    // pool; `reqwest::Client` is already cheap to clone.
    client: Arc<reqwest::Client>,
    request_timeout: Duration,
}

/// A buffered channel call that came back with a status.
#[derive(Debug)]
pub struct CallOutcome {
    /// What the provider returned. Only a 2xx with non-empty content counts as
    /// healthy.
    pub status: u16,
    /// Seconds from a `Retry-After` header, both forms already parsed.
    pub retry_after_secs: Option<u64>,
    /// The whole reply.
    pub body: Bytes,
    /// Wall clock of the call.
    pub latency: Duration,
}

/// A streamed channel call whose headers have arrived.
pub struct StreamedCall {
    /// What the provider returned on the headers.
    pub status: u16,
    /// Seconds from a `Retry-After` header.
    pub retry_after_secs: Option<u64>,
    /// The raw server-sent-event bytes, still in the provider's format.
    pub body: std::pin::Pin<
        Box<dyn Stream<Item = std::result::Result<Vec<u8>, CallError>> + Send + 'static>,
    >,
}

impl std::fmt::Debug for StreamedCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamedCall")
            .field("status", &self.status)
            .field("retry_after_secs", &self.retry_after_secs)
            .finish_non_exhaustive()
    }
}

/// Why a channel call did not produce a reply. A status code is not an error
/// here: it arrives on [`CallOutcome`] and the race decides what it means.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The call passed `request_timeout`.
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    /// DNS, connect, TLS or a dropped socket.
    #[error("transport: {0}")]
    Transport(String),
    /// The race cancelled this call. Cancelled is not unhealthy: the channel
    /// keeps its current state.
    #[error("cancelled")]
    Cancelled,
}

impl Upstream {
    /// Builds the one client, with the HTTP/2 and pool settings from
    /// DESIGN.md: fixed stream and connection windows, adaptive window off,
    /// 64 idle connections per host, a 90 second idle timeout and a 30 second
    /// TCP keepalive.
    ///
    /// HTTP/2 is reached through ALPN rather than
    /// `http2_prior_knowledge`. DESIGN.md asks for prior knowledge "where the
    /// provider allows it", and one shared client cannot vary that per
    /// provider: switching it on would force h2 at every provider and break
    /// any that still only speaks HTTP/1.1. Every provider base URL is https,
    /// so ALPN already negotiates h2 wherever it is on offer, which is the
    /// multiplexing the memory budget depends on.
    pub fn new(config: &RouterConfig) -> crate::Result<Self> {
        let client = reqwest::Client::builder()
            // No redirects. reqwest's default is to follow up to 10, and on a
            // cross-host hop it only strips `authorization`, `cookie`,
            // `cookie2`, `proxy-authorization` and `www-authenticate`. An
            // Anthropic-wire channel carries the provider key in `x-api-key`,
            // which is not on that list, so a redirect would replay the bare
            // key to whatever host the `Location` names, over plain http if it
            // asks for it. An API call has no business following a redirect
            // anyway: a 3xx becomes an ordinary non-2xx status that the race
            // already treats as a failure.
            .redirect(reqwest::redirect::Policy::none())
            .http2_initial_stream_window_size(Some(clamp_window(config.http2_stream_window)))
            .http2_initial_connection_window_size(Some(clamp_window(
                config.http2_connection_window,
            )))
            .http2_adaptive_window(false)
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| crate::RouterError::Config(format!("http client: {e}")))?;

        Ok(Self {
            client: Arc::new(client),
            request_timeout: config.request_timeout,
        })
    }

    /// One buffered channel call. `body` is the shared outbound body; cloning
    /// it is a refcount bump.
    pub async fn call_buffered(
        &self,
        channel: &Channel,
        body: SharedBody,
    ) -> std::result::Result<CallOutcome, CallError> {
        let started = Instant::now();
        let response = self
            .request(channel, body, false)
            .send()
            .await
            .map_err(|e| self.classify(e))?;

        let status = response.status().as_u16();
        let retry_after_secs = retry_after_secs(response.headers(), SystemTime::now());
        let body = response.bytes().await.map_err(|e| self.classify(e))?;

        Ok(CallOutcome {
            status,
            retry_after_secs,
            body,
            latency: started.elapsed(),
        })
    }

    /// One streamed channel call. Returns once the headers arrive; the body
    /// follows as raw server-sent-event bytes.
    pub async fn call_streamed(
        &self,
        channel: &Channel,
        body: SharedBody,
    ) -> std::result::Result<StreamedCall, CallError> {
        let response = self
            .request(channel, body, true)
            .send()
            .await
            .map_err(|e| self.classify(e))?;

        let status = response.status().as_u16();
        let retry_after_secs = retry_after_secs(response.headers(), SystemTime::now());
        let request_timeout = self.request_timeout;
        let stream = response.bytes_stream().map(move |chunk| match chunk {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(e) => Err(classify_error(e, request_timeout)),
        });

        Ok(StreamedCall {
            status,
            retry_after_secs,
            body: Box::pin(stream),
        })
    }

    /// The client itself, for the probes and the network test, so they share
    /// one connection pool with the channel calls.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// True when both handles are clones of the same client, and so share one
    /// connection pool. Building a second [`Upstream`] gives a second pool.
    pub fn shares_client(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.client, &other.client)
    }

    /// The common part of both calls: one POST, the channel's key, and the
    /// shared body sent as a per-channel prefix plus the shared tail.
    ///
    /// The prefix names this channel's own `model_id`, which is the model this
    /// provider is being asked for. The client's model string never leaves the
    /// edge, because no provider would recognise it.
    ///
    /// `stream` only sets the `Accept` header. Whether the body asks the
    /// provider for a stream is decided where the body is encoded, and this
    /// module never rewrites the shared tail.
    fn request(
        &self,
        channel: &Channel,
        body: SharedBody,
        stream: bool,
    ) -> reqwest::RequestBuilder {
        let chunks = body.chunks(&channel.model_id);
        let length = chunks[0].len() + chunks[1].len();
        let body = reqwest::Body::wrap_stream(futures::stream::iter(
            chunks.map(Ok::<Bytes, std::convert::Infallible>),
        ));

        let mut builder = self
            .client
            .post(completion_url(channel))
            .header(CONTENT_TYPE, "application/json")
            // Set by hand: the body is a two-chunk stream, so without this it
            // would go out chunked and some providers refuse that.
            .header(CONTENT_LENGTH, length)
            .header(
                ACCEPT,
                if stream {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            );

        builder = match channel.wire {
            WireName::OpenAi => builder.header(AUTHORIZATION, auth_header(channel)),
            WireName::Anthropic => builder
                .header("x-api-key", auth_header(channel))
                .header("anthropic-version", ANTHROPIC_VERSION),
        };

        builder.body(body)
    }

    fn classify(&self, error: reqwest::Error) -> CallError {
        classify_error(error, self.request_timeout)
    }
}

/// Maps a `reqwest` failure onto the two kinds the race cares about. A status
/// code is never an error here, so only transport trouble and the timeout
/// reach this point. The error is formatted without its URL, which carries no
/// key but is noise in a log.
fn classify_error(error: reqwest::Error, request_timeout: Duration) -> CallError {
    if error.is_timeout() {
        CallError::Timeout(request_timeout)
    } else {
        CallError::Transport(error.without_url().to_string())
    }
}

/// Reads `Retry-After` off a reply. Both forms, a count of seconds and an HTTP
/// date, go through the one parser in `health`.
fn retry_after_secs(headers: &HeaderMap, now: SystemTime) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    crate::health::parse_retry_after(value.trim(), now)
}

/// `reqwest` takes a `u32` window. Anything larger in config is clamped rather
/// than refused: the window is a performance dial, not a correctness one.
fn clamp_window(bytes: u64) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

/// The completion endpoint for a channel, built from its base URL and wire
/// format.
pub fn completion_url(channel: &Channel) -> String {
    let base = channel.base_url.trim_end_matches('/');
    match channel.wire {
        WireName::OpenAi => format!("{base}/chat/completions"),
        WireName::Anthropic => format!("{base}/messages"),
    }
}

/// The authorisation header value for a channel. Never logged.
pub fn auth_header(channel: &Channel) -> String {
    match channel.wire {
        // Anthropic takes the bare key in `x-api-key`, OpenAI-format providers
        // take it as a bearer token.
        WireName::OpenAi => format!("Bearer {}", channel.key),
        WireName::Anthropic => channel.key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::{ChannelHealth, ChannelId, ChannelTelemetry};
    use std::sync::Arc;

    fn channel(base_url: &str, wire: WireName) -> Channel {
        Channel {
            id: ChannelId(1),
            provider: Arc::from("testprovider"),
            model_id: Arc::from("test-model"),
            key_id: Arc::from("key-0"),
            key: Arc::from("sk-secret-value"),
            base_url: Arc::from(base_url),
            wire,
            limit_context: 128_000,
            limit_output: 32_000,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_string()],
            intelligence: 50,
            intelligence_rank: 1,
            created: None,
            health: ChannelHealth::default(),
            telemetry: ChannelTelemetry::default(),
        }
    }

    #[test]
    fn openai_url_is_chat_completions() {
        let c = channel("https://api.example.com/v1", WireName::OpenAi);
        assert_eq!(
            completion_url(&c),
            "https://api.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn anthropic_url_is_messages() {
        let c = channel("https://api.anthropic.example/v1", WireName::Anthropic);
        assert_eq!(
            completion_url(&c),
            "https://api.anthropic.example/v1/messages"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_double_up() {
        let c = channel("https://api.example.com/v1/", WireName::OpenAi);
        assert_eq!(
            completion_url(&c),
            "https://api.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn openai_key_is_a_bearer_token_and_anthropic_is_bare() {
        let openai = channel("https://a", WireName::OpenAi);
        assert_eq!(auth_header(&openai), "Bearer sk-secret-value");

        let anthropic = channel("https://a", WireName::Anthropic);
        assert_eq!(auth_header(&anthropic), "sk-secret-value");
    }

    #[test]
    fn no_retry_after_header_means_none() {
        // The two header forms are parsed by `health::parse_retry_after`, which
        // is tested there. This covers the absent case, which is the common one.
        assert_eq!(retry_after_secs(&HeaderMap::new(), SystemTime::now()), None);
    }

    #[test]
    fn the_body_names_the_channels_own_model_not_the_clients() {
        // The client asked for "claude-sonnet-4-5"; the channel serves
        // "llama-3.3-70b". Without the per-channel prefix every provider in
        // the race would be asked for the client's name and refuse it.
        let shared = SharedBody::new(Bytes::from_static(
            br#"{"messages":[{"role":"user","content":"hi"}],"stream":false}"#,
        ));
        let body = shared.joined("llama-3.3-70b");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(value["model"], "llama-3.3-70b");
        assert_eq!(value["messages"][0]["content"], "hi");
        assert_eq!(value["stream"], false);

        // The same shared tail, a different channel, a different model.
        let other: serde_json::Value =
            serde_json::from_slice(&shared.joined("gemini-3.7-flash")).expect("valid json");
        assert_eq!(other["model"], "gemini-3.7-flash");
    }

    #[test]
    fn a_model_id_with_a_quote_in_it_stays_valid_json() {
        let shared = SharedBody::new(Bytes::from_static(br#"{"stream":true}"#));
        let value: serde_json::Value =
            serde_json::from_slice(&shared.joined("od\"d")).expect("valid json");
        assert_eq!(value["model"], "od\"d");
    }

    #[test]
    fn an_empty_body_does_not_grow_a_trailing_comma() {
        let shared = SharedBody::new(Bytes::from_static(b"{}"));
        let value: serde_json::Value =
            serde_json::from_slice(&shared.joined("m")).expect("valid json");
        assert_eq!(value["model"], "m");
    }

    #[test]
    fn an_oversized_window_clamps_instead_of_failing() {
        assert_eq!(clamp_window(131_072), 131_072);
        assert_eq!(clamp_window(u64::MAX), u32::MAX);
    }

    /// A redirect must not be followed. An Anthropic-wire channel sends the
    /// raw provider key in `x-api-key`, and reqwest does not strip that header
    /// on a cross-host hop, so following a `Location` would hand the key to
    /// whatever host it names. The redirect should surface as a plain 302 and
    /// the host it points at should never be contacted.
    #[tokio::test]
    async fn a_redirect_is_not_followed_and_the_key_never_reaches_the_new_host() {
        use tokio::io::AsyncWriteExt;

        // The host the redirect points at. It only has to notice a connection.
        let collector = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port binds");
        let collector_addr = collector.local_addr().expect("the port is known");
        let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&reached);
        let collector_task = tokio::spawn(async move {
            if collector.accept().await.is_ok() {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        // The provider, which answers every request with a redirect.
        let provider = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port binds");
        let provider_addr = provider.local_addr().expect("the port is known");
        let provider_task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = provider.accept().await {
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{collector_addr}/v1/messages\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let upstream = Upstream::new(&RouterConfig::default()).expect("client builds");
        let channel = channel(&format!("http://{provider_addr}/v1"), WireName::Anthropic);
        let body = SharedBody::new(Bytes::from_static(br#"{"messages":[]}"#));

        let outcome = upstream
            .call_buffered(&channel, body)
            .await
            .expect("the redirect is a status, not a transport failure");
        assert_eq!(outcome.status, 302, "the 3xx must surface as a status");
        assert!(
            !reached.load(std::sync::atomic::Ordering::SeqCst),
            "the key must never reach the host the redirect named"
        );

        collector_task.abort();
        provider_task.abort();
    }

    #[test]
    fn the_client_builds_from_the_default_config() {
        let config = RouterConfig::default();
        let upstream = Upstream::new(&config).expect("client builds");
        assert_eq!(upstream.request_timeout, config.request_timeout);
    }
}
