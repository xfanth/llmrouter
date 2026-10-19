//! The only module that names `switchyard_translation`.
//!
//! Four buffered functions and two streaming ones. Everything else in the
//! router sees [`NeutralRequest`], [`NeutralResponse`], [`ChunkStream`] and
//! [`EventStream`], so swapping the translation crate is a change to this file
//! alone.
//!
//! Signatures below match ARCHITECTURE.md, which were verified by compiling
//! them rather than read from documentation. Note the argument order differs
//! between the request and response pairs.

use futures::{Stream, StreamExt};
use serde_json::Value;
use std::sync::LazyLock;
use switchyard_protocol::LlmClientError;
use switchyard_translation::{PreservationPolicy, TranslationEngine, TranslationPolicy};

/// The crate's free functions hard-wire `TranslationPolicy::default()`, whose
/// `preservation` is `InMemory`: every decode deep-clones the whole inbound
/// body into the neutral value and keeps it there. That doubles the parsed
/// prompt in memory and undoes rule 2, freeing the raw body after the parse.
///
/// Nothing here reads a preserved body: `canonical_bytes` drops the field
/// before hashing and the cache blanks it. The one thing lost is the encoder's
/// shortcut of replaying a caller's exact body when source and target formats
/// match; the outbound body is rebuilt from the neutral value instead, and
/// `UnknownFieldPolicy::Preserve` still carries the odd fields across.
static POLICY: LazyLock<TranslationPolicy> = LazyLock::new(|| TranslationPolicy {
    preservation: PreservationPolicy::Disabled,
    ..TranslationPolicy::default()
});
static ENGINE: LazyLock<TranslationEngine> = LazyLock::new(TranslationEngine::default);

/// Which wire format a body is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// `POST /v1/chat/completions`.
    OpenAiChat,
    /// `POST /v1/messages`.
    AnthropicMessages,
    /// `POST /v1/responses`. Not served, but the translation handles it.
    OpenAiResponses,
}

impl From<Format> for switchyard_translation::WireFormat {
    fn from(format: Format) -> Self {
        match format {
            Format::OpenAiChat => Self::OpenAiChat,
            Format::AnthropicMessages => Self::AnthropicMessages,
            Format::OpenAiResponses => Self::OpenAiResponses,
        }
    }
}

/// The neutral request every module except this one works with.
pub type NeutralRequest = switchyard_translation::llm::LlmRequest;

/// The neutral aggregated response.
pub type NeutralResponse = switchyard_translation::llm::AggLlmResponse;

/// The token counts off a neutral response. Named here so the metrics call
/// sites can hold the usage after the response itself has been handed on to
/// be encoded, without any other module naming the translation crate.
pub type NeutralUsage = switchyard_translation::llm::Usage;

/// Anything the translation layer can refuse.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The inbound body is not a valid request in its stated format.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Translating a request or response failed.
    #[error("translation failed: {0}")]
    Translation(String),
    /// The byte stream feeding a decode failed partway through.
    #[error("stream failed: {0}")]
    Stream(String),
}

/// A stream of neutral response chunks. Opaque on purpose: the race passes it
/// around without naming what is inside.
pub struct ChunkStream(switchyard_translation::LlmResponseStream);

impl ChunkStream {
    /// Calls `on_error` for each item the stream fails on, passing the item
    /// through untouched.
    ///
    /// A provider that closes its body part way through an answer produces a
    /// clean end of stream, not a transport error, so the byte stream sees
    /// nothing wrong. The decoder is the first thing that knows: it reaches
    /// the end with no terminal event and fails here. This is the hook that
    /// carries that back to the caller.
    ///
    /// A provider can also report a failure inside an otherwise healthy
    /// stream, as an error frame part way through an answer. The decoder
    /// normalises that into a `StreamError` chunk and yields it as an `Ok`
    /// item, so it never reaches the failing branch. It is still the channel
    /// failing mid-answer, so it fires the hook too, and so does a frame the
    /// decoder could not read at all.
    pub fn on_error(self, on_error: impl Fn() + Send + 'static) -> Self {
        Self(Box::pin(self.0.map(move |item| {
            let failed = match &item {
                Err(_) => true,
                Ok(event) => event.normalized().iter().any(|chunk| {
                    matches!(
                        chunk,
                        switchyard_protocol::stream::LlmResponseChunk::StreamError { .. }
                            | switchyard_protocol::stream::LlmResponseChunk::DecodeError { .. }
                    )
                }),
            };
            if failed {
                on_error();
            }
            item
        })))
    }
    /// Calls `on_terminal` once the answer's terminal chunk has gone past,
    /// passing every item through untouched.
    ///
    /// `MessageStop` is where both wire formats meet: the OpenAI decoder makes
    /// one out of `finish_reason`, the Anthropic decoder out of `message_stop`.
    /// It is the only neutral signal that the answer is complete, which is what
    /// the stall timer upstream needs so it stops treating a quiet socket as a
    /// dead channel.
    pub fn on_terminal(self, on_terminal: impl Fn() + Send + 'static) -> Self {
        Self(Box::pin(futures::TryStreamExt::inspect_ok(
            self.0,
            move |event| {
                if event.normalized().iter().any(|chunk| {
                    matches!(
                        chunk,
                        switchyard_protocol::stream::LlmResponseChunk::MessageStop { .. }
                    )
                }) {
                    on_terminal();
                }
            },
        )))
    }
}

/// A stream of wire-format event objects, ready for the server to frame as
/// server-sent events.
pub struct EventStream {
    inner: switchyard_translation::RawEventStream,
    /// One timestamp for the whole stream, so every chunk of an answer carries
    /// the same `created`, as the OpenAI shape expects.
    created: u64,
}

impl EventStream {
    /// Pulls the next event, or `None` at the end of the stream.
    pub async fn next(&mut self) -> Option<std::result::Result<Value, WireError>> {
        let created = self.created;
        self.inner.next().await.map(|item| {
            item.map_err(|error| WireError::Stream(error.to_string()))
                .map(|mut event| {
                    stamp_created(&mut event, created);
                    event
                })
        })
    }
}

/// Seconds since the unix epoch, saturating at zero before 1970.
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

/// Fills the OpenAI `created` field when the translation crate left it at
/// zero.
///
/// `switchyard-translation` hard-codes `"created": 0` on every OpenAI-shaped
/// body it builds, buffered and streaming alike, because the neutral response
/// has no equivalent field and the Anthropic shape never needed one. A strict
/// OpenAI client parses that as 1 January 1970, so the router stamps the real
/// time on the way out. A non-zero value the crate did produce is left alone.
fn stamp_created(body: &mut Value, created: u64) {
    let Some(object) = body.as_object_mut() else {
        return;
    };
    if object.get("created") == Some(&Value::from(0)) {
        object.insert("created".to_owned(), Value::from(created));
    }
}

/// Edge in: the body the client sent, in whichever format its endpoint uses.
pub fn decode_request(
    format: Format,
    body: &Value,
) -> std::result::Result<NeutralRequest, WireError> {
    ENGINE
        .decode_request(
            switchyard_translation::WireFormat::from(format),
            body,
            &POLICY,
        )
        .map(|output| output.request)
        .map_err(|error| WireError::InvalidRequest(error.to_string()))
}

/// Edge out: the body one channel receives, in that provider's format.
pub fn encode_request(
    request: &NeutralRequest,
    format: Format,
) -> std::result::Result<Value, WireError> {
    let mut body = switchyard_translation::encode_request(request, format.into())
        .map_err(|error| WireError::Translation(error.to_string()))?;
    restore_stop(request, format, &mut body);
    Ok(body)
}

/// Puts the caller's stop sequences back on an OpenAI chat body.
///
/// The translation crate's OpenAI decoder parks `stop` in
/// `extensions.fields.stop`, but its OpenAI encoder only looks for
/// `stop_sequences`, the Anthropic spelling. So an OpenAI client talking to an
/// OpenAI provider, the commonest path we serve, silently loses its stop
/// sequences and the model runs past where the caller asked it to halt. The
/// Anthropic encoder reads both spellings, so only this one edge needs it.
///
/// Worth keeping even once the crate is fixed: `entry` leaves an already
/// encoded `stop` alone.
fn restore_stop(request: &NeutralRequest, format: Format, body: &mut Value) {
    if format != Format::OpenAiChat {
        return;
    }
    let Some(stop) = request.extensions.fields.get("stop") else {
        return;
    };
    if let Some(object) = body.as_object_mut() {
        object.entry("stop").or_insert_with(|| stop.clone());
    }
}

/// A winning channel's buffered reply, in that provider's format.
pub fn decode_response(
    body: &Value,
    format: Format,
) -> std::result::Result<NeutralResponse, WireError> {
    ENGINE
        .decode_response(
            switchyard_translation::WireFormat::from(format),
            body,
            &POLICY,
        )
        .map(|output| output.response)
        .map_err(|error| WireError::Translation(error.to_string()))
}

/// Edge out: the reply the client receives, in the format its endpoint uses.
/// `served_model` overrides the model name on the response.
pub fn encode_response(
    response: &NeutralResponse,
    format: Format,
    served_model: Option<&str>,
) -> std::result::Result<Value, WireError> {
    let mut body =
        switchyard_translation::encode_aggregated_response(response, format.into(), served_model)
            .map_err(|error| WireError::Translation(error.to_string()))?;
    stamp_created(&mut body, now_unix());
    Ok(body)
}

/// Edge out, streaming: a channel's raw server-sent-event bytes become neutral
/// chunks.
pub fn decode_stream<S>(bytes: S, source: Format) -> std::result::Result<ChunkStream, WireError>
where
    S: Stream<Item = std::result::Result<Vec<u8>, WireError>> + Send + 'static,
{
    // The crate wants its own error on the byte stream, so carry ours across as
    // a transport failure rather than flattening it to a string.
    let bytes = bytes.map(|item| {
        item.map_err(|error| LlmClientError::Transport {
            source: Box::new(error),
        })
    });
    switchyard_translation::decode_stream(bytes, source.into())
        .map(ChunkStream)
        .map_err(|error| WireError::Stream(error.to_string()))
}

/// Edge in, streaming: neutral chunks become wire events in the client's
/// format.
pub fn encode_stream(
    chunks: ChunkStream,
    target: Format,
    served_model: Option<String>,
) -> std::result::Result<EventStream, WireError> {
    switchyard_translation::encode_stream(chunks.0, target.into(), served_model)
        .map(|inner| EventStream {
            inner,
            created: now_unix(),
        })
        .map_err(|error| WireError::Stream(error.to_string()))
}

/// Folds a neutral chunk stream into one whole answer.
///
/// This is the buffering half of **Streaming**: the providers always stream,
/// so a client that did not ask for one still has to be handed a single piece.
/// The fold is the translation crate's own, which is the only thing that knows
/// how tool-call arguments and reasoning details rejoin across chunks.
pub async fn aggregate_stream(
    chunks: ChunkStream,
) -> std::result::Result<NeutralResponse, WireError> {
    switchyard_protocol::stream::LlmResponse::Stream(chunks.0)
        .into_agg()
        .await
        .map_err(|error| WireError::Stream(error.to_string()))
}

/// Splits a chunk stream in two.
///
/// The returned stream forwards every event onwards untouched. A copy of the
/// whole run arrives on the receiver once the stream has finished cleanly, so
/// the caller can fold it and offer it to the cache. A stream that fails or is
/// dropped part way sends nothing, which is the cache rule: only a complete
/// stream goes in.
/// What one stream did, as watched by [`tap_stream`]: the answer text and
/// the usage block the provider reported.
#[derive(Debug, Default)]
pub struct StreamFacts {
    /// The first part of the answer text, capped at [`TAP_TEXT_CHARS`].
    pub text: String,
    /// The provider's usage block, on a stream that ran to its end. A broken
    /// stream sends no usage: a partial answer is not a whole one.
    pub usage: Option<NeutralUsage>,
}

/// How much answer text one tap keeps for the request log.
const TAP_TEXT_CHARS: usize = 2_000;

/// Passes a stream through untouched and hands back what it produced: the
/// first part of the answer text and the usage block, once the stream has
/// run to its end or died.
///
/// Separate from [`tee_stream`], which keeps a copy of every event so the
/// whole answer can be folded for the cache. Counting tokens does not need
/// the answer, only its last usage chunk, and hanging the count off the
/// cache's copy made it depend on an unrelated config flag: with
/// `cache.enabled: false` a streaming client was counting no tokens at all.
/// Teeing a second copy to fix that would put every streamed body in RAM
/// twice, and memory is what killed omniroute, so this watches the stream go
/// past and keeps five numbers.
///
/// Nothing is sent when the provider reported no usage, so a receiver that
/// resolves to an error means there was nothing to count.
pub fn tap_stream(
    chunks: ChunkStream,
) -> (ChunkStream, tokio::sync::oneshot::Receiver<StreamFacts>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut inner = chunks.0;
    let forwarded = async_stream::stream! {
        let mut facts = StreamFacts { usage: None, text: String::new() };
        while let Some(item) = inner.next().await {
            if let Ok(event) = &item {
                // The last usage block wins: a provider may report more than
                // once over a stream, and the final figure is the whole
                // answer's rather than a running part of it.
                for chunk in event.normalized().iter() {
                    match chunk {
                        switchyard_protocol::stream::LlmResponseChunk::Usage(reported) => {
                            facts.usage = Some(reported.clone());
                        }
                        switchyard_protocol::stream::LlmResponseChunk::TextDelta { text, .. } => {
                            push_capped(&mut facts.text, text, TAP_TEXT_CHARS);
                        }
                        _ => {}
                    }
                }
            }
            let failed = item.is_err();
            yield item;
            if failed {
                // A broken stream is a partial answer: the text so far goes
                // to the request log's row, the usage block does not.
                let _ = sender.send(facts);
                return;
            }
        }
        let _ = sender.send(facts);
    };
    (ChunkStream(Box::pin(forwarded)), receiver)
}

/// Appends to a string up to a character cap.
fn push_capped(target: &mut String, add: &str, cap: usize) {
    let room = cap.saturating_sub(target.chars().count());
    if room > 0 {
        target.push_str(&add.chars().take(room).collect::<String>());
    }
}

pub fn tee_stream(
    chunks: ChunkStream,
) -> (ChunkStream, tokio::sync::oneshot::Receiver<ChunkStream>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut inner = chunks.0;
    let forwarded = async_stream::stream! {
        let mut copy = Vec::new();
        while let Some(item) = inner.next().await {
            match item {
                Ok(event) => {
                    copy.push(event.clone());
                    yield Ok(event);
                }
                Err(error) => {
                    yield Err(error);
                    return;
                }
            }
        }
        let _ = sender.send(ChunkStream(Box::pin(futures::stream::iter(
            copy.into_iter().map(Ok),
        ))));
    };
    (ChunkStream(Box::pin(forwarded)), receiver)
}

/// Turns a whole answer back into a chunk stream, for replaying a cached or
/// buffered reply to a client that asked for a stream.
pub fn stream_of(response: NeutralResponse) -> ChunkStream {
    ChunkStream(response.into_stream())
}

/// The format one of the router's own endpoints speaks, from its path.
pub fn format_for_path(path: &str) -> Option<Format> {
    match path.trim_end_matches('/') {
        "/v1/messages" => Some(Format::AnthropicMessages),
        "/v1/chat/completions" => Some(Format::OpenAiChat),
        _ => None,
    }
}

/// The format a provider speaks, from `providers.yml`.
pub fn format_for_provider(wire: crate::config::WireName) -> Format {
    match wire {
        crate::config::WireName::OpenAi => Format::OpenAiChat,
        crate::config::WireName::Anthropic => Format::AnthropicMessages,
    }
}

/// Fields dropped before hashing. The model is chosen by the router, not the
/// caller, and the streaming flag does not change the answer. `preservation`
/// holds the caller's original body keyed by format, which is exactly what two
/// clients asking the same question disagree about.
const EXCLUDED_FIELDS: [&str; 3] = ["model", "stream", "preservation"];

/// Provider extension fields dropped before hashing. `extensions.fields` holds
/// everything the codec had no neutral home for, and most of it changes the
/// answer: `stop`, `seed`, `n`, `logit_bias`, `frequency_penalty` and
/// `presence_penalty` all reach the provider and all move the output, so they
/// stay in the key. Only caller identity, bookkeeping and streaming options
/// come out.
///
/// `switchyard_anthropic_request` is the translation crate's own marker: the
/// Anthropic codec stamps it on every request it decodes and strips it again
/// on encode. It says which endpoint the client used, nothing about the
/// answer, so leaving it in would give an Anthropic client and an OpenAI
/// client different keys for the same conversation, which is the one thing
/// the key is built to avoid.
const EXCLUDED_EXTENSION_FIELDS: [&str; 10] = [
    "switchyard_anthropic_request",
    "metadata",
    "prompt_cache_key",
    "prompt_cache_retention",
    "request_id",
    "safety_identifier",
    "service_tier",
    "store",
    "stream_options",
    "user",
];

/// Serialises a neutral request canonically: object keys sorted, no
/// whitespace, one fixed number format. The cache key hashes this, so two
/// requests that differ only in field order must produce the same bytes.
///
/// The model is excluded, along with the client key, headers, request ids,
/// caller identity, timestamps and the streaming flag. Provider extension
/// fields that change the answer, stop sequences among them, are kept.
pub fn canonical_bytes(request: &NeutralRequest) -> std::result::Result<Vec<u8>, WireError> {
    let mut value =
        serde_json::to_value(request).map_err(|error| WireError::Translation(error.to_string()))?;

    let Some(object) = value.as_object_mut() else {
        return Err(WireError::Translation(
            "neutral request did not serialise to an object".into(),
        ));
    };
    for field in EXCLUDED_FIELDS {
        object.remove(field);
    }
    // Narrow removal, not the whole bag: see EXCLUDED_EXTENSION_FIELDS.
    if let Some(fields) = object
        .get_mut("extensions")
        .and_then(Value::as_object_mut)
        .and_then(|extensions| extensions.get_mut("fields"))
        .and_then(Value::as_object_mut)
    {
        for field in EXCLUDED_EXTENSION_FIELDS {
            fields.remove(field);
        }
    }
    // Two callers may send the same tools in a different order. Sort by name so
    // they land on the same key.
    if let Some(tools) = object.get_mut("tools").and_then(Value::as_array_mut) {
        tools.sort_by(|left, right| {
            let name = |tool: &Value| {
                tool.get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            name(left).cmp(&name(right))
        });
    }

    // serde_json has preserve_order on in this build, so sort every nested
    // object explicitly before writing.
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(|error| WireError::Translation(error.to_string()))
}

/// True when the request carries tool definitions. Claude Code always does, and
/// the filter drops any model without tool support.
pub fn has_tools(request: &NeutralRequest) -> bool {
    !request.tools.is_empty()
}

/// The model the caller named, if any. `auto` and an absent model both give
/// `None`.
pub fn named_model(request: &NeutralRequest) -> Option<String> {
    let named = request.model.as_deref()?.trim();
    if named.is_empty() || named.eq_ignore_ascii_case("auto") {
        return None;
    }
    Some(named.to_owned())
}

/// What the filter assumes the caller wants when the request names no limit.
const DEFAULT_OUTPUT_TOKENS: u32 = 4_096;

/// The largest completion the caller asked for, which the filter adds to the
/// prompt estimate.
pub fn requested_output_tokens(request: &NeutralRequest) -> u32 {
    request
        .output
        .max_output_tokens
        .map(|tokens| u32::try_from(tokens).unwrap_or(u32::MAX))
        .unwrap_or(DEFAULT_OUTPUT_TOKENS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use switchyard_translation::llm::{
        ContentBlock, InstructionBlock, Message, Role, ToolDefinition,
    };

    fn tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_owned(),
            description: None,
            parameters: serde_json::json!({"type": "object"}),
            strict: None,
        }
    }

    /// The forwarded half must deliver every event, and the copy must fold
    /// back into the same answer, or a streamed reply can never be cached.
    #[tokio::test]
    async fn a_teed_stream_forwards_and_yields_a_foldable_copy() {
        let provider_reply = serde_json::json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello world"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        let response =
            decode_response(&provider_reply, Format::OpenAiChat).expect("the reply decodes");

        let (mut forwarded, complete) = tee_stream(stream_of(response));
        let mut seen = 0;
        while let Some(item) = forwarded.0.next().await {
            item.expect("every forwarded event is good");
            seen += 1;
        }
        assert!(seen > 0, "the forwarded half carries the events");

        let copy = complete.await.expect("a complete stream hands back a copy");
        let folded = aggregate_stream(copy).await.expect("the copy folds");
        let out = encode_response(&folded, Format::OpenAiChat, Some("served"))
            .expect("the folded answer translates");
        assert_eq!(out["choices"][0]["message"]["content"], "hello world");
    }

    /// The translation crate hard-codes `"created": 0` on the OpenAI shape, so
    /// the router has to stamp the real time on the way out. A client that
    /// parses the field as a date would otherwise read 1 January 1970.
    #[tokio::test]
    async fn an_openai_reply_carries_a_real_created_timestamp() {
        let provider_reply = serde_json::json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello world"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        let response =
            decode_response(&provider_reply, Format::OpenAiChat).expect("the reply decodes");

        // Anything after the start of 2025 proves it is a clock reading and not
        // the crate's zero.
        let floor = 1_735_689_600;
        let buffered = encode_response(&response, Format::OpenAiChat, Some("served"))
            .expect("the answer translates");
        assert!(
            buffered["created"].as_u64().expect("created is a number") > floor,
            "the buffered reply carries a real timestamp, got {}",
            buffered["created"]
        );

        let mut events = encode_stream(
            stream_of(response),
            Format::OpenAiChat,
            Some("served".to_owned()),
        )
        .expect("the stream translates");
        let mut stamps = Vec::new();
        while let Some(event) = events.next().await {
            let event = event.expect("every event is good");
            if let Some(created) = event.get("created").and_then(Value::as_u64) {
                stamps.push(created);
            }
        }
        assert!(!stamps.is_empty(), "the stream carries created fields");
        assert!(
            stamps.iter().all(|at| *at > floor),
            "every chunk carries a real timestamp, got {stamps:?}"
        );
        assert!(
            stamps.windows(2).all(|pair| pair[0] == pair[1]),
            "one timestamp for the whole stream, got {stamps:?}"
        );
    }

    /// A stream the client abandons part way must leave nothing behind: a
    /// partial answer in the cache is a wrong answer.
    #[tokio::test]
    async fn an_abandoned_stream_offers_nothing_to_the_cache() {
        let provider_reply = serde_json::json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello world"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        let response =
            decode_response(&provider_reply, Format::OpenAiChat).expect("the reply decodes");

        let (forwarded, complete) = tee_stream(stream_of(response));
        drop(forwarded);
        assert!(complete.await.is_err(), "nothing is offered to the cache");
    }

    /// Rule 2 frees the inbound body after the parse. That only saves anything
    /// if the decode has not kept a copy of it inside the neutral request.
    #[test]
    fn a_decode_keeps_no_copy_of_the_inbound_body() {
        let body = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "a long prompt"}]
        });
        let request = decode_request(Format::OpenAiChat, &body).expect("the body decodes");
        assert!(
            request.preservation.requests.is_empty(),
            "the inbound body must not be cloned into the neutral request"
        );

        let reply = serde_json::json!({
            "id": "1",
            "model": "upstream",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
        });
        let response = decode_response(&reply, Format::OpenAiChat).expect("the reply decodes");
        assert!(
            response.preservation.responses.is_empty(),
            "the provider reply must not be cloned into the neutral response"
        );
    }

    #[test]
    fn paths_map_to_formats() {
        assert_eq!(
            format_for_path("/v1/messages"),
            Some(Format::AnthropicMessages)
        );
        assert_eq!(
            format_for_path("/v1/chat/completions"),
            Some(Format::OpenAiChat)
        );
        assert_eq!(
            format_for_path("/v1/messages/"),
            Some(Format::AnthropicMessages)
        );
        assert_eq!(format_for_path("/v1/responses"), None);
        assert_eq!(format_for_path("/healthz"), None);
    }

    #[test]
    fn provider_wire_names_map_to_formats() {
        assert_eq!(
            format_for_provider(crate::config::WireName::OpenAi),
            Format::OpenAiChat
        );
        assert_eq!(
            format_for_provider(crate::config::WireName::Anthropic),
            Format::AnthropicMessages
        );
    }

    #[test]
    fn auto_and_empty_models_are_none() {
        let mut request = NeutralRequest::default();
        assert_eq!(named_model(&request), None);

        request.model = Some("auto".into());
        assert_eq!(named_model(&request), None);
        request.model = Some("AUTO".into());
        assert_eq!(named_model(&request), None);
        request.model = Some("  ".into());
        assert_eq!(named_model(&request), None);

        request.model = Some(" gpt-4o ".into());
        assert_eq!(named_model(&request), Some("gpt-4o".to_owned()));
    }

    #[test]
    fn tools_are_detected() {
        let mut request = NeutralRequest::default();
        assert!(!has_tools(&request));
        request.tools.push(tool("read_file"));
        assert!(has_tools(&request));
    }

    #[test]
    fn output_tokens_fall_back_and_saturate() {
        let mut request = NeutralRequest::default();
        assert_eq!(requested_output_tokens(&request), DEFAULT_OUTPUT_TOKENS);

        request.output.max_output_tokens = Some(32_000);
        assert_eq!(requested_output_tokens(&request), 32_000);

        request.output.max_output_tokens = Some(u64::MAX);
        assert_eq!(requested_output_tokens(&request), u32::MAX);
    }

    #[test]
    fn canonical_bytes_sort_keys_and_drop_whitespace() {
        // A nested object inserted out of order, deep inside the request, must
        // come out with every key sorted and no whitespace.
        let mut request = NeutralRequest::default();
        request.extensions.fields.insert(
            "logit_bias".into(),
            serde_json::json!({"b": 1, "a": {"d": [3, {"y": 0, "x": 0}], "c": null}}),
        );
        let text = String::from_utf8(canonical_bytes(&request).unwrap()).unwrap();
        assert!(
            text.contains(r#""logit_bias":{"a":{"c":null,"d":[3,{"x":0,"y":0}]},"b":1}"#),
            "{text}"
        );
        assert!(!text.contains(char::is_whitespace), "{text}");
    }

    #[test]
    fn canonical_bytes_ignore_the_model_and_the_streaming_flag() {
        let mut first = NeutralRequest {
            model: Some("auto".into()),
            stream: false,
            ..NeutralRequest::default()
        };
        first
            .messages
            .push(Message::text(Role::User, "fix this bug"));

        let mut second = first.clone();
        second.model = Some("claude-sonnet-4".into());
        second.stream = true;

        assert_eq!(
            canonical_bytes(&first).unwrap(),
            canonical_bytes(&second).unwrap()
        );
    }

    #[test]
    fn canonical_bytes_ignore_the_caller_body_kept_for_replay() {
        // The whole point of hashing the neutral request: an Anthropic client
        // and an OpenAI client asking the same question must share a key, even
        // though each carries its own original body in `preservation`.
        let mut anthropic = NeutralRequest::default();
        anthropic
            .messages
            .push(Message::text(Role::User, "fix this bug"));
        anthropic.preservation.requests.insert(
            switchyard_translation::WireFormat::AnthropicMessages.into(),
            serde_json::json!({"messages": "anthropic shape"}),
        );

        let mut openai = NeutralRequest::default();
        openai
            .messages
            .push(Message::text(Role::User, "fix this bug"));
        openai.preservation.requests.insert(
            switchyard_translation::WireFormat::OpenAiChat.into(),
            serde_json::json!({"messages": "openai shape"}),
        );

        assert_eq!(
            canonical_bytes(&anthropic).unwrap(),
            canonical_bytes(&openai).unwrap()
        );
    }

    #[test]
    fn canonical_bytes_keep_answer_changing_provider_extensions() {
        // A request carrying `stop` must not share a key with the same request
        // without it: the provider truncates one answer and not the other.
        let base = {
            let mut request = NeutralRequest::default();
            request
                .messages
                .push(Message::text(Role::User, "write a shell script"));
            request
        };

        for (field, value) in [
            ("stop", serde_json::json!(["```"])),
            ("seed", serde_json::json!(7)),
            ("n", serde_json::json!(3)),
            ("logit_bias", serde_json::json!({"1234": -100})),
            ("frequency_penalty", serde_json::json!(0.5)),
            ("presence_penalty", serde_json::json!(0.5)),
        ] {
            let mut tweaked = base.clone();
            tweaked
                .extensions
                .fields
                .insert(field.to_owned(), value.clone());
            assert_ne!(
                canonical_bytes(&base).unwrap(),
                canonical_bytes(&tweaked).unwrap(),
                "{field} must stay in the cache key"
            );
        }
    }

    #[test]
    fn canonical_bytes_ignore_caller_identity_extensions() {
        let base = {
            let mut request = NeutralRequest::default();
            request
                .messages
                .push(Message::text(Role::User, "write a shell script"));
            request
        };

        let mut labelled = base.clone();
        labelled
            .extensions
            .fields
            .insert("user".to_owned(), serde_json::json!("alice"));
        labelled
            .extensions
            .fields
            .insert("request_id".to_owned(), serde_json::json!("req_1"));
        labelled
            .extensions
            .fields
            .insert("store".to_owned(), serde_json::json!(true));

        assert_eq!(
            canonical_bytes(&base).unwrap(),
            canonical_bytes(&labelled).unwrap()
        );
    }

    #[test]
    fn canonical_bytes_sort_tools_by_name() {
        let mut first = NeutralRequest::default();
        first.tools.push(tool("write_file"));
        first.tools.push(tool("read_file"));

        let mut second = NeutralRequest::default();
        second.tools.push(tool("read_file"));
        second.tools.push(tool("write_file"));

        assert_eq!(
            canonical_bytes(&first).unwrap(),
            canonical_bytes(&second).unwrap()
        );
    }

    #[test]
    fn canonical_bytes_separate_different_conversations() {
        let mut first = NeutralRequest::default();
        first
            .messages
            .push(Message::text(Role::User, "fix this bug"));

        let mut second = NeutralRequest::default();
        second
            .messages
            .push(Message::text(Role::User, "fix that bug"));

        assert_ne!(
            canonical_bytes(&first).unwrap(),
            canonical_bytes(&second).unwrap()
        );
    }

    #[test]
    fn canonical_bytes_keep_the_system_prompt_and_sampling() {
        let base = {
            let mut request = NeutralRequest::default();
            request
                .messages
                .push(Message::text(Role::User, "fix this bug"));
            request
        };

        let mut with_system = base.clone();
        with_system.instructions.push(InstructionBlock {
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "You are helpful".into(),
            }],
        });
        assert_ne!(
            canonical_bytes(&base).unwrap(),
            canonical_bytes(&with_system).unwrap()
        );

        let mut hotter = base.clone();
        hotter.sampling.temperature = Some(0.9);
        assert_ne!(
            canonical_bytes(&base).unwrap(),
            canonical_bytes(&hotter).unwrap()
        );
    }

    #[test]
    fn a_decoded_request_round_trips_through_both_formats() {
        let anthropic = serde_json::json!({
            "model": "claude-sonnet-4",
            "max_tokens": 1024,
            "system": [{"type": "text", "text": "You are helpful"}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": "fix this bug"}]}],
        });
        let request = decode_request(Format::AnthropicMessages, &anthropic).unwrap();
        assert_eq!(named_model(&request), Some("claude-sonnet-4".to_owned()));
        assert_eq!(requested_output_tokens(&request), 1024);
        assert!(!has_tools(&request));

        let openai = encode_request(&request, Format::OpenAiChat).unwrap();
        assert!(openai.get("messages").is_some());
    }

    #[test]
    fn a_bad_body_is_an_invalid_request() {
        let error = decode_request(Format::OpenAiChat, &serde_json::json!("not an object"));
        assert!(matches!(error, Err(WireError::InvalidRequest(_))));
    }
}
