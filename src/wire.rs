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
use switchyard_protocol::LlmClientError;

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

/// A stream of wire-format event objects, ready for the server to frame as
/// server-sent events.
pub struct EventStream(switchyard_translation::RawEventStream);

impl EventStream {
    /// Pulls the next event, or `None` at the end of the stream.
    pub async fn next(&mut self) -> Option<std::result::Result<Value, WireError>> {
        self.0
            .next()
            .await
            .map(|item| item.map_err(|error| WireError::Stream(error.to_string())))
    }
}

/// Edge in: the body the client sent, in whichever format its endpoint uses.
pub fn decode_request(
    format: Format,
    body: &Value,
) -> std::result::Result<NeutralRequest, WireError> {
    switchyard_translation::decode_request(format.into(), body)
        .map_err(|error| WireError::InvalidRequest(error.to_string()))
}

/// Edge out: the body one channel receives, in that provider's format.
pub fn encode_request(
    request: &NeutralRequest,
    format: Format,
) -> std::result::Result<Value, WireError> {
    switchyard_translation::encode_request(request, format.into())
        .map_err(|error| WireError::Translation(error.to_string()))
}

/// A winning channel's buffered reply, in that provider's format.
pub fn decode_response(
    body: &Value,
    format: Format,
) -> std::result::Result<NeutralResponse, WireError> {
    switchyard_translation::decode_aggregated_response(body, format.into())
        .map_err(|error| WireError::Translation(error.to_string()))
}

/// Edge out: the reply the client receives, in the format its endpoint uses.
/// `served_model` overrides the model name on the response.
pub fn encode_response(
    response: &NeutralResponse,
    format: Format,
    served_model: Option<&str>,
) -> std::result::Result<Value, WireError> {
    switchyard_translation::encode_aggregated_response(response, format.into(), served_model)
        .map_err(|error| WireError::Translation(error.to_string()))
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
        .map(EventStream)
        .map_err(|error| WireError::Stream(error.to_string()))
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
const EXCLUDED_EXTENSION_FIELDS: [&str; 9] = [
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

    let mut out = Vec::new();
    write_canonical(&value, &mut out).map_err(|error| WireError::Translation(error.to_string()))?;
    Ok(out)
}

/// Writes a value with every object key sorted and no whitespace. Scalars go
/// through `serde_json`, which gives one fixed number format.
fn write_canonical(value: &Value, out: &mut Vec<u8>) -> std::result::Result<(), serde_json::Error> {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key)?;
                out.push(b':');
                write_canonical(&map[key], out)?;
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out)?;
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar)?,
    }
    Ok(())
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
        let value = serde_json::json!({"b": 1, "a": {"d": [3, 2], "c": null}});
        let mut out = Vec::new();
        write_canonical(&value, &mut out).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"a":{"c":null,"d":[3,2]},"b":1}"#
        );
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
