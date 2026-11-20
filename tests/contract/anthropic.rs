//! The Anthropic `POST /v1/messages` wire format.
//!
//! These pin the shapes the router must accept from an Anthropic client and
//! hand back to it: a system prompt as an array of blocks, content as an array
//! of blocks, `tool_use` blocks with their ids, `tool_result` blocks arriving
//! on a `user` message, `stop_reason` in both directions, and a `ping` event
//! inside a stream.
//!
//! The hardest case in the whole translation is the last one here: a tool-call
//! id minted by Anthropic has to come out of an OpenAI-shaped channel
//! unchanged, otherwise the client cannot pair the result with the call.
//!
//! No sockets, no upstreams, no storage. Every key in a fixture is an obvious
//! fake.

use futures::stream;
use llmrouter::wire::{self, Format};
use rstest::rstest;
use serde_json::{json, Value};

/// Frames a list of event objects as server-sent events, the way an Anthropic
/// upstream writes them.
fn sse(events: &[Value]) -> Vec<u8> {
    events
        .iter()
        .map(|event| {
            let name = event
                .get("type")
                .and_then(Value::as_str)
                .expect("every fixture event names its type");
            format!("event: {name}\ndata: {event}\n\n")
        })
        .collect::<String>()
        .into_bytes()
}

/// Decodes a whole Anthropic SSE body and folds it into one answer.
async fn fold_anthropic_stream(events: &[Value]) -> wire::NeutralResponse {
    let bytes = sse(events);
    let chunks = wire::decode_stream(
        stream::once(async move { Ok(bytes) }),
        Format::AnthropicMessages,
    )
    .expect("the stream decoder starts");
    wire::aggregate_stream(chunks)
        .await
        .expect("the stream folds cleanly")
}

/// The text of every `text` block across every output of an answer.
fn answer_text(response: &wire::NeutralResponse) -> String {
    serde_json::to_value(response)
        .expect("a neutral response serialises")
        .get("outputs")
        .and_then(Value::as_array)
        .map(|outputs| {
            outputs
                .iter()
                .filter_map(|output| output.get("content")?.as_array())
                .flatten()
                .filter(|block| block.get("type") == Some(&json!("text")))
                .filter_map(|block| block.get("text")?.as_str())
                .collect::<String>()
        })
        .unwrap_or_default()
}

/// Every content block of the first output of an answer.
fn answer_blocks(response: &wire::NeutralResponse) -> Vec<Value> {
    serde_json::to_value(response)
        .expect("a neutral response serialises")
        .pointer("/outputs/0/content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// An Anthropic request whose system prompt is an array of two text blocks.
fn request_with_blocked_system() -> Value {
    json!({
        "model": "claude-sonnet-4",
        "max_tokens": 256,
        "system": [
            {"type": "text", "text": "You are a careful assistant."},
            {"type": "text", "text": "Answer in British English."},
        ],
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "ping"}]},
        ],
    })
}

#[test]
fn a_system_prompt_given_as_blocks_becomes_system_instructions() {
    let neutral = wire::decode_request(Format::AnthropicMessages, &request_with_blocked_system())
        .expect("an array system prompt decodes");

    let value = serde_json::to_value(&neutral).expect("a neutral request serialises");
    let instructions = value
        .get("instructions")
        .and_then(Value::as_array)
        .expect("the system prompt lands in instructions");

    let texts: Vec<&str> = instructions
        .iter()
        .filter_map(|block| block.get("content")?.as_array())
        .flatten()
        .filter_map(|block| block.get("text")?.as_str())
        .collect();
    assert_eq!(
        texts,
        vec!["You are a careful assistant.", "Answer in British English."],
        "both system blocks survive, in order"
    );
    assert!(
        instructions
            .iter()
            .all(|block| block.get("role") == Some(&json!("system"))),
        "an Anthropic system prompt carries system authority, not user authority"
    );

    // The prompt must still be a system prompt on the way back out, not folded
    // into the first user message.
    let encoded = wire::encode_request(&neutral, Format::AnthropicMessages)
        .expect("re-encodes for Anthropic");
    assert!(
        encoded.get("system").is_some(),
        "the system prompt stays in `system`, got {encoded}"
    );
    assert_eq!(
        encoded.pointer("/messages/0/role"),
        Some(&json!("user")),
        "the only message is still the user's"
    );
    insta::assert_json_snapshot!("anthropic_blocked_system_request", encoded);
}

#[test]
fn a_system_prompt_as_a_string_and_as_one_block_decode_the_same() {
    let as_string = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 256,
        "system": "You are a careful assistant.",
        "messages": [{"role": "user", "content": "ping"}],
    });
    let as_block = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 256,
        "system": [{"type": "text", "text": "You are a careful assistant."}],
        "messages": [{"role": "user", "content": "ping"}],
    });

    let from_string = wire::decode_request(Format::AnthropicMessages, &as_string)
        .expect("a string system prompt decodes");
    let from_block = wire::decode_request(Format::AnthropicMessages, &as_block)
        .expect("a blocked system prompt decodes");

    assert_eq!(
        serde_json::to_value(&from_string).unwrap(),
        serde_json::to_value(&from_block).unwrap(),
        "the two spellings of one system prompt must not produce different requests, \
         or they would miss each other in the cache"
    );
}

#[test]
fn content_given_as_an_array_of_blocks_keeps_every_block_and_its_order() {
    let body = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 256,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is in this picture?"},
                {"type": "image", "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": "aW1hZ2UtYnl0ZXM=",
                }},
                {"type": "text", "text": "be brief"},
            ],
        }],
    });

    let neutral =
        wire::decode_request(Format::AnthropicMessages, &body).expect("blocked content decodes");
    let value = serde_json::to_value(&neutral).expect("a neutral request serialises");
    let blocks = value
        .pointer("/messages/0/content")
        .and_then(Value::as_array)
        .expect("the user message keeps its blocks");

    let kinds: Vec<&str> = blocks
        .iter()
        .filter_map(|block| block.get("type")?.as_str())
        .collect();
    assert_eq!(
        kinds,
        vec!["text", "image", "text"],
        "three blocks in, three blocks out, in the order the client sent them"
    );
    assert_eq!(
        blocks[2].get("text"),
        Some(&json!("be brief")),
        "the trailing instruction is not swallowed by the image"
    );

    let encoded = wire::encode_request(&neutral, Format::AnthropicMessages)
        .expect("re-encodes for Anthropic");
    assert_eq!(
        encoded
            .pointer("/messages/0/content")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3),
        "all three blocks come back out, got {encoded}"
    );
}

#[test]
fn a_tool_use_block_keeps_its_id_from_anthropic_to_openai_and_back() {
    // DESIGN.md calls this the hardest part of translation. An Anthropic client
    // mints `toolu_...`; an OpenAI-shaped channel must hand the same string
    // back, or the client cannot pair the result with the call it answers.
    const ID: &str = "toolu_fake_01abcdef";

    let anthropic_reply = json!({
        "id": "msg_fake_01",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4",
        "content": [
            {"type": "text", "text": "Looking that up."},
            {"type": "tool_use", "id": ID, "name": "get_weather",
             "input": {"city": "London"}},
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 11, "output_tokens": 7},
    });

    let neutral = wire::decode_response(&anthropic_reply, Format::AnthropicMessages)
        .expect("an Anthropic reply decodes");
    let tool_call = answer_blocks(&neutral)
        .into_iter()
        .find(|block| block.get("type") == Some(&json!("tool_call")))
        .expect("the tool_use block becomes a neutral tool call");
    assert_eq!(
        tool_call.get("id"),
        Some(&json!(ID)),
        "the id survives decode"
    );
    assert_eq!(tool_call.get("name"), Some(&json!("get_weather")));

    // Anthropic to OpenAI.
    let openai = wire::encode_response(&neutral, Format::OpenAiChat, None)
        .expect("encodes into the OpenAI shape");
    assert_eq!(
        openai.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!(ID)),
        "an OpenAI-shaped channel must carry the Anthropic id verbatim, got {openai}"
    );
    assert_eq!(
        openai.pointer("/choices/0/finish_reason"),
        Some(&json!("tool_calls"))
    );

    // ...and back again, as if that channel had been the one to answer.
    let round_tripped =
        wire::decode_response(&openai, Format::OpenAiChat).expect("the OpenAI shape decodes");
    let anthropic_again = wire::encode_response(&round_tripped, Format::AnthropicMessages, None)
        .expect("encodes back into the Anthropic shape");

    let returned = anthropic_again
        .get("content")
        .and_then(Value::as_array)
        .expect("the reply has content")
        .iter()
        .find(|block| block.get("type") == Some(&json!("tool_use")))
        .expect("the tool call is a tool_use block again")
        .clone();
    assert_eq!(
        returned.get("id"),
        Some(&json!(ID)),
        "the id is unchanged after Anthropic to OpenAI and back, got {anthropic_again}"
    );
    assert_eq!(returned.get("name"), Some(&json!("get_weather")));
    assert_eq!(returned.get("input"), Some(&json!({"city": "London"})));
    assert_eq!(
        anthropic_again.get("stop_reason"),
        Some(&json!("tool_use")),
        "the stop reason survives the same two hops"
    );
}

#[test]
fn a_tool_result_arrives_on_a_user_message_and_keeps_the_id_it_answers() {
    const ID: &str = "toolu_fake_02abcdef";

    let body = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 256,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "weather in London?"}]},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": ID, "name": "get_weather",
                 "input": {"city": "London"}},
            ]},
            // Anthropic sends tool output back as a *user* turn, not a
            // dedicated tool role. This is the shape that trips translation up.
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": ID,
                 "content": [{"type": "text", "text": "12C and raining"}]},
            ]},
        ],
    });

    let neutral =
        wire::decode_request(Format::AnthropicMessages, &body).expect("a tool result decodes");
    let value = serde_json::to_value(&neutral).expect("a neutral request serialises");
    let result_block = value
        .pointer("/messages/2/content/0")
        .expect("the third message keeps its block");
    assert_eq!(result_block.get("type"), Some(&json!("tool_result")));
    assert_eq!(
        result_block.get("tool_call_id"),
        Some(&json!(ID)),
        "the result is pinned to the call it answers"
    );

    // Out to an OpenAI-shaped channel: the same output becomes a `tool` role
    // message carrying `tool_call_id`.
    let openai = wire::encode_request(&neutral, Format::OpenAiChat).expect("encodes for OpenAI");
    let tool_message = openai
        .get("messages")
        .and_then(Value::as_array)
        .expect("the request has messages")
        .iter()
        .find(|message| message.get("role") == Some(&json!("tool")))
        .expect("the tool output becomes a tool-role message, got {openai}")
        .clone();
    assert_eq!(
        tool_message.get("tool_call_id"),
        Some(&json!(ID)),
        "the pairing id crosses to OpenAI untouched"
    );

    // ...and back to Anthropic: a user turn again, never a tool role.
    let back_to_neutral =
        wire::decode_request(Format::OpenAiChat, &openai).expect("the OpenAI request decodes");
    let anthropic_again = wire::encode_request(&back_to_neutral, Format::AnthropicMessages)
        .expect("encodes back for Anthropic");
    let messages = anthropic_again
        .get("messages")
        .and_then(Value::as_array)
        .expect("the request has messages");
    let carrier = messages
        .iter()
        .find(|message| {
            message
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| {
                    blocks
                        .iter()
                        .any(|block| block.get("type") == Some(&json!("tool_result")))
                })
        })
        .expect("a tool_result block comes back");
    assert_eq!(
        carrier.get("role"),
        Some(&json!("user")),
        "Anthropic carries tool output on a user turn, got {anthropic_again}"
    );
    assert_eq!(
        carrier.pointer("/content/0/tool_use_id"),
        Some(&json!(ID)),
        "the id is still the one the call was minted with"
    );
    insta::assert_json_snapshot!("anthropic_tool_result_request", anthropic_again);
}

/// Anthropic's vocabulary in, the neutral reason in the middle, Anthropic's
/// vocabulary out. The last row is the lossy one: a reason the crate does not
/// know becomes `end_turn` rather than being replayed, so a client never sees a
/// word it cannot handle.
#[rstest]
#[case::end_turn("end_turn", "end_turn", "end_turn")]
#[case::max_tokens("max_tokens", "max_tokens", "max_tokens")]
#[case::tool_use("tool_use", "tool_use", "tool_use")]
#[case::refusal("refusal", "content_filter", "refusal")]
#[case::unknown_to_us("pause_turn", "unknown", "end_turn")]
fn an_anthropic_stop_reason_maps_both_ways(
    #[case] from_provider: &str,
    #[case] neutral_name: &str,
    #[case] back_to_client: &str,
) {
    let reply = json!({
        "id": "msg_fake_03",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4",
        "content": [{"type": "text", "text": "done"}],
        "stop_reason": from_provider,
        "usage": {"input_tokens": 4, "output_tokens": 2},
    });

    let neutral =
        wire::decode_response(&reply, Format::AnthropicMessages).expect("the reply decodes");
    let value = serde_json::to_value(&neutral).expect("a neutral response serialises");
    assert_eq!(
        value.pointer("/outputs/0/stop_reason"),
        Some(&json!(neutral_name)),
        "{from_provider} must normalise to {neutral_name}"
    );

    let encoded = wire::encode_response(&neutral, Format::AnthropicMessages, None)
        .expect("re-encodes for Anthropic");
    assert_eq!(
        encoded.get("stop_reason"),
        Some(&json!(back_to_client)),
        "{neutral_name} must go back out as {back_to_client}, got {encoded}"
    );
}

#[test]
fn a_missing_stop_reason_is_read_as_end_turn() {
    let reply = json!({
        "id": "msg_fake_04",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4",
        "content": [{"type": "text", "text": "done"}],
        "usage": {"input_tokens": 4, "output_tokens": 2},
    });

    let neutral = wire::decode_response(&reply, Format::AnthropicMessages)
        .expect("a reply with no stop reason decodes");
    assert_eq!(
        serde_json::to_value(&neutral)
            .unwrap()
            .pointer("/outputs/0/stop_reason"),
        Some(&json!("end_turn"))
    );
}

/// The events a well-behaved Anthropic stream sends for a two-word answer.
fn text_stream_events() -> Vec<Value> {
    vec![
        json!({"type": "message_start", "message": {
            "id": "msg_fake_05", "type": "message", "role": "assistant",
            "model": "claude-sonnet-4", "content": [],
            "usage": {"input_tokens": 5, "output_tokens": 0},
        }}),
        json!({"type": "content_block_start", "index": 0,
               "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "text_delta", "text": "all "}}),
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "text_delta", "text": "good"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
               "usage": {"output_tokens": 2}}),
        json!({"type": "message_stop"}),
    ]
}

#[tokio::test]
async fn a_ping_event_in_a_stream_changes_nothing() {
    // Anthropic sends `ping` to hold an idle connection open. It carries no
    // content, so the fold must be byte-for-byte the same with and without it,
    // and the stream must not fail on an event type we do not model.
    let without_ping = text_stream_events();

    let mut with_ping = Vec::new();
    with_ping.push(without_ping[0].clone());
    with_ping.push(json!({"type": "ping"}));
    with_ping.extend(without_ping[1..].iter().cloned());
    with_ping.insert(4, json!({"type": "ping"}));

    let plain = fold_anthropic_stream(&without_ping).await;
    let pinged = fold_anthropic_stream(&with_ping).await;

    assert_eq!(answer_text(&plain), "all good", "the control stream folds");
    assert_eq!(
        serde_json::to_value(&pinged).unwrap(),
        serde_json::to_value(&plain).unwrap(),
        "a ping adds nothing and removes nothing"
    );
}

#[tokio::test]
async fn a_ping_before_anything_else_does_not_break_the_stream() {
    // A keepalive can arrive before `message_start`, which is the case most
    // likely to upset a decoder that assumes the first event opens the message.
    let mut events = vec![json!({"type": "ping"})];
    events.extend(text_stream_events());

    let folded = fold_anthropic_stream(&events).await;
    assert_eq!(answer_text(&folded), "all good");
    assert_eq!(
        serde_json::to_value(&folded)
            .unwrap()
            .pointer("/outputs/0/stop_reason"),
        Some(&json!("end_turn")),
        "the stop reason still arrives on message_delta"
    );
}

#[tokio::test]
async fn a_streamed_tool_use_keeps_its_id_through_to_openai() {
    const ID: &str = "toolu_fake_06abcdef";

    let events = vec![
        json!({"type": "message_start", "message": {
            "id": "msg_fake_06", "type": "message", "role": "assistant",
            "model": "claude-sonnet-4", "content": [],
            "usage": {"input_tokens": 9, "output_tokens": 0},
        }}),
        json!({"type": "ping"}),
        json!({"type": "content_block_start", "index": 0, "content_block": {
            "type": "tool_use", "id": ID, "name": "get_weather", "input": {},
        }}),
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "input_json_delta", "partial_json": "{\"city\":"}}),
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "input_json_delta", "partial_json": "\"London\"}"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
               "usage": {"output_tokens": 12}}),
        json!({"type": "message_stop"}),
    ];

    let folded = fold_anthropic_stream(&events).await;
    let tool_call = answer_blocks(&folded)
        .into_iter()
        .find(|block| block.get("type") == Some(&json!("tool_call")))
        .expect("the streamed tool_use folds into a neutral tool call");
    assert_eq!(tool_call.get("id"), Some(&json!(ID)));
    assert_eq!(
        tool_call.get("arguments"),
        Some(&json!({"city": "London"})),
        "the arguments rejoin across the partial_json deltas"
    );

    let openai = wire::encode_response(&folded, Format::OpenAiChat, None)
        .expect("the folded answer encodes for OpenAI");
    assert_eq!(
        openai.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!(ID)),
        "a streamed Anthropic id reaches an OpenAI client unchanged, got {openai}"
    );
}

#[tokio::test]
async fn an_error_event_mid_stream_fails_the_fold_rather_than_truncating_it() {
    let mut events = text_stream_events()[..4].to_vec();
    events.push(json!({"type": "error", "error": {
        "type": "overloaded_error", "message": "upstream is overloaded",
    }}));
    // Append the real tail (content_block_stop, message_delta, message_stop) so the
    // stream is properly terminated. Without it the fold would fail on truncation
    // alone and the test would pass whether or not the error event is honoured.
    events.extend(text_stream_events()[4..].iter().cloned());

    let bytes = sse(&events);
    let chunks = wire::decode_stream(
        stream::once(async move { Ok(bytes) }),
        Format::AnthropicMessages,
    )
    .expect("the stream decoder starts");

    let folded = wire::aggregate_stream(chunks).await;
    let reason = folded
        .expect_err("a stream that errors part way must not be handed on as a complete answer")
        .to_string();
    assert!(
        !reason.contains("ended before a terminal event"),
        "the fold must fail on the error event, not on truncation, got {reason}"
    );
}

#[tokio::test]
async fn a_stream_re_encoded_for_an_anthropic_client_opens_and_closes_the_message() {
    let bytes = sse(&text_stream_events());
    let chunks = wire::decode_stream(
        stream::once(async move { Ok(bytes) }),
        Format::AnthropicMessages,
    )
    .expect("the stream decoder starts");
    let mut events = wire::encode_stream(
        chunks,
        Format::AnthropicMessages,
        Some("claude-sonnet-4".to_owned()),
    )
    .expect("the stream re-encoder starts");

    let mut kinds = Vec::new();
    let mut text = String::new();
    while let Some(event) = events.next().await {
        let event = event.expect("every event encodes");
        if let Some(kind) = event.get("type").and_then(Value::as_str) {
            kinds.push(kind.to_owned());
        }
        if let Some(delta) = event.pointer("/delta/text").and_then(Value::as_str) {
            text.push_str(delta);
        }
    }

    assert_eq!(text, "all good", "the answer survives the re-encode");
    assert_eq!(
        kinds.first().map(String::as_str),
        Some("message_start"),
        "an Anthropic client expects message_start first, got {kinds:?}"
    );
    assert_eq!(
        kinds.last().map(String::as_str),
        Some("message_stop"),
        "and message_stop last, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|kind| kind == "content_block_start"),
        "the text block is opened, got {kinds:?}"
    );
}

#[tokio::test]
async fn a_stream_of_a_tool_call_answer_re_encodes_for_an_anthropic_client() {
    let openai_reply = json!({
        "id": "chatcmpl-tool1",
        "object": "chat.completion",
        "created": 1_700_000_000u64,
        "model": "gpt-4o-mini",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_9kQx",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": "{\"path\":\"/etc/hosts\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": { "prompt_tokens": 42, "completion_tokens": 17, "total_tokens": 59 }
    });

    let neutral = wire::decode_response(&openai_reply, Format::OpenAiChat).expect("decodes");
    let chunks = wire::stream_of(neutral);
    let mut events = wire::encode_stream(
        chunks,
        Format::AnthropicMessages,
        Some("claude-sonnet-4".to_owned()),
    )
    .expect("the stream re-encoder starts");

    let mut event_list = Vec::new();
    while let Some(item) = events.next().await {
        event_list.push(item.expect("event"));
    }
    eprintln!("TOOL EVENTS: {:#?}", event_list);
}

#[tokio::test]
async fn a_reasoning_model_reply_re_encodes_for_an_anthropic_client() {
    let openai_reply = json!({
        "id": "chatcmpl-reason1",
        "object": "chat.completion",
        "created": 1_700_000_000u64,
        "model": "nemotron",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "Let me think about this carefully..."
            },
            "finish_reason": "stop"
        }],
        "usage": { "prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30 }
    });

    let neutral = wire::decode_response(&openai_reply, Format::OpenAiChat).expect("decodes");
    let encoded = wire::encode_response(&neutral, Format::AnthropicMessages, None).expect("encodes");
    eprintln!("ENCODED REASONING BUFFERED: {:#?}", encoded);

    let chunks = wire::stream_of(neutral);
    let mut events = wire::encode_stream(
        chunks,
        Format::AnthropicMessages,
        Some("claude-sonnet-4".to_owned()),
    )
    .expect("the stream re-encoder starts");

    let mut event_list = Vec::new();
    while let Some(item) = events.next().await {
        event_list.push(item.expect("event"));
    }
    eprintln!("ENCODED REASONING STREAMED: {:#?}", event_list);
}

#[test]
fn an_anthropic_request_with_thinking_and_tool_use_translates_to_openai() {
    let anthropic_req = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "find the bug"}]
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "I should run git status",
                        "signature": ""
                    },
                    {
                        "type": "text",
                        "text": "Running git status now"
                    },
                    {
                        "type": "tool_use",
                        "id": "call_123",
                        "name": "Bash",
                        "input": {"command": "git status"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call_123",
                        "content": "On branch master\nnothing to commit"
                    }
                ]
            }
        ]
    });

    let neutral = wire::decode_request(Format::AnthropicMessages, &anthropic_req)
        .expect("decodes Anthropic request");
    let openai_body = wire::encode_request(&neutral, Format::OpenAiChat)
        .expect("encodes for OpenAI");
    eprintln!("OPENAI BODY: {:#?}", openai_body);
}

#[test]
fn an_anthropic_request_with_multiple_tool_uses_translates_to_openai() {
    let anthropic_req = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "read two files"}]
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "Read",
                        "input": {"path": "a.txt"}
                    },
                    {
                        "type": "tool_use",
                        "id": "call_2",
                        "name": "Read",
                        "input": {"path": "b.txt"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call_1",
                        "content": "content of a"
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "call_2",
                        "content": "content of b"
                    }
                ]
            }
        ]
    });

    let neutral = wire::decode_request(Format::AnthropicMessages, &anthropic_req)
        .expect("decodes Anthropic request");
    let openai_body = wire::encode_request(&neutral, Format::OpenAiChat)
        .expect("encodes for OpenAI");
    eprintln!("OPENAI MULTI TOOL BODY: {:#?}", openai_body);
}

#[tokio::test]
async fn a_stream_of_multiple_tool_calls_re_encodes_for_an_anthropic_client() {
    let openai_reply = json!({
        "id": "chatcmpl-tool2",
        "object": "chat.completion",
        "created": 1_700_000_000u64,
        "model": "gpt-4o-mini",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"a.txt\"}"
                        }
                    },
                    {
                        "id": "call_2",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"b.txt\"}"
                        }
                    }
                ]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": { "prompt_tokens": 42, "completion_tokens": 30, "total_tokens": 72 }
    });

    let neutral = wire::decode_response(&openai_reply, Format::OpenAiChat).expect("decodes");
    let chunks = wire::stream_of(neutral);
    let mut events = wire::encode_stream(
        chunks,
        Format::AnthropicMessages,
        Some("claude-sonnet-4".to_owned()),
    )
    .expect("the stream re-encoder starts");

    let mut event_list = Vec::new();
    while let Some(item) = events.next().await {
        event_list.push(item.expect("event"));
    }
    eprintln!("MULTI TOOL STREAMED EVENTS: {:#?}", event_list);
}





