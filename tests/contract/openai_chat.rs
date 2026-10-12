//! The OpenAI chat-completions wire format, both directions.
//!
//! Outbound, a body the router builds must be one a real provider accepts:
//! the model, the messages, tools in the `function` wrapper, the sampling
//! knobs, the token cap, the stop sequences and the streaming flag.
//!
//! Inbound, a reply the router parses must cover the five shapes a provider
//! actually sends back: a plain text answer, a tool-call answer whose
//! `function.arguments` is a JSON string, an empty `choices` array, an error
//! object wearing a `200`, and a `200` with no content at all.
//!
//! Snapshots are held by `insta`, so a change to the bytes on either edge
//! shows up as a diff in review rather than silently going out.

use llmrouter::health::is_healthy_response;
use llmrouter::wire::{self, Format};
use rstest::rstest;
use serde_json::{json, Value};

/// A request with every knob a caller can turn, as an OpenAI client sends it.
fn full_request() -> Value {
    json!({
        "model": "gpt-4o-mini",
        "messages": [
            { "role": "system", "content": "Be terse." },
            { "role": "user", "content": "List two colours." }
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a file from disk.",
                "parameters": {
                    "type": "object",
                    "properties": { "path": { "type": "string" } },
                    "required": ["path"]
                }
            }
        }],
        "tool_choice": "auto",
        "temperature": 0.2,
        "max_tokens": 256,
        "stop": ["\n\n", "END"],
        "stream": true,
    })
}

/// Builds the body one OpenAI-wire channel receives.
fn outbound(body: &Value) -> Value {
    let neutral = wire::decode_request(Format::OpenAiChat, body).expect("the client body decodes");
    wire::encode_request(&neutral, Format::OpenAiChat).expect("the outbound body encodes")
}

/// Parses a provider reply and re-encodes it for the client, exactly as the
/// buffered path does, renaming the model to the one we advertise.
fn inbound(reply: &Value) -> Value {
    let neutral = wire::decode_response(reply, Format::OpenAiChat).expect("the reply decodes");
    wire::encode_response(&neutral, Format::OpenAiChat, Some("router/auto"))
        .expect("the client body encodes")
}

/// Everything the caller asked for has to reach the provider. A field that
/// quietly goes missing here changes the answer and nobody finds out.
#[test]
fn an_outbound_request_keeps_every_field_the_caller_set() {
    let encoded = outbound(&full_request());

    assert_eq!(encoded["model"], "gpt-4o-mini");
    assert_eq!(encoded["messages"][0]["role"], "system");
    assert_eq!(encoded["messages"][1]["content"], "List two colours.");
    assert_eq!(encoded["temperature"], 0.2);
    // OpenAI renamed `max_tokens` to `max_completion_tokens` and rejects the
    // old name on its newer models, so the new name is the one we must send.
    assert_eq!(encoded["max_completion_tokens"], 256);
    assert_eq!(encoded["stop"], json!(["\n\n", "END"]));
    assert_eq!(encoded["stream"], true);
    assert_eq!(encoded["tool_choice"], "auto");

    insta::assert_json_snapshot!("request_full", encoded);
}

/// A provider rejects a tool that is not wrapped in `{"type":"function",
/// "function":{...}}`, and Claude Code sends tools on every request, so this
/// is the shape that breaks first if the translation drifts.
#[test]
fn outbound_tools_sit_inside_the_function_wrapper() {
    let encoded = outbound(&full_request());
    let tool = &encoded["tools"][0];

    assert_eq!(tool["type"], "function");
    assert_eq!(tool["function"]["name"], "read_file");
    assert_eq!(tool["function"]["description"], "Read a file from disk.");
    assert_eq!(tool["function"]["parameters"]["type"], "object");
    assert_eq!(
        tool["function"]["parameters"]["required"],
        json!(["path"]),
        "a tool schema must cross the wire whole, not just its name"
    );
    assert!(
        tool.get("name").is_none(),
        "a bare tool, unwrapped, is a 400 from the provider"
    );
}

/// A tool defined by an Anthropic client has to come out the other side in the
/// OpenAI wrapper, because the router sends an Anthropic caller's request to
/// an OpenAI provider whenever that channel wins.
#[test]
fn an_anthropic_tool_becomes_an_openai_function() {
    let body = json!({
        "model": "claude-sonnet-4",
        "max_tokens": 64,
        "messages": [{ "role": "user", "content": "read it" }],
        "tools": [{
            "name": "read_file",
            "description": "Read a file from disk.",
            "input_schema": {
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"]
            }
        }],
    });
    let neutral = wire::decode_request(Format::AnthropicMessages, &body).expect("decodes");
    let encoded = wire::encode_request(&neutral, Format::OpenAiChat).expect("encodes");

    assert_eq!(encoded["tools"][0]["type"], "function");
    assert_eq!(encoded["tools"][0]["function"]["name"], "read_file");
    assert_eq!(
        encoded["tools"][0]["function"]["parameters"]["required"],
        json!(["path"]),
        "Anthropic calls the schema `input_schema`; OpenAI calls it `parameters`"
    );

    insta::assert_json_snapshot!("request_anthropic_tool_to_openai", encoded);
}

/// Stop sequences decide where the answer ends. Dropping them does not fail
/// loudly: the model simply keeps going past the point the caller asked for.
///
/// This one caught a real bug. The translation crate's OpenAI decoder parks
/// `stop` under that name, while its OpenAI encoder only looked for
/// `stop_sequences`, so OpenAI in to OpenAI out lost them. See `restore_stop`
/// in src/wire.rs.
#[rstest]
#[case::openai_client(Format::OpenAiChat, json!({
    "model": "gpt-4o-mini",
    "messages": [{ "role": "user", "content": "count" }],
    "stop": ["END"],
}))]
#[case::anthropic_client(Format::AnthropicMessages, json!({
    "model": "claude-sonnet-4",
    "max_tokens": 64,
    "messages": [{ "role": "user", "content": "count" }],
    "stop_sequences": ["END"],
}))]
fn stop_sequences_reach_an_openai_provider(#[case] from: Format, #[case] body: Value) {
    let neutral = wire::decode_request(from, &body).expect("the client body decodes");
    let encoded = wire::encode_request(&neutral, Format::OpenAiChat).expect("encodes");

    assert_eq!(
        encoded["stop"],
        json!(["END"]),
        "the caller's stop sequences must reach the provider"
    );
}

/// The ordinary answer. Note the model name: the client asked the router for a
/// model, so the client is told the router's name, not the upstream's.
#[test]
fn a_plain_text_answer_is_handed_back_whole() {
    let encoded = inbound(&json!({
        "id": "chatcmpl-abc123",
        "object": "chat.completion",
        "created": 1_700_000_000u64,
        "model": "gpt-4o-mini-2024-07-18",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": "Red and blue." },
            "finish_reason": "stop"
        }],
        "usage": { "prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15 }
    }));

    assert_eq!(encoded["choices"][0]["message"]["content"], "Red and blue.");
    assert_eq!(encoded["choices"][0]["finish_reason"], "stop");
    assert_eq!(encoded["usage"]["total_tokens"], 15);
    assert_eq!(
        encoded["model"], "router/auto",
        "the client is told the model it asked the router for"
    );

    insta::assert_json_snapshot!("response_text", encoded);
}

/// `function.arguments` is a JSON *string*, not an object. A round trip that
/// turns it into an object looks right in a diff and fails in every client
/// that parses it, so pin both the type and the contents.
#[test]
fn a_tool_call_answer_keeps_its_arguments_as_a_json_string() {
    let encoded = inbound(&json!({
        "id": "chatcmpl-tool1",
        "object": "chat.completion",
        "created": 1_700_000_000u64,
        "model": "gpt-4o-mini-2024-07-18",
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
                        "arguments": "{\"path\":\"/etc/hosts\",\"limit\":10}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": { "prompt_tokens": 42, "completion_tokens": 17, "total_tokens": 59 }
    }));

    let call = &encoded["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(
        call["id"], "call_9kQx",
        "the id ties the result the client sends next to this call"
    );
    assert_eq!(call["type"], "function");
    assert_eq!(call["function"]["name"], "read_file");

    let arguments = call["function"]["arguments"]
        .as_str()
        .expect("arguments is a JSON string, never an object");
    let parsed: Value = serde_json::from_str(arguments).expect("that string is itself valid JSON");
    assert_eq!(parsed, json!({ "path": "/etc/hosts", "limit": 10 }));
    assert_eq!(encoded["choices"][0]["finish_reason"], "tool_calls");

    insta::assert_json_snapshot!("response_tool_call", encoded);
}

/// The three bodies a provider sends when it has failed but still answered
/// `200`, next to the two that are genuine answers.
///
/// All five parse without complaint, which is the point: the parser is not the
/// guard. `is_healthy_response` is, and it is the only thing standing between
/// a rate limit dressed as a success and a channel winning the race with it.
#[rstest]
#[case::text(true, json!({
    "id": "c", "object": "chat.completion", "created": 1, "model": "m",
    "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Red." }, "finish_reason": "stop" }]
}))]
#[case::tool_call(true, json!({
    "id": "c", "object": "chat.completion", "created": 1, "model": "m",
    "choices": [{ "index": 0, "message": { "role": "assistant", "content": null, "tool_calls": [
        { "id": "call_1", "type": "function", "function": { "name": "read_file", "arguments": "{}" } }
    ]}, "finish_reason": "tool_calls" }]
}))]
#[case::empty_choices(false, json!({
    "id": "c", "object": "chat.completion", "created": 1, "model": "m",
    "choices": [],
    "usage": { "prompt_tokens": 11, "completion_tokens": 0, "total_tokens": 11 }
}))]
#[case::error_inside_a_two_hundred(false, json!({
    "error": { "message": "insufficient quota", "type": "insufficient_quota", "code": "quota" }
}))]
#[case::null_content(false, json!({
    "id": "c", "object": "chat.completion", "created": 1, "model": "m",
    "choices": [{ "index": 0, "message": { "role": "assistant", "content": null }, "finish_reason": "stop" }]
}))]
#[case::whitespace_content(false, json!({
    "id": "c", "object": "chat.completion", "created": 1, "model": "m",
    "choices": [{ "index": 0, "message": { "role": "assistant", "content": "   \n" }, "finish_reason": "stop" }]
}))]
fn only_a_body_carrying_content_counts_as_an_answer(#[case] healthy: bool, #[case] reply: Value) {
    assert!(
        wire::decode_response(&reply, Format::OpenAiChat).is_ok(),
        "the parser accepts all of these, so it cannot be the guard"
    );

    let bytes = serde_json::to_vec(&reply).expect("serialises");
    assert_eq!(
        is_healthy_response(200, &bytes),
        healthy,
        "a 200 only wins the race when it carries content: {reply}"
    );
}
