//! Server-sent-event framing, in both directions.
//!
//! Inbound: the bytes a provider puts on the wire, chopped the way a real
//! socket chops them, must rejoin into the same answer. Outbound: the
//! keepalive comment the router emits must be an SSE comment a client skips,
//! not a frame it tries to read.
//!
//! No sockets and no upstreams here. `wire::decode_stream` is the same entry
//! point the race uses, and `health::stream_verdict` is the same parser that
//! decides whether a 2xx is carrying an answer.

use futures::stream;
use llmrouter::health::{self, StreamVerdict};
use llmrouter::wire::{self, Format};
use rstest::rstest;
use serde_json::Value;

/// A multi-byte character in the answer, so a chunk boundary can land in the
/// middle of one. Three bytes in UTF-8, and nothing a byte-at-a-time reader
/// can get right by accident.
const MULTIBYTE: &str = "→";

/// Cuts a blob into fixed-size chunks, the way a socket hands bytes over: no
/// respect for event boundaries and none for character boundaries.
fn chopped(sse: &str, chunk: usize) -> Vec<Vec<u8>> {
    sse.as_bytes().chunks(chunk).map(<[u8]>::to_vec).collect()
}

/// Runs chunks through the provider-facing decoder and folds the result,
/// giving back whatever the fold decided: an answer in the client's OpenAI
/// format, or the error the decoder raised.
async fn fold(chunks: Vec<Vec<u8>>, source: Format) -> Result<Value, wire::WireError> {
    let bytes = stream::iter(chunks.into_iter().map(Ok));
    let decoded = wire::decode_stream(bytes, source)?;
    let aggregated = wire::aggregate_stream(decoded).await?;
    wire::encode_response(&aggregated, Format::OpenAiChat, None)
}

/// The same fold, for the cases that must succeed.
async fn decode_and_aggregate(chunks: Vec<Vec<u8>>, source: Format) -> Value {
    fold(chunks, source)
        .await
        .expect("the chunks fold into one answer")
}

/// The assistant text of an aggregated OpenAI chat reply.
fn text_of(response: &Value) -> String {
    response["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// An OpenAI chat content delta, as one SSE event.
fn delta(text: &str) -> String {
    let chunk = serde_json::json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "model": "gpt-4o-mini",
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": Value::Null }],
    });
    format!("data: {chunk}\n\n")
}

/// The closing event: an empty delta carrying a `finish_reason`. This is what
/// makes a stream terminal, with or without the `[DONE]` line after it.
fn finish(reason: &str) -> String {
    let chunk = serde_json::json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "model": "gpt-4o-mini",
        "choices": [{ "index": 0, "delta": {}, "finish_reason": reason }],
    });
    format!("data: {chunk}\n\n")
}

/// A provider stream chopped at every size from one byte upwards rejoins into
/// the same answer, including the characters a cut landed inside.
#[rstest]
#[case::one_byte_at_a_time(1)]
#[case::mid_event(7)]
#[case::awkward(23)]
#[case::whole_blob(4096)]
#[tokio::test]
async fn a_provider_stream_split_at_any_byte_rejoins_into_the_same_answer(#[case] chunk: usize) {
    let want = format!("left {MULTIBYTE} right");
    let sse = format!(
        "{}{}{}data: [DONE]\n\n",
        delta("left "),
        delta(MULTIBYTE),
        delta(" right"),
    );

    let chunks = chopped(&sse, chunk);
    let response = decode_and_aggregate(chunks, Format::OpenAiChat).await;

    assert_eq!(text_of(&response), want, "split every {chunk} bytes");
}

/// The one-byte case above is only worth having if the cuts really do land
/// inside a multi-byte character. This pins that: the decoder is handed bytes
/// that are not valid UTF-8 on their own.
#[test]
fn chopping_a_stream_byte_by_byte_really_does_split_a_character() {
    let sse = delta(MULTIBYTE);
    let chunks = chopped(&sse, 1);

    let broken = chunks
        .iter()
        .filter(|chunk| std::str::from_utf8(chunk).is_err())
        .count();

    assert_eq!(
        broken,
        MULTIBYTE.len(),
        "every byte of {MULTIBYTE:?} past the first is an invalid chunk on its own"
    );
}

/// The other inbound framing. Anthropic puts a named `event:` line above every
/// `data:` line, so a cut between the two halves of a frame is a different
/// shape of problem from OpenAI's. Chopped, it must still reach the client as
/// the same answer.
#[rstest]
#[case::one_byte_at_a_time(1)]
#[case::mid_event(17)]
#[case::whole_blob(4096)]
#[tokio::test]
async fn an_anthropic_stream_split_across_its_event_and_data_lines_rejoins(#[case] chunk: usize) {
    let mut sse = String::new();
    for (name, body) in [
        (
            "message_start",
            serde_json::json!({ "type": "message_start", "message": { "id": "msg_fake", "type": "message", "role": "assistant", "model": "claude-fake", "content": [], "stop_reason": Value::Null, "usage": { "input_tokens": 1, "output_tokens": 0 } } }),
        ),
        (
            "content_block_start",
            serde_json::json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "text", "text": "" } }),
        ),
        (
            "content_block_delta",
            serde_json::json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": "left " } }),
        ),
        (
            "content_block_delta",
            serde_json::json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": MULTIBYTE } }),
        ),
        (
            "content_block_delta",
            serde_json::json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": " right" } }),
        ),
        (
            "content_block_stop",
            serde_json::json!({ "type": "content_block_stop", "index": 0 }),
        ),
        (
            "message_delta",
            serde_json::json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 3 } }),
        ),
        (
            "message_stop",
            serde_json::json!({ "type": "message_stop" }),
        ),
    ] {
        sse.push_str(&format!("event: {name}\ndata: {body}\n\n"));
    }

    let response = decode_and_aggregate(chopped(&sse, chunk), Format::AnthropicMessages).await;

    assert_eq!(
        text_of(&response),
        format!("left {MULTIBYTE} right"),
        "split every {chunk} bytes"
    );
}

/// A tool call whose name, id and arguments arrive as fragments across several
/// events rejoins with its id intact and its arguments parseable as JSON.
#[tokio::test]
async fn a_tool_call_arriving_in_fragments_rejoins_with_its_id_and_arguments_intact() {
    // The opener carries the id and the name; the arguments then dribble in,
    // split mid-token and mid-string the way providers really send them.
    let opener = serde_json::json!({
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "model": "gpt-4o-mini",
        "choices": [{
            "index": 0,
            "delta": { "tool_calls": [{
                "index": 0,
                "id": "call_fake_0001",
                "type": "function",
                "function": { "name": "get_weather", "arguments": "" },
            }] },
            "finish_reason": Value::Null,
        }],
    });

    let fragments = ["{\"ci", "ty\":\"Lon", "don\",\"unit", "s\":\"c\"}"];
    let mut sse = format!("data: {opener}\n\n");
    for fragment in fragments {
        let piece = serde_json::json!({
            "id": "chatcmpl-fake",
            "object": "chat.completion.chunk",
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "delta": { "tool_calls": [{
                    "index": 0,
                    "function": { "arguments": fragment },
                }] },
                "finish_reason": Value::Null,
            }],
        });
        sse.push_str(&format!("data: {piece}\n\n"));
    }
    sse.push_str(&finish("tool_calls"));
    sse.push_str("data: [DONE]\n\n");

    // Chopped as well, so the fragments cross chunk boundaries too.
    let response = decode_and_aggregate(chopped(&sse, 13), Format::OpenAiChat).await;

    let call = &response["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(
        call["id"], "call_fake_0001",
        "the id survives the fragments"
    );
    assert_eq!(call["function"]["name"], "get_weather");

    let arguments: Value = serde_json::from_str(
        call["function"]["arguments"]
            .as_str()
            .expect("the arguments rejoin as a string"),
    )
    .expect("the rejoined arguments are valid JSON");
    assert_eq!(
        arguments,
        serde_json::json!({ "city": "London", "units": "c" })
    );
}

/// `[DONE]` is not the only terminator. A stream that stops after a
/// `finish_reason` is complete, and gives the same answer as one that sends
/// `[DONE]` as well.
#[tokio::test]
async fn a_finish_reason_terminates_a_stream_without_a_done_line() {
    let without_done = format!("{}{}{}", delta("half "), delta("an answer"), finish("stop"));
    let with_done = format!("{without_done}data: [DONE]\n\n");

    let ended = decode_and_aggregate(chopped(&without_done, 9), Format::OpenAiChat).await;
    let terminated = decode_and_aggregate(chopped(&with_done, 9), Format::OpenAiChat).await;

    assert_eq!(text_of(&ended), "half an answer");
    assert_eq!(ended["choices"][0]["finish_reason"], "stop");
    assert_eq!(
        ended, terminated,
        "the `[DONE]` line changes nothing about the answer"
    );
}

/// A stream cut off with no terminator at all is an error, not a short
/// answer. This is the bit that matters: a provider that dies mid-answer must
/// not have its half-sentence served to the client as if it were finished.
#[tokio::test]
async fn a_stream_that_ends_with_no_terminator_at_all_is_an_error_not_a_short_answer() {
    let truncated = format!("{}{}", delta("half "), delta("an answer"));

    let outcome = fold(chopped(&truncated, 9), Format::OpenAiChat).await;

    let error = outcome.expect_err("a truncated stream must not fold into an answer");
    assert!(
        matches!(error, wire::WireError::Stream(_)),
        "a cut-off stream is a stream failure, got {error:?}"
    );
    assert!(
        error.to_string().contains("terminal event"),
        "the reason says what was missing: {error}"
    );
}

/// Comment lines are ignored. Providers use them as keepalives, so a stream
/// peppered with them must decode to exactly the stream without them.
#[tokio::test]
async fn comment_lines_in_a_provider_stream_are_ignored() {
    let plain = format!("{}{}data: [DONE]\n\n", delta("one "), delta("two"));
    let commented = format!(
        ": OPENROUTER PROCESSING\n\n{}: OPENROUTER PROCESSING\n\n{}:\n\ndata: [DONE]\n\n",
        delta("one "),
        delta("two"),
    );

    let plain = decode_and_aggregate(chopped(&plain, 11), Format::OpenAiChat).await;
    let commented = decode_and_aggregate(chopped(&commented, 11), Format::OpenAiChat).await;

    assert_eq!(text_of(&commented), "one two");
    assert_eq!(text_of(&plain), text_of(&commented));
}

/// The race's own verdict on a stream must agree: comments alone are not an
/// answer, and the content frame behind them is.
#[rstest]
#[case::comment_only(": OPENROUTER PROCESSING\n\n", StreamVerdict::Pending)]
#[case::bare_comment(":\n\n", StreamVerdict::Pending)]
#[case::terminator_only("data: [DONE]\n\n", StreamVerdict::Pending)]
#[case::comment_then_content(
    ": OPENROUTER PROCESSING\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
    StreamVerdict::Content
)]
fn a_comment_is_never_counted_as_content(#[case] sse: &str, #[case] want: StreamVerdict) {
    assert_eq!(health::stream_verdict(sse.as_bytes()), want);
}

/// The keepalive the router emits to a waiting client is an SSE comment: the
/// exact bytes `: ping`, no `data:` field, so a client skips it rather than
/// handing it to the application. This builds the frame the same way
/// `with_keepalive` does and reads what goes on the wire.
#[tokio::test]
async fn the_keepalive_the_router_emits_is_a_comment_a_client_skips() {
    use axum::response::sse::{Event, Sse};
    use axum::response::IntoResponse;

    let frames =
        stream::once(async { Ok::<_, std::convert::Infallible>(Event::default().comment("ping")) });
    let response = Sse::new(frames).into_response();

    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream"),
    );

    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("the keepalive body reads");

    assert_eq!(
        std::str::from_utf8(&body).expect("the keepalive is UTF-8"),
        ": ping\n\n",
        "the keepalive must be an SSE comment and nothing else"
    );
    assert_eq!(
        health::stream_verdict(&body),
        StreamVerdict::Pending,
        "a keepalive must never be mistaken for an answer"
    );
}
