//! Contract tier: the wire formats, and nothing else.
//!
//! See tests/README.md. `rstest` for the tables and `insta` for the
//! snapshots, so a change to the bytes we emit shows up as a snapshot diff.

mod openai_chat;
mod sse;

use llmrouter::wire::{self, Format};
use rstest::rstest;
use serde_json::json;

/// A minimal OpenAI chat request decodes, and re-encoding it into each served
/// format produces a body we have pinned with a snapshot.
#[rstest]
#[case::openai("openai_chat", Format::OpenAiChat)]
#[case::anthropic("anthropic_messages", Format::AnthropicMessages)]
fn request_round_trips(#[case] name: &str, #[case] target: Format) {
    let body = json!({
        "model": "gpt-4o-mini",
        "messages": [{ "role": "user", "content": "ping" }],
        "max_tokens": 16,
    });

    let neutral = wire::decode_request(Format::OpenAiChat, &body).expect("decodes");
    let encoded = wire::encode_request(&neutral, target).expect("encodes");

    insta::assert_json_snapshot!(name, encoded);
}

mod anthropic;
