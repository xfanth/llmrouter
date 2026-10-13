//! The response cache against a real redb file on a tempdir, with keys and
//! bodies that came through the wire layer rather than hand-built structs.
//!
//! The unit tests in `src/cache.rs` cover the write rule against synthetic
//! keys. What is proved here is the whole path a request takes: decode the
//! client's body, hash it, store a real serialised answer, read it back and
//! hand it to a client in either format.

use std::sync::Arc;
use std::time::Duration;

use llmrouter::cache::{cache_key, open_database, CacheEntry, StreamEnd, SCHEMA_VERSION};
use llmrouter::{Cache, CacheConfig, CacheKey, ChannelId, Format, NeutralResponse};
use rstest::rstest;
use serde_json::json;
use tempfile::TempDir;

const DAY: u64 = 86_400;
const NOW: u64 = 1_900_000_000;

/// A cache on its own redb file, with the directory kept alive alongside it.
fn store(max_bytes: u64, ttl: Duration) -> (TempDir, Cache) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = CacheConfig {
        path: dir.path().join("cache.redb"),
        max_bytes,
        ttl,
        ..CacheConfig::default()
    };
    let db = Arc::new(open_database(&config).expect("open the database"));
    let cache = Cache::open(
        db,
        &config,
        llmrouter::metrics::Metrics::new().expect("the recorder builds"),
    )
    .expect("open the responses table");
    (dir, cache)
}

/// An OpenAI chat request, as a client would send it.
fn openai_request(prompt: &str, model: &str) -> serde_json::Value {
    json!({
        "model": model,
        "max_tokens": 256,
        "messages": [{ "role": "user", "content": prompt }],
    })
}

/// The same conversation in Anthropic shape.
fn anthropic_request(prompt: &str, model: &str) -> serde_json::Value {
    json!({
        "model": model,
        "max_tokens": 256,
        "messages": [{ "role": "user", "content": [{ "type": "text", "text": prompt }] }],
    })
}

/// The key the router would compute for a client body.
fn key_for(format: Format, body: &serde_json::Value) -> CacheKey {
    let request = llmrouter::wire::decode_request(format, body).expect("the body decodes");
    cache_key(&request).expect("the request hashes")
}

/// A provider's buffered reply, decoded into the neutral response the cache
/// stores.
fn answer(text: &str) -> NeutralResponse {
    let body = json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "model": "glm-5.3",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text },
            "finish_reason": "stop",
        }],
        "usage": { "prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12 },
    });
    llmrouter::wire::decode_response(&body, Format::OpenAiChat).expect("the reply decodes")
}

/// What `server::write_cache` builds after a win.
fn entry(model: &str, intelligence: u16, text: &str, created_at: u64) -> CacheEntry {
    CacheEntry {
        schema_version: SCHEMA_VERSION,
        created_at,
        model_id: model.to_owned(),
        intelligence,
        channel_id: ChannelId(3),
        provider: "groq".to_owned(),
        body: serde_json::to_vec(&answer(text)).expect("the answer serialises"),
    }
}

/// The assistant text a client would end up reading, taken back out of a
/// stored entry the way the server does it.
fn served_text(entry: &CacheEntry, format: Format) -> String {
    let response: NeutralResponse =
        serde_json::from_slice(&entry.body).expect("the stored body reads back");
    let encoded =
        llmrouter::wire::encode_response(&response, format, None).expect("the answer encodes");
    let text = match format {
        Format::AnthropicMessages => encoded["content"][0]["text"].clone(),
        _ => encoded["choices"][0]["message"]["content"].clone(),
    };
    text.as_str().expect("the reply carries text").to_owned()
}

#[test]
fn a_repeat_prompt_sent_with_auto_hits_the_entry_whatever_model_wrote_it() {
    let (_dir, cache) = store(u64::MAX, Duration::from_secs(7 * DAY));
    let key = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );

    assert!(cache.get(key, None, NOW).expect("first read").is_none());
    assert!(cache
        .put(key, entry("glm-5.3", 50, "the lockfile drifted", NOW), NOW)
        .expect("first write"));

    // The same prompt a second time, still on auto, is served the stored
    // answer even though the caller never named the model that wrote it.
    let repeat = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );
    assert_eq!(repeat.0, key.0);
    let hit = cache
        .get(repeat, None, NOW)
        .expect("second read")
        .expect("the repeat prompt hits");
    assert_eq!(hit.model_id, "glm-5.3");
    assert_eq!(
        served_text(&hit, Format::OpenAiChat),
        "the lockfile drifted"
    );

    // A different prompt is a different entry, not the same answer again.
    let other = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build green", "auto"),
    );
    assert_ne!(other.0, key.0);
    assert!(cache.get(other, None, NOW).expect("third read").is_none());
}

#[test]
fn a_named_model_misses_when_the_entry_came_from_another_model() {
    let (_dir, cache) = store(u64::MAX, Duration::from_secs(7 * DAY));
    let key = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );
    cache
        .put(key, entry("glm-5.3", 50, "the lockfile drifted", NOW), NOW)
        .expect("write");

    // The model is not in the key, so a caller naming a different model lands
    // on this entry and must still be told it is a miss.
    let named = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "claude-fable-5-1"),
    );
    assert_eq!(named.0, key.0);
    assert!(cache
        .get(named, Some("claude-fable-5-1"), NOW)
        .expect("read")
        .is_none());

    // Naming the model that wrote it is a hit, in any case.
    assert!(cache
        .get(named, Some("GLM-5.3"), NOW)
        .expect("read")
        .is_some());
}

#[rstest]
#[case::weaker(10, false, "glm-5.3", "the lockfile drifted")]
#[case::equal(50, false, "glm-5.3", "the lockfile drifted")]
#[case::smarter(90, true, "claude-fable-5-1", "a dependency bumped a major")]
fn the_write_rule_keeps_the_smarter_answer(
    #[case] candidate_intelligence: u16,
    #[case] expect_stored: bool,
    #[case] expect_model: &str,
    #[case] expect_text: &str,
) {
    let (_dir, cache) = store(u64::MAX, Duration::from_secs(7 * DAY));
    let key = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );
    assert!(cache
        .put(key, entry("glm-5.3", 50, "the lockfile drifted", NOW), NOW)
        .expect("the first answer is stored"));

    let wrote = cache
        .put(
            key,
            entry(
                "claude-fable-5-1",
                candidate_intelligence,
                "a dependency bumped a major",
                NOW,
            ),
            NOW,
        )
        .expect("the second write runs");
    assert_eq!(wrote, expect_stored);

    let stored = cache
        .get(key, None, NOW)
        .expect("read")
        .expect("something is stored");
    assert_eq!(stored.model_id, expect_model);
    assert_eq!(served_text(&stored, Format::OpenAiChat), expect_text);
}

#[test]
fn a_cancelled_stream_is_never_written_and_never_displaces_a_stored_answer() {
    let (_dir, cache) = store(u64::MAX, Duration::from_secs(7 * DAY));
    let key = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );

    // A stream the race cancelled holds part of an answer, so an empty slot
    // stays empty.
    assert!(!cache
        .put_stream(
            key,
            entry("claude-fable-5-1", 90, "a dependenc", NOW),
            StreamEnd::Cancelled,
            NOW,
        )
        .expect("the cancelled write runs"));
    assert_eq!(cache.entry_count().expect("count"), 0);

    // A complete stream from a far weaker model takes the slot and keeps it,
    // because the smarter candidate never finished.
    assert!(cache
        .put_stream(
            key,
            entry("glm-5.3", 1, "the lockfile drifted", NOW),
            StreamEnd::Complete,
            NOW,
        )
        .expect("the complete write runs"));
    assert!(!cache
        .put_stream(
            key,
            entry("claude-fable-5-1", 90, "a dependenc", NOW),
            StreamEnd::Cancelled,
            NOW,
        )
        .expect("the second cancelled write runs"));

    let stored = cache
        .get(key, None, NOW)
        .expect("read")
        .expect("the complete answer is still there");
    assert_eq!(stored.model_id, "glm-5.3");
    assert_eq!(
        served_text(&stored, Format::OpenAiChat),
        "the lockfile drifted"
    );
}

#[test]
fn an_entry_past_the_ttl_is_a_miss_on_read() {
    let ttl = Duration::from_secs(60);
    let (_dir, cache) = store(u64::MAX, ttl);
    let key = key_for(
        Format::OpenAiChat,
        &openai_request("why is the build red", "auto"),
    );
    cache
        .put(key, entry("glm-5.3", 90, "the lockfile drifted", NOW), NOW)
        .expect("write");

    // One second short of the ttl is still a hit.
    assert!(cache
        .get(key, None, NOW + ttl.as_secs() - 1)
        .expect("read inside the ttl")
        .is_some());

    // The ttl exactly is a miss, and the row is still on disk until something
    // overwrites it.
    let expired = NOW + ttl.as_secs();
    assert!(cache
        .get(key, None, expired)
        .expect("read past the ttl")
        .is_none());
    assert_eq!(cache.entry_count().expect("count"), 1);

    // Expired loses to anything, however unintelligent.
    assert!(cache
        .put(key, entry("tiny", 1, "no idea", expired), expired)
        .expect("write over the expired row"));
    assert_eq!(
        cache
            .get(key, None, expired)
            .expect("read")
            .expect("the new answer is stored")
            .model_id,
        "tiny"
    );
}

#[test]
fn a_write_over_max_bytes_evicts_the_oldest_tenth_first() {
    let dir = tempfile::tempdir().expect("tempdir");
    let roomy = CacheConfig {
        path: dir.path().join("cache.redb"),
        max_bytes: u64::MAX,
        ttl: Duration::from_secs(7 * DAY),
        ..CacheConfig::default()
    };
    let db = Arc::new(open_database(&roomy).expect("open the database"));
    let cache = Cache::open(
        Arc::clone(&db),
        &roomy,
        llmrouter::metrics::Metrics::new().expect("the recorder builds"),
    )
    .expect("open the responses table");

    // Twenty answers, oldest first.
    let keys: Vec<CacheKey> = (0..20u8)
        .map(|i| {
            let key = key_for(
                Format::OpenAiChat,
                &openai_request(&format!("question {i}"), "auto"),
            );
            cache
                .put(
                    key,
                    entry("glm-5.3", 50, "an answer", NOW - 1_000 + u64::from(i)),
                    NOW,
                )
                .expect("write");
            key
        })
        .collect();
    assert_eq!(cache.entry_count().expect("count"), 20);

    // The same file, now with a cap the file is already over. The next write
    // frees space before it stores.
    let tight = Cache::open(
        Arc::clone(&db),
        &CacheConfig {
            max_bytes: 1,
            ..roomy.clone()
        },
        llmrouter::metrics::Metrics::new().expect("the recorder builds"),
    )
    .expect("open the responses table again");
    let fresh = key_for(Format::OpenAiChat, &openai_request("question 20", "auto"));
    assert!(tight
        .put(fresh, entry("glm-5.3", 50, "an answer", NOW), NOW)
        .expect("the write over the cap"));

    // Two of twenty went, the oldest two, and the new answer is there.
    assert_eq!(tight.entry_count().expect("count"), 19);
    assert!(cache.get(keys[0], None, NOW).expect("read").is_none());
    assert!(cache.get(keys[1], None, NOW).expect("read").is_none());
    assert!(cache.get(keys[2], None, NOW).expect("read").is_some());
    assert!(cache.get(keys[19], None, NOW).expect("read").is_some());
    assert!(cache.get(fresh, None, NOW).expect("read").is_some());
}

#[test]
fn the_same_conversation_as_anthropic_and_as_openai_lands_on_one_key() {
    let (_dir, cache) = store(u64::MAX, Duration::from_secs(7 * DAY));
    let prompt = "why is the build red";
    let openai = key_for(Format::OpenAiChat, &openai_request(prompt, "auto"));
    let anthropic = key_for(
        Format::AnthropicMessages,
        &anthropic_request(prompt, "claude-fable-5-1"),
    );
    assert_eq!(
        anthropic.0, openai.0,
        "the model and the client's wire format are not part of the key"
    );

    // The OpenAI client asks first and the answer is stored.
    cache
        .put(
            openai,
            entry("glm-5.3", 50, "the lockfile drifted", NOW),
            NOW,
        )
        .expect("write");

    // The Anthropic client asking the same question is served the same answer,
    // re-encoded into its own format.
    let hit = cache
        .get(anthropic, None, NOW)
        .expect("read")
        .expect("the Anthropic client hits the OpenAI client's entry");
    assert_eq!(
        served_text(&hit, Format::AnthropicMessages),
        "the lockfile drifted"
    );

    // A different question in Anthropic shape is a different key.
    let other = key_for(
        Format::AnthropicMessages,
        &anthropic_request("why is the build green", "claude-fable-5-1"),
    );
    assert_ne!(other.0, openai.0);
}
