// API probe. Throwaway: exercises the crates the router depends on so the real
// signatures are known before any module is written against them.

use serde_json::json;

fn probe_switchyard() {
    use switchyard_translation::{
        decode_aggregated_response, decode_request, encode_aggregated_response, encode_request,
        WireFormat,
    };
    let body = json!({"model": "x", "messages": [{"role": "user", "content": "hi"}]});
    let req = decode_request(WireFormat::OpenAiChat, &body).unwrap();
    let out = encode_request(&req, WireFormat::AnthropicMessages).unwrap();
    println!("encoded anthropic request: {}", out);

    let reply = json!({
        "id": "r1", "object": "chat.completion", "model": "x",
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": "hello"}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    });
    let agg = decode_aggregated_response(&reply, WireFormat::OpenAiChat).unwrap();
    let back = encode_aggregated_response(&agg, WireFormat::AnthropicMessages, Some("x")).unwrap();
    println!("encoded anthropic reply: {}", back);
}

fn probe_redb() {
    use redb::{Database, ReadableTable, TableDefinition};
    const T: TableDefinition<&[u8], &[u8]> = TableDefinition::new("responses");
    let _ = std::fs::remove_file("/tmp/probe.redb");
    let db = Database::create("/tmp/probe.redb").unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(T).unwrap();
        t.insert(b"k".as_slice(), b"v".as_slice()).unwrap();
    }
    tx.commit().unwrap();
    let rtx = db.begin_read().unwrap();
    let t = rtx.open_table(T).unwrap();
    println!("redb round trip: {:?}", t.get(b"k".as_slice()).unwrap().map(|v| v.value().to_vec()));
}

fn probe_blake3() {
    let h = blake3::hash(b"prompt");
    println!("blake3 digest bytes: {}", h.as_bytes().len());
}

fn main() {
    probe_switchyard();
    probe_redb();
    probe_blake3();
}
