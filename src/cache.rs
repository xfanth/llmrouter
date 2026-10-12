//! The response cache: a redb `responses` table, one entry per prompt.
//!
//! The key is blake3 over the canonicalised neutral request with the model
//! excluded, so an Anthropic client and an OpenAI client asking the same
//! question land on the same entry.
//!
//! The write rule is one transaction: read what is stored, compare
//! intelligence, and write only when the candidate is higher, the entry is
//! absent, or the entry has expired. The client gets the fastest answer; the
//! cache keeps the best one.
//!
//! A streamed answer goes through the same write rule, with one gate in front
//! of it: only a complete stream may be offered. A cancelled or stalled stream
//! is a partial answer and a partial answer is wrong, so it never reaches
//! redb. See [`StreamEnd`] and [`Cache::put_stream`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::catalogue::ChannelId;
use crate::config::CacheConfig;
use crate::wire::NeutralRequest;
use crate::RouterError;

/// Bump this to invalidate every stored entry at once.
pub const SCHEMA_VERSION: u8 = 1;

/// The `responses` table: blake3 key to a framed entry.
const RESPONSES: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("responses");

/// Blake3 over the canonicalised neutral request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey(pub [u8; 32]);

/// One stored answer. The prompt is deliberately not here: nothing ever reads
/// it back, and it would be 31 times the bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Invalidation marker.
    pub schema_version: u8,
    /// Unix seconds, for the ttl check on read.
    pub created_at: u64,
    /// Which model produced this answer. A caller naming a model is served
    /// only when this matches.
    pub model_id: String,
    /// That model's intelligence score, so a later write can compare. A model
    /// absent from the ranking scores 0 and never displaces a ranked one.
    pub intelligence: u16,
    /// Which channel produced it, for logging.
    pub channel_id: ChannelId,
    /// Which provider produced it, for logging.
    pub provider: String,
    /// The neutral `AggLlmResponse`, serialised.
    pub body: Vec<u8>,
}

/// How a streamed channel call ended, which is the only thing the cache needs
/// to know about a stream.
///
/// The race picks one winner and cancels the rest the moment it has one. Every
/// cancelled stream, and any stream that stalled or died partway, is
/// [`StreamEnd::Cancelled`]. Only a stream the provider ended on its own, with
/// the whole answer delivered, is [`StreamEnd::Complete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEnd {
    /// The provider finished the stream and the whole answer is in hand.
    Complete,
    /// Cancelled, stalled or cut off. A partial answer, never cached.
    Cancelled,
}

/// Everything on an entry except the body. Stored as JSON in front of the raw
/// body bytes, so the body is never re-encoded: JSON would turn a 125KB body
/// into a 475KB array of numbers, and the whole cache is sized in bytes.
#[derive(Debug, Serialize, Deserialize)]
struct Header {
    schema_version: u8,
    created_at: u64,
    model_id: String,
    intelligence: u16,
    channel_id: ChannelId,
    provider: String,
}

/// `header length as four little-endian bytes` + `header JSON` + `body`.
fn encode(entry: &CacheEntry) -> crate::Result<Vec<u8>> {
    let header = Header {
        schema_version: entry.schema_version,
        created_at: entry.created_at,
        model_id: entry.model_id.clone(),
        intelligence: entry.intelligence,
        channel_id: entry.channel_id,
        provider: entry.provider.clone(),
    };
    let json = serde_json::to_vec(&header)
        .map_err(|e| RouterError::Store(format!("encoding cache header: {e}")))?;
    let mut out = Vec::with_capacity(4 + json.len() + entry.body.len());
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(&entry.body);
    Ok(out)
}

/// Reads the header only. Eviction needs `created_at` and nothing else, so it
/// never touches the body.
fn decode_header(raw: &[u8]) -> crate::Result<(Header, usize)> {
    if raw.len() < 4 {
        return Err(RouterError::Store("cache entry is truncated".into()));
    }
    let len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let end = 4usize
        .checked_add(len)
        .filter(|end| *end <= raw.len())
        .ok_or_else(|| RouterError::Store("cache entry header length is out of range".into()))?;
    let header: Header = serde_json::from_slice(&raw[4..end])
        .map_err(|e| RouterError::Store(format!("decoding cache header: {e}")))?;
    Ok((header, end))
}

fn decode(raw: &[u8]) -> crate::Result<CacheEntry> {
    let (header, body_start) = decode_header(raw)?;
    Ok(CacheEntry {
        schema_version: header.schema_version,
        created_at: header.created_at,
        model_id: header.model_id,
        intelligence: header.intelligence,
        channel_id: header.channel_id,
        provider: header.provider,
        body: raw[body_start..].to_vec(),
    })
}

/// Maps anything redb can refuse into the crate's store error.
fn store<E: std::fmt::Display>(what: &str, e: E) -> RouterError {
    RouterError::Store(format!("{what}: {e}"))
}

/// Opens the one redb file both the `responses` and `health` tables live in,
/// with redb's page cache capped at `config.page_cache_bytes`. redb's own
/// default is 1GiB, which on its own would push the process past
/// `memory_high_water` and make the admission gate refuse every request, so the
/// cap is applied on every open rather than left to redb.
pub fn open_database(config: &CacheConfig) -> crate::Result<Database> {
    redb::Builder::new()
        .set_cache_size(usize::try_from(config.page_cache_bytes).unwrap_or(usize::MAX))
        .create(&config.path)
        .map_err(|e| store("opening the database", e))
}

/// The `responses` table.
#[derive(Debug)]
pub struct Cache {
    db: Arc<Database>,
    ttl: Duration,
    max_bytes: u64,
    path: std::path::PathBuf,
    /// File size at the last eviction. redb never shrinks its file, so the
    /// stat alone stays over the cap forever once it has been reached. Holding
    /// the size we last evicted at means the next eviction waits until the
    /// file has actually grown again, which is the backstop DESIGN.md asks
    /// for rather than a scan on every write.
    evicted_at: AtomicU64,
}

impl Cache {
    /// Opens the `responses` table in `db`.
    pub fn open(db: Arc<Database>, config: &CacheConfig) -> crate::Result<Self> {
        // Creating the table once here means every later read transaction
        // finds it, rather than failing on a cold database.
        let txn = db.begin_write().map_err(|e| store("begin write", e))?;
        txn.open_table(RESPONSES)
            .map_err(|e| store("opening the responses table", e))?;
        txn.commit().map_err(|e| store("commit", e))?;
        Ok(Self {
            db,
            ttl: config.ttl,
            max_bytes: config.max_bytes,
            path: config.path.clone(),
            evicted_at: AtomicU64::new(0),
        })
    }

    /// Reads an entry.
    ///
    /// `named_model` is `None` when the caller sent `auto` or no model, in
    /// which case any stored model will do. When it is set, only an entry whose
    /// `model_id` matches is a hit. An entry older than the ttl is a miss and
    /// the next write overwrites it.
    pub fn get(
        &self,
        key: CacheKey,
        named_model: Option<&str>,
        now_unix: u64,
    ) -> crate::Result<Option<CacheEntry>> {
        let txn = self.db.begin_read().map_err(|e| store("begin read", e))?;
        let table = txn
            .open_table(RESPONSES)
            .map_err(|e| store("opening the responses table", e))?;
        let Some(raw) = table.get(&key.0).map_err(|e| store("cache read", e))? else {
            return Ok(None);
        };
        let entry = decode(raw.value())?;
        if !self.is_fresh(&entry, now_unix) {
            return Ok(None);
        }
        // A caller naming a model is never served another model's answer.
        if let Some(wanted) = named_model {
            if !entry.model_id.eq_ignore_ascii_case(wanted) {
                return Ok(None);
            }
        }
        Ok(Some(entry))
    }

    /// True when the entry is on the current schema and inside the ttl.
    fn is_fresh(&self, entry: &CacheEntry, now_unix: u64) -> bool {
        entry.schema_version == SCHEMA_VERSION
            && now_unix.saturating_sub(entry.created_at) < self.ttl.as_secs()
    }

    /// The write rule, in one transaction: read what is stored, compare, write
    /// only if the candidate wins. Returns true when the candidate was stored.
    ///
    /// Never called for a failure. Only a 2xx with non-empty content is
    /// written, the same test the race uses to pick a winner.
    pub fn put(&self, key: CacheKey, candidate: CacheEntry, now_unix: u64) -> crate::Result<bool> {
        // Over the cap, free space once. redb allows one write transaction at
        // a time, so eviction has to finish before this one opens. The freed
        // pages are reused inside the same file, so a second eviction only
        // earns its keep once the file has grown past the size we last evicted
        // at.
        let size = self.size_bytes()?;
        if size > self.max_bytes && size > self.evicted_at.load(Ordering::Relaxed) {
            self.evict_oldest()?;
            self.evicted_at.store(size, Ordering::Relaxed);
        }

        let encoded = encode(&candidate)?;
        let txn = self.db.begin_write().map_err(|e| store("begin write", e))?;
        let wrote = {
            let mut table = txn
                .open_table(RESPONSES)
                .map_err(|e| store("opening the responses table", e))?;
            let stored = table.get(&key.0).map_err(|e| store("cache read", e))?;
            let beaten = match &stored {
                None => true,
                Some(raw) => {
                    let entry = decode(raw.value())?;
                    // Absent, expired or less intelligent all lose. Equal keeps
                    // what is already there, so a tie costs no write.
                    !self.is_fresh(&entry, now_unix) || entry.intelligence < candidate.intelligence
                }
            };
            drop(stored);
            if beaten {
                table
                    .insert(&key.0, encoded.as_slice())
                    .map_err(|e| store("cache write", e))?;
            }
            beaten
        };
        txn.commit().map_err(|e| store("commit", e))?;
        Ok(wrote)
    }

    /// The write rule for a streamed answer: the same single transaction as
    /// [`Cache::put`], with the completeness gate in front of it.
    ///
    /// A stream that was cancelled, stalled or cut off mid-flight holds part
    /// of an answer, and half an answer served back a week later is simply
    /// wrong. So a losing stream offers nothing: the late-arrival rule that
    /// applies to buffered channel calls has no streamed equivalent, because a
    /// cancelled stream has no complete body to offer. Returns true only when
    /// the candidate was stored.
    pub fn put_stream(
        &self,
        key: CacheKey,
        candidate: CacheEntry,
        end: StreamEnd,
        now_unix: u64,
    ) -> crate::Result<bool> {
        match end {
            // Nothing is read, nothing is compared, no transaction opens.
            StreamEnd::Cancelled => Ok(false),
            StreamEnd::Complete => self.put(key, candidate, now_unix),
        }
    }

    /// [`Cache::put_stream`] on the blocking pool.
    ///
    /// The write is blocking work: a `stat`, possibly an eviction scan over
    /// the whole table, and a redb write transaction that waits for whichever
    /// writer holds the file, since redb allows exactly one. Run inline it
    /// parks a tokio worker, and enough of them parked means no stream is
    /// polled and no connection is accepted. So it runs off the runtime.
    pub async fn put_stream_off_thread(
        self: Arc<Self>,
        key: CacheKey,
        candidate: CacheEntry,
        end: StreamEnd,
        now_unix: u64,
    ) -> crate::Result<bool> {
        tokio::task::spawn_blocking(move || self.put_stream(key, candidate, end, now_unix))
            .await
            .map_err(|e| store("the cache write task", e))?
    }

    /// Deletes the oldest 10 percent by `created_at` in one transaction.
    /// Called on write when the file is over `max_bytes`. redb does not shrink
    /// its file, so the cap is on bytes and the freed space is reused.
    pub fn evict_oldest(&self) -> crate::Result<usize> {
        let txn = self.db.begin_write().map_err(|e| store("begin write", e))?;
        let removed = {
            let mut table = txn
                .open_table(RESPONSES)
                .map_err(|e| store("opening the responses table", e))?;
            let mut ages: Vec<(u64, [u8; 32])> = Vec::new();
            for row in table.iter().map_err(|e| store("cache scan", e))? {
                let (k, v) = row.map_err(|e| store("cache scan", e))?;
                // A row that will not decode is treated as ancient, so a bad
                // entry is the first thing eviction clears.
                let created_at = decode_header(v.value())
                    .map(|(h, _)| h.created_at)
                    .unwrap_or(0);
                ages.push((created_at, *k.value()));
            }
            // Round up, so a small table still makes progress rather than
            // evicting nothing and writing over the cap forever.
            let take = ages.len().div_ceil(10);
            ages.sort_unstable_by_key(|(created_at, _)| *created_at);
            for (_, k) in ages.iter().take(take) {
                table.remove(k).map_err(|e| store("cache delete", e))?;
            }
            take
        };
        txn.commit().map_err(|e| store("commit", e))?;
        Ok(removed)
    }

    /// Entry count, for `router_cache_entries`.
    pub fn entry_count(&self) -> crate::Result<u64> {
        let txn = self.db.begin_read().map_err(|e| store("begin read", e))?;
        let table = txn
            .open_table(RESPONSES)
            .map_err(|e| store("opening the responses table", e))?;
        table.len().map_err(|e| store("cache length", e))
    }

    /// File size on disk, one stat. Compared against `max_bytes` on write.
    pub fn size_bytes(&self) -> crate::Result<u64> {
        Ok(std::fs::metadata(&self.path)
            .map_err(|e| store("stat on the cache file", e))?
            .len())
    }
}

/// Blake3 over the canonicalised neutral request, with the model excluded.
///
/// In the key: messages in order, the system prompt, tool definitions sorted by
/// name, temperature, top_p, max_tokens, stop sequences and the response
/// format. Not in the key: the model, the client's API key, headers, request
/// ids, timestamps and the streaming flag.
pub fn cache_key(request: &NeutralRequest) -> crate::Result<CacheKey> {
    let bytes = crate::wire::canonical_bytes(request)?;
    Ok(CacheKey(*blake3::hash(&bytes).as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: u64 = 86_400;

    /// A cache on its own file in a throwaway directory, removed on drop.
    struct TempCache {
        cache: Cache,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for TempCache {
        type Target = Cache;
        fn deref(&self) -> &Cache {
            &self.cache
        }
    }

    fn temp_cache(name: &str, max_bytes: u64) -> TempCache {
        let dir = tempfile::tempdir().expect("scratch dir");
        let config = CacheConfig {
            path: dir.path().join(format!("{name}.redb")),
            max_bytes,
            ttl: Duration::from_secs(7 * DAY),
            ..CacheConfig::default()
        };
        let db = Arc::new(open_database(&config).expect("create"));
        let cache = Cache::open(db, &config).expect("open");
        TempCache { cache, _dir: dir }
    }

    fn key(byte: u8) -> CacheKey {
        CacheKey([byte; 32])
    }

    fn entry(model: &str, intelligence: u16, created_at: u64) -> CacheEntry {
        CacheEntry {
            schema_version: SCHEMA_VERSION,
            created_at,
            model_id: model.to_string(),
            intelligence,
            channel_id: ChannelId(7),
            provider: "groq".to_string(),
            body: b"{\"ok\":true}".to_vec(),
        }
    }

    /// A cache write must never sit on a runtime worker. With one worker and
    /// another writer holding redb's single write lock, an inline write parks
    /// the only thread and nothing else on the runtime is polled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_blocked_cache_write_leaves_the_runtime_polling() {
        let dir = tempfile::tempdir().expect("scratch dir");
        let config = CacheConfig {
            path: dir.path().join("off-thread.redb"),
            max_bytes: 1 << 20,
            ttl: Duration::from_secs(7 * DAY),
            ..CacheConfig::default()
        };
        let db = Arc::new(open_database(&config).expect("create"));
        let cache = Arc::new(Cache::open(Arc::clone(&db), &config).expect("open"));

        // Take redb's one write lock from outside the runtime and sit on it
        // until told to let go. The holder says when it has the lock, so no
        // wall-clock guess decides who gets there first.
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let txn = db.begin_write().expect("begin write");
            locked_tx.send(()).expect("signal locked");
            release_rx.recv().expect("wait for release");
            txn.commit().expect("commit");
        });
        locked_rx.recv().expect("holder took the lock");

        // Both tasks go on the runtime's shared queue, which is first in,
        // first out, so the one worker picks up the write first. An inline
        // write then parks that worker on the lock and `other` never runs.
        let write = tokio::spawn(Arc::clone(&cache).put_stream_off_thread(
            key(9),
            entry("claude-fable-5-1", 10, 1_000),
            StreamEnd::Complete,
            1_000,
        ));

        let (ran_tx, ran_rx) = std::sync::mpsc::channel();
        let other = tokio::spawn(async move { ran_tx.send(()).expect("signal ran") });

        // Wait on a plain thread channel, not a tokio timer: tokio timers are
        // driven by the worker, which a regressed write would have parked.
        let polled = ran_rx.recv_timeout(Duration::from_secs(5));
        release_tx.send(()).expect("release the lock");
        polled.expect("the runtime stopped polling while the cache write waited");

        other.await.expect("join");
        assert!(write.await.expect("join").expect("put"));
        holder.join().expect("holder");
    }

    /// The named-model check matches the catalogue's, which ignores ASCII case.
    #[test]
    fn a_named_model_in_another_case_still_hits() {
        let cache = temp_cache("model-case", 1 << 20);
        assert!(cache
            .put(key(1), entry("claude-fable-5-1", 10, 1_000), 1_000)
            .expect("put"));

        assert!(cache
            .get(key(1), Some("Claude-Fable-5-1"), 1_000)
            .expect("get")
            .is_some());
        assert!(cache
            .get(key(1), Some("some-other-model"), 1_000)
            .expect("get")
            .is_none());
    }

    #[test]
    fn framing_round_trips_a_body_with_arbitrary_bytes() {
        let mut e = entry("glm-5.3", 42, 1_000);
        e.body = (0u8..=255).collect();
        let decoded = decode(&encode(&e).unwrap()).unwrap();
        assert_eq!(decoded.body, e.body);
        assert_eq!(decoded.model_id, "glm-5.3");
        assert_eq!(decoded.intelligence, 42);
        assert_eq!(decoded.channel_id, ChannelId(7));
    }

    #[test]
    fn a_truncated_entry_is_an_error_not_a_panic() {
        assert!(decode(&[1, 2]).is_err());
        assert!(decode(&[255, 255, 255, 255, 9]).is_err());
    }

    #[test]
    fn absent_key_is_a_miss() {
        let c = temp_cache("absent", u64::MAX);
        assert!(c.get(key(1), None, 1_000).unwrap().is_none());
    }

    #[test]
    fn write_rule_absent_then_lower_equal_higher() {
        let c = temp_cache("write-rule", u64::MAX);
        let now = 10 * DAY;

        // Absent: the candidate is written.
        assert!(c.put(key(1), entry("glm-5.3", 50, now), now).unwrap());

        // Lower intelligence: kept out.
        assert!(!c.put(key(1), entry("tiny", 10, now), now).unwrap());
        assert_eq!(
            c.get(key(1), None, now).unwrap().unwrap().model_id,
            "glm-5.3"
        );

        // Equal intelligence: what is stored stays, so a tie costs no write.
        assert!(!c.put(key(1), entry("other", 50, now), now).unwrap());
        assert_eq!(
            c.get(key(1), None, now).unwrap().unwrap().model_id,
            "glm-5.3"
        );

        // Higher intelligence: overwrites.
        assert!(c
            .put(key(1), entry("claude-fable-5-1", 90, now).clone(), now)
            .unwrap());
        let stored = c.get(key(1), None, now).unwrap().unwrap();
        assert_eq!(stored.model_id, "claude-fable-5-1");
        assert_eq!(stored.intelligence, 90);
    }

    #[test]
    fn an_unranked_model_scores_zero_and_never_displaces_a_ranked_one() {
        let c = temp_cache("unranked", u64::MAX);
        let now = 10 * DAY;
        assert!(c.put(key(1), entry("ranked", 1, now), now).unwrap());
        assert!(!c.put(key(1), entry("unranked", 0, now), now).unwrap());
    }

    #[test]
    fn an_expired_entry_is_a_miss_and_is_overwritten_by_anything() {
        let c = temp_cache("expired", u64::MAX);
        let stored_at = 1_000_000;
        assert!(c
            .put(key(1), entry("glm-5.3", 90, stored_at), stored_at)
            .unwrap());

        // One second short of seven days is still a hit.
        let inside = stored_at + 7 * DAY - 1;
        assert!(c.get(key(1), None, inside).unwrap().is_some());

        // Seven days exactly is a miss, and a far less intelligent candidate
        // takes the slot.
        let outside = stored_at + 7 * DAY;
        assert!(c.get(key(1), None, outside).unwrap().is_none());
        assert!(c.put(key(1), entry("tiny", 1, outside), outside).unwrap());
        assert_eq!(
            c.get(key(1), None, outside).unwrap().unwrap().model_id,
            "tiny"
        );
    }

    #[test]
    fn a_named_model_is_served_only_its_own_answer() {
        let c = temp_cache("named", u64::MAX);
        let now = 10 * DAY;
        c.put(key(1), entry("glm-5.3", 50, now), now).unwrap();

        // auto, or no model at all, takes whatever is stored.
        assert!(c.get(key(1), None, now).unwrap().is_some());
        assert!(c.get(key(1), Some("glm-5.3"), now).unwrap().is_some());
        // A different model is a miss, not a wrong answer.
        assert!(c
            .get(key(1), Some("claude-fable-5-1"), now)
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_entry_on_an_old_schema_is_a_miss_and_is_overwritten() {
        let c = temp_cache("schema", u64::MAX);
        let now = 10 * DAY;
        let mut old = entry("glm-5.3", 90, now);
        old.schema_version = SCHEMA_VERSION.wrapping_sub(1);
        c.put(key(1), old, now).unwrap();
        assert!(c.get(key(1), None, now).unwrap().is_none());
        assert!(c.put(key(1), entry("tiny", 1, now), now).unwrap());
    }

    #[test]
    fn eviction_removes_the_oldest_tenth_by_created_at() {
        let c = temp_cache("evict", u64::MAX);
        let now = 100 * DAY;
        // Twenty entries, created_at ascending with the key byte.
        for i in 0..20u8 {
            c.put(key(i), entry("m", 1, now - 1_000 + u64::from(i)), now)
                .unwrap();
        }
        assert_eq!(c.entry_count().unwrap(), 20);

        assert_eq!(c.evict_oldest().unwrap(), 2);
        assert_eq!(c.entry_count().unwrap(), 18);
        // The two oldest went, the rest stayed.
        assert!(c.get(key(0), None, now).unwrap().is_none());
        assert!(c.get(key(1), None, now).unwrap().is_none());
        assert!(c.get(key(2), None, now).unwrap().is_some());
        assert!(c.get(key(19), None, now).unwrap().is_some());
    }

    #[test]
    fn eviction_on_a_small_table_still_makes_progress() {
        let c = temp_cache("evict-small", u64::MAX);
        let now = 100 * DAY;
        for i in 0..3u8 {
            c.put(key(i), entry("m", 1, now - 1_000 + u64::from(i)), now)
                .unwrap();
        }
        assert_eq!(c.evict_oldest().unwrap(), 1);
        assert_eq!(c.entry_count().unwrap(), 2);
        // An empty table evicts nothing rather than failing.
        let empty = temp_cache("evict-empty", u64::MAX);
        assert_eq!(empty.evict_oldest().unwrap(), 0);
    }

    #[test]
    fn a_write_over_the_cap_evicts_first_and_still_stores() {
        // max_bytes of zero puts every write over the cap.
        let c = temp_cache("cap", 0);
        let now = 100 * DAY;
        for i in 0..10u8 {
            c.put(key(i), entry("m", 1, now - 1_000 + u64::from(i)), now)
                .unwrap();
        }
        assert!(c.size_bytes().unwrap() > 0);
        // The newest write is present, and the cap has not emptied the store.
        assert!(c.get(key(9), None, now).unwrap().is_some());
        assert!(c.entry_count().unwrap() > 1);
    }

    /// A cancelled stream is half an answer. It must not land in the cache,
    /// and it must not displace a complete answer already there.
    #[test]
    fn a_cancelled_stream_is_never_written() {
        let c = temp_cache("stream-cancelled", u64::MAX);
        let now = 10 * DAY;

        // Nothing stored: a cancelled stream still writes nothing.
        assert!(!c
            .put_stream(key(1), entry("glm-5.3", 90, now), StreamEnd::Cancelled, now)
            .unwrap());
        assert!(c.get(key(1), None, now).unwrap().is_none());

        // A complete stream from a weaker model takes the empty slot.
        assert!(c
            .put_stream(key(1), entry("tiny", 1, now), StreamEnd::Complete, now)
            .unwrap());

        // A cancelled stream from a far smarter model still loses, because it
        // has no complete body to offer.
        assert!(!c
            .put_stream(
                key(1),
                entry("claude-fable-5-1", 90, now),
                StreamEnd::Cancelled,
                now
            )
            .unwrap());
        assert_eq!(c.get(key(1), None, now).unwrap().unwrap().model_id, "tiny");
    }

    /// A complete stream is an ordinary candidate: same transaction, same
    /// intelligence comparison, no streamed exception.
    #[test]
    fn a_complete_stream_goes_through_the_same_write_rule() {
        let c = temp_cache("stream-complete", u64::MAX);
        let now = 10 * DAY;

        assert!(c
            .put_stream(key(1), entry("glm-5.3", 50, now), StreamEnd::Complete, now)
            .unwrap());

        // Lower loses, equal keeps what is stored, higher overwrites.
        assert!(!c
            .put_stream(key(1), entry("tiny", 10, now), StreamEnd::Complete, now)
            .unwrap());
        assert!(!c
            .put_stream(key(1), entry("other", 50, now), StreamEnd::Complete, now)
            .unwrap());
        assert!(c
            .put_stream(
                key(1),
                entry("claude-fable-5-1", 90, now),
                StreamEnd::Complete,
                now
            )
            .unwrap());

        let stored = c.get(key(1), None, now).unwrap().unwrap();
        assert_eq!(stored.model_id, "claude-fable-5-1");
        assert_eq!(stored.intelligence, 90);
    }

    /// An expired entry is overwritten by a complete stream, and left alone by
    /// a cancelled one.
    #[test]
    fn a_cancelled_stream_does_not_even_clear_an_expired_entry() {
        let c = temp_cache("stream-expired", u64::MAX);
        let stored_at = 1_000_000;
        c.put(key(1), entry("glm-5.3", 90, stored_at), stored_at)
            .unwrap();
        let outside = stored_at + 7 * DAY;

        assert!(!c
            .put_stream(
                key(1),
                entry("tiny", 1, outside),
                StreamEnd::Cancelled,
                outside
            )
            .unwrap());
        assert_eq!(c.entry_count().unwrap(), 1);
        assert_eq!(
            c.get(key(1), None, stored_at).unwrap().unwrap().model_id,
            "glm-5.3"
        );

        // The same candidate, this time complete, takes the expired slot.
        assert!(c
            .put_stream(
                key(1),
                entry("tiny", 1, outside),
                StreamEnd::Complete,
                outside
            )
            .unwrap());
        assert_eq!(
            c.get(key(1), None, outside).unwrap().unwrap().model_id,
            "tiny"
        );
    }

    /// redb keeps its file at the high-water mark after a delete, so a cap
    /// measured by a bare `stat` stays tripped forever. Eviction has to wait
    /// for the file to grow again, or every later write pays a full scan and
    /// clears another tenth until the store is empty.
    #[test]
    fn being_over_the_cap_evicts_once_not_on_every_later_write() {
        let c = temp_cache("cap-once", u64::MAX);
        let now = 100 * DAY;
        for i in 0..20u8 {
            c.put(key(i), entry("m", 1, now - 1_000 + u64::from(i)), now)
                .unwrap();
        }
        assert_eq!(c.entry_count().unwrap(), 20);

        // The same file, now with a cap every write is over.
        let over = Cache {
            db: Arc::clone(&c.cache.db),
            ttl: c.ttl,
            max_bytes: 0,
            path: c.path.clone(),
            evicted_at: AtomicU64::new(0),
        };
        // The first write frees space: the oldest tenth goes, the entry lands.
        assert!(over.put(key(100), entry("m", 1, now), now).unwrap());
        assert_eq!(over.entry_count().unwrap(), 19);

        // Overwriting one key ten times does not grow the file, so nothing
        // more is evicted. Without the fix the store collapses to one entry.
        for i in 2..12u16 {
            over.put(key(100), entry("m", i, now), now).unwrap();
        }
        assert!(
            over.entry_count().unwrap() >= 15,
            "eviction repeated on writes that did not grow the file"
        );
    }
}
