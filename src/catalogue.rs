//! The channel list.
//!
//! One [`Channel`] per provider/model/key tuple. The vector is sorted once,
//! by `intelligence_rank` ascending, and never sorted per client request.
//! Health fields are atomics on the row, so a failure mutates in place without
//! rebuilding anything. Only a refreshed model list rebuilds the vector, which
//! is a pointer swap on the `ArcSwap`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use arc_swap::ArcSwap;
use rand::seq::SliceRandom;

use crate::config::WireName;

/// A channel's identity: blake3 over provider, key and model, truncated to the
/// first 8 bytes. Deterministic, so it survives a rebuild and means the same
/// thing in a log a week later. Never a list index.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ChannelId(pub u64);

impl ChannelId {
    /// Hashes the tuple. The key is hashed but never stored or logged.
    pub fn new(provider: &str, key: &str, model_id: &str) -> Self {
        // A null separator keeps ("ab", "c") from hashing the same as
        // ("a", "bc"). The key goes through the hasher and nowhere else.
        let mut hasher = blake3::Hasher::new();
        hasher.update(provider.as_bytes());
        hasher.update(b"\x00");
        hasher.update(key.as_bytes());
        hasher.update(b"\x00");
        hasher.update(model_id.as_bytes());
        let digest = hasher.finalize();
        let first_eight: [u8; 8] = digest.as_bytes()[..8].try_into().expect("8 of 32 bytes");
        Self(u64::from_be_bytes(first_eight))
    }
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// One provider/model/key tuple. Everything the filter needs is on the row, so
/// a query is one pass over the vector with no lookups.
#[derive(Debug)]
pub struct Channel {
    /// Hash of provider, key and model.
    pub id: ChannelId,
    /// Provider name, as written in `providers.yml`.
    pub provider: Arc<str>,
    /// The provider's own model id, which is what its API expects.
    pub model_id: Arc<str>,
    /// Stable label for the key, for logs and metrics. Never the key itself.
    pub key_id: Arc<str>,
    /// The key used on the wire. Never logged, never returned in a response:
    /// `Debug` prints `[REDACTED]`, and reading it takes `expose_secret()`.
    pub key: secrecy::SecretString,
    /// Root of the provider's API, with no trailing slash.
    pub base_url: Arc<str>,
    /// Which wire format this channel speaks.
    pub wire: WireName,

    /// Largest prompt this model accepts, in tokens.
    pub limit_context: u32,
    /// Largest completion this model produces, in tokens.
    pub limit_output: u32,
    /// Whether the model supports tool calls. Claude Code always sends tools.
    pub tool_call: bool,
    /// Whether the model is a reasoning model.
    pub reasoning: bool,
    /// Whether the model accepts structured output requests.
    pub structured_output: bool,
    /// Input modalities the model accepts, for example `text`, `image`.
    pub modalities_input: Vec<String>,

    /// The general intelligence score. Higher is better.
    pub intelligence: u16,
    /// Precomputed rank. Lower is better; the vector sorts on this.
    pub intelligence_rank: u32,
    /// The provider's own release timestamp in unix seconds, when it
    /// published one on its `/models` row. `None` when it did not.
    pub created: Option<u64>,

    /// Mutated in place by live traffic and the poller.
    pub health: ChannelHealth,
    /// Mutated in place by live traffic.
    pub telemetry: ChannelTelemetry,
}

/// Per-channel health, mutated in place through atomics so a failure never
/// rebuilds the vector.
#[derive(Debug, Default)]
pub struct ChannelHealth {
    /// False hides the row from every query.
    pub enabled: AtomicBool,
    /// Consecutive failures. 0 means healthy.
    pub retry_counter: AtomicU32,
    /// Seconds. 1 by default; a `Retry-After` header replaces it.
    pub retry_base_secs: AtomicU64,
    /// Absolute unix second from which this channel is eligible again, from
    /// this channel's own failures alone. A deadline already past means
    /// eligible now.
    ///
    /// The three deadline fields are kept apart rather than folded into one
    /// because each has exactly one writer: the channel's own trials write
    /// this one, `escalate_failure` writes the pair's and the provider's. A
    /// single shared field made each writer able to erase the other two, and
    /// it did: a channel failing with no `Retry-After` stored its one second
    /// base over a ten minute provider hold and went straight back into a
    /// provider already handed to the poller as dead.
    pub eligible_at: AtomicU64,
    /// The deadline the provider/key pair above this row is serving, 0 when
    /// the pair is healthy. Written for every row on the pair at once, so a
    /// dead key holds its channels back without waiting for a rebuild.
    pub pair_eligible_at: AtomicU64,
    /// The deadline the provider above this row is serving, 0 when the
    /// provider is healthy. Written for every row on the provider at once.
    pub provider_eligible_at: AtomicU64,
    /// Channel calls currently open against this channel, hedged copies
    /// included. Shared behind an `Arc` so that a catalogue rebuild can hand
    /// the same counter to the new row: requests already running hold the old
    /// snapshot, and splitting the count across two counters would make the
    /// self-inflicted `429` rule read zero and back off a busy channel.
    pub in_flight: Arc<AtomicU32>,
}

impl ChannelHealth {
    /// The second this row is next callable: the latest deadline any of the
    /// three levels is serving. Every eligibility test goes through here, so
    /// a level that is down cannot be missed by a reader that only knew about
    /// one of the fields.
    pub fn not_before(&self) -> u64 {
        self.eligible_at
            .load(Ordering::Relaxed)
            .max(self.pair_eligible_at.load(Ordering::Relaxed))
            .max(self.provider_eligible_at.load(Ordering::Relaxed))
    }

    /// A fresh row that may be called straight away. `Default` leaves
    /// `enabled` false and `retry_base_secs` 0, neither of which is what a
    /// newly built channel wants, so build rows through this.
    pub fn live() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            retry_counter: AtomicU32::new(0),
            retry_base_secs: AtomicU64::new(1),
            eligible_at: AtomicU64::new(0),
            pair_eligible_at: AtomicU64::new(0),
            provider_eligible_at: AtomicU64::new(0),
            in_flight: Arc::new(AtomicU32::new(0)),
        }
    }
}

/// Per-channel counters, for `/metrics` and `/healthz`.
#[derive(Debug, Default)]
pub struct ChannelTelemetry {
    /// Channel calls made.
    pub calls: AtomicU64,
    /// Calls that returned 2xx with non-empty content.
    pub successes: AtomicU64,
    /// Calls that did not.
    pub failures: AtomicU64,
    /// Wall clock of the most recent completed call.
    pub last_latency_ms: AtomicU64,
}

impl Channel {
    /// True when this row may be called right now: enabled, and its backoff
    /// deadline has passed.
    pub fn eligible(&self, now_unix: u64) -> bool {
        self.health.enabled.load(Ordering::Relaxed) && self.health.not_before() <= now_unix
    }

    /// True when this row satisfies the client request's requirements, as of
    /// `now_unix`. The clock is an argument rather than a field on the filter
    /// so that every query sees the eligibility picture at the moment it runs:
    /// a request can walk for minutes, and channels recover while it does.
    pub fn matches(&self, filter: &ChannelFilter, now_unix: u64) -> bool {
        // Integer compares first, atomics last: the cheap checks reject most
        // rows on a long scan.
        if self.limit_context < filter.min_context {
            return false;
        }
        if filter.needs_tools && !self.tool_call {
            return false;
        }
        if let Some(wanted) = &filter.named_model {
            // Never substitute a different model: the caller asked for a
            // specific thing. Case is the only slack allowed here.
            if !self.model_id.eq_ignore_ascii_case(wanted) {
                return false;
            }
        }
        self.eligible(now_unix)
    }
}

/// What one client request needs from a channel. Built once per client request
/// and reused for every query that request makes.
#[derive(Debug, Clone)]
pub struct ChannelFilter {
    /// Prompt tokens plus requested output, with a margin.
    pub min_context: u32,
    /// True when the client request carries tool definitions.
    pub needs_tools: bool,
    /// Set when the caller named a model. `auto` and no model both mean `None`.
    pub named_model: Option<String>,
}

/// The sorted channel list, swapped as a whole when a model list refreshes.
#[derive(Debug, Default)]
pub struct Catalogue {
    channels: ArcSwap<Vec<Channel>>,
    /// Held for writing across a whole rebuild, from reading the health table
    /// to swapping the new rows in, and for reading by every health move that
    /// mirrors itself onto a row. Without it a move landing mid-rebuild goes
    /// onto a row about to be orphaned, and the new row keeps the stale value.
    rows: RwLock<()>,
}

impl Catalogue {
    /// An empty catalogue. The router serves from the first second with zero
    /// channels; the probes fill it in the background.
    pub fn new() -> Self {
        Self {
            channels: ArcSwap::from_pointee(Vec::new()),
            rows: RwLock::new(()),
        }
    }

    /// Blocks a rebuild for as long as it is held. A health move takes this,
    /// then loads the snapshot and writes its row, so the row it writes is
    /// the one the next query scans and not one a rebuild is replacing.
    pub fn hold_rows(&self) -> RwLockReadGuard<'_, ()> {
        self.rows.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Builds the rows and swaps them in with every health move shut out, so
    /// nothing written to the health table after `build` read it is lost.
    pub fn replace_with(&self, build: impl FnOnce() -> Vec<Channel>) {
        let _rebuilding = self.rows.write().unwrap_or_else(|e| e.into_inner());
        self.store(build());
    }

    /// The snapshot one client request holds for its whole life. Cloning the
    /// `Arc` is the only cost.
    pub fn snapshot(&self) -> Arc<Vec<Channel>> {
        self.channels.load_full()
    }

    /// Sorts by `intelligence_rank` ascending, shuffling within each rank
    /// group, then swaps the pointer. Called by the startup waves and the
    /// poller, never per client request.
    pub fn replace(&self, channels: Vec<Channel>) {
        self.replace_with(|| channels);
    }

    fn store(&self, mut channels: Vec<Channel>) {
        // Carry each channel's in-flight count over from the row it replaces.
        // Calls already running hold the previous snapshot and keep counting
        // on the old row, so a fresh counter would hide them from the
        // self-inflicted `429` rule.
        let previous = self.channels.load();
        if !previous.is_empty() {
            let held: HashMap<ChannelId, &Arc<AtomicU32>> = previous
                .iter()
                .map(|c| (c.id, &c.health.in_flight))
                .collect();
            for channel in &mut channels {
                if let Some(counter) = held.get(&channel.id) {
                    channel.health.in_flight = Arc::clone(counter);
                }
            }
        }

        // Many channels share a rank: the same model on several providers, or
        // the same provider and model on several keys. Shuffle first, then
        // sort: `sort_by_key` is stable, so equal ranks keep the shuffled
        // order and ties break at random. No latency tiebreak: measuring
        // latency means calling the channels first, which is the thing being
        // chosen.
        channels.shuffle(&mut rand::thread_rng());
        channels.sort_by_key(|c| c.intelligence_rank);
        self.channels.store(Arc::new(channels));
    }

    /// Live channel count, for `/healthz`.
    pub fn len(&self) -> usize {
        self.channels.load().len()
    }

    /// True when no channel exists yet, which is the first second of a cold
    /// start.
    pub fn is_empty(&self) -> bool {
        self.channels.load().is_empty()
    }
}

/// Scans from the top of the sorted vector and returns the first `limit` rows
/// that pass `filter` and are not in `excluded`. `limit` is N for the opening
/// query of a client request and 1 for every replacement.
///
/// No `ORDER BY` is needed: the vector is already sorted, so the first match
/// found is the highest-ranked one available.
pub fn query<'a>(
    channels: &'a [Channel],
    filter: &ChannelFilter,
    excluded: &HashSet<ChannelId>,
    limit: usize,
    now_unix: u64,
) -> Vec<&'a Channel> {
    let mut found = Vec::new();
    if limit == 0 {
        return found;
    }
    found.reserve(limit.min(channels.len()));
    for channel in channels {
        // The excluded lookup comes first: tried channels cluster at the top
        // of the scan, so skipping one costs a single hash lookup.
        if excluded.contains(&channel.id) || !channel.matches(filter, now_unix) {
            continue;
        }
        found.push(channel);
        if found.len() == limit {
            break;
        }
    }
    found
}

/// The largest `limit_context` on any row, whatever its health. The `413` body
/// names it so the caller knows how far over the prompt is.
pub fn largest_context(channels: &[Channel]) -> u32 {
    channels.iter().map(|c| c.limit_context).max().unwrap_or(0)
}

/// Rough token count for a prompt. Characters divided by 3.5 is accurate
/// enough; exact tokenisation is not worth the dependency.
pub fn estimate_tokens(characters: usize) -> u32 {
    // characters / 3.5 is characters * 2 / 7, kept in integers and rounded up.
    let tokens = (characters as u64).saturating_mul(2).div_ceil(7);
    u32::try_from(tokens).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row with sensible defaults. Every field the filter reads is an
    /// argument; the rest never varies in these tests.
    fn channel(provider: &str, model: &str, key: &str, rank: u32) -> Channel {
        Channel {
            id: ChannelId::new(provider, key, model),
            provider: provider.into(),
            model_id: model.into(),
            key_id: "key-0".into(),
            key: key.into(),
            base_url: "https://example.invalid/v1".into(),
            wire: WireName::OpenAi,
            limit_context: 128_000,
            limit_output: 8_192,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_string()],
            intelligence: 1_000u16.saturating_sub(rank as u16),
            intelligence_rank: rank,
            created: None,
            health: ChannelHealth::live(),
            telemetry: ChannelTelemetry::default(),
        }
    }

    fn filter() -> ChannelFilter {
        ChannelFilter {
            min_context: 1_000,
            needs_tools: true,
            named_model: None,
        }
    }

    /// The clock the filter tests use. Any fixed value does.
    const NOW: u64 = 1_000_000;

    #[test]
    fn debug_never_prints_the_key() {
        let printed = format!("{:?}", channel("p", "m", "sk-live-do-not-print", 1));
        assert!(!printed.contains("do-not-print"), "{printed}");
        assert!(printed.contains("REDACTED"), "{printed}");
    }

    #[test]
    fn id_is_deterministic_and_separates_the_parts() {
        let a = ChannelId::new("groq", "sk-1", "llama-4");
        assert_eq!(a, ChannelId::new("groq", "sk-1", "llama-4"));
        assert_ne!(a, ChannelId::new("groq", "sk-2", "llama-4"));
        assert_ne!(a, ChannelId::new("groq", "sk-1", "llama-5"));
        // Without a separator these two tuples would hash identically.
        assert_ne!(
            ChannelId::new("ab", "c", "d"),
            ChannelId::new("a", "bc", "d")
        );
        assert_eq!(format!("{a}").len(), 16);
    }

    #[test]
    fn eligibility_follows_enabled_and_the_backoff_deadline() {
        let c = channel("groq", "llama-4", "sk-1", 1);
        assert!(c.eligible(1_000_000));

        // A deadline in the future hides the row, a deadline already past
        // does not.
        c.health.eligible_at.store(1_000_001, Ordering::Relaxed);
        assert!(!c.eligible(1_000_000));
        assert!(c.eligible(1_000_001));

        c.health.eligible_at.store(0, Ordering::Relaxed);
        c.health.enabled.store(false, Ordering::Relaxed);
        assert!(!c.eligible(1_000_000));
    }

    #[test]
    fn filter_checks_context_tools_and_the_named_model() {
        let c = channel("groq", "llama-4", "sk-1", 1);
        assert!(c.matches(&filter(), NOW));

        // A prompt larger than the window rejects the row.
        let mut big = filter();
        big.min_context = 200_000;
        assert!(!c.matches(&big, NOW));
        // Exactly the window still fits.
        big.min_context = 128_000;
        assert!(c.matches(&big, NOW));

        // Tools required, model has none.
        let no_tools = Channel {
            tool_call: false,
            ..channel("groq", "llama-4", "sk-2", 1)
        };
        assert!(!no_tools.matches(&filter(), NOW));
        let mut toolless = filter();
        toolless.needs_tools = false;
        assert!(no_tools.matches(&toolless, NOW));

        // A named model never matches a different one, whatever its rank.
        let mut named = filter();
        named.named_model = Some("LLAMA-4".to_string());
        assert!(c.matches(&named, NOW));
        named.named_model = Some("llama-5".to_string());
        assert!(!c.matches(&named, NOW));
    }

    #[test]
    fn query_returns_the_top_rows_skipping_excluded_ones() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![
            channel("a", "m1", "k", 1),
            channel("b", "m2", "k", 2),
            channel("c", "m3", "k", 3),
        ]);
        let rows = catalogue.snapshot();

        let mut excluded = HashSet::new();
        let opening = query(&rows, &filter(), &excluded, 2, NOW);
        assert_eq!(opening.len(), 2);
        assert_eq!(opening[0].provider.as_ref(), "a");
        assert_eq!(opening[1].provider.as_ref(), "b");

        // A replacement query scans from the top again and skips what has
        // already been tried.
        for c in &opening {
            excluded.insert(c.id);
        }
        let replacement = query(&rows, &filter(), &excluded, 1, NOW);
        assert_eq!(replacement.len(), 1);
        assert_eq!(replacement[0].provider.as_ref(), "c");

        // Exhausted: everything tried, so nothing comes back. That is the
        // 504 path.
        excluded.insert(replacement[0].id);
        assert!(query(&rows, &filter(), &excluded, 1, NOW).is_empty());
    }

    #[test]
    fn query_asked_for_more_than_exists_returns_what_there_is() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel("a", "m1", "k", 1)]);
        let rows = catalogue.snapshot();
        assert_eq!(query(&rows, &filter(), &HashSet::new(), 32, NOW).len(), 1);
        assert!(query(&rows, &filter(), &HashSet::new(), 0, NOW).is_empty());
    }

    #[test]
    fn a_channel_in_backoff_drops_out_without_a_rebuild() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel("a", "m1", "k", 1), channel("b", "m2", "k", 2)]);
        let rows = catalogue.snapshot();
        rows[0]
            .health
            .eligible_at
            .store(NOW + 60, Ordering::Relaxed);

        let found = query(&rows, &filter(), &HashSet::new(), 2, NOW);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].provider.as_ref(), "b");
        // The row is still in the vector, only hidden from the query.
        assert_eq!(catalogue.len(), 2);
    }

    #[test]
    fn a_channel_that_recovers_mid_walk_is_offered_again() {
        // One client request can walk for minutes. The clock belongs to the
        // query, not to the filter, or a channel whose backoff expires during
        // the walk would stay hidden for the rest of the request.
        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel("a", "m1", "k", 1), channel("b", "m2", "k", 2)]);
        let rows = catalogue.snapshot();
        rows[0]
            .health
            .eligible_at
            .store(NOW + 60, Ordering::Relaxed);

        let filter = filter();
        let mut excluded = HashSet::new();

        // Opening query: the best channel is in backoff, so the walk takes
        // the second one and fails it.
        let opening = query(&rows, &filter, &excluded, 1, NOW);
        assert_eq!(opening.len(), 1);
        assert_eq!(opening[0].provider.as_ref(), "b");
        excluded.insert(opening[0].id);

        // Still inside the backoff: nothing left to try.
        assert!(query(&rows, &filter, &excluded, 1, NOW + 59).is_empty());

        // The backoff has now expired. The same filter, the same snapshot,
        // and the top channel is back.
        let recovered = query(&rows, &filter, &excluded, 1, NOW + 60);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].provider.as_ref(), "a");
    }

    #[test]
    fn cold_start_serves_an_empty_catalogue() {
        let catalogue = Catalogue::new();
        assert!(catalogue.is_empty());
        assert_eq!(catalogue.len(), 0);
        let rows = catalogue.snapshot();
        assert!(query(&rows, &filter(), &HashSet::new(), 32, NOW).is_empty());
        assert_eq!(largest_context(&rows), 0);
    }

    #[test]
    fn replace_sorts_by_rank_and_shuffles_ties() {
        // Ranks a:1, b:2, c:2, d:2, e:3, the worked example in DESIGN.md.
        let build = || {
            vec![
                channel("e", "m", "k", 3),
                channel("c", "m", "k", 2),
                channel("a", "m", "k", 1),
                channel("d", "m", "k", 2),
                channel("b", "m", "k", 2),
            ]
        };
        let catalogue = Catalogue::new();

        let mut orders = std::collections::HashSet::new();
        for _ in 0..200 {
            catalogue.replace(build());
            let rows = catalogue.snapshot();
            let ranks: Vec<u32> = rows.iter().map(|c| c.intelligence_rank).collect();
            assert_eq!(ranks, vec![1, 2, 2, 2, 3]);
            assert_eq!(rows[0].provider.as_ref(), "a");
            assert_eq!(rows[4].provider.as_ref(), "e");
            let middle: Vec<String> = rows[1..4].iter().map(|c| c.provider.to_string()).collect();
            orders.insert(middle);
        }
        // Three tied rows have six orderings and every one must be reachable:
        // a tie-break that only shuffles part of the rank group would show
        // fewer. Over 200 rebuilds the odds of missing one are negligible.
        assert_eq!(orders.len(), 6, "tied ranks were not shuffled freely");
    }

    #[test]
    fn replace_swaps_the_pointer_and_an_in_flight_snapshot_survives() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel("a", "m1", "k", 1)]);
        let old = catalogue.snapshot();

        catalogue.replace(vec![channel("b", "m2", "k", 1), channel("c", "m3", "k", 2)]);
        // A query already running finishes against the vector it started on.
        assert_eq!(old.len(), 1);
        assert_eq!(old[0].provider.as_ref(), "a");
        assert_eq!(catalogue.len(), 2);
    }

    #[test]
    fn a_rebuild_keeps_the_in_flight_count_of_a_channel_that_survives() {
        let catalogue = Catalogue::new();
        catalogue.replace(vec![channel("a", "m1", "k", 1)]);
        let old = catalogue.snapshot();

        // Twenty calls are open against the row the running requests hold.
        old[0].health.in_flight.fetch_add(20, Ordering::Relaxed);

        // The poller sees a changed model list and swaps a whole new vector
        // in. The same channel's counter must still read twenty, or the next
        // `429` looks like a real limit and the channel backs off.
        catalogue.replace(vec![channel("a", "m1", "k", 1), channel("b", "m2", "k", 2)]);
        let fresh = catalogue.snapshot();
        assert_eq!(fresh[0].health.in_flight.load(Ordering::Relaxed), 20);

        // Still one counter, not two: a call that ends against the old row
        // decrements what the new row reads.
        old[0].health.in_flight.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(fresh[0].health.in_flight.load(Ordering::Relaxed), 19);

        // A channel that did not exist before starts at zero.
        assert_eq!(fresh[1].health.in_flight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn largest_context_ignores_health() {
        let disabled = Channel {
            limit_context: 1_000_000,
            ..channel("a", "m1", "k", 1)
        };
        disabled.health.enabled.store(false, Ordering::Relaxed);
        let rows = vec![disabled, channel("b", "m2", "k", 2)];
        // The 413 body names the largest window found on any row, whatever
        // its health, so the caller knows how far over the prompt is.
        assert_eq!(largest_context(&rows), 1_000_000);
    }

    #[test]
    fn token_estimate_rounds_up() {
        assert_eq!(estimate_tokens(0), 0);
        assert_eq!(estimate_tokens(1), 1);
        assert_eq!(estimate_tokens(7), 2);
        assert_eq!(estimate_tokens(8), 3);
        assert_eq!(estimate_tokens(3_500), 1_000);
        // A 3.8MB prompt, the largest the design costs out.
        assert_eq!(estimate_tokens(3_800_000), 1_085_715);
        // Saturates rather than wrapping on an absurd prompt.
        assert_eq!(estimate_tokens(usize::MAX), u32::MAX);
    }
}
