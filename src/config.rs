//! Config loading: `router.yml`, `providers.yml`, `models.yml`,
//! `intelligence.yml`, `model_capabilities.csv` and `.env`.
//!
//! Every struct carries serde defaults matching the values written in
//! DESIGN.md, so an empty file still boots. Durations parse through
//! `humantime_serde` (`30d`, `3600s`, `5m`). Byte sizes parse through
//! [`parse_byte_size`], which accepts both a bare integer (`131072`) and a
//! suffixed one (`100GB`, `3GB`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use regex::Regex;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, Serialize};

/// Everything the router reads off disk at boot. Read once; a change needs a
/// restart.
#[derive(Debug, Clone)]
pub struct Config {
    /// `router.yml`: N, M, timeouts, health, cache.
    pub router: RouterConfig,
    /// `providers.yml`: one entry per provider.
    pub providers: Vec<Provider>,
    /// `models.yml`: per-model overrides, keyed by provider and model id.
    pub models: Vec<ModelOverride>,
    /// `intelligence.yml`: the ranking, built from `models.csv`.
    pub intelligence: Vec<IntelligenceEntry>,
    /// `.env`: the environment variable name from `keys_env` to its keys.
    /// `Debug` prints each key as `[REDACTED]`.
    pub keys: HashMap<String, Vec<SecretString>>,
    /// `model_capabilities.csv`: what each provider's models can do, keyed by
    /// lowercased `(provider, model_id)`. The catalogue joins every probed
    /// model against this to learn its context window and whether it accepts
    /// tools. A snapshot that goes stale, so it is never the list of what
    /// exists: only detail about what the provider says exists.
    pub capabilities: HashMap<(String, String), Capability>,
}

impl Config {
    /// Reads all six files from `dir`. A missing optional file falls back to
    /// its defaults; a malformed one is an error.
    pub fn load(dir: &Path) -> crate::Result<Self> {
        let config = Self {
            router: read_yaml(&dir.join("router.yml"))?.unwrap_or_default(),
            providers: read_list(&dir.join("providers.yml"))?,
            models: read_list(&dir.join("models.yml"))?,
            intelligence: read_list(&dir.join("intelligence.yml"))?,
            keys: read_text(&dir.join(".env"))?
                .as_deref()
                .map(parse_env_file)
                .unwrap_or_default(),
            capabilities: read_capabilities(&dir.join("model_capabilities.csv"))?,
        };
        // A provider whose `keys_env` names no `.env` variable gets no keyed
        // probe and so no channels, with nothing else to say why. That is a
        // typo or a missed `.env` line, so it fails the boot. A variable that
        // is present but empty is a deliberate keyless entry: warn and go on.
        for provider in &config.providers {
            match config.keys.get(&provider.keys_env) {
                None => {
                    return Err(crate::RouterError::Config(format!(
                        "providers.yml: provider {} names keys_env {}, which .env does not set",
                        provider.name, provider.keys_env
                    )));
                }
                Some(keys) if keys.is_empty() => tracing::warn!(
                    provider = %provider.name,
                    keys_env = %provider.keys_env,
                    "provider has no keys in .env, so it serves no channels"
                ),
                Some(_) => {}
            }
        }
        Ok(config)
    }

    /// What the capabilities file knows about one provider's model. `None`
    /// when the file does not carry the row, which DESIGN.md treats as "take
    /// the provider's word for it and assume tool support".
    pub fn capability(&self, provider: &str, model_id: &str) -> Option<&Capability> {
        self.capabilities
            .get(&(provider.to_lowercase(), model_id.to_lowercase()))
    }

    /// The keys for one provider, resolved through its `keys_env` name.
    pub fn keys_for(&self, provider: &Provider) -> &[SecretString] {
        self.keys
            .get(&provider.keys_env)
            .map_or(&[][..], Vec::as_slice)
    }

    /// The intelligence score and rank for a model id, matched through the
    /// normalising rules in DESIGN.md, plus which pass found it. An unranked
    /// model scores 0 and sorts to the bottom. The caller logs the loose ones,
    /// so a wrong pairing is visible rather than silent.
    pub fn rank_of(&self, provider: &str, model_id: &str) -> (u16, u32, RankMatch) {
        self.ranking_of(provider, model_id)
            .map_or((0, u32::MAX, RankMatch::None), |(e, matched)| {
                (e.intelligence, e.intelligence_rank, matched)
            })
    }

    /// The `intelligence.yml` row a model id matches, and which pass found
    /// it. `rank_of` is the score-only view of this.
    pub fn ranking_of(
        &self,
        provider: &str,
        model_id: &str,
    ) -> Option<(&IntelligenceEntry, RankMatch)> {
        // An explicit `ranks_as` in models.yml replaces the model id outright.
        // That is the hand correction for anything the automatic pass gets
        // wrong.
        let raw = self
            .models
            .iter()
            .find(|m| m.provider == provider && m.model_id == model_id)
            .and_then(|m| m.ranks_as.as_deref())
            .unwrap_or(model_id);
        let target = normalise_model_name(raw);

        // A vendor prefix is not a loose match. `mistralai/mistral-large` is
        // the model `mistral-large` with its vendor written in front, so the
        // part after the last slash gets the same exact pass as the whole id.
        // Without this, every prefixed id fell through to the suffix pass and
        // asked an operator to check a pairing that was never in doubt.
        let bare = raw
            .rsplit_once('/')
            .map(|(_, tail)| normalise_model_name(tail))
            .filter(|t| !t.is_empty() && *t != target);

        // ponytail: linear scan over roughly 650 rows per lookup. Build a map
        // keyed on the normalised slug if catalogue build time ever shows up.
        if let Some(entry) = self.intelligence.iter().find(|e| {
            let slug = normalise_model_name(&e.slug);
            slug == target || bare.as_deref().is_some_and(|b| slug == b)
        }) {
            return Some((entry, RankMatch::Exact));
        }

        // Loose pass, for ids carrying a vendor prefix the slug does not:
        // openrouter's `anthropic/claude-fable-5.1` against the slug
        // `claude-fable-5-1`. Suffix match only, longest slug wins, so
        // `gemini-3.7-flash` cannot pick up the `gemini-3.7-flash-lite` row and
        // vice versa.
        self.intelligence
            .iter()
            .filter_map(|e| {
                let slug = normalise_model_name(&e.slug);
                (!slug.is_empty() && target.ends_with(&slug)).then_some((slug.len(), e))
            })
            .max_by_key(|(len, _)| *len)
            .map(|(_, e)| (e, RankMatch::Loose(e.slug.clone())))
    }
}

/// Which pass in `Config::rank_of` produced the score it returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RankMatch {
    /// The normalised slug matched the normalised model id outright.
    Exact,
    /// Matched only on a suffix, carrying the ranking slug that won. These
    /// are the ones DESIGN.md asks to be logged at startup: `coder` matching
    /// `qwen3-coder-plus` is plausible and wrong.
    Loose(String),
    /// No ranking row matched, so the score is 0 and the rank is the bottom.
    None,
}

/// `router.yml`.
///
/// Unknown keys are an error rather than a shrug: a key the router does not
/// read is a setting an operator believes is doing something.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RouterConfig {
    /// Address to bind. `0.0.0.0:8080`; network security sits elsewhere.
    pub listen: SocketAddr,
    /// N at the start of a walk.
    pub race_width: usize,
    /// The most N may ever reach. The walk escalates one channel at a time,
    /// one `race_escalation_ms` apart, until it stops climbing.
    pub race_width_max: usize,
    /// How often an in-progress walk adds one more channel, up to
    /// `race_width_max`. 0 disables the escalation: N stays at `race_width`
    /// for the whole walk.
    pub race_escalation_ms: u64,
    /// M: hedged copies of one channel call.
    pub max_per_channel: usize,
    /// 0 means the M copies fire with the first one.
    pub hedge_delay_ms: u64,
    /// Stream from the providers. This is about the upstream calls, not the
    /// client: `stream` in the client's request is a separate thing and a
    /// mismatch is fine. Off gives the buffered path in **Core loop**.
    pub streaming: bool,
    /// Read so an older `router.yml` still loads, and otherwise ignored: the
    /// first stream to finish its whole answer wins. See DESIGN.md "Commit on
    /// completion".
    pub hedge_strategy: HedgeStrategy,
    /// Read so an older `router.yml` still loads, and otherwise ignored, for
    /// the same reason as `hedge_strategy`.
    pub hedge_window_ms: u64,
    /// No token for this long means the channel failed.
    #[serde(with = "humantime_serde")]
    pub stream_stall_timeout: Duration,
    /// Gap between keepalives, sent from accepting the client request until
    /// the first real byte of the answer reaches it.
    #[serde(with = "humantime_serde")]
    pub keepalive_interval: Duration,
    /// One channel call. The client request itself has no cap.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Handshake only, so a dead host fails fast.
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Above this many concurrent client requests, outbound bodies spill to
    /// disk instead of staying in RAM.
    pub spill_above_concurrent: usize,
    /// Where spilled outbound bodies go.
    pub spill_path: PathBuf,
    /// Stop admitting client requests above this RSS.
    #[serde(deserialize_with = "de_byte_size")]
    pub memory_high_water: u64,
    /// HTTP/2 receive buffer per request slot.
    #[serde(deserialize_with = "de_byte_size")]
    pub http2_stream_window: u64,
    /// HTTP/2 receive buffer shared across one connection.
    #[serde(deserialize_with = "de_byte_size")]
    pub http2_connection_window: u64,
    /// Probes, backoff and the network test.
    pub health_poll: HealthPollConfig,
    /// The response cache.
    pub cache: CacheConfig,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            race_width: 2,
            race_width_max: 32,
            race_escalation_ms: 1,
            max_per_channel: 2,
            hedge_delay_ms: 1,
            streaming: true,
            hedge_strategy: HedgeStrategy::default(),
            hedge_window_ms: 1000,
            stream_stall_timeout: Duration::from_secs(60),
            keepalive_interval: Duration::from_secs(15),
            request_timeout: Duration::from_secs(3600),
            connect_timeout: Duration::from_secs(5),
            spill_above_concurrent: 100,
            spill_path: PathBuf::from("/data/spill"),
            memory_high_water: 3 * 1024 * 1024 * 1024,
            http2_stream_window: 131_072,
            http2_connection_window: 1_048_576,
            health_poll: HealthPollConfig::default(),
            cache: CacheConfig::default(),
        }
    }
}

/// The values `hedge_strategy` accepts. Kept so an older `router.yml` loads;
/// neither changes the race any more, because the first stream to finish its
/// whole answer wins.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HedgeStrategy {
    /// Tokens per second across `hedge_window_ms`, highest wins. The default:
    /// it costs a second of buffering and can save seventeen minutes.
    #[default]
    Tps,
    /// Time to first token. The first channel to emit a token wins, and the
    /// router is committed to it from that moment.
    Ttft,
}

/// `router.yml` -> `health_poll`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthPollConfig {
    /// Per probe, in either wave and in the poller.
    #[serde(with = "humantime_serde")]
    pub probe_timeout: Duration,
    /// Default base, for a failure the router cannot put in a group: the
    /// poller's probes. A `Retry-After` header overrides it per failure.
    #[serde(with = "humantime_serde")]
    pub retry_base: Duration,
    /// The ceiling on `retry_base * 2^(counter - 1)`.
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
    /// Per network test call, both of which run beside every client request.
    #[serde(with = "humantime_serde")]
    pub network_test_timeout: Duration,
    /// One schedule per kind of failure a live call can have.
    pub backoff: BackoffGroups,
}

impl Default for HealthPollConfig {
    fn default() -> Self {
        Self {
            probe_timeout: Duration::from_secs(5),
            retry_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(24 * 3600),
            network_test_timeout: Duration::from_secs(2),
            backoff: BackoffGroups::default(),
        }
    }
}

/// The six kinds of failure a live channel call can have. Each one says
/// something different about how soon the channel is worth trying again, so
/// each backs off on its own schedule. Alex's spec, 22 Oct 2026.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureGroup {
    /// The key is bad, has no access, or is out of credit: `401`, `403` and
    /// `402`. Rarely fixes itself inside minutes. The other 4xx are the
    /// request's fault and bench nothing.
    Account,
    /// The provider does not serve this model: `404`.
    ModelMissing,
    /// The provider is overloaded for now: `429` and any 5xx.
    Busy,
    /// The call never got as far as an answer: no connection, a timeout
    /// (`408` included), or a socket that broke before the first token.
    Unreachable,
    /// The provider answered with something that is not an answer: no
    /// content, a body that will not parse, or an error frame in the stream.
    BadAnswer,
    /// The provider started answering and then stopped: the stream went
    /// quiet, closed early, or the socket broke part way through. The client
    /// already holds half an answer, so this is the costliest failure there
    /// is.
    DiedMidAnswer,
}

/// `router.yml` -> `health_poll` -> `backoff`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffGroups {
    /// See [`FailureGroup::Account`].
    pub account: Backoff,
    /// See [`FailureGroup::ModelMissing`].
    pub model_missing: Backoff,
    /// See [`FailureGroup::Busy`].
    pub busy: Backoff,
    /// See [`FailureGroup::Unreachable`].
    pub unreachable: Backoff,
    /// See [`FailureGroup::BadAnswer`].
    pub bad_answer: Backoff,
    /// See [`FailureGroup::DiedMidAnswer`].
    pub died_mid_answer: Backoff,
}

impl BackoffGroups {
    /// The schedule a failure in `group` backs off on.
    pub fn get(&self, group: FailureGroup) -> &Backoff {
        match group {
            FailureGroup::Account => &self.account,
            FailureGroup::ModelMissing => &self.model_missing,
            FailureGroup::Busy => &self.busy,
            FailureGroup::Unreachable => &self.unreachable,
            FailureGroup::BadAnswer => &self.bad_answer,
            FailureGroup::DiedMidAnswer => &self.died_mid_answer,
        }
    }
}

impl Default for BackoffGroups {
    fn default() -> Self {
        const MINUTE: u64 = 60;
        const HOUR: u64 = 3600;
        const DAY: u64 = 24 * HOUR;
        let schedule = |start: u64, max: u64| Backoff {
            start: Duration::from_secs(start),
            multiplier: 2.0,
            max: Duration::from_secs(max),
        };
        Self {
            account: schedule(HOUR, 30 * DAY),
            model_missing: schedule(6 * HOUR, 30 * DAY),
            busy: schedule(30, HOUR),
            unreachable: schedule(10, HOUR),
            bad_answer: schedule(MINUTE, DAY),
            died_mid_answer: schedule(5 * MINUTE, DAY),
        }
    }
}

/// One backoff schedule: `wait = min(max, start * multiplier^(counter - 1))`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Backoff {
    /// The first wait. A `Retry-After` header replaces it for that failure.
    #[serde(with = "humantime_serde")]
    pub start: Duration,
    /// What each further failure multiplies the wait by. Below 1 would shrink
    /// the wait as failures pile up, so it is floored at 1.
    pub multiplier: f64,
    /// The longest wait.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
}

/// `router.yml` -> `cache`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Off means every client request goes upstream.
    pub enabled: bool,
    /// The single redb file holding both the `responses` and `health` tables.
    pub path: PathBuf,
    /// Backstop. The ttl normally binds first.
    #[serde(deserialize_with = "de_byte_size")]
    pub max_bytes: u64,
    /// Cap on redb's own page cache. redb defaults to 1GiB, which on its own
    /// would push the process past `memory_high_water` and make the admission
    /// gate refuse every request, so it is always set explicitly.
    #[serde(deserialize_with = "de_byte_size")]
    pub page_cache_bytes: u64,
    /// Background flush of both tables.
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// Entry lifetime, checked on read. No background sweep.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: PathBuf::from("/data/cache.redb"),
            max_bytes: 100 * 1_000_000_000,
            page_cache_bytes: 256_000_000,
            flush_interval: Duration::from_secs(300),
            ttl: Duration::from_secs(168 * 3600),
        }
    }
}

/// One entry in `providers.yml`. It names its keys rather than holding them, so
/// the file is safe to share.
///
/// Unknown keys are an error for the same reason as in `RouterConfig`: a
/// mistyped field name is a setting the operator believes is doing something.
/// It also catches a `keys:` list pasted in from the generator's source file,
/// which would otherwise look accepted while the keys went nowhere.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    /// Short name used in logs, metrics and the channel id.
    pub name: String,
    /// Which wire format this provider speaks.
    pub wire: WireName,
    /// Root of the provider's API, with no trailing slash.
    pub base_url: String,
    /// Environment variable in `.env` holding this provider's comma-separated
    /// keys.
    pub keys_env: String,
    /// Path appended to `base_url` for the model list. Defaults to `/models`.
    #[serde(default = "default_models_path")]
    pub models_path: String,
    /// Model ids to fall back on when the provider publishes no model list.
    /// Used only when the probe gets a 404 or a 405 back, which is what a
    /// provider whose listing endpoint does not exist answers. An auth
    /// failure still fails, so a dead key never produces channels.
    ///
    /// Setting it is the mark that this provider has no listing endpoint, so
    /// set it on nothing else: a 404 is also what a base URL missing its
    /// version segment answers, and a list here would hide that.
    /// `llmrouter-make-config` writes it for Cloudflare Workers AI only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
}

/// The wire format a provider speaks on the way out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WireName {
    /// `POST /chat/completions`.
    #[serde(alias = "openai")]
    OpenAi,
    /// `POST /messages`.
    #[serde(alias = "anthropic")]
    Anthropic,
}

/// One entry in `models.yml`: a correction to what the catalogue worked out on
/// its own.
///
/// Unknown keys are an error. This file is hand written, and a misspelt
/// `limit_contex` that parses and applies nothing leaves the operator with a
/// correction that is silently inert.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOverride {
    /// Provider this override applies to.
    pub provider: String,
    /// The provider's own model id.
    pub model_id: String,
    /// Replaces the context window from `/models` or the capabilities file.
    #[serde(default)]
    pub limit_context: Option<u32>,
    /// Replaces the output limit.
    #[serde(default)]
    pub limit_output: Option<u32>,
    /// Replaces the tool-support flag.
    #[serde(default)]
    pub tool_call: Option<bool>,
    /// The Artificial Analysis slug this model ranks under, when the automatic
    /// match gets it wrong.
    #[serde(default)]
    pub ranks_as: Option<String>,
}

/// One row of `model_capabilities.csv`, narrowed to the fields the channel
/// filter runs on. Every field is optional because the file leaves cells
/// blank where it does not know.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Capability {
    /// Context window in tokens.
    pub limit_context: Option<u32>,
    /// Maximum tokens the model will emit in one answer.
    pub limit_output: Option<u32>,
    /// Whether the model is a reasoning model.
    pub reasoning: Option<bool>,
    /// Whether the model can be held to a JSON schema.
    pub structured_output: Option<bool>,
    /// Which input modalities the model takes, lower case, e.g. `text`,
    /// `image`. Drives `supports_vision` on `GET /v1/models`.
    pub modalities_input: Option<Vec<String>>,
    /// Whether the model accepts a tools array. 146 of the 935 models on
    /// these providers do not, and a request from a coding tool always
    /// carries tools.
    pub tool_call: Option<bool>,
}

/// One raw row of the file. The file carries nine more columns; the csv
/// reader matches on the header name and drops the rest.
#[derive(Debug, Deserialize)]
struct CapabilityRow {
    provider: String,
    model_id: String,
    /// `1`, `0` or blank.
    tool_call: Option<u8>,
    /// `1`, `0` or blank.
    reasoning: Option<u8>,
    /// `1`, `0` or blank.
    structured_output: Option<u8>,
    limit_context: Option<u32>,
    limit_output: Option<u32>,
    /// A JSON array of strings, e.g. `["text","image"]`.
    modalities_input: Option<String>,
}

/// Reads `model_capabilities.csv`. A missing file is not an error: the join
/// then finds nothing and every model falls back to what its provider
/// published. A malformed row is skipped rather than failing the boot, since
/// the file is a 9,927 row snapshot from an external export.
fn read_capabilities(path: &Path) -> crate::Result<HashMap<(String, String), Capability>> {
    let mut reader = match csv::Reader::from_path(path) {
        Ok(reader) => reader,
        Err(e)
            if matches!(e.kind(), csv::ErrorKind::Io(io)
                if io.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(HashMap::new())
        }
        Err(e) => {
            return Err(crate::RouterError::Config(format!(
                "{}: {e}",
                path.display()
            )))
        }
    };
    let mut out = HashMap::new();
    for row in reader.deserialize::<CapabilityRow>().flatten() {
        out.insert(
            (row.provider.to_lowercase(), row.model_id.to_lowercase()),
            Capability {
                limit_context: row.limit_context,
                limit_output: row.limit_output,
                tool_call: row.tool_call.map(|n| n != 0),
                reasoning: row.reasoning.map(|n| n != 0),
                structured_output: row.structured_output.map(|n| n != 0),
                modalities_input: row
                    .modalities_input
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                    .map(|list| list.iter().map(|m| m.to_lowercase()).collect()),
            },
        );
    }
    Ok(out)
}

/// One row of `intelligence.yml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IntelligenceEntry {
    /// Artificial Analysis slug.
    pub slug: String,
    /// The general intelligence score. Higher is better.
    #[serde(deserialize_with = "de_score")]
    pub intelligence: u16,
    /// Precomputed rank. Lower is better; the channel list sorts on it.
    pub intelligence_rank: u32,
    /// `models.csv`'s `release_date`, `YYYY-MM-DD`. `GET /v1/models` reports
    /// it as `created` when the provider gives no date of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,
}

impl IntelligenceEntry {
    /// `release_date` as unix seconds at midnight UTC. `None` when absent or
    /// not a date.
    pub fn released_unix(&self) -> Option<u64> {
        let day = self.release_date.as_deref()?.trim();
        let when = humantime::parse_rfc3339(&format!("{day}T00:00:00Z")).ok()?;
        Some(when.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs())
    }
}

/// Parses a byte size: a bare integer, or an integer with a `KB`, `MB`, `GB`,
/// `TB`, `KiB`, `MiB`, `GiB` or `TiB` suffix. Decimal suffixes are powers of
/// 1,000 and binary ones powers of 1,024.
pub fn parse_byte_size(input: &str) -> std::result::Result<u64, String> {
    let text = input.trim();
    let digits = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    if digits == 0 {
        return Err(format!("byte size {input:?} does not start with a number"));
    }
    let value: u64 = text[..digits]
        .parse()
        .map_err(|_| format!("byte size {input:?} does not fit in 64 bits"))?;
    let multiplier = match text[digits..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1_u64,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "kib" => 1_024,
        "mib" => 1_024 * 1_024,
        "gib" => 1_024 * 1_024 * 1_024,
        "tib" => 1_024_u64 * 1_024 * 1_024 * 1_024,
        other => return Err(format!("byte size {input:?} has unknown suffix {other:?}")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte size {input:?} overflows 64 bits"))
}

/// Reads a `.env` file into a map of variable name to its comma-separated
/// values. Never logs a value.
/// `dotenvy` owns the shell syntax: `export `, quoting, escapes and inline
/// comments. All that is left here is the comma split. A malformed line is
/// skipped rather than failing the whole file, and the error itself is never
/// printed because it quotes the line, and the line holds key material. The
/// count is warned about instead, so a dropped variable leaves a trail.
pub fn parse_env_file(contents: &str) -> HashMap<String, Vec<SecretString>> {
    let mut dropped = 0_usize;
    let out = dotenvy::from_read_iter(contents.as_bytes())
        .filter_map(|entry| match entry {
            Ok((name, value)) => {
                let values = value
                    .split(',')
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(SecretString::from)
                    .collect();
                Some((name, values))
            }
            Err(_) => {
                dropped += 1;
                None
            }
        })
        .collect();
    if dropped > 0 {
        tracing::warn!(
            dropped,
            "ignored unparseable lines in .env; a value containing spaces must be quoted"
        );
    }
    out
}

/// Normalises a model name for matching: lowercase, trailing date stripped,
/// dots, dashes, underscores and slashes flattened away.
pub fn normalise_model_name(name: &str) -> String {
    let mut text = name.trim().to_ascii_lowercase();
    // OpenRouter tags a variant of the same model with a trailing `:free`,
    // `:beta`, `:thinking` and so on. The slug never carries it, so it goes
    // before anything else. Letters only: a size tag such as `qwen3:32b` names
    // a different model and keeps its place.
    let variant = text.rsplit_once(':').filter(|(stem, tag)| {
        !stem.is_empty() && !tag.is_empty() && tag.bytes().all(|b| b.is_ascii_lowercase())
    });
    if let Some(len) = variant.map(|(stem, _)| stem.len()) {
        text.truncate(len);
    }
    // The trailing date goes first. Once the separators are gone there is no
    // separator left to recognise it by. `.+?` keeps a bare date, which is
    // all there is, as the name rather than stripping it to nothing.
    static DATE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(.+?)[-_@](?:\d{8}|\d{4}-\d{2}-\d{2}|\d{4}_\d{2}_\d{2}|\d{4}\.\d{2}\.\d{2})$")
            .expect("the date pattern is a valid regex")
    });
    if let Some(stem) = DATE.captures(&text).map(|c| c[1].to_owned()) {
        text = stem;
    }
    text.retain(|c| !matches!(c, '.' | '-' | '_' | '/'));
    text
}

fn de_byte_size<'de, D: Deserializer<'de>>(de: D) -> std::result::Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Text(String),
    }
    match Raw::deserialize(de)? {
        Raw::Int(n) => Ok(n),
        Raw::Text(s) => parse_byte_size(&s).map_err(serde::de::Error::custom),
    }
}

/// Scores in `models.csv` are decimals such as `53.4`, so take a float and
/// round it rather than failing the whole file on the first row.
fn de_score<'de, D: Deserializer<'de>>(de: D) -> std::result::Result<u16, D::Error> {
    let raw = f64::deserialize(de)?;
    if !raw.is_finite() || raw < 0.0 {
        return Err(serde::de::Error::custom(format!(
            "intelligence score {raw} is not a positive number"
        )));
    }
    Ok(raw.round().min(f64::from(u16::MAX)) as u16)
}

/// Reads a file, returning `None` when it is simply absent.
fn read_text(path: &Path) -> crate::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(crate::RouterError::Config(format!(
            "{}: {e}",
            path.display()
        ))),
    }
}

/// Parses one optional YAML file. Missing is fine, malformed is not.
fn read_yaml<T: serde::de::DeserializeOwned>(path: &Path) -> crate::Result<Option<T>> {
    let Some(text) = read_text(path)? else {
        return Ok(None);
    };
    serde_yaml::from_str(&text)
        .map(Some)
        .map_err(|e| crate::RouterError::Config(format!("{}: {e}", path.display())))
}

/// Parses a YAML file holding a list, written either as a bare sequence or
/// wrapped under a single key (`providers:`, `models:`, `intelligence:`).
///
/// The wrapper is unwrapped as a value first and the sequence deserialised on
/// its own, rather than matching both shapes through an untagged enum. An
/// untagged enum reports only "data did not match any variant", which hides
/// which field in which entry was wrong.
fn read_list<T: serde::de::DeserializeOwned>(path: &Path) -> crate::Result<Vec<T>> {
    let Some(value) = read_yaml::<serde_yaml::Value>(path)? else {
        return Ok(Vec::new());
    };
    let items = match value {
        serde_yaml::Value::Null => return Ok(Vec::new()),
        serde_yaml::Value::Mapping(map) => serde_yaml::Value::Sequence(
            map.into_iter()
                .flat_map(|(_, v)| v.as_sequence().cloned().unwrap_or_else(|| vec![v]))
                .collect(),
        ),
        other => other,
    };
    serde_yaml::from_value(items)
        .map_err(|e| crate::RouterError::Config(format!("{}: {e}", path.display())))
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 8080))
}

fn default_models_path() -> String {
    "/models".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    fn entry(slug: &str, intelligence: u16, rank: u32) -> IntelligenceEntry {
        IntelligenceEntry {
            slug: slug.to_string(),
            intelligence,
            intelligence_rank: rank,
            release_date: None,
        }
    }

    fn config_with(intelligence: Vec<IntelligenceEntry>, models: Vec<ModelOverride>) -> Config {
        Config {
            router: RouterConfig::default(),
            providers: Vec::new(),
            models,
            intelligence,
            keys: HashMap::new(),
            capabilities: HashMap::new(),
        }
    }

    fn override_for(provider: &str, model_id: &str, ranks_as: Option<&str>) -> ModelOverride {
        ModelOverride {
            provider: provider.to_string(),
            model_id: model_id.to_string(),
            limit_context: None,
            limit_output: None,
            tool_call: None,
            ranks_as: ranks_as.map(str::to_string),
        }
    }

    #[test]
    fn capabilities_parse_and_a_missing_file_is_empty() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");
        let dir = tmp.path();

        assert!(Config::load(dir)
            .expect("a missing csv is fine")
            .capabilities
            .is_empty());

        std::fs::write(
            dir.join("model_capabilities.csv"),
            concat!(
                "provider,model_id,tool_call,reasoning,structured_output,",
                "limit_context,limit_output,modalities_input,family,open_weights\n",
                r#"302ai,MiniMax-M1,1,0,,1000000,128000,"[""text""]",minimax,0"#,
                "\n",
                r#"gemini,gemini-3.7-flash,0,0,,1048576,,"[""text""]",,0"#,
                "\n",
                "gemini,mystery,,,,,,,,\n",
            ),
        )
        .unwrap();

        let cfg = Config::load(dir).expect("the csv parses");
        assert_eq!(
            cfg.capability("gemini", "gemini-3.7-flash"),
            Some(&Capability {
                limit_context: Some(1_048_576),
                limit_output: None,
                tool_call: Some(false),
                reasoning: Some(false),
                structured_output: None,
                modalities_input: Some(vec!["text".to_string()]),
            })
        );
        // The lookup is case-insensitive on both halves of the key.
        assert_eq!(
            cfg.capability("302ai", "minimax-m1")
                .and_then(|c| c.tool_call),
            Some(true)
        );
        // A blank cell means "not known", not "no".
        assert_eq!(
            cfg.capability("gemini", "mystery"),
            Some(&Capability::default())
        );
        assert_eq!(cfg.capability("gemini", "never-heard-of-it"), None);
    }

    #[test]
    fn byte_size_accepts_bare_and_suffixed() {
        assert_eq!(parse_byte_size("131072"), Ok(131_072));
        assert_eq!(parse_byte_size("100GB"), Ok(100_000_000_000));
        assert_eq!(parse_byte_size("3GB"), Ok(3_000_000_000));
        assert_eq!(parse_byte_size("1048576"), Ok(1_048_576));
        assert_eq!(parse_byte_size(" 2 MiB "), Ok(2 * 1_024 * 1_024));
        assert_eq!(parse_byte_size("1tib"), Ok(1_024_u64.pow(4)));
        assert_eq!(parse_byte_size("512b"), Ok(512));
    }

    #[test]
    fn byte_size_rejects_rubbish() {
        for bad in ["", "GB", "1.5GB", "-1", "10 bananas", "1 KB KB"] {
            assert!(parse_byte_size(bad).is_err(), "{bad:?} should be rejected");
        }
        assert!(parse_byte_size("99999999999999999999GB").is_err());
        assert!(parse_byte_size("18446744073709551615TB").is_err());
    }

    #[test]
    fn env_file_splits_on_commas_and_skips_noise() {
        let parsed = parse_env_file(
            "# a comment\n\
             \n\
             GEMINI_KEYS=\"aaa,bbb , ccc\"\n\
             export OPENROUTER_KEYS=\"ddd,eee\"\n\
             QUOTED='fff'\n\
             EMPTY=\n\
             TRAILING=ggg,,\n\
             COMMENTED=hhh,iii  # second key is the billing account\n\
             IN_QUOTES=\"jjj,kkk#notacomment\"\n\
             no_equals_sign\n",
        );
        let exposed = |keys: &[SecretString]| -> Vec<String> {
            keys.iter().map(|k| k.expose_secret().to_string()).collect()
        };
        assert_eq!(exposed(&parsed["GEMINI_KEYS"]), vec!["aaa", "bbb", "ccc"]);
        assert_eq!(exposed(&parsed["OPENROUTER_KEYS"]), vec!["ddd", "eee"]);
        assert_eq!(exposed(&parsed["QUOTED"]), vec!["fff"]);
        assert!(parsed["EMPTY"].is_empty());
        assert_eq!(exposed(&parsed["TRAILING"]), vec!["ggg"]);
        // An inline comment is shell syntax, not key material.
        assert_eq!(exposed(&parsed["COMMENTED"]), vec!["hhh", "iii"]);
        // A `#` inside quotes is part of the value.
        assert_eq!(
            exposed(&parsed["IN_QUOTES"]),
            vec!["jjj", "kkk#notacomment"]
        );
        // A line that is not an assignment is skipped, not fatal.
        assert!(!parsed.contains_key("no_equals_sign"));
    }

    #[test]
    fn normalise_strips_date_and_separators() {
        assert_eq!(normalise_model_name("claude-fable-5-1"), "claudefable51");
        assert_eq!(
            normalise_model_name("claude-fable-5-1-20260901"),
            "claudefable51"
        );
        assert_eq!(
            normalise_model_name("anthropic/claude-fable-5.1"),
            "anthropicclaudefable51"
        );
        assert_eq!(normalise_model_name("GPT_5.5-High"), "gpt55high");
        // Eight digits that are part of the name, not a date, keep their place.
        assert_eq!(normalise_model_name("model-1234567"), "model1234567");
        // OpenAI dates its ids with dashes, as OpenRouter passes them on.
        assert_eq!(
            normalise_model_name("openai/gpt-4o-2024-11-20"),
            "openaigpt4o"
        );
        assert_eq!(normalise_model_name("gpt-4o-mini-2024-07-18"), "gpt4omini");
        // Underscored and dotted dates are dates too, not part of the name.
        assert_eq!(normalise_model_name("model-2024_10_22"), "model");
        assert_eq!(normalise_model_name("model-2024.10.22"), "model");
        // A bare date has nothing before it to keep, so it stays the name.
        assert_eq!(normalise_model_name("-20241022"), "20241022");
    }

    #[test]
    fn rank_matches_exactly_after_normalising() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(
            cfg.rank_of("anthropic", "claude-fable-5-1-20260901"),
            (53, 1, RankMatch::Exact)
        );
    }

    /// A vendor prefix is bookkeeping, not ambiguity. Stripping it leaves an
    /// exact slug, so the pairing is certain and must not ask an operator to
    /// check it.
    #[test]
    fn rank_matches_a_vendor_prefixed_id_exactly() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(
            cfg.rank_of("openrouter", "anthropic/claude-fable-5.1"),
            (53, 1, RankMatch::Exact)
        );
    }

    /// A free OpenRouter key can only call the `:free` variants, so they must
    /// rank the same as the model they are a variant of.
    #[test]
    fn rank_ignores_an_openrouter_variant_tag() {
        let cfg = config_with(
            vec![entry("nvidia/nemotron-3-ultra-550b-a55b", 60, 3)],
            Vec::new(),
        );
        assert_eq!(
            cfg.rank_of("openrouter", "nvidia/nemotron-3-ultra-550b-a55b:free"),
            (60, 3, RankMatch::Exact)
        );
        assert_eq!(normalise_model_name("model:beta"), "model");
        assert_eq!(normalise_model_name("qwen3:32b"), "qwen3:32b");
    }

    /// Stripping the vendor prefix must not drag a shorter row in: the bare
    /// name has to match a slug outright, the same as any other exact match.
    #[test]
    fn stripping_the_vendor_prefix_still_needs_a_whole_slug() {
        let cfg = config_with(vec![entry("flash", 10, 50)], Vec::new());
        assert_eq!(
            cfg.rank_of("gemini", "google/gemini-3.7-flash"),
            (10, 50, RankMatch::Loose("flash".to_string()))
        );
    }

    /// The silent wrong match DESIGN.md names: flash must not pick up the
    /// lite row, and lite must not pick up the flash row.
    #[test]
    fn rank_does_not_confuse_flash_with_flash_lite() {
        let cfg = config_with(
            vec![
                entry("gemini-3.7-flash", 40, 10),
                entry("gemini-3.7-flash-lite", 30, 20),
            ],
            Vec::new(),
        );
        assert_eq!(
            cfg.rank_of("gemini", "gemini-3.7-flash"),
            (40, 10, RankMatch::Exact)
        );
        assert_eq!(
            cfg.rank_of("gemini", "gemini-3.7-flash-lite"),
            (30, 20, RankMatch::Exact)
        );
    }

    /// The silent wrong pairing DESIGN.md names under "Open problems":
    /// `qwen3-max-coder` has no row of its own, so the loose pass hands it the
    /// score of the short slug `coder`. The score is wrong either way; what
    /// the caller needs is to be told the match was loose, so it can be logged
    /// rather than slipping through.
    #[test]
    fn a_loose_match_reports_the_slug_it_matched() {
        let cfg = config_with(vec![entry("coder", 12, 44)], Vec::new());
        assert_eq!(
            cfg.rank_of("qwen", "qwen3-max-coder"),
            (12, 44, RankMatch::Loose("coder".to_string()))
        );
    }

    #[test]
    fn rank_prefers_the_longest_loose_match() {
        let cfg = config_with(
            vec![entry("coder", 10, 50), entry("max-coder", 40, 10)],
            Vec::new(),
        );
        assert_eq!(
            cfg.rank_of("qwen", "qwen3-max-coder"),
            (40, 10, RankMatch::Loose("max-coder".to_string()))
        );
    }

    #[test]
    fn ranks_as_override_wins_and_is_scoped_to_its_provider() {
        let cfg = config_with(
            vec![entry("claude-fable-5-1", 53, 1), entry("tiny", 5, 99)],
            vec![override_for("zai", "glm-5", Some("claude-fable-5-1"))],
        );
        assert_eq!(cfg.rank_of("zai", "glm-5"), (53, 1, RankMatch::Exact));
        // Same model id at another provider gets no override, so it is unranked.
        assert_eq!(
            cfg.rank_of("gemini", "glm-5"),
            (0, u32::MAX, RankMatch::None)
        );
    }

    #[test]
    fn unranked_model_sorts_to_the_bottom() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(
            cfg.rank_of("pollinations", "mystery-model"),
            (0, u32::MAX, RankMatch::None)
        );
    }

    #[test]
    fn debug_never_prints_a_key_from_the_env_file() {
        let mut cfg = config_with(Vec::new(), Vec::new());
        cfg.keys = parse_env_file("GEMINI_KEYS=sk-live-do-not-print\n");
        let printed = format!("{cfg:?}");
        assert!(!printed.contains("do-not-print"), "{printed}");
        assert!(printed.contains("REDACTED"), "{printed}");
    }

    #[test]
    fn keys_for_is_empty_when_the_variable_is_absent() {
        let mut cfg = config_with(Vec::new(), Vec::new());
        cfg.keys
            .insert("GEMINI_KEYS".to_string(), vec!["aaa".into()]);
        let provider = |keys_env: &str| Provider {
            name: "gemini".to_string(),
            wire: WireName::OpenAi,
            base_url: "https://example.invalid".to_string(),
            keys_env: keys_env.to_string(),
            models_path: default_models_path(),
            models: Vec::new(),
        };
        assert_eq!(
            cfg.keys_for(&provider("GEMINI_KEYS"))[0].expose_secret(),
            "aaa"
        );
        assert!(cfg.keys_for(&provider("MISSING_KEYS")).is_empty());
    }

    #[test]
    fn router_defaults_match_the_design() {
        let d = RouterConfig::default();
        assert_eq!(d.race_width, 2);
        assert_eq!(d.race_width_max, 32);
        assert_eq!(d.race_escalation_ms, 1);
        assert_eq!(d.max_per_channel, 2);
        assert_eq!(d.request_timeout, Duration::from_secs(3600));
        assert_eq!(d.memory_high_water, 3 * 1_024 * 1_024 * 1_024);
        assert_eq!(d.cache.max_bytes, 100_000_000_000);
        assert_eq!(d.cache.ttl, Duration::from_secs(168 * 3600));
        // 24 hours: a channel that exhausts its quota today gets another
        // chance tomorrow, and from a 1 second base the cap is first
        // reached at failure 18, not 12.
        assert_eq!(d.health_poll.backoff_max, Duration::from_secs(24 * 3600));
        assert!(d.streaming);
        assert_eq!(d.hedge_strategy, HedgeStrategy::Tps);
        assert_eq!(d.hedge_window_ms, 1000);
        assert_eq!(d.stream_stall_timeout, Duration::from_secs(60));
        assert_eq!(d.keepalive_interval, Duration::from_secs(15));
    }

    #[test]
    fn router_yml_from_the_design_parses() {
        let yaml = "\
race_width: 2
race_width_max: 32
race_escalation_ms: 1
max_per_channel: 2
hedge_delay_ms: 1
streaming: true
hedge_strategy: tps
hedge_window_ms: 1000
stream_stall_timeout: 60s
keepalive_interval: 15s
request_timeout: 3600s
connect_timeout: 5s
spill_above_concurrent: 100
spill_path: /data/spill
memory_high_water: 3GB
http2_stream_window: 131072
http2_connection_window: 1048576
health_poll:
  probe_timeout: 5s
  retry_base: 1s
  backoff_max: 24h
  network_test_timeout: 2s
cache:
  enabled: true
  path: /data/cache.redb
  max_bytes: 100GB
  flush_interval: 5m
  ttl: 168h
";
        let cfg: RouterConfig = serde_yaml::from_str(yaml).expect("router.yml should parse");
        assert_eq!(cfg.memory_high_water, 3_000_000_000);
        assert_eq!(cfg.http2_stream_window, 131_072);
        assert_eq!(cfg.cache.max_bytes, 100_000_000_000);
        assert_eq!(cfg.cache.ttl, Duration::from_secs(168 * 3600));
        assert_eq!(cfg.health_poll.backoff_max, Duration::from_secs(24 * 3600));
        assert_eq!(cfg.health_poll.probe_timeout, Duration::from_secs(5));
        assert!(cfg.streaming);
        assert_eq!(cfg.hedge_strategy, HedgeStrategy::Tps);
        assert_eq!(cfg.hedge_window_ms, 1000);
        assert_eq!(cfg.stream_stall_timeout, Duration::from_secs(60));
        assert_eq!(cfg.keepalive_interval, Duration::from_secs(15));
        // An empty file still boots on the defaults.
        let empty: RouterConfig = serde_yaml::from_str("{}").expect("empty router.yml");
        assert_eq!(empty.race_width, 2);
        assert_eq!(empty.race_width_max, 32);
    }

    /// A key the router does not read is a setting an operator believes is
    /// doing something, so it stops the boot rather than quietly changing
    /// nothing. `hedge_decision_window` is a name an earlier draft used for
    /// what is now `hedge_window_ms`.
    #[test]
    fn a_setting_the_router_does_not_read_is_rejected() {
        for yaml in [
            "hedge_decision_window: 2s\n",
            "hedge_window: 1s\n",
            "race_wdith: 32\n",
        ] {
            assert!(
                serde_yaml::from_str::<RouterConfig>(yaml).is_err(),
                "{yaml} should not load silently"
            );
        }
    }

    #[test]
    fn hedge_strategy_takes_both_names_and_nothing_else() {
        let tps: RouterConfig = serde_yaml::from_str("hedge_strategy: tps\n").expect("tps");
        assert_eq!(tps.hedge_strategy, HedgeStrategy::Tps);
        let ttft: RouterConfig = serde_yaml::from_str("hedge_strategy: ttft\n").expect("ttft");
        assert_eq!(ttft.hedge_strategy, HedgeStrategy::Ttft);
        assert!(serde_yaml::from_str::<RouterConfig>("hedge_strategy: fastest\n").is_err());
    }

    /// From the 1 second default base the wait doubles every failure, so the
    /// 24 hour cap first binds at failure 18. An hourly cap would bind at 12
    /// and retry a dead channel 24 times a day.
    #[test]
    fn the_backoff_cap_is_reached_at_failure_18() {
        let cap = RouterConfig::default().health_poll.backoff_max.as_secs();
        let base = RouterConfig::default().health_poll.retry_base.as_secs();
        let wait = |counter: u32| crate::health::wait_seconds(counter, base, cap);
        assert!(wait(17) < cap);
        assert_eq!(wait(18), cap);
        assert_eq!(wait(12), 2048);
    }

    /// The six schedules Alex signed off on 22 Oct 2026, and one of them
    /// changed from `router.yml` while the rest keep their defaults.
    #[test]
    fn each_failure_group_has_its_own_schedule() {
        let d = BackoffGroups::default();
        let secs = |b: &Backoff| (b.start.as_secs(), b.max.as_secs());
        assert_eq!(secs(&d.account), (3600, 30 * 86_400));
        assert_eq!(secs(&d.model_missing), (6 * 3600, 30 * 86_400));
        assert_eq!(secs(&d.busy), (30, 3600));
        assert_eq!(secs(&d.unreachable), (10, 3600));
        assert_eq!(secs(&d.bad_answer), (60, 86_400));
        assert_eq!(secs(&d.died_mid_answer), (300, 86_400));

        let cfg: RouterConfig = serde_yaml::from_str(
            "health_poll:\n  backoff:\n    busy: {start: 5s, multiplier: 3, max: 10m}\n",
        )
        .expect("a group set from router.yml");
        let busy = cfg.health_poll.backoff.get(FailureGroup::Busy);
        assert_eq!(busy.start, Duration::from_secs(5));
        assert_eq!(busy.multiplier, 3.0);
        assert_eq!(busy.max, Duration::from_secs(600));
        assert_eq!(
            cfg.health_poll.backoff.died_mid_answer.start,
            Duration::from_secs(300)
        );
        // A schedule missing a field is a typo, not a default.
        assert!(serde_yaml::from_str::<RouterConfig>(
            "health_poll:\n  backoff:\n    busy: {start: 5s}\n"
        )
        .is_err());
    }

    #[test]
    fn durations_take_days_and_byte_sizes_take_both_shapes() {
        let cfg: CacheConfig =
            serde_yaml::from_str("ttl: 30d\nmax_bytes: 2048\n").expect("cache config");
        assert_eq!(cfg.ttl, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(cfg.max_bytes, 2048);
    }

    #[test]
    fn the_redb_page_cache_is_capped_well_under_the_memory_gate() {
        // redb's own default is 1GiB. Left at that it would take a third of the
        // 3GB gate on its own and the router would start refusing requests, so
        // the cap is carried in config and stays small against the gate.
        let d = CacheConfig::default();
        assert_eq!(d.page_cache_bytes, 256_000_000);
        assert!(d.page_cache_bytes * 4 < RouterConfig::default().memory_high_water);
        let cfg: CacheConfig =
            serde_yaml::from_str("page_cache_bytes: 64MB\n").expect("cache config");
        assert_eq!(cfg.page_cache_bytes, 64_000_000);
    }

    #[test]
    fn a_bad_byte_size_fails_the_file() {
        assert!(serde_yaml::from_str::<CacheConfig>("max_bytes: 100 bananas\n").is_err());
    }

    #[test]
    fn provider_wire_names_and_default_models_path() {
        let providers: Vec<Provider> = serde_yaml::from_str(
            "- name: gemini\n  wire: openai\n  base_url: https://example.invalid/v1beta/openai\n  keys_env: GEMINI_KEYS\n\
             - name: anth\n  wire: anthropic\n  base_url: https://example.invalid\n  keys_env: ANTH_KEYS\n  models_path: /v1/models\n",
        )
        .expect("providers.yml should parse");
        assert_eq!(providers[0].wire, WireName::OpenAi);
        assert_eq!(providers[0].models_path, "/models");
        assert_eq!(providers[1].wire, WireName::Anthropic);
        assert_eq!(providers[1].models_path, "/v1/models");
    }

    #[test]
    fn intelligence_takes_a_decimal_score() {
        let rows: Vec<IntelligenceEntry> = serde_yaml::from_str(
            "- slug: claude-fable-5-1\n  intelligence: 53.4\n  intelligence_rank: 1\n",
        )
        .expect("intelligence.yml should parse");
        assert_eq!(rows[0].intelligence, 53);
    }

    #[test]
    fn load_reads_every_file_and_tolerates_missing_ones() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");
        let dir = tmp.path();

        // Nothing on disk: defaults, and no panic.
        let bare = Config::load(dir).expect("empty dir loads");
        assert_eq!(bare.router.race_width, 2);
        assert_eq!(bare.router.race_width_max, 32);
        assert!(bare.providers.is_empty());
        assert_eq!(
            bare.rank_of("gemini", "anything"),
            (0, u32::MAX, RankMatch::None)
        );

        std::fs::write(
            dir.join("router.yml"),
            "race_width: 8\ncache:\n  ttl: 30d\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("providers.yml"),
            "providers:\n  - name: gemini\n    wire: openai\n    base_url: https://example.invalid\n    keys_env: GEMINI_KEYS\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("models.yml"),
            "models:\n  - provider: gemini\n    model_id: gemini-3.7-flash\n    limit_context: 1000000\n    ranks_as: gemini-3.7-flash\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("intelligence.yml"),
            "intelligence:\n  - slug: gemini-3.7-flash\n    intelligence: 40\n    intelligence_rank: 10\n",
        )
        .unwrap();
        std::fs::write(dir.join(".env"), "GEMINI_KEYS=aaa,bbb\n").unwrap();

        let cfg = Config::load(dir).expect("full dir loads");
        assert_eq!(cfg.router.race_width, 8);
        assert_eq!(cfg.router.cache.ttl, Duration::from_secs(30 * 24 * 3600));
        // An unset field in router.yml still falls back to its default.
        assert_eq!(cfg.router.max_per_channel, 2);
        assert_eq!(cfg.providers.len(), 1);
        assert_eq!(cfg.keys_for(&cfg.providers[0]).len(), 2);
        assert_eq!(cfg.models[0].limit_context, Some(1_000_000));
        assert_eq!(
            cfg.rank_of("gemini", "gemini-3.7-flash"),
            (40, 10, RankMatch::Exact)
        );

        std::fs::write(dir.join("router.yml"), "race_width: [not a number]\n").unwrap();
        assert!(
            Config::load(dir).is_err(),
            "malformed router.yml is an error"
        );
    }

    /// A typo inside `health_poll:` or `cache:` is an error, same as a typo at
    /// the top level. `make-config` tells the operator router.yml is theirs to
    /// hand edit, so a misspelt key there must not be swallowed.
    #[test]
    fn a_misspelt_key_in_a_nested_router_yml_block_is_an_error() {
        let err = serde_yaml::from_str::<RouterConfig>("cache:\n  max_byte: 5GB\n")
            .expect_err("a misspelt cache key is rejected");
        assert!(err.to_string().contains("max_byte"), "{err}");

        let err = serde_yaml::from_str::<RouterConfig>("health_poll:\n  backoff_ma: 1h\n")
            .expect_err("a misspelt health_poll key is rejected");
        assert!(err.to_string().contains("backoff_ma"), "{err}");

        // The correctly spelt keys still load.
        let cfg: RouterConfig =
            serde_yaml::from_str("cache:\n  max_bytes: 5GB\nhealth_poll:\n  backoff_max: 1h\n")
                .expect("the real field names parse");
        assert_eq!(cfg.cache.max_bytes, 5_000_000_000);
        assert_eq!(cfg.health_poll.backoff_max, Duration::from_secs(3600));
    }

    /// `models.yml` is the operator's hand correction surface, and
    /// `providers.yml` is hand edited too. A misspelt field in either must
    /// fail the boot rather than load as an override that does nothing.
    #[test]
    fn a_misspelt_field_in_models_yml_or_providers_yml_is_an_error() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");
        let dir = tmp.path();

        std::fs::write(
            dir.join("models.yml"),
            "models:\n  - provider: zai\n    model_id: glm-5.3\n    limit_contex: 200000\n",
        )
        .unwrap();
        let err = Config::load(dir).expect_err("a misspelt models.yml field is rejected");
        assert!(err.to_string().contains("limit_contex"), "{err}");

        // The right spelling loads and carries the value.
        std::fs::write(
            dir.join("models.yml"),
            "models:\n  - provider: zai\n    model_id: glm-5.3\n    limit_context: 200000\n",
        )
        .unwrap();
        assert_eq!(
            Config::load(dir).expect("the real field name loads").models[0].limit_context,
            Some(200_000)
        );

        // A `keys:` list pasted in from the generator's source file is an
        // error, not a provider whose keys quietly went nowhere.
        std::fs::write(
            dir.join("providers.yml"),
            "providers:\n  - name: zai\n    wire: openai\n    base_url: https://example.invalid\n    keys_env: ZAI_KEYS\n    keys: []\n",
        )
        .unwrap();
        let err = Config::load(dir).expect_err("a stray providers.yml field is rejected");
        assert!(err.to_string().contains("keys"), "{err}");
    }

    /// A `keys_env` that names no `.env` variable used to load cleanly and
    /// then drop the provider from every race without a word. It fails the
    /// boot now, naming the provider and the variable. A variable that is
    /// present but empty is a keyless entry and still loads.
    #[test]
    fn a_keys_env_missing_from_dot_env_is_an_error() {
        let tmp = tempfile::tempdir().expect("a throwaway directory");
        let dir = tmp.path();
        std::fs::write(
            dir.join("providers.yml"),
            "providers:\n  - name: deepseek\n    wire: openai\n    base_url: https://example.invalid\n    keys_env: DEEPSEEK_KEY\n",
        )
        .unwrap();
        std::fs::write(dir.join(".env"), "DEEPSEEK_KEYS=aaa\n").unwrap();
        let err = Config::load(dir).expect_err("an unset keys_env is rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("deepseek") && msg.contains("DEEPSEEK_KEY,"),
            "{msg}"
        );

        std::fs::write(dir.join(".env"), "DEEPSEEK_KEY=\n").unwrap();
        let cfg = Config::load(dir).expect("an empty variable is a keyless provider");
        assert!(cfg.keys_for(&cfg.providers[0]).is_empty());
    }

    /// The deploy contract has to name every file `Config::load` reads. An
    /// operator who provisions `/config` by hand from DESIGN.md and leaves
    /// `model_capabilities.csv` behind gets a silently wider race: every
    /// channel falls back to its default context window and tool-call flag.
    #[test]
    fn design_md_names_every_config_file() {
        let design = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("DESIGN.md"),
        )
        .expect("DESIGN.md sits at the crate root");
        let volume_row = design
            .lines()
            .find(|line| line.contains("| `/config` | read-only |"))
            .expect("the volume table lists /config");
        let config_table = design
            .split_once("\n## Config\n")
            .expect("DESIGN.md has a Config section")
            .1;
        for file in [
            "router.yml",
            "providers.yml",
            "models.yml",
            "intelligence.yml",
            ".env",
            "model_capabilities.csv",
        ] {
            assert!(volume_row.contains(file), "volume table omits {file}");
            assert!(
                config_table.contains(&format!("| `{file}` |")),
                "the Config file table omits {file}"
            );
        }
    }
}
