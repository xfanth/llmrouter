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
use std::time::Duration;

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
    pub keys: HashMap<String, Vec<String>>,
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
        Ok(Self {
            router: read_yaml(&dir.join("router.yml"))?.unwrap_or_default(),
            providers: read_list(&dir.join("providers.yml"))?,
            models: read_list(&dir.join("models.yml"))?,
            intelligence: read_list(&dir.join("intelligence.yml"))?,
            keys: read_text(&dir.join(".env"))?
                .as_deref()
                .map(parse_env_file)
                .unwrap_or_default(),
            capabilities: read_capabilities(&dir.join("model_capabilities.csv"))?,
        })
    }

    /// What the capabilities file knows about one provider's model. `None`
    /// when the file does not carry the row, which DESIGN.md treats as "take
    /// the provider's word for it and assume tool support".
    pub fn capability(&self, provider: &str, model_id: &str) -> Option<&Capability> {
        self.capabilities
            .get(&(provider.to_lowercase(), model_id.to_lowercase()))
    }

    /// The keys for one provider, resolved through its `keys_env` name.
    pub fn keys_for(&self, provider: &Provider) -> &[String] {
        self.keys
            .get(&provider.keys_env)
            .map_or(&[][..], Vec::as_slice)
    }

    /// The intelligence score and rank for a model id, matched through the
    /// normalising rules in DESIGN.md. An unranked model scores 0 and sorts to
    /// the bottom.
    pub fn rank_of(&self, provider: &str, model_id: &str) -> (u16, u32) {
        // An explicit `ranks_as` in models.yml replaces the model id outright.
        // That is the hand correction for anything the automatic pass gets
        // wrong.
        let target = self
            .models
            .iter()
            .find(|m| m.provider == provider && m.model_id == model_id)
            .and_then(|m| m.ranks_as.as_deref())
            .map_or_else(|| normalise_model_name(model_id), normalise_model_name);

        // ponytail: linear scan over roughly 650 rows per lookup. Build a map
        // keyed on the normalised slug if catalogue build time ever shows up.
        if let Some(entry) = self
            .intelligence
            .iter()
            .find(|e| normalise_model_name(&e.slug) == target)
        {
            return (entry.intelligence, entry.intelligence_rank);
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
            .map_or((0, u32::MAX), |(_, e)| {
                (e.intelligence, e.intelligence_rank)
            })
    }
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
    /// N: concurrent channel calls to different channels.
    pub race_width: usize,
    /// M: hedged copies of one channel call.
    pub max_per_channel: usize,
    /// 0 means the M copies fire with the first one.
    pub hedge_delay_ms: u64,
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
            race_width: 32,
            max_per_channel: 2,
            hedge_delay_ms: 0,
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

/// `router.yml` -> `health_poll`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct HealthPollConfig {
    /// Per probe, in either wave and in the poller.
    #[serde(with = "humantime_serde")]
    pub probe_timeout: Duration,
    /// Default base. A `Retry-After` header overrides it per failure.
    #[serde(with = "humantime_serde")]
    pub retry_base: Duration,
    /// The ceiling on `retry_base * 2^(counter - 1)`.
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
    /// Per network test call, both of which run beside every client request.
    #[serde(with = "humantime_serde")]
    pub network_test_timeout: Duration,
}

impl Default for HealthPollConfig {
    fn default() -> Self {
        Self {
            probe_timeout: Duration::from_secs(5),
            retry_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(3600),
            network_test_timeout: Duration::from_secs(2),
        }
    }
}

/// `router.yml` -> `cache`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Capability {
    /// Context window in tokens.
    pub limit_context: Option<u32>,
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
    limit_context: Option<u32>,
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
                tool_call: row.tool_call.map(|n| n != 0),
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
pub fn parse_env_file(contents: &str) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        // Quotes are shell syntax, not part of a key. The commas inside them
        // still separate keys.
        let value = value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(value);
        let values = value
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .collect();
        out.insert(name.to_string(), values);
    }
    out
}

/// Normalises a model name for matching: lowercase, trailing date stripped,
/// dots, dashes, underscores and slashes flattened away.
pub fn normalise_model_name(name: &str) -> String {
    let mut text = name.trim().to_ascii_lowercase();
    // The trailing date goes first. Once the separators are gone there is no
    // separator left to recognise it by.
    if text.len() > 9 && text.is_char_boundary(text.len() - 9) {
        let (head, tail) = text.split_at(text.len() - 9);
        let dated = matches!(tail.as_bytes()[0], b'-' | b'_' | b'@')
            && tail[1..].bytes().all(|b| b.is_ascii_digit());
        if dated {
            text = head.to_string();
        }
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
fn read_list<T: serde::de::DeserializeOwned>(path: &Path) -> crate::Result<Vec<T>> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Shape<T> {
        Bare(Vec<T>),
        Wrapped(HashMap<String, Vec<T>>),
    }
    Ok(match read_yaml::<Shape<T>>(path)? {
        None => Vec::new(),
        Some(Shape::Bare(items)) => items,
        Some(Shape::Wrapped(map)) => map.into_values().flatten().collect(),
    })
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

    fn entry(slug: &str, intelligence: u16, rank: u32) -> IntelligenceEntry {
        IntelligenceEntry {
            slug: slug.to_string(),
            intelligence,
            intelligence_rank: rank,
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
        let dir = std::env::temp_dir().join(format!("llmrouter-caps-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");

        assert!(Config::load(&dir)
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

        let cfg = Config::load(&dir).expect("the csv parses");
        assert_eq!(
            cfg.capability("gemini", "gemini-3.7-flash"),
            Some(&Capability {
                limit_context: Some(1_048_576),
                tool_call: Some(false),
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

        std::fs::remove_dir_all(&dir).ok();
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
             GEMINI_KEYS=aaa,bbb , ccc\n\
             export OPENROUTER_KEYS=\"ddd,eee\"\n\
             QUOTED='fff'\n\
             EMPTY=\n\
             TRAILING=ggg,,\n\
             no_equals_sign\n",
        );
        assert_eq!(parsed["GEMINI_KEYS"], vec!["aaa", "bbb", "ccc"]);
        assert_eq!(parsed["OPENROUTER_KEYS"], vec!["ddd", "eee"]);
        assert_eq!(parsed["QUOTED"], vec!["fff"]);
        assert!(parsed["EMPTY"].is_empty());
        assert_eq!(parsed["TRAILING"], vec!["ggg"]);
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
    }

    #[test]
    fn rank_matches_exactly_after_normalising() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(
            cfg.rank_of("anthropic", "claude-fable-5-1-20260901"),
            (53, 1)
        );
    }

    #[test]
    fn rank_matches_a_vendor_prefixed_id_loosely() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(
            cfg.rank_of("openrouter", "anthropic/claude-fable-5.1"),
            (53, 1)
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
        assert_eq!(cfg.rank_of("gemini", "gemini-3.7-flash"), (40, 10));
        assert_eq!(cfg.rank_of("gemini", "gemini-3.7-flash-lite"), (30, 20));
    }

    #[test]
    fn rank_prefers_the_longest_loose_match() {
        let cfg = config_with(
            vec![entry("flash", 10, 50), entry("gemini-3.7-flash", 40, 10)],
            Vec::new(),
        );
        assert_eq!(cfg.rank_of("gemini", "google/gemini-3.7-flash"), (40, 10));
    }

    #[test]
    fn ranks_as_override_wins_and_is_scoped_to_its_provider() {
        let cfg = config_with(
            vec![entry("claude-fable-5-1", 53, 1), entry("tiny", 5, 99)],
            vec![override_for("zai", "glm-5", Some("claude-fable-5-1"))],
        );
        assert_eq!(cfg.rank_of("zai", "glm-5"), (53, 1));
        // Same model id at another provider gets no override, so it is unranked.
        assert_eq!(cfg.rank_of("gemini", "glm-5"), (0, u32::MAX));
    }

    #[test]
    fn unranked_model_sorts_to_the_bottom() {
        let cfg = config_with(vec![entry("claude-fable-5-1", 53, 1)], Vec::new());
        assert_eq!(cfg.rank_of("pollinations", "mystery-model"), (0, u32::MAX));
    }

    #[test]
    fn keys_for_is_empty_when_the_variable_is_absent() {
        let mut cfg = config_with(Vec::new(), Vec::new());
        cfg.keys
            .insert("GEMINI_KEYS".to_string(), vec!["aaa".to_string()]);
        let provider = |keys_env: &str| Provider {
            name: "gemini".to_string(),
            wire: WireName::OpenAi,
            base_url: "https://example.invalid".to_string(),
            keys_env: keys_env.to_string(),
            models_path: default_models_path(),
        };
        assert_eq!(cfg.keys_for(&provider("GEMINI_KEYS")), ["aaa"]);
        assert!(cfg.keys_for(&provider("MISSING_KEYS")).is_empty());
    }

    #[test]
    fn router_defaults_match_the_design() {
        let d = RouterConfig::default();
        assert_eq!(d.race_width, 32);
        assert_eq!(d.max_per_channel, 2);
        assert_eq!(d.request_timeout, Duration::from_secs(3600));
        assert_eq!(d.memory_high_water, 3 * 1_024 * 1_024 * 1_024);
        assert_eq!(d.cache.max_bytes, 100_000_000_000);
        assert_eq!(d.cache.ttl, Duration::from_secs(168 * 3600));
        assert_eq!(d.health_poll.backoff_max, Duration::from_secs(3600));
    }

    #[test]
    fn router_yml_from_the_design_parses() {
        let yaml = "\
race_width: 32
max_per_channel: 2
hedge_delay_ms: 0
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
  backoff_max: 1h
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
        assert_eq!(cfg.health_poll.backoff_max, Duration::from_secs(3600));
        // An empty file still boots on the defaults.
        let empty: RouterConfig = serde_yaml::from_str("{}").expect("empty router.yml");
        assert_eq!(empty.race_width, 32);
    }

    /// `hedge_strategy` and `hedge_decision_window` were knobs the race never
    /// read: there is no streaming in v1, so there was nothing to compare by
    /// tokens per second. A stale one left in `router.yml` must stop the boot
    /// rather than quietly change nothing.
    #[test]
    fn a_setting_the_router_does_not_read_is_rejected() {
        for yaml in [
            "hedge_strategy: tps\n",
            "hedge_decision_window: 2s\n",
            "race_wdith: 32\n",
        ] {
            assert!(
                serde_yaml::from_str::<RouterConfig>(yaml).is_err(),
                "{yaml} should not load silently"
            );
        }
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
        let dir = std::env::temp_dir().join(format!("llmrouter-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");

        // Nothing on disk: defaults, and no panic.
        let bare = Config::load(&dir).expect("empty dir loads");
        assert_eq!(bare.router.race_width, 32);
        assert!(bare.providers.is_empty());
        assert_eq!(bare.rank_of("gemini", "anything"), (0, u32::MAX));

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

        let cfg = Config::load(&dir).expect("full dir loads");
        assert_eq!(cfg.router.race_width, 8);
        assert_eq!(cfg.router.cache.ttl, Duration::from_secs(30 * 24 * 3600));
        // An unset field in router.yml still falls back to its default.
        assert_eq!(cfg.router.max_per_channel, 2);
        assert_eq!(cfg.providers.len(), 1);
        assert_eq!(cfg.keys_for(&cfg.providers[0]).len(), 2);
        assert_eq!(cfg.models[0].limit_context, Some(1_000_000));
        assert_eq!(cfg.rank_of("gemini", "gemini-3.7-flash"), (40, 10));

        std::fs::write(dir.join("router.yml"), "race_width: [not a number]\n").unwrap();
        assert!(
            Config::load(&dir).is_err(),
            "malformed router.yml is an error"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
