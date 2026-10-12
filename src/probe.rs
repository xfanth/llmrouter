//! The two startup waves and the poller.
//!
//! Wave 1 is a keyless reachability probe of every provider: call `/models`
//! with no authorisation header. Any HTTP status back, `401` included, means
//! the host is alive. Only a connection error, a DNS failure or a timeout is a
//! fail.
//!
//! Wave 2 runs only against providers wave 1 proved healthy: call `/models`
//! with one key per provider/key pair. A 2xx gives that pair's model list, and
//! every model on it becomes a channel.
//!
//! Neither wave blocks startup. The router binds its port and serves with zero
//! channels from the first second.
//!
//! The poller touches only unhealthy things, each with its own backoff. A
//! channel is never polled: it rejoins the list when its backoff expires and
//! the next real request is the test.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::catalogue::{Catalogue, Channel, ChannelHealth, ChannelId};
use crate::config::{Config, Provider, WireName};
use crate::health::{parse_retry_after, HealthEntry, HealthKey, HealthTable};
use crate::upstream::Upstream;

/// How often the poller wakes. The shortest backoff is one second, so a one
/// second tick never delays an eligible thing by more than its own wait.
const POLL_TICK: Duration = Duration::from_secs(1);

/// Sent with every keyed probe of an Anthropic-format provider, which rejects
/// a request without it.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Used when neither `models.yml`, the capabilities file nor the provider's
/// own list publishes a
/// context window. Zero would hide the model from every query, which is worse
/// than a conservative guess for a provider that publishes nothing.
const DEFAULT_LIMIT_CONTEXT: u32 = 8_192;

/// Same rule for the output limit.
const DEFAULT_LIMIT_OUTPUT: u32 = 4_096;

/// Fields a provider might publish the context window under, in the order they
/// are tried. A dotted name walks into a nested object.
const CONTEXT_FIELDS: &[&str] = &[
    "context_length",
    "context_window",
    "max_context_length",
    "max_input_tokens",
    "inputTokenLimit",
    "limit.context",
    "top_provider.context_length",
];

/// Fields a provider might publish the output limit under.
const OUTPUT_FIELDS: &[&str] = &[
    "max_output_tokens",
    "max_completion_tokens",
    "outputTokenLimit",
    "limit.output",
    "top_provider.max_completion_tokens",
];

/// What one probe found.
#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    /// True when any HTTP status came back. Only a transport failure is false.
    /// This is the whole test for the keyless probe. The keyed probe needs a
    /// 2xx as well, which is `error.is_none()`.
    pub alive: bool,
    /// The model list, present only when a keyed probe returned 2xx.
    pub models: Vec<ProbedModel>,
    /// Seconds from a `Retry-After` header, when the provider sent one. It
    /// replaces the default backoff base on a failure.
    pub retry_after_secs: Option<u64>,
    /// What went wrong, for `/healthz`. Never carries key material.
    pub error: Option<String>,
}

impl ProbeOutcome {
    /// True when the probe both reached the host and got a 2xx out of it.
    pub fn succeeded(&self) -> bool {
        self.alive && self.error.is_none()
    }

    /// A probe that never reached the host.
    fn transport_failure(error: String) -> Self {
        Self {
            alive: false,
            models: Vec::new(),
            retry_after_secs: None,
            error: Some(error),
        }
    }
}

/// One model as the provider's `/models` endpoint described it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbedModel {
    /// The provider's own model id, which is what its API expects.
    pub id: String,
    /// The context window, when the provider publishes one. Otherwise the
    /// capabilities file supplies it.
    pub limit_context: Option<u32>,
    /// The output limit, when published.
    pub limit_output: Option<u32>,
}

/// Runs the waves and the poller.
#[derive(Debug)]
pub struct Prober {
    config: Arc<Config>,
    upstream: Upstream,
    catalogue: Arc<Catalogue>,
    health: Arc<HealthTable>,
    /// The most recent model list for each provider/key pair, keyed by
    /// provider name and key id. Holds no key material. A rebuild walks the
    /// config and joins against this, so one pair recovering does not drop
    /// every other pair's channels.
    seen: Mutex<HashMap<(String, String), Vec<ProbedModel>>>,
}

impl Prober {
    /// Builds a prober over the shared client, catalogue and health table.
    pub fn new(
        config: Arc<Config>,
        upstream: Upstream,
        catalogue: Arc<Catalogue>,
        health: Arc<HealthTable>,
    ) -> Self {
        Self {
            config,
            upstream,
            catalogue,
            health,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// The client this prober calls `/models` with.
    pub fn upstream(&self) -> &Upstream {
        &self.upstream
    }

    /// Wave 1 then wave 2, everything inside a wave at once. Swaps the
    /// catalogue when it finishes. Runs in the background; nothing waits on it.
    pub async fn run_startup_waves(&self) {
        // Wave 1: every provider at once, no key.
        let wave_one = futures::future::join_all(
            self.config
                .providers
                .iter()
                .map(|p| async move { (p, self.probe_reachable(p).await) }),
        )
        .await;

        let mut reachable = Vec::new();
        for (provider, outcome) in wave_one {
            let key = HealthKey::Provider(provider.name.clone());
            if outcome.alive {
                self.health.record_success(&key);
                reachable.push(provider);
            } else {
                tracing::warn!(
                    provider = %provider.name,
                    error = outcome.error.as_deref().unwrap_or("unreachable"),
                    "reachability probe failed"
                );
                self.health
                    .record_failure(&key, outcome.retry_after_secs, now_unix());
            }
        }

        // Wave 2: one probe per provider/key pair on the reachable providers,
        // all at once.
        let pairs: Vec<(&Provider, usize, &String)> = reachable
            .iter()
            .flat_map(|p| {
                self.config
                    .keys_for(p)
                    .iter()
                    .enumerate()
                    .map(move |(i, k)| (*p, i, k))
            })
            .collect();

        let wave_two = futures::future::join_all(
            pairs
                .into_iter()
                .map(|(p, i, k)| async move { (p, i, self.probe_models(p, k).await) }),
        )
        .await;

        for (provider, index, outcome) in wave_two {
            self.apply_keyed_outcome(provider, index, outcome);
        }

        self.rebuild();
        tracing::info!(channels = self.catalogue.len(), "startup waves finished");
    }

    /// Keyless reachability probe of one provider.
    pub async fn probe_reachable(&self, provider: &Provider) -> ProbeOutcome {
        self.fetch_models(provider, None).await
    }

    /// Keyed `/models` probe of one provider/key pair. A 2xx gives the model
    /// list; anything else marks the pair unhealthy.
    pub async fn probe_models(&self, provider: &Provider, key: &str) -> ProbeOutcome {
        self.fetch_models(provider, Some(key)).await
    }

    /// Turns probe results into channel rows: joins each model against the
    /// capabilities file and the ranking, then expands to provider/model/key
    /// tuples.
    pub fn build_channels(
        &self,
        provider: &Provider,
        key_id: &str,
        key: &str,
        models: &[ProbedModel],
    ) -> Vec<Channel> {
        let base_url: Arc<str> = Arc::from(provider.base_url.trim_end_matches('/'));
        let provider_name: Arc<str> = Arc::from(provider.name.as_str());
        let key_id: Arc<str> = Arc::from(key_id);
        let key: Arc<str> = Arc::from(key);

        models
            .iter()
            .map(|model| {
                // `models.yml` corrects whatever the provider said about
                // itself, so it is read first.
                let over = self
                    .config
                    .models
                    .iter()
                    .find(|o| o.provider == provider.name && o.model_id == model.id);

                // `model_capabilities.csv` is the primary source for the
                // context window and the only source for tool support. The
                // provider's own list is the fallback, per DESIGN.md step 3.
                let caps = self.config.capability(&provider.name, &model.id);

                let (intelligence, intelligence_rank) =
                    self.config.rank_of(&provider.name, &model.id);

                let id = ChannelId::new(&provider.name, &key, &model.id);

                // A channel that was already known to be dead keeps its
                // backoff across a rebuild and across a restart.
                let known = self
                    .health
                    .get(&HealthKey::Channel(id))
                    .unwrap_or_else(|| HealthEntry::new(self.health.retry_base_secs()));

                Channel {
                    id,
                    provider: provider_name.clone(),
                    model_id: Arc::from(model.id.as_str()),
                    key_id: key_id.clone(),
                    key: key.clone(),
                    base_url: base_url.clone(),
                    wire: provider.wire,
                    limit_context: over
                        .and_then(|o| o.limit_context)
                        .or_else(|| caps.and_then(|c| c.limit_context))
                        .or(model.limit_context)
                        .unwrap_or(DEFAULT_LIMIT_CONTEXT),
                    limit_output: over
                        .and_then(|o| o.limit_output)
                        .or(model.limit_output)
                        .unwrap_or(DEFAULT_LIMIT_OUTPUT),
                    // Assume tool support only for a model the capabilities
                    // file does not know. Where it does know, 16% of these
                    // models cannot take a tools array, and racing them is a
                    // guaranteed 4xx.
                    tool_call: over
                        .and_then(|o| o.tool_call)
                        .or_else(|| caps.and_then(|c| c.tool_call))
                        .unwrap_or(true),
                    reasoning: false,
                    structured_output: false,
                    modalities_input: vec!["text".to_string()],
                    intelligence,
                    intelligence_rank,
                    health: ChannelHealth {
                        enabled: AtomicBool::new(true),
                        retry_counter: AtomicU32::new(known.retry_counter),
                        retry_base_secs: AtomicU64::new(known.retry_base_secs),
                        eligible_at: AtomicU64::new(known.eligible_at),
                        // A rebuild carries the live count over from the row
                        // this one replaces, so starting at zero is right.
                        in_flight: Arc::new(AtomicU32::new(0)),
                    },
                    telemetry: Default::default(),
                }
            })
            .collect()
    }

    /// Polls only unhealthy things, each when its own backoff expires. A
    /// provider gets probe 1; a provider/key pair gets probe 2 and, on
    /// success, a refreshed model list and a rebuilt catalogue.
    pub async fn poll_loop(self: Arc<Self>, shutdown: tokio_util::sync::CancellationToken) {
        let mut ticker = tokio::time::interval(POLL_TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            self.poll_once().await;
        }
    }

    /// One pass of the poller. Returns true when the catalogue was rebuilt.
    ///
    /// The clock is read at each eligibility test and each failure, never once
    /// per pass: a pass over many providers can take longer than the shortest
    /// backoff, and a frozen clock would write deadlines into the past and
    /// re-probe a dead provider on every tick.
    async fn poll_once(&self) -> bool {
        let mut changed = false;

        for provider in &self.config.providers {
            let provider_key = HealthKey::Provider(provider.name.clone());
            let unhealthy = self
                .health
                .get(&provider_key)
                .is_some_and(|e| e.retry_counter > 0);

            if unhealthy {
                if !self.health.eligible(&provider_key, now_unix()) {
                    continue;
                }
                let outcome = self.probe_reachable(provider).await;
                if !outcome.alive {
                    self.health
                        .record_failure(&provider_key, outcome.retry_after_secs, now_unix());
                    continue;
                }
                // The host answered again, so every key on it is worth one
                // keyed probe whatever its own deadline says.
                self.health.record_success(&provider_key);
                changed |= self.poll_keys(provider, true).await;
            } else {
                changed |= self.poll_keys(provider, false).await;
            }
        }

        if changed {
            self.rebuild();
        }
        changed
    }

    /// Keyed probes for one provider. `force` probes every key regardless of
    /// its deadline, which is what a provider coming back calls for. Otherwise
    /// only a pair that is unhealthy and out of backoff, or one never probed
    /// at all, is touched. Returns true when a model list changed.
    async fn poll_keys(&self, provider: &Provider, force: bool) -> bool {
        let mut changed = false;

        for (index, key) in self.config.keys_for(provider).iter().enumerate() {
            let pair = HealthKey::ProviderKey {
                provider: provider.name.clone(),
                key_id: key_id(provider, index),
            };

            if !force {
                let entry = self.health.get(&pair);
                let unhealthy = entry.is_some_and(|e| e.retry_counter > 0);
                if unhealthy {
                    if !self.health.eligible(&pair, now_unix()) {
                        continue;
                    }
                } else if self.has_model_list(provider, index) {
                    // Healthy and already known. Live traffic tests it.
                    continue;
                }
            }

            let outcome = self.probe_models(provider, key).await;
            changed |= self.apply_keyed_outcome(provider, index, outcome);
        }

        changed
    }

    /// Records a keyed probe against the health table and, on a 2xx, stores
    /// the model list. Returns true when the stored list changed.
    fn apply_keyed_outcome(
        &self,
        provider: &Provider,
        index: usize,
        outcome: ProbeOutcome,
    ) -> bool {
        let key_id = key_id(provider, index);
        let pair = HealthKey::ProviderKey {
            provider: provider.name.clone(),
            key_id: key_id.clone(),
        };

        if !outcome.succeeded() {
            tracing::warn!(
                provider = %provider.name,
                key_id = %key_id,
                error = outcome.error.as_deref().unwrap_or("unreachable"),
                "keyed models probe failed"
            );
            self.health
                .record_failure(&pair, outcome.retry_after_secs, now_unix());
            // Drop its channels: the pair cannot serve them.
            return self
                .seen
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&(provider.name.clone(), key_id))
                .is_some();
        }

        self.health.record_success(&pair);
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let slot = seen.entry((provider.name.clone(), key_id));
        match slot {
            std::collections::hash_map::Entry::Occupied(mut held) => {
                if *held.get() == outcome.models {
                    return false;
                }
                held.insert(outcome.models);
                true
            }
            std::collections::hash_map::Entry::Vacant(empty) => {
                empty.insert(outcome.models);
                true
            }
        }
    }

    /// True when this pair has been probed successfully at least once, even if
    /// the provider listed no models.
    fn has_model_list(&self, provider: &Provider, index: usize) -> bool {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&(provider.name.clone(), key_id(provider, index)))
    }

    /// Rebuilds the whole channel list from every model list held and swaps it
    /// in. The catalogue sorts and shuffles ties on the way in.
    fn rebuild(&self) {
        let mut channels = Vec::new();
        {
            let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            for provider in &self.config.providers {
                for (index, key) in self.config.keys_for(provider).iter().enumerate() {
                    let id = key_id(provider, index);
                    if let Some(models) = seen.get(&(provider.name.clone(), id.clone())) {
                        channels.extend(self.build_channels(provider, &id, key, models));
                    }
                }
            }
        }
        self.catalogue.replace(channels);
    }

    /// The one HTTP call both probes make. `key` absent is the keyless probe.
    async fn fetch_models(&self, provider: &Provider, key: Option<&str>) -> ProbeOutcome {
        let mut request = self
            .upstream
            .client()
            .get(models_url(provider))
            .timeout(self.config.router.health_poll.probe_timeout);

        if let Some(key) = key {
            request = match provider.wire {
                WireName::OpenAi => request.header("authorization", format!("Bearer {key}")),
                WireName::Anthropic => request
                    .header("x-api-key", key)
                    .header("anthropic-version", ANTHROPIC_VERSION),
            };
        }

        let response = match request.send().await {
            Ok(response) => response,
            // `without_url` keeps the message to the transport fault itself.
            Err(error) => return ProbeOutcome::transport_failure(error.without_url().to_string()),
        };

        let status = response.status();
        let retry_after_secs = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_retry_after(v, SystemTime::now()));

        if !status.is_success() {
            // The host answered, so it is reachable. The keyed probe still
            // treats this as a failure, through `error`.
            return ProbeOutcome {
                alive: true,
                models: Vec::new(),
                retry_after_secs,
                error: Some(format!("models returned {}", status.as_u16())),
            };
        }

        match response.json::<serde_json::Value>().await {
            Ok(body) => ProbeOutcome {
                alive: true,
                models: parse_model_list(&body),
                retry_after_secs,
                error: None,
            },
            Err(_) => ProbeOutcome {
                alive: true,
                models: Vec::new(),
                retry_after_secs,
                error: Some("models body was not JSON".to_string()),
            },
        }
    }
}

/// Parses an OpenAI-shaped or Anthropic-shaped `/models` body into model rows.
pub fn parse_model_list(body: &serde_json::Value) -> Vec<ProbedModel> {
    let rows = body
        .get("data")
        .or_else(|| body.get("models"))
        .and_then(|v| v.as_array())
        .or_else(|| body.as_array());

    rows.map(|rows| rows.iter().filter_map(parse_model_row).collect())
        .unwrap_or_default()
}

/// One row of a model list. A row with no usable id is skipped rather than
/// turned into a channel that cannot be called.
fn parse_model_row(row: &serde_json::Value) -> Option<ProbedModel> {
    let id = row
        .get("id")
        .or_else(|| row.get("name"))
        .and_then(|v| v.as_str())?
        .trim();

    // Gemini writes its ids as `models/gemini-...` under `name`.
    let id = id.strip_prefix("models/").unwrap_or(id);
    if id.is_empty() {
        return None;
    }

    Some(ProbedModel {
        id: id.to_string(),
        limit_context: first_number(row, CONTEXT_FIELDS),
        limit_output: first_number(row, OUTPUT_FIELDS),
    })
}

/// The first of `paths` that holds a usable number.
fn first_number(row: &serde_json::Value, paths: &[&str]) -> Option<u32> {
    paths.iter().find_map(|path| number_at(row, path))
}

/// Reads a number at a dotted path. Accepts a JSON number or a string holding
/// one, which several providers send.
fn number_at(row: &serde_json::Value, path: &str) -> Option<u32> {
    let mut at = row;
    for segment in path.split('.') {
        at = at.get(segment)?;
    }
    let value = at
        .as_u64()
        .or_else(|| at.as_str().and_then(|s| s.trim().parse().ok()))?;
    u32::try_from(value).ok()
}

/// The model list URL for a provider, with exactly one slash at the join.
fn models_url(provider: &Provider) -> String {
    let base = provider.base_url.trim_end_matches('/');
    let path = provider.models_path.trim_start_matches('/');
    format!("{base}/{path}")
}

/// A stable label for one of a provider's keys. Never the key itself: the
/// position in `keys_env` is enough to name it in a log.
fn key_id(provider: &Provider, index: usize) -> String {
    format!("{}#{index}", provider.name)
}

/// Now, in whole unix seconds.
fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A prober over a scratch store, carrying the capabilities rows and the
    /// `models.yml` overrides the test needs.
    fn prober_with(
        capabilities: Vec<((&str, &str), crate::config::Capability)>,
        models: Vec<crate::config::ModelOverride>,
    ) -> Prober {
        let dir = std::env::temp_dir().join(format!(
            "llmrouter-probe-caps-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");

        let router = crate::config::RouterConfig::default();
        let config = Config {
            router: router.clone(),
            providers: Vec::new(),
            models,
            intelligence: Vec::new(),
            keys: HashMap::new(),
            capabilities: capabilities
                .into_iter()
                .map(|((p, m), cap)| ((p.to_string(), m.to_string()), cap))
                .collect(),
        };
        let db = Arc::new(
            redb::Database::create(dir.join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                router.health_poll.retry_base,
                router.health_poll.backoff_max,
            )
            .expect("health table opens"),
        );
        Prober::new(
            Arc::new(config),
            Upstream::new(&router).expect("client builds"),
            Arc::new(Catalogue::new()),
            health,
        )
    }

    /// A prober over a scratch store with the given router settings and
    /// providers, and no keys.
    fn prober_for(router: crate::config::RouterConfig, providers: Vec<Provider>) -> Prober {
        let dir = std::env::temp_dir().join(format!(
            "llmrouter-probe-clock-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");

        let config = Config {
            router: router.clone(),
            providers,
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: HashMap::new(),
            capabilities: HashMap::new(),
        };
        let db = Arc::new(
            redb::Database::create(dir.join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                router.health_poll.retry_base,
                router.health_poll.backoff_max,
            )
            .expect("health table opens"),
        );
        Prober::new(
            Arc::new(config),
            Upstream::new(&router).expect("client builds"),
            Arc::new(Catalogue::new()),
            health,
        )
    }

    fn probed(id: &str, limit_context: Option<u32>) -> ProbedModel {
        ProbedModel {
            id: id.to_string(),
            limit_context,
            limit_output: None,
        }
    }

    /// The pre-race tool filter only works if the capabilities file is
    /// actually joined on. Without that join every channel claims tool
    /// support and the race fires at models that answer 4xx to a tools array.
    #[test]
    fn the_capabilities_file_decides_tool_support_and_context() {
        let cap = |tool_call, limit_context| crate::config::Capability {
            limit_context,
            tool_call,
        };
        let prober = prober_with(
            vec![
                (("gemini", "no-tools"), cap(Some(false), Some(1_000_000))),
                (("gemini", "has-tools"), cap(Some(true), None)),
            ],
            Vec::new(),
        );
        let p = provider("https://example.invalid", "/models");
        let channels = prober.build_channels(
            &p,
            "gemini#0",
            "secret",
            &[
                probed("no-tools", Some(32_000)),
                probed("has-tools", Some(32_000)),
                // Not in the file at all: assume tools, take the provider's
                // context window.
                probed("unknown", Some(64_000)),
            ],
        );

        assert!(!channels[0].tool_call, "the file says this one takes none");
        // The file is the primary source for the window, the probe the
        // fallback.
        assert_eq!(channels[0].limit_context, 1_000_000);

        assert!(channels[1].tool_call);
        assert_eq!(channels[1].limit_context, 32_000);

        assert!(channels[2].tool_call, "unknown models are assumed capable");
        assert_eq!(channels[2].limit_context, 64_000);
    }

    /// `models.yml` is the hand-written correction, so it wins over the
    /// snapshot.
    #[test]
    fn a_models_yml_override_beats_the_capabilities_file() {
        let prober = prober_with(
            vec![(
                ("gemini", "no-tools"),
                crate::config::Capability {
                    limit_context: Some(1_000_000),
                    tool_call: Some(false),
                },
            )],
            vec![crate::config::ModelOverride {
                provider: "gemini".to_string(),
                model_id: "no-tools".to_string(),
                limit_context: Some(8_000),
                limit_output: None,
                tool_call: Some(true),
                ranks_as: None,
            }],
        );
        let p = provider("https://example.invalid", "/models");
        let channels = prober.build_channels(&p, "gemini#0", "secret", &[probed("no-tools", None)]);
        assert!(channels[0].tool_call);
        assert_eq!(channels[0].limit_context, 8_000);
    }

    fn provider(base_url: &str, models_path: &str) -> Provider {
        Provider {
            name: "gemini".to_string(),
            wire: WireName::OpenAi,
            base_url: base_url.to_string(),
            keys_env: "GEMINI_KEYS".to_string(),
            models_path: models_path.to_string(),
        }
    }

    #[test]
    fn openai_shape_gives_ids_and_context() {
        let body = json!({
            "object": "list",
            "data": [
                { "id": "gpt-5", "context_length": 400000, "max_output_tokens": 128000 },
                { "id": "gpt-5-mini" }
            ]
        });
        let models = parse_model_list(&body);
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].id, "gpt-5");
        assert_eq!(models[0].limit_context, Some(400_000));
        assert_eq!(models[0].limit_output, Some(128_000));
        assert_eq!(models[1].limit_context, None);
    }

    #[test]
    fn gemini_shape_strips_the_models_prefix() {
        let body = json!({
            "models": [
                {
                    "name": "models/gemini-3.7-flash",
                    "inputTokenLimit": 1048576,
                    "outputTokenLimit": 65536
                }
            ]
        });
        let models = parse_model_list(&body);
        assert_eq!(models[0].id, "gemini-3.7-flash");
        assert_eq!(models[0].limit_context, Some(1_048_576));
        assert_eq!(models[0].limit_output, Some(65_536));
    }

    #[test]
    fn a_bare_array_is_a_model_list_too() {
        let body = json!([{ "id": "llama-4" }]);
        assert_eq!(parse_model_list(&body)[0].id, "llama-4");
    }

    #[test]
    fn nested_and_string_numbers_are_read() {
        let body = json!({
            "data": [{
                "id": "claude-fable-5-1",
                "limit": { "context": "200000", "output": 64000 }
            }]
        });
        let models = parse_model_list(&body);
        assert_eq!(models[0].limit_context, Some(200_000));
        assert_eq!(models[0].limit_output, Some(64_000));
    }

    #[test]
    fn rows_without_a_usable_id_are_skipped() {
        let body = json!({ "data": [{ "id": "" }, { "owned_by": "nobody" }, 7, { "id": "ok" }] });
        let models = parse_model_list(&body);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "ok");
    }

    #[test]
    fn an_empty_or_odd_body_gives_no_models() {
        assert!(parse_model_list(&json!({})).is_empty());
        assert!(parse_model_list(&json!({ "data": {} })).is_empty());
        assert!(parse_model_list(&json!({ "data": [] })).is_empty());
        assert!(parse_model_list(&json!("nonsense")).is_empty());
    }

    #[test]
    fn a_context_window_too_big_for_u32_is_ignored_not_truncated() {
        let body = json!({ "data": [{ "id": "x", "context_length": 5_000_000_000u64 }] });
        assert_eq!(parse_model_list(&body)[0].limit_context, None);
    }

    #[test]
    fn the_first_published_context_field_wins() {
        let body = json!({
            "data": [{
                "id": "x",
                "context_length": 128000,
                "top_provider": { "context_length": 64000 }
            }]
        });
        assert_eq!(parse_model_list(&body)[0].limit_context, Some(128_000));
    }

    #[test]
    fn the_models_url_has_exactly_one_slash_at_the_join() {
        assert_eq!(
            models_url(&provider("https://example.com/v1", "/models")),
            "https://example.com/v1/models"
        );
        assert_eq!(
            models_url(&provider("https://example.com/v1/", "models")),
            "https://example.com/v1/models"
        );
        assert_eq!(
            models_url(&provider("https://example.com/v1///", "///models")),
            "https://example.com/v1/models"
        );
    }

    #[test]
    fn a_key_id_names_the_provider_and_the_slot_never_the_key() {
        let p = provider("https://example.com", "/models");
        assert_eq!(key_id(&p, 0), "gemini#0");
        assert_eq!(key_id(&p, 8), "gemini#8");
    }

    #[test]
    fn only_a_2xx_counts_as_a_keyed_success() {
        let reachable_but_refused = ProbeOutcome {
            alive: true,
            models: Vec::new(),
            retry_after_secs: None,
            error: Some("models returned 401".to_string()),
        };
        assert!(reachable_but_refused.alive);
        assert!(!reachable_but_refused.succeeded());

        let dead = ProbeOutcome::transport_failure("dns failure".to_string());
        assert!(!dead.alive);
        assert!(!dead.succeeded());

        let good = ProbeOutcome {
            alive: true,
            models: vec![ProbedModel {
                id: "x".to_string(),
                limit_context: None,
                limit_output: None,
            }],
            retry_after_secs: None,
            error: None,
        };
        assert!(good.succeeded());
    }

    /// A provider that accepts the connection and never answers, so a probe
    /// against it burns the whole `probe_timeout`.
    async fn hanging_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port binds");
        let addr = listener.local_addr().expect("the port is known");
        let handle = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                // Hold it open and send nothing.
                held.push(socket);
            }
        });
        (format!("http://{addr}"), handle)
    }

    /// The poller walks providers one at a time, each probe bounded by
    /// `probe_timeout`, so a pass can easily outlast the shortest backoff. If
    /// the clock is read once at the top of the pass, the deadline written for
    /// a failure is already in the past by the time the pass ends and the dead
    /// provider is probed again on the very next tick.
    #[tokio::test]
    async fn a_failed_probe_backs_off_from_the_clock_at_the_failure() {
        let (base_url, server) = hanging_server().await;

        let mut router = crate::config::RouterConfig::default();
        router.health_poll.probe_timeout = Duration::from_secs(2);
        let provider = Provider {
            name: "slowhost".to_string(),
            wire: WireName::OpenAi,
            base_url,
            keys_env: "SLOWHOST_KEYS".to_string(),
            models_path: "/models".to_string(),
        };
        let prober = prober_for(router, vec![provider]);

        let key = HealthKey::Provider("slowhost".to_string());
        // Already unhealthy and already out of backoff, so the pass probes it.
        prober.health.record_failure(&key, None, 0);

        let started = now_unix();
        prober.poll_once().await;
        let finished = now_unix();
        assert!(
            finished > started,
            "the pass must outlast a second for this test to mean anything"
        );
        assert!(
            !prober.health.eligible(&key, finished),
            "a provider that just failed must still be in backoff"
        );

        server.abort();
    }
}
