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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use secrecy::{ExposeSecret, SecretString};

use crate::catalogue::{Catalogue, Channel, ChannelHealth, ChannelId};
use crate::config::{Config, Provider, RankMatch, WireName};
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
    /// The provider's own release timestamp in unix seconds, when it
    /// publishes one. `GET /v1/models` reports it as `created`.
    pub created: Option<u64>,
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
    /// Provider/model pairs whose ranking came from a loose suffix match and
    /// have already been logged. The rebuild runs on every poller tick and
    /// once per key, so without this the same warning repeats forever.
    loose_logged: Mutex<HashSet<(String, String)>>,
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
            loose_logged: Mutex::new(HashSet::new()),
        }
    }

    /// The client this prober calls `/models` with.
    pub fn upstream(&self) -> &Upstream {
        &self.upstream
    }

    /// Wave 1 then wave 2, everything inside a wave at once. Swaps the
    /// catalogue when it finishes. Runs in the background; nothing waits on
    /// it. Returns the summary it logged, which the tests read.
    pub async fn run_startup_waves(&self) -> crate::logging::StartupSummary {
        // Wave 1: every provider at once, no key.
        let bar = crate::logging::bar(self.config.providers.len() as u64, "wave 1 reachability");
        let wave_one = futures::future::join_all(self.config.providers.iter().map(|p| {
            let bar = bar.clone();
            async move {
                let outcome = self.probe_reachable(p).await;
                bar.inc(1);
                (p, outcome)
            }
        }))
        .await;
        bar.finish_and_clear();

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
        let pairs: Vec<(&Provider, usize, &SecretString)> = reachable
            .iter()
            .flat_map(|p| {
                self.config
                    .keys_for(p)
                    .iter()
                    .enumerate()
                    .map(move |(i, k)| (*p, i, k))
            })
            .collect();

        let bar = crate::logging::bar(pairs.len() as u64, "wave 2 keyed /models");
        let wave_two = futures::future::join_all(pairs.into_iter().map(|(p, i, k)| {
            let bar = bar.clone();
            async move {
                let outcome = self.probe_models(p, k.expose_secret()).await;
                bar.inc(1);
                (p, i, outcome)
            }
        }))
        .await;
        bar.finish_and_clear();

        let mut summary = crate::logging::StartupSummary {
            providers: self.config.providers.len(),
            providers_reachable: reachable.len(),
            keys: wave_two.len(),
            ..Default::default()
        };
        for (provider, index, outcome) in wave_two {
            // A 401 answers, so counting `alive` here would report a dead key
            // as a working one. Only a 2xx means the pair fetched its models.
            if outcome.succeeded() {
                summary.keys_answered += 1;
            }
            self.apply_keyed_outcome(provider, index, outcome);
        }

        self.rebuild(true);
        summary.channels = self.catalogue.len();
        summary.in_backoff = self.health.in_backoff(now_unix()).len();
        tracing::info!(channels = summary.channels, "startup waves finished");
        tracing::info!("startup summary\n{}", summary.table());
        summary
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
        let pair_eligible_at = self
            .health
            .get(&HealthKey::ProviderKey {
                provider: provider.name.clone(),
                key_id: key_id.to_string(),
            })
            .filter(|e| e.retry_counter > 0)
            .map_or(0, |e| e.eligible_at);
        // A provider in backoff holds its channels back too. Reading only the
        // pair left a provider marked unreachable with every one of its rows
        // still eligible, so the walk kept racing channels on a host the
        // poller had already written off.
        let provider_eligible_at = self
            .health
            .get(&HealthKey::Provider(provider.name.clone()))
            .filter(|e| e.retry_counter > 0)
            .map_or(0, |e| e.eligible_at);
        let key_id: Arc<str> = Arc::from(key_id);

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

                let ranking = self.config.ranking_of(&provider.name, &model.id);
                let (intelligence, intelligence_rank) =
                    ranking.as_ref().map_or((0, u32::MAX), |(e, _)| {
                        (e.intelligence, e.intelligence_rank)
                    });
                // The provider's own date first, else the release date in
                // models.csv, per DESIGN.md "Where each field comes from".
                let created = model
                    .created
                    .or_else(|| ranking.as_ref().and_then(|(e, _)| e.released_unix()));
                if let Some((_, RankMatch::Loose(slug))) = ranking {
                    self.log_loose_rank(&provider.name, &model.id, &slug, intelligence);
                }

                let id = ChannelId::new(&provider.name, key, &model.id);

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
                    key: SecretString::from(key),
                    base_url: base_url.clone(),
                    wire: provider.wire,
                    limit_context: over
                        .and_then(|o| o.limit_context)
                        .or_else(|| caps.and_then(|c| c.limit_context))
                        .or(model.limit_context)
                        .unwrap_or(DEFAULT_LIMIT_CONTEXT),
                    limit_output: over
                        .and_then(|o| o.limit_output)
                        .or_else(|| caps.and_then(|c| c.limit_output))
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
                    // The capabilities file is the only source for these
                    // three. A blank cell means "not known", and the safe
                    // reading of "not known" is "no", so a client never gets
                    // told a model can do something it cannot.
                    reasoning: caps.and_then(|c| c.reasoning).unwrap_or(false),
                    structured_output: caps.and_then(|c| c.structured_output).unwrap_or(false),
                    modalities_input: caps
                        .and_then(|c| c.modalities_input.clone())
                        .unwrap_or_else(|| vec!["text".to_string()]),
                    intelligence,
                    intelligence_rank,
                    created,
                    health: ChannelHealth {
                        enabled: AtomicBool::new(true),
                        retry_counter: AtomicU32::new(known.retry_counter),
                        retry_base_secs: AtomicU64::new(known.retry_base_secs),
                        eligible_at: AtomicU64::new(known.eligible_at),
                        // A pair or a provider in backoff holds every channel
                        // under it back too, and each level keeps its own
                        // field so a later move at one level cannot erase
                        // another's.
                        pair_eligible_at: AtomicU64::new(pair_eligible_at),
                        provider_eligible_at: AtomicU64::new(provider_eligible_at),
                        // A rebuild carries the live count over from the row
                        // this one replaces, so starting at zero is right.
                        in_flight: Arc::new(AtomicU32::new(0)),
                    },
                    telemetry: Default::default(),
                    max_per_channel: provider.max_per_channel,
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
                if self.every_key_down(provider) {
                    // Down because every key is down, not because the host is
                    // unreachable. A keyless probe cannot test a key, so it
                    // always says alive and clearing the provider on it would
                    // reset the provider backoff and force a keyed sweep every
                    // tick, forever. Leave each pair to its own deadline; a
                    // pair that answers clears the provider.
                    changed |= self.poll_keys(provider, false).await;
                    continue;
                }
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
            self.rebuild(false);
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
                key_id: key_id(&provider.name, key.expose_secret()),
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

            let outcome = self.probe_models(provider, key.expose_secret()).await;
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
        let key_id = self.pair_id(provider, index);
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
            self.escalate_provider(provider, outcome.retry_after_secs);
            // Keep the last model list. DESIGN.md: never filter GET /v1/models
            // by health, so the pair's models stay listed with `healthy: 0`.
            // The rebuild folds the pair's backoff into each row, which keeps
            // the race off them until the pair is eligible again.
            return self
                .seen
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&(provider.name.clone(), key_id));
        }

        // A pair coming out of backoff needs a rebuild even with the same
        // list, to clear the backoff the last rebuild folded into its rows.
        let was_down = self.health.get(&pair).is_some_and(|e| e.retry_counter > 0);
        self.health.record_success(&pair);
        // A key answering proves the provider is alive, so an escalation that
        // marked the provider down because every key was down clears with it.
        self.health
            .record_success(&HealthKey::Provider(provider.name.clone()));
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let slot = seen.entry((provider.name.clone(), key_id));
        match slot {
            std::collections::hash_map::Entry::Occupied(mut held) => {
                if *held.get() == outcome.models {
                    return was_down;
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

    /// Takes the provider itself down once every key on it is unhealthy,
    /// which is the second clause of DESIGN.md's unit-of-health table. The
    /// live-traffic path does this in `race::escalate_failure`; the probe path
    /// needs its own, because a provider whose keys all fail ends up with zero
    /// channels and so never sees live traffic again. Left healthy, it would
    /// cost one keyed call per key per backoff period forever, when one
    /// keyless reachability call is the whole answer.
    fn escalate_provider(&self, provider: &Provider, retry_after_secs: Option<u64>) {
        let provider_key = HealthKey::Provider(provider.name.clone());
        if self
            .health
            .get(&provider_key)
            .is_some_and(|e| e.retry_counter > 0)
        {
            // Already with the poller. Bumping it again would push the
            // re-probe out for a fault the poller is the one meant to clear.
            return;
        }
        if self.every_key_down(provider) {
            tracing::warn!(
                provider = %provider.name,
                keys = self.config.keys_for(provider).len(),
                "every key on this provider is unhealthy, so the provider is unhealthy"
            );
            self.health
                .record_failure(&provider_key, retry_after_secs, now_unix());
        }
    }

    /// True when the provider has keys and every one of them is unhealthy,
    /// which is the second clause of DESIGN.md's unit-of-health table. False
    /// for a provider with no keys, whose health is reachability alone.
    fn every_key_down(&self, provider: &Provider) -> bool {
        every_key_down(&self.config, &self.health, &provider.name)
    }

    /// The label of the key at `index` in this provider's `keys_env` list.
    fn pair_id(&self, provider: &Provider, index: usize) -> String {
        key_id(
            &provider.name,
            self.config.keys_for(provider)[index].expose_secret(),
        )
    }

    /// True when this pair has been probed successfully at least once, which
    /// means it returned at least one model: an empty list is a failed probe.
    fn has_model_list(&self, provider: &Provider, index: usize) -> bool {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&(provider.name.clone(), self.pair_id(provider, index)))
    }

    /// Rebuilds the whole channel list from every model list held and swaps it
    /// in. The catalogue sorts and shuffles ties on the way in.
    /// `show_progress` is on for the one bounded build at startup and off for
    /// the poller, which rebuilds every tick and would otherwise draw a bar
    /// forever.
    fn rebuild(&self, show_progress: bool) {
        let pairs: usize = self
            .config
            .providers
            .iter()
            .map(|p| self.config.keys_for(p).len())
            .sum();
        let bar = show_progress.then(|| crate::logging::bar(pairs as u64, "catalogue build"));

        // Built inside the catalogue's rebuild lock: a race writing health
        // between the reads in `build_channels` and the swap would otherwise
        // land on a row about to be orphaned.
        self.catalogue.replace_with(|| {
            let mut channels = Vec::new();
            let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
            for provider in &self.config.providers {
                for key in self.config.keys_for(provider) {
                    let id = key_id(&provider.name, key.expose_secret());
                    if let Some(models) = seen.get(&(provider.name.clone(), id.clone())) {
                        channels.extend(self.build_channels(
                            provider,
                            &id,
                            key.expose_secret(),
                            models,
                        ));
                    }
                    if let Some(bar) = &bar {
                        bar.inc(1);
                    }
                }
            }
            channels
        });
        if let Some(bar) = bar {
            bar.finish_and_clear();
        }
    }

    /// Warns once per provider/model pair that its ranking came only from a
    /// suffix match, which DESIGN.md calls the mitigation for a silent wrong
    /// name match. The pair is logged the first time it is built, which is
    /// the startup rebuild, and never again.
    fn log_loose_rank(&self, provider: &str, model_id: &str, slug: &str, intelligence: u16) {
        let fresh = self
            .loose_logged
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((provider.to_string(), model_id.to_string()));
        if fresh {
            tracing::warn!(
                provider = %provider,
                model = %model_id,
                ranking_slug = %slug,
                intelligence,
                "ranked by a loose suffix match, so check the pairing"
            );
        }
    }

    /// The one HTTP call both probes make. `key` absent is the keyless probe.
    async fn fetch_models(&self, provider: &Provider, key: Option<&str>) -> ProbeOutcome {
        let mut request = self
            .upstream
            .client()
            .get(models_url(provider))
            .timeout(self.config.router.health_poll.probe_timeout);

        if let Some(key) = key {
            request = authorise(request, provider, key);
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
            // A provider with no model-listing endpoint answers 404 or 405.
            // Cloudflare Workers AI is one: its OpenAI-compatible path takes
            // POST only. Its models are listed in `providers.yml` instead, and
            // the pair counts as healthy. Any other status, 401 included,
            // still fails, so a dead key never produces channels.
            if matches!(status.as_u16(), 404 | 405) && !provider.models.is_empty() {
                // A base URL missing its version segment answers 404 here too,
                // and then every chat call 404s while the router calls the
                // provider healthy. The configured list cannot tell the two
                // apart, so say loudly which URL answered what: that line is
                // what the `BASE_URL_FIXUPS` list in `src/bin/make-config.rs`
                // is maintained from.
                tracing::warn!(
                    provider = %provider.name,
                    status = status.as_u16(),
                    url = %models_url(provider),
                    models = provider.models.len(),
                    "no model list at this URL, falling back to the configured \
                     models: check the base URL has its version segment"
                );
                return ProbeOutcome {
                    alive: true,
                    models: provider
                        .models
                        .iter()
                        .map(|id| ProbedModel {
                            id: id.clone(),
                            limit_context: None,
                            limit_output: None,
                            created: None,
                        })
                        .collect(),
                    retry_after_secs,
                    error: None,
                };
            }
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
            Ok(body) => {
                let mut models = parse_model_list(&body);
                // DESIGN.md filter check 3, "the key can reach the model".
                // OpenRouter's list is not scoped to the key: a free key sees
                // all 417 models and can call only the zero-priced ones. The
                // paid rest would answer 402 on every call, so a free key
                // never gets channels for them.
                if let Some(key) = key {
                    let paid = paid_model_ids(&body);
                    if !paid.is_empty() && self.is_free_tier(provider, key).await {
                        models.retain(|m| !paid.contains(&m.id));
                    }
                }
                // DESIGN.md: no list, no channels. A 2xx that yields zero
                // models is a failure, so the pair goes into backoff and the
                // poller retries it. Counted as a success it would keep an
                // empty list forever: the pair has no channels, so no live
                // traffic can ever correct it.
                let error = models
                    .is_empty()
                    .then(|| "models list was empty".to_string());
                ProbeOutcome {
                    alive: true,
                    models,
                    retry_after_secs,
                    error,
                }
            }
            Err(_) => ProbeOutcome {
                alive: true,
                models: Vec::new(),
                retry_after_secs,
                error: Some("models body was not JSON".to_string()),
            },
        }
    }
}

/// Adds the key to a request in the provider's own auth header.
fn authorise(
    request: reqwest::RequestBuilder,
    provider: &Provider,
    key: &str,
) -> reqwest::RequestBuilder {
    match provider.wire {
        WireName::OpenAi => request.header("authorization", format!("Bearer {key}")),
        WireName::Anthropic => request
            .header("x-api-key", key)
            .header("anthropic-version", ANTHROPIC_VERSION),
    }
}

impl Prober {
    /// True when the provider says this key is on its free tier, through
    /// OpenRouter's `GET /key` (`data.is_free_tier`). Asked only of a provider
    /// whose model list carries prices. Any failure reads as "not free", which
    /// keeps every model: live traffic still learns the dead ones.
    async fn is_free_tier(&self, provider: &Provider, key: &str) -> bool {
        let url = format!("{}/key", provider.base_url.trim_end_matches('/'));
        let request = self
            .upstream
            .client()
            .get(url)
            .timeout(self.config.router.health_poll.probe_timeout);
        let Ok(response) = authorise(request, provider, key).send().await else {
            return false;
        };
        if !response.status().is_success() {
            return false;
        }
        response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| body.pointer("/data/is_free_tier")?.as_bool())
            .unwrap_or(false)
    }
}

/// The rows of an OpenAI-shaped or Anthropic-shaped `/models` body.
fn model_rows(body: &serde_json::Value) -> &[serde_json::Value] {
    body.get("data")
        .or_else(|| body.get("models"))
        .and_then(|v| v.as_array())
        .or_else(|| body.as_array())
        .map_or(&[], Vec::as_slice)
}

/// Parses an OpenAI-shaped or Anthropic-shaped `/models` body into model rows.
pub fn parse_model_list(body: &serde_json::Value) -> Vec<ProbedModel> {
    model_rows(body)
        .iter()
        .filter_map(parse_model_row)
        .collect()
}

/// Ids of the rows whose `pricing` charges for prompt or completion tokens.
/// OpenRouter writes prices as strings: `"0"` for a free model, `"-1"` for a
/// variable one such as `openrouter/auto`. Any non-zero price counts as paid.
/// A row with no `pricing` is not counted.
fn paid_model_ids(body: &serde_json::Value) -> std::collections::HashSet<String> {
    model_rows(body)
        .iter()
        .filter(|row| {
            ["prompt", "completion"].iter().any(|field| {
                row.get("pricing")
                    .and_then(|p| p.get(field))
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
                    })
                    .is_some_and(|price| price != 0.0)
            })
        })
        .filter_map(parse_model_row)
        .map(|m| m.id)
        .collect()
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
        created: created_at(row),
    })
}

/// The provider's own `created` for this row, in unix seconds. Accepts a
/// number, a string holding one, or an RFC 3339 timestamp, which is what
/// Anthropic's own model list sends.
fn created_at(row: &serde_json::Value) -> Option<u64> {
    let at = row.get("created").or_else(|| row.get("created_at"))?;
    at.as_u64()
        .or_else(|| at.as_str().and_then(|s| s.trim().parse().ok()))
        .or_else(|| {
            let when = humantime::parse_rfc3339(at.as_str()?.trim()).ok()?;
            Some(when.duration_since(UNIX_EPOCH).ok()?.as_secs())
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

/// True when the named provider has keys configured and every one of them is
/// unhealthy, which is the second clause of DESIGN.md's unit-of-health table.
/// False for a provider with no keys, whose health is reachability alone, and
/// false for a name that is not configured.
///
/// This is the one definition. It counts every configured key, not only the
/// keys that happen to hold channel rows: a key whose `/models` call came back
/// empty, or whose keyed probe failed, has no rows at all and must not be
/// taken as proof that the provider is down.
pub(crate) fn every_key_down(config: &Config, health: &HealthTable, provider_name: &str) -> bool {
    let Some(provider) = config.providers.iter().find(|p| p.name == provider_name) else {
        return false;
    };
    let keys = config.keys_for(provider);
    !keys.is_empty()
        && keys.iter().all(|key| {
            health
                .get(&HealthKey::ProviderKey {
                    provider: provider.name.clone(),
                    key_id: key_id(&provider.name, key.expose_secret()),
                })
                .is_some_and(|e| e.retry_counter > 0)
        })
}

/// A stable label for one of a provider's keys: the provider name and a short
/// hash of provider plus key, as `ChannelId` does. Never the key itself, and
/// never its position in `keys_env`: pair health outlives a restart in redb,
/// and DESIGN.md says an index means nothing after a rebuild. Reordering or
/// deleting a key must not hand one key's backoff to another.
pub fn key_id(provider: &str, key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(provider.as_bytes());
    hasher.update(b"\x00");
    hasher.update(key.as_bytes());
    format!("{provider}#{}", &hasher.finalize().to_hex()[..8])
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
    ) -> (Prober, tempfile::TempDir) {
        prober_ranked(capabilities, models, Vec::new())
    }

    /// `prober_with`, plus the `intelligence.yml` rows.
    fn prober_ranked(
        capabilities: Vec<((&str, &str), crate::config::Capability)>,
        models: Vec<crate::config::ModelOverride>,
        intelligence: Vec<crate::config::IntelligenceEntry>,
    ) -> (Prober, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a throwaway directory");

        let router = crate::config::RouterConfig::default();
        let config = Config {
            router: router.clone(),
            providers: Vec::new(),
            models,
            intelligence,
            keys: HashMap::new(),
            capabilities: capabilities
                .into_iter()
                .map(|((p, m), cap)| ((p.to_string(), m.to_string()), cap))
                .collect(),
        };
        let db = Arc::new(
            redb::Database::create(dir.path().join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                router.health_poll.retry_base,
                router.health_poll.backoff_max,
                crate::metrics::Metrics::new().expect("the recorder builds"),
            )
            .expect("health table opens"),
        );
        let prober = Prober::new(
            Arc::new(config),
            Upstream::new(&router).expect("client builds"),
            Arc::new(Catalogue::new()),
            health,
        );
        (prober, dir)
    }

    /// A prober over a scratch store with the given router settings and
    /// providers, and no keys.
    fn prober_for(
        router: crate::config::RouterConfig,
        providers: Vec<Provider>,
        keys: HashMap<String, Vec<String>>,
    ) -> (Prober, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a throwaway directory");

        let config = Config {
            router: router.clone(),
            providers,
            models: Vec::new(),
            intelligence: Vec::new(),
            keys: keys
                .into_iter()
                .map(|(name, keys)| (name, keys.into_iter().map(SecretString::from).collect()))
                .collect(),
            capabilities: HashMap::new(),
        };
        let db = Arc::new(
            redb::Database::create(dir.path().join("state.redb")).expect("the scratch store opens"),
        );
        let health = Arc::new(
            HealthTable::open(
                db,
                router.health_poll.retry_base,
                router.health_poll.backoff_max,
                crate::metrics::Metrics::new().expect("the recorder builds"),
            )
            .expect("health table opens"),
        );
        let prober = Prober::new(
            Arc::new(config),
            Upstream::new(&router).expect("client builds"),
            Arc::new(Catalogue::new()),
            health,
        );
        (prober, dir)
    }

    #[test]
    fn a_model_row_keeps_the_providers_created() {
        let body = serde_json::json!({"data": [
            {"id": "claude-fable-5-1", "created": 1_760_140_800u64},
            {"id": "gpt-6-astra", "created": "1757894400"},
            {"id": "no-date"},
            {"id": "claude-sonnet-4-5", "created_at": "2025-02-19T00:00:00Z"}
        ]});
        let models = parse_model_list(&body);
        assert_eq!(models[0].created, Some(1_760_140_800));
        assert_eq!(models[1].created, Some(1_757_894_400));
        assert_eq!(models[2].created, None);
        // Anthropic's model list dates are RFC 3339, not unix seconds.
        assert_eq!(models[3].created, Some(1_739_923_200));
    }

    /// The scratch store goes with its guard, including when the test that
    /// holds it panics. A hand-rolled helper that deletes on the success path
    /// leaks a redb file under the temp directory on every failed run.
    #[test]
    fn a_panicking_test_still_loses_its_scratch_store() {
        let payload = std::panic::catch_unwind(|| {
            let (_prober, dir) = prober_with(Vec::new(), Vec::new());
            let path = dir.path().to_path_buf();
            assert!(path.join("state.redb").is_file(), "the store is on disk");
            std::panic::panic_any(path)
        })
        .expect_err("the closure panics on purpose");
        let path = payload
            .downcast::<std::path::PathBuf>()
            .expect("the path rides the panic out");
        assert!(
            !path.exists(),
            "unwinding must still remove the scratch directory"
        );
    }

    fn probed(id: &str, limit_context: Option<u32>) -> ProbedModel {
        ProbedModel {
            id: id.to_string(),
            limit_context,
            limit_output: None,
            created: None,
        }
    }

    /// A provider that publishes no `created` (Gemini, the cloudflare
    /// fallback list) gets the release date from models.csv, not the boot
    /// time. A provider's own date still wins.
    #[test]
    fn created_falls_back_to_the_release_date_not_boot_time() {
        let (prober, _dir) = prober_ranked(
            Vec::new(),
            Vec::new(),
            vec![crate::config::IntelligenceEntry {
                slug: "gemini-3.7-flash".to_string(),
                intelligence: 40,
                intelligence_rank: 10,
                release_date: Some("2025-02-19".to_string()),
            }],
        );
        let p = provider("https://example.invalid", "/models");
        let mut dated = probed("gemini-3.7-flash", None);
        dated.created = Some(1_760_140_800);
        let channels = prober.build_channels(
            &p,
            "gemini#0",
            "secret",
            &[
                probed("gemini-3.7-flash", None),
                dated,
                probed("unranked", None),
            ],
        );
        assert_eq!(channels[0].created, Some(1_739_923_200));
        assert_eq!(channels[1].created, Some(1_760_140_800));
        assert_eq!(channels[2].created, None);
    }

    /// `GET /v1/models` reports `supports_vision`, `supports_reasoning` and
    /// `max_output` straight off the channel list, so if these three columns
    /// are not carried over from `model_capabilities.csv` every model reads
    /// as text-only, non-reasoning and default-sized.
    #[test]
    fn the_capabilities_file_decides_reasoning_vision_and_output() {
        let (prober, _dir) = prober_with(
            vec![
                (
                    ("gemini", "sees-and-thinks"),
                    crate::config::Capability {
                        limit_output: Some(64_000),
                        reasoning: Some(true),
                        structured_output: Some(true),
                        modalities_input: Some(vec!["text".to_string(), "image".to_string()]),
                        ..Default::default()
                    },
                ),
                (("gemini", "plain"), crate::config::Capability::default()),
            ],
            Vec::new(),
        );
        let p = provider("https://example.invalid", "/models");
        let channels = prober.build_channels(
            &p,
            "gemini#0",
            "secret",
            &[
                probed("sees-and-thinks", None),
                probed("plain", None),
                // Not in the file at all.
                probed("unknown", None),
            ],
        );

        assert!(channels[0].reasoning);
        assert!(channels[0].structured_output);
        assert_eq!(channels[0].modalities_input, ["text", "image"]);
        assert_eq!(channels[0].limit_output, 64_000);

        // A blank cell reads as "no", not "yes".
        for channel in &channels[1..] {
            assert!(!channel.reasoning, "{} claims reasoning", channel.model_id);
            assert!(!channel.structured_output);
            assert_eq!(channel.modalities_input, ["text"]);
            assert_eq!(channel.limit_output, DEFAULT_LIMIT_OUTPUT);
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
            ..Default::default()
        };
        let (prober, _dir) = prober_with(
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
        let (prober, _dir) = prober_with(
            vec![(
                ("gemini", "no-tools"),
                crate::config::Capability {
                    limit_context: Some(1_000_000),
                    tool_call: Some(false),
                    ..Default::default()
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

    /// The poller rebuilds the catalogue every tick, and a rebuild that read
    /// only the pair handed back fresh rows with the provider's own backoff
    /// thrown away. The walk then raced a host the keyless probe had already
    /// found unreachable.
    #[test]
    fn a_rebuild_keeps_a_provider_level_backoff_on_the_new_rows() {
        let (prober, _dir) = prober_ranked(Vec::new(), Vec::new(), Vec::new());
        let p = provider("https://example.invalid", "/models");
        let entry = prober.health.record_failure(
            &HealthKey::Provider("gemini".to_string()),
            Some(600),
            1_000,
        );

        let channels = prober.build_channels(&p, "gemini#0", "secret", &[probed("model-a", None)]);
        assert_eq!(
            channels[0].health.not_before(),
            entry.eligible_at,
            "a rebuilt row carries the provider's deadline"
        );
        assert!(!channels[0].eligible(1_010));
    }

    fn provider(base_url: &str, models_path: &str) -> Provider {
        Provider {
            name: "gemini".to_string(),
            wire: WireName::OpenAi,
            base_url: base_url.to_string(),
            keys_env: "GEMINI_KEYS".to_string(),
            models_path: models_path.to_string(),
            models: Vec::new(),
            max_per_channel: None,
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
    fn a_key_id_follows_the_key_not_its_position() {
        let a = key_id("gemini", "sk-not-a-real-key-a");
        assert!(a.starts_with("gemini#") && !a.contains("sk-"));
        assert_eq!(a, key_id("gemini", "sk-not-a-real-key-a"), "stable");
        assert_ne!(a, key_id("gemini", "sk-not-a-real-key-b"));
        assert_ne!(a, key_id("openai", "sk-not-a-real-key-a"));
    }

    /// Gemini has keys [A, B, C] and B is deep in backoff. B is deleted and
    /// the router restarts with [A, C]. C now sits where B was and must not
    /// inherit B's retry counter from the health store.
    #[test]
    fn a_deleted_key_does_not_hand_its_backoff_to_the_next_key() {
        let provider = Provider {
            name: "gemini".to_string(),
            wire: WireName::OpenAi,
            base_url: "http://127.0.0.1:1".to_string(),
            keys_env: "GEMINI_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let (before, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            HashMap::from([(
                "GEMINI_KEYS".to_string(),
                vec![
                    "key-a".to_string(),
                    "key-b".to_string(),
                    "key-c".to_string(),
                ],
            )]),
        );
        let b = before.pair_id(&provider, 1);
        for _ in 0..20 {
            before.health.record_failure(
                &HealthKey::ProviderKey {
                    provider: "gemini".to_string(),
                    key_id: b.clone(),
                },
                None,
                now_unix(),
            );
        }

        let (after, _dir2) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            HashMap::from([(
                "GEMINI_KEYS".to_string(),
                vec!["key-a".to_string(), "key-c".to_string()],
            )]),
        );
        // The store B wrote to, read with the label C has after the restart.
        let c = HealthKey::ProviderKey {
            provider: "gemini".to_string(),
            key_id: after.pair_id(&provider, 1),
        };
        assert!(
            before.health.get(&c).is_none(),
            "C must start clean, not with B's retry counter"
        );
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
                created: None,
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
            models: Vec::new(),
            max_per_channel: None,
        };
        let (prober, _dir) = prober_for(router, vec![provider], HashMap::new());

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

    /// DESIGN.md's unit-of-health table: a provider is unhealthy when its
    /// reachability probe fails **or** every key on it is unhealthy. The probe
    /// path only recorded the pair failures, so a reachable provider whose
    /// billing lapsed (every key answers `401`) stayed marked healthy with
    /// zero channels, and the poller kept making one keyed call per key per
    /// backoff period instead of one keyless reachability call.
    #[test]
    fn every_key_failing_takes_the_provider_down() {
        let provider = Provider {
            name: "openrouter".to_string(),
            wire: WireName::OpenAi,
            base_url: "http://127.0.0.1:1".to_string(),
            keys_env: "OPENROUTER_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "OPENROUTER_KEYS".to_string(),
            vec!["one".to_string(), "two".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        // Wave 1 passed: the host answers, so the provider starts healthy.
        let provider_key = HealthKey::Provider("openrouter".to_string());
        prober.health.record_success(&provider_key);

        let rejected = || ProbeOutcome {
            alive: true,
            models: Vec::new(),
            retry_after_secs: None,
            error: Some("401".to_string()),
        };

        prober.apply_keyed_outcome(&provider, 0, rejected());
        assert!(
            prober
                .health
                .get(&provider_key)
                .is_none_or(|e| e.retry_counter == 0),
            "one dead key must not take its provider down while another key is untested"
        );

        prober.apply_keyed_outcome(&provider, 1, rejected());
        assert!(
            prober
                .health
                .get(&provider_key)
                .is_some_and(|e| e.retry_counter > 0),
            "with every key unhealthy the provider is unhealthy, so the poller goes back to one keyless reachability call"
        );
    }

    /// DESIGN.md, "Wrong turns already taken": never filter GET /v1/models by
    /// health. A pair whose keyed probe fails must keep its models in the
    /// catalogue, held back by the pair's backoff, so `/v1/models` still shows
    /// them with `healthy: 0` and a `next_eligible` time.
    #[test]
    fn a_failed_keyed_probe_keeps_the_models_listed_but_out_of_the_race() {
        let provider = Provider {
            name: "zai".to_string(),
            wire: WireName::OpenAi,
            base_url: "http://127.0.0.1:1".to_string(),
            keys_env: "ZAI_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([("ZAI_KEYS".to_string(), vec!["one".to_string()])]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );
        let listed = || ProbeOutcome {
            alive: true,
            models: vec![ProbedModel {
                created: None,
                id: "glm-5.3".to_string(),
                limit_context: None,
                limit_output: None,
            }],
            retry_after_secs: None,
            error: None,
        };

        assert!(prober.apply_keyed_outcome(&provider, 0, listed()));
        prober.rebuild(false);

        let quota_gone = ProbeOutcome {
            alive: true,
            models: Vec::new(),
            retry_after_secs: Some(3 * 3_600),
            error: Some("429".to_string()),
        };
        assert!(prober.apply_keyed_outcome(&provider, 0, quota_gone));
        prober.rebuild(false);

        let now = now_unix();
        let rows = prober.catalogue.snapshot();
        assert_eq!(rows.len(), 1, "the broken model must stay listed");
        assert!(
            !rows[0].eligible(now),
            "a pair in backoff must not be raced"
        );
        assert!(
            rows[0].health.not_before() > now + 3_600,
            "the row carries the pair's Retry-After as its next_eligible time"
        );

        // The key comes back with the same list: the row is callable again.
        assert!(prober.apply_keyed_outcome(&provider, 0, listed()));
        prober.rebuild(false);
        assert!(prober.catalogue.snapshot()[0].eligible(now_unix()));
    }

    /// A server that answers every request `401`, which is what both a keyless
    /// `/models` and a `/models` with an expired key give back. It counts the
    /// connections it accepts, so a test can assert that no call was made.
    async fn refusing_server() -> (String, Arc<AtomicU32>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port binds");
        let addr = listener.local_addr().expect("the port is known");
        let calls = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&calls);
        let handle = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let _ = socket
                        .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (format!("http://{addr}"), calls, handle)
    }

    /// A provider that is unhealthy because every key on it expired is not a
    /// provider whose host is down, and the keyless reachability probe cannot
    /// tell the two apart: a keyless `/models` answers `401`, which counts as
    /// alive. Clearing the provider on that reset its backoff to one second
    /// and force-probed every key past its own deadline, which re-escalated
    /// the provider and started the whole thing again on the next tick. The
    /// poller must leave the pairs to their own backoff instead.
    #[tokio::test]
    async fn every_key_down_is_not_cleared_by_the_keyless_probe() {
        let (base_url, calls, server) = refusing_server().await;

        let provider = Provider {
            name: "openrouter".to_string(),
            wire: WireName::OpenAi,
            base_url,
            keys_env: "OPENROUTER_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "OPENROUTER_KEYS".to_string(),
            vec!["one".to_string(), "two".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let now = now_unix();
        let provider_key = HealthKey::Provider("openrouter".to_string());
        let pairs: Vec<HealthKey> = (0..2)
            .map(|index| HealthKey::ProviderKey {
                provider: "openrouter".to_string(),
                key_id: prober.pair_id(&provider, index),
            })
            .collect();
        // Both keys have failed several times, so both are well inside their
        // own backoff. The provider went down with them and is out of its own.
        for pair in &pairs {
            for _ in 0..4 {
                prober.health.record_failure(pair, None, now);
            }
        }
        prober.health.record_failure(&provider_key, None, 0);
        let before: Vec<u32> = pairs
            .iter()
            .map(|p| {
                prober
                    .health
                    .get(p)
                    .expect("the pair is down")
                    .retry_counter
            })
            .collect();

        prober.poll_once().await;

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no key is out of backoff, so the pass must make no call at all"
        );
        let after: Vec<u32> = pairs
            .iter()
            .map(|p| {
                prober
                    .health
                    .get(p)
                    .expect("the pair is still down")
                    .retry_counter
            })
            .collect();
        assert_eq!(
            before, after,
            "a pair inside its own backoff must not be probed"
        );
        assert!(
            prober
                .health
                .get(&provider_key)
                .is_some_and(|e| e.retry_counter > 0),
            "a provider down because every key is down stays down until a key answers"
        );

        server.abort();
    }

    /// DESIGN.md: "No list, no channels." A keyed `/models` call that answers
    /// `200` with an empty list used to count as a success: the pair was
    /// recorded healthy, stored an empty model list, produced zero channels,
    /// and `poll_keys` then skipped it forever because it was healthy and
    /// already known. With no channels there is no live traffic to correct it,
    /// so the capacity was gone until a restart.
    #[tokio::test]
    async fn an_empty_model_list_is_a_failed_probe() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"object": "list", "data": []})),
            )
            .mount(&server)
            .await;

        let provider = Provider {
            name: "openrouter".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "OPENROUTER_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "OPENROUTER_KEYS".to_string(),
            vec!["sk-not-a-real-key".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let outcome = prober.probe_models(&provider, "sk-not-a-real-key").await;
        assert!(
            !outcome.succeeded(),
            "a 200 with no models must not count as a successful probe"
        );

        prober.apply_keyed_outcome(&provider, 0, outcome);
        let pair = HealthKey::ProviderKey {
            provider: "openrouter".to_string(),
            key_id: prober.pair_id(&provider, 0),
        };
        assert!(
            prober
                .health
                .get(&pair)
                .is_some_and(|e| e.retry_counter > 0),
            "the pair must be in backoff so the poller retries it"
        );
        assert!(
            !prober.has_model_list(&provider, 0),
            "an empty list must not be stored, or the poller skips the pair forever"
        );
    }

    /// Cloudflare Workers AI has no `GET /models`: the OpenAI-compatible path
    /// accepts POST only and answers 405, which lost every channel on all five
    /// Cloudflare accounts in the first live run. A provider that lists its
    /// models in `providers.yml` uses that list instead.
    #[tokio::test]
    async fn a_provider_with_no_listing_endpoint_uses_its_configured_models() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(405))
            .mount(&server)
            .await;

        let provider = Provider {
            name: "cloudflare-b4b050cc".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "CLOUDFLARE_B4B050CC_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: vec!["@cf/meta/llama-3.1-8b-instruct".to_string()],
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "CLOUDFLARE_B4B050CC_KEYS".to_string(),
            vec!["not-a-real-key".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let outcome = prober.probe_models(&provider, "not-a-real-key").await;
        assert!(outcome.succeeded(), "405 with a configured list is a pass");
        assert_eq!(
            outcome
                .models
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>(),
            vec!["@cf/meta/llama-3.1-8b-instruct"]
        );

        prober.apply_keyed_outcome(&provider, 0, outcome);
        assert!(
            prober.has_model_list(&provider, 0),
            "the configured list must be stored like a probed one"
        );
    }

    /// DESIGN.md filter check 3: OpenRouter's `/models` is not scoped to the
    /// key, so a free key must lose the paid rows before they become channels.
    /// A paying key keeps them all.
    #[tokio::test]
    async fn a_free_key_gets_no_channels_for_paid_models() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {"id": "paid/model", "pricing": {"prompt": "0.000003", "completion": "0.000015"}},
                    {"id": "openrouter/auto", "pricing": {"prompt": "-1", "completion": "-1"}},
                    {"id": "free/model:free", "pricing": {"prompt": "0", "completion": "0"}},
                ]
            })))
            .mount(&server)
            .await;
        for (key, free) in [("free-key", true), ("paid-key", false)] {
            Mock::given(method("GET"))
                .and(path("/key"))
                .and(header("authorization", format!("Bearer {key}").as_str()))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"data": {"is_free_tier": free}})),
                )
                .mount(&server)
                .await;
        }

        let provider = Provider {
            name: "openrouter".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "OPENROUTER_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "OPENROUTER_KEYS".to_string(),
            vec!["free-key".to_string(), "paid-key".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let ids =
            |outcome: ProbeOutcome| outcome.models.into_iter().map(|m| m.id).collect::<Vec<_>>();
        assert_eq!(
            ids(prober.probe_models(&provider, "free-key").await),
            vec!["free/model:free"]
        );
        assert_eq!(
            ids(prober.probe_models(&provider, "paid-key").await),
            vec!["paid/model", "openrouter/auto", "free/model:free"]
        );
    }

    /// An auth failure must still fail, configured models or not, or a revoked
    /// key would produce channels that 401 on every real request.
    #[tokio::test]
    async fn configured_models_do_not_rescue_a_rejected_key() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let provider = Provider {
            name: "cloudflare-60bf2f13".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "CLOUDFLARE_60BF2F13_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: vec!["@cf/meta/llama-3.1-8b-instruct".to_string()],
            max_per_channel: None,
        };
        let keys = HashMap::from([(
            "CLOUDFLARE_60BF2F13_KEYS".to_string(),
            vec!["revoked".to_string()],
        )]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let outcome = prober.probe_models(&provider, "revoked").await;
        assert!(!outcome.succeeded(), "401 is still a failed probe");
        assert!(outcome.models.is_empty());
    }

    /// A provider with no `models` list is marked as having a listing
    /// endpoint, so a 404 there is a wrong base URL, not a missing endpoint,
    /// and the probe must fail rather than come up healthy.
    #[tokio::test]
    async fn a_404_without_configured_models_fails_the_probe() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let provider = Provider {
            name: "groq".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "GROQ_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: Vec::new(),
            max_per_channel: None,
        };
        let keys = HashMap::from([("GROQ_KEYS".to_string(), vec!["k".to_string()])]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );

        let outcome = prober.probe_models(&provider, "k").await;
        assert!(
            !outcome.succeeded(),
            "404 with no configured list is a fail"
        );
        assert!(outcome.models.is_empty());
    }

    /// The fallback must not be silent. A 404 on `/models` is also what a base
    /// URL missing its version segment gives, and the configured list cannot
    /// tell the two apart, so the warning naming the provider, the status and
    /// the full URL is the only thing left pointing at the `BASE_URL_FIXUPS`
    /// list in `src/bin/make-config.rs`.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn the_configured_model_fallback_warns_with_the_url_that_answered() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/models"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let provider = Provider {
            name: "groq".to_string(),
            wire: WireName::OpenAi,
            base_url: server.uri(),
            keys_env: "GROQ_KEYS".to_string(),
            models_path: "/models".to_string(),
            models: vec!["llama-3.3-70b".to_string()],
            max_per_channel: None,
        };
        let keys = HashMap::from([("GROQ_KEYS".to_string(), vec!["k".to_string()])]);
        let (prober, _dir) = prober_for(
            crate::config::RouterConfig::default(),
            vec![provider.clone()],
            keys,
        );
        let outcome = prober.probe_models(&provider, "k").await;
        let uri = server.uri();

        assert!(outcome.succeeded(), "the fallback itself still passes");
        // tracing-test installs one global subscriber, so the capture does not
        // race other tests for the callsite interest cache the way a scoped
        // `with_default` subscriber did.
        logs_assert(|lines: &[&str]| {
            lines
                .iter()
                .any(|line| {
                    line.contains(" WARN ")
                        && line.contains("provider=groq")
                        && line.contains("status=404")
                        && line.contains(&format!("url={uri}/models"))
                })
                .then_some(())
                .ok_or_else(|| format!("no warning logged: {lines:?}"))
        });
    }
}
