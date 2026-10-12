//! The race.
//!
//! N channels in flight, M hedged copies each. `hedge_delay_ms` is 0 by
//! default, so the copies fire with the first one.
//!
//! The M copies are one trial, not M. Any copy returning 2xx with content makes
//! the channel healthy. Every copy failing bumps `retry_counter` by 1, and the
//! longest `Retry-After` among them sets `retry_base`.
//!
//! A sliding window, not waves: when a channel call fails, the next candidate
//! starts at once, keeping N in flight. The first 2xx with non-empty content
//! wins and the cancellation token tears down everything else.
//!
//! The network test runs in the same `select!`. Both test calls failing means
//! the fault is local: no `retry_counter` is touched, the walk is cancelled,
//! and the answer is `502`.
//!
//! Because the test answers at its own pace, a failed trial does not write its
//! health move straight away. It hands the move back and the walk holds it
//! until the link state is known: applied once the link is proven up, dropped
//! when it is down.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::stream::{FuturesUnordered, Stream, StreamExt};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::catalogue::{Catalogue, Channel, ChannelFilter, ChannelId};
use crate::config::RouterConfig;
use crate::health::{HealthKey, HealthTable, StreamVerdict};
use crate::metrics::{AttemptOutcome, Metrics};
use crate::net::{LinkState, NetTest};
use crate::server::PromptBody;
use crate::upstream::{CallError, SharedBody, Upstream};
use crate::wire::{self, ChunkStream, NeutralResponse};

/// Everything one client request hands the race.
pub struct RaceInput {
    /// The body every channel call sends, encoded once and shared. It is
    /// either held in RAM or spilled to a scratch file, and each channel call
    /// asks for it when it is about to upload.
    pub outbound: Arc<PromptBody>,
    /// What a channel must satisfy to enter the race.
    pub filter: ChannelFilter,
    /// True when the client asked for a stream.
    pub streaming: bool,
    /// The caller's model, for the `404` when nothing matches and for the
    /// cache rules. `auto` and no model both mean `None`.
    pub named_model: Option<String>,
}

/// A channel call that returned 2xx with non-empty content.
#[derive(Debug)]
pub struct Winner {
    /// Which channel produced it.
    pub channel_id: ChannelId,
    /// Which provider produced it.
    pub provider: String,
    /// Which model produced it.
    pub model_id: String,
    /// That model's intelligence score, for the cache write rule.
    pub intelligence: u16,
    /// The decoded neutral response.
    pub response: NeutralResponse,
    /// Wall clock of the winning channel call.
    pub latency: Duration,
}

/// A streamed winner. The chunks are neutral; the server encodes them into the
/// client's format at the edge.
pub struct StreamWinner {
    /// Which channel produced it.
    pub channel_id: ChannelId,
    /// Which provider produced it.
    pub provider: String,
    /// Which model produced it.
    pub model_id: String,
    /// That model's intelligence score.
    pub intelligence: u16,
    /// The neutral chunk stream.
    pub chunks: ChunkStream,
    /// Wall clock of the winning channel call up to its first bytes.
    pub latency: Duration,
}

/// How the race ended.
pub enum RaceOutcome {
    /// A channel answered with a whole reply. `200`.
    ///
    /// `also_finished` holds every other channel call that had already
    /// returned 2xx with content at the moment of cancellation. They are
    /// already paid for, and one may come from a smarter model than the
    /// winner, so the cache picks the best of the set.
    Buffered {
        /// The answer served to the client, which is the fastest one.
        winner: Box<Winner>,
        /// Everything else that finished before the cancellation landed.
        also_finished: Vec<Winner>,
        /// Channel calls made.
        attempts: u32,
    },
    /// A channel started streaming. `200`.
    Streamed {
        /// The stream served to the client.
        winner: Box<StreamWinner>,
        /// Channel calls made.
        attempts: u32,
    },
    /// Both network test calls failed. `502`, and no channel's health moved.
    NoNetwork {
        /// Which test URLs failed, for the body.
        failed: Vec<String>,
    },
    /// The query returned nothing, or every channel call failed. `504`.
    Exhausted {
        /// Channel calls made.
        attempts: u32,
        /// The last upstream error, for the body.
        last_error: Option<String>,
    },
    /// A winning stream died after the client already had part of the answer.
    /// There is no second chance: the client request ends here.
    ///
    /// The race never returns this itself: once [`RaceOutcome::Streamed`] is
    /// handed back, the server owns the stream and is the only thing that can
    /// see it die. A stream that dies *before* any chunk is handed over is an
    /// ordinary failed trial and the walk carries on, which is what keeps the
    /// "no second chance" rule to streams the client has already started
    /// reading.
    StreamDied {
        /// Which channel's stream died.
        channel_id: ChannelId,
        /// What killed it.
        error: String,
    },
    /// The client went away. Everything in flight was cancelled.
    ClientGone,
}

/// Runs races over the shared catalogue, health table and client.
#[derive(Debug)]
pub struct Racer {
    config: Arc<RouterConfig>,
    catalogue: Arc<Catalogue>,
    health: Arc<HealthTable>,
    upstream: Upstream,
    net: NetTest,
    metrics: Arc<Metrics>,
}

impl Racer {
    /// Builds a racer over the shared state.
    pub fn new(
        config: Arc<RouterConfig>,
        catalogue: Arc<Catalogue>,
        health: Arc<HealthTable>,
        upstream: Upstream,
        net: NetTest,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            catalogue,
            health,
            upstream,
            net,
            metrics,
        }
    }

    /// The shared client, so the prober reuses this pool rather than opening
    /// a second set of connections to the same provider hosts.
    pub fn upstream(&self) -> &Upstream {
        &self.upstream
    }

    /// One client request's whole walk.
    ///
    /// Opens with one query at `LIMIT N`, fires all N with M copies each, then
    /// replaces each failed channel with one query at `LIMIT 1` so N stays in
    /// flight. Keeps going until a winner, an empty query, or `cancel` firing.
    ///
    /// `cancel` is the client-disconnect token. Firing it tears down every
    /// channel call at once.
    pub async fn run(&self, input: RaceInput, cancel: CancellationToken) -> RaceOutcome {
        let snapshot = self.catalogue.snapshot();
        // A child token, so the race can tear its own calls down on a win
        // without touching the client's token.
        let race_cancel = cancel.child_token();

        let width = self.config.race_width.max(1);
        let copies = self.config.max_per_channel.max(1) as u32;

        let mut excluded: Excluded = HashSet::new();
        let mut attempts: u32 = 0;
        let mut last_error: Option<String> = None;
        // Health moves owed by trials that failed before the network test
        // answered. They land only once the link is proven up, and are dropped
        // when it is down, because a local fault is not the channel's.
        let mut held: Vec<PendingFailure> = Vec::new();
        let mut link_up = false;

        let mut inflight = FuturesUnordered::new();
        for channel in
            crate::catalogue::query(&snapshot, &input.filter, &excluded, width, now_unix())
        {
            excluded.insert(channel.id);
            attempts = attempts.saturating_add(copies);
            inflight.push(self.run_trial(
                channel,
                Arc::clone(&input.outbound),
                input.streaming,
                race_cancel.child_token(),
            ));
        }

        if inflight.is_empty() {
            // The filter emptied the list before a single channel call. Nothing
            // to cancel and no health to move.
            return RaceOutcome::Exhausted {
                attempts: 0,
                last_error: None,
            };
        }

        // The network test runs beside the channel calls, not after a failure,
        // so a dead link is known at the same moment the failures arrive.
        let mut net_check = Box::pin(self.net.check());
        let mut net_done = false;

        loop {
            let finished: TrialOutcome<'_> = tokio::select! {
                biased;

                _ = cancel.cancelled() => {
                    race_cancel.cancel();
                    return RaceOutcome::ClientGone;
                }

                link = &mut net_check, if !net_done => {
                    net_done = true;
                    if let LinkState::Down { failed } = link {
                        // Local fault. No channel's counter moves and the walk
                        // stops here.
                        race_cancel.cancel();
                        return RaceOutcome::NoNetwork { failed };
                    }
                    // The link is up, so the failures held back so far really
                    // were the channels' own, and they can be written now.
                    link_up = true;
                    self.apply_held(&mut held);
                    continue;
                }

                Some(outcome) = inflight.next(), if !inflight.is_empty() => outcome,

                else => {
                    let outcome =
                        exhausted(net_check.as_mut(), net_done, attempts, last_error).await;
                    self.settle_held(&mut held, &outcome);
                    return outcome;
                }
            };

            // The borrow of `inflight` ends with the select, so the top-up
            // below is free to push.
            match finished {
                TrialOutcome::Won(winner) => {
                    race_cancel.cancel();
                    // Anything that had already returned 2xx with content is
                    // paid for either way, and one of them may come from a
                    // smarter model than the winner.
                    let mut also_finished = Vec::new();
                    while let Some(rest) = inflight.next().await {
                        match rest {
                            TrialOutcome::Won(other) => also_finished.push(*other),
                            TrialOutcome::Failed {
                                pending: Some(pending),
                                ..
                            } => held.push(pending),
                            _ => {}
                        }
                    }
                    // A channel answered, so the link is up whatever the test
                    // is still doing. The held failures are owed.
                    self.apply_held(&mut held);
                    // The latency histogram is recorded once, by the
                    // handler, after translation and the cache write. Recording
                    // it here as well would double every count and leave the
                    // router stage short of the work that happens after the
                    // race returns.
                    return RaceOutcome::Buffered {
                        winner,
                        also_finished,
                        attempts,
                    };
                }
                TrialOutcome::WonStreaming(winner) => {
                    // Dropping the losing trials tears their sockets down; the
                    // winner's stream has already moved out of its trial.
                    race_cancel.cancel();
                    // Trials that had already failed before the token fired
                    // are real channel failures, not cancellations, and they
                    // are owed the same backoff as on the buffered path. The
                    // set is drained without waiting, so the stream is not
                    // held up by trials still in flight.
                    hold_finished_failures(&mut inflight, &mut held);
                    // A channel streamed bytes, so the link is up.
                    self.apply_held(&mut held);
                    return RaceOutcome::Streamed { winner, attempts };
                }
                TrialOutcome::Cancelled => {}
                TrialOutcome::Failed { error, pending } => {
                    last_error = Some(error);
                    if let Some(pending) = pending {
                        if link_up {
                            self.apply_failure(pending.channel, pending.retry_after_secs);
                        } else {
                            // The test has not answered yet. This may be a
                            // local fault, so the counter stays where it is
                            // until the link state is known.
                            held.push(pending);
                        }
                    }
                    // Replace the failed channel at once. One finishes, one
                    // starts: a sliding window of N, not waves of N.
                    if let Some(channel) =
                        // The clock is read here, not at the start of the
                        // request: a channel that came out of backoff while
                        // this walk was running is eligible again.
                        crate::catalogue::query(
                            &snapshot,
                            &input.filter,
                            &excluded,
                            1,
                            now_unix(),
                        )
                        .first()
                        .copied()
                    {
                        excluded.insert(channel.id);
                        attempts = attempts.saturating_add(copies);
                        inflight.push(self.run_trial(
                            channel,
                            Arc::clone(&input.outbound),
                            input.streaming,
                            race_cancel.child_token(),
                        ));
                    }
                }
            }

            if inflight.is_empty() {
                let outcome = exhausted(net_check.as_mut(), net_done, attempts, last_error).await;
                self.settle_held(&mut held, &outcome);
                return outcome;
            }
        }
    }

    /// One channel's trial: M copies of the same call, `hedge_delay_ms` apart.
    ///
    /// The first copy to produce an answer wins and the rest are cancelled.
    ///
    /// The whole trial counts once against health, whatever M is.
    async fn run_trial<'a>(
        &self,
        channel: &'a Channel,
        body: Arc<PromptBody>,
        streaming: bool,
        cancel: CancellationToken,
    ) -> TrialOutcome<'a> {
        let copies = self.config.max_per_channel.max(1);
        let delay = Duration::from_millis(self.config.hedge_delay_ms);
        // Our own open copies, so a `429` can tell a sibling copy apart from
        // another client request hitting the same channel.
        let own = Arc::new(AtomicU32::new(0));

        let mut running = FuturesUnordered::new();
        for index in 0..copies {
            let body = Arc::clone(&body);
            let own = own.clone();
            let offset = hedge_offset(index, delay);
            running.push(async move {
                if !offset.is_zero() {
                    tokio::time::sleep(offset).await;
                }
                // Read as late as possible and dropped as soon as the upload
                // is done, so a spilled body is only resident for the upload.
                let body = match body.load().await {
                    Ok(body) => SharedBody::new(body),
                    Err(error) => {
                        // A scratch disk fault is ours, not the channel's, so
                        // it must not move the channel's health.
                        return Err(CopyFailure {
                            error: format!("outbound body could not be read back: {error}"),
                            retry_after_secs: None,
                            self_inflicted: true,
                        });
                    }
                };
                if streaming {
                    self.copy_streamed(channel, body, &own).await
                } else {
                    self.copy_buffered(channel, body, &own).await
                }
            });
        }

        let mut failures: Vec<CopyFailure> = Vec::new();

        loop {
            // The select is biased on cancellation, so a copy that finished
            // just before the token fired would never be polled. Handle the
            // cancellation after the select, once the borrow of `running` has
            // ended, so those copies can still be collected.
            let (cancelled, next) = tokio::select! {
                biased;
                _ = cancel.cancelled() => (true, None),
                next = running.next() => (false, next),
            };

            if cancelled {
                // A copy that already returned is paid for either way, and may
                // come from a smarter model than whatever won the race.
                if let Some(win) = finished_now(&mut running).into_iter().flatten().next() {
                    return self.trial_won(channel, win);
                }
                self.metrics
                    .record_attempt(channel.id, AttemptOutcome::Cancelled);
                return TrialOutcome::Cancelled;
            }

            match next {
                // The first copy home wins outright. A buffered reply is
                // complete, so there is nothing a later copy could buy that
                // waiting longer does not cost more of.
                Some(Ok(win)) => return self.trial_won(channel, win),
                Some(Err(failure)) => failures.push(failure),
                None => break,
            }
        }

        // Every copy failed, which is one failure against health, not M. The
        // health write itself is the walk's to make, not the trial's: until
        // the network test has answered, nobody knows whether the fault is the
        // channel's or the local link's.
        let self_inflicted = !failures.is_empty() && failures.iter().all(|f| f.self_inflicted);
        let retry_after_secs = failures.iter().filter_map(|f| f.retry_after_secs).max();
        TrialOutcome::Failed {
            error: failures
                .last()
                .map(|f| f.error.clone())
                .unwrap_or_else(|| "no copy ran".to_string()),
            pending: (!self_inflicted).then_some(PendingFailure {
                channel,
                retry_after_secs,
            }),
        }
    }

    /// Writes every health move held back while the link state was unknown.
    /// Only called once something has proven the link is up.
    fn apply_held(&self, held: &mut Vec<PendingFailure<'_>>) {
        settle_held(&self.health, &self.catalogue.snapshot(), held, false);
    }

    /// Settles the held moves against how the walk ended: a dead link drops
    /// them, anything else writes them.
    fn settle_held(&self, held: &mut Vec<PendingFailure<'_>>, outcome: &RaceOutcome) {
        let link_down = matches!(outcome, RaceOutcome::NoNetwork { .. });
        settle_held(&self.health, &self.catalogue.snapshot(), held, link_down);
    }

    /// One buffered copy of a channel call.
    async fn copy_buffered(
        &self,
        channel: &Channel,
        body: SharedBody,
        own: &Arc<AtomicU32>,
    ) -> Result<CopyWin, CopyFailure> {
        let _open = InFlight::open(channel, own);
        channel.telemetry.calls.fetch_add(1, Ordering::Relaxed);

        let call = match self.upstream.call_buffered(channel, body).await {
            Ok(call) => call,
            Err(error) => return Err(self.copy_failed(channel, error)),
        };

        channel
            .telemetry
            .last_latency_ms
            .store(call.latency.as_millis() as u64, Ordering::Relaxed);

        if call.status == 429
            && is_self_inflicted(
                channel.health.in_flight.load(Ordering::Relaxed),
                own.load(Ordering::Relaxed),
            )
        {
            // The router caused this one. Fail the call, keep the channel's
            // counters where they are.
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_attempt(channel.id, AttemptOutcome::HttpError);
            return Err(CopyFailure {
                error: "429 caused by this router's own concurrency".to_string(),
                retry_after_secs: None,
                self_inflicted: true,
            });
        }

        if !crate::health::is_healthy_response(call.status, &call.body) {
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_attempt(channel.id, AttemptOutcome::HttpError);
            return Err(CopyFailure {
                error: failure_reason(call.status, &call.body),
                retry_after_secs: call.retry_after_secs,
                self_inflicted: false,
            });
        }

        let format = wire::format_for_provider(channel.wire);
        let value = match serde_json::from_slice::<serde_json::Value>(&call.body) {
            Ok(value) => value,
            Err(error) => {
                channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .record_attempt(channel.id, AttemptOutcome::HttpError);
                return Err(CopyFailure {
                    error: format!("unparseable body: {error}"),
                    retry_after_secs: call.retry_after_secs,
                    self_inflicted: false,
                });
            }
        };
        let response = match wire::decode_response(&value, format) {
            Ok(response) => response,
            Err(error) => {
                channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .record_attempt(channel.id, AttemptOutcome::HttpError);
                return Err(CopyFailure {
                    error: format!("undecodable body: {error}"),
                    retry_after_secs: call.retry_after_secs,
                    self_inflicted: false,
                });
            }
        };

        self.metrics
            .record_attempt(channel.id, AttemptOutcome::Success);
        Ok(CopyWin::Buffered(Box::new(Winner {
            channel_id: channel.id,
            provider: channel.provider.to_string(),
            model_id: channel.model_id.to_string(),
            intelligence: channel.intelligence,
            response,
            latency: call.latency,
        })))
    }

    /// One streamed copy of a channel call.
    ///
    /// It returns as soon as the copy produces content, holding what it read
    /// so the caller can replay it ahead of the rest of the stream.
    async fn copy_streamed(
        &self,
        channel: &Channel,
        body: SharedBody,
        own: &Arc<AtomicU32>,
    ) -> Result<CopyWin, CopyFailure> {
        // The guard travels with the stream: a streamed call ends when the
        // stream ends, not when its first frame arrives.
        let open = InFlight::open(channel, own);
        channel.telemetry.calls.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let call = match self.upstream.call_streamed(channel, body).await {
            Ok(call) => call,
            Err(error) => return Err(self.copy_failed(channel, error)),
        };

        if call.status == 429
            && is_self_inflicted(
                channel.health.in_flight.load(Ordering::Relaxed),
                own.load(Ordering::Relaxed),
            )
        {
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_attempt(channel.id, AttemptOutcome::HttpError);
            return Err(CopyFailure {
                error: "429 caused by this router's own concurrency".to_string(),
                retry_after_secs: None,
                self_inflicted: true,
            });
        }

        if !(200..300).contains(&call.status) {
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .record_attempt(channel.id, AttemptOutcome::HttpError);
            return Err(CopyFailure {
                error: format!("status {}", call.status),
                retry_after_secs: call.retry_after_secs,
                self_inflicted: false,
            });
        }

        let mut rest = call.body;

        let buffered = match read_stream_start(&mut rest).await {
            Ok(start) => start,
            Err(failure) => {
                channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .record_attempt(channel.id, AttemptOutcome::HttpError);
                let (error, retry_after_secs) = match failure {
                    // The stream died before anything reached the client, so
                    // this is an ordinary failed trial and the walk carries on.
                    StreamStartError::Transport(error) => (error, None),
                    StreamStartError::NotAnAnswer(error) => (error, call.retry_after_secs),
                };
                return Err(CopyFailure {
                    error,
                    retry_after_secs,
                    self_inflicted: false,
                });
            }
        };

        self.metrics
            .record_attempt(channel.id, AttemptOutcome::Success);
        Ok(CopyWin::Streamed(Box::new(ReadyStream {
            channel_id: channel.id,
            provider: channel.provider.to_string(),
            model_id: channel.model_id.to_string(),
            intelligence: channel.intelligence,
            wire: channel.wire,
            buffered,
            elapsed: started.elapsed(),
            rest,
            open,
        })))
    }

    /// Turns a winning copy into a trial outcome, resetting the channel's
    /// health first. Any copy succeeding makes the channel healthy.
    fn trial_won<'a>(&self, channel: &'a Channel, win: CopyWin) -> TrialOutcome<'a> {
        self.apply_success(channel);
        match win {
            CopyWin::Buffered(winner) => TrialOutcome::Won(winner),
            CopyWin::Streamed(ready) => match ready.into_winner() {
                Ok(winner) => TrialOutcome::WonStreaming(Box::new(winner)),
                // The channel answered, so its backoff stays where it is.
                Err(error) => TrialOutcome::Failed {
                    error,
                    pending: None,
                },
            },
        }
    }

    /// A transport failure or a timeout on one copy.
    fn copy_failed(&self, channel: &Channel, error: CallError) -> CopyFailure {
        channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
        let outcome = match error {
            CallError::Timeout(_) => AttemptOutcome::Timeout,
            CallError::Cancelled => AttemptOutcome::Cancelled,
            CallError::Transport(_) => AttemptOutcome::HttpError,
        };
        self.metrics.record_attempt(channel.id, outcome);
        CopyFailure {
            error: error.to_string(),
            retry_after_secs: None,
            self_inflicted: false,
        }
    }

    /// Records one failed trial and mirrors the result onto the channel row, so
    /// the next query sees the backoff without waiting for anything else.
    fn apply_failure(&self, channel: &Channel, retry_after_secs: Option<u64>) {
        write_failure(
            &self.health,
            &self.catalogue.snapshot(),
            channel,
            retry_after_secs,
        );
    }

    /// Records a successful trial and clears the channel row's backoff.
    fn apply_success(&self, channel: &Channel) {
        channel.telemetry.successes.fetch_add(1, Ordering::Relaxed);
        self.health.record_success(&HealthKey::Channel(channel.id));
        // A channel answering proves its pair and its provider are alive, so
        // whatever an earlier escalation wrote above the channel clears too.
        self.health.record_success(&pair_key(channel));
        self.health
            .record_success(&HealthKey::Provider(channel.provider.to_string()));
        channel.health.retry_counter.store(0, Ordering::Relaxed);
        channel
            .health
            .retry_base_secs
            .store(self.health.retry_base_secs(), Ordering::Relaxed);
        channel.health.eligible_at.store(0, Ordering::Relaxed);
    }
}

/// The health key for the provider/key pair a channel belongs to.
fn pair_key(channel: &Channel) -> HealthKey {
    HealthKey::ProviderKey {
        provider: channel.provider.to_string(),
        key_id: channel.key_id.to_string(),
    }
}

/// True when the thing has a live failure count against it.
fn is_unhealthy(health: &HealthTable, key: &HealthKey) -> bool {
    health.get(key).is_some_and(|e| e.retry_counter > 0)
}

/// Settles the health moves a walk held back while the link state was unknown.
///
/// A dead link drops every one of them. The fault was local, so no channel's
/// counter may move for it, in flight or already failed: that is the whole
/// point of the `502`, and inflating counters here would leave the channels in
/// a backoff that was never theirs once the link came back. Any other ending
/// means a channel really did fail, so the moves are written.
fn settle_held(
    health: &HealthTable,
    channels: &[Channel],
    held: &mut Vec<PendingFailure<'_>>,
    link_down: bool,
) {
    for pending in held.drain(..) {
        if !link_down {
            write_failure(health, channels, pending.channel, pending.retry_after_secs);
        }
    }
}

/// Records one failed trial and mirrors the result onto the channel row, so the
/// next query sees the backoff without waiting for anything else.
fn write_failure(
    health: &HealthTable,
    channels: &[Channel],
    channel: &Channel,
    retry_after_secs: Option<u64>,
) {
    let now = now_unix();
    let entry = health.record_failure(&HealthKey::Channel(channel.id), retry_after_secs, now);
    channel
        .health
        .retry_counter
        .store(entry.retry_counter, Ordering::Relaxed);
    channel
        .health
        .retry_base_secs
        .store(entry.retry_base_secs, Ordering::Relaxed);
    channel
        .health
        .eligible_at
        .store(entry.eligible_at, Ordering::Relaxed);
    escalate_failure(health, channels, channel, retry_after_secs, now);
}

/// Walks a channel failure up the two levels above it: the provider/key pair
/// goes unhealthy when no channel on that pair is callable, and the provider
/// goes unhealthy when every key on it is already down.
///
/// Live traffic is the only thing that sees a key die after startup, and the
/// poller only re-probes a pair that is marked unhealthy. Without this step an
/// expired key keeps its startup "healthy" mark forever: every channel on it
/// backs off alone, and nothing ever refreshes its model list when the key is
/// reinstated upstream.
fn escalate_failure(
    health: &HealthTable,
    channels: &[Channel],
    failed: &Channel,
    retry_after_secs: Option<u64>,
    now: u64,
) {
    let pair = pair_key(failed);
    if is_unhealthy(health, &pair) {
        // Already with the poller. Bumping it again would push the re-probe
        // out for a fault the poller is the one meant to clear.
        return;
    }
    let pair_alive = channels
        .iter()
        .any(|c| c.provider == failed.provider && c.key_id == failed.key_id && c.eligible(now));
    if pair_alive {
        return;
    }
    health.record_failure(&pair, retry_after_secs, now);

    let provider = HealthKey::Provider(failed.provider.to_string());
    if is_unhealthy(health, &provider) {
        return;
    }
    let keys: HashSet<&str> = channels
        .iter()
        .filter(|c| c.provider == failed.provider)
        .map(|c| &*c.key_id)
        .collect();
    let every_key_down = keys.iter().all(|key_id| {
        is_unhealthy(
            health,
            &HealthKey::ProviderKey {
                provider: failed.provider.to_string(),
                key_id: (*key_id).to_string(),
            },
        )
    });
    if every_key_down {
        health.record_failure(&provider, retry_after_secs, now);
    }
}

/// A failed trial's health move, waiting on the network test.
///
/// It is kept apart from the write itself because only the walk knows whether
/// the link is up: writing it inside the trial would inflate the counter of
/// every channel tried during a local outage, which is exactly the fault
/// `NoNetwork` exists to keep off the channels.
pub struct PendingFailure<'a> {
    channel: &'a Channel,
    retry_after_secs: Option<u64>,
}

/// What one channel's trial produced, after the M copies have been reduced to
/// a single answer.
pub enum TrialOutcome<'a> {
    /// At least one copy returned 2xx with content.
    Won(Box<Winner>),
    /// At least one copy started streaming successfully.
    WonStreaming(Box<StreamWinner>),
    /// Every copy failed.
    Failed {
        /// The last error seen, for the `504` body.
        error: String,
        /// The health move this failure owes the channel, which the walk
        /// writes once it knows the link is up. `None` when the failure was
        /// the router's own doing, such as a self-inflicted `429` or a scratch
        /// disk fault: those never move the channel's backoff.
        pending: Option<PendingFailure<'a>>,
    },
    /// The trial was cancelled mid-flight. Cancelled is not unhealthy: the
    /// channel keeps its current state.
    Cancelled,
}

/// One copy that got somewhere.
enum CopyWin {
    /// A whole reply.
    Buffered(Box<Winner>),
    /// A stream that has produced bytes.
    Streamed(Box<ReadyStream>),
}

/// Why one copy did not produce a reply.
struct CopyFailure {
    error: String,
    retry_after_secs: Option<u64>,
    /// True for a `429` this router's own concurrency caused. Health does not
    /// move for those: a burst must not push the best channels out of the list.
    self_inflicted: bool,
}

/// A channel's raw reply bytes, still in that provider's framing.
type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, CallError>> + Send + 'static>>;

/// Why a stream's opening did not count as a win.
enum StreamStartError {
    /// The byte stream failed partway through the read.
    Transport(String),
    /// A 2xx that carried no answer: nothing but comments and keep-alives, or
    /// an error object dressed as a data frame.
    NotAnAnswer(String),
}

/// Reads a channel's event stream until it shows content or shows a failure,
/// and hands back what it read.
///
/// Content, not bytes, is the test. The buffered path uses
/// [`crate::health::is_healthy_response`] for the same reason: an empty 200, a
/// keep-alive comment or an error object all come back sooner than a real
/// first token, so counting bytes would hand the race to whichever channel was
/// quickest to say nothing.
async fn read_stream_start(rest: &mut ByteStream) -> Result<Vec<Vec<u8>>, StreamStartError> {
    let mut buffered: Vec<Vec<u8>> = Vec::new();
    let mut sse: Vec<u8> = Vec::new();
    let mut content = false;

    loop {
        match rest.next().await {
            Some(Ok(chunk)) => {
                if chunk.is_empty() {
                    continue;
                }
                sse.extend_from_slice(&chunk);
                buffered.push(chunk);
                // ponytail: rescans the whole opening on every chunk. That is
                // a few kilobytes before the first token. Carry a scan offset
                // if a provider ever sends thousands of frames before it.
                match crate::health::stream_verdict(&sse) {
                    StreamVerdict::Failed => {
                        return Err(StreamStartError::NotAnAnswer(
                            "200 carrying an error event".to_string(),
                        ))
                    }
                    // The first real content is the whole decision.
                    StreamVerdict::Content => {
                        content = true;
                        break;
                    }
                    StreamVerdict::Pending => {}
                }
            }
            Some(Err(error)) => return Err(StreamStartError::Transport(error.to_string())),
            None => break,
        }
    }

    if !content {
        return Err(StreamStartError::NotAnAnswer(
            "stream carried no content".to_string(),
        ));
    }
    Ok(buffered)
}

/// A streamed copy that has produced content, with whatever was read before
/// it showed.
struct ReadyStream {
    channel_id: ChannelId,
    provider: String,
    model_id: String,
    intelligence: u16,
    wire: crate::config::WireName,
    /// Everything read before the decision, replayed ahead of the rest.
    buffered: Vec<Vec<u8>>,
    elapsed: Duration,
    rest: Pin<Box<dyn Stream<Item = Result<Vec<u8>, CallError>> + Send + 'static>>,
    /// Holds the channel's in-flight count up until this stream is finished or
    /// dropped. The socket is open for the whole answer, so the count must say
    /// so, or a burst of streams looks idle to the self-inflicted `429` rule.
    open: InFlight,
}

impl ReadyStream {
    /// Replays what was buffered, then the rest of the live stream, through the
    /// neutral decoder.
    fn into_winner(self) -> Result<StreamWinner, String> {
        let open = self.open;
        let replayed = futures::stream::iter(self.buffered.into_iter().map(Ok)).chain(self.rest);
        let bytes = replayed.map(move |item| {
            // The closure owns the guard, so it lives exactly as long as the
            // stream the client is reading.
            let _open = &open;
            item.map_err(|e| wire::WireError::Stream(e.to_string()))
        });
        let chunks = wire::decode_stream(bytes, wire::format_for_provider(self.wire))
            .map_err(|e| e.to_string())?;
        Ok(StreamWinner {
            channel_id: self.channel_id,
            provider: self.provider,
            model_id: self.model_id,
            intelligence: self.intelligence,
            chunks,
            latency: self.elapsed,
        })
    }
}

/// Keeps the channel's in-flight count honest, cancellation included. The
/// `429` rule reads both numbers at the moment the status arrives, so a copy
/// that is dropped mid-call must still decrement.
///
/// It holds the two counters rather than borrowing the channel, so a streamed
/// call can carry its guard out of the race and drop it when the stream ends,
/// not when the first frame arrives.
struct InFlight {
    in_flight: Arc<AtomicU32>,
    own: Arc<AtomicU32>,
}

impl InFlight {
    fn open(channel: &Channel, own: &Arc<AtomicU32>) -> Self {
        channel.health.in_flight.fetch_add(1, Ordering::Relaxed);
        own.fetch_add(1, Ordering::Relaxed);
        Self {
            in_flight: Arc::clone(&channel.health.in_flight),
            own: Arc::clone(own),
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.own.fetch_sub(1, Ordering::Relaxed);
    }
}

/// When copy `index` starts, relative to the trial. `hedge_delay_ms` of 0 means
/// every copy fires with the first one.
fn hedge_offset(index: usize, delay: Duration) -> Duration {
    delay.saturating_mul(index as u32)
}

/// True when a `429` is this router's own doing: the channel had calls in
/// flight beyond this trial's own copies. M hedged copies of the same call do
/// not count as other calls in flight.
fn is_self_inflicted(in_flight: u32, own: u32) -> bool {
    in_flight.saturating_sub(own) > 0
}

/// Everything in `running` that has already finished, without waiting on the
/// rest. A call that returned before the cancellation landed is already paid
/// for, so it is collected rather than thrown away.
fn finished_now<F: std::future::Future>(running: &mut FuturesUnordered<F>) -> Vec<F::Output> {
    let mut done = Vec::new();
    // `now_or_never` polls the set once: anything ready completes, and a
    // pending set yields `None` and ends the drain.
    while let Some(Some(item)) = running.next().now_or_never() {
        done.push(item);
    }
    done
}

/// Takes the health moves owed by trials that had already failed before the
/// cancellation landed. Those are genuine channel failures, so they are owed
/// the same backoff as on the buffered path. The set is polled once, so trials
/// still in flight are left where they are and the caller is not held up.
fn hold_finished_failures<'a, F>(
    running: &mut FuturesUnordered<F>,
    held: &mut Vec<PendingFailure<'a>>,
) where
    F: std::future::Future<Output = TrialOutcome<'a>>,
{
    for rest in finished_now(running) {
        if let TrialOutcome::Failed {
            pending: Some(pending),
            ..
        } = rest
        {
            held.push(pending);
        }
    }
}

/// Of every channel call that finished before cancellation, the one from the
/// smartest model. That is the candidate the cache compares against what it
/// already holds.
pub fn best_for_cache<'a>(winner: &'a Winner, also_finished: &'a [Winner]) -> &'a Winner {
    also_finished.iter().fold(winner, |best, other| {
        if other.intelligence > best.intelligence {
            other
        } else {
            best
        }
    })
}

/// The ids already tried, which the next query excludes. Ids are hashes, not
/// indices, so the set still works after the catalogue is rebuilt underneath.
pub type Excluded = HashSet<ChannelId>;

/// The walk has run out of channel calls. The answer is `504` only if the link
/// is up.
///
/// A small catalogue, or one mostly in backoff, drains in milliseconds while
/// the two network test calls are still on the wire. Returning `504` then
/// blames the providers for what may be a dead local link, and `502` and `504`
/// exist precisely so the caller can tell those apart. So the pending test gets
/// the last word. It carries its own 2 second timeout, so this cannot hang.
async fn exhausted<F>(
    net_check: Pin<&mut F>,
    net_done: bool,
    attempts: u32,
    last_error: Option<String>,
) -> RaceOutcome
where
    F: std::future::Future<Output = LinkState>,
{
    if !net_done {
        if let LinkState::Down { failed } = net_check.await {
            // Local fault. Nothing is in flight to cancel and no channel's
            // counter moves.
            return RaceOutcome::NoNetwork { failed };
        }
    }
    RaceOutcome::Exhausted {
        attempts,
        last_error,
    }
}

/// Why a buffered reply did not count as a win, for the `504` body.
///
/// The status on its own is not a reason. A provider that signals quota
/// exhaustion as a `200` carrying `{"error":{"message":"insufficient quota"}}`
/// would be reported as "status 200", which names a success and throws away
/// the one line that tells an operator what is wrong. So the provider's own
/// message is carried across whenever the body holds one.
fn failure_reason(status: u16, body: &[u8]) -> String {
    let Some(message) = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|json| error_message(&json))
    else {
        return format!("status {status}");
    };
    // Bodies can be long. One line is enough to diagnose from.
    let message: String = message.chars().take(ERROR_MESSAGE_CHARS).collect();
    format!("status {status}: {message}")
}

/// How much of an upstream error message reaches the `504` body.
const ERROR_MESSAGE_CHARS: usize = 200;

/// The readable part of an upstream error object, in either shape:
/// `{"error":{"message":"..."}}` or `{"error":"..."}`.
fn error_message(json: &serde_json::Value) -> Option<String> {
    let error = json.get("error")?;
    if let Some(text) = error.as_str() {
        return non_empty(text);
    }
    if let Some(text) = error.get("message").and_then(|message| message.as_str()) {
        return non_empty(text);
    }
    // No message field, but an error object is still worth printing whole.
    if error.is_object() {
        return non_empty(&error.to_string());
    }
    None
}

fn non_empty(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::catalogue::{ChannelHealth, ChannelTelemetry};

    /// A row on one provider/key pair. Only the fields escalation reads vary.
    fn row(provider: &str, key_id: &str, model: &str) -> Channel {
        Channel {
            id: ChannelId::new(provider, key_id, model),
            provider: provider.into(),
            model_id: model.into(),
            key_id: key_id.into(),
            key: "secret".into(),
            base_url: "https://example.invalid/v1".into(),
            wire: crate::config::WireName::OpenAi,
            limit_context: 128_000,
            limit_output: 8_192,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_string()],
            intelligence: 100,
            intelligence_rank: 0,
            health: ChannelHealth::live(),
            telemetry: ChannelTelemetry::default(),
        }
    }

    /// Several providers answer a dead key or an empty quota with a `200`
    /// whose body holds the error. Reporting that as "status 200" hands the
    /// operator a success code and no reason, so the message has to survive.
    #[test]
    fn a_two_hundred_carrying_an_error_reports_the_upstream_message() {
        let body = br#"{"error":{"message":"insufficient quota","type":"insufficient_quota"}}"#;
        assert_eq!(
            failure_reason(200, body),
            "status 200: insufficient quota".to_string()
        );

        // The flat shape some gateways use.
        assert_eq!(
            failure_reason(200, br#"{"error":"bad model name"}"#),
            "status 200: bad model name".to_string()
        );

        // Nothing to add: the status stands on its own.
        assert_eq!(failure_reason(503, b"upstream down"), "status 503");
        assert_eq!(failure_reason(200, b"{}"), "status 200");
    }

    /// A health table over a throwaway redb file. Removed on drop.
    fn health_table() -> (HealthTable, std::path::PathBuf) {
        use std::sync::atomic::AtomicU64;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("llmrouter-race-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        let db = redb::Database::create(dir.join("health.redb")).unwrap();
        let table = HealthTable::open(
            std::sync::Arc::new(db),
            Duration::from_secs(1),
            Duration::from_secs(3600),
        )
        .unwrap();
        (table, dir)
    }

    /// Marks a row unhealthy the way a failed trial does, without a Racer.
    fn fail_channel(health: &HealthTable, channel: &Channel, now: u64) {
        let entry = health.record_failure(&HealthKey::Channel(channel.id), None, now);
        channel
            .health
            .eligible_at
            .store(entry.eligible_at, Ordering::Relaxed);
    }

    /// A streaming win cancels the other trials, but a trial that had already
    /// failed before the token fired is a real channel failure, not a
    /// cancellation. It owes the channel its backoff exactly as it would on
    /// the buffered path, so the walk must take it off the set before the set
    /// is dropped.
    #[tokio::test]
    async fn a_streaming_win_still_takes_the_failures_that_had_already_landed() {
        let failed = row("alpha", "k1", "m");
        let mut inflight: FuturesUnordered<
            Pin<Box<dyn std::future::Future<Output = TrialOutcome<'_>> + Send>>,
        > = FuturesUnordered::new();
        inflight.push(Box::pin(async {
            TrialOutcome::Failed {
                error: "status 401".to_string(),
                pending: Some(PendingFailure {
                    channel: &failed,
                    retry_after_secs: None,
                }),
            }
        }));
        // A trial still in flight: the drain must not wait on it.
        inflight.push(Box::pin(async {
            std::future::pending::<()>().await;
            TrialOutcome::Cancelled
        }));

        let mut held = Vec::new();
        hold_finished_failures(&mut inflight, &mut held);

        assert_eq!(held.len(), 1, "the 401 that landed first is owed a backoff");
        assert_eq!(held[0].channel.id, failed.id);
    }

    /// The channel calls can all fail in milliseconds while the two network
    /// test calls are still resolving. The answer then has to wait for the
    /// test: `502` means the local link is dead, `504` means the providers are
    /// not answering, and the caller must be able to tell those apart.
    #[tokio::test]
    async fn a_drained_walk_waits_for_a_pending_network_test() {
        let mut check = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            LinkState::Down {
                failed: vec!["https://example.invalid: dns".to_string()],
            }
        });
        let outcome = exhausted(check.as_mut(), false, 4, Some("status 500".to_string())).await;
        match outcome {
            RaceOutcome::NoNetwork { failed } => assert_eq!(failed.len(), 1),
            _ => panic!("a dead link while the walk drained must be 502, not 504"),
        }
    }

    /// The test already said the link is up, so a drained walk is the
    /// providers' fault and nothing is waited on.
    #[tokio::test]
    async fn a_drained_walk_with_the_link_up_is_exhausted() {
        let mut check = Box::pin(async { LinkState::Up });
        let outcome = exhausted(check.as_mut(), true, 4, Some("status 500".to_string())).await;
        match outcome {
            RaceOutcome::Exhausted {
                attempts,
                last_error,
            } => {
                assert_eq!(attempts, 4);
                assert_eq!(last_error.as_deref(), Some("status 500"));
            }
            _ => panic!("a live link with dead upstreams must be 504"),
        }
    }

    /// A key that expires while the router is running only ever shows up as
    /// channel failures. The pair has to follow them down, or the poller never
    /// re-probes it and its model list is never refreshed.
    #[test]
    fn the_last_channel_on_a_pair_takes_the_pair_and_the_provider_down() {
        let (health, dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![
            row("openrouter", "key-0", "model-a"),
            row("openrouter", "key-0", "model-b"),
            row("openrouter", "key-1", "model-a"),
        ];
        let pair_zero = pair_key(&channels[0]);
        let pair_one = pair_key(&channels[2]);
        let provider = HealthKey::Provider("openrouter".to_string());

        // One of the two channels on key-0 dies. The other still answers, so
        // nothing above the channel moves.
        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], None, now);
        assert!(!is_unhealthy(&health, &pair_zero));

        // Now the last one on key-0 dies: the pair goes to the poller. key-1
        // is still healthy, so the provider does not.
        fail_channel(&health, &channels[1], now);
        escalate_failure(&health, &channels, &channels[1], None, now);
        assert!(is_unhealthy(&health, &pair_zero));
        assert!(!is_unhealthy(&health, &provider));

        // The last key on the provider dies too.
        fail_channel(&health, &channels[2], now);
        escalate_failure(&health, &channels, &channels[2], None, now);
        assert!(is_unhealthy(&health, &pair_one));
        assert!(is_unhealthy(&health, &provider));

        let _ = std::fs::remove_dir_all(dir);
    }

    /// A pair already with the poller must not have its re-probe pushed out by
    /// every later channel failure on the same pair.
    #[test]
    fn a_pair_already_unhealthy_is_not_backed_off_again() {
        let (health, dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![row("groq", "key-0", "model-a")];
        let pair = pair_key(&channels[0]);

        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], None, now);
        let first = health.get(&pair).expect("pair marked");

        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], None, now);
        assert_eq!(health.get(&pair).expect("pair still marked"), first);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// A local outage must not cost the channels anything. Every trial fired
    /// during it fails, and if those failures were written the channels would
    /// sit in a backoff caused by a fault that was never theirs.
    #[test]
    fn a_dead_link_drops_every_held_failure() {
        let (health, dir) = health_table();
        let channels = vec![
            row("openrouter", "key-0", "model-a"),
            row("openrouter", "key-0", "model-b"),
        ];
        let mut held: Vec<PendingFailure> = channels
            .iter()
            .map(|channel| PendingFailure {
                channel,
                retry_after_secs: None,
            })
            .collect();

        settle_held(&health, &channels, &mut held, true);

        assert!(held.is_empty(), "the held moves are spent either way");
        for channel in &channels {
            assert_eq!(channel.health.retry_counter.load(Ordering::Relaxed), 0);
            assert_eq!(channel.health.eligible_at.load(Ordering::Relaxed), 0);
            assert!(health.get(&HealthKey::Channel(channel.id)).is_none());
        }
        // The pair must not follow them down either, or the poller starts
        // re-probing a key that never failed.
        assert!(!is_unhealthy(&health, &pair_key(&channels[0])));

        let _ = std::fs::remove_dir_all(dir);
    }

    /// The same failures with the link up are the channels' own, so they are
    /// written the moment the network test says so.
    #[test]
    fn a_live_link_writes_every_held_failure() {
        let (health, dir) = health_table();
        let channels = vec![row("openrouter", "key-0", "model-a")];
        let mut held = vec![PendingFailure {
            channel: &channels[0],
            retry_after_secs: None,
        }];

        settle_held(&health, &channels, &mut held, false);

        assert!(held.is_empty());
        assert_eq!(channels[0].health.retry_counter.load(Ordering::Relaxed), 1);
        assert!(channels[0].health.eligible_at.load(Ordering::Relaxed) > 0);

        let _ = std::fs::remove_dir_all(dir);
    }

    fn winner(intelligence: u16, model: &str) -> Winner {
        Winner {
            channel_id: ChannelId(intelligence as u64),
            provider: "p".to_string(),
            model_id: model.to_string(),
            intelligence,
            response: NeutralResponse::default(),
            latency: Duration::from_millis(1),
        }
    }

    /// A channel's opening bytes, as a stream the reader can consume.
    fn sse_stream(frames: &[&str]) -> ByteStream {
        let items: Vec<Result<Vec<u8>, CallError>> =
            frames.iter().map(|f| Ok(f.as_bytes().to_vec())).collect();
        Box::pin(futures::stream::iter(items))
    }

    /// OpenRouter sends `: OPENROUTER PROCESSING` while the request is still
    /// queued behind a cold model. It is 26 bytes and no answer, so it must
    /// not take the race off the channels about to deliver real tokens.
    #[tokio::test]
    async fn an_sse_comment_alone_does_not_win() {
        let mut rest = sse_stream(&[": OPENROUTER PROCESSING\n\n"]);
        let outcome = read_stream_start(&mut rest).await;
        assert!(matches!(outcome, Err(StreamStartError::NotAnAnswer(_))));
    }

    /// The same comment, followed by a real token, is a win, and the comment
    /// still reaches the client.
    #[tokio::test]
    async fn a_comment_then_real_content_wins_and_replays_both() {
        let mut rest = sse_stream(&[
            ": OPENROUTER PROCESSING\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
        ]);
        let Ok(buffered) = read_stream_start(&mut rest).await else {
            panic!("a content frame should win");
        };
        assert_eq!(buffered.len(), 2);
    }

    /// Groq and OpenRouter both answer a streamed request with HTTP 200 and a
    /// single error frame. That is a failure, not a winner, and it must not
    /// mark the channel healthy.
    #[tokio::test]
    async fn an_error_frame_in_a_two_hundred_does_not_win() {
        let mut rest =
            sse_stream(&["data: {\"error\":{\"code\":429,\"message\":\"rate limited\"}}\n\n"]);
        let outcome = read_stream_start(&mut rest).await;
        assert!(matches!(outcome, Err(StreamStartError::NotAnAnswer(_))));
    }

    /// An Anthropic stream that only ever opens a message carries nothing.
    #[tokio::test]
    async fn anthropic_openers_without_a_delta_do_not_win() {
        let mut rest = sse_stream(&[
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
        ]);
        let outcome = read_stream_start(&mut rest).await;
        assert!(matches!(outcome, Err(StreamStartError::NotAnAnswer(_))));
    }

    fn ready(elapsed_ms: u64) -> CopyWin {
        CopyWin::Streamed(Box::new(ReadyStream {
            channel_id: ChannelId(1),
            provider: "p".to_string(),
            model_id: "m".to_string(),
            intelligence: 0,
            wire: crate::config::WireName::OpenAi,
            buffered: Vec::new(),
            elapsed: Duration::from_millis(elapsed_ms),
            rest: Box::pin(futures::stream::empty()),
            open: InFlight::open(&test_channel(), &Arc::new(AtomicU32::new(0))),
        }))
    }

    /// The handler is the only place the latency histogram is written, so the
    /// stream winner has to carry its own upstream time out of the race.
    #[test]
    fn stream_winner_carries_the_time_to_first_bytes() {
        let CopyWin::Streamed(ready) = ready(120) else {
            panic!("expected a streamed copy");
        };
        let winner = ready.into_winner().expect("decoder built");
        assert_eq!(winner.latency, Duration::from_millis(120));
    }

    #[test]
    fn hedge_delay_zero_fires_every_copy_at_once() {
        let delay = Duration::ZERO;
        for index in 0..5 {
            assert_eq!(hedge_offset(index, delay), Duration::ZERO);
        }
    }

    #[test]
    fn hedge_delay_staggers_the_copies() {
        let delay = Duration::from_millis(50);
        assert_eq!(hedge_offset(0, delay), Duration::ZERO);
        assert_eq!(hedge_offset(1, delay), Duration::from_millis(50));
        assert_eq!(hedge_offset(3, delay), Duration::from_millis(150));
    }

    #[test]
    fn a_sibling_copy_is_not_another_call_in_flight() {
        // Two of our own copies open, nothing else: the 429 is a real limit.
        assert!(!is_self_inflicted(2, 2));
        // One of ours plus one from another client request: our own doing.
        assert!(is_self_inflicted(2, 1));
        // A single copy against an idle channel is a real limit.
        assert!(!is_self_inflicted(1, 1));
        // The counters can race; never report a negative as self-inflicted.
        assert!(!is_self_inflicted(0, 1));
    }

    #[tokio::test]
    async fn a_copy_that_finished_before_cancellation_is_still_collected() {
        // Mirrors the trial loop: the select is biased on cancellation, so a
        // copy that is already ready would never be polled without the
        // post-select drain.
        let cancel = CancellationToken::new();
        let mut running: FuturesUnordered<futures::future::BoxFuture<'static, u32>> =
            FuturesUnordered::new();
        running.push(Box::pin(async { 7u32 }));
        running.push(Box::pin(futures::future::pending::<u32>()));
        cancel.cancel();

        let (cancelled, next) = tokio::select! {
            biased;
            _ = cancel.cancelled() => (true, None),
            next = running.next() => (false, next),
        };
        assert!(cancelled, "the biased branch wins, so nothing is polled");
        assert!(next.is_none());

        // Without the drain the finished copy is thrown away.
        assert_eq!(finished_now(&mut running), vec![7u32]);
    }

    #[test]
    fn the_cache_candidate_is_the_smartest_answer_not_the_fastest() {
        let fastest = winner(40, "fast");
        let others = vec![winner(90, "smart"), winner(10, "thick")];
        assert_eq!(best_for_cache(&fastest, &others).model_id, "smart");
    }

    #[test]
    fn the_winner_is_the_cache_candidate_when_nothing_else_finished() {
        let only = winner(40, "fast");
        assert_eq!(best_for_cache(&only, &[]).model_id, "fast");
    }

    #[test]
    fn an_equal_score_keeps_the_winner() {
        let fastest = winner(50, "fast");
        let others = vec![winner(50, "equal")];
        assert_eq!(best_for_cache(&fastest, &others).model_id, "fast");
    }

    #[test]
    fn the_longest_retry_after_among_the_copies_sets_the_base() {
        let failures = [
            CopyFailure {
                error: "429".to_string(),
                retry_after_secs: Some(30),
                self_inflicted: false,
            },
            CopyFailure {
                error: "429".to_string(),
                retry_after_secs: None,
                self_inflicted: false,
            },
            CopyFailure {
                error: "429".to_string(),
                retry_after_secs: Some(120),
                self_inflicted: false,
            },
        ];
        let longest = failures.iter().filter_map(|f| f.retry_after_secs).max();
        assert_eq!(longest, Some(120));
    }

    #[test]
    fn health_moves_only_when_a_failure_was_not_self_inflicted() {
        let all_ours = [CopyFailure {
            error: "429".to_string(),
            retry_after_secs: None,
            self_inflicted: true,
        }];
        assert!(all_ours.iter().all(|f| f.self_inflicted));

        let mixed = [
            CopyFailure {
                error: "429".to_string(),
                retry_after_secs: None,
                self_inflicted: true,
            },
            CopyFailure {
                error: "500".to_string(),
                retry_after_secs: None,
                self_inflicted: false,
            },
        ];
        assert!(!mixed.iter().all(|f| f.self_inflicted));
    }

    #[test]
    fn an_in_flight_guard_decrements_on_drop_including_cancellation() {
        let channel = test_channel();
        let own = Arc::new(AtomicU32::new(0));
        {
            let _one = InFlight::open(&channel, &own);
            let _two = InFlight::open(&channel, &own);
            assert_eq!(channel.health.in_flight.load(Ordering::Relaxed), 2);
            assert_eq!(own.load(Ordering::Relaxed), 2);
        }
        assert_eq!(channel.health.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(own.load(Ordering::Relaxed), 0);
    }

    /// A streamed call ends when the stream ends, not when its first frame
    /// arrives. If the guard dropped at the first frame, thirty open streams
    /// would read as idle, and the next `429` they caused would be taken for a
    /// real provider limit and park the best channel.
    #[test]
    fn a_streamed_copy_holds_its_count_until_the_stream_ends() {
        let channel = test_channel();
        let own = Arc::new(AtomicU32::new(0));
        let ready = ReadyStream {
            channel_id: ChannelId(1),
            provider: "p".to_string(),
            model_id: "m".to_string(),
            intelligence: 0,
            wire: crate::config::WireName::OpenAi,
            buffered: Vec::new(),
            elapsed: Duration::from_millis(1),
            rest: Box::pin(futures::stream::empty()),
            open: InFlight::open(&channel, &own),
        };
        let winner = ready.into_winner().expect("decoder built");

        assert_eq!(
            channel.health.in_flight.load(Ordering::Relaxed),
            1,
            "the socket is still open, so the count must say so"
        );
        assert_eq!(own.load(Ordering::Relaxed), 1);

        drop(winner);
        assert_eq!(channel.health.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(own.load(Ordering::Relaxed), 0);
    }

    fn test_channel() -> Channel {
        Channel {
            id: ChannelId(1),
            provider: "p".into(),
            model_id: "m".into(),
            key_id: "k".into(),
            key: "secret".into(),
            base_url: "https://example.invalid".into(),
            wire: crate::config::WireName::OpenAi,
            limit_context: 128_000,
            limit_output: 4_096,
            tool_call: true,
            reasoning: false,
            structured_output: false,
            modalities_input: vec!["text".to_string()],
            intelligence: 50,
            intelligence_rank: 1,
            health: Default::default(),
            telemetry: Default::default(),
        }
    }
}
