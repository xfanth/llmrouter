//! The race.
//!
//! N channels in flight, M hedged copies each. `hedge_delay_ms` is 1 by
//! default, 1ms behind the first one.
//!
//! N escalates. A walk opens at `race_width` channels and adds one more every
//! `race_escalation_ms` while it runs, up to `race_width_max`, in intelligence
//! order: the slowest channels are still growing the race while the fastest
//! are answering. An escalation period of 0 holds N at `race_width` for the
//! whole walk.
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
//!
//! When the providers are streamed, nothing is committed at the first token.
//! Every stream that gets through prefill is read on to its end and buffered,
//! and keeps counting against N, which goes on widening to `race_width_max`.
//! The first stream to finish with an answer that decodes whole wins, and only
//! then are the other calls cancelled: any stream can die part way, and one
//! killed early is one less to fall back on. Alex's spec, 24 Oct 2026.
//!
//! No sign of life for `stream_stall_timeout` means the channel failed; a token
//! or a reasoning frame counts, a keepalive comment does not. A stream that
//! dies part way is an ordinary failed trial, replaced at once and backed off.
//! Nothing reached the client, so another channel is never spliced onto it.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::stream::{FuturesUnordered, Stream, StreamExt};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::catalogue::{Catalogue, Channel, ChannelFilter, ChannelHealth, ChannelId};
use crate::config::Config;
use crate::health::{HealthKey, HealthTable, StreamVerdict};
use crate::metrics::{
    AttemptOutcome, CopyOutcome, ErrorClass, HedgeOutcome, Metrics, TrialOutcomeLabel,
};
use crate::net::{LinkState, NetTest};
use crate::server::PromptBody;
use crate::upstream::{CallError, SharedBody, Upstream};
use crate::wire::{self, NeutralResponse};

/// Everything one client request hands the race.
#[derive(Clone)]
pub struct RaceInput {
    /// The body every channel call sends, encoded once and shared. It is
    /// either held in RAM or spilled to a scratch file, and each channel call
    /// asks for it when it is about to upload.
    pub outbound: Arc<PromptBody>,
    /// What a channel must satisfy to enter the race.
    pub filter: ChannelFilter,
    /// Stream from the providers. This is `streaming` in `router.yml`, not the
    /// client's own `stream` flag: the hedge strategy needs a stream to
    /// measure, and what the client asked for only decides whether the server
    /// forwards the winning stream or buffers it into one piece.
    pub streaming: bool,
    /// The caller's model, for the `404` when nothing matches and for the
    /// cache rules. `auto` and no model both mean `None`.
    pub named_model: Option<String>,
    /// Channels this walk must not call. The server lists the channel whose
    /// winning stream died, so the failover walk finds fresh channels.
    pub exclude: std::collections::HashSet<ChannelId>,
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

    /// This trial's slot in the walk's fire order, which the N-race
    /// outcome reads: slot 0 winning means the race bought nothing.
    pub slot: u32,
}

/// How the race ended.
pub enum RaceOutcome {
    /// A channel answered with a whole reply. `200`. A streamed answer lands
    /// here too, once its stream has finished: nothing is handed to the
    /// client part way through, so a stream that dies is never the client's
    /// problem while another channel can still finish.
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
        /// The whole walk, from the first channel call to this answer.
        ///
        /// Not the same as `winner.latency`, which is the winning call on its
        /// own. The gap between the two is every failed channel the walk got
        /// through first, and that time is the providers', not the router's.
        /// Splitting the latency histogram on `winner.latency` put all of it
        /// in the router's half: a live run reported a mean of 829 seconds of
        /// router overhead for work the router never did.
        race_elapsed: Duration,
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
    /// The client went away. Everything in flight was cancelled.
    ClientGone,
}

/// Runs races over the shared catalogue, health table and client.
#[derive(Debug)]
pub struct Racer {
    config: Arc<Config>,
    catalogue: Arc<Catalogue>,
    health: Arc<HealthTable>,
    upstream: Upstream,
    net: NetTest,
    metrics: Arc<Metrics>,
    /// Channel trials in flight across every walk at once, which is what
    /// `router_trials_inflight` reports. One walk's own set is the wrong
    /// number: the racer is shared, so a second client request would
    /// overwrite the first's gauge with its own. A lock rather than an
    /// atomic because the count and the gauge have to move together, and
    /// this is the number read at the moment of peak concurrency.
    trials_inflight: Mutex<u32>,
}

impl Racer {
    /// Builds a racer over the shared state.
    pub fn new(
        config: Arc<Config>,
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
            trials_inflight: Mutex::new(0),
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
        let fired = WalkCount::default();
        // Started here rather than inside `walk`, so every one of the walk's
        // ten exits carries the same reading and none of them can forget to
        // take it.
        let started = std::time::Instant::now();
        let mut outcome = self.walk(input, cancel, &fired).await;
        match &mut outcome {
            RaceOutcome::Buffered { race_elapsed, .. } => *race_elapsed = started.elapsed(),
            // The other three never answered, so there is no split to make.
            RaceOutcome::NoNetwork { .. }
            | RaceOutcome::Exhausted { .. }
            | RaceOutcome::ClientGone => {}
        }
        // The gap between the two is what M cost: the live run came back with
        // 9.2 trials and 18.4 copies for each client request, and nothing on
        // `/metrics` said so.
        self.metrics.record_walk(fired.trials(), fired.copies());
        outcome
    }

    /// The walk itself.
    ///
    /// `fired` comes back carrying what the walk actually started, so
    /// [`Racer::run`] records the walk once on every way out of here, the
    /// client hanging up and a dead link included. There are ten of those,
    /// and a counter written at each one is a counter that will be missed off
    /// the eleventh.
    async fn walk(
        &self,
        input: RaceInput,
        cancel: CancellationToken,
        fired: &WalkCount,
    ) -> RaceOutcome {
        // One clock for every copy's telemetry offset: start_ms on each
        // channel_copy line is measured from here, so a saved run's copies
        // line up on one timeline for any (N, M) rebuild.
        let walk_started = tokio::time::Instant::now();
        let snapshot = self.catalogue.snapshot();
        // A child token, so the race can tear its own calls down on a win
        // without touching the client's token.
        let race_cancel = cancel.child_token();

        // Behind a lock so a top-up can run from any branch of the walk
        // without holding a mutable borrow across an await.
        let excluded = std::sync::RwLock::new(input.exclude.clone());
        let mut last_error: Option<String> = None;
        // Failed trials so far, which is what the N-race outcome reads at the
        // moment a winner emerges.
        let mut failed_before_win: u32 = 0;
        // Health moves owed by trials that failed before the network test
        // answered. They land only once the link is proven up, and are dropped
        // when it is down, because a local fault is not the channel's.
        let mut held: Vec<PendingFailure> = Vec::new();
        let mut link_up = false;

        // The escalation clock. N starts at `race_width` and climbs one
        // channel at a time, one `race_escalation_ms` apart, up to
        // `race_width_max`, so a request the providers are slow on ends up
        // racing every channel the catalogue can serve, in intelligence
        // order. A period of 0 holds N at `race_width` for the whole walk.
        let max_width = self
            .config
            .router
            .race_width_max
            .max(1)
            .max(self.config.router.race_width);
        let mut width = self.config.router.race_width.max(1).min(max_width);
        let escalates = self.config.router.race_escalation_ms > 0;
        let escalation = Duration::from_millis(self.config.router.race_escalation_ms.max(1));
        let mut escalator = tokio::time::interval(escalation);
        escalator.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        escalator.tick().await;

        let mut inflight: Trials = Trials::new();
        // Streams that have produced a first token and are being read to the
        // end. None is committed to: the first to finish its whole answer
        // wins, and every other call stays live until then, because any of
        // them may yet die part way. Alex's spec, 24 Oct 2026. They count
        // against the width like the trials still in prefill.
        let mut streams: Streams = FuturesUnordered::new();
        self.top_up(
            &mut inflight,
            &snapshot,
            &input,
            &excluded,
            &race_cancel,
            fired,
            walk_started,
            width,
        );

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
                    return self.client_gone(net_check.as_mut(), net_done, &mut held).await;
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

                _ = escalator.tick(), if escalates && width < max_width => {
                    width = (width + 1).min(max_width);
                    self.top_up(
                        &mut inflight,
                        &snapshot,
                        &input,
                        &excluded,
                        &race_cancel,
                        fired,
                        walk_started,
                        width.saturating_sub(streams.len()),
                    );
                    continue;
                }

                // A stream reached its end. A whole answer is the win; one
                // that broke on the way is a failed trial like any other.
                Some(done) = streams.next(), if !streams.is_empty() => {
                    self.stream_outcome(done, &snapshot).await
                }

                Some(outcome) = inflight.next(), if !inflight.is_empty() => outcome,

                else => {
                    let outcome =
                        exhausted(net_check.as_mut(), net_done, fired.queued(), last_error).await;
                    self.settle_held(&mut held, &outcome);
                    return outcome;
                }
            };

            // The borrow of `inflight` ends with the select, so the top-up
            // below is free to push.
            match finished {
                TrialOutcome::Won(winner) => {
                    race_cancel.cancel();
                    // What the N-race bought: slot 0 winning means a
                    // sequential walk would have landed here anyway; a later
                    // slot winning after a failure means the race carried the
                    // walk past a dead channel.
                    let stat = race_stat(winner.slot, failed_before_win);
                    self.metrics.record_race_outcome(stat);
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
                        // Stamped by `run`, which owns the clock for the
                        // whole walk. A reading taken here would miss the
                        // walk's other nine exits.
                        race_elapsed: Duration::ZERO,
                        winner,
                        also_finished,
                        attempts: fired.queued(),
                    };
                }
                TrialOutcome::WonStreaming(ready) => {
                    // Read on to the end beside everything else. The trial
                    // only moved from one set to the other, so the width is
                    // still full and nothing needs topping up.
                    streams.push(Box::pin(read_to_end(
                        *ready,
                        self.config.router.stream_stall_timeout,
                    )));
                }
                TrialOutcome::Cancelled => {}
                TrialOutcome::Failed { error, pending } => {
                    last_error = Some(error);
                    failed_before_win += 1;
                    if let Some(pending) = pending {
                        if link_up {
                            self.apply_failure(
                                pending.channel,
                                pending.retry_after_secs,
                                pending.class,
                            );
                        } else {
                            // The test has not answered yet. This may be a
                            // local fault, so the counter stays where it is
                            // until the link state is known.
                            held.push(pending);
                        }
                    }
                    // Replace the failed channel at once. One finishes, one
                    // starts: a sliding window of the walk's current width,
                    // not waves of N.
                    self.top_up(
                        &mut inflight,
                        &snapshot,
                        &input,
                        &excluded,
                        &race_cancel,
                        fired,
                        walk_started,
                        width.saturating_sub(streams.len()),
                    );
                }
            }

            if inflight.is_empty() && streams.is_empty() {
                let outcome =
                    exhausted(net_check.as_mut(), net_done, fired.queued(), last_error).await;
                self.settle_held(&mut held, &outcome);
                return outcome;
            }
        }
    }

    /// Tops the trials in flight back up to `width`, which is the walk's
    /// current width: `race_width` at the start, then one more channel every
    /// `race_escalation_ms` while the walk runs, to `race_width_max`.
    ///
    /// Every path that loses a trial comes through here, so a stream that
    /// broke pulls in untried channels exactly as a buffered failure does. A
    /// query that comes back short leaves the set smaller, and an empty one
    /// ends the walk at the caller's `inflight.is_empty()` check.
    #[allow(clippy::too_many_arguments)] // walk_started rides the same list
    fn top_up<'a>(
        &'a self,
        inflight: &mut Trials<'a>,
        snapshot: &'a [Channel],
        input: &RaceInput,
        excluded: &std::sync::RwLock<Excluded>,
        cancel: &CancellationToken,
        fired: &'a WalkCount,
        walk_started: tokio::time::Instant,
        width: usize,
    ) {
        let copies = self.config.router.max_per_channel.max(1) as u32;
        let excluded = &mut *excluded.write().unwrap();
        let wanted = width.max(1).saturating_sub(inflight.len());
        if wanted == 0 {
            return;
        }
        // The clock is read here, not at the start of the request: a channel
        // that came out of backoff while this walk was running is eligible
        // again.
        for channel in
            crate::catalogue::query(snapshot, &input.filter, excluded, wanted, now_unix())
        {
            excluded.insert(channel.id);
            let slot = fired.queued();
            fired.queued.fetch_add(copies, Ordering::Relaxed);
            inflight.push(Box::pin(self.run_trial(
                channel,
                Arc::clone(&input.outbound),
                input.streaming,
                slot,
                cancel.child_token(),
                fired,
                walk_started,
            )));
        }
    }

    /// One channel's trial: M copies of the same call, `hedge_delay_ms` apart.
    ///
    /// The first copy to produce an answer wins and the rest are cancelled.
    ///
    /// The whole trial counts once against health, whatever M is.
    #[allow(clippy::too_many_arguments)] // slot rides the same list
    async fn run_trial<'a>(
        &self,
        channel: &'a Channel,
        body: Arc<PromptBody>,
        streaming: bool,
        slot: u32,
        cancel: CancellationToken,
        fired: &'a WalkCount,
        walk_started: tokio::time::Instant,
    ) -> TrialOutcome<'a> {
        let copies = self.config.router.max_per_channel.max(1);
        let delay = Duration::from_millis(self.config.router.hedge_delay_ms);
        // Our own open copies, so a `429` can tell a sibling copy apart from
        // another client request hitting the same channel.
        let own = Arc::new(AtomicU32::new(0));
        // This trial's verdict and its share of the in-flight gauge. Both
        // ride on a guard because three of the walk's endings tear a trial
        // down by dropping its future rather than polling it, and nothing
        // written in the body below would run at all.
        let mut trial = TrialRecord::open(
            &self.metrics,
            &self.trials_inflight,
            &channel.provider,
            fired,
        );

        let mut running = FuturesUnordered::new();
        for index in 0..copies {
            let body = Arc::clone(&body);
            let own = own.clone();
            let offset = hedge_offset(index, delay);
            let mut record = CopyRecord::open(
                Arc::clone(&self.metrics),
                channel,
                &cancel,
                fired,
                walk_started,
                index as u32,
            );
            running.push(async move {
                if !offset.is_zero() {
                    tokio::time::sleep(offset).await;
                    // The stagger is the router's own wait, not the
                    // provider's, so the clock starts when the call does.
                    record.restart();
                }
                // Read as late as possible and dropped as soon as the upload
                // is done, so a spilled body is only resident for the upload.
                let body = match body.load().await {
                    Ok(body) => SharedBody::new(body),
                    Err(error) => {
                        // A scratch disk fault is ours, not the channel's, so
                        // it must not move the channel's health.
                        let failure = CopyFailure {
                            error: format!("outbound body could not be read back: {error}"),
                            retry_after_secs: None,
                            self_inflicted: true,
                            class: Some(ErrorClass::Other),
                        };
                        record.failed(&failure);
                        return Err(failure);
                    }
                };
                let called = if streaming {
                    self.copy_streamed(channel, body, &own, slot).await
                } else {
                    self.copy_buffered(channel, body, &own, slot).await
                };
                match called {
                    Ok(win) => {
                        if let CopyWin::Streamed(stream) = &win {
                            record.note_first_token(stream.content_at);
                        }
                        record.finish(CopyOutcome::Success, None);
                        // The index travels with the win, because which copy
                        // won is the only thing that says whether M bought
                        // anything at all.
                        Ok((index, win))
                    }
                    Err(failure) => {
                        record.failed(&failure);
                        Err(failure)
                    }
                }
            });
        }

        let mut failures: Vec<CopyFailure> = Vec::new();
        let raced = first_copy(&mut running, &cancel, |failure| {
            note_failure(channel, &failure);
            failures.push(failure);
        })
        .await;
        match raced {
            CopyRace::Won((index, win)) => {
                // Which copy won, and then what the hedge actually bought.
                // The index alone is a coin flip: two copies race and one of
                // them wins, and a live run came back 5 against 5. What
                // decides M is the index together with what became of the
                // siblings, which is what `failures` holds at this point.
                self.metrics.record_hedge_win(&channel.provider, index);
                self.metrics.record_hedge_outcome(
                    &channel.provider,
                    hedge_outcome(copies, index, failures.len()),
                );
                trial.settle(TrialOutcomeLabel::Won);
                return self.trial_won(channel, win);
            }
            CopyRace::Cancelled => {
                self.metrics.record_attempt(
                    &channel.provider,
                    &channel.model_id,
                    AttemptOutcome::Cancelled,
                );
                trial.settle(TrialOutcomeLabel::Cancelled);
                return TrialOutcome::Cancelled;
            }
            CopyRace::AllFailed => {}
        }
        trial.settle(TrialOutcomeLabel::Failed);

        // Every copy failed, which is one failure against health, not M. The
        // health write itself is the walk's to make, not the trial's: until
        // the network test has answered, nobody knows whether the fault is the
        // channel's or the local link's.
        let self_inflicted = !failures.is_empty() && failures.iter().all(|f| f.self_inflicted);
        let retry_after_secs = longest_retry_after(&failures);
        // The same copy the `504` quotes, skipping any the router caused
        // itself: those say nothing about the channel.
        let class = failures
            .iter()
            .rev()
            .filter(|f| !f.self_inflicted)
            .find_map(|f| f.class);
        // A failure whose class benches nothing, such as a `400`, leaves the
        // channel's health alone, like one the router caused itself.
        let benches = class.map_or(true, |class| class.group().is_some());
        TrialOutcome::Failed {
            error: failures
                .last()
                .map(|f| f.error.clone())
                .unwrap_or_else(|| "no copy ran".to_string()),
            pending: (!self_inflicted && benches).then_some(PendingFailure {
                channel,
                retry_after_secs,
                class,
            }),
        }
    }

    /// Writes every health move held back while the link state was unknown.
    /// Only called once something has proven the link is up.
    fn apply_held(&self, held: &mut Vec<PendingFailure<'_>>) {
        settle_held(&self.health, &self.catalogue, held, false);
    }

    /// Settles the held moves against how the walk ended: a dead link drops
    /// them, anything else writes them.
    fn settle_held(&self, held: &mut Vec<PendingFailure<'_>>, outcome: &RaceOutcome) {
        let link_down = matches!(outcome, RaceOutcome::NoNetwork { .. });
        settle_held(&self.health, &self.catalogue, held, link_down);
    }

    /// The client went away. The calls are already cancelled, but the trials
    /// that failed before that are real channel failures and still owe their
    /// backoff. The client leaving says nothing about the link, so the held
    /// moves are settled exactly as any other ending settles them: at once
    /// when the test has answered, or once it does. The test carries its own
    /// 2 second timeout, so this cannot hang.
    async fn client_gone<F>(
        &self,
        net_check: Pin<&mut F>,
        net_done: bool,
        held: &mut Vec<PendingFailure<'_>>,
    ) -> RaceOutcome
    where
        F: std::future::Future<Output = LinkState>,
    {
        let link_down =
            !held.is_empty() && !net_done && matches!(net_check.await, LinkState::Down { .. });
        settle_held(&self.health, &self.catalogue, held, link_down);
        RaceOutcome::ClientGone
    }

    /// One buffered copy of a channel call.
    async fn copy_buffered(
        &self,
        channel: &Channel,
        body: SharedBody,
        own: &Arc<AtomicU32>,
        slot: u32,
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
            self.metrics.record_attempt(
                &channel.provider,
                &channel.model_id,
                AttemptOutcome::HttpError,
            );
            return Err(CopyFailure {
                error: "429 caused by this router's own concurrency".to_string(),
                retry_after_secs: None,
                self_inflicted: true,
                class: Some(ErrorClass::from_status(429, true)),
            });
        }

        if !crate::health::is_healthy_response(call.status, &call.body) {
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics.record_attempt(
                &channel.provider,
                &channel.model_id,
                AttemptOutcome::HttpError,
            );
            return Err(CopyFailure {
                error: failure_reason(call.status, &call.body),
                retry_after_secs: call.retry_after_secs,
                self_inflicted: false,
                class: Some(ErrorClass::from_status(call.status, false)),
            });
        }

        let format = wire::format_for_provider(channel.wire);
        let value = match serde_json::from_slice::<serde_json::Value>(&call.body) {
            Ok(value) => value,
            Err(error) => {
                channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                self.metrics.record_attempt(
                    &channel.provider,
                    &channel.model_id,
                    AttemptOutcome::HttpError,
                );
                return Err(CopyFailure {
                    error: format!("unparseable body: {error}"),
                    retry_after_secs: call.retry_after_secs,
                    self_inflicted: false,
                    class: Some(ErrorClass::BodyUnparseable),
                });
            }
        };
        let response = match wire::decode_response(&value, format) {
            Ok(response) => response,
            Err(error) => {
                channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                self.metrics.record_attempt(
                    &channel.provider,
                    &channel.model_id,
                    AttemptOutcome::HttpError,
                );
                return Err(CopyFailure {
                    error: format!("undecodable body: {error}"),
                    retry_after_secs: call.retry_after_secs,
                    self_inflicted: false,
                    class: Some(ErrorClass::BodyUndecodable),
                });
            }
        };

        self.metrics.record_attempt(
            &channel.provider,
            &channel.model_id,
            AttemptOutcome::Success,
        );
        Ok(CopyWin::Buffered(Box::new(Winner {
            channel_id: channel.id,
            provider: channel.provider.to_string(),
            model_id: channel.model_id.to_string(),
            intelligence: channel.intelligence,
            response,
            latency: call.latency,
            slot,
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
        slot: u32,
    ) -> Result<CopyWin, CopyFailure> {
        // The guard travels with the stream: a streamed call ends when the
        // stream ends, not when its first frame arrives.
        let open = InFlight::open(channel, own);
        channel.telemetry.calls.fetch_add(1, Ordering::Relaxed);
        let started = tokio::time::Instant::now();

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
            self.metrics.record_attempt(
                &channel.provider,
                &channel.model_id,
                AttemptOutcome::HttpError,
            );
            return Err(CopyFailure {
                error: "429 caused by this router's own concurrency".to_string(),
                retry_after_secs: None,
                self_inflicted: true,
                class: Some(ErrorClass::from_status(429, true)),
            });
        }

        let mut rest = call.body;

        if !(200..300).contains(&call.status) {
            channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            self.metrics.record_attempt(
                &channel.provider,
                &channel.model_id,
                AttemptOutcome::HttpError,
            );
            // A bare "status 429" tells an operator nothing about which of a
            // provider's half dozen 429s this was. The body does, and on this
            // path it arrives as a stream rather than as bytes, so it has to
            // be read back before the failure is reported.
            let body = read_error_body(&mut rest).await;
            return Err(CopyFailure {
                error: failure_reason(call.status, &body),
                retry_after_secs: call.retry_after_secs,
                self_inflicted: false,
                class: Some(ErrorClass::from_status(call.status, false)),
            });
        }

        let (buffered, counter) =
            // The prefill deadline, not `stream_stall_timeout`. That one is
            // the gap allowed *between* tokens; prefill is not a gap. A 1M
            // token prompt at 1,000 tok/s is 16.7 minutes of silence before
            // the first token, and judging it against a 60 second stall timer
            // fails every long prompt. `request_timeout` already bounds the
            // channel call, so it is the right ceiling here.
            match read_stream_start(&mut rest, self.config.router.request_timeout).await {
                Ok(start) => start,
                Err(failure) => {
                    channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    let (outcome, class) = prefill_failure_class(&failure);
                    let (error, retry_after_secs) = match failure {
                        StreamStartError::Timeout(error)
                        | StreamStartError::Transport(error) => (error, None),
                        // Only the provider's own refusal carries a
                        // `Retry-After`: the other two never heard back.
                        StreamStartError::NotAnAnswer(error) => {
                            (error, call.retry_after_secs)
                        }
                    };
                    self.metrics.record_attempt(&channel.provider, &channel.model_id, outcome);
                    return Err(CopyFailure {
                        error,
                        retry_after_secs,
                        self_inflicted: false,
                        class: Some(class),
                    });
                }
            };

        self.metrics.record_attempt(
            &channel.provider,
            &channel.model_id,
            AttemptOutcome::Success,
        );
        Ok(CopyWin::Streamed(Box::new(ReadyStream {
            channel_id: channel.id,
            provider: channel.provider.to_string(),
            model_id: channel.model_id.to_string(),
            intelligence: channel.intelligence,
            wire: channel.wire,
            buffered,
            counter,
            content_at: tokio::time::Instant::now(),
            elapsed: started.elapsed(),
            slot,
            rest,
            _open: open,
        })))
    }

    /// Turns a winning copy into a trial outcome. A whole reply makes the
    /// channel healthy at once; a stream only once it has finished its answer,
    /// because until then it can still break part way.
    fn trial_won<'a>(&self, channel: &'a Channel, win: CopyWin) -> TrialOutcome<'a> {
        match win {
            CopyWin::Buffered(winner) => {
                self.apply_success(channel);
                TrialOutcome::Won(winner)
            }
            // The stream is handed over as it stands. Which strategy judges it,
            // and when it is decoded, is the walk's business, not the trial's.
            CopyWin::Streamed(ready) => TrialOutcome::WonStreaming(ready),
        }
    }

    /// A transport failure or a timeout on one copy.
    fn copy_failed(&self, channel: &Channel, error: CallError) -> CopyFailure {
        channel.telemetry.failures.fetch_add(1, Ordering::Relaxed);
        let (outcome, class) = match error {
            CallError::Timeout(_) => (AttemptOutcome::Timeout, Some(ErrorClass::Timeout)),
            // The call was torn down, so the provider did nothing wrong here
            // and owes `router_errors_total` nothing.
            CallError::Cancelled => (AttemptOutcome::Cancelled, None),
            CallError::Transport(_) => (AttemptOutcome::HttpError, Some(ErrorClass::Connect)),
        };
        self.metrics
            .record_attempt(&channel.provider, &channel.model_id, outcome);
        CopyFailure {
            error: error.to_string(),
            retry_after_secs: None,
            self_inflicted: false,
            class,
        }
    }

    /// Records one failed trial and mirrors the result onto the channel row, so
    /// the next query sees the backoff without waiting for anything else.
    fn apply_failure(
        &self,
        channel: &Channel,
        retry_after_secs: Option<u64>,
        class: Option<ErrorClass>,
    ) {
        write_failure(
            &self.health,
            &self.catalogue,
            channel,
            retry_after_secs,
            class,
        );
    }

    /// Turns a stream that has stopped into a trial outcome. A whole answer is
    /// the win, and its channel is healthy. Anything else is a failed trial,
    /// backed off on the class of the way it broke.
    async fn stream_outcome<'a>(
        &self,
        done: Finished,
        channels: &'a [Channel],
    ) -> TrialOutcome<'a> {
        let channel = channels.iter().find(|c| c.id == done.ready.channel_id);
        let (ready, class) = match judge(done).await {
            Ok(winner) => {
                if let Some(channel) = channel {
                    self.apply_success(channel);
                }
                return TrialOutcome::Won(winner);
            }
            Err(dead) => *dead,
        };
        self.metrics.record_error(&ready.provider, class);
        TrialOutcome::Failed {
            error: format!(
                "{} {} stopped part way through its answer: {}",
                ready.provider,
                ready.model_id,
                class.label()
            ),
            pending: channel.map(|channel| PendingFailure {
                channel,
                retry_after_secs: None,
                class: Some(class),
            }),
        }
    }

    /// Records a successful trial and clears the channel row's backoff.
    fn apply_success(&self, channel: &Channel) {
        channel.telemetry.successes.fetch_add(1, Ordering::Relaxed);
        // Onto the live row, not the walk's own snapshot: a rebuild part way
        // through the walk leaves that row orphaned, and clearing the backoff
        // on it would leave the channel in backoff for everybody else. The row
        // is cleared under the table entry's lock, so a failure on the same
        // channel lands wholly before or wholly after, never in between.
        let _rows = self.catalogue.hold_rows();
        let snapshot = self.catalogue.snapshot();
        let row = current_row(&snapshot, channel);
        let base = self.health.retry_base_secs();
        self.health
            .record_success_then(&HealthKey::Channel(channel.id), || {
                row.health.retry_counter.store(0, Ordering::Relaxed);
                row.health.retry_base_secs.store(base, Ordering::Relaxed);
                row.health.eligible_at.store(0, Ordering::Relaxed);
            });
        write_success_above(&self.health, snapshot.as_slice(), channel);
    }
}

/// Logs one failed copy.
///
/// A trial reduces M copies to a single reason, so without this the two that
/// failed for different causes are indistinguishable afterwards and only the
/// last one's message survives into the `504`.
///
/// `key_id` is the label from `providers.yml`, not the secret: no key material
/// goes anywhere near a log line.
fn note_failure(channel: &Channel, failure: &CopyFailure) {
    tracing::debug!(
        channel = %channel.id,
        provider = %channel.provider,
        model = %channel.model_id,
        key_id = %channel.key_id,
        self_inflicted = failure.self_inflicted,
        retry_after_secs = failure.retry_after_secs,
        error = %failure.error,
        "channel call copy failed"
    );
}

/// The backoff base for a channel whose copies all failed.
///
/// DESIGN.md: the longest `Retry-After` among the M copies sets the base. The
/// shortest would send the walk straight back into a provider that is still
/// refusing, so the maximum is the only safe pick.
fn longest_retry_after(failures: &[CopyFailure]) -> Option<u64> {
    failures.iter().filter_map(|f| f.retry_after_secs).max()
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
    catalogue: &Catalogue,
    held: &mut Vec<PendingFailure<'_>>,
    link_down: bool,
) {
    for pending in held.drain(..) {
        if !link_down {
            write_failure(
                health,
                catalogue,
                pending.channel,
                pending.retry_after_secs,
                pending.class,
            );
        }
    }
}

/// The row for this channel in the current catalogue, falling back to the one
/// handed in when the channel has gone from the list.
///
/// `Catalogue::replace` builds brand-new rows, so health written onto a row
/// from an older snapshot is never read again. `catalogue::query` decides
/// eligibility from the row atomics alone, so the write has to land on the row
/// the next query will actually scan.
fn current_row<'a>(channels: &'a [Channel], channel: &'a Channel) -> &'a Channel {
    channels
        .iter()
        .find(|row| row.id == channel.id)
        .unwrap_or(channel)
}

/// Clears the two levels above a channel that has just answered, in the table
/// and on every row they were holding.
///
/// A channel answering proves its pair and its provider are alive, so an
/// earlier escalation above it is over. This is the mirror image of
/// `escalate_failure` and has to undo everything that wrote, or the rows it
/// stamped stay dark with nothing left in the table to explain why.
fn write_success_above(health: &HealthTable, channels: &[Channel], channel: &Channel) {
    clear_hold(health, channels, &pair_key(channel), pair_level, |c| {
        c.provider == channel.provider && c.key_id == channel.key_id
    });
    clear_hold(
        health,
        channels,
        &HealthKey::Provider(channel.provider.to_string()),
        provider_level,
        |c| c.provider == channel.provider,
    );
}

/// Records one failed trial and mirrors the result onto the channel row, so the
/// next query sees the backoff without waiting for anything else.
fn write_failure(
    health: &HealthTable,
    catalogue: &Catalogue,
    channel: &Channel,
    retry_after_secs: Option<u64>,
    class: Option<ErrorClass>,
) {
    let now = now_unix();
    // The snapshot is loaded under the rebuild lock: loaded before it, a
    // rebuild could swap new rows in between the load and the write below,
    // and the write would land on an orphan.
    let _rows = catalogue.hold_rows();
    let snapshot = catalogue.snapshot();
    let channels = snapshot.as_slice();
    // Onto the live row, not whatever snapshot the caller is holding. A walk
    // runs for minutes and the poller may have rebuilt the catalogue under it,
    // in which case the caller's row is an orphan nothing queries any more.
    // The copy runs under the table entry's lock, so a success on the same
    // channel cannot land between the table write and the row write.
    let row = current_row(channels, channel);
    record_failure(
        health,
        &HealthKey::Channel(channel.id),
        class,
        retry_after_secs,
        now,
        |entry| {
            row.health
                .retry_counter
                .store(entry.retry_counter, Ordering::Relaxed);
            row.health
                .retry_base_secs
                .store(entry.retry_base_secs, Ordering::Relaxed);
            row.health
                .eligible_at
                .store(entry.eligible_at, Ordering::Relaxed);
        },
    );
    escalate_failure(health, channels, channel, retry_after_secs, class, now);
}

/// One failure against `key`, on the schedule its class belongs to, or the
/// default schedule when nothing says what kind of failure it was.
fn record_failure(
    health: &HealthTable,
    key: &HealthKey,
    class: Option<ErrorClass>,
    retry_after_secs: Option<u64>,
    now: u64,
    mirror: impl FnOnce(&crate::health::HealthEntry),
) {
    match class.and_then(ErrorClass::group) {
        Some(group) => health.record_failure_as_then(key, group, retry_after_secs, now, mirror),
        None => health.record_failure_then(key, retry_after_secs, now, mirror),
    };
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
    class: Option<ErrorClass>,
    now: u64,
) {
    let pair = pair_key(failed);
    if is_unhealthy(health, &pair) {
        // Already with the poller. Bumping it again would push the re-probe
        // out for a fault the poller is the one meant to clear.
        return;
    }
    // Health is binary and failures are not classified by status: a `402` or
    // a `403` is as often about one model as about the key, so the pair goes
    // down only when no channel on it is callable (DESIGN.md "The unit of
    // health"). A dead key is caught by its keyed probe, not here.
    let pair_alive = channels
        .iter()
        .any(|c| c.provider == failed.provider && c.key_id == failed.key_id && c.eligible(now));
    if pair_alive {
        return;
    }
    record_failure(health, &pair, class, retry_after_secs, now, |entry| {
        set_hold(channels, pair_level, entry.eligible_at, |c| {
            c.provider == failed.provider && c.key_id == failed.key_id
        });
    });

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
        record_failure(health, &provider, class, retry_after_secs, now, |entry| {
            set_hold(channels, provider_level, entry.eligible_at, |c| {
                c.provider == failed.provider
            });
        });
    }
}

/// The row field the provider/key pair level writes.
fn pair_level(health: &ChannelHealth) -> &AtomicU64 {
    &health.pair_eligible_at
}

/// The row field the provider level writes.
fn provider_level(health: &ChannelHealth) -> &AtomicU64 {
    &health.provider_eligible_at
}

/// Writes a deadline recorded above the channel level onto every row it
/// covers. A deadline of 0 lifts the hold again.
///
/// `catalogue::query` decides eligibility from the row's own atomics and
/// nothing else, so a pair or a provider marked down in the health table is
/// invisible to the walk until the poller next rebuilds the catalogue. That
/// gap is what let one run keep racing all 1,401 channels on a single key
/// after the key itself had already been handed to the poller as dead.
/// `probe::build_channels` writes the same three fields when it builds fresh
/// rows, so this only makes the live path agree with the rebuilt one.
///
/// A plain `store` is right because `level` picks the one field this level
/// owns. Nothing else writes it, so there is no other writer to race, and a
/// lift cannot be swallowed by a `fetch_max` that only ever climbs.
fn set_hold(
    channels: &[Channel],
    level: fn(&ChannelHealth) -> &AtomicU64,
    deadline: u64,
    covered: impl Fn(&Channel) -> bool,
) {
    for row in channels.iter().filter(|c| covered(c)) {
        level(&row.health).store(deadline, Ordering::Relaxed);
    }
}

/// Records a success against a level above the channel and lifts the hold
/// that level had put on the rows it covers.
///
/// Both halves are owed. Dropping the table entry alone leaves every row the
/// escalation stamped sitting in a backoff nothing will ever lift: the poller
/// skips a pair it now reads as healthy and already known (`poll_keys`), so no
/// rebuild comes to clear the rows, and `/healthz` shows nothing because the
/// entry it reads has gone. A key that has just proved itself alive would go
/// dark for the rest of its old deadline.
///
/// The lift is skipped when the level was not down. Every success passes
/// through here, and a scan of every row on a provider is not worth paying
/// when there was nothing held against it.
fn clear_hold(
    health: &HealthTable,
    channels: &[Channel],
    key: &HealthKey,
    level: fn(&ChannelHealth) -> &AtomicU64,
    covered: impl Fn(&Channel) -> bool,
) {
    if health.get(key).is_none() {
        health.record_success(key);
        return;
    }
    health.record_success_then(key, || set_hold(channels, level, 0, covered));
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
    /// Why the trial failed, which picks the backoff schedule. `None` falls
    /// back to the default one.
    class: Option<ErrorClass>,
}

/// What one channel's trial produced, after the M copies have been reduced to
/// a single answer.
pub enum TrialOutcome<'a> {
    /// At least one copy returned 2xx with content.
    Won(Box<Winner>),
    /// At least one copy started streaming successfully. It has produced
    /// content and nothing more: whether it wins is the walk's decision.
    WonStreaming(Box<ReadyStream>),
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
    /// Why it failed, for `router_errors_total`. Carried as a class rather
    /// than read back out of `error`, which is prose written for a human
    /// reading a `504` and would break the first time somebody reworded one.
    /// `None` is the one ending that is not the channel's doing: the call was
    /// cancelled out from under it.
    class: Option<ErrorClass>,
}

/// Which copy counter a failure class belongs on.
///
/// Deliberately lossy: a connect failure and a `502` are both `http_error` on
/// `router_copies_total` and stay apart on `router_errors_total`. The coarse
/// counter is the one an operator reads first, so it has few enough values to
/// read at a glance.
/// How a stream that died before its first token maps onto the counters.
///
/// Split out of the match it came from so the mapping can be pinned by a test:
/// the three cases look alike from the call site and mean three different
/// things to whoever reads the page.
fn prefill_failure_class(failure: &StreamStartError) -> (AttemptOutcome, ErrorClass) {
    match failure {
        // A prefill deadline is a timeout, not a bad key. The operator reads
        // `http_error` as a dead channel, so a provider that is merely slow
        // must not land there.
        StreamStartError::Timeout(_) => (AttemptOutcome::Timeout, ErrorClass::Timeout),
        // The socket broke after a 2xx and before a single token, so the
        // answer never started. Not a connect failure, because the socket
        // opened, and not a stall, because nothing went quiet: it went away.
        //
        // Its own class rather than the catch-all. On a live run this was 70
        // of 140 failures, the largest group on the page, sharing one bin
        // with a 2xx that carried no answer and with the router's own scratch
        // disk faults. Three different problems under one name that was meant
        // for the rare leftover.
        StreamStartError::Transport(_) => (AttemptOutcome::HttpError, ErrorClass::PrefillTransport),
        // A 2xx carrying no answer. A channel failure like any other: the
        // sibling or the next channel takes over, and health counts it.
        StreamStartError::NotAnAnswer(_) => (AttemptOutcome::HttpError, ErrorClass::NotAnAnswer),
    }
}

fn copy_outcome_for(class: ErrorClass) -> CopyOutcome {
    match class {
        ErrorClass::Timeout => CopyOutcome::Timeout,
        ErrorClass::SelfInflicted429 => CopyOutcome::SelfInflicted429,
        ErrorClass::BodyUnparseable | ErrorClass::BodyUndecodable => CopyOutcome::BodyError,
        _ => CopyOutcome::HttpError,
    }
}

/// What one client request actually fired, for `router_trials_per_request`
/// and `router_copies_per_request`.
///
/// Counted when a trial and its copies start, not when the walk queues them.
/// A trial pushed onto the set and never polled once, which is what a client
/// that hung up before the first poll leaves behind, fires nothing: counting
/// it would put M copies into the histogram that never left the box and leave
/// it disagreeing with `router_copies_total`.
#[derive(Default)]
struct WalkCount {
    /// Channel calls queued, which is the number the `504` body reports. Not
    /// the same as `copies`: a trial queued and never polled is counted here
    /// and fires nothing.
    queued: AtomicU32,
    trials: AtomicU32,
    copies: AtomicU32,
}

impl WalkCount {
    fn queued(&self) -> u32 {
        self.queued.load(Ordering::Relaxed)
    }

    fn trials(&self) -> u32 {
        self.trials.load(Ordering::Relaxed)
    }

    fn copies(&self) -> u32 {
        self.copies.load(Ordering::Relaxed)
    }
}

/// One copy's own counters, including the copy nobody ever awaits.
///
/// A copy whose sibling wins is dropped mid-await when the trial's set goes,
/// so its own code never runs again. That is how the live run came back with
/// 150 `http_error` records against 92 trials and no count whatever of the
/// copies that were torn down: nothing was watching the drop. This watches it.
struct CopyRecord {
    metrics: Arc<Metrics>,
    provider: String,
    started: tokio::time::Instant,
    /// Identity and clock for the per-copy telemetry line. Every offset the
    /// line carries is measured on the walk's own clock, so a saved log can
    /// rebuild any (N, M) no larger than the configuration that ran.
    channel_id: ChannelId,
    model_id: String,
    copy_index: u32,
    walk_started: tokio::time::Instant,
    /// Set when this copy's stream produced its first content, as an offset
    /// from `fired_at`, so the line can say when the first token would have
    /// reached a client. A buffered copy leaves it unset: its content arrives
    /// at the end, so its ttft is its lifetime.
    fired_at: tokio::time::Instant,
    first_token: Option<Duration>,
    /// Set once the copy has recorded its own ending. One that reaches `Drop`
    /// still clear was torn down rather than finished.
    reported: bool,
    /// This trial's own token, a child of the walk's. Cancelled means the
    /// walk tore the whole trial down, so a copy dropped under it was
    /// cancelled; clear means the only thing that can have taken this copy is
    /// a sibling of its own winning. Reading the token rather than a flag the
    /// trial raises is the point: three of the walk's endings drop the trial
    /// future without polling it again, so no flag the trial sets itself
    /// would ever be raised on them.
    cancel: CancellationToken,
}

impl CopyRecord {
    #[allow(clippy::too_many_arguments)] // the telemetry identity rides on open()
    fn open(
        metrics: Arc<Metrics>,
        channel: &Channel,
        cancel: &CancellationToken,
        fired: &WalkCount,
        walk_started: tokio::time::Instant,
        copy_index: u32,
    ) -> Self {
        fired.copies.fetch_add(1, Ordering::Relaxed);
        let fired_at = tokio::time::Instant::now();
        Self {
            metrics,
            provider: channel.provider.to_string(),
            channel_id: channel.id,
            model_id: channel.model_id.to_string(),
            copy_index,
            walk_started,
            fired_at,
            first_token: None,
            started: fired_at,
            reported: false,
            cancel: cancel.clone(),
        }
    }

    /// Restarts the clock for a hedge copy that has waited out its
    /// offset. The stagger is the router's own wait, and folding it into the
    /// latency would make every copy above 0 look slower than the first.
    fn restart(&mut self) {
        self.started = tokio::time::Instant::now();
    }

    /// Records when this copy's stream produced its first content, as an
    /// offset from the copy's own fire moment, so the ending line can say
    /// when the first token would have reached a client.
    fn note_first_token(&mut self, at: tokio::time::Instant) {
        self.first_token = Some(at.saturating_duration_since(self.fired_at));
    }

    /// The copy ended on its own terms, or its Drop caught a torn-down one.
    /// The log line is the per-copy telemetry: one line per copy, all offsets
    /// on the walk's clock, so a saved run rebuilds any smaller (N, M).
    fn finish(&mut self, outcome: CopyOutcome, class: Option<ErrorClass>) {
        if self.reported {
            return;
        }
        self.reported = true;
        self.metrics.record_copy(&self.provider, outcome);
        self.metrics
            .record_copy_latency(outcome, self.started.elapsed());
        tracing::info!(
            channel = %self.channel_id,
            provider = %self.provider,
            model = %self.model_id,
            copy_index = self.copy_index,
            start_ms = self
                .fired_at
                .saturating_duration_since(self.walk_started)
                .as_millis() as u64,
            ms = self.started.elapsed().as_millis() as u64,
            ttft_ms = self
                .first_token
                .map(|offset| offset.as_millis() as u64),
            outcome = outcome.label(),
            class = class.map(|c| c.label()),
            "channel_copy"
        );
    }

    /// The copy failed, with the class off the failure itself.
    fn failed(&mut self, failure: &CopyFailure) {
        match failure.class {
            Some(class) => {
                self.metrics.record_error(&self.provider, class);
                self.finish(copy_outcome_for(class), Some(class));
            }
            None => self.finish(CopyOutcome::Cancelled, None),
        }
    }
}

impl Drop for CopyRecord {
    fn drop(&mut self) {
        if self.reported {
            return;
        }
        let outcome = if self.cancel.is_cancelled() {
            CopyOutcome::Cancelled
        } else {
            CopyOutcome::Superseded
        };
        self.finish(outcome, None);
    }
}

/// One trial's verdict and its share of `router_trials_inflight`.
///
/// Both ride on the drop, because three of the walk's endings tear a trial
/// down by dropping its future rather than polling it: a streaming win, the
/// client hanging up and the link proving to be down. Recorded in the trial's
/// own body, those three would record nothing at all, and the sum that this
/// whole change exists for, M copies to one trial, would stop reconciling.
///
/// The gauge is counted on the shared racer rather than off one walk's own
/// set, because several client requests race at once and a walk publishing
/// its own N would overwrite whatever its neighbours were showing.
struct TrialRecord<'a> {
    metrics: &'a Metrics,
    inflight: &'a Mutex<u32>,
    provider: &'a str,
    /// Set once the trial has recorded its verdict, so `Drop` knows whether
    /// it ended or was torn down.
    settled: bool,
}

impl<'a> TrialRecord<'a> {
    fn open(
        metrics: &'a Metrics,
        inflight: &'a Mutex<u32>,
        provider: &'a str,
        fired: &WalkCount,
    ) -> Self {
        fired.trials.fetch_add(1, Ordering::Relaxed);
        move_gauge(metrics, inflight, 1);
        Self {
            metrics,
            inflight,
            provider,
            settled: false,
        }
    }

    /// The trial reached a verdict of its own.
    fn settle(&mut self, outcome: TrialOutcomeLabel) {
        if self.settled {
            return;
        }
        self.settled = true;
        self.metrics.record_trial(self.provider, outcome);
    }
}

impl Drop for TrialRecord<'_> {
    fn drop(&mut self) {
        // Torn down mid-flight, which is only ever another channel winning,
        // the client leaving, or a dead link. All three are the same thing to
        // this counter: this channel never got to finish.
        self.settle(TrialOutcomeLabel::Cancelled);
        move_gauge(self.metrics, self.inflight, -1);
    }
}

/// Moves the trials-in-flight count and publishes it under one lock.
///
/// Counting and publishing as two steps lets two trials opening at once
/// interleave and latch the gauge below the true count until the next trial
/// moves it. This is the number read against `race_width` at the moment of
/// peak concurrency, which is exactly the moment it would be wrong.
fn move_gauge(metrics: &Metrics, inflight: &Mutex<u32>, by: i32) {
    let mut open = inflight
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *open = open.saturating_add_signed(by);
    metrics.set_trials_inflight(u64::from(*open));
}

/// A channel's raw reply bytes, still in that provider's framing.
type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, CallError>> + Send + 'static>>;

/// Why a stream's opening did not count as a win.
enum StreamStartError {
    /// No first token arrived inside the prefill budget.
    Timeout(String),
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
///
/// `first_token` is the whole prefill budget, not a per-chunk gap. The
/// channel has that long to get from the request to its first token; a
/// channel that has not managed it by then has failed, and because nothing
/// has reached the client yet it is an ordinary failed trial and the walk
/// carries on without it. `stream_stall_timeout` governs the gaps after this,
/// once the stream is running, and is applied by [`watch_for_stalls`].
async fn read_stream_start(
    rest: &mut ByteStream,
    first_token: Duration,
) -> Result<(Vec<Vec<u8>>, FrameCounter), StreamStartError> {
    let mut buffered: Vec<Vec<u8>> = Vec::new();
    let mut sse: Vec<u8> = Vec::new();
    let mut content = false;
    let deadline = tokio::time::Instant::now() + first_token;

    loop {
        let next = match tokio::time::timeout_at(deadline, rest.next()).await {
            Ok(next) => next,
            Err(_) => {
                return Err(StreamStartError::Timeout(format!(
                    "no first token in {}s",
                    first_token.as_secs()
                )))
            }
        };
        match next {
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
    // The opening already holds tokens, so the count starts from what was read
    // rather than from the next chunk.
    let mut counter = FrameCounter::default();
    counter.feed(&sse);
    Ok((buffered, counter))
}

/// How much of a failed streamed call's body is read back for the reason, and
/// how long that read may wait. The body is an error, not an answer, so a few
/// kilobytes carry every message any provider sends; and a provider that opens
/// a stream and then goes quiet must not hold the walk up behind it.
const ERROR_BODY_BYTES: usize = 8 * 1024;
const ERROR_BODY_WAIT: Duration = Duration::from_secs(2);

/// Reads the opening of a failed streamed call's body, so the reason carries
/// the provider's own message rather than a bare status code.
///
/// Both caps are deliberate. Whatever has arrived when either runs out is what
/// gets reported, and nothing arriving at all simply gives the status on its
/// own, which is what this path did before.
async fn read_error_body(rest: &mut ByteStream) -> Vec<u8> {
    let deadline = tokio::time::Instant::now() + ERROR_BODY_WAIT;
    let mut body: Vec<u8> = Vec::new();
    while body.len() < ERROR_BODY_BYTES {
        match tokio::time::timeout_at(deadline, rest.next()).await {
            Ok(Some(Ok(chunk))) => body.extend_from_slice(&chunk),
            // End of stream, a transport error, or the wait ran out.
            _ => break,
        }
    }
    body.truncate(ERROR_BODY_BYTES);
    body
}

/// Counts the content-bearing `data:` frames a stream has produced, across
/// chunk boundaries, and records whether the stream has carried an in-band
/// provider error frame.
///
/// One content frame is one token: every provider here sends a delta per
/// token. Counting frames is therefore the tokens-per-second measure, with no
/// tokeniser and without decoding the stream twice.
struct FrameCounter {
    /// Raw chunks go in here and come out of `events` as whole events.
    wire: futures::channel::mpsc::UnboundedSender<Result<Vec<u8>, std::convert::Infallible>>,
    /// The SSE decoder. It holds a half-read event until the rest arrives, so
    /// an event is counted once and never early, and the `data:` lines of one
    /// event are joined before anything parses them.
    events: eventsource_stream::EventStream<
        futures::channel::mpsc::UnboundedReceiver<Result<Vec<u8>, std::convert::Infallible>>,
    >,
    /// Content frames seen so far.
    frames: u32,
    /// Reasoning frames seen so far. Not tokens, but they prove the model is
    /// still working, so they hold the stall timer off.
    reasoning: u32,
    /// The stream has carried an in-band provider error frame.
    failed: bool,
    /// The stream has carried its terminal event: OpenAI's `[DONE]` or
    /// Anthropic's `message_stop`. Nothing more is owed.
    terminal: bool,
    /// The answer is complete and only a trailing frame, such as OpenAI's
    /// usage chunk, may still follow: a `finish_reason` or a `stop_reason`.
    finishing: bool,
}

impl Default for FrameCounter {
    fn default() -> Self {
        let (wire, rx) = futures::channel::mpsc::unbounded();
        Self {
            wire,
            events: eventsource_stream::EventStream::new(rx),
            frames: 0,
            reasoning: 0,
            failed: false,
            terminal: false,
            finishing: false,
        }
    }
}

impl FrameCounter {
    /// Notes whether this event ends the answer, or says it is complete.
    fn note_ending(&mut self, data: &str) {
        let data = data.trim();
        if data == "[DONE]" {
            self.terminal = true;
            return;
        }
        let Ok(json) = serde_json::from_str::<serde_json::Value>(data) else {
            return;
        };
        match json.get("type").and_then(serde_json::Value::as_str) {
            Some("message_stop") => self.terminal = true,
            Some("message_delta") => {
                if json
                    .pointer("/delta/stop_reason")
                    .is_some_and(|reason| !reason.is_null())
                {
                    self.finishing = true;
                }
            }
            _ => {
                let finished = json
                    .get("choices")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|choices| {
                        choices.iter().any(|choice| {
                            choice
                                .get("finish_reason")
                                .is_some_and(|reason| !reason.is_null())
                        })
                    });
                if finished {
                    self.finishing = true;
                }
            }
        }
    }

    /// Takes the next raw chunk off the wire.
    fn feed(&mut self, chunk: &[u8]) {
        // The receiver lives in `self`, so the send cannot fail.
        let _ = self.wire.unbounded_send(Ok(chunk.to_vec()));
        // Drain every event the chunk completed. Pending means the decoder
        // wants more bytes; the sender is never closed, so it never ends.
        while let Some(Some(event)) = self.events.next().now_or_never() {
            let Ok(event) = event else { continue };
            self.note_ending(&event.data);
            // The same reader the race uses to decide a stream is an answer,
            // so keep-alives and error events are not tokens here either, and
            // an error frame marks the whole stream as failed: a 200 carrying
            // an error is a failure in disguise, not a fast answer. Issue 27,
            // 21 Oct 2026.
            match crate::health::event_verdict(&event.data) {
                StreamVerdict::Failed => self.failed = true,
                StreamVerdict::Content => self.frames = self.frames.saturating_add(1),
                StreamVerdict::Pending if crate::health::event_is_reasoning(&event.data) => {
                    self.reasoning = self.reasoning.saturating_add(1);
                }
                StreamVerdict::Pending => {}
            }
        }
    }
}

/// How long a finished answer's socket is given after its terminal event
/// before the stream ends itself.
///
/// The decoder keeps reading bytes past the terminal event, because a provider
/// may still send a trailing frame: OpenAI's usage chunk arrives after
/// `finish_reason`. Anthropic sends nothing after `message_stop`, so for that
/// wire this is simply how long the router waits on a provider holding the
/// body open for connection reuse.
const TAIL_GRACE: Duration = Duration::from_secs(1);

/// A streamed copy that has produced content, with whatever was read before
/// it showed.
pub struct ReadyStream {
    channel_id: ChannelId,
    provider: String,
    model_id: String,
    intelligence: u16,
    wire: crate::config::WireName,
    /// Everything read so far. Decoded into the whole answer once it ends.
    buffered: Vec<Vec<u8>>,
    /// Tokens and reasoning seen so far, and whether the answer has ended.
    counter: FrameCounter,
    /// When this stream produced its first token. With the time to it, this
    /// gives the winning call's wall clock.
    content_at: tokio::time::Instant,

    /// This trial's slot in the walk's fire order, which the N-race
    /// outcome reads: slot 0 winning means the race bought nothing.
    slot: u32,
    elapsed: Duration,
    rest: Pin<Box<dyn Stream<Item = Result<Vec<u8>, CallError>> + Send + 'static>>,
    /// Holds the channel's in-flight count up until this stream is finished or
    /// dropped. The socket is open for the whole answer, so the count must say
    /// so, or a burst of streams looks idle to the self-inflicted `429` rule.
    _open: InFlight,
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

/// What the hedge bought on a trial that won.
///
/// `winner` is the index the winning copy fired at, and it decides the answer
/// before the failure count does. Copy 0 is the one that runs at `M=1`, so a
/// win on 0 means the hedge changed nothing even if every sibling failed: the
/// trial would have won without them.
///
/// Past that, `failed_siblings` separates the two ways a hedge copy can win.
/// Every other copy dead means the trial would have failed without M, so the
/// hedge rescued it. A sibling still running means the hedge copy simply got
/// there first, which saved time by an amount nothing can measure, because
/// the race kills the loser rather than letting it finish.
fn hedge_outcome(copies: usize, winner: usize, failed_siblings: usize) -> HedgeOutcome {
    if copies <= 1 {
        return HedgeOutcome::NoHedge;
    }
    if winner == 0 {
        // The copy that would have run on its own at M=1 is the one that
        // answered, so every other copy was spent for nothing, whatever
        // became of them.
        return HedgeOutcome::Redundant;
    }
    if failed_siblings >= copies - 1 {
        HedgeOutcome::Rescued
    } else {
        HedgeOutcome::Faster
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

/// How a trial's copies came out.
#[derive(Debug, PartialEq)]
enum CopyRace<T> {
    /// A copy answered, possibly in the same instant the token fired.
    Won(T),
    /// The token fired and no copy had answered.
    Cancelled,
    /// Every copy failed. Each failure went to the caller's callback.
    AllFailed,
}

/// Runs a trial's copies until one answers, all fail, or `cancel` fires.
///
/// The select is biased on cancellation, so a copy that finished just before
/// the token fired would never be polled. The cancellation is handled after
/// the select, once the borrow of `running` has ended, so that copy is still
/// collected: it is paid for either way, and may come from a smarter model
/// than whatever won the race.
async fn first_copy<T, E, F>(
    running: &mut FuturesUnordered<F>,
    cancel: &CancellationToken,
    mut failed: impl FnMut(E),
) -> CopyRace<T>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    loop {
        let (cancelled, next) = tokio::select! {
            biased;
            _ = cancel.cancelled() => (true, None),
            next = running.next() => (false, next),
        };

        if cancelled {
            return match finished_now(running).into_iter().flatten().next() {
                Some(win) => CopyRace::Won(win),
                None => CopyRace::Cancelled,
            };
        }

        match next {
            // The first copy home wins outright. A buffered reply is
            // complete, so there is nothing a later copy could buy that
            // waiting longer does not cost more of.
            Some(Ok(win)) => return CopyRace::Won(win),
            Some(Err(failure)) => failed(failure),
            None => return CopyRace::AllFailed,
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

/// What the N-race bought for the walk that just ended with a winner: slot 0
/// is the first channel fired, so a slot-0 win means a sequential walk would
/// have landed on the same answer, and a later slot winning means the race
/// carried the walk past whatever the earlier channels were doing.
fn race_stat(slot: u32, failed_before_win: u32) -> crate::metrics::RaceStat {
    if slot == 0 {
        crate::metrics::RaceStat::FirstWon
    } else if failed_before_win > 0 {
        crate::metrics::RaceStat::Rescued
    } else {
        crate::metrics::RaceStat::LaterWon
    }
}

/// The streams being read to their end, beside the trials in flight.
type Streams = FuturesUnordered<Pin<Box<dyn std::future::Future<Output = Finished> + Send>>>;

/// A stream read as far as it went, and how it ended.
struct Finished {
    ready: ReadyStream,
    /// `Ok` when the stream reached its end, `Err` with the reason it broke.
    /// An end is not yet a whole answer: the decode decides that.
    end: Result<(), ErrorClass>,
}

/// Reads one stream to its end, buffering everything, with the stall timer
/// running between signs of life.
///
/// A token or a reasoning frame resets the timer; a keepalive comment does not.
/// The provider's terminal event ends the read at once. After a
/// `finish_reason` only a trailing frame such as OpenAI's usage chunk is still
/// owed, so the wait shortens to [`TAIL_GRACE`] rather than holding a winner
/// back on a provider that keeps its socket open.
async fn read_to_end(mut ready: ReadyStream, stall: Duration) -> Finished {
    let mut deadline = tokio::time::Instant::now() + stall;
    let end = loop {
        match tokio::time::timeout_at(deadline, ready.rest.next()).await {
            Err(_) if ready.counter.finishing => break Ok(()),
            Err(_) => break Err(ErrorClass::StreamStalled),
            Ok(None) => break Ok(()),
            Ok(Some(Err(_))) => break Err(ErrorClass::StreamTransport),
            Ok(Some(Ok(chunk))) => {
                let before = (ready.counter.frames, ready.counter.reasoning);
                ready.counter.feed(&chunk);
                ready.buffered.push(chunk);
                if ready.counter.failed {
                    break Err(ErrorClass::StreamError);
                }
                if ready.counter.terminal {
                    break Ok(());
                }
                if (ready.counter.frames, ready.counter.reasoning) != before {
                    deadline = tokio::time::Instant::now() + stall;
                }
                if ready.counter.finishing {
                    deadline = deadline.min(tokio::time::Instant::now() + TAIL_GRACE);
                }
            }
        }
    };
    Finished { ready, end }
}

/// The verdict on a stream that has stopped: the whole answer as a winner, or
/// the stream back with the class of the way it broke. No side effects: the
/// caller moves health and counts the error.
async fn judge(done: Finished) -> Result<Box<Winner>, Box<(ReadyStream, ErrorClass)>> {
    let Finished { mut ready, end } = done;
    if let Err(class) = end {
        return Err(Box::new((ready, class)));
    }
    // Taken by value: the stream itself is not `Sync`, so it cannot be
    // borrowed across the await.
    let buffered = std::mem::take(&mut ready.buffered);
    match whole_answer(buffered, wire::format_for_provider(ready.wire)).await {
        Ok(response) => Ok(Box::new(Winner {
            channel_id: ready.channel_id,
            provider: ready.provider.clone(),
            model_id: ready.model_id.clone(),
            intelligence: ready.intelligence,
            response,
            latency: ready.elapsed + ready.content_at.elapsed(),
            slot: ready.slot,
        })),
        Err(_) => Err(Box::new((ready, ErrorClass::StreamTruncated))),
    }
}

/// Decodes a stream that reached its end into one whole answer. A body that
/// stopped without its terminal event fails here, which is the only place a
/// clean close part way through an answer shows.
async fn whole_answer(
    buffered: Vec<Vec<u8>>,
    format: wire::Format,
) -> Result<NeutralResponse, wire::WireError> {
    let bytes = futures::stream::iter(buffered.into_iter().map(Ok));
    let chunks = wire::decode_stream(bytes, format)?;
    wire::aggregate_stream(chunks).await
}

/// The trials in flight. The futures are boxed so the top-up is one helper
/// rather than a copy of the same push on every path that loses a trial.
type Trials<'a> =
    FuturesUnordered<Pin<Box<dyn std::future::Future<Output = TrialOutcome<'a>> + Send + 'a>>>;

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
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            created: None,
            health: ChannelHealth::live(),
            telemetry: ChannelTelemetry::default(),
        }
    }

    /// Several providers answer a dead key or an empty quota with a `200`
    /// whose body holds the error. Reporting that as "status 200" hands the
    /// operator a success code and no reason, so the message has to survive.
    #[test]
    fn race_stat_reads_the_winner_slot_and_failures() {
        // Slot 0 winning: a sequential walk lands on the same channel anyway.
        assert!(matches!(
            race_stat(0, 0),
            crate::metrics::RaceStat::FirstWon
        ));
        assert!(matches!(
            race_stat(0, 3),
            crate::metrics::RaceStat::FirstWon
        ));
        // A later slot winning with nothing failed: the race bought speed.
        assert!(matches!(
            race_stat(4, 0),
            crate::metrics::RaceStat::LaterWon
        ));
        // A later slot winning after a failure: the race carried the walk
        // past a dead channel.
        assert!(matches!(race_stat(4, 1), crate::metrics::RaceStat::Rescued));
    }

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
    fn health_table() -> (HealthTable, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a throwaway directory");
        let db = redb::Database::create(dir.path().join("health.redb")).unwrap();
        let table = HealthTable::open(
            std::sync::Arc::new(db),
            Duration::from_secs(1),
            Duration::from_secs(3600),
            Metrics::new().expect("the prometheus recorder builds"),
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
        let (health, _dir) = health_table();
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
        escalate_failure(&health, &channels, &channels[0], None, None, now);
        assert!(!is_unhealthy(&health, &pair_zero));

        // Now the last one on key-0 dies: the pair goes to the poller. key-1
        // is still healthy, so the provider does not.
        fail_channel(&health, &channels[1], now);
        escalate_failure(&health, &channels, &channels[1], None, None, now);
        assert!(is_unhealthy(&health, &pair_zero));
        assert!(!is_unhealthy(&health, &provider));

        // The last key on the provider dies too.
        fail_channel(&health, &channels[2], now);
        escalate_failure(&health, &channels, &channels[2], None, None, now);
        assert!(is_unhealthy(&health, &pair_one));
        assert!(is_unhealthy(&health, &provider));
    }

    /// A dead key puts the pair into backoff, but `catalogue::query` reads
    /// eligibility off the channel row alone. Without the deadline being
    /// pushed down onto the rows, every channel on that key reads eligible
    /// again the moment its own short backoff expires, and the walk goes
    /// straight back into a key the poller has already written off. One live
    /// run raced all 1,401 channels on a single provider this way while
    /// `/metrics` reported nothing in backoff.
    #[test]
    fn a_pair_and_a_provider_in_backoff_hold_their_channel_rows_back() {
        let (health, _dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![
            row("nvidia", "key-0", "model-a"),
            row("nvidia", "key-0", "model-b"),
        ];

        // Both channels fail with no `Retry-After`, so each row serves the
        // one second base.
        for channel in &channels {
            fail_channel(&health, channel, now);
        }
        // The upstream asked for ten minutes on the last one, which is what
        // the pair and the provider back off for.
        escalate_failure(&health, &channels, &channels[1], Some(600), None, now);

        let pair = health
            .get(&pair_key(&channels[0]))
            .expect("the pair is down");
        assert_eq!(pair.eligible_at, now + 600);
        let provider = health
            .get(&HealthKey::Provider("nvidia".to_string()))
            .expect("the provider is down");
        assert_eq!(provider.eligible_at, now + 600);

        // Ten seconds on, each row's own one second backoff is long gone.
        let later = now + 10;
        for channel in &channels {
            assert!(
                !channel.eligible(later),
                "a channel on a pair in backoff must not read eligible"
            );
            assert_eq!(
                channel.health.not_before(),
                now + 600,
                "the row carries the deadline recorded above it"
            );
        }
    }

    /// Both levels above the channel have to reach the rows, and they cover
    /// different rows.
    ///
    /// A pair going down covers only its own key, which is the common case of
    /// one expired key out of several. A provider going down covers every key
    /// under it, including the keys the failed channel never touched, and the
    /// walk holds one snapshot it scans in rank order, so those other keys are
    /// exactly the rows it tries next. Neither level reaches the rows on its
    /// own, so a run keeps racing a key or a host already written off.
    #[test]
    fn a_pair_and_a_provider_going_down_mid_walk_each_hold_the_rows_they_cover() {
        let (health, _dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![
            row("nvidia", "key-0", "model-a"),
            row("nvidia", "key-1", "model-a"),
            row("nvidia", "key-1", "model-b"),
        ];
        let provider = HealthKey::Provider("nvidia".to_string());
        // Ten seconds on, every row's own one second base has long expired, so
        // anything still held is held by a level above it.
        let later = now + 10;

        // key-0 dies and takes its own pair down for five minutes. key-1 is
        // still live, so the provider does not move.
        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], Some(300), None, now);
        assert!(!is_unhealthy(&health, &provider));
        assert!(
            !channels[0].eligible(later),
            "the pair level must hold the rows on its own key"
        );
        assert!(
            channels[1].eligible(later) && channels[2].eligible(later),
            "and must not touch the rows on a key that is still live"
        );

        // Now both channels on key-1 die. That is the last key, so the
        // provider follows it down, for ten minutes.
        fail_channel(&health, &channels[1], now);
        fail_channel(&health, &channels[2], now);
        escalate_failure(&health, &channels, &channels[2], Some(600), None, now);
        assert_eq!(
            health
                .get(&provider)
                .expect("the provider is down")
                .eligible_at,
            now + 600
        );
        for channel in &channels {
            assert!(
                !channel.eligible(later),
                "the provider level must hold every key under it"
            );
        }
        assert_eq!(
            channels[0]
                .health
                .provider_eligible_at
                .load(Ordering::Relaxed),
            now + 600,
            "including the key that did not fire the escalation"
        );
        assert_eq!(
            channels[0].health.not_before(),
            now + 600,
            "and the longer provider deadline wins over its own pair's"
        );
    }

    /// A pair coming back has to lift the hold off every row it stamped, not
    /// just off the channel that proved it.
    ///
    /// The poller will not do it: once live traffic clears the pair entry the
    /// poller reads the pair as healthy and already known and skips it, so no
    /// rebuild comes. Without the lift, 1,400 rows sit dark for the rest of a
    /// deadline whose table entry has gone, and `/healthz` has nothing left to
    /// show. That is a worse fault than the one the hold was added for: an
    /// unavailable key that has just proved itself alive, and no metric says so.
    #[test]
    fn a_pair_coming_back_lifts_the_hold_off_every_row_it_stamped() {
        let (health, _dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![
            row("nvidia", "key-0", "model-a"),
            row("nvidia", "key-0", "model-b"),
            row("nvidia", "key-0", "model-c"),
        ];

        for channel in &channels {
            fail_channel(&health, channel, now);
        }
        escalate_failure(&health, &channels, &channels[2], Some(600), None, now);
        let later = now + 10;
        assert!(
            channels.iter().all(|c| !c.eligible(later)),
            "every row is held before the success"
        );

        // One trial that was already in flight comes back 2xx. The channel
        // level clears its own row, and the levels above it clear the rest.
        health.record_success(&HealthKey::Channel(channels[0].id));
        channels[0].health.eligible_at.store(0, Ordering::Relaxed);
        write_success_above(&health, &channels, &channels[0]);

        for channel in &channels {
            assert!(
                channel.eligible(later),
                "a pair that has proved itself alive must not leave rows dark"
            );
        }
    }

    /// A channel failing on its own must not shorten a deadline a level above
    /// it is serving.
    ///
    /// The mirror in `write_failure` stores the channel entry's own deadline,
    /// and that entry knows nothing about the pair or the provider. Sharing
    /// one field let a one second base land on top of a ten minute provider
    /// hold, which put the row back into a provider already handed to the
    /// poller as dead, 599 seconds early. `escalate_failure` returns at its
    /// `is_unhealthy` guard by then, so nothing re-applies the hold.
    #[test]
    fn a_channel_failing_alone_cannot_shorten_a_provider_hold() {
        let (health, _dir) = health_table();
        let catalogue = Catalogue::new();
        // The real clock, because `write_failure` reads it itself. A made up
        // epoch would put the channel's own deadline decades past the
        // provider's and the test would pass without proving anything.
        let now = now_unix();
        catalogue.replace(vec![
            row("nvidia", "key-0", "model-a"),
            row("nvidia", "key-1", "model-a"),
        ]);
        let snapshot = catalogue.snapshot();
        // `replace` shuffles rows of equal rank, so the rows are found by key
        // rather than by position.
        let find = |key_id: &str| {
            snapshot
                .iter()
                .find(|c| c.key_id.as_ref() == key_id)
                .expect("the row is in the catalogue")
        };
        let on_key_zero = find("key-0");
        let on_key_one = find("key-1");

        // Every key dies, so both pairs go down and the provider follows for
        // ten minutes. Every row is held.
        for channel in snapshot.iter() {
            fail_channel(&health, channel, now);
        }
        escalate_failure(&health, &snapshot, on_key_zero, Some(600), None, now);
        escalate_failure(&health, &snapshot, on_key_one, Some(600), None, now);
        assert_eq!(
            health
                .get(&HealthKey::Provider("nvidia".to_string()))
                .expect("the provider is down")
                .eligible_at,
            now + 600
        );
        let later = now + 10;
        assert!(snapshot.iter().all(|c| !c.eligible(later)));

        // A trial still in flight on key-0 now fails with no `Retry-After`,
        // so its own entry gives it the one second base.
        write_failure(&health, &catalogue, on_key_zero, None, None);

        assert!(
            on_key_zero.health.eligible_at.load(Ordering::Relaxed) < later,
            "the channel level takes its own short base, which is seconds, not \
             the provider's ten minutes"
        );
        assert_eq!(
            on_key_zero
                .health
                .provider_eligible_at
                .load(Ordering::Relaxed),
            now + 600,
            "and the provider hold above it is untouched"
        );
        assert!(
            !on_key_zero.eligible(later),
            "a channel's own short backoff must not release it from a provider hold"
        );
    }

    /// A pair already with the poller must not have its re-probe pushed out by
    /// every later channel failure on the same pair.
    #[test]
    fn a_pair_already_unhealthy_is_not_backed_off_again() {
        let (health, _dir) = health_table();
        let now = 1_000_000u64;
        let channels = vec![row("groq", "key-0", "model-a")];
        let pair = pair_key(&channels[0]);

        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], None, None, now);
        let first = health.get(&pair).expect("pair marked");

        fail_channel(&health, &channels[0], now);
        escalate_failure(&health, &channels, &channels[0], None, None, now);
        assert_eq!(health.get(&pair).expect("pair still marked"), first);
    }

    /// A local outage must not cost the channels anything. Every trial fired
    /// during it fails, and if those failures were written the channels would
    /// sit in a backoff caused by a fault that was never theirs.
    #[test]
    fn a_dead_link_drops_every_held_failure() {
        let (health, _dir) = health_table();
        let channels = vec![
            row("openrouter", "key-0", "model-a"),
            row("openrouter", "key-0", "model-b"),
        ];
        let mut held: Vec<PendingFailure> = channels
            .iter()
            .map(|channel| PendingFailure {
                channel,
                retry_after_secs: None,
                class: None,
            })
            .collect();

        settle_held(&health, &Catalogue::new(), &mut held, true);

        assert!(held.is_empty(), "the held moves are spent either way");
        for channel in &channels {
            assert_eq!(channel.health.retry_counter.load(Ordering::Relaxed), 0);
            assert_eq!(channel.health.eligible_at.load(Ordering::Relaxed), 0);
            assert!(health.get(&HealthKey::Channel(channel.id)).is_none());
        }
        // The pair must not follow them down either, or the poller starts
        // re-probing a key that never failed.
        assert!(!is_unhealthy(&health, &pair_key(&channels[0])));
    }

    /// The same failures with the link up are the channels' own, so they are
    /// written the moment the network test says so.
    #[test]
    fn a_live_link_writes_every_held_failure() {
        let (health, _dir) = health_table();
        let channels = [row("openrouter", "key-0", "model-a")];
        let mut held = vec![PendingFailure {
            channel: &channels[0],
            retry_after_secs: None,
            class: None,
        }];

        settle_held(&health, &Catalogue::new(), &mut held, false);

        assert!(held.is_empty());
        assert_eq!(channels[0].health.retry_counter.load(Ordering::Relaxed), 1);
        assert!(channels[0].health.eligible_at.load(Ordering::Relaxed) > 0);
    }

    /// A walk holds its snapshot for its whole life, and the poller can
    /// rebuild the catalogue underneath it. The backoff has to land on the row
    /// the next query will scan, not on the orphan the walk is still carrying.
    #[test]
    fn a_failure_after_a_rebuild_backs_off_the_live_row() {
        let (health, _dir) = health_table();
        let stale = row("chutes", "key-0", "model-a");
        // What `Catalogue::replace` produces: a brand-new row, same id, health
        // atomics re-seeded from the health table.
        let catalogue = Catalogue::new();
        catalogue.replace(vec![row("chutes", "key-0", "model-a")]);
        let live = catalogue.snapshot();
        assert_eq!(stale.id, live[0].id);

        write_failure(&health, &catalogue, &stale, Some(3_600), None);

        assert!(
            live[0].health.eligible_at.load(Ordering::Relaxed) > 0,
            "the live row carries the backoff"
        );
        assert_eq!(live[0].health.retry_counter.load(Ordering::Relaxed), 1);
        assert_eq!(
            stale.health.eligible_at.load(Ordering::Relaxed),
            0,
            "nothing is written onto the orphan"
        );
    }

    /// A rebuild reads the health table, builds new rows, then swaps them
    /// in. A failure written between the read and the swap must not land on
    /// the outgoing row and leave the new one looking healthy.
    #[test]
    fn a_failure_during_a_rebuild_reaches_the_new_row() {
        let (health, _dir) = health_table();
        let catalogue = Catalogue::new();
        catalogue.replace(vec![row("chutes", "key-0", "model-a")]);
        let walk = catalogue.snapshot();

        std::thread::scope(|scope| {
            catalogue.replace_with(|| {
                // The rebuild has read a healthy entry. A race now fails the
                // channel while the new rows are still being built.
                let fresh = row("chutes", "key-0", "model-a");
                scope.spawn(|| write_failure(&health, &catalogue, &walk[0], None, None));
                std::thread::sleep(Duration::from_millis(100));
                vec![fresh]
            });
        });

        let live = catalogue.snapshot();
        assert_eq!(live[0].health.retry_counter.load(Ordering::Relaxed), 1);
        assert!(
            live[0].health.eligible_at.load(Ordering::Relaxed) > 0,
            "the row the next query scans carries the backoff"
        );
    }

    fn winner(intelligence: u16, model: &str) -> Winner {
        Winner {
            channel_id: ChannelId(intelligence as u64),
            provider: "p".to_string(),
            model_id: model.to_string(),
            intelligence,
            response: NeutralResponse::default(),
            latency: Duration::from_millis(1),
            slot: 0,
        }
    }

    /// `stream_stall_timeout`, the design's default.
    const STALL: Duration = Duration::from_secs(60);

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
        let outcome = read_stream_start(&mut rest, STALL).await;
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
        let Ok((buffered, counter)) = read_stream_start(&mut rest, STALL).await else {
            panic!("a content frame should win");
        };
        assert_eq!(buffered.len(), 2);
        // The comment is not a token; the delta is. The stall timer starts
        // from what the opening already carried.
        assert_eq!(counter.frames, 1);
    }

    /// A channel that opens a stream and then says nothing has failed. Nothing
    /// has reached the client, so it is an ordinary failed trial and the walk
    /// carries on without it.
    #[tokio::test(start_paused = true)]
    async fn a_stream_with_no_first_token_inside_the_prefill_budget_drops_out() {
        let opening: ByteStream = Box::pin(
            futures::stream::iter(vec![Ok(b": OPENROUTER PROCESSING\n\n".to_vec())])
                .chain(futures::stream::pending()),
        );
        let mut rest = opening;
        let outcome = read_stream_start(&mut rest, STALL).await;
        match outcome {
            Err(StreamStartError::Timeout(error)) => assert!(error.contains("no first token")),
            _ => panic!("a stalled opening is a failed trial"),
        }
    }

    /// Groq and OpenRouter both answer a streamed request with HTTP 200 and a
    /// single error frame. That is a failure, not a winner, and it must not
    /// mark the channel healthy.
    #[tokio::test]
    async fn an_error_frame_in_a_two_hundred_does_not_win() {
        let mut rest =
            sse_stream(&["data: {\"error\":{\"code\":429,\"message\":\"rate limited\"}}\n\n"]);
        let outcome = read_stream_start(&mut rest, STALL).await;
        assert!(matches!(outcome, Err(StreamStartError::NotAnAnswer(_))));
    }

    /// An Anthropic stream that only ever opens a message carries nothing.
    #[tokio::test]
    async fn anthropic_openers_without_a_delta_do_not_win() {
        let mut rest = sse_stream(&[
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"content\":[]}}\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
        ]);
        let outcome = read_stream_start(&mut rest, STALL).await;
        assert!(matches!(outcome, Err(StreamStartError::NotAnAnswer(_))));
    }

    /// Prefill is not a gap between tokens. A 1M token prompt is 16.7 minutes
    /// of silence before the first token, so judging the opening of a stream
    /// against `stream_stall_timeout` failed every long prompt on a channel
    /// that was working perfectly well.
    #[tokio::test(start_paused = true)]
    async fn the_first_token_gets_the_prefill_budget_not_the_stall_timer() {
        let mut rest: ByteStream = Box::pin(async_stream::stream! {
            tokio::time::sleep(STALL * 3).await;
            yield Ok(token("hello"));
        });
        let outcome = read_stream_start(&mut rest, STALL * 10).await;
        assert!(
            outcome.is_ok(),
            "a channel still in prefill has not stalled"
        );
    }

    /// The prefill budget is one deadline over the whole opening, not a gap
    /// allowed between chunks. OpenRouter sends `: OPENROUTER PROCESSING`
    /// while it waits on the provider, and against a per-chunk timer those
    /// comments reset the clock for ever: the trial never failed and never
    /// produced an answer, and the walk sat behind it.
    #[tokio::test(start_paused = true)]
    async fn a_stream_of_keepalives_still_runs_out_of_prefill_budget() {
        let mut rest: ByteStream = Box::pin(async_stream::stream! {
            loop {
                tokio::time::sleep(STALL / 2).await;
                yield Ok(b": OPENROUTER PROCESSING\n\n".to_vec());
            }
        });
        let outcome = read_stream_start(&mut rest, STALL).await;
        assert!(matches!(outcome, Err(StreamStartError::Timeout(_))));
    }

    /// The same stream against a prefill budget it genuinely misses is an
    /// ordinary failed trial, so the deadline is still a deadline. It is a
    /// timeout: `router_attempts_total{outcome="http_error"}` means a dead
    /// key, and a provider sitting on a full GPU queue is not that.
    /// A socket that breaks before the first token was the largest group of
    /// failures on a live run, 70 of 140, and it shared `other` with a 2xx
    /// carrying no answer and with the router's own scratch disk faults. Three
    /// problems under one name that was meant for the rare leftover, so an
    /// operator reading `other` learned nothing from it.
    #[test]
    fn a_stream_that_dies_before_its_first_token_is_not_the_catch_all() {
        assert_eq!(
            prefill_failure_class(&StreamStartError::Transport("reset".into())),
            (AttemptOutcome::HttpError, ErrorClass::PrefillTransport)
        );

        // The other two keep what they had. A slow provider is a timeout and
        // must not read as a dead channel, and a 2xx with nothing in it is
        // the case `other` was written for.
        assert_eq!(
            prefill_failure_class(&StreamStartError::Timeout("too slow".into())),
            (AttemptOutcome::Timeout, ErrorClass::Timeout)
        );
        assert_eq!(
            prefill_failure_class(&StreamStartError::NotAnAnswer("empty".into())),
            (AttemptOutcome::HttpError, ErrorClass::NotAnAnswer)
        );
    }

    /// The byte stream is what produces the transport case, so the mapping
    /// above is only worth anything if a broken stream really lands there.
    #[tokio::test(start_paused = true)]
    async fn a_broken_byte_stream_produces_the_transport_case() {
        let mut rest: ByteStream = Box::pin(futures::stream::iter(vec![Err(
            crate::upstream::CallError::Transport("connection reset".to_string()),
        )]));
        let Err(failure) = read_stream_start(&mut rest, STALL).await else {
            panic!("a broken stream cannot start");
        };
        assert!(matches!(failure, StreamStartError::Transport(_)));
        assert_eq!(
            prefill_failure_class(&failure).1,
            ErrorClass::PrefillTransport
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_channel_that_misses_the_prefill_budget_fails_the_trial() {
        let mut rest: ByteStream = Box::pin(futures::stream::pending());
        let outcome = read_stream_start(&mut rest, STALL).await;
        assert!(matches!(outcome, Err(StreamStartError::Timeout(_))));
    }

    /// A streamed call that comes back non-2xx carries the provider's own
    /// message in its body. Reporting "status 429" alone hands an operator a
    /// number and no reason, which is the fault the buffered path was already
    /// fixed for.
    #[tokio::test]
    async fn a_failed_streamed_call_reports_the_providers_message() {
        let mut rest: ByteStream = Box::pin(futures::stream::iter(vec![Ok(
            br#"{"error":{"message":"insufficient quota"}}"#.to_vec(),
        )]));
        let read = read_error_body(&mut rest).await;
        assert_eq!(
            failure_reason(429, &read),
            "status 429: insufficient quota".to_string()
        );
    }

    /// A provider that opens a stream and then goes quiet must not hold the
    /// walk up behind it, and an endless body must not be read whole.
    #[tokio::test(start_paused = true)]
    async fn the_error_body_read_is_capped_on_both_bytes_and_time() {
        let mut rest: ByteStream = Box::pin(futures::stream::repeat_with(|| Ok(vec![b'x'; 1024])));
        assert_eq!(read_error_body(&mut rest).await.len(), ERROR_BODY_BYTES);

        let mut rest: ByteStream = Box::pin(futures::stream::pending());
        let started = tokio::time::Instant::now();
        assert!(read_error_body(&mut rest).await.is_empty());
        assert!(started.elapsed() <= ERROR_BODY_WAIT);
    }

    /// One candidate stream, with whatever is left of it still to come.
    fn ready_with(id: u64, elapsed_ms: u64, rest: ByteStream) -> ReadyStream {
        ReadyStream {
            channel_id: ChannelId(id),
            provider: "p".to_string(),
            model_id: "m".to_string(),
            intelligence: 0,
            wire: crate::config::WireName::OpenAi,
            buffered: Vec::new(),
            counter: FrameCounter::default(),
            content_at: tokio::time::Instant::now(),
            elapsed: Duration::from_millis(elapsed_ms),
            slot: 0,
            rest,
            _open: InFlight::open(&test_channel(), &Arc::new(AtomicU32::new(0))),
        }
    }

    /// One token frame, as a provider sends it.
    fn token(text: &str) -> Vec<u8> {
        format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n").into_bytes()
    }

    /// An error frame split across two chunks must still end the candidacy.
    /// The counter's SSE decoder holds a half-read event, so the verdict
    /// cannot be taken from one chunk alone.
    #[test]
    fn an_error_frame_split_across_chunks_still_breaks_the_candidate() {
        let frame = b"data: {\"error\":{\"message\":\"1211 Unknown Model\"}}\n\n".to_vec();
        let (head, tail) = frame.split_at(10);
        let mut counter = FrameCounter::default();

        counter.feed(head);
        assert!(
            !counter.failed,
            "a half-read error frame is not a failure yet"
        );
        counter.feed(tail);
        assert!(counter.failed, "the completed error frame is a failure");
    }

    /// A frame split across two chunks is one token, not two and not none.
    #[test]
    fn a_frame_split_across_chunks_is_counted_once() {
        let frame = token("hello");
        let (head, tail) = frame.split_at(20);
        let mut counter = FrameCounter::default();
        counter.feed(head);
        assert_eq!(counter.frames, 0, "a half-read frame is not a token yet");
        counter.feed(tail);
        assert_eq!(counter.frames, 1);
    }

    /// One event whose JSON is split over two `data:` lines, with CRLF line
    /// ends, is one token. Neither line parses on its own.
    #[test]
    fn a_multi_line_data_event_is_one_token() {
        let mut counter = FrameCounter::default();
        counter.feed(b"data: {\"choices\":[{\"delta\":\r\ndata: {\"content\":\"hi\"}}]}\r\n\r\n");
        assert_eq!(counter.frames, 1);
    }

    /// Keep-alives and error frames are not tokens. Counting them would hand
    /// the window to whichever channel was quickest to say nothing.
    #[test]
    fn comments_and_error_frames_are_not_tokens() {
        let mut counter = FrameCounter::default();
        counter.feed(b": OPENROUTER PROCESSING\n\n");
        counter.feed(b"data: [DONE]\n\n");
        counter.feed(b"data: {\"error\":{\"message\":\"rate limited\"}}\n\n");
        assert_eq!(counter.frames, 0);
        counter.feed(&token("x"));
        assert_eq!(counter.frames, 1);
    }

    /// A reasoning model can write a sentence and then think for longer than
    /// the stall timeout, sending reasoning the whole time. It is alive, so
    /// the read goes on; only when the reasoning stops too does the timer
    /// fire. The live router cut seven Nemotron 3 Ultra streams this way on
    /// 22 Oct 2026, each 60 seconds after its last visible word.
    #[tokio::test(start_paused = true)]
    async fn reasoning_keeps_a_stream_alive_past_the_stall_timeout() {
        let live: ByteStream = Box::pin(async_stream::stream! {
            yield Ok(token("x"));
            for _ in 0..36 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                yield Ok(b"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"hmm\"}}]}\n\n".to_vec());
            }
            futures::future::pending::<()>().await;
        });
        let started = tokio::time::Instant::now();
        let done = read_to_end(ready_with(0, 1, live), STALL).await;
        assert_eq!(done.end, Err(ErrorClass::StreamStalled));
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(180) + STALL,
            "three minutes of reasoning, then a whole stall timeout of silence"
        );
    }

    /// Keep-alive comments are not signs of life: a provider whose upstream
    /// hangs keeps its socket busy with them, and the stall must still fire.
    #[tokio::test(start_paused = true)]
    async fn keepalives_do_not_hold_a_stream_open() {
        let live: ByteStream = Box::pin(async_stream::stream! {
            yield Ok(token("x"));
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                yield Ok(b": OPENROUTER PROCESSING\n\n".to_vec());
            }
        });
        let started = tokio::time::Instant::now();
        let done = read_to_end(ready_with(0, 1, live), STALL).await;
        assert_eq!(done.end, Err(ErrorClass::StreamStalled));
        assert_eq!(started.elapsed(), STALL);
    }

    /// A finished answer on a socket the provider keeps open ends after the
    /// tail grace, not at the stall timeout: only a trailing usage frame was
    /// still owed, and a winner must not be held back a minute for it.
    #[tokio::test(start_paused = true)]
    async fn a_finished_answer_on_a_lingering_socket_ends_after_the_tail_grace() {
        let live: ByteStream = Box::pin(async_stream::stream! {
            yield Ok(token("done"));
            yield Ok(b"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n".to_vec());
            futures::future::pending::<()>().await;
        });
        let started = tokio::time::Instant::now();
        let done = read_to_end(ready_with(0, 1, live), STALL).await;
        assert_eq!(done.end, Ok(()));
        assert_eq!(started.elapsed(), TAIL_GRACE);
    }

    /// The terminal event ends the read at once, whatever the socket does.
    #[tokio::test(start_paused = true)]
    async fn the_terminal_event_ends_the_read_at_once() {
        let live: ByteStream = Box::pin(async_stream::stream! {
            yield Ok(token("done"));
            yield Ok(b"data: [DONE]\n\n".to_vec());
            futures::future::pending::<()>().await;
        });
        let started = tokio::time::Instant::now();
        let done = read_to_end(ready_with(0, 1, live), STALL).await;
        assert_eq!(done.end, Ok(()));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    /// Reads `rest` to its end as the walk does, judges it, and applies what
    /// the walk would: the error class and the backoff for a stream that broke.
    /// Returns whether it broke, the channel's health entry, and the scrape.
    async fn run_winner_to_end(
        rest: ByteStream,
    ) -> (bool, Option<crate::health::HealthEntry>, String) {
        let (health, _dir) = health_table();
        let catalogue = Catalogue::new();
        let metrics = Metrics::new().expect("the prometheus recorder builds");
        let channel = row("groq", "key-0", "model-a");
        let id = channel.id;
        catalogue.replace(vec![channel]);
        let mut ready = ready_with(0, 1, rest);
        ready.channel_id = id;
        let failed = match judge(read_to_end(ready, STALL).await).await {
            Ok(_) => false,
            Err(dead) => {
                let (ready, class) = *dead;
                metrics.record_error(&ready.provider, class);
                let snapshot = catalogue.snapshot();
                write_failure(&health, &catalogue, &snapshot[0], None, Some(class));
                true
            }
        };
        (
            failed,
            health.get(&HealthKey::Channel(id)),
            metrics.render(),
        )
    }

    /// A provider can report a failure inside an otherwise healthy stream: an
    /// error frame part way through an answer, after some content has already
    /// gone to the client. The byte stream is fine and the body ends cleanly,
    /// so neither the stall watcher nor a transport error sees it, but the
    /// channel has still failed mid-answer and must back off for it.
    #[tokio::test(start_paused = true)]
    async fn a_mid_stream_error_frame_backs_the_channel_off() {
        // Some content, then the provider says its quota is gone, then a
        // clean end of body.
        let live: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(token("hello")),
            Ok(b"data: {\"error\":{\"message\":\"quota exhausted\"}}\n\n".to_vec()),
        ]));
        let (failed, entry, render) = run_winner_to_end(live).await;

        assert!(failed, "the error frame reaches the client as an error");
        let entry = entry.expect("the channel is marked unhealthy");
        assert_eq!(entry.retry_counter, 1, "exactly once");
        assert!(entry.eligible_at > 0);
        // The provider said why, so the class says so too rather than
        // reporting the clean end of body that followed it.
        assert_eq!(
            sample(&render, "router_errors_total", &[r#"class="stream_error""#]),
            1.0
        );
        assert_eq!(
            sample(
                &render,
                "router_errors_total",
                &[r#"class="stream_truncated""#]
            ),
            0.0
        );
    }

    /// A provider that closes the body part way through an answer ends the
    /// byte stream cleanly, so the stall watcher sees nothing wrong. The
    /// decoder is what notices, at the end, with no terminal event in sight,
    /// and the channel must still back off for it.
    #[tokio::test(start_paused = true)]
    async fn a_truncated_stream_backs_the_channel_off() {
        // Half an answer, then a clean end of body: no `[DONE]`, no error.
        let live: ByteStream = Box::pin(futures::stream::iter(vec![Ok(token("half"))]));
        let (failed, entry, render) = run_winner_to_end(live).await;

        assert!(failed, "the truncation reaches the client as an error");
        let entry = entry.expect("the channel is marked unhealthy");
        assert_eq!(entry.retry_counter, 1, "exactly once");
        assert!(entry.eligible_at > 0);
        assert_eq!(
            sample(
                &render,
                "router_errors_total",
                &[r#"class="stream_truncated""#]
            ),
            1.0
        );
        assert_eq!(
            sample(&render, "router_errors_total", &[r#"class="stream_error""#]),
            0.0,
            "nothing reported a failure: the body simply stopped"
        );
    }

    /// A socket that breaks part way through an answer. The provider
    /// reported nothing and the body did not close, so this is neither of the
    /// two above, and a provider whose sockets break mid-answer is its own
    /// operational fault rather than a share of a catch-all.
    #[tokio::test(start_paused = true)]
    async fn a_stream_whose_socket_breaks_mid_answer_is_recorded_as_transport() {
        // Half an answer, then the connection goes.
        let live: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(token("half")),
            Err(CallError::Transport("connection reset".to_string())),
        ]));
        let (failed, entry, render) = run_winner_to_end(live).await;

        assert!(failed, "the break reaches the client as an error");
        let entry = entry.expect("the channel is marked unhealthy");
        assert_eq!(entry.retry_counter, 1, "exactly once");
        assert_eq!(
            sample(
                &render,
                "router_errors_total",
                &[r#"class="stream_transport""#]
            ),
            1.0
        );
        for other in ["stream_truncated", "stream_error", "stream_stalled"] {
            assert_eq!(
                sample(
                    &render,
                    "router_errors_total",
                    &[&format!(r#"class="{other}""#)]
                ),
                0.0,
                "a broken socket recorded {other}"
            );
        }
    }

    /// The guard on the three above. A complete answer also ends its body, so a
    /// truncation test that only reads the end of the stream would mark every
    /// healthy channel in the fleet as truncated. The terminal event is the
    /// whole difference.
    #[tokio::test(start_paused = true)]
    async fn a_complete_answer_is_not_recorded_as_a_death() {
        let live: ByteStream = Box::pin(futures::stream::iter(vec![
            Ok(token("all")),
            Ok(b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n".to_vec()),
            Ok(b"data: [DONE]\n\n".to_vec()),
        ]));
        let (failed, entry, render) = run_winner_to_end(live).await;

        assert!(!failed, "a complete answer is not an error");
        assert!(entry.is_none(), "and does not back the channel off");
        for class in [
            "stream_truncated",
            "stream_error",
            "stream_stalled",
            "stream_transport",
            "prefill_transport",
            "other",
        ] {
            assert_eq!(
                sample(
                    &render,
                    "router_errors_total",
                    &[&format!(r#"class="{class}""#)]
                ),
                0.0,
                "a complete answer recorded {class}"
            );
        }
    }

    /// A trial whose copies failed for different reasons backs off on the
    /// class of the copy the `504` quotes, skipping any the router caused.
    #[test]
    fn a_failed_trial_backs_off_on_its_own_class() {
        let (health, _dir) = health_table();
        let channels = [row("nvidia", "key-0", "model-a")];
        let mut held = vec![PendingFailure {
            channel: &channels[0],
            retry_after_secs: None,
            class: Some(ErrorClass::NotFound),
        }];
        let before = now_unix();

        settle_held(&health, &Catalogue::new(), &mut held, false);

        let wait = channels[0].health.eligible_at.load(Ordering::Relaxed) - before;
        assert!(
            (6 * 3600..=6 * 3600 + 1).contains(&wait),
            "waited {wait}s, not 6 hours"
        );
    }

    /// A reasoning delta is not a token, but it is counted as the model
    /// working.
    #[test]
    fn a_reasoning_frame_is_not_a_token_but_is_counted() {
        let mut counter = FrameCounter::default();
        counter.feed(b"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"hmm\"}}]}\n\n");
        counter.feed(b"data: {\"choices\":[{\"delta\":{\"reasoning\":\"hmm\"}}]}\n\n");
        assert_eq!((counter.frames, counter.reasoning), (0, 2));
        // An empty one is a keep-alive in disguise, not work.
        counter.feed(b"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"\"}}]}\n\n");
        assert_eq!(counter.reasoning, 2);
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

    /// A copy that answered in the same instant the token fired is already
    /// paid for, so the trial returns it rather than reporting a cancellation.
    /// Without the drain after the biased select it would be thrown away.
    #[tokio::test]
    async fn a_copy_that_finished_before_cancellation_is_still_collected() {
        let cancel = CancellationToken::new();
        let mut running: FuturesUnordered<futures::future::BoxFuture<'static, Result<u32, ()>>> =
            FuturesUnordered::new();
        running.push(Box::pin(futures::future::pending()));
        running.push(Box::pin(async { Ok(7u32) }));
        cancel.cancel();

        let raced = first_copy(&mut running, &cancel, |_: ()| {}).await;
        assert_eq!(raced, CopyRace::Won(7));
    }

    /// With nothing answered, a fired token is a cancellation, and a copy
    /// that failed is not mistaken for a win.
    #[tokio::test]
    async fn a_trial_with_nothing_answered_is_cancelled() {
        let cancel = CancellationToken::new();
        let mut running: FuturesUnordered<futures::future::BoxFuture<'static, Result<u32, ()>>> =
            FuturesUnordered::new();
        running.push(Box::pin(futures::future::pending()));
        running.push(Box::pin(async { Err(()) }));
        cancel.cancel();

        let raced = first_copy(&mut running, &cancel, |_: ()| {}).await;
        assert_eq!(raced, CopyRace::Cancelled);
    }

    /// Every copy failing is one outcome, and each failure reaches the caller.
    #[tokio::test]
    async fn a_trial_whose_copies_all_fail_reports_each_failure() {
        let cancel = CancellationToken::new();
        let mut running: FuturesUnordered<futures::future::BoxFuture<'static, Result<u32, u8>>> =
            FuturesUnordered::new();
        running.push(Box::pin(async { Err(1) }));
        running.push(Box::pin(async { Err(2) }));

        let mut seen = Vec::new();
        let raced = first_copy(&mut running, &cancel, |e| seen.push(e)).await;
        assert_eq!(raced, CopyRace::AllFailed);
        seen.sort();
        assert_eq!(seen, vec![1, 2]);
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

    fn copy_failure(retry_after_secs: Option<u64>) -> CopyFailure {
        CopyFailure {
            error: "429".to_string(),
            retry_after_secs,
            self_inflicted: false,
            class: Some(ErrorClass::RateLimited),
        }
    }

    #[test]
    fn the_longest_retry_after_among_the_copies_sets_the_base() {
        let failures = [
            copy_failure(Some(30)),
            copy_failure(None),
            copy_failure(Some(120)),
        ];
        assert_eq!(longest_retry_after(&failures), Some(120));
        assert_eq!(longest_retry_after(&[copy_failure(None)]), None);
        assert_eq!(longest_retry_after(&[]), None);
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

    /// A racer over one wiremock upstream, driving `run_trial` on its own.
    ///
    /// Its `Metrics` is its own instance rather than a process-wide recorder,
    /// so every assertion below reads the counters this trial wrote and
    /// nothing a test running beside it did.
    struct TrialHarness {
        racer: Racer,
        metrics: Arc<Metrics>,
        channel: Channel,
        fired: WalkCount,
        /// Held so the upstream outlives the trial that calls it.
        _server: Option<MockServer>,
        _dir: tempfile::TempDir,
    }

    impl TrialHarness {
        fn build(
            copies: usize,
            hedge_delay_ms: u64,
            base_url: &str,
            server: Option<MockServer>,
        ) -> Self {
            let dir = tempfile::tempdir().expect("a scratch directory");
            let metrics = Arc::new(Metrics::new().expect("the prometheus recorder builds"));
            let config = Arc::new(Config {
                router: crate::config::RouterConfig {
                    max_per_channel: copies,
                    hedge_delay_ms,
                    spill_path: dir.path().to_path_buf(),
                    ..Default::default()
                },
                providers: Vec::new(),
                models: Vec::new(),
                intelligence: Vec::new(),
                keys: std::collections::HashMap::new(),
                capabilities: std::collections::HashMap::new(),
            });
            let db = std::sync::Arc::new(
                redb::Database::create(dir.path().join("state.redb")).expect("the store opens"),
            );
            let health = Arc::new(
                HealthTable::open(
                    db,
                    Duration::from_secs(1),
                    Duration::from_secs(3600),
                    (*metrics).clone(),
                )
                .expect("the health table opens"),
            );
            let upstream = Upstream::new(&config.router).expect("the client builds");
            // No URLs, so the network test is never the thing under test here.
            let net = NetTest::with_urls(
                upstream.client().clone(),
                Duration::from_secs(1),
                Vec::new(),
            );
            let mut channel = test_channel();
            channel.base_url = base_url.into();

            Self {
                racer: Racer::new(
                    config,
                    Arc::new(Catalogue::new()),
                    health,
                    upstream,
                    net,
                    Arc::clone(&metrics),
                ),
                metrics,
                channel,
                fired: WalkCount::default(),
                _server: server,
                _dir: dir,
            }
        }

        async fn trial(&self, cancel: CancellationToken) -> TrialOutcome<'_> {
            let body = Arc::new(PromptBody::Memory(bytes::Bytes::from_static(
                br#"{"messages":[{"role":"user","content":"ping"}],"max_tokens":16}"#,
            )));
            self.racer
                .run_trial(
                    &self.channel,
                    body,
                    false,
                    0,
                    cancel,
                    &self.fired,
                    tokio::time::Instant::now(),
                )
                .await
        }
    }

    /// The tear-down the common endings actually use. A streaming win, a
    /// client hanging up and a dead link all drop the trial's future rather
    /// than polling it again, so nothing written inside `run_trial` runs at
    /// all. `streaming` is the default, so this is the path most trials take,
    /// and a copy recorded as `superseded` here would read as hedging beating
    /// its own sibling when another channel actually won.
    #[tokio::test]
    async fn a_trial_dropped_mid_flight_is_cancelled_not_superseded() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(chat_reply("too late"))
                    .set_delay(Duration::from_secs(30)),
            )
            .mount(&server)
            .await;
        let uri = server.uri();

        let harness = TrialHarness::build(2, 0, &uri, Some(server));
        let cancel = CancellationToken::new();
        let mut trial = Box::pin(harness.trial(cancel.clone()));
        // Polled until both copies are out on the wire and waiting.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut trial)
                .await
                .is_err(),
            "the upstream is slow, so the trial is still running"
        );

        // Another channel won: the walk cancels the race and drops the set
        // without draining it, which is what `concede_to_stream` leaves.
        cancel.cancel();
        drop(trial);

        let render = harness.metrics.render();
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="cancelled""#]),
            2.0
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="superseded""#]),
            0.0,
            "another channel won, not a sibling copy"
        );
        assert_eq!(
            sample(&render, "router_trials_total", &[r#"outcome="cancelled""#]),
            1.0,
            "a trial nobody polls again still owes a verdict"
        );
        assert_eq!(
            sample(&render, "router_trials_inflight", &[]),
            0.0,
            "the gauge comes back down on the drop"
        );
    }

    /// A trial queued and never polled fires nothing. Counting it at queue
    /// time would put M copies into `router_copies_per_request` that never
    /// left the box, and leave the histogram disagreeing with
    /// `router_copies_total`.
    #[tokio::test]
    async fn a_trial_that_is_never_polled_fires_nothing() {
        let harness = TrialHarness::build(2, 0, "http://127.0.0.1:1", None);
        drop(harness.trial(CancellationToken::new()));

        assert_eq!(harness.fired.copies(), 0);
        assert_eq!(harness.fired.trials(), 0);
        assert_eq!(
            sample(
                &harness.metrics.render(),
                "router_copies_total",
                &[r#"outcome="cancelled""#]
            ),
            0.0
        );
    }

    /// A whole OpenAI chat reply.
    fn chat_reply(text: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-fake",
            "object": "chat.completion",
            "model": "fake",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": text },
                "finish_reason": "stop",
            }],
        })
    }

    /// One sample's value out of a rendered scrape, found by the labels it
    /// carries rather than by an exact line: the exporter orders labels as it
    /// likes, and a missing series reads as the zero it stands for.
    fn sample(render: &str, metric: &str, labels: &[&str]) -> f64 {
        render
            .lines()
            .filter(|line| line.starts_with(metric))
            .find(|line| labels.iter().all(|label| line.contains(label)))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
            .unwrap_or(0.0)
    }

    /// `hedge_outcome` is the number that decides M, so its three cases are
    /// pinned here directly rather than only through a live trial. A live run
    /// returned a 5 against 5 split on the copy index, which says nothing;
    /// this is what tells the two apart.
    #[test]
    fn the_hedge_only_counts_as_a_rescue_when_every_sibling_had_failed() {
        // M=1: there is no hedge at all.
        assert_eq!(hedge_outcome(1, 0, 0), HedgeOutcome::NoHedge);

        // Copy 0 won. That is the copy that runs at M=1, so the hedge changed
        // nothing. This holds even when the sibling failed: counting it as a
        // rescue would credit the hedge for a trial that was going to win
        // without it.
        assert_eq!(hedge_outcome(2, 0, 0), HedgeOutcome::Redundant);
        assert_eq!(hedge_outcome(2, 0, 1), HedgeOutcome::Redundant);

        // Copy 1 won with copy 0 already dead: without the hedge the trial
        // would have failed and the walk would have moved to the next
        // channel.
        assert_eq!(hedge_outcome(2, 1, 1), HedgeOutcome::Rescued);

        // Copy 1 won while copy 0 was still running, so it got there first.
        assert_eq!(hedge_outcome(2, 1, 0), HedgeOutcome::Faster);

        // M=3 keeps the same split: every other copy dead is a rescue, one
        // still running is a win on speed.
        assert_eq!(hedge_outcome(3, 2, 2), HedgeOutcome::Rescued);
        assert_eq!(hedge_outcome(3, 2, 1), HedgeOutcome::Faster);
        assert_eq!(hedge_outcome(3, 0, 2), HedgeOutcome::Redundant);

        // M=0 is not reachable through config, which clamps with `max(1)`,
        // but the function must not take the floor out from under itself.
        assert_eq!(hedge_outcome(0, 0, 0), HedgeOutcome::NoHedge);
    }

    /// The copy that wins is not always the one fired first, and the sibling
    /// it beats used to vanish: dropped mid-await, its own code never ran
    /// again, so nothing counted it. Those two numbers together are the whole
    /// case for M, and neither was on `/metrics` before.
    #[tokio::test]
    async fn a_hedge_copy_winning_is_counted_and_its_sibling_is_superseded() {
        let server = MockServer::start().await;
        // Good for one request only, so the copy fired first takes it and is
        // still waiting on it when its sibling answers.
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(chat_reply("slow"))
                    .set_delay(Duration::from_secs(5)),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("quick")))
            .mount(&server)
            .await;
        let uri = server.uri();

        // 100ms apart, so which copy reaches the upstream first is not a race.
        let harness = TrialHarness::build(2, 100, &uri, Some(server));
        let outcome = harness.trial(CancellationToken::new()).await;
        assert!(
            matches!(outcome, TrialOutcome::Won(_)),
            "the hedge answered"
        );

        let render = harness.metrics.render();
        assert_eq!(
            sample(&render, "router_hedge_wins_total", &[r#"copy="1""#]),
            1.0,
            "the second copy fired is the one that won"
        );
        assert_eq!(
            sample(&render, "router_hedge_wins_total", &[r#"copy="0""#]),
            0.0
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="success""#]),
            1.0
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="superseded""#]),
            1.0,
            "the copy its sibling beat is counted, not dropped in silence"
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="cancelled""#]),
            0.0,
            "a sibling winning is not another channel winning"
        );
        assert_eq!(
            sample(&render, "router_trials_total", &[r#"outcome="won""#]),
            1.0
        );
        // Two copies, two latency observations: a copy that is never counted
        // is a copy whose time nobody can see either.
        assert_eq!(
            sample(
                &render,
                "router_copy_latency_seconds_count",
                &[r#"outcome="success""#]
            ) + sample(
                &render,
                "router_copy_latency_seconds_count",
                &[r#"outcome="superseded""#]
            ),
            2.0
        );
    }

    /// Every copy failing is M copy records and one trial, which is the pair
    /// the live run could not tell apart: 150 `http_error` records against 92
    /// trials, and no way to say which number counted what.
    #[tokio::test]
    async fn every_copy_failing_is_counted_once_each_and_the_trial_once() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream is down"))
            .mount(&server)
            .await;
        let uri = server.uri();

        let harness = TrialHarness::build(2, 0, &uri, Some(server));
        let outcome = harness.trial(CancellationToken::new()).await;
        assert!(matches!(outcome, TrialOutcome::Failed { .. }));

        let render = harness.metrics.render();
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="http_error""#]),
            2.0
        );
        assert_eq!(
            sample(&render, "router_trials_total", &[r#"outcome="failed""#]),
            1.0,
            "M copies are one trial, not M"
        );
        assert_eq!(
            sample(&render, "router_errors_total", &[r#"class="server_error""#]),
            2.0,
            "each failed copy says why it failed"
        );
        assert_eq!(
            sample(
                &render,
                "router_copy_latency_seconds_count",
                &[r#"outcome="http_error""#]
            ),
            2.0
        );
    }

    /// Another channel winning the race is not a sibling copy winning, and an
    /// operator reading `superseded` where the truth is `cancelled` would
    /// take a working hedge for a wasted one.
    #[tokio::test]
    async fn a_trial_another_channel_beat_counts_its_copies_as_cancelled() {
        // Port 1 is never listening, but no call is made: the token has
        // already fired, so the copies are torn down before they are polled.
        let harness = TrialHarness::build(2, 0, "http://127.0.0.1:1", None);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = harness.trial(cancel).await;
        assert!(matches!(outcome, TrialOutcome::Cancelled));

        let render = harness.metrics.render();
        assert_eq!(
            sample(&render, "router_trials_total", &[r#"outcome="cancelled""#]),
            1.0
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="cancelled""#]),
            2.0
        );
        assert_eq!(
            sample(&render, "router_copies_total", &[r#"outcome="superseded""#]),
            0.0
        );
        assert_eq!(
            sample(&render, "router_trials_total", &[r#"outcome="won""#]),
            0.0
        );
    }

    /// The classes an operator acts on. A dead key, a provider refusing us and
    /// our own M refusing us are three different jobs, and before this they
    /// were one number.
    #[test]
    fn an_http_status_maps_to_the_class_that_says_what_to_do_about_it() {
        assert_eq!(
            ErrorClass::from_status(401, false),
            ErrorClass::Unauthorized
        );
        assert_eq!(ErrorClass::from_status(403, false), ErrorClass::Forbidden);
        assert_eq!(ErrorClass::from_status(404, false), ErrorClass::NotFound);
        assert_eq!(ErrorClass::from_status(429, false), ErrorClass::RateLimited);
        assert_eq!(
            ErrorClass::from_status(429, true),
            ErrorClass::SelfInflicted429
        );
        assert_eq!(
            ErrorClass::from_status(402, false),
            ErrorClass::PaymentRequired
        );
        assert_eq!(
            ErrorClass::from_status(408, false),
            ErrorClass::RequestTimeout
        );
        assert_eq!(
            ErrorClass::from_status(400, false),
            ErrorClass::OtherClientError
        );
        use crate::config::FailureGroup;
        assert_eq!(
            ErrorClass::PaymentRequired.group(),
            Some(FailureGroup::Account)
        );
        assert_eq!(
            ErrorClass::RequestTimeout.group(),
            Some(FailureGroup::Unreachable)
        );
        assert_eq!(ErrorClass::OtherClientError.group(), None);
        assert_eq!(ErrorClass::from_status(503, false), ErrorClass::ServerError);
        // A 2xx reaches a class only when its body was not an answer.
        assert_eq!(ErrorClass::from_status(200, false), ErrorClass::Other);
    }

    /// The copy counter is the coarse view, so several classes share one
    /// value there and stay apart on `router_errors_total`.
    #[test]
    fn a_failure_class_lands_on_the_copy_counter_it_belongs_to() {
        assert_eq!(copy_outcome_for(ErrorClass::Timeout), CopyOutcome::Timeout);
        assert_eq!(
            copy_outcome_for(ErrorClass::SelfInflicted429),
            CopyOutcome::SelfInflicted429
        );
        assert_eq!(
            copy_outcome_for(ErrorClass::BodyUnparseable),
            CopyOutcome::BodyError
        );
        assert_eq!(
            copy_outcome_for(ErrorClass::BodyUndecodable),
            CopyOutcome::BodyError
        );
        assert_eq!(
            copy_outcome_for(ErrorClass::Unauthorized),
            CopyOutcome::HttpError
        );
        assert_eq!(
            copy_outcome_for(ErrorClass::Connect),
            CopyOutcome::HttpError
        );
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
            created: None,
            health: Default::default(),
            telemetry: Default::default(),
        }
    }
}
