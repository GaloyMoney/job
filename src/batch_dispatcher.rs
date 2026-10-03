//! Executes one batch of claimed jobs and commits all of their bookkeeping
//! together.
//!
//! The per-job [`JobDispatcher`](crate::dispatcher::JobDispatcher) writes one
//! commit per job. This dispatcher writes **one commit per batch**: the
//! execution rows, the entity events and the notifications for every job in the
//! batch land in a single transaction — the runner's own, when it returns one.

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use es_entity::errlanes;
use es_entity::errlanes::{Fatal, FatalKind, Fault, Laned, Transient, TransientKind};
use futures::FutureExt;
use tracing::Span;

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Weak};

use super::{
    JobId,
    batched::{
        AnyBatchedJobRunner, BatchItemOutcome, BatchOutcomes, BatchRunCtx, JobBatchCompletion,
        RawBatchItem, ShutdownRx,
    },
    entity::JobType,
    error::JobFault,
    finalizer::{ClaimDisposition, Disposition, FinalizeOutcome, Finalizer, RunFailure},
    notifier::JobEventNotifier,
    poller::JobPoller,
    repo::JobRepo,
    runner::RetrySettings,
    tracker::{JobTracker, UnitReservation},
};

/// Renders a batch's ids as a comma-separated list for one log field.
///
/// The `batch dispatcher error` log carries these alongside the error: an
/// error line that names no jobs cannot be tied to the work it stranded,
/// which is what makes a burst of lost jobs undiagnosable.
struct DisplayIds<'a>(&'a [JobId]);

impl std::fmt::Display for DisplayIds<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{id}")?;
        }
        Ok(())
    }
}

pub(crate) struct BatchDispatcher {
    /// Reaches this process's poller for the head-swap completion-recycle
    /// claim (`try_recycle_own_type`). `Weak` so a dispatcher never keeps the
    /// poller alive on its own -- mirrors `JobDispatcher`'s identical field.
    poller: Weak<JobPoller>,
    repo: Arc<JobRepo>,
    runner: Option<Box<dyn AnyBatchedJobRunner>>,
    tracker: Arc<JobTracker>,
    job_type: JobType,
    ids: Vec<JobId>,
    attempts: HashMap<JobId, u32>,
    /// How long `run_batch` actually ran, on a monotonic clock -- the retry
    /// policy's evidence of recovery, set once the batch returns and read by
    /// every [`Disposition::Fail`] this batch produces. Every item in a batch
    /// shares it, which is correct: they were all up for exactly that long.
    run_duration: std::time::Duration,
    rescheduled: bool,
    dispatched: bool,
    /// Whether this batch's unit has already been handed to a recycle
    /// reservation. See [`Self::try_recycle_own_type`] -- it must happen at
    /// most once, including across a retried seal.
    recycled: bool,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
    finalizer: Finalizer,
}

impl BatchDispatcher {
    /// Claims the batch's slot **synchronously**, at construction.
    ///
    /// The poller builds this before spawning the execution task, so the very
    /// next poll already sees the slot occupied. Doing it inside the task
    /// instead leaves a window in which the poll loop re-polls, still reads
    /// zero batches in flight, and claims a second round of rows — which is
    /// exactly the over-claiming the slot budget exists to prevent. The slot is
    /// released by `Drop`, so it cannot leak once taken.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        tracker: Arc<JobTracker>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        job_type: JobType,
        runner: Box<dyn AnyBatchedJobRunner>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
        items: &[RawBatchItem],
    ) -> Self {
        let ids: Vec<JobId> = items.iter().map(|item| item.job.id).collect();
        let attempts = items
            .iter()
            .map(|item| (item.job.id, item.attempt))
            .collect();
        tracker.dispatch_batch(&job_type, &ids);
        let finalizer = Finalizer::new(
            poller.clone(),
            repo.clone(),
            notifier,
            retry_settings,
            false, // batched types are never keyed, so never retain state
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            runner: Some(runner),
            tracker,
            job_type,
            ids,
            attempts,
            run_duration: std::time::Duration::ZERO,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            instance_id,
            clock,
            finalizer,
        }
    }

    /// Build from an already-taken [`UnitReservation`] (the head-swap
    /// short-circuit fast path): the reservation already accounted for this
    /// unit, so this consumes it via [`UnitReservation::into_live_batch`]
    /// instead of calling `tracker.dispatch_batch` a second time. Mirrors
    /// `JobDispatcher::from_reservation`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_reservation(
        reservation: UnitReservation,
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        tracker: Arc<JobTracker>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        job_type: JobType,
        runner: Box<dyn AnyBatchedJobRunner>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
        items: &[RawBatchItem],
    ) -> Self {
        let ids: Vec<JobId> = items.iter().map(|item| item.job.id).collect();
        let attempts = items
            .iter()
            .map(|item| (item.job.id, item.attempt))
            .collect();
        reservation.into_live_batch(&ids);
        let finalizer = Finalizer::new(
            poller.clone(),
            repo.clone(),
            notifier,
            retry_settings,
            false, // batched types are never keyed, so never retain state
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            runner: Some(runner),
            tracker,
            job_type,
            ids,
            attempts,
            run_duration: std::time::Duration::ZERO,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            instance_id,
            clock,
            finalizer,
        }
    }

    /// This batch's job type, for the shutdown-coordination spawn wrapper
    /// (`JobPoller::spawn_batch_dispatch_task`).
    pub(crate) fn job_type(&self) -> &JobType {
        &self.job_type
    }

    /// Detach this batch's unit from the ordinary Drop-triggered release:
    /// [`Self::try_recycle_own_type`] found due work of the same type to
    /// [`JobTracker::recycle`] the about-to-be-freed unit into instead.
    fn recycle_unit(&mut self) {
        self.dispatched = false;
        self.tracker.mark_finished_without_releasing_unit(&self.ids);
    }

    /// This batch's unit of `job_type`'s capacity is about to free (`Drop`
    /// fires `batch_completed` below unless this recycles it first). Hands
    /// it, unconditionally, to a `ClaimHook` that will try to spend it on
    /// this SAME type's own oldest due backlog at commit time -- if nothing
    /// turns out to be due (or shutdown is underway, or the type opted out),
    /// the reservation the hook holds simply releases, identical to not
    /// recycling at all. Called exactly ONCE per `execute_batch` -- from the
    /// end of `seal` for a disposed batch, from `fail_batch` for a
    /// whole-batch error -- never once per disposition kind: a batch is
    /// always exactly one execution unit (`JobTracker::dispatch_batch`), so
    /// recycling per completed AND per terminally-failed item would try to
    /// spend the SAME freed unit twice whenever one batch disposes items
    /// both ways.
    fn try_recycle_own_type(&mut self, op: &mut (impl AtomicOperation + ?Sized)) {
        // "Exactly once" has to hold across the conflict retries in
        // `seal_in_own_op`/`fail_batch` too, not just across sub-outcome
        // branches. A rolled-back attempt drops its `UnitReservation`, and
        // dropping one *releases* the unit -- so handing out a second
        // reservation for the same unit would release it twice and hand the
        // tracker capacity it does not have. A retried seal therefore
        // forfeits the recycle optimisation, which costs at most one
        // extra poll: `recycle_unit` has already cleared `dispatched`, so
        // `Drop` correctly declines to release the unit a second time.
        if self.recycled {
            return;
        }
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        self.recycled = true;
        self.recycle_unit();
        let reservation = self.tracker.recycle(&self.job_type);
        poller.register_claim_recycle(op, &self.job_type, reservation);
    }

    #[errlanes::instrument(
        name = "job.execute_batch",
        skip_all,
        fields(job_type, n_items, poller_id, conclusion, now, claim_disposition)
    )]
    #[cfg_attr(feature = "es-entity", es_entity::es_event_context)]
    pub async fn execute_batch(
        mut self,
        items: Vec<RawBatchItem>,
        shutdown_rx: ShutdownRx,
    ) -> Result<(), JobFault> {
        let span = Span::current();
        span.record("job_type", tracing::field::display(&self.job_type));
        span.record("n_items", items.len());
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("now", tracing::field::display(self.clock.now()));

        // A batch of one keeps the per-job trace lineage of the non-batched
        // path. With more than one there is no single parent to adopt, so the
        // batch span stands on its own and items are identified by id.
        if let [only] = items.as_slice() {
            only.job.inject_tracing_parent();
        }

        #[cfg(feature = "es-entity")]
        {
            let mut ctx = es_entity::EventContext::current();
            ctx.insert(
                "job",
                &serde_json::json!({
                    "job_type": self.job_type,
                    "n_items": self.ids.len(),
                    "poller_id": self.instance_id
                }),
            )
            .expect("EventContext insert batch data");
        }

        let ctx = BatchRunCtx {
            pool: self.repo.pool().clone(),
            clock: self.clock.clone(),
            repo: Arc::clone(&self.repo),
            job_type: self.job_type.clone(),
            shutdown_rx,
        };

        let runner = self.runner.take().expect("runner");
        // Monotonic, deliberately not `self.clock` -- see `Self::run_duration`.
        let started = std::time::Instant::now();
        let batch_result = Self::run_batch(runner, items, ctx).await;
        self.run_duration = started.elapsed();
        let outcome = match batch_result {
            Ok(completion) => self.apply(completion).await,
            Err(e) => {
                // Decided here, not inside `fail_batch`: `fail_batch` runs
                // in its own child span (`job.fail_batch`), so it cannot
                // record onto this "job.execute_batch" span's `conclusion`
                // field.
                span.record(
                    "conclusion",
                    if e.is_congestion() {
                        "Congestion"
                    } else {
                        "Error"
                    },
                );
                self.fail_batch(e).await
            }
        };

        // Every failure funnels here, and every one of them leaves this
        // batch's rows still `running` under this instance -- the seal is
        // atomic, so an error means nothing was dispositioned. Before
        // sb-max10 the error was logged and that was the end of it: `Drop`
        // had already dropped the ids from the live tracker, so the
        // keep-alive stopped refreshing their `alive_at`, and the rows sat
        // frozen until the lost-handler noticed them a full
        // `job_lost_interval` later. In production that stalled five loan
        // approvals for 7m40s behind a single deadlock.
        //
        // Hand the rows back instead, so the lost-handler is a true backstop
        // rather than the primary recovery path.
        if let Err(e) = outcome {
            let disposition = self.rescue_claimed_rows().await;
            span.record("claim_disposition", tracing::field::display(disposition));
            // Emitted here rather than in the poller's spawn wrapper: only
            // this scope knows WHICH jobs were affected and what became of
            // them, and an error line that names no jobs cannot be tied to
            // the work it stranded.
            tracing::error!(
                job_type = %self.job_type,
                job_ids = %DisplayIds(&self.ids),
                n_items = self.ids.len(),
                claim_disposition = %disposition,
                exception.message = %e,
                exception.type = std::any::type_name_of_val(&e),
                "batch dispatcher error"
            );
            return Err(e);
        }
        Ok(())
    }

    /// Last-resort release of rows this batch still holds, in a fresh
    /// transaction (the failed one is poisoned).
    ///
    /// Deliberately [`Disposition::Fresh`] rather than [`Disposition::Fail`]:
    /// a deadlock victim or a dead connection says nothing about the job's
    /// own correctness, so it must not burn a retry attempt or push the job
    /// terminal. The rows go back to `pending` with `execute_at = now` and
    /// `attempt_index = 1`, which is what the next poll re-dispatches.
    ///
    /// Runs through [`Finalizer::finalize`]'s pool policy, same as
    /// `fail_batch`: this is the LAST chance to record a disposition before
    /// the lost-handler is the only thing left, so it cannot afford to
    /// compete with the shared pool for a connection -- especially since
    /// this rescue is often reached BECAUSE the shared pool was the thing
    /// under pressure. Before that pool existed for this purpose, both this
    /// write and `fail_batch`'s acquired from the shared pool, so sustained
    /// exhaustion could fail both and strand the row exactly as
    /// `handoff-pool-aware-claiming-and-fail-path.md` §1 describes.
    ///
    /// Best-effort by construction: if this fails too there is nothing left
    /// to try, and the lost-handler remains as the final backstop -- but the
    /// return value makes that outcome visible instead of silent.
    async fn rescue_claimed_rows(&mut self) -> ClaimDisposition {
        if self.ids.is_empty() {
            return ClaimDisposition::AlreadyDisposed;
        }
        let now = self.clock.now();
        let items: Vec<(JobId, Disposition)> = self
            .ids
            .iter()
            .map(|id| (*id, Disposition::Fresh { at: now }))
            .collect();

        match self.finalizer.finalize(&items, |_, _| {}).await {
            // The row writes filter on `poller_instance_id`, so a batch
            // whose rows were already dispositioned matches nothing and
            // still lands here -- harmless either way.
            Ok(_) => {
                self.rescheduled = true;
                ClaimDisposition::Rescheduled
            }
            Err(e) => {
                tracing::error!(
                    job_type = %self.job_type,
                    job_ids = %DisplayIds(&self.ids),
                    exception.message = %e,
                    exception.type = std::any::type_name_of_val(&e),
                    "could not release claimed rows; they stay running until \
                     the lost-handler reclaims them"
                );
                ClaimDisposition::Leaked
            }
        }
    }

    async fn run_batch(
        runner: Box<dyn AnyBatchedJobRunner>,
        items: Vec<RawBatchItem>,
        ctx: BatchRunCtx,
    ) -> Result<JobBatchCompletion, RunFailure> {
        match AssertUnwindSafe(runner.run_batch_erased(items, ctx))
            .catch_unwind()
            .await
        {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(e)) => {
                // Classified here, while the box is still borrowable and
                // before the next `.await`: see `RunFailure`.
                let failure: RunFailure = Fault::classify(&*e).narrow_denied();
                failure.record(&Span::current());
                Err(failure)
            }
            Err(panic) => {
                let message = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic payload".to_string()
                };

                tracing::error!(
                    target: "job.panic",
                    panic_message = %message,
                    panic_backtrace = ?std::backtrace::Backtrace::capture(),
                    "Batched job panicked during execution"
                );

                let failure: RunFailure = Fatal::new(FatalKind::Panic).with_context(message).into();
                failure.record(&Span::current());
                Err(failure)
            }
        }
    }

    async fn apply(&mut self, completion: JobBatchCompletion) -> Result<(), JobFault> {
        let span = Span::current();
        match completion {
            JobBatchCompletion::CompleteAll => {
                span.record("conclusion", "CompleteAll");
                let outcomes = self.all_complete();
                self.seal_in_own_op(outcomes).await?;
            }
            JobBatchCompletion::WithOutcomes(outcomes) => {
                span.record("conclusion", "WithOutcomes");
                if let Err(e) = self.validate(&outcomes) {
                    return self.fail_batch(e).await;
                }
                self.seal_in_own_op(outcomes).await?;
            }
            #[cfg(feature = "es-entity")]
            JobBatchCompletion::CompleteAllWithOp(mut op) => {
                span.record("conclusion", "CompleteAllWithOp");
                let outcomes = self.all_complete();
                self.seal(&mut op, outcomes).await?;
                op.commit().await?;
            }
            #[cfg(feature = "es-entity")]
            JobBatchCompletion::WithOutcomesWithOp(op, outcomes) => {
                span.record("conclusion", "WithOutcomesWithOp");
                if let Err(e) = self.validate(&outcomes) {
                    // Dropping the operation rolls the runner's work back: the
                    // batch is retried rather than half-applied.
                    drop(op);
                    return self.fail_batch(e).await;
                }
                let mut op = op;
                self.seal(&mut op, outcomes).await?;
                op.commit().await?;
            }
            JobBatchCompletion::CompleteAllWithTx(mut tx) => {
                span.record("conclusion", "CompleteAllWithTx");
                let outcomes = self.all_complete();
                self.seal(&mut tx, outcomes).await?;
                tx.commit().await?;
            }
            JobBatchCompletion::WithOutcomesWithTx(tx, outcomes) => {
                span.record("conclusion", "WithOutcomesWithTx");
                if let Err(e) = self.validate(&outcomes) {
                    drop(tx);
                    return self.fail_batch(e).await;
                }
                let mut tx = tx;
                self.seal(&mut tx, outcomes).await?;
                tx.commit().await?;
            }
        }
        Ok(())
    }

    fn all_complete(&self) -> BatchOutcomes {
        self.ids
            .iter()
            .map(|id| (*id, BatchItemOutcome::Complete))
            .collect()
    }

    /// Every job in the batch must be dispositioned exactly once. A runner that
    /// breaks this contract has an unclear intent for the jobs it left out, so
    /// nothing is guessed: the batch is rolled back and retried. A mismatch is
    /// a bug in the runner, not evidence about any particular job, so it is
    /// always `Fatal` -- the dispatcher's own contract check states the lane
    /// itself rather than inferring one from whatever the runner returned.
    /// Whether a `Fatal` ends the job is then the type's
    /// `RetrySettings::terminal_on_fatal` decision, like any other fatal.
    fn validate(&self, outcomes: &BatchOutcomes) -> Result<(), RunFailure> {
        let expected: HashSet<JobId> = self.ids.iter().copied().collect();
        let mut seen: HashSet<JobId> = HashSet::with_capacity(outcomes.len());
        for (id, _) in outcomes {
            if !expected.contains(id) {
                return Err(Fatal::invariant(format!(
                    "outcome returned for job {id}, which is not part of the batch"
                ))
                .into());
            }
            if !seen.insert(*id) {
                return Err(
                    Fatal::invariant(format!("duplicate outcome returned for job {id}")).into(),
                );
            }
        }
        if seen.len() != expected.len() {
            let mut missing: Vec<String> = expected
                .difference(&seen)
                .map(|id| id.to_string())
                .collect();
            missing.sort();
            return Err(Fatal::invariant(format!(
                "no outcome returned for job(s): {}",
                missing.join(", ")
            ))
            .into());
        }
        Ok(())
    }

    /// Seal into a transaction this dispatcher owns, via
    /// [`Finalizer::finalize`]'s pool-choice and abort-retry policy.
    ///
    /// Retrying is only sound *because* the transaction is the crate's own:
    /// it holds nothing but this batch's bookkeeping, an abort rolled all of
    /// it back, and the dispositions are plain data that re-apply
    /// identically. The `*WithOp`/`*WithTx` completions cannot use this --
    /// the runner's own writes live in that same transaction, so an abort
    /// destroys work this dispatcher cannot recreate. Those fall through to
    /// the rescue in `execute_batch`, which hands the rows back so the
    /// runner runs again.
    async fn seal_in_own_op(&mut self, outcomes: BatchOutcomes) -> Result<(), JobFault> {
        let items = self.disposition_items(outcomes, self.clock.now());
        let finalizer = self.finalizer.clone();
        let outcome = finalizer
            .finalize(&items, |op, _| self.try_recycle_own_type(op))
            .await?;
        self.record_seal_outcome(&outcome);
        Ok(())
    }

    /// Seal onto the caller's op (the runner's own transaction, for the
    /// `*WithOp`/`*WithTx` completions): one [`Finalizer::finalize_in_op`]
    /// pass over every disposition, then the completion-recycle.
    async fn seal(
        &mut self,
        op: &mut (impl AtomicOperation + ?Sized),
        outcomes: BatchOutcomes,
    ) -> Result<(), JobFault> {
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let items = self.disposition_items(outcomes, now);
        let outcome = self.finalizer.finalize_in_op(op, &items).await?;
        self.record_seal_outcome(&outcome);
        self.try_recycle_own_type(op);
        Ok(())
    }

    /// Translate a runner's [`BatchOutcomes`] into finalizer
    /// [`Disposition`]s -- per-item attempt numbers come from
    /// `self.attempts`, and `RescheduleIn` resolves against the caller's
    /// `now` (the sealing transaction's time where one exists).
    fn disposition_items(
        &self,
        outcomes: BatchOutcomes,
        now: DateTime<Utc>,
    ) -> Vec<(JobId, Disposition)> {
        outcomes
            .into_iter()
            .map(|(id, outcome)| {
                let disposition = match outcome {
                    BatchItemOutcome::Complete => Disposition::Complete,
                    BatchItemOutcome::RescheduleIn(d) => Disposition::Fresh { at: now + d },
                    BatchItemOutcome::RescheduleAt(t) => Disposition::Fresh { at: t },
                    // A per-item `BatchItemOutcome::Fail` is a plain string
                    // the runner chose to report -- no lane payload travels
                    // with it, so it is laned as `Transient(Other)` so it can
                    // never be terminal on its own regardless of
                    // `terminal_on_fatal`; it goes through the ordinary
                    // attempt-count retry path.
                    BatchItemOutcome::Fail(reason) => Disposition::Fail {
                        failure: Transient::new(TransientKind::Other)
                            .with_context(reason)
                            .into(),
                        attempt: self.attempts.get(&id).copied().unwrap_or(1),
                        run_duration: self.run_duration,
                    },
                };
                (id, disposition)
            })
            .collect()
    }

    /// Span fields + `rescheduled` flag from what the finalizer actually
    /// did.
    fn record_seal_outcome(&mut self, outcome: &FinalizeOutcome) {
        let span = Span::current();
        span.record("n_retried", outcome.retried.len());
        span.record("n_errored", outcome.errored_terminal.len());
        self.rescheduled |= outcome.any_rescheduled();
    }

    /// Fail every job in the batch with the same error — the batch's work was
    /// rolled back, so no job in it can be considered done. One
    /// [`Disposition::Fail`] per id through [`Finalizer::finalize`]: the
    /// retry policy is applied to each job independently — some may be
    /// rescheduled for another attempt while others exhaust their attempts
    /// and become terminal, all in the same transaction.
    ///
    /// A `failure.is_congestion()` error is routed to the finalizer's
    /// congestion reschedule instead of the retry-policy path: congestion carries no
    /// evidence any of these jobs is broken, so applying `RetryPolicy`'s
    /// attempt escalation to it would walk perfectly good jobs toward
    /// `max_attempts` termination for a condition they didn't cause. The
    /// congestion branch also deliberately skips
    /// [`Self::try_recycle_own_type`], unlike the ordinary-error branch:
    /// right after evidence the pool is unhealthy, an immediate same-type
    /// re-claim -- even through `ClaimHook`'s budget gate -- would bypass
    /// the congestion delay's cool-off, so the unit releases through the
    /// ORDINARY path (`Drop`'s `batch_completed`) and the backlog is picked
    /// back up by the next pool-aware poll instead.
    #[errlanes::instrument(name = "job.fail_batch", skip_all,
        fields(job_type = %self.job_type, n_items = self.ids.len(),
               n_retried = tracing::field::Empty, n_errored = tracing::field::Empty)
    )]
    async fn fail_batch(&mut self, failure: RunFailure) -> Result<(), JobFault> {
        let span = tracing::Span::current();
        failure.record(&span);
        if failure.is_congestion() {
            self.rescheduled = true;
            let message = failure.message();
            return self
                .finalizer
                .reschedule_congested(&self.ids, &self.attempts, message)
                .await;
        }
        let items: Vec<(JobId, Disposition)> = self
            .ids
            .iter()
            .map(|id| {
                (
                    *id,
                    Disposition::Fail {
                        failure: failure.clone(),
                        attempt: self.attempts.get(id).copied().unwrap_or(1),
                        run_duration: self.run_duration,
                    },
                )
            })
            .collect();
        let finalizer = self.finalizer.clone();
        let outcome = finalizer
            .finalize(&items, |op, _| self.try_recycle_own_type(op))
            .await?;
        span.record(
            "error.level",
            tracing::field::display(if outcome.retried.is_empty() {
                tracing::Level::ERROR
            } else {
                tracing::Level::WARN
            }),
        );
        self.record_seal_outcome(&outcome);
        Ok(())
    }
}

impl Drop for BatchDispatcher {
    fn drop(&mut self) {
        if self.dispatched {
            self.tracker
                .batch_completed(&self.job_type, &self.ids, self.rescheduled);
        }
    }
}
