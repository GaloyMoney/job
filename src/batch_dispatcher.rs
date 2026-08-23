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
use futures::FutureExt;
use tracing::{Span, instrument};

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Weak};

use super::{
    JobId,
    batched::{
        AnyBatchedJobRunner, BatchItemOutcome, BatchOutcomes, BatchRunCtx, JobBatchCompletion,
        RawBatchItem, ShutdownRx,
    },
    congestion::CongestionHandler,
    entity::{Job, JobType, RetryPolicy},
    error::{JobError, TX_ABORT_MAX_ATTEMPTS, is_retryable_conflict},
    execution_hooks::PromoteHeadsHook,
    notifier::JobEventNotifier,
    poller::JobPoller,
    repo::JobRepo,
    runner::RetrySettings,
    tracker::{JobTracker, UnitReservation},
};

/// What happened to a batch's claimed rows after the dispatcher failed
/// terminally. Reported on the `batch dispatcher error` log so an operator
/// can tell a self-healing blip from a genuine stall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimDisposition {
    /// Rows were handed back as `pending` with `execute_at = now`; the next
    /// poll re-dispatches them.
    Rescheduled,
    /// The batch had already dispositioned its rows (they are terminal or
    /// pending by the seal's own doing) -- nothing was left claimed.
    AlreadyDisposed,
    /// The rescue itself failed. Rows stay `running` under this instance and
    /// only the lost-handler will recover them, one `job_lost_interval`
    /// later. This is the case that used to be silent.
    Leaked,
}

impl std::fmt::Display for ClaimDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ClaimDisposition::Rescheduled => "rescheduled",
            ClaimDisposition::AlreadyDisposed => "already-disposed",
            ClaimDisposition::Leaked => "leaked",
        };
        f.write_str(s)
    }
}

/// Renders a batch's ids as a comma-separated list for one log field.
///
/// The `batch dispatcher error` log used to carry only the error, which is
/// why two production runs of lost-job bursts went undiagnosed: there was no
/// way to tie an error line to the jobs it stranded.
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
    retry_settings: RetrySettings,
    runner: Option<Box<dyn AnyBatchedJobRunner>>,
    tracker: Arc<JobTracker>,
    notifier: Arc<JobEventNotifier>,
    job_type: JobType,
    ids: Vec<JobId>,
    attempts: HashMap<JobId, u32>,
    rescheduled: bool,
    dispatched: bool,
    /// Whether this batch's unit has already been handed to a recycle
    /// reservation. See [`Self::try_recycle_own_type`] -- it must happen at
    /// most once, including across a retried seal.
    recycled: bool,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
    congestion: CongestionHandler,
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
        let congestion = CongestionHandler::new(
            poller.clone(),
            repo.clone(),
            notifier.clone(),
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            notifier,
            job_type,
            ids,
            attempts,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            instance_id,
            clock,
            congestion,
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
        let congestion = CongestionHandler::new(
            poller.clone(),
            repo.clone(),
            notifier.clone(),
            instance_id,
            clock.clone(),
        );
        Self {
            poller,
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            notifier,
            job_type,
            ids,
            attempts,
            rescheduled: false,
            dispatched: true,
            recycled: false,
            instance_id,
            clock,
            congestion,
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
    /// whole-batch error -- never once per sub-outcome branch: a batch is
    /// always exactly one execution unit (`JobTracker::dispatch_batch`), so
    /// recycling it from inside `complete_in_op` AND the terminal branch of
    /// `fail_in_op` would try to spend the SAME freed unit twice whenever
    /// one batch disposes items through both branches.
    fn try_recycle_own_type(&mut self, op: &mut impl AtomicOperation) {
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

    #[instrument(name = "job.execute_batch", skip_all,
        fields(job_type, n_items, poller_id, error, error.level, error.message, conclusion, now,
               claim_disposition)
    )]
    #[cfg_attr(feature = "es-entity", es_entity::es_event_context)]
    pub async fn execute_batch(
        mut self,
        items: Vec<RawBatchItem>,
        shutdown_rx: ShutdownRx,
    ) -> Result<(), JobError> {
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
            shutdown_rx,
        };

        let runner = self.runner.take().expect("runner");
        let outcome = match Self::run_batch(&self.congestion, runner, items, ctx).await {
            Ok(completion) => self.apply(completion).await,
            Err(e) => {
                // Decided here, not inside `fail_batch`: `fail_batch` runs
                // in its own child span (`job.fail_batch`), so it cannot
                // record onto this "job.execute_batch" span's `conclusion`
                // field.
                span.record(
                    "conclusion",
                    if matches!(e, JobError::PoolCongestion(_)) {
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
            // Emitted here rather than in the poller's spawn wrapper, which
            // is where it used to live: only this scope knows WHICH jobs were
            // affected and what became of them. The old log carried the error
            // alone, which is why two production runs of lost-job bursts went
            // undiagnosed -- nothing tied an error line to the jobs it
            // stranded.
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
    /// Deliberately `reschedule_in_op` rather than `fail_in_op`: a deadlock
    /// victim or a dead connection says nothing about the job's own
    /// correctness, so it must not burn a retry attempt or push the job
    /// terminal. The rows go back to `pending` with `execute_at = now` and
    /// `attempt_index = 1`, which is what the next poll re-dispatches.
    ///
    /// Opens its op via `CongestionHandler::begin_terminal_write_op` (the
    /// internal pool), same as
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
        let reschedules: Vec<(JobId, DateTime<Utc>)> =
            self.ids.iter().map(|id| (*id, now)).collect();

        let attempt = async {
            let mut op = self.congestion.begin_terminal_write_op().await?;
            self.reschedule_in_op(&mut op, reschedules).await?;
            op.commit().await?;
            Ok::<_, JobError>(())
        }
        .await;

        match attempt {
            // `reschedule_in_op` filters on `poller_instance_id`, so a batch
            // whose rows were already dispositioned matches nothing and
            // still lands here -- harmless either way.
            Ok(()) => ClaimDisposition::Rescheduled,
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
        congestion: &CongestionHandler,
        runner: Box<dyn AnyBatchedJobRunner>,
        items: Vec<RawBatchItem>,
        ctx: BatchRunCtx,
    ) -> Result<JobBatchCompletion, JobError> {
        match AssertUnwindSafe(runner.run_batch_erased(items, ctx))
            .catch_unwind()
            .await
        {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(e)) => Err(congestion.maybe_reclassify(e)),
            Err(panic) => {
                let span = Span::current();
                let message = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic payload".to_string()
                };

                span.record("error", true);
                span.record(
                    "error.message",
                    tracing::field::display(&format!("Panic: {message}")),
                );
                span.record(
                    "error.level",
                    tracing::field::display(tracing::Level::ERROR),
                );

                tracing::error!(
                    target: "job.panic",
                    panic_message = %message,
                    panic_backtrace = ?std::backtrace::Backtrace::capture(),
                    "Batched job panicked during execution"
                );

                Err(JobError::JobExecutionError(format!(
                    "Job panicked: {message}"
                )))
            }
        }
    }

    async fn apply(&mut self, completion: JobBatchCompletion) -> Result<(), JobError> {
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
    /// nothing is guessed: the batch is rolled back and retried.
    fn validate(&self, outcomes: &BatchOutcomes) -> Result<(), JobError> {
        let expected: HashSet<JobId> = self.ids.iter().copied().collect();
        let mut seen: HashSet<JobId> = HashSet::with_capacity(outcomes.len());
        for (id, _) in outcomes {
            if !expected.contains(id) {
                return Err(JobError::BatchOutcomeMismatch(format!(
                    "outcome returned for job {id}, which is not part of the batch"
                )));
            }
            if !seen.insert(*id) {
                return Err(JobError::BatchOutcomeMismatch(format!(
                    "duplicate outcome returned for job {id}"
                )));
            }
        }
        if seen.len() != expected.len() {
            let mut missing: Vec<String> = expected
                .difference(&seen)
                .map(|id| id.to_string())
                .collect();
            missing.sort();
            return Err(JobError::BatchOutcomeMismatch(format!(
                "no outcome returned for job(s): {}",
                missing.join(", ")
            )));
        }
        Ok(())
    }

    /// Seal into a transaction this dispatcher owns, re-attempting the whole
    /// transaction when Postgres aborts it as a deadlock victim or
    /// serialization failure.
    ///
    /// Retrying is only sound *because* the transaction is the crate's own:
    /// it holds nothing but this batch's bookkeeping, the abort rolled all of
    /// it back, and `outcomes` is plain data that re-applies identically. The
    /// `*WithOp`/`*WithTx` completions cannot use this -- the runner's own
    /// writes live in that same transaction, so an abort destroys work this
    /// dispatcher cannot recreate. Those fall through to the rescue in
    /// `execute_batch`, which hands the rows back so the runner runs again.
    async fn seal_in_own_op(&mut self, outcomes: BatchOutcomes) -> Result<(), JobError> {
        let mut attempt = 1;
        loop {
            let result = async {
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                self.seal(&mut op, outcomes.clone()).await?;
                op.commit().await?;
                Ok::<_, JobError>(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) if attempt < TX_ABORT_MAX_ATTEMPTS && is_retryable_conflict(&e) => {
                    tracing::warn!(
                        job_type = %self.job_type,
                        job_ids = %DisplayIds(&self.ids),
                        attempt,
                        exception.message = %e,
                        "batch seal lost a lock conflict; retrying"
                    );
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn seal(
        &mut self,
        op: &mut impl AtomicOperation,
        outcomes: BatchOutcomes,
    ) -> Result<(), JobError> {
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());

        let mut completes = Vec::new();
        let mut reschedules = Vec::new();
        let mut fails = Vec::new();
        for (id, outcome) in outcomes {
            match outcome {
                BatchItemOutcome::Complete => completes.push(id),
                BatchItemOutcome::RescheduleIn(d) => reschedules.push((id, now + d)),
                BatchItemOutcome::RescheduleAt(t) => reschedules.push((id, t)),
                BatchItemOutcome::Fail(reason) => fails.push((id, reason)),
            }
        }

        self.complete_in_op(op, completes).await?;
        self.reschedule_in_op(op, reschedules).await?;
        self.fail_in_op(op, fails, now).await?;
        self.try_recycle_own_type(op);
        Ok(())
    }

    /// Deletes every completed row's execution (batched jobs are never
    /// keyed, so no retained-state guard is needed) and promotes each freed
    /// queue's oldest parked sibling via a [`PromoteHeadsHook`] freed-queue
    /// registration on this same transaction — deliberately NOT a CTE of the
    /// `DELETE`; see that hook for the snapshot-visibility reason —
    /// mirroring `dispatcher.rs`'s `delete_execution_in_op`. A batch can
    /// free several distinct queues in one commit, and each one's next job
    /// can be a different type than this batch's own; the hook wakes every
    /// promoted type itself.
    ///
    /// The `to_delete` CTE locks the rows in `(queue_id, id)` order BEFORE
    /// the `DELETE` touches them, for deadlock avoidance rather than for
    /// correctness: a spawn parking into several queues locks those queues'
    /// occupants in that same order (`execution_hooks::insert`'s
    /// `lock_queue_occupants`), as does the promote swap
    /// (`PromoteHeadsHook::apply`/`apply_freed`) and
    /// `ExecutionInsertHook::insert_many`'s `input` CTE, and this statement
    /// contends with them for exactly those rows. A bare `DELETE ... WHERE
    /// id = ANY(...)` would acquire in scan order -- unordered under a
    /// bitmap scan -- so a spawn and a batch completion touching the same
    /// two rows could each hold what the other wants. `MATERIALIZED` plus
    /// `ORDER BY` puts `LockRows` above `Sort`, which is what makes the
    /// order actually hold at execution time. Ordering by `queue_id` here
    /// even though this CTE is id-addressed (not queue-addressed, unlike
    /// the other lockers) is what makes the agreement crate-wide rather
    /// than local: a bare `id` order was internally consistent with itself
    /// but not with a `queue_id`-leading locker touching an overlapping row
    /// set.
    #[instrument(name = "job.batch_complete", skip_all, fields(n = ids.len()))]
    async fn complete_in_op(
        &mut self,
        op: &mut impl AtomicOperation,
        ids: Vec<JobId>,
    ) -> Result<(), JobError> {
        if ids.is_empty() {
            return Ok(());
        }
        let uuids: Vec<uuid::Uuid> = ids.iter().map(|id| uuid::Uuid::from(*id)).collect();
        let rows = sqlx::query!(
            r#"
            WITH to_delete AS MATERIALIZED (
                SELECT id FROM job_executions
                WHERE id = ANY($1) AND poller_instance_id = $2
                ORDER BY queue_id, id
                FOR UPDATE
            ), deleted AS (
                DELETE FROM job_executions je USING to_delete t WHERE je.id = t.id
                RETURNING je.id, je.queue_id
            ), cleanup AS (
                DELETE FROM job_execution_states s USING deleted d WHERE s.id = d.id
            )
            SELECT id AS "id!: JobId", queue_id AS "queue_id?"
            FROM deleted
            "#,
            &uuids,
            self.instance_id,
        )
        .fetch_all(op.as_executor())
        .await?;

        if rows.is_empty() {
            return Ok(());
        }
        let deleted: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let freed_queues: Vec<String> =
            rows.iter().filter_map(|row| row.queue_id.clone()).collect();
        PromoteHeadsHook::register_freed_queues(op, &self.notifier, freed_queues).await?;

        let mut entities = self.repo.find_all_in_op::<Job>(&mut *op, &deleted).await?;
        let mut jobs = Vec::with_capacity(deleted.len());
        for id in &deleted {
            if let Some(mut job) = entities.remove(id) {
                job.complete_job();
                jobs.push(job);
            }
        }
        self.repo.update_all_in_op(op, &mut jobs).await?;

        for id in &deleted {
            self.notifier.job_terminal_in_op(op, *id).await?;
        }
        Ok(())
    }

    #[instrument(name = "job.batch_reschedule", skip_all, fields(n = reschedules.len()))]
    async fn reschedule_in_op(
        &mut self,
        op: &mut impl AtomicOperation,
        reschedules: Vec<(JobId, DateTime<Utc>)>,
    ) -> Result<(), JobError> {
        if reschedules.is_empty() {
            return Ok(());
        }
        self.rescheduled = true;

        let uuids: Vec<uuid::Uuid> = reschedules
            .iter()
            .map(|(id, _)| uuid::Uuid::from(*id))
            .collect();
        let times: Vec<DateTime<Utc>> = reschedules.iter().map(|(_, at)| *at).collect();

        // `(queue_id, id)`-ordered lock before the write, same reason and
        // same `MATERIALIZED` shape as `complete_in_op` -- see its doc
        // comment. A bare `UPDATE ... FROM UNNEST(...)` acquires in join
        // order, which is not an order any other writer agrees with.
        sqlx::query!(
            r#"
            WITH to_reschedule AS MATERIALIZED (
                SELECT je.id, u.execute_at
                FROM job_executions je
                JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                  ON je.id = u.id
                WHERE je.poller_instance_id = $3
                ORDER BY je.queue_id, je.id
                FOR UPDATE
            )
            UPDATE job_executions AS je
            SET state = 'pending', execute_at = t.execute_at, attempt_index = 1,
                poller_instance_id = NULL
            FROM to_reschedule t
            WHERE je.id = t.id
            "#,
            &uuids,
            &times,
            self.instance_id,
        )
        .execute(op.as_executor())
        .await?;
        let ids: Vec<JobId> = reschedules.iter().map(|(id, _)| *id).collect();
        let mut entities = self.repo.find_all_in_op::<Job>(&mut *op, &ids).await?;
        let mut jobs = Vec::with_capacity(ids.len());
        for (id, at) in &reschedules {
            if let Some(mut job) = entities.remove(id) {
                job.reschedule_execution(*at);
                jobs.push(job);
            }
        }
        self.repo.update_all_in_op(op, &mut jobs).await?;
        // Invariant B: a swap only demotes the ONE row whose queue actually
        // had an older parked sibling, so any other row in `uuids` is still
        // `pending` and still needs its own type's wake.
        PromoteHeadsHook::register(op, &self.notifier, [self.job_type.clone()], uuids).await?;
        Ok(())
    }

    /// Apply the type's retry policy to each failed job independently: some may
    /// be rescheduled for another attempt while others exhaust their attempts
    /// and become terminal, all in the same transaction.
    #[instrument(name = "job.batch_fail", skip_all,
        fields(n = fails.len(), n_retried = tracing::field::Empty, n_errored = tracing::field::Empty)
    )]
    async fn fail_in_op(
        &mut self,
        op: &mut impl AtomicOperation,
        fails: Vec<(JobId, String)>,
        now: DateTime<Utc>,
    ) -> Result<(), JobError> {
        if fails.is_empty() {
            return Ok(());
        }
        let span = Span::current();
        let retry_policy = RetryPolicy::from(&self.retry_settings);

        let ids: Vec<JobId> = fails.iter().map(|(id, _)| *id).collect();
        let mut entities = self.repo.find_all_in_op::<Job>(&mut *op, &ids).await?;

        let mut retry_uuids = Vec::new();
        let mut retry_times = Vec::new();
        let mut retry_attempts = Vec::new();
        let mut terminal_uuids = Vec::new();
        let mut jobs = Vec::with_capacity(ids.len());

        for (id, reason) in fails {
            let Some(mut job) = entities.remove(&id) else {
                continue;
            };
            let attempt = self.attempts.get(&id).copied().unwrap_or(1);
            match job.maybe_schedule_retry(now, attempt, &retry_policy, reason) {
                Some((reschedule_at, next_attempt)) => {
                    retry_uuids.push(uuid::Uuid::from(id));
                    retry_times.push(reschedule_at);
                    retry_attempts.push(next_attempt as i32);
                    self.rescheduled = true;
                }
                None => terminal_uuids.push(uuid::Uuid::from(id)),
            }
            jobs.push(job);
        }

        span.record("n_retried", retry_uuids.len());
        span.record("n_errored", terminal_uuids.len());

        if !retry_uuids.is_empty() {
            // Same `(queue_id, id)`-ordered lock as the reschedule path
            // above, for the same deadlock-avoidance reason.
            sqlx::query!(
                r#"
                WITH to_retry AS MATERIALIZED (
                    SELECT je.id, u.execute_at, u.attempt_index
                    FROM job_executions je
                    JOIN UNNEST($1::uuid[], $2::timestamptz[], $3::int4[])
                        AS u(id, execute_at, attempt_index)
                      ON je.id = u.id
                    WHERE je.poller_instance_id = $4
                    ORDER BY je.queue_id, je.id
                    FOR UPDATE
                )
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = t.execute_at,
                    attempt_index = t.attempt_index, poller_instance_id = NULL
                FROM to_retry t
                WHERE je.id = t.id
                "#,
                &retry_uuids,
                &retry_times,
                &retry_attempts,
                self.instance_id,
            )
            .execute(op.as_executor())
            .await?;
            // Invariant B: same ordering fixup as the reschedule path above.
            // See `PromoteHeadsHook`'s doc comment for the notify policy.
            PromoteHeadsHook::register(op, &self.notifier, [self.job_type.clone()], retry_uuids)
                .await?;
        }

        if !terminal_uuids.is_empty() {
            // Same per-freed-queue promotion as `complete_in_op` -- a
            // `PromoteHeadsHook` freed-queue registration, never a CTE of
            // the DELETE (see that hook for the snapshot-visibility
            // reason; here it also merges with the retry registration
            // above into one hook execution) -- including its
            // `(queue_id, id)`-ordered `to_delete` lock; see
            // `complete_in_op`'s doc comment for why the ordering (and
            // leading with `queue_id`) is load-bearing.
            let rows = sqlx::query!(
                r#"
                WITH to_delete AS MATERIALIZED (
                    SELECT id FROM job_executions
                    WHERE id = ANY($1) AND poller_instance_id = $2
                    ORDER BY queue_id, id
                    FOR UPDATE
                ), deleted AS (
                    DELETE FROM job_executions je USING to_delete t WHERE je.id = t.id
                    RETURNING je.id, je.queue_id
                ), cleanup AS (
                    DELETE FROM job_execution_states s USING deleted d WHERE s.id = d.id
                )
                SELECT id AS "id!: JobId", queue_id AS "queue_id?"
                FROM deleted
                "#,
                &terminal_uuids,
                self.instance_id,
            )
            .fetch_all(op.as_executor())
            .await?;

            let freed_queues: Vec<String> =
                rows.iter().filter_map(|row| row.queue_id.clone()).collect();
            PromoteHeadsHook::register_freed_queues(op, &self.notifier, freed_queues).await?;
            for row in &rows {
                self.notifier.job_terminal_in_op(op, row.id).await?;
            }
        }

        self.repo.update_all_in_op(op, &mut jobs).await?;
        Ok(())
    }

    /// Fail every job in the batch with the same error — the batch's work was
    /// rolled back, so no job in it can be considered done.
    ///
    /// A `PoolCongestion` error is routed to [`Self::reschedule_congestion`]
    /// instead of the ordinary retry-policy path below: congestion carries
    /// no evidence any of these jobs is broken, so applying `RetryPolicy`'s
    /// attempt escalation to it would walk perfectly good jobs toward
    /// `max_attempts` termination for a condition they didn't cause.
    #[instrument(name = "job.fail_batch", skip_all,
        fields(job_type = %self.job_type, n_items = self.ids.len(), error = true, error.message = %error)
    )]
    async fn fail_batch(&mut self, error: JobError) -> Result<(), JobError> {
        let message = match error {
            JobError::PoolCongestion(message) => return self.reschedule_congestion(message).await,
            other => other.to_string(),
        };
        // Same bounded conflict retry as `seal_in_own_op`, and sound for the
        // same reason: this transaction is the crate's own. Losing the
        // failure-recording transaction to a deadlock is the worst time to
        // give up -- it would strand rows whose disposition was already
        // decided.
        let mut attempt = 1;
        loop {
            let result = async {
                let mut op = self.congestion.begin_terminal_write_op().await?;
                let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
                let fails: Vec<(JobId, String)> =
                    self.ids.iter().map(|id| (*id, message.clone())).collect();
                self.fail_in_op(&mut op, fails, now).await?;
                self.try_recycle_own_type(&mut op);
                op.commit().await?;
                Ok::<_, JobError>(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) if attempt < TX_ABORT_MAX_ATTEMPTS && is_retryable_conflict(&e) => {
                    tracing::warn!(
                        job_type = %self.job_type,
                        job_ids = %DisplayIds(&self.ids),
                        attempt,
                        exception.message = %e,
                        "batch fail-record lost a lock conflict; retrying"
                    );
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Reschedule every job in the batch after a `PoolCongestion`
    /// classification, via the shared [`congestion::reschedule_congested`]
    /// -- see `congestion.rs`'s module doc for the full rationale (attempt
    /// preserved, fixed jittered delay, `CongestionRescheduled` event,
    /// terminal-write repo).
    ///
    /// Deliberately does NOT call [`Self::try_recycle_own_type`], unlike
    /// every other terminal path (`seal`, `fail_batch`): that call hands the
    /// just-freed unit straight to [`crate::poller::ClaimHook`], which
    /// claims and immediately DISPATCHES more due work of this SAME type --
    /// a short-circuit path that has no idea about `pool_unit_budget` and
    /// so cannot check whether the shared pool it's about to hand a fresh
    /// dispatch to has actually recovered. Right after a completion or an
    /// ordinary error, that blindness is fine (neither is evidence the pool
    /// is unhealthy). Right after a CONGESTION reschedule it is exactly
    /// backwards: the freed unit would immediately re-claim and re-dispatch
    /// into the same pool that JUST failed to give this batch a connection,
    /// with no backoff and no headroom check, which can feed a tight
    /// claim-dispatch-fail-reschedule loop for this type specifically
    /// instead of letting it cool off. Not calling it here means the unit
    /// releases through the ORDINARY path (`Drop`'s `batch_completed`)
    /// instead -- picked back up by the next POOL-AWARE poll, which does
    /// check `pool_unit_budget` fresh.
    async fn reschedule_congestion(&mut self, message: String) -> Result<(), JobError> {
        self.rescheduled = true;
        self.congestion
            .reschedule(&self.ids, &self.attempts, message)
            .await
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
