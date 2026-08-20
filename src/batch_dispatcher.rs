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
    entity::{Job, JobType, RetryPolicy},
    error::JobError,
    execution_hooks::PromoteHeadsHook,
    notifier::JobEventNotifier,
    poller::JobPoller,
    repo::JobRepo,
    runner::RetrySettings,
    tracker::{JobTracker, UnitReservation},
};

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
    instance_id: uuid::Uuid,
    clock: ClockHandle,
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
            instance_id,
            clock,
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
            instance_id,
            clock,
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
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        self.recycle_unit();
        let reservation = self.tracker.recycle(&self.job_type);
        poller.register_claim_recycle(op, &self.job_type, reservation);
    }

    #[instrument(name = "job.execute_batch", skip_all,
        fields(job_type, n_items, poller_id, error, error.level, error.message, conclusion, now)
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
        match Self::run_batch(runner, items, ctx).await {
            Ok(completion) => self.apply(completion).await,
            Err(e) => {
                span.record("conclusion", "Error");
                self.fail_batch(e).await
            }
        }
    }

    async fn run_batch(
        runner: Box<dyn AnyBatchedJobRunner>,
        items: Vec<RawBatchItem>,
        ctx: BatchRunCtx,
    ) -> Result<JobBatchCompletion, JobError> {
        match AssertUnwindSafe(runner.run_batch_erased(items, ctx))
            .catch_unwind()
            .await
        {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(e)) => {
                let span = Span::current();
                let error = e.to_string();
                span.record("error", true);
                span.record("error.message", tracing::field::display(&error));
                span.record("error.level", tracing::field::display(tracing::Level::WARN));
                Err(JobError::JobExecutionError(error))
            }
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
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                self.seal(&mut op, outcomes).await?;
                op.commit().await?;
            }
            JobBatchCompletion::WithOutcomes(outcomes) => {
                span.record("conclusion", "WithOutcomes");
                if let Err(e) = self.validate(&outcomes) {
                    return self.fail_batch(e).await;
                }
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                self.seal(&mut op, outcomes).await?;
                op.commit().await?;
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
    /// The `to_delete` CTE locks the rows in id order BEFORE the `DELETE`
    /// touches them, for deadlock avoidance rather than for correctness: a
    /// spawn parking into several queues locks those queues' occupants in id
    /// order too (`execution_hooks::insert`'s `lock_queue_occupants`), and
    /// this statement contends with it for exactly those rows. A bare
    /// `DELETE ... WHERE id = ANY(...)` would acquire in scan order --
    /// unordered under a bitmap scan -- so a spawn and a batch completion
    /// touching the same two rows could each hold what the other wants.
    /// `MATERIALIZED` plus `ORDER BY` puts `LockRows` above `Sort`, which is
    /// what makes the order actually hold at execution time.
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
                ORDER BY id
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

        sqlx::query!(
            r#"
            UPDATE job_executions AS je
            SET state = 'pending', execute_at = u.execute_at, attempt_index = 1,
                poller_instance_id = NULL
            FROM UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
            WHERE je.id = u.id AND je.poller_instance_id = $3
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
            sqlx::query!(
                r#"
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = u.execute_at,
                    attempt_index = u.attempt_index, poller_instance_id = NULL
                FROM UNNEST($1::uuid[], $2::timestamptz[], $3::int4[])
                    AS u(id, execute_at, attempt_index)
                WHERE je.id = u.id AND je.poller_instance_id = $4
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
            // above into one hook execution) -- including its id-ordered
            // `to_delete` lock; see `complete_in_op`'s doc comment for why
            // the ordering is load-bearing.
            let rows = sqlx::query!(
                r#"
                WITH to_delete AS MATERIALIZED (
                    SELECT id FROM job_executions
                    WHERE id = ANY($1) AND poller_instance_id = $2
                    ORDER BY id
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
    #[instrument(name = "job.fail_batch", skip_all,
        fields(job_type = %self.job_type, n_items = self.ids.len(), error = true, error.message = %error)
    )]
    async fn fail_batch(&mut self, error: JobError) -> Result<(), JobError> {
        let message = error.to_string();
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let fails: Vec<(JobId, String)> =
            self.ids.iter().map(|id| (*id, message.clone())).collect();
        self.fail_in_op(&mut op, fails, now).await?;
        self.try_recycle_own_type(&mut op);
        op.commit().await?;
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
