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
use std::sync::Arc;

use super::{
    JobId,
    batched::{
        AnyBatchedJobRunner, BatchItemOutcome, BatchOutcomes, BatchRunCtx, JobBatchCompletion,
        RawBatchItem, ShutdownRx,
    },
    entity::{Job, JobType, RetryPolicy},
    error::JobError,
    notifier::JobEventNotifier,
    repo::JobRepo,
    runner::RetrySettings,
    spawner::swap_older_parked_siblings_in_op,
    tracker::JobTracker,
};

pub(crate) struct BatchDispatcher {
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
        Ok(())
    }

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
        // Unlike `dispatcher.rs`'s per-job `delete_execution_in_op`, no
        // `unique_key IS NULL` guard is needed here: batched jobs are never
        // keyed (`KeyedJobInitializer` registers through `add_initializer`'s
        // ordinary per-job dispatch path, not `add_batched_initializer`), so
        // every execution row a batch ever deletes already has a NULL
        // `unique_key` and this cleanup always applies.
        //
        // Promotes each freed queue's oldest parked sibling in the same
        // statement — mirrors `dispatcher.rs`'s `delete_execution_in_op`
        // exactly (same head tiebreak), rather than the coarser
        // "notify my own type if any queue freed" this used to do: a batch
        // can free several distinct queues in one commit, and each one's
        // next job can be a different type than this batch's own.
        let rows = sqlx::query!(
            r#"
            WITH deleted AS (
                DELETE FROM job_executions
                WHERE id = ANY($1) AND poller_instance_id = $2
                RETURNING id, queue_id
            ), cleanup AS (
                DELETE FROM job_execution_states s USING deleted d WHERE s.id = d.id
            ), heads AS (
                SELECT DISTINCT ON (d.queue_id) p.id
                FROM deleted d
                CROSS JOIN LATERAL (
                    SELECT id FROM job_executions
                    WHERE state = 'parked' AND queue_id = d.queue_id
                    ORDER BY execute_at, id
                    LIMIT 1
                ) p
                WHERE d.queue_id IS NOT NULL
            ), promoted AS (
                UPDATE job_executions je SET state = 'pending'
                FROM heads h WHERE je.id = h.id
                RETURNING je.job_type
            )
            SELECT id AS "id!: JobId", queue_id AS "queue_id?",
                   (SELECT array_agg(job_type) FROM promoted) AS "promoted_types?"
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
        let promoted_types: Vec<String> = rows
            .first()
            .and_then(|row| row.promoted_types.clone())
            .unwrap_or_default();

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
        for promoted_type in promoted_types {
            self.notifier
                .execution_ready_in_op(op, &JobType::from_owned(promoted_type))
                .await?;
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
        // Invariant B: mirrors `dispatcher.rs::reschedule_job` — each
        // rescheduled row keeps its queue's active slot, but an older parked
        // sibling should run first.
        let promoted = swap_older_parked_siblings_in_op(op, &uuids).await?;

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
        if promoted.is_empty() {
            self.notifier
                .execution_ready_in_op(op, &self.job_type)
                .await?;
        } else {
            for promoted_type in promoted {
                self.notifier
                    .execution_ready_in_op(op, &JobType::from_owned(promoted_type))
                    .await?;
            }
        }
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
            let promoted = swap_older_parked_siblings_in_op(op, &retry_uuids).await?;
            if promoted.is_empty() {
                self.notifier
                    .execution_ready_in_op(op, &self.job_type)
                    .await?;
            } else {
                for promoted_type in promoted {
                    self.notifier
                        .execution_ready_in_op(op, &JobType::from_owned(promoted_type))
                        .await?;
                }
            }
        }

        if !terminal_uuids.is_empty() {
            // See the comment in `complete_in_op` above: batched jobs are
            // never keyed, so no `unique_key IS NULL` guard is needed here.
            // Same per-freed-queue promotion as `complete_in_op` — mirrors
            // `dispatcher.rs::delete_execution_in_op`'s precision rather
            // than notifying this batch's own type.
            let rows = sqlx::query!(
                r#"
                WITH deleted AS (
                    DELETE FROM job_executions
                    WHERE id = ANY($1) AND poller_instance_id = $2
                    RETURNING id, queue_id
                ), cleanup AS (
                    DELETE FROM job_execution_states s USING deleted d WHERE s.id = d.id
                ), heads AS (
                    SELECT DISTINCT ON (d.queue_id) p.id
                    FROM deleted d
                    CROSS JOIN LATERAL (
                        SELECT id FROM job_executions
                        WHERE state = 'parked' AND queue_id = d.queue_id
                        ORDER BY execute_at, id
                        LIMIT 1
                    ) p
                    WHERE d.queue_id IS NOT NULL
                ), promoted AS (
                    UPDATE job_executions je SET state = 'pending'
                    FROM heads h WHERE je.id = h.id
                    RETURNING je.job_type
                )
                SELECT id AS "id!: JobId", queue_id AS "queue_id?",
                       (SELECT array_agg(job_type) FROM promoted) AS "promoted_types?"
                FROM deleted
                "#,
                &terminal_uuids,
                self.instance_id,
            )
            .fetch_all(op.as_executor())
            .await?;

            let promoted_types: Vec<String> = rows
                .first()
                .and_then(|row| row.promoted_types.clone())
                .unwrap_or_default();
            for row in &rows {
                self.notifier.job_terminal_in_op(op, row.id).await?;
            }
            for promoted_type in promoted_types {
                self.notifier
                    .execution_ready_in_op(op, &JobType::from_owned(promoted_type))
                    .await?;
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
