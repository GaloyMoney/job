use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use futures::FutureExt;
use serde_json::Value as JsonValue;
use tracing::{Span, instrument};

use std::{panic::AssertUnwindSafe, sync::Arc};

use super::{
    JobId,
    current::CurrentJob,
    entity::{Job, JobType, RetryPolicy},
    error::JobError,
    notifier::JobEventNotifier,
    repo::JobRepo,
    runner::*,
    tracker::JobTracker,
};

#[derive(Debug)]
pub struct PolledJob {
    pub id: JobId,
    pub data_json: Option<JsonValue>,
    pub attempt: u32,
    /// The queue this row was claimed from. At most one row per queue is
    /// claimed per poll, so this doubles as the canonical ordering key when
    /// forming batches.
    pub queue_id: Option<String>,
}

pub(crate) struct JobDispatcher {
    repo: Arc<JobRepo>,
    retry_settings: RetrySettings,
    runner: Option<Box<dyn JobRunner>>,
    tracker: Arc<JobTracker>,
    notifier: Arc<JobEventNotifier>,
    job_type: JobType,
    /// Whether `job_type` carries a `max_concurrent_global` cap. A freed
    /// global slot only becomes claimable by a PEER instance if that peer
    /// learns about it — unlike the local case, which the tracker's
    /// capped-type notify rule already covers. See `delete_execution_in_op`.
    globally_capped: bool,
    rescheduled: bool,
    dispatched: bool,
    id: JobId,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
}
impl JobDispatcher {
    /// Claims the type's per-process slot **synchronously**, at construction.
    ///
    /// The poller builds this before spawning the execution task, so the very
    /// next poll already sees the slot occupied. Doing it inside
    /// `execute_job` instead leaves a window in which the poll loop re-polls
    /// — immediately, since a non-empty poll returns `Duration::ZERO` — still
    /// reads zero units in flight, and claims a second round of rows: exactly
    /// the over-claiming a per-process cap exists to prevent. Mirrors
    /// `BatchDispatcher::new`. The slot is released by `Drop`, so it cannot
    /// leak once taken.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo: Arc<JobRepo>,
        tracker: Arc<JobTracker>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        id: JobId,
        job_type: JobType,
        globally_capped: bool,
        runner: Box<dyn JobRunner>,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
    ) -> Self {
        tracker.dispatch_job(id, &job_type);
        Self {
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            notifier,
            job_type,
            globally_capped,
            rescheduled: false,
            dispatched: true,
            id,
            instance_id,
            clock,
        }
    }

    #[instrument(name = "job.execute_job", skip_all,
        fields(job_id, job_type, attempt, error, error.level, error.message, conclusion, now)
    )]
    #[cfg_attr(feature = "es-entity", es_entity::es_event_context)]
    pub async fn execute_job(
        mut self,
        job: Job,
        polled_job: PolledJob,
        shutdown_rx: tokio::sync::broadcast::Receiver<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >,
    ) -> Result<(), JobError> {
        let span = Span::current();
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("attempt", polled_job.attempt);
        span.record("now", tracing::field::display(self.clock.now()));
        job.inject_tracing_parent();
        #[cfg(feature = "es-entity")]
        {
            let mut ctx = es_entity::EventContext::current();
            ctx.insert(
                "job",
                &serde_json::json!({
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "attempt": polled_job.attempt,
                    "poller_id": self.instance_id
                }),
            )
            .expect("EventContext insert job data");
        }
        let current_job = CurrentJob::new(
            polled_job.id,
            polled_job.attempt,
            self.repo.pool().clone(),
            polled_job.data_json,
            shutdown_rx,
            self.clock.clone(),
            Arc::clone(&self.repo),
        );
        match Self::dispatch_job(self.runner.take().expect("runner"), current_job).await {
            Err(e) => {
                span.record("conclusion", "Error");
                self.fail_job(job.id, e, polled_job.attempt).await?
            }
            Ok(JobCompletion::Complete) => {
                span.record("conclusion", "Complete");
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                self.complete_job(&mut op, job.id).await?;
                op.commit().await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::CompleteWithOp(mut op)) => {
                span.record("conclusion", "CompleteWithOp");
                self.complete_job(&mut op, job.id).await?;
                op.commit().await?;
            }
            Ok(JobCompletion::CompleteWithTx(mut tx)) => {
                span.record("conclusion", "CompleteWithTx");
                self.complete_job(&mut tx, job.id).await?;
                tx.commit().await?;
            }
            Ok(JobCompletion::RescheduleNow) => {
                span.record("conclusion", "RescheduleNow");
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleNowWithOp(mut op)) => {
                span.record("conclusion", "RescheduleNowWithOp");
                let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            Ok(JobCompletion::RescheduleNowWithTx(mut tx)) => {
                span.record("conclusion", "RescheduleNowWithTx");
                let t = self.clock.now();
                self.reschedule_job(&mut tx, job.id, t).await?;
                tx.commit().await?;
            }
            Ok(JobCompletion::RescheduleIn(d)) => {
                span.record("conclusion", "RescheduleIn");
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                let t = t + d;
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleInWithOp(mut op, d)) => {
                span.record("conclusion", "RescheduleInWithOp");
                let t = op.maybe_now().unwrap_or_else(|| self.clock.now());
                let t = t + d;
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            Ok(JobCompletion::RescheduleInWithTx(mut tx, d)) => {
                span.record("conclusion", "RescheduleInWithOp");
                let t = self.clock.now() + d;
                self.reschedule_job(&mut tx, job.id, t).await?;
                tx.commit().await?;
            }
            Ok(JobCompletion::RescheduleAt(t)) => {
                span.record("conclusion", "RescheduleAt");
                let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleAtWithOp(mut op, t)) => {
                span.record("conclusion", "RescheduleAtWithOp");
                self.reschedule_job(&mut op, job.id, t).await?;
                op.commit().await?;
            }
            Ok(JobCompletion::RescheduleAtWithTx(mut tx, t)) => {
                span.record("conclusion", "RescheduleAtWithTx");
                self.reschedule_job(&mut tx, job.id, t).await?;
                tx.commit().await?;
            }
        }
        Ok(())
    }

    async fn dispatch_job(
        runner: Box<dyn JobRunner>,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, JobError> {
        match AssertUnwindSafe(runner.run(current_job))
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
                    "Job panicked during execution"
                );

                Err(JobError::JobExecutionError(format!(
                    "Job panicked: {message}"
                )))
            }
        }
    }

    #[instrument(
        name = "job.fail_job",
        skip(self),
        fields(
            job_id = tracing::field::Empty,
            job_type = tracing::field::Empty,
            poller_id = tracing::field::Empty,
            attempt,
            will_retry = tracing::field::Empty,
            error = tracing::field::Empty,
            error.level = tracing::field::Empty,
            error.message = tracing::field::Empty
        )
    )]
    async fn fail_job(&mut self, id: JobId, error: JobError, attempt: u32) -> Result<(), JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let mut job = self.repo.find_by_id_in_op(&mut op, id).await?;

        let span = Span::current();
        let error_str = error.to_string();
        span.record("job_id", tracing::field::display(id));
        span.record("job_type", tracing::field::display(&job.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("error", true);
        span.record("error.message", tracing::field::display(&error_str));

        let retry_policy = RetryPolicy::from(&self.retry_settings);

        if let Some((reschedule_at, next_attempt)) =
            job.maybe_schedule_retry(self.clock.now(), attempt, &retry_policy, error_str)
        {
            let exceeded_warn_attempts = self
                .retry_settings
                .n_warn_attempts
                .is_some_and(|limit| next_attempt > limit);

            let level = if exceeded_warn_attempts {
                tracing::Level::ERROR
            } else {
                tracing::Level::WARN
            };
            span.record("error.level", tracing::field::display(level));
            self.rescheduled = true;
            span.record("will_retry", true);

            sqlx::query!(
                r#"
                UPDATE job_executions
                SET state = 'pending', execute_at = $2, attempt_index = $3, poller_instance_id = NULL
                WHERE id = $1 AND poller_instance_id = $4
              "#,
                id as JobId,
                reschedule_at,
                next_attempt as i32,
                self.instance_id
            )
            .execute(op.as_executor())
            .await?;
            self.notifier
                .execution_ready_in_op(&mut op, &job.job_type)
                .await?;
        } else {
            span.record(
                "error.level",
                tracing::field::display(tracing::Level::ERROR),
            );
            span.record("will_retry", false);

            self.delete_execution_in_op(&mut op, id, &job.job_type)
                .await?;
        }

        self.repo.update_in_op(&mut op, &mut job).await?;
        op.commit().await?;
        Ok(())
    }

    /// Delete the execution row and report what its removal makes true: the job
    /// is terminal, and its type may now have claimable backlog it didn't
    /// before — because it held a `queue_id` and that queue's next job is now
    /// eligible, or because `job_type` carries a global cap and this was the
    /// slot that saturated it. The queue case only unblocks work THIS process
    /// can already see (any instance may claim it on its next ordinary poll),
    /// but the global-cap case is different: a PEER instance may have already
    /// dropped `job_type` from its own poll entirely, having observed the cap
    /// as saturated, and — absent this report — would only rediscover the
    /// freed slot on its next unrelated poll or the 60s `MAX_WAIT` backstop.
    /// The reschedule paths (`reschedule_job`, the retry branch of
    /// `fail_job`) don't need this: they always report unconditionally,
    /// since a job going back to `pending` is exactly the ordinary case every
    /// instance already polls for. Reports nothing when no row was deleted.
    ///
    /// The `job_execution_states` cleanup is conditional on `unique_key IS
    /// NULL`: a KEYED job's state row is deliberately RETAINED here (rather
    /// than deleted alongside its execution row) so it stays readable after
    /// terminal and can seed the next generation under
    /// `KeyedJobInitializer::inherits_state` (see `keyed.rs`) — the retained
    /// row is compacted away at that key's next spawn. Every other flavor's
    /// execution never carries a `unique_key`, so this changes nothing for
    /// them.
    async fn delete_execution_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        job_type: &JobType,
    ) -> Result<(), JobError> {
        let deleted = sqlx::query_scalar!(
            r#"
          WITH deleted AS (
              DELETE FROM job_executions
              WHERE id = $1 AND poller_instance_id = $2
              RETURNING id, queue_id, unique_key
          ), cleanup AS (
              DELETE FROM job_execution_states s USING deleted d
              WHERE s.id = d.id AND d.unique_key IS NULL
          )
          SELECT queue_id AS "queue_id?" FROM deleted
        "#,
            id as JobId,
            self.instance_id
        )
        .fetch_optional(op.as_executor())
        .await?;

        let Some(freed_queue_id) = deleted else {
            return Ok(());
        };

        self.notifier.job_terminal_in_op(op, id).await?;

        if freed_queue_id.is_some() || self.globally_capped {
            self.notifier.execution_ready_in_op(op, job_type).await?;
        }

        Ok(())
    }

    #[instrument(name = "job.complete_job", skip(self, op), fields(id = %id))]
    async fn complete_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
    ) -> Result<(), JobError> {
        let mut job = self.repo.find_by_id_in_op(&mut *op, &id).await?;
        self.delete_execution_in_op(op, id, &job.job_type).await?;
        job.complete_job();
        self.repo.update_in_op(op, &mut job).await?;
        Ok(())
    }

    #[instrument(name = "job.reschedule_job", skip(self, op), fields(id = %id, reschedule_at = %reschedule_at, attempt = 1))]
    async fn reschedule_job(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        reschedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        self.rescheduled = true;
        let mut job = self.repo.find_by_id_in_op(&mut *op, &id).await?;
        sqlx::query!(
            r#"
          UPDATE job_executions
          SET state = 'pending', execute_at = $2, attempt_index = 1, poller_instance_id = NULL
          WHERE id = $1 AND poller_instance_id = $3
        "#,
            id as JobId,
            reschedule_at,
            self.instance_id
        )
        .execute(op.as_executor())
        .await?;
        self.notifier
            .execution_ready_in_op(op, &job.job_type)
            .await?;
        job.reschedule_execution(reschedule_at);
        self.repo.update_in_op(op, &mut job).await?;
        Ok(())
    }
}

impl Drop for JobDispatcher {
    fn drop(&mut self) {
        if self.dispatched {
            self.tracker
                .job_completed(self.id, &self.job_type, self.rescheduled);
        }
    }
}
