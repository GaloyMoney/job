use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use futures::FutureExt;
use serde_json::Value as JsonValue;
use tracing::{Span, instrument};

use std::{panic::AssertUnwindSafe, sync::Arc};

use super::{
    JobId, current::CurrentJob, error::JobError, repo::JobRepo, runner::*, tracker::JobTracker,
};

#[derive(Debug)]
pub struct PolledJob {
    pub id: JobId,
    pub data_json: Option<JsonValue>,
    pub attempt: u32,
}

pub(crate) struct JobDispatcher {
    repo: JobRepo,
    retry_settings: RetrySettings,
    runner: Option<Box<dyn JobRunner>>,
    tracker: Arc<JobTracker>,
    rescheduled: bool,
    instance_id: uuid::Uuid,
}
impl JobDispatcher {
    pub fn new(
        repo: JobRepo,
        tracker: Arc<JobTracker>,
        retry_settings: RetrySettings,
        _id: JobId,
        runner: Box<dyn JobRunner>,
        instance_id: uuid::Uuid,
    ) -> Self {
        Self {
            repo,
            retry_settings,
            runner: Some(runner),
            tracker,
            rescheduled: false,
            instance_id,
        }
    }

    #[instrument(name = "job.execute_job", skip_all,
        fields(job_id, job_type, attempt, error, error.level, error.message, conclusion, now),
    err)]
    #[cfg_attr(feature = "es-entity", es_entity::es_event_context)]
    pub async fn execute_job(mut self, polled_job: PolledJob) -> Result<(), JobError> {
        let job = self.repo.find_by_id(polled_job.id).await?;
        let span = Span::current();
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        span.record("poller_id", tracing::field::display(self.instance_id));
        span.record("attempt", polled_job.attempt);
        span.record("now", tracing::field::display(crate::time::now()));
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
        );
        self.tracker.dispatch_job();
        match Self::dispatch_job(
            self.runner.take().expect("runner"),
            current_job,
            self.retry_settings.n_warn_attempts,
            polled_job.attempt,
        )
        .await
        {
            Err(e) => {
                span.record("conclusion", "Error");
                self.fail_job(job.id, e, polled_job.attempt).await?
            }
            Ok(JobCompletion::Complete) => {
                span.record("conclusion", "Complete");
                let op = self.repo.begin_op().await?;
                self.complete_job(op, job.id).await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::CompleteWithOp(op)) => {
                span.record("conclusion", "CompleteWithOp");
                self.complete_job(op, job.id).await?;
            }
            Ok(JobCompletion::CompleteWithTx(tx)) => {
                span.record("conclusion", "CompleteWithTx");
                self.complete_job(tx, job.id).await?;
            }
            Ok(JobCompletion::RescheduleNow) => {
                span.record("conclusion", "RescheduleNow");
                let op = self.repo.begin_op().await?;
                let t = op.now().unwrap_or_else(crate::time::now);
                self.reschedule_job(op, job.id, t).await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleNowWithOp(op)) => {
                span.record("conclusion", "RescheduleNowWithOp");
                let t = op.now().unwrap_or_else(crate::time::now);
                self.reschedule_job(op, job.id, t).await?;
            }
            Ok(JobCompletion::RescheduleNowWithTx(tx)) => {
                span.record("conclusion", "RescheduleNowWithTx");
                let t = crate::time::now();
                self.reschedule_job(tx, job.id, t).await?;
            }
            Ok(JobCompletion::RescheduleIn(d)) => {
                span.record("conclusion", "RescheduleIn");
                let op = self.repo.begin_op().await?;
                let t = op.now().unwrap_or_else(crate::time::now);
                let t = t + d;
                self.reschedule_job(op, job.id, t).await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleInWithOp(d, op)) => {
                span.record("conclusion", "RescheduleInWithOp");
                let t = op.now().unwrap_or_else(crate::time::now);
                let t = t + d;
                self.reschedule_job(op, job.id, t).await?;
            }
            Ok(JobCompletion::RescheduleInWithTx(d, tx)) => {
                span.record("conclusion", "RescheduleInWithOp");
                let t = crate::time::now() + d;
                self.reschedule_job(tx, job.id, t).await?;
            }
            Ok(JobCompletion::RescheduleAt(t)) => {
                span.record("conclusion", "RescheduleAt");
                let op = self.repo.begin_op().await?;
                self.reschedule_job(op, job.id, t).await?;
            }
            #[cfg(feature = "es-entity")]
            Ok(JobCompletion::RescheduleAtWithOp(t, op)) => {
                span.record("conclusion", "RescheduleAtWithOp");
                self.reschedule_job(op, job.id, t).await?;
            }
            Ok(JobCompletion::RescheduleAtWithTx(t, tx)) => {
                span.record("conclusion", "RescheduleAtWithTx");
                self.reschedule_job(tx, job.id, t).await?;
            }
        }
        Ok(())
    }

    async fn dispatch_job(
        runner: Box<dyn JobRunner>,
        current_job: CurrentJob,
        n_warn_attempts: Option<u32>,
        attempt: u32,
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
                if attempt <= n_warn_attempts.unwrap_or(u32::MAX) {
                    span.record("error.level", tracing::field::display(tracing::Level::WARN));
                } else {
                    span.record(
                        "error.level",
                        tracing::field::display(tracing::Level::ERROR),
                    );
                }
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

    #[instrument(name = "job.fail_job", skip(self), fields(attempt, will_retry = tracing::field::Empty))]
    async fn fail_job(&mut self, id: JobId, error: JobError, attempt: u32) -> Result<(), JobError> {
        let mut op = self.repo.begin_op().await?;
        let mut job = self.repo.find_by_id(id).await?;
        if self.retry_settings.n_attempts.unwrap_or(u32::MAX) > attempt {
            self.rescheduled = true;
            let next_attempt = attempt + 1;
            Span::current().record("will_retry", true);
            let reschedule_at = self.retry_settings.next_attempt_at(attempt);
            job.retry_scheduled(error.to_string(), reschedule_at, next_attempt);
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
        } else {
            Span::current().record("will_retry", false);
            job.job_errored(error.to_string());
            sqlx::query!(
                r#"
                DELETE FROM job_executions
                WHERE id = $1 AND poller_instance_id = $2
              "#,
                id as JobId,
                self.instance_id
            )
            .execute(op.as_executor())
            .await?;
        }

        self.repo.update_in_op(&mut op, &mut job).await?;

        op.commit().await?;
        Ok(())
    }

    #[instrument(name = "job.complete_job", skip(self, op), fields(id = %id))]
    async fn complete_job(
        &mut self,
        op: impl Into<sqlx::Transaction<'_, sqlx::Postgres>>,
        id: JobId,
    ) -> Result<(), JobError> {
        let mut tx = op.into();
        let mut job = self.repo.find_by_id(&id).await?;
        sqlx::query!(
            r#"
          DELETE FROM job_executions
          WHERE id = $1 AND poller_instance_id = $2
        "#,
            id as JobId,
            self.instance_id
        )
        .execute(&mut *tx)
        .await?;
        job.job_completed();
        self.repo.update_in_op(&mut tx, &mut job).await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument(name = "job.reschedule_job", skip(self, op), fields(id = %id, reschedule_at = %reschedule_at, attempt = 1))]
    async fn reschedule_job(
        &mut self,
        op: impl Into<sqlx::Transaction<'_, sqlx::Postgres>>,
        id: JobId,
        reschedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        let mut tx = op.into();
        self.rescheduled = true;
        let mut job = self.repo.find_by_id(&id).await?;
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
        .execute(&mut *tx)
        .await?;
        job.execution_rescheduled(reschedule_at);
        self.repo.update_in_op(&mut tx, &mut job).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl Drop for JobDispatcher {
    fn drop(&mut self) {
        self.tracker.job_completed(self.rescheduled)
    }
}
