//! Execution-time helpers available to running jobs.

use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;

use std::sync::Arc;

use super::{JobId, error::JobError, outcome::JobReturnValue, repo::JobRepo};

/// Context provided to a [`JobRunner`](crate::JobRunner) while a job is executing.
pub struct CurrentJob {
    id: JobId,
    attempt: u32,
    pool: PgPool,
    execution_state_json: Option<serde_json::Value>,
    shutdown_rx: tokio::sync::broadcast::Receiver<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    clock: ClockHandle,
    repo: Arc<JobRepo>,
}

impl CurrentJob {
    pub(super) fn new(
        id: JobId,
        attempt: u32,
        pool: PgPool,
        execution_state: Option<serde_json::Value>,
        shutdown_rx: tokio::sync::broadcast::Receiver<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >,
        clock: ClockHandle,
        repo: Arc<JobRepo>,
    ) -> Self {
        Self {
            id,
            attempt,
            pool,
            execution_state_json: execution_state,
            shutdown_rx,
            clock,
            repo,
        }
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Deserialize the persisted execution state into your custom type.
    pub fn execution_state<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        if let Some(execution_state) = self.execution_state_json.as_ref() {
            serde_json::from_value(execution_state.clone()).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Update the execution state as part of an existing database operation.
    pub async fn update_execution_state_in_op<T: Serialize>(
        &mut self,
        op: &mut impl es_entity::AtomicOperation,
        execution_state: &T,
    ) -> Result<(), JobError> {
        let execution_state_json = serde_json::to_value(execution_state)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query!(
            r#"
          INSERT INTO job_execution_states (id, execution_state_json)
          VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET execution_state_json = EXCLUDED.execution_state_json
        "#,
            &self.id as &JobId,
            execution_state_json,
        )
        .execute(op.as_executor())
        .await?;
        self.execution_state_json = Some(execution_state_json);
        Ok(())
    }

    pub async fn update_execution_state<T: Serialize>(
        &mut self,
        execution_state: T,
    ) -> Result<(), JobError> {
        let execution_state_json = serde_json::to_value(execution_state)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query!(
            r#"
          INSERT INTO job_execution_states (id, execution_state_json)
          VALUES ($1, $2)
          ON CONFLICT (id) DO UPDATE SET execution_state_json = EXCLUDED.execution_state_json
        "#,
            &self.id as &JobId,
            execution_state_json,
        )
        .execute(&self.pool)
        .await?;
        self.execution_state_json = Some(execution_state_json);
        Ok(())
    }

    pub fn id(&self) -> &JobId {
        &self.id
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a reference to the clock handle used by this job.
    ///
    /// Use this to get the current time or perform time-related operations
    /// that are consistent with the job service's configured clock.
    pub fn clock(&self) -> &ClockHandle {
        &self.clock
    }

    /// Begin a new database operation using the job's configured clock.
    ///
    /// The returned `DbOp` will use the same clock as the job service,
    /// ensuring consistent time handling in tests with artificial clocks.
    #[cfg(feature = "es-entity")]
    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, JobError> {
        let ret = es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await?;
        Ok(ret)
    }

    /// Attach or update the result value for this job execution.
    ///
    /// The result is serialized to JSON and persisted to the database
    /// immediately. It will be available to callers via
    /// [`JobHandle::await_completion`](crate::JobHandle::await_completion). Each call
    /// overwrites the previous value — the **last** value set is what callers
    /// see. This allows incremental progress updates; for example, a batch job
    /// can call `set_result` after each chunk so that partial progress is
    /// preserved even on failure.
    ///
    /// Tolerates a concurrent shutdown/lost-job kill appending to the same
    /// job's event log: both writers append commutative events, so a lost
    /// append race retries on a fresh entity instead of failing the runner
    /// (see [`JobRepo::append_events_in_op_with_retry`]).
    pub async fn set_result<T: Serialize>(&self, result: &T) -> Result<(), JobError> {
        let job_result =
            JobReturnValue::try_from(result).map_err(JobError::CouldNotSerializeResult)?;
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        self.set_result_in_op_with_retry(&mut op, job_result)
            .await?;
        op.commit().await?;
        Ok(())
    }

    /// Attach or update the result value as part of an existing database
    /// operation.
    ///
    /// This is the composable variant of [`set_result`](Self::set_result) —
    /// callers provide an open atomic operation so the result write participates
    /// in a larger transaction.
    pub async fn set_result_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        result: &impl Serialize,
    ) -> Result<(), JobError> {
        let job_result =
            JobReturnValue::try_from(result).map_err(JobError::CouldNotSerializeResult)?;
        self.set_result_in_op_with_retry(op, job_result).await
    }

    /// Find, idempotently update, and persist the return value, retrying a
    /// concurrent-modification race inside a savepoint so the caller's op
    /// stays usable (a failed event INSERT would otherwise abort it).
    async fn set_result_in_op_with_retry(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job_result: JobReturnValue,
    ) -> Result<(), JobError> {
        self.repo
            .append_events_in_op_with_retry(op, &self.id, |job| {
                // `update_return_value` is an idempotency guard: when the value
                // is unchanged it pushes no event, and the helper's
                // `update_in_op` persists nothing for an entity with no new
                // events — so the unchanged case stays write-free.
                let _ = job.update_return_value(job_result.clone());
            })
            .await?;
        Ok(())
    }

    /// Wait for a shutdown signal. Returns `true` if shutdown was requested.
    ///
    /// Job runners can use this to detect when the application is shutting down
    /// and perform cleanup or finish their current work gracefully.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use job::CurrentJob;
    /// # async fn example(mut current_job: CurrentJob) {
    /// tokio::select! {
    ///     _ = current_job.shutdown_requested() => {
    ///         // Shutdown requested, exit gracefully
    ///         return;
    ///     }
    ///     result = do_work() => {
    ///         // Normal work completion
    ///     }
    /// }
    /// # }
    /// # async fn do_work() {}
    /// ```
    pub async fn shutdown_requested(&mut self) -> bool {
        self.shutdown_rx.recv().await.is_ok()
    }

    /// Non-blocking check if shutdown has been requested.
    ///
    /// Returns `true` if a shutdown signal has been sent.
    /// Use this for polling-style shutdown detection within loops.
    pub fn is_shutdown_requested(&mut self) -> bool {
        self.shutdown_rx.try_recv().is_ok()
    }
}
