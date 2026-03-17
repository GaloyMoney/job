//! Execution-time helpers available to running jobs.

use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use std::sync::{Arc, Mutex};

use super::{JobId, error::JobError};

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
    result: Arc<Mutex<Option<serde_json::Value>>>,
    cancel_token: CancellationToken,
}

impl CurrentJob {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        id: JobId,
        attempt: u32,
        pool: PgPool,
        execution_state: Option<serde_json::Value>,
        shutdown_rx: tokio::sync::broadcast::Receiver<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >,
        clock: ClockHandle,
        result: Arc<Mutex<Option<serde_json::Value>>>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            id,
            attempt,
            pool,
            execution_state_json: execution_state,
            shutdown_rx,
            clock,
            result,
            cancel_token,
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
          UPDATE job_executions
          SET execution_state_json = $1
          WHERE id = $2
        "#,
            execution_state_json,
            &self.id as &JobId
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
          UPDATE job_executions
          SET execution_state_json = $1
          WHERE id = $2
        "#,
            execution_state_json,
            &self.id as &JobId
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
    /// The result is serialized to JSON and will be available to callers via
    /// [`Jobs::await_completion`](crate::Jobs::await_completion). Each call
    /// overwrites the previous value — the **last** value set before the job
    /// completes (or errors) is what gets persisted. This allows incremental
    /// progress updates; for example, a batch job can call `set_result` after
    /// each chunk so that partial progress is preserved even on failure.
    pub fn set_result<T: Serialize>(&self, result: &T) -> Result<(), JobError> {
        let json =
            serde_json::to_value(result).map_err(JobError::CouldNotSerializeExecutionState)?;
        let mut guard = self.result.lock().expect("result mutex poisoned");
        *guard = Some(json);
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

    /// Non-blocking check if cancellation has been requested for this job.
    ///
    /// Returns `true` once the job has been cancelled via [`Jobs::cancel_job`](crate::Jobs::cancel_job).
    /// Job runners should check this periodically and return
    /// [`JobCompletion::Cancelled`](crate::JobCompletion::Cancelled) when `true`.
    pub fn cancellation_requested(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Returns a future that resolves when cancellation is requested.
    ///
    /// Useful in `tokio::select!` branches for cooperative cancellation.
    pub async fn cancellation_notified(&self) {
        self.cancel_token.cancelled().await;
    }
}
