//! Execution-time helpers available to running jobs.

use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;

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
    ) -> Self {
        Self {
            id,
            attempt,
            pool,
            execution_state_json: execution_state,
            shutdown_rx,
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
