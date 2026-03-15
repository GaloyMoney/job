//! Execution-time helpers available to running jobs.

use es_entity::clock::ClockHandle;
use es_entity::db;
use serde::{Serialize, de::DeserializeOwned};

use super::{JobId, error::JobError};

/// Context provided to a [`JobRunner`](crate::JobRunner) while a job is executing.
pub struct CurrentJob {
    id: JobId,
    attempt: u32,
    pool: db::Pool,
    execution_state_json: Option<serde_json::Value>,
    shutdown_rx: tokio::sync::broadcast::Receiver<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    clock: ClockHandle,
}

impl CurrentJob {
    pub(super) fn new(
        id: JobId,
        attempt: u32,
        pool: db::Pool,
        execution_state: Option<serde_json::Value>,
        shutdown_rx: tokio::sync::broadcast::Receiver<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >,
        clock: ClockHandle,
    ) -> Self {
        Self {
            id,
            attempt,
            pool,
            execution_state_json: execution_state,
            shutdown_rx,
            clock,
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
        let json_str = serde_json::to_string(&execution_state_json)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query(
            r#"
          UPDATE job_executions
          SET execution_state_json = ?1
          WHERE id = ?2
        "#,
        )
        .bind(&json_str)
        .bind(self.id)
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
        let json_str = serde_json::to_string(&execution_state_json)
            .map_err(JobError::CouldNotSerializeExecutionState)?;
        sqlx::query(
            r#"
          UPDATE job_executions
          SET execution_state_json = ?1
          WHERE id = ?2
        "#,
        )
        .bind(&json_str)
        .bind(self.id)
        .execute(&self.pool)
        .await?;
        self.execution_state_json = Some(execution_state_json);
        Ok(())
    }

    pub fn id(&self) -> &JobId {
        &self.id
    }

    pub fn pool(&self) -> &db::Pool {
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

    /// Wait for a shutdown signal. Returns `true` if shutdown was requested.
    ///
    /// Job runners can use this to detect when the application is shutting down
    /// and perform cleanup or finish their current work gracefully.
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
