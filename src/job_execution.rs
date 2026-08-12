//! The `job_executions` index row and the runtime status derived from it.
//!
//! A row exists for as long as a job is schedulable (pending) or in-flight
//! (running); the terminal transition deletes it. [`JobSnapshot`] pairs an
//! optional [`JobExecutionRow`] with the durable [`Job`] entity and delegates
//! its accessors to them.
//!
//! [`JobSnapshot`]: crate::JobSnapshot
//! [`Job`]: crate::Job

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

/// Rust-side mirror of the `JobExecutionState` Postgres enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "jobexecutionstate", rename_all = "lowercase")]
pub(crate) enum JobExecutionState {
    Pending,
    Running,
}

/// A row of the `job_executions` index table — present until the job reaches a
/// terminal state, at which point the row is deleted (see `src/dispatcher.rs`).
#[derive(Debug, Clone)]
pub(crate) struct JobExecutionRow {
    pub state: JobExecutionState,
    pub execute_at: Option<DateTime<Utc>>,
    pub attempt_index: i32,
    pub alive_at: DateTime<Utc>,
    pub execution_state_json: Option<serde_json::Value>,
}

impl JobExecutionRow {
    /// The current attempt number.
    pub(crate) fn attempt(&self) -> u32 {
        self.attempt_index as u32
    }

    /// The next scheduled run time — `Some` only while pending.
    pub(crate) fn next_run(&self) -> Option<DateTime<Utc>> {
        match self.state {
            JobExecutionState::Pending => self.execute_at,
            JobExecutionState::Running => None,
        }
    }

    /// The runtime [`JobStatus`] for a live row, given the job's `queue_id`.
    pub(crate) fn status(&self, queue_id: Option<String>) -> JobStatus {
        match self.state {
            JobExecutionState::Pending => JobStatus::Pending {
                scheduled_at: self
                    .execute_at
                    .expect("pending execution row always has execute_at"),
                attempt: self.attempt(),
                queue_id,
            },
            JobExecutionState::Running => JobStatus::Running {
                attempt: self.attempt(),
                alive_at: self.alive_at,
                queue_id,
            },
        }
    }

    /// Decode the committed execution state as `S` (`Ok(None)` when unset).
    pub(crate) fn execution_state<S: DeserializeOwned>(
        &self,
    ) -> Result<Option<S>, serde_json::Error> {
        match &self.execution_state_json {
            Some(json) => serde_json::from_value(json.clone()).map(Some),
            None => Ok(None),
        }
    }
}

/// Runtime status of a job, carried by [`JobSnapshot::state`](crate::JobSnapshot::state).
///
/// `Pending`/`Running` describe a live execution row; `Completed`/`Errored`
/// are terminal (the execution row has been removed). `queue_id` is immutable
/// job metadata and is present on every variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    /// The job is scheduled and waiting to be claimed by a poller.
    Pending {
        scheduled_at: DateTime<Utc>,
        attempt: u32,
        queue_id: Option<String>,
    },
    /// The job is currently executing on a poller instance.
    Running {
        attempt: u32,
        alive_at: DateTime<Utc>,
        queue_id: Option<String>,
    },
    /// The job reached terminal success.
    Completed { queue_id: Option<String> },
    /// The job exhausted its retries and was marked as errored; `error` is
    /// the final error string from the entity's last `ExecutionErrored` event.
    Errored {
        error: String,
        queue_id: Option<String>,
    },
}

impl JobStatus {
    /// The queue this job belongs to, if any (immutable metadata; present on
    /// every variant).
    pub fn queue_id(&self) -> Option<&str> {
        match self {
            JobStatus::Pending { queue_id, .. }
            | JobStatus::Running { queue_id, .. }
            | JobStatus::Completed { queue_id }
            | JobStatus::Errored { queue_id, .. } => queue_id.as_deref(),
        }
    }

    /// `true` once the job has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Completed { .. } | JobStatus::Errored { .. }
        )
    }
}
