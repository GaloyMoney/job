//! [`JobSnapshot`] — a point-in-time view of a job, pairing the durable
//! [`Job`] entity with an optional [`JobExecutionRow`] and delegating every
//! accessor to them.

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

use crate::{
    Job, JobTerminalState,
    job_execution::{JobExecutionRow, JobStatus},
};

/// A point-in-time view of a job, produced by [`JobHandle::load`](crate::JobHandle::load).
///
/// It pairs the durable [`Job`] entity with an optional [`JobExecutionRow`]
/// (present until the job is terminal) and delegates every accessor to them —
/// so one `load()` does a single pair of committed reads (within one op, so
/// the two are consistent) and every getter below is then synchronous (only
/// decoding can fail). Nothing is cached on the handle: a fresh `load()`
/// always reflects the latest committed state.
pub struct JobSnapshot {
    job: Job,
    row: Option<JobExecutionRow>,
}

impl JobSnapshot {
    pub(crate) fn from_parts(job: Job, row: Option<JobExecutionRow>) -> Self {
        Self { job, row }
    }

    /// The runtime [`JobStatus`] at load time. `Pending`/`Running` come from
    /// the execution row; `Completed`/`Errored` from the entity once the row
    /// is gone.
    pub fn state(&self) -> JobStatus {
        let queue_id = self.job.queue_id.clone();
        match &self.row {
            Some(row) => row.status(queue_id),
            None => match self.job.terminal_state() {
                Some(JobTerminalState::Completed) => JobStatus::Completed { queue_id },
                Some(JobTerminalState::Errored) => JobStatus::Errored {
                    error: self.job.last_error().unwrap_or_default(),
                    queue_id,
                },
                // `JobRepo::load_snapshot_by_id` keeps a row only for a
                // non-terminal entity (and discards it once terminal), so an
                // absent row here always corresponds to a terminal entity.
                None => unreachable!(
                    "snapshot invariant: an absent execution row implies a terminal entity"
                ),
            },
        }
    }

    /// The next scheduled run time — `Some` only while `Pending`.
    pub fn next_run(&self) -> Option<DateTime<Utc>> {
        self.row.as_ref().and_then(|row| row.next_run())
    }

    /// The current attempt — `Some` only while the job has a live execution
    /// row (`Pending`/`Running`).
    pub fn attempt(&self) -> Option<u32> {
        self.row.as_ref().map(|row| row.attempt())
    }

    /// The queue this job belongs to, if any.
    pub fn queue_id(&self) -> Option<&str> {
        self.job.queue_id.as_deref()
    }

    /// Decode the job's committed execution state as `S`.
    ///
    /// Honest absence: `Ok(None)` when no state has been written yet or the
    /// execution row is gone (terminal job). Mirrors
    /// [`CurrentJob::execution_state`](crate::CurrentJob::execution_state).
    pub fn execution_state<S: DeserializeOwned>(&self) -> Result<Option<S>, serde_json::Error> {
        match &self.row {
            Some(row) => row.execution_state(),
            None => Ok(None),
        }
    }

    /// Decode the job's stored configuration payload as `T`.
    pub fn config<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        self.job.config()
    }

    /// Decode the return value the runner attached via
    /// [`CurrentJob::set_result`](crate::CurrentJob::set_result), if any.
    pub fn return_value<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.job.return_value()
    }

    /// The underlying durable entity.
    pub fn job(&self) -> &Job {
        &self.job
    }
}
