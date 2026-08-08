//! Per-job capability handles minted by [`Jobs`](crate::Jobs): observe live
//! status, read back committed execution state (typed), proxy entity data,
//! and await completion of jobs you did not run yourself.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::de::DeserializeOwned;
use tracing::instrument;

use std::{sync::Arc, time::Duration};

use crate::{
    JobId,
    error::JobError,
    notification_router::JobNotificationRouter,
    outcome::{JobOutcome, JobTerminalState},
    repo::JobRepo,
};

/// Rust-side mirror of the `JobExecutionState` Postgres enum, used to decode
/// the `state` column when reading a live execution row.
#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(type_name = "jobexecutionstate", rename_all = "lowercase")]
enum JobExecutionRowState {
    Pending,
    Running,
}

/// Live status of a job.
///
/// `Pending`/`Running` are sourced from the `job_executions` row;
/// `Completed`/`Errored` from the job entity once the terminal DELETE has
/// removed the row. `queue_id` is row-sourced on `Pending`/`Running` and
/// entity-sourced on the terminal variants (the entity and the row are
/// written in the same atomic operation at spawn, so they always agree).
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

/// A minted, cloneable per-job capability: the public way to observe and
/// await a job you did not run yourself.
///
/// Obtain one from [`Jobs::handle`](crate::Jobs::handle),
/// [`Jobs::handles`](crate::Jobs::handles), or
/// [`JobSpawner::spawn_unique`](crate::JobSpawner::spawn_unique).
///
/// # Contracts
///
/// 1. **Live committed reads; no cached state on handles.**
/// 2. **Order preservation:** `await_all` outcomes\[i\] ↔ handles\[i\].
/// 3. **Cancel safety:** dropping an in-flight `await_all`/`await_completion`
///    future (`select!` loser) leaks nothing and a re-registered wait
///    resolves — the dominant call pattern is `tokio::select!` against
///    [`CurrentJob::shutdown_requested`](crate::CurrentJob::shutdown_requested).
/// 4. **Torn-read-free `status()`:** the terminal DELETE of the execution row
///    commits in the same atomic operation as the terminal entity events, so
///    an absent row implies the entity is terminal.
/// 5. **Honest absence:** `execution_state` ⇒ `Ok(None)` on no-row/no-state;
///    `status()` ⇒ `Err(Find)` only if the job never existed.
/// 6. **`spawn_unique` handle identity:** on the duplicate path the returned
///    handle's id is the PERSISTED job's id, not the caller's fresh one.
#[derive(Clone)]
pub struct JobHandle {
    id: JobId,
    repo: Arc<JobRepo>,
    router: Arc<JobNotificationRouter>,
    // Held so later stages (e.g. snapshots) can time-stamp reads consistently
    // with the service clock; no current method reads it.
    #[allow(dead_code)]
    clock: ClockHandle,
}

impl JobHandle {
    pub(crate) fn new(
        id: JobId,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            id,
            repo,
            router,
            clock,
        }
    }

    /// The id of the job this handle observes.
    pub fn id(&self) -> JobId {
        self.id
    }

    /// Live status of the job (contract 4: torn-read-free).
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if the job never existed.
    #[instrument(name = "job.handle.status", skip(self), fields(id = %self.id))]
    pub async fn status(&self) -> Result<JobStatus, JobError> {
        let row = sqlx::query!(
            r#"
            SELECT state AS "state: JobExecutionRowState", execute_at, attempt_index, alive_at, queue_id
            FROM job_executions WHERE id = $1
            "#,
            self.id as JobId,
        )
        .fetch_optional(self.repo.pool())
        .await?;

        if let Some(row) = row {
            return Ok(match row.state {
                JobExecutionRowState::Pending => JobStatus::Pending {
                    scheduled_at: row
                        .execute_at
                        .expect("pending execution row always has execute_at"),
                    attempt: row.attempt_index as u32,
                    queue_id: row.queue_id,
                },
                JobExecutionRowState::Running => JobStatus::Running {
                    attempt: row.attempt_index as u32,
                    alive_at: row.alive_at,
                    queue_id: row.queue_id,
                },
            });
        }

        // Row absent ⇒ the job is terminal (or never existed). The terminal
        // DELETE commits in the same op as the terminal entity events
        // (`src/dispatcher.rs` `complete_job` / `fail_job`), so this
        // two-step read cannot observe a torn state.
        let job = self.repo.find_by_id(self.id).await?;
        match job.terminal_state() {
            Some(JobTerminalState::Completed) => Ok(JobStatus::Completed {
                queue_id: job.queue_id,
            }),
            Some(JobTerminalState::Errored) => Ok(JobStatus::Errored {
                error: job.last_error().unwrap_or_default(),
                queue_id: job.queue_id,
            }),
            None => Err(JobError::JobExecutionError(format!(
                "job {} has no execution row but its entity is not terminal",
                self.id
            ))),
        }
    }

    /// Read back the job's committed execution state, decoded as `S`.
    ///
    /// Contract 5 (honest absence): returns `Ok(None)` when no state has been
    /// written yet or the execution row is gone (terminal job).
    #[instrument(
        name = "job.handle.execution_state",
        skip(self),
        fields(id = %self.id)
    )]
    pub async fn execution_state<S: DeserializeOwned>(&self) -> Result<Option<S>, JobError> {
        // Read-side mirror of the write in `CurrentJob::update_execution_state_in_op`
        // (src/current.rs).
        let row = sqlx::query!(
            r#"SELECT execution_state_json FROM job_executions WHERE id = $1"#,
            self.id as JobId,
        )
        .fetch_optional(self.repo.pool())
        .await?;
        match row.and_then(|r| r.execution_state_json) {
            Some(json) => serde_json::from_value(json)
                .map(Some)
                .map_err(JobError::CouldNotDeserializeExecutionState),
            None => Ok(None),
        }
    }

    /// Decode the job's stored configuration payload (entity proxy).
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if the job never existed.
    #[instrument(name = "job.handle.config", skip(self), fields(id = %self.id))]
    pub async fn config<T: DeserializeOwned>(&self) -> Result<T, JobError> {
        let job = self.repo.find_by_id(self.id).await?;
        // Reuses the `BadConfig` variant for the decode failure.
        job.config().map_err(JobError::CouldNotSerializeConfig)
    }

    /// Decode the return value the runner attached via
    /// [`CurrentJob::set_result`](crate::CurrentJob::set_result), if any
    /// (entity proxy).
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if the job never existed.
    #[instrument(
        name = "job.handle.return_value",
        skip(self),
        fields(id = %self.id)
    )]
    pub async fn return_value<T: DeserializeOwned>(&self) -> Result<Option<T>, JobError> {
        let job = self.repo.find_by_id(self.id).await?;
        // Reuses the `BadResult` variant for the decode failure.
        job.return_value()
            .map_err(JobError::CouldNotSerializeResult)
    }

    /// Block until the job reaches a terminal state (completed or errored)
    /// and return the outcome together with any result value the runner
    /// attached via [`CurrentJob::set_result`](crate::CurrentJob::set_result).
    ///
    /// The timeout is REQUIRED: the await is structurally bounded.
    /// Wait-forever is expressed only by an explicit caller loop that
    /// re-awaits on [`JobError::TimedOut`] — each re-await re-registers a
    /// fresh waiter, which is also what makes a lost in-memory notification
    /// self-heal instead of wedging.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::RouterNotStarted`] if called before
    /// [`Jobs::start_poll`](crate::Jobs::start_poll).
    /// Returns [`JobError::Find`] if the job does not exist.
    /// Returns [`JobError::TimedOut`] if the timeout elapses first.
    /// Returns [`JobError::AwaitCompletionShutdown`] if the notification
    /// channel is dropped (e.g., during shutdown) before delivering the
    /// terminal state.
    #[instrument(
        name = "job.handle.await_completion",
        skip(self),
        fields(id = %self.id)
    )]
    pub async fn await_completion(&self, timeout: Duration) -> Result<JobOutcome, JobError> {
        tokio::time::timeout(timeout, self.wait_for_outcome())
            .await
            .map_err(|_| JobError::TimedOut(self.id))?
    }

    /// Unbounded wait shared by [`Self::await_completion`] and
    /// [`JobHandles::await_all`], which each wrap it in their own timeout.
    ///
    /// Cancel-safe (contract 3): dropping this future drops the oneshot
    /// receiver, which unsubscribes the waiter (the router's sweep prunes
    /// closed senders); a later re-registered wait resolves normally.
    pub(crate) async fn wait_for_outcome(&self) -> Result<JobOutcome, JobError> {
        // Router-started check FIRST (before the fail-fast find) so awaiting
        // before `Jobs::start_poll` is a `RouterNotStarted` error, not a
        // panic. Registering before the find is race-free: the waiter
        // manager checks the DB for already-terminal jobs at registration.
        let rx = self
            .router
            .try_wait_for_terminal(self.id)
            .ok_or(JobError::RouterNotStarted)?;
        // Fail fast if the job doesn't exist — avoids a silent park in the
        // waiter manager for a JobId that will never resolve.
        self.repo.find_by_id(self.id).await?;
        let state = rx
            .await
            .map_err(|_| JobError::AwaitCompletionShutdown(self.id))?;
        // Reload the entity to retrieve any result value set by the runner.
        let job = self.repo.find_by_id(self.id).await?;
        Ok(JobOutcome::new(state, job.raw_return_value().cloned()))
    }
}

/// An ordered collection of [`JobHandle`]s.
///
/// Mint one with [`Jobs::handles`](crate::Jobs::handles) or collect handles
/// with [`FromIterator`]. Contract 2 (order preservation) holds for every
/// batch method: results align positionally with the handles.
pub struct JobHandles(Vec<JobHandle>);

impl JobHandles {
    /// Block until every job reaches a terminal state and return all
    /// outcomes, positionally aligned with the handles (contract 2).
    ///
    /// Each job is awaited concurrently; the call resolves once **all** jobs
    /// have finished. An empty collection returns an empty `Vec` immediately.
    /// The timeout is REQUIRED and bounds the whole batch; on expiry the
    /// error carries the first handle's id.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::RouterNotStarted`] if called before
    /// [`Jobs::start_poll`](crate::Jobs::start_poll).
    /// Returns [`JobError::Find`] if any job in the batch does not exist.
    /// Returns [`JobError::TimedOut`] if the timeout elapses before every job
    /// reaches a terminal state.
    /// Returns [`JobError::AwaitCompletionShutdown`] if the notification
    /// channel is dropped (e.g., during shutdown) before all jobs resolve.
    #[instrument(name = "job.handles.await_all", skip(self), fields(count = self.0.len()))]
    pub async fn await_all(&self, timeout: Duration) -> Result<Vec<JobOutcome>, JobError> {
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        let first_id = self.0[0].id;
        let futs: Vec<_> = self.0.iter().map(|h| h.wait_for_outcome()).collect();
        let results = tokio::time::timeout(timeout, futures::future::join_all(futs))
            .await
            .map_err(|_| JobError::TimedOut(first_id))?;
        results.into_iter().collect()
    }

    /// Snapshot the live status of every job, positionally aligned with the
    /// handles (contract 2).
    #[instrument(name = "job.handles.statuses", skip(self), fields(count = self.0.len()))]
    pub async fn statuses(&self) -> Result<Vec<JobStatus>, JobError> {
        let futs: Vec<_> = self.0.iter().map(|h| h.status()).collect();
        futures::future::join_all(futs).await.into_iter().collect()
    }
}

impl FromIterator<JobHandle> for JobHandles {
    fn from_iter<I: IntoIterator<Item = JobHandle>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for JobHandles {
    type Item = JobHandle;
    type IntoIter = std::vec::IntoIter<JobHandle>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Extend<JobHandle> for JobHandles {
    fn extend<I: IntoIterator<Item = JobHandle>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl std::ops::Deref for JobHandles {
    type Target = [JobHandle];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
