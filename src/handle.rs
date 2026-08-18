//! Per-job capability handles minted by [`Jobs`](crate::Jobs): load a
//! point-in-time [`JobSnapshot`] (runtime status + typed committed state,
//! config, and return value) and await completion of jobs you did not run
//! yourself.

use es_entity::clock::ClockHandle;
use serde::de::DeserializeOwned;
use tracing::instrument;

use std::{sync::Arc, time::Duration};

use crate::{
    JobId, error::JobError, notification_router::JobNotificationRouter, outcome::JobOutcome,
    repo::JobRepo, snapshot::JobSnapshot,
};

/// A minted, cloneable per-job capability: the public way to observe and
/// await a job you did not run yourself.
///
/// Obtain one from [`Jobs::handle`](crate::Jobs::handle),
/// [`Jobs::handles`](crate::Jobs::handles),
/// [`ResidentJobSpawner::spawn`](crate::ResidentJobSpawner::spawn), or
/// [`KeyedJobSpawner::spawn`](crate::KeyedJobSpawner::spawn).
///
/// The handle is a capability, not a value: it holds no cached state and
/// exposes exactly two operations — [`load`](Self::load) for a point-in-time
/// [`JobSnapshot`], and [`await_completion`](Self::await_completion) for the
/// terminal outcome.
///
/// # Contracts
///
/// 1. **Live committed reads; no cached state on handles.** Every `load()` is
///    a fresh read.
/// 2. **Order preservation:** `await_all`/`load_all` results\[i\] ↔ handles\[i\].
/// 3. **Cancel safety:** dropping an in-flight `await_all`/`await_completion`
///    future (`select!` loser) leaks nothing and a re-registered wait
///    resolves — the dominant call pattern is `tokio::select!` against
///    [`CurrentJob::shutdown_requested`](crate::CurrentJob::shutdown_requested).
/// 4. **Torn-read-free `load()`:** the entity is authoritative for terminal
///    state — a terminal entity discards any execution row a concurrent
///    completion left visible mid-commit — so `load()` never reports
///    `Pending`/`Running` for a finished job, regardless of isolation level.
/// 5. **Honest absence:** [`JobSnapshot::execution_state`] ⇒ `Ok(None)` on
///    no-row/no-state; `load()` ⇒ `Err(Find)` only if the job never existed.
/// 6. **Keyed/resident handle identity:** both
///    [`KeyedJobSpawner::spawn`](crate::KeyedJobSpawner::spawn) and
///    [`ResidentJobSpawner::spawn`](crate::ResidentJobSpawner::spawn)
///    generate the job's id internally; on the duplicate path the returned
///    handle's id is the PERSISTED job's id, not a new one — for keyed, the
///    still-LIVE job holding the key; for resident, the job that exists at
///    all (possibly long-terminal, though a resident job never actually
///    reaches terminal — see [`crate::ResidentJobCompletion`]). Either way,
///    callers read the id back from the handle.
#[derive(Clone)]
pub struct JobHandle {
    id: JobId,
    repo: Arc<JobRepo>,
    router: Arc<JobNotificationRouter>,
    // Held so later stages can time-stamp reads consistently with the service
    // clock; no current method reads it.
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

    /// Load a point-in-time [`JobSnapshot`]: runtime status plus the committed
    /// execution state, config, and return value.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if the job never existed.
    #[instrument(name = "job.handle.load", skip(self), fields(id = %self.id))]
    pub async fn load(&self) -> Result<JobSnapshot, JobError> {
        self.repo.load_snapshot_by_id(self.id).await
    }

    /// Read back only the committed execution state, decoded as `S`.
    ///
    /// A cheap point-read: one single-row `SELECT` on `job_executions`, no
    /// entity hydration and no snapshot reconciliation — for hot poll loops
    /// (e.g. a caught-up barrier) that only need the execution state and would
    /// otherwise pay a full [`load`](Self::load) whose cost grows with the
    /// event log. Use `load()` when you need a consistent status/config view.
    ///
    /// Honest absence: `Ok(None)` on a missing row (never spawned, or already
    /// terminal) or unset state. Does not distinguish "never existed" from
    /// "terminal" — use [`load`](Self::load) if that matters.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::CouldNotDeserializeExecutionState`] if the stored
    /// state does not decode into `S`.
    #[instrument(
        name = "job.handle.execution_state",
        skip(self),
        fields(id = %self.id)
    )]
    pub async fn execution_state<S: DeserializeOwned>(&self) -> Result<Option<S>, JobError> {
        match self.repo.execution_state_json_by_id(self.id).await? {
            Some(json) => serde_json::from_value(json)
                .map(Some)
                .map_err(JobError::CouldNotDeserializeExecutionState),
            None => Ok(None),
        }
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

    /// Load a [`JobSnapshot`] for every job, positionally aligned with the
    /// handles (contract 2). Loads run concurrently.
    #[instrument(name = "job.handles.load_all", skip(self), fields(count = self.0.len()))]
    pub async fn load_all(&self) -> Result<Vec<JobSnapshot>, JobError> {
        let futs: Vec<_> = self.0.iter().map(|h| h.load()).collect();
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
