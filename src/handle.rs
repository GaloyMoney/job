//! Per-job capability handles minted by [`Jobs`](crate::Jobs): load a
//! point-in-time [`JobSnapshot`] (runtime status + typed committed state,
//! config, and return value) and await completion of jobs you did not run
//! yourself.

use es_entity::clock::ClockHandle;
use serde::de::DeserializeOwned;
use tracing::instrument;

use std::{sync::Arc, time::Duration};

use crate::{
    JobId, JobType, error::JobError, notification_router::JobNotificationRouter,
    notifier::JobEventNotifier, outcome::JobOutcome, poller::PollerHandle, repo::JobRepo,
    snapshot::JobSnapshot, waiters::JobWaiters,
};

/// The service-side wiring a handle needs in order to ACT on its job rather
/// than merely observe it: registering a wait and pulling the row forward
/// both write, and a write that moves a row has to announce it exactly as a
/// spawn does. Behind one `Arc` so a handle stays cheap to mint in bulk --
/// a fan-out that mints tens of thousands pays one pointer, not three.
pub(crate) struct HandleOps {
    pub(crate) waiters: JobWaiters,
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) poller_ref: PollerHandle,
}

/// A minted, cloneable per-job capability: the public way to observe and
/// await a job you did not run yourself.
///
/// Obtain one from [`Jobs::handle`](crate::Jobs::handle),
/// [`Jobs::handles`](crate::Jobs::handles), or from any `spawn*` method on
/// any spawner flavor — every one of them yields a `JobHandle`, or a
/// [`JobHandles`] for the multi-spec forms.
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
/// 6. **Spawn handle identity:** a `spawn*` call that resolves to an
///    existing job rather than creating one returns a handle on the
///    PERSISTED job, not on the spec that was suppressed — for keyed and
///    plain dedup, the still-LIVE job holding the key; for resident, the job
///    that exists at all (possibly long-terminal, though a resident job
///    never actually reaches terminal — see
///    [`crate::ResidentJobCompletion`]). Keyed and resident generate the id
///    internally in the first place. Either way, callers read the id back
///    from the handle and ask [`created`](Self::created) which case they got.
#[derive(Clone)]
pub struct JobHandle {
    id: JobId,
    created: bool,
    pulled_forward: bool,
    repo: Arc<JobRepo>,
    router: Arc<JobNotificationRouter>,
    ops: Arc<HandleOps>,
    clock: ClockHandle,
}

impl JobHandle {
    /// A handle that makes no claim about having created anything —
    /// [`Jobs::handle`](crate::Jobs::handle)/[`Jobs::handles`](crate::Jobs::handles)
    /// mint these, and [`created`](Self::created) reads `false` on them,
    /// which is the truthful answer: minting a handle creates no job.
    pub(crate) fn new(
        id: JobId,
        repo: Arc<JobRepo>,
        router: Arc<JobNotificationRouter>,
        ops: Arc<HandleOps>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            id,
            created: false,
            pulled_forward: false,
            repo,
            router,
            ops,
            clock,
        }
    }

    /// Stamp the spawn disposition onto a freshly minted handle. Spawner-only.
    pub(crate) fn with_created(mut self, created: bool) -> Self {
        self.created = created;
        self
    }

    /// Spawner-only: keyed spawn resolves `pulled_forward` after the fact,
    /// once `pull_forward_in_op` reports which holders actually moved.
    pub(crate) fn set_pulled_forward(&mut self, pulled_forward: bool) {
        self.pulled_forward = pulled_forward;
    }

    /// The id of the job this handle observes.
    pub fn id(&self) -> JobId {
        self.id
    }

    /// Run this job no later than `at`, if it is parked and eligible:
    /// `pending`, on its first attempt, and currently scheduled later than
    /// `at`. The [`Jobs::pull_forward_in_op`](crate::Jobs::pull_forward_in_op)
    /// twin for a caller already holding a handle; see there for the full
    /// semantics. Returns whether the row moved.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the write fails.
    #[instrument(name = "job.handle.pull_forward_in_op", skip(self, op), fields(id = %self.id))]
    pub async fn pull_forward_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, JobError> {
        let moved = self
            .ops
            .waiters
            .pull_forward_ids_in_op(op, &[self.id], at)
            .await?;
        let Some((_, job_type)) = moved.into_iter().next() else {
            return Ok(false);
        };
        self.announce(op, job_type, at).await?;
        Ok(true)
    }

    /// Register `waiter` to be woken when THIS job reaches a terminal state,
    /// with the semantics of [`JobSpec::waiter`](crate::JobSpec::waiter).
    /// `Ok(true)` if this job is live and the wait was registered;
    /// `Ok(false)` if it is already terminal, in which case nothing is
    /// written and the caller should read the outcome ([`Self::load`])
    /// instead of parking.
    ///
    /// A job spawned on this same `op` (`spawn_in_op` / `spawn_all_in_op`,
    /// keyless or not) is live by construction and is attached; the wait
    /// and the spawn commit together.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the write fails.
    #[instrument(name = "job.handle.register_waiter_in_op", skip(self, op, waiter), fields(id = %self.id, waiter))]
    pub async fn register_waiter_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        waiter: impl Into<JobId>,
    ) -> Result<bool, JobError> {
        let waiter: JobId = waiter.into();
        tracing::Span::current().record("waiter", tracing::field::display(waiter));
        Ok(!self
            .ops
            .waiters
            .register_waiters_in_op(op, &[self.id], &[waiter])
            .await?
            .is_empty())
    }

    /// The two signals a spawn fires, for a row a pull-forward just made
    /// due: the `ExecutionReady` notify for its type (a peer poller may be
    /// asleep on a deadline computed before this write) and, when the target
    /// is already due, this process's claim demand.
    async fn announce(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        job_type: JobType,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), JobError> {
        self.ops
            .notifier
            .execution_ready_in_op(op, &job_type)
            .await?;
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        if at <= now
            && let Some(poller) = self.ops.poller_ref.get().and_then(|w| w.upgrade())
        {
            poller.register_claim_demand(op, &job_type, 1);
        }
        Ok(())
    }

    /// `true` if the `spawn*` call that produced this handle CREATED the job
    /// it points at; `false` otherwise.
    ///
    /// This is the only thing separating "I created this" from "this already
    /// existed": both cases yield an equally usable handle. Branch on it
    /// before performing first-time side effects alongside the spawn, in the
    /// same `op`.
    ///
    /// # What it returns on every path that can produce a handle
    ///
    /// | produced by | `created()` |
    /// |---|---|
    /// | any `spawn*` that minted a new job | `true` |
    /// | a `spawn*` that resolved onto a live `dedup_key`/keyed holder | `false` |
    /// | [`ResidentJobSpawner::spawn`](crate::ResidentJobSpawner::spawn) onto the job that already exists | `false` |
    /// | [`Jobs::handle`](crate::Jobs::handle) / [`handles`](crate::Jobs::handles) / [`keyed_handle`](crate::Jobs::keyed_handle) / [`keyed_handles`](crate::Jobs::keyed_handles) / [`resident_handle`](crate::Jobs::resident_handle) / [`keyed_handle_in_op`](crate::Jobs::keyed_handle_in_op) / [`keyed_handles_in_op`](crate::Jobs::keyed_handles_in_op) / [`resident_handle_in_op`](crate::Jobs::resident_handle_in_op) | `false` |
    /// | [`Clone`] of any of the above | whatever the source said |
    ///
    /// **Read `false` as "this call did not create the job", not as "a spawn
    /// found it already live".** Those coincide on the spawn paths but not on
    /// the lookup paths: a handle obtained by lookup was not created by
    /// anybody *in that call*, so it reports `false` for a different reason
    /// than a coalesced spawn does — no spawn was attempted at all. Code that
    /// branches on `created()` to decide whether to run first-time setup is
    /// correct either way (a lookup handle never triggers it), but code that
    /// reads `false` as evidence that a key was contended is not.
    pub fn created(&self) -> bool {
        self.created
    }

    /// `true` if the `spawn*` call that produced this handle moved the
    /// holder's `execute_at` EARLIER, to the time the spec asked for.
    ///
    /// Only ever set for a
    /// [`KeyedJobSpec::force_reschedule`](crate::KeyedJobSpec::force_reschedule)
    /// spec that resolved to a holder scheduled later than that and eligible
    /// to be woken (see
    /// [`KeyedJobSpawner::spawn_all_in_op`](crate::KeyedJobSpawner::spawn_all_in_op)).
    /// Always `false` when [`created`](Self::created) is `true` — a job
    /// created here already carries the time it asked for — and always
    /// `false` for every non-keyed flavor, which has no wake to request.
    ///
    /// `created() == false && pulled_forward() == false` is the ordinary
    /// resolve-to-holder outcome: the job was already live and nothing was
    /// changed.
    pub fn pulled_forward(&self) -> bool {
        self.pulled_forward
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
        rx.await
            .map_err(|_| JobError::AwaitCompletionShutdown(self.id))
    }
}

/// An ordered collection of [`JobHandle`]s.
///
/// Mint one with [`Jobs::handles`](crate::Jobs::handles) or collect handles
/// with [`FromIterator`]. Contract 2 (order preservation) holds for every
/// batch method: results align positionally with the handles.
#[derive(Default)]
pub struct JobHandles(Vec<JobHandle>);

const AWAIT_ALL_CHUNK: usize = 1000;

impl JobHandles {
    /// Register `waiter` to be woken when the jobs in this collection reach
    /// a terminal state, and report which of them were still live and so
    /// actually attached. A handle absent from the result is already
    /// terminal: nothing was written for it, and its outcome is there to be
    /// read ([`JobHandle::load`]) rather than waited on.
    ///
    /// **This is the durable counterpart to [`Self::await_all`], and the two
    /// are not interchangeable.** `await_all` is a *continuation*: the
    /// calling task blocks, resumes on the same line, and has the outcomes
    /// in hand -- so a job that calls it keeps its slot and its task alive
    /// for the whole wait. Registering a wait is a *restart*: the caller is
    /// expected to park itself (return
    /// [`JobCompletion::RescheduleAt`](crate::JobCompletion::RescheduleAt)
    /// with a fallback deadline), which releases its slot, and to be run
    /// again FROM THE TOP when a callee finishes. Nothing is handed back on
    /// re-entry, so a job converting from one to the other has to persist
    /// what it spawned ([`CurrentJob::update_execution_state_in_op`](crate::CurrentJob::update_execution_state_in_op))
    /// and reload the outcomes itself. That restructuring is the real cost
    /// of the swap; it is also what makes a wide fan-out affordable.
    ///
    /// The wake is per-callee, not "all of them": the waiter is pulled
    /// forward when the FIRST of these finishes, and should re-check what is
    /// still outstanding and park again if it is not done.
    ///
    /// A callee spawned on this same `op` (`spawn_in_op` / `spawn_all_in_op`,
    /// keyless or not) is live by construction and is attached; the wait
    /// and the spawn commit together.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the write fails.
    #[instrument(
        name = "job.handles.register_waiter_in_op",
        skip(self, op, waiter),
        fields(count = self.0.len(), waiter)
    )]
    pub async fn register_waiter_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        waiter: impl Into<JobId>,
    ) -> Result<Vec<JobId>, JobError> {
        let waiter: JobId = waiter.into();
        tracing::Span::current().record("waiter", tracing::field::display(waiter));
        if self.0.is_empty() {
            return Ok(Vec::new());
        }
        let callees: Vec<JobId> = self.0.iter().map(|h| h.id).collect();
        let waiters = vec![waiter; callees.len()];
        self.0[0]
            .ops
            .waiters
            .register_waiters_in_op(op, &callees, &waiters)
            .await
    }

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
        let repo = &self.0[0].repo;
        let router = &self.0[0].router;
        let ids: Vec<JobId> = self.0.iter().map(|h| h.id).collect();

        let mut rxs = Vec::with_capacity(ids.len());
        for id in &ids {
            rxs.push(
                router
                    .try_wait_for_terminal(*id)
                    .ok_or(JobError::RouterNotStarted)?,
            );
        }

        for chunk in ids.chunks(AWAIT_ALL_CHUNK) {
            let found = repo.find_all::<crate::Job>(chunk).await?;
            if found.len() != chunk.len() {
                for id in chunk {
                    if !found.contains_key(id) {
                        repo.find_by_id(*id).await?;
                    }
                }
            }
        }

        let received = tokio::time::timeout(timeout, futures::future::join_all(rxs))
            .await
            .map_err(|_| JobError::TimedOut(first_id))?;
        ids.iter()
            .zip(received)
            .map(|(id, r)| r.map_err(|_| JobError::AwaitCompletionShutdown(*id)))
            .collect()
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

impl<'a> IntoIterator for &'a JobHandles {
    type Item = &'a JobHandle;
    type IntoIter = std::slice::Iter<'a, JobHandle>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
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
