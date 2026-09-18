//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
};
use tracing::instrument;

use super::{
    JobId,
    entity::{JobType, NewJob},
    error::JobError,
    execution_hooks::{ExecutionInsertHook, NewExecutionRow},
    handle::{JobHandle, JobHandles},
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    poller::PollerHandle,
    repo::JobRepo,
};

/// Describes a job to be created as part of a bulk [`JobSpawner::spawn_all`] call.
///
/// Use [`JobSpec::new`] to create a spec with just an id and config, then
/// chain [`JobSpec::schedule_at`], [`JobSpec::queue_id`], or
/// [`JobSpec::dedup_key`] for optional overrides. `queue_id` and `dedup_key`
/// are composable — set both to serialize a facility's cross-type work
/// (`queue_id`) while also collapsing repeat spawns for a still-live one
/// (`dedup_key`).
///
/// # Examples
///
/// ```ignore
/// let specs = vec![
///     JobSpec::new(JobId::new(), MyConfig { value: 1 }),
///     JobSpec::new(JobId::new(), MyConfig { value: 2 })
///         .schedule_at(future_time)
///         .queue_id("my-queue")
///         .dedup_key("my-queue"),
/// ];
/// spawner.spawn_all(specs).await?;
/// ```
pub struct JobSpec<Config> {
    pub id: JobId,
    pub config: Config,
    pub schedule_at: Option<DateTime<Utc>>,
    pub queue_id: Option<String>,
    pub dedup_key: Option<String>,
    /// A job to wake once this one is terminal — see [`Self::waiter`].
    pub waiter: Option<JobId>,
}

impl<Config> JobSpec<Config> {
    pub fn new(id: impl Into<JobId>, config: Config) -> Self {
        Self {
            id: id.into(),
            config,
            schedule_at: None,
            queue_id: None,
            dedup_key: None,
            waiter: None,
        }
    }

    /// Register `waiter` to be woken when the job this spec creates (or
    /// resolves to, under [`Self::dedup_key`]) reaches a terminal state:
    /// its `execute_at` is pulled forward to that instant if it is parked
    /// (pending, first attempt, scheduled later), under exactly
    /// [`crate::KeyedJobSpec::force_reschedule`]'s guards — never over a
    /// retry backoff, never a running row.
    ///
    /// The shape this exists for: a job spawns another inside its own
    /// transaction and ends with
    /// [`RescheduleAtWithOp`](crate::JobCompletion::RescheduleAtWithOp) on
    /// that same operation, so the spawn and the park commit together and
    /// the callee cannot be terminal before its waiter is parked. The
    /// deadline it parks with is the safety net: a wake is at-least-once in
    /// spirit, never a guarantee, so a caller re-entered by its deadline
    /// must read the callee's outcome and decide for itself. A waiter that
    /// parks with a plain [`RescheduleAt`](crate::JobCompletion::RescheduleAt)
    /// (its park written after its run returns) is still covered: a wake
    /// that finds it running marks the wait, and its park write lands the
    /// row due when it finds the mark.
    ///
    /// Under [`Self::dedup_key`], resolving to a live holder registers the
    /// waiter on the holder; a holder that went terminal between the live
    /// check and this registration is replaced by a new job the waiter is
    /// registered on, so the spec always yields something to wait for.
    ///
    /// Equivalent to spawning and then registering through the handle on
    /// the same op (e.g. [`crate::JobHandle::register_waiter_in_op`]); this
    /// form saves the hook lookup, the other lets the wait be decided after
    /// the spawn.
    pub fn waiter(mut self, waiter: impl Into<JobId>) -> Self {
        self.waiter = Some(waiter.into());
        self
    }

    pub fn schedule_at(mut self, schedule_at: DateTime<Utc>) -> Self {
        self.schedule_at = Some(schedule_at);
        self
    }

    pub fn queue_id(mut self, queue_id: impl Into<String>) -> Self {
        self.queue_id = Some(queue_id.into());
        self
    }

    /// Opt this spec into live-window dedup: if a LIVE (pending/parked/
    /// running) execution already holds `(job_type, key)`, this spec mints
    /// nothing of its own — no `jobs` row, no execution row — and the call
    /// yields a handle on the LIVE holder in its place, with
    /// [`JobHandle::created`] reading `false` to say so. The key becomes
    /// respawnable the instant the holder goes terminal; this is the SAME
    /// `(job_type, unique_key)` live-window enforced for keyed jobs
    /// (`idx_job_executions_job_type_unique_key`), just opted into from the
    /// bulk/regular spawn path instead of [`crate::KeyedJobSpawner::spawn`].
    ///
    /// Like [`crate::KeyedJobSpawner::spawn`], a collision resolves to the
    /// live holder. Coalescing does not extend that job's inputs.
    ///
    /// # Batching
    ///
    /// A dedup-keyed row is inserted INLINE, within the call, rather than
    /// buffered into the one-statement commit-time batch keyless rows get
    /// (`execution_hooks/insert.rs::ExecutionInsertHook::register`). That is
    /// deliberate and load-bearing: a transaction sees its own uncommitted
    /// writes, so a LATER call on the same `op` finds this row in its own
    /// live-check and resolves to it, which is what makes same-`op` dedup
    /// work at all.
    ///
    /// The cost is that dedup-keyed rows do not batch ACROSS calls. One
    /// [`JobSpawner::spawn_all_in_op`] carrying N keyed specs is still ONE
    /// insert; N separate [`JobSpawner::spawn_in_op`]/`spawn_spec_in_op`
    /// calls on one `op` are N inserts. Fan-out producers should pass the
    /// whole shard set to a single `spawn_all_in_op` rather than looping.
    ///
    /// Only safe for a producer that re-checks and re-spawns after the
    /// holder goes terminal — e.g. a sweep/reconcile loop that re-scans its
    /// trigger condition on every pass, so a spawn suppressed by a still-
    /// running holder is simply retried, cheaply, on the next pass. A
    /// one-shot producer that spawns once and never re-checks would lose
    /// the suppressed unit of work outright.
    pub fn dedup_key(mut self, dedup_key: impl Into<String>) -> Self {
        self.dedup_key = Some(dedup_key.into());
        self
    }
}

/// A handle for spawning jobs of a specific type.
///
/// Returned by [`crate::Jobs::add_initializer`]. The spawner encapsulates the job type
/// and provides type-safe job creation methods.
///
/// # Examples
///
/// ```ignore
/// // Registration returns a spawner
/// let spawner = jobs.add_initializer(MyInitializer);
///
/// // Use the spawner to create jobs
/// spawner.spawn(JobId::new(), MyConfig { value: 42 }).await?;
/// ```
#[derive(Clone)]
pub struct JobSpawner<Config> {
    repo: Arc<JobRepo>,
    handle_ops: Arc<crate::handle::HandleOps>,
    job_type: JobType,
    router: Arc<JobNotificationRouter>,
    clock: ClockHandle,
    notifier: Arc<JobEventNotifier>,
    /// Reaches this process's poller for the short-circuit spawn fast path
    /// once it exists. See [`PollerHandle`].
    poller_ref: PollerHandle,
    _phantom: PhantomData<Config>,
}

impl<Config> JobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        handle_ops: Arc<crate::handle::HandleOps>,
        job_type: JobType,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
        poller_ref: PollerHandle,
    ) -> Self {
        Self {
            repo,
            handle_ops,
            job_type,
            router,
            clock,
            notifier,
            poller_ref,
            _phantom: PhantomData,
        }
    }

    /// Returns the job type this spawner creates.
    pub fn job_type(&self) -> &JobType {
        &self.job_type
    }

    fn handle(&self, id: JobId) -> JobHandle {
        JobHandle::new(
            id,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            Arc::clone(&self.handle_ops),
            self.clock.clone(),
        )
    }

    /// Create and spawn a job described by `spec`, in a single atomic
    /// operation. The single-spawn entry point every other `spawn*`
    /// convenience method on this spawner ultimately delegates to (via
    /// [`Self::spawn_spec_in_op`]). See there for the dedup semantics.
    #[instrument(
        name = "job_spawner.spawn_spec",
        skip(self, spec),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_spec(&self, spec: JobSpec<Config>) -> Result<JobHandle, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let handle = self.spawn_spec_in_op(&mut op, spec).await?;
        op.commit().await?;
        Ok(handle)
    }

    /// Create and spawn a job described by `spec`, as part of an existing
    /// atomic operation. The single-spawn core every other `spawn*`
    /// convenience method delegates to, directly or transitively (through
    /// [`Self::spawn_at_in_op`] / [`Self::spawn_at_with_queue_id_in_op`]).
    ///
    /// Honors [`JobSpec::dedup_key`] exactly like [`Self::spawn_all_in_op`]
    /// does per spec: a key already held by a LIVE execution yields a handle
    /// on that holder -- no `jobs` row, no execution row created, and
    /// [`JobHandle::created`] reads `false` -- otherwise a handle on the job
    /// just created, with `created` reading `true`. Every other `spawn*`
    /// method builds a `JobSpec` with `dedup_key: None`, for which `created`
    /// is always `true`.
    #[instrument(
        name = "job_spawner.spawn_spec_in_op",
        skip(self, op, spec),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_spec_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        spec: JobSpec<Config>,
    ) -> Result<JobHandle, JobError> {
        let schedule_at = spec
            .schedule_at
            .unwrap_or_else(|| op.maybe_now().unwrap_or_else(|| self.clock.now()));

        if let Some(key) = &spec.dedup_key {
            let live_keys = self
                .repo
                .lock_and_check_live_keys_in_op(op, &self.job_type, std::slice::from_ref(key))
                .await?;
            // Only the holder's id is needed to answer the caller, so the
            // coalesced path costs no read of its own. A waiter is the one
            // thing that has to touch the holder: it locks the holder's row
            // to prove it is still live, and if it is not, the key is free
            // (this op holds its advisory lock) and a new job is created
            // for the waiter to wait on.
            if let Some(id) = live_keys.get(key) {
                let attached = match spec.waiter {
                    None => true,
                    Some(waiter) => !self
                        .handle_ops
                        .waiters
                        .register_waiters_on_live_in_op(op, &[*id], &[waiter])
                        .await?
                        .is_empty(),
                };
                if attached {
                    return Ok(self.handle(*id).with_created(false));
                }
            }
        }

        let mut builder = NewJob::builder();
        builder
            .id(spec.id)
            .job_type(self.job_type.clone())
            .config(spec.config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .queue_id(spec.queue_id.clone())
            .schedule_at(schedule_at);
        if let Some(key) = spec.dedup_key.clone() {
            builder.unique_key(key);
        }
        let new_job = builder.build().expect("Could not build new job");

        let job = self.repo.create_in_op(op, new_job).await?;
        if let Some(waiter) = spec.waiter {
            self.handle_ops
                .waiters
                .insert_waiters_in_op(op, &[job.id], &[waiter])
                .await?;
        }

        ExecutionInsertHook::register_one(
            op,
            &self.notifier,
            &self.poller_ref,
            &self.clock,
            NewExecutionRow {
                id: job.id,
                job_type: self.job_type.clone(),
                schedule_at,
                queue_id: spec.queue_id,
                unique_key: spec.dedup_key,
            },
        )
        .await?;

        Ok(self.handle(job.id).with_created(true))
    }

    /// Create and spawn a job for immediate execution.
    #[instrument(
        name = "job_spawner.spawn",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<JobHandle, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self.spawn_in_op(&mut op, id, config).await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<JobHandle, JobError> {
        let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
        self.spawn_at_in_op(op, id, config, schedule_at).await
    }

    /// Create and spawn a job for execution at a specific time.
    #[instrument(
        name = "job_spawner.spawn_at",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<JobHandle, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_at_in_op(&mut op, id, config, schedule_at)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job for execution at a specific time as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_at_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<JobHandle, JobError> {
        self.spawn_spec_in_op(op, JobSpec::new(id, config).schedule_at(schedule_at))
            .await
    }

    /// Create and spawn a job for immediate execution within a queue.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_with_queue_id",
        skip(self, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_with_queue_id(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        queue_id: impl Into<String> + Send,
    ) -> Result<JobHandle, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_with_queue_id_in_op(&mut op, id, config, queue_id)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job within a queue as part of an existing atomic operation.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_with_queue_id_in_op",
        skip(self, op, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_with_queue_id_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        queue_id: impl Into<String> + Send,
    ) -> Result<JobHandle, JobError> {
        let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
        self.spawn_at_with_queue_id_in_op(op, id, config, schedule_at, queue_id)
            .await
    }

    /// Create and spawn a job for execution at a specific time within a queue.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_at_with_queue_id",
        skip(self, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_with_queue_id(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
        queue_id: impl Into<String> + Send,
    ) -> Result<JobHandle, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self
            .spawn_at_with_queue_id_in_op(&mut op, id, config, schedule_at, queue_id)
            .await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job for execution at a specific time within a queue,
    /// as part of an existing atomic operation.
    ///
    /// At most one job per `queue_id` will run globally at any time.
    #[instrument(
        name = "job_spawner.spawn_at_with_queue_id_in_op",
        skip(self, op, config, queue_id),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_at_with_queue_id_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
        queue_id: impl Into<String> + Send,
    ) -> Result<JobHandle, JobError> {
        self.spawn_spec_in_op(
            op,
            JobSpec::new(id, config)
                .schedule_at(schedule_at)
                .queue_id(queue_id),
        )
        .await
    }

    /// Create and spawn multiple jobs in a single atomic operation.
    ///
    /// All jobs are created within a single transaction — either all succeed or all roll back.
    /// Each [`JobSpec`] can independently specify `schedule_at`, `queue_id`, and `dedup_key`.
    #[instrument(
        name = "job_spawner.spawn_all",
        skip(self, specs),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_all(&self, specs: Vec<JobSpec<Config>>) -> Result<JobHandles, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let result = self.spawn_all_in_op(&mut op, specs).await?;
        op.commit().await?;
        Ok(result)
    }

    /// Create and spawn multiple jobs as part of an existing atomic operation.
    ///
    /// Each [`JobSpec`] can independently specify `schedule_at`, `queue_id`, and
    /// `dedup_key`. Internally uses batch inserts for both the job entities and
    /// `job_executions` rows.
    ///
    /// Returns one [`JobHandle`] per input spec, positionally aligned with
    /// `specs` (contract 2), so the result zips straight back against the
    /// inputs. A spec whose `dedup_key` is already held by a LIVE execution —
    /// or that repeats an earlier spec's key within this same call — creates
    /// NO `jobs` row and NO execution row, and its position instead carries a
    /// handle on the holder, with [`JobHandle::created`] reading `false`.
    /// That flag is the only thing separating the two cases; see
    /// [`JobSpec::dedup_key`].
    ///
    /// The SAME id can therefore appear at more than one position — two specs
    /// sharing a key both resolve to the first one's job — so this is a list
    /// aligned to the specs, not a set of distinct jobs.
    ///
    /// Dedup resolution (`JobRepo::lock_and_check_live_keys_in_op`) runs
    /// BEFORE any `jobs` row is built, deliberately: `job_executions.id`
    /// references `jobs(id)`, so a deduped spec must never reach
    /// `create_all_in_op` at all, or its `jobs` row would outlive the
    /// decision to drop it.
    #[instrument(
        name = "job_spawner.spawn_all_in_op",
        skip(self, op, specs),
        fields(job_type = %self.job_type, count)
    )]
    pub async fn spawn_all_in_op(
        &self,
        op: &mut (impl es_entity::AtomicOperation + ?Sized),
        specs: Vec<JobSpec<Config>>,
    ) -> Result<JobHandles, JobError> {
        tracing::Span::current().record("count", specs.len());
        if specs.is_empty() {
            return Ok(JobHandles::default());
        }

        let default_schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());

        // Resolve every dedup key up front — locked, then checked against
        // the live table — before a single `jobs` row is created. See this
        // method's doc and `JobRepo::lock_and_check_live_keys_in_op`.
        let requested_dedup_keys: Vec<String> =
            specs.iter().filter_map(|s| s.dedup_key.clone()).collect();
        let live_keys = self
            .repo
            .lock_and_check_live_keys_in_op(op, &self.job_type, &requested_dedup_keys)
            .await?;

        // One entry per input spec, in spec order: the id the caller ends up
        // holding, and whether THIS spec is the one that minted it. `seen`
        // is the intra-call seen-set — a key resolved once (to a
        // pre-existing holder or to a surviving spec) resolves the same way
        // for every later spec repeating it.
        //
        // A waiter on a pre-existing holder locks the holder's row to prove
        // it is still live; a holder found terminal is replaced by the spec
        // itself, so the waiter always gets a job to wait on. Waiters on
        // jobs this call creates are plain inserts, batched after the rows
        // exist.
        //
        // Liveness is a property of the KEY, not of the spec, so every proof
        // this batch needs is taken in ONE locking statement before any spec
        // is resolved. That is what lets a key's specs agree: all of them
        // see the same answer, and the `FOR SHARE` taken here is held for
        // the rest of `op`, so a later spec repeating the key can attach
        // with a plain insert and still be covered. Registering one spec at
        // a time instead would both leave the second spec of a key
        // unprotected (the first, if waiter-less, proves nothing) and take
        // the locks out of the crate's `(queue_id, id)` order.
        let mut proof_callees: Vec<JobId> = Vec::new();
        let mut proof_waiters: Vec<JobId> = Vec::new();
        for spec in &specs {
            if let (Some(key), Some(waiter)) = (&spec.dedup_key, spec.waiter)
                && let Some(&id) = live_keys.get(key)
            {
                proof_callees.push(id);
                proof_waiters.push(waiter);
            }
        }
        let attached: HashSet<JobId> = self
            .handle_ops
            .waiters
            .register_waiters_on_live_in_op(op, &proof_callees, &proof_waiters)
            .await?
            .into_iter()
            .collect();
        let proven: HashSet<JobId> = proof_callees.into_iter().collect();

        let mut seen: HashMap<String, JobId> = HashMap::new();
        let mut resolved: Vec<(JobId, bool)> = Vec::with_capacity(specs.len());
        let mut surviving = Vec::with_capacity(specs.len());
        let mut waiter_callees: Vec<JobId> = Vec::new();
        let mut waiters: Vec<JobId> = Vec::new();
        for spec in specs {
            if let Some(key) = &spec.dedup_key {
                if let Some(&id) = seen.get(key) {
                    // Waits on a pre-existing holder were all registered
                    // under its liveness proof above; only ids this batch
                    // creates still need one here.
                    if let Some(waiter) = spec.waiter
                        && !proven.contains(&id)
                    {
                        waiter_callees.push(id);
                        waiters.push(waiter);
                    }
                    resolved.push((id, false));
                    continue;
                }
                if let Some(&id) = live_keys.get(key)
                    && (!proven.contains(&id) || attached.contains(&id))
                {
                    seen.insert(key.clone(), id);
                    resolved.push((id, false));
                    continue;
                }
                seen.insert(key.clone(), spec.id);
            }
            if let Some(waiter) = spec.waiter {
                waiter_callees.push(spec.id);
                waiters.push(waiter);
            }
            resolved.push((spec.id, true));
            surviving.push(spec);
        }

        let mut new_jobs = Vec::with_capacity(surviving.len());
        let mut schedule_times = Vec::with_capacity(surviving.len());
        let mut queue_ids: Vec<Option<String>> = Vec::with_capacity(surviving.len());
        let mut dedup_keys: Vec<Option<String>> = Vec::with_capacity(surviving.len());

        for spec in surviving {
            let schedule_at = spec.schedule_at.unwrap_or(default_schedule_at);
            schedule_times.push(schedule_at);

            let mut builder = NewJob::builder();
            builder
                .id(spec.id)
                .job_type(self.job_type.clone())
                .config(spec.config)?
                .tracing_context(es_entity::context::TracingContext::current())
                .queue_id(spec.queue_id.clone())
                .schedule_at(schedule_at);
            if let Some(key) = spec.dedup_key.clone() {
                builder.unique_key(key);
            }
            let new_job = builder.build().expect("Could not build new job");
            new_jobs.push(new_job);
            queue_ids.push(spec.queue_id);
            dedup_keys.push(spec.dedup_key);
        }

        let jobs = self.repo.create_all_in_op(op, new_jobs).await?;

        let rows: Vec<NewExecutionRow> = jobs
            .iter()
            .zip(&schedule_times)
            .zip(&queue_ids)
            .zip(&dedup_keys)
            .map(
                |(((job, schedule_at), queue_id), unique_key)| NewExecutionRow {
                    id: job.id,
                    job_type: self.job_type.clone(),
                    schedule_at: *schedule_at,
                    queue_id: queue_id.clone(),
                    unique_key: unique_key.clone(),
                },
            )
            .collect();
        ExecutionInsertHook::register(op, &self.notifier, &self.poller_ref, &self.clock, rows)
            .await?;
        self.handle_ops
            .waiters
            .insert_waiters_in_op(op, &waiter_callees, &waiters)
            .await?;

        // Handles are minted from ids alone, so a coalesced position costs no
        // read of its own — the holder is never loaded just to be returned.
        Ok(resolved
            .into_iter()
            .map(|(id, created)| self.handle(id).with_created(created))
            .collect())
    }
}
