//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use std::{collections::HashSet, marker::PhantomData, sync::Arc};
use tracing::instrument;

use super::{
    Job, JobId,
    entity::{JobType, NewJob},
    error::JobError,
    execution_hooks::{ExecutionInsertHook, NewExecutionRow},
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
}

impl<Config> JobSpec<Config> {
    pub fn new(id: impl Into<JobId>, config: Config) -> Self {
        Self {
            id: id.into(),
            config,
            schedule_at: None,
            queue_id: None,
            dedup_key: None,
        }
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
    /// running) execution already holds `(job_type, key)`, [`JobSpawner::spawn_all`]/
    /// [`JobSpawner::spawn_all_in_op`] silently drop this spec — no `jobs`
    /// row, no execution row — and report it via
    /// [`BulkSpawnResult::deduped`] rather than minting a duplicate. The key
    /// becomes respawnable the instant the holder goes terminal; this is the
    /// SAME `(job_type, unique_key)` live-window enforced for keyed jobs
    /// (`idx_job_executions_job_type_unique_key`), just opted into from the
    /// bulk/regular spawn path instead of [`crate::KeyedJobSpawner::spawn`].
    ///
    /// Unlike [`crate::KeyedJobSpawner::spawn`], a collision here is NOT
    /// resolved to a handle on the live holder — it is dropped outright, no
    /// lookup performed. This is a **skip**, not a **resolve**.
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

/// Return value of [`JobSpawner::spawn_all`]/[`JobSpawner::spawn_all_in_op`].
///
/// `jobs.len() + deduped.len() == ` the number of input specs whenever no
/// spec used [`JobSpec::dedup_key`] (today's behavior, unchanged); with
/// dedup keys in play a caller can get FEWER jobs than specs given — this is
/// deliberate (see [`JobSpec::dedup_key`]), not a partial failure.
#[derive(Default)]
pub struct BulkSpawnResult {
    /// The jobs actually created, in spec order (excluding deduped specs).
    pub jobs: Vec<Job>,
    /// The `id` of each spec that was silently dropped because its
    /// `dedup_key` was already held by a LIVE execution, or duplicated an
    /// earlier spec's key within the same call. No `jobs` row, no execution
    /// row exists for these ids — do not [`crate::Jobs::handle`] them
    /// expecting a live job.
    pub deduped: Vec<JobId>,
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
    job_type: JobType,
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
        job_type: JobType,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
        poller_ref: PollerHandle,
    ) -> Self {
        Self {
            repo,
            job_type,
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

    /// Create and spawn a job described by `spec`, in a single atomic
    /// operation. The single-spawn entry point every other `spawn*`
    /// convenience method on this spawner ultimately delegates to (via
    /// [`Self::spawn_spec_in_op`]). See there for the dedup semantics.
    #[instrument(
        name = "job_spawner.spawn_spec",
        skip(self, spec),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_spec(&self, spec: JobSpec<Config>) -> Result<Option<Job>, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let job = self.spawn_spec_in_op(&mut op, spec).await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job described by `spec`, as part of an existing
    /// atomic operation. The single-spawn core every other `spawn*`
    /// convenience method delegates to, directly or transitively (through
    /// [`Self::spawn_at_in_op`] / [`Self::spawn_at_with_queue_id_in_op`]).
    ///
    /// Honors [`JobSpec::dedup_key`] exactly like [`Self::spawn_all_in_op`]
    /// does per spec: `Ok(None)` if `spec`'s key is already held by a LIVE
    /// execution -- no `jobs` row, no execution row created -- `Ok(Some(job))`
    /// otherwise. Every other `spawn*` method builds a `JobSpec` with
    /// `dedup_key: None`, for which this can never return `None`; those call
    /// sites `.expect(...)` that invariant rather than threading `Option`
    /// through every public signature that never sets a dedup key.
    #[instrument(
        name = "job_spawner.spawn_spec_in_op",
        skip(self, op, spec),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_spec_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        spec: JobSpec<Config>,
    ) -> Result<Option<Job>, JobError> {
        let schedule_at = spec
            .schedule_at
            .unwrap_or_else(|| op.maybe_now().unwrap_or_else(|| self.clock.now()));

        if let Some(key) = &spec.dedup_key {
            let live_keys = self
                .repo
                .lock_and_check_live_keys_in_op(op, &self.job_type, std::slice::from_ref(key))
                .await?;
            if live_keys.contains(key) {
                return Ok(None);
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

        Ok(Some(job))
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
    ) -> Result<Job, JobError> {
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
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<Job, JobError> {
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
    ) -> Result<Job, JobError> {
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
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        self.spawn_spec_in_op(op, JobSpec::new(id, config).schedule_at(schedule_at))
            .await
            .map(|job| job.expect("a JobSpec without dedup_key is never deduped"))
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
    ) -> Result<Job, JobError> {
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
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
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
    ) -> Result<Job, JobError> {
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
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
        queue_id: impl Into<String> + Send,
    ) -> Result<Job, JobError> {
        self.spawn_spec_in_op(
            op,
            JobSpec::new(id, config)
                .schedule_at(schedule_at)
                .queue_id(queue_id),
        )
        .await
        .map(|job| job.expect("a JobSpec without dedup_key is never deduped"))
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
    pub async fn spawn_all(
        &self,
        specs: Vec<JobSpec<Config>>,
    ) -> Result<BulkSpawnResult, JobError> {
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
    /// A spec whose `dedup_key` is already held by a LIVE execution — or that
    /// repeats an earlier spec's key within this same call — creates NO
    /// `jobs` row and NO execution row; see [`JobSpec::dedup_key`] and
    /// [`BulkSpawnResult`]. Dedup resolution (`JobRepo::lock_and_check_live_keys_in_op`)
    /// runs BEFORE any `jobs` row is built, deliberately: `job_executions.id`
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
        op: &mut impl es_entity::AtomicOperation,
        specs: Vec<JobSpec<Config>>,
    ) -> Result<BulkSpawnResult, JobError> {
        tracing::Span::current().record("count", specs.len());
        if specs.is_empty() {
            return Ok(BulkSpawnResult::default());
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

        let mut seen_keys: HashSet<String> = HashSet::new();
        let mut deduped: Vec<JobId> = Vec::new();
        let mut surviving = Vec::with_capacity(specs.len());
        for spec in specs {
            if let Some(key) = &spec.dedup_key
                && (live_keys.contains(key) || !seen_keys.insert(key.clone()))
            {
                deduped.push(spec.id);
                continue;
            }
            surviving.push(spec);
        }

        if surviving.is_empty() {
            return Ok(BulkSpawnResult {
                jobs: Vec::new(),
                deduped,
            });
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

        Ok(BulkSpawnResult { jobs, deduped })
    }
}
