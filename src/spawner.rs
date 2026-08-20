//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use std::{marker::PhantomData, sync::Arc};
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
/// chain [`JobSpec::schedule_at`] or [`JobSpec::queue_id`] for optional
/// overrides. Bulk spawning is deliberately regular-only — there is no
/// keyed equivalent; use [`KeyedJobSpawner::spawn`](crate::KeyedJobSpawner::spawn)
/// one key at a time.
///
/// # Examples
///
/// ```ignore
/// let specs = vec![
///     JobSpec::new(JobId::new(), MyConfig { value: 1 }),
///     JobSpec::new(JobId::new(), MyConfig { value: 2 })
///         .schedule_at(future_time)
///         .queue_id("my-queue"),
/// ];
/// spawner.spawn_all(specs).await?;
/// ```
pub struct JobSpec<Config> {
    pub id: JobId,
    pub config: Config,
    pub schedule_at: Option<DateTime<Utc>>,
    pub queue_id: Option<String>,
}

impl<Config> JobSpec<Config> {
    pub fn new(id: impl Into<JobId>, config: Config) -> Self {
        Self {
            id: id.into(),
            config,
            schedule_at: None,
            queue_id: None,
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
    /// (§3.2 of the handoff this implements) once it exists. See
    /// [`PollerHandle`].
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
        self.create_job_internal(op, id.into(), config, schedule_at, None)
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
        self.create_job_internal(op, id.into(), config, schedule_at, Some(queue_id.into()))
            .await
    }

    /// Create and spawn multiple jobs in a single atomic operation.
    ///
    /// All jobs are created within a single transaction — either all succeed or all roll back.
    /// Each [`JobSpec`] can independently specify `schedule_at` and `queue_id`.
    #[instrument(
        name = "job_spawner.spawn_all",
        skip(self, specs),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_all(&self, specs: Vec<JobSpec<Config>>) -> Result<Vec<Job>, JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let jobs = self.spawn_all_in_op(&mut op, specs).await?;
        op.commit().await?;
        Ok(jobs)
    }

    /// Create and spawn multiple jobs as part of an existing atomic operation.
    ///
    /// Each [`JobSpec`] can independently specify `schedule_at` and `queue_id`.
    /// Internally uses batch inserts for both the job entities and `job_executions` rows.
    #[instrument(
        name = "job_spawner.spawn_all_in_op",
        skip(self, op, specs),
        fields(job_type = %self.job_type, count)
    )]
    pub async fn spawn_all_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        specs: Vec<JobSpec<Config>>,
    ) -> Result<Vec<Job>, JobError> {
        tracing::Span::current().record("count", specs.len());
        if specs.is_empty() {
            return Ok(Vec::new());
        }

        let default_schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());

        let mut new_jobs = Vec::with_capacity(specs.len());
        let mut schedule_times = Vec::with_capacity(specs.len());
        let mut queue_ids: Vec<Option<String>> = Vec::with_capacity(specs.len());

        for spec in specs {
            schedule_times.push(spec.schedule_at.unwrap_or(default_schedule_at));

            let mut builder = NewJob::builder();
            builder
                .id(spec.id)
                .job_type(self.job_type.clone())
                .config(spec.config)?
                .tracing_context(es_entity::context::TracingContext::current())
                .queue_id(spec.queue_id.clone());
            let new_job = builder.build().expect("Could not build new job");
            new_jobs.push(new_job);
            queue_ids.push(spec.queue_id);
        }

        let mut jobs = self.repo.create_all_in_op(op, new_jobs).await?;

        // `unique_key` is always NULL here: keyed and bulk spawning are
        // disjoint APIs (`JobSpec` deliberately carries no unique_key — see
        // `KeyedJobSpawner::spawn` for the keyed path).
        //
        // Head-swap kernel, insert half (handoff addendum §8.2a): one
        // `ExecutionInsertHook` registration for the whole batch, merged
        // into a single multi-row insert statement at commit time -- this
        // ALSO gains the bulk path Invariant-B swap semantics it never had
        // before (see `ExecutionInsertHook::insert_many`'s doc comment).
        let rows: Vec<NewExecutionRow> = jobs
            .iter()
            .zip(&schedule_times)
            .zip(&queue_ids)
            .map(|((job, schedule_at), queue_id)| NewExecutionRow {
                id: job.id,
                job_type: self.job_type.clone(),
                schedule_at: *schedule_at,
                queue_id: queue_id.clone(),
            })
            .collect();
        ExecutionInsertHook::register(op, &self.notifier, &self.poller_ref, &self.clock, rows)
            .await?;

        for (job, schedule_at) in jobs.iter_mut().zip(&schedule_times) {
            job.schedule_execution(*schedule_at);
        }
        self.repo.update_all_in_op(op, &mut jobs).await?;

        Ok(jobs)
    }

    #[instrument(name = "job.create_internal", skip(self, op, config), fields(job_type = %self.job_type))]
    async fn create_job_internal<C: Serialize + Send>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        config: C,
        schedule_at: DateTime<Utc>,
        queue_id: Option<String>,
    ) -> Result<Job, JobError> {
        let new_job = NewJob::builder()
            .id(id)
            .job_type(self.job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .queue_id(queue_id.clone())
            .build()
            .expect("Could not build new job");

        let mut job = self.repo.create_in_op(op, new_job).await?;

        // Head-swap kernel, insert half (handoff addendum §8.2a): registers
        // an `ExecutionInsertHook` that does the actual `job_executions`
        // insert, any Invariant-B swap, notify, and due-now claim demand --
        // all in `pre_commit`, at commit time. Several `spawn_in_op` calls
        // sharing one `op` merge into ONE multi-row insert statement (see
        // `ExecutionInsertHook::merge`).
        ExecutionInsertHook::register_one(
            op,
            &self.notifier,
            &self.poller_ref,
            &self.clock,
            NewExecutionRow {
                id: job.id,
                job_type: self.job_type.clone(),
                schedule_at,
                queue_id,
            },
        )
        .await?;

        job.schedule_execution(schedule_at);
        self.repo.update_in_op(op, &mut job).await?;

        Ok(job)
    }
}

/// Outcome of a keyed execution insert.
pub(crate) enum KeyedInsert {
    /// The key was free; this job now holds it.
    Inserted,
    /// The key is already held by this LIVE job.
    Live(JobId),
    /// The key was taken, but its holder is no longer visible — it went
    /// terminal between the conflict and the read. The caller should retry.
    Contended,
}

/// Insert a keyed execution row, resolving a live-key conflict in the SAME
/// round trip rather than following up with a separate lookup.
///
/// `ON CONFLICT DO NOTHING` (inferring the partial live-key index) turns the
/// conflict into data instead of an error, so the holder's id comes back
/// alongside it and there is no constraint-name string matching to keep in
/// sync with the schema. The holder is read at this statement's snapshot, so a
/// key claimed by a transaction that committed after it reports
/// [`KeyedInsert::Contended`] rather than a stale id.
#[instrument(name = "job.insert_keyed_execution", skip_all)]
pub(crate) async fn insert_keyed_execution(
    repo: &JobRepo,
    notifier: &Arc<JobEventNotifier>,
    op: &mut impl es_entity::AtomicOperation,
    job: &mut Job,
    schedule_at: DateTime<Utc>,
    unique_key: &str,
) -> Result<KeyedInsert, JobError> {
    let row = sqlx::query!(
        r#"
        WITH ins AS (
            INSERT INTO job_executions
                (id, job_type, queue_id, unique_key, execute_at, alive_at, created_at)
            VALUES ($1, $2, NULL, $3, $4, COALESCE($5, NOW()), COALESCE($5, NOW()))
            ON CONFLICT (job_type, unique_key) WHERE unique_key IS NOT NULL
            DO NOTHING
            RETURNING id
        )
        SELECT (SELECT id FROM ins) AS "inserted?: JobId",
               (SELECT id FROM job_executions
                WHERE job_type = $2 AND unique_key = $3) AS "live?: JobId"
        "#,
        job.id as JobId,
        &job.job_type as &JobType,
        unique_key,
        schedule_at,
        op.maybe_now(),
    )
    .fetch_one(op.as_executor())
    .await?;

    if row.inserted.is_some() {
        notifier.execution_ready_in_op(op, &job.job_type).await?;
        job.schedule_execution(schedule_at);
        repo.update_in_op(op, job).await?;
        return Ok(KeyedInsert::Inserted);
    }
    Ok(match row.live {
        Some(id) => KeyedInsert::Live(id),
        None => KeyedInsert::Contended,
    })
}
