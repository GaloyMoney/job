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
    handle::JobHandle,
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    repo::{JobCreateError, JobRepo},
};

/// The key [`JobSpawner::spawn_unique`] registers under. Keyed and unique
/// spawns of one type share this one key namespace per type — see
/// [`JobSpawner::spawn_keyed`].
pub const DEFAULT_UNIQUE_KEY: &str = "default";

/// Describes a job to be created as part of a bulk [`JobSpawner::spawn_all`] call.
///
/// Use [`JobSpec::new`] to create a spec with just an id and config, then
/// chain [`JobSpec::schedule_at`] or [`JobSpec::queue_id`] for optional overrides.
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
    router: Arc<JobNotificationRouter>,
    clock: ClockHandle,
    notifier: Arc<JobEventNotifier>,
    _phantom: PhantomData<Config>,
}

impl<Config> JobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(
        repo: Arc<JobRepo>,
        job_type: JobType,
        router: Arc<JobNotificationRouter>,
        clock: ClockHandle,
        notifier: Arc<JobEventNotifier>,
    ) -> Self {
        Self {
            repo,
            job_type,
            router,
            clock,
            notifier,
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

            let new_job = NewJob::builder()
                .id(spec.id)
                .job_type(self.job_type.clone())
                .config(spec.config)?
                .tracing_context(es_entity::context::TracingContext::current())
                .queue_id(spec.queue_id.clone())
                .build()
                .expect("Could not build new job");
            new_jobs.push(new_job);
            queue_ids.push(spec.queue_id);
        }

        let mut jobs = self.repo.create_all_in_op(op, new_jobs).await?;

        let ids: Vec<JobId> = jobs.iter().map(|j| j.id).collect();
        sqlx::query(
            r#"
            INSERT INTO job_executions (id, job_type, queue_id, execute_at, alive_at, created_at)
            SELECT unnested.id, $2, unnested.queue_id, unnested.execute_at,
                   COALESCE($5, NOW()), COALESCE($5, NOW())
            FROM UNNEST($1::uuid[], $3::text[], $4::timestamptz[])
                AS unnested(id, queue_id, execute_at)
            "#,
        )
        .bind(&ids)
        .bind(&self.job_type)
        .bind(&queue_ids)
        .bind(&schedule_times)
        .bind(op.maybe_now())
        .execute(op.as_executor())
        .await?;

        self.notifier
            .execution_ready_in_op(op, &self.job_type)
            .await?;

        for (job, schedule_at) in jobs.iter_mut().zip(&schedule_times) {
            job.schedule_execution(*schedule_at);
        }
        self.repo.update_all_in_op(op, &mut jobs).await?;

        Ok(jobs)
    }

    /// Create a keyed singleton: at most one job of this `(job_type, key)`
    /// EVER exists (`jobs` rows are never deleted, so this is not "at most
    /// one live at a time" — resharding or otherwise reusing a key needs a
    /// fresh one, e.g. a generation-scoped `g2:3`). Returns a [`JobHandle`]
    /// whether the job was created or already exists; on the duplicate path
    /// the handle carries the PERSISTED job's id, so a double-spawning
    /// caller always observes the job that actually runs. The job's id is
    /// generated internally — a keyed singleton is identified by its
    /// `(job_type, key)`, not a caller-chosen id — so read the id back from
    /// the returned handle.
    ///
    /// Keys are opaque to the crate — a sharded consumer can spawn one
    /// singleton per shard and later enumerate them all via
    /// [`Jobs::keyed_handles`](crate::Jobs::keyed_handles), pairing each
    /// handle with [`JobHandle::load`]'s execution state to answer "caught
    /// up?" per shard. Does not consume the spawner: unlike
    /// [`Self::spawn_unique`], many keys of one type are expected.
    #[instrument(
        name = "job_spawner.spawn_keyed",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_keyed(
        &self,
        key: impl Into<String> + Send + std::fmt::Debug,
        config: Config,
    ) -> Result<JobHandle, JobError> {
        let key = key.into();
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_key(key.clone())
            .job_type(self.job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        match self.repo.create_in_op(&mut op, new_job).await {
            Ok(mut job) => {
                let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
                self.insert_execution(&mut op, &mut job, schedule_at, None)
                    .await?;
                op.commit().await?;
                Ok(self.handle(job.id))
            }
            // The id is an internally generated v7 uuid, so the only
            // constraint that can fire on this insert is the
            // (job_type, unique_key) partial unique index — es_entity's
            // column attribution for a composite index is version-dependent,
            // so match the violation broadly rather than by column and
            // resolve the persisted job by lookup (contract preserved from
            // `spawn_unique`). The violation poisons the transaction, so
            // drop (roll back) the op before reading.
            Err(JobCreateError::ConstraintViolation { .. }) => {
                drop(op);
                let existing = self
                    .repo
                    .find_keyed(&self.job_type, &key)
                    .await?
                    // `jobs` rows are never deleted and the unique-key
                    // collision just fired, so exactly one row exists.
                    .expect("unique-key collision guarantees the row exists");
                Ok(self.handle(existing.id))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create and spawn a unique job: the type-level special case of a keyed
    /// singleton, sugar for `spawn_keyed(`[`DEFAULT_UNIQUE_KEY`]`, config)`.
    /// Only one job of this type can exist at a time. This method consumes
    /// the spawner since no further jobs of this type can be created —
    /// use [`Self::spawn_keyed`] directly when many jobs of one type, each
    /// keyed, are expected.
    ///
    /// The job's id is generated internally — a unique-per-type job is
    /// identified by its type, not a caller-chosen id — so read the id back
    /// from the returned [`JobHandle`].
    ///
    /// Returns a [`JobHandle`] whether the job was created or already exists.
    /// On the duplicate path the handle carries the PERSISTED job's id, so a
    /// double-spawning orchestrator always observes the job that actually
    /// runs. Note: since `spawn_unique` and `spawn_keyed` share one key
    /// namespace per type, calling both on the same job type collides by
    /// design — `spawn_unique` IS `spawn_keyed(DEFAULT_UNIQUE_KEY, …)`.
    #[instrument(
        name = "job_spawner.spawn_unique",
        skip(self, config),
        fields(job_type = %self.job_type)
    )]
    pub async fn spawn_unique(self, config: Config) -> Result<JobHandle, JobError> {
        self.spawn_keyed(DEFAULT_UNIQUE_KEY, config).await
    }

    fn handle(&self, id: JobId) -> JobHandle {
        JobHandle::new(
            id,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
        )
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
        self.insert_execution(op, &mut job, schedule_at, queue_id.as_deref())
            .await?;
        Ok(job)
    }

    #[instrument(name = "job.insert_execution", skip_all)]
    async fn insert_execution(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job: &mut Job,
        schedule_at: DateTime<Utc>,
        queue_id: Option<&str>,
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
          INSERT INTO job_executions (id, job_type, queue_id, execute_at, alive_at, created_at)
          VALUES ($1, $2, $3, $4, COALESCE($5, NOW()), COALESCE($5, NOW()))
        "#,
            job.id as JobId,
            &job.job_type as &JobType,
            queue_id,
            schedule_at,
            op.maybe_now()
        )
        .execute(op.as_executor())
        .await?;
        self.notifier
            .execution_ready_in_op(op, &job.job_type)
            .await?;
        job.schedule_execution(schedule_at);
        self.repo.update_in_op(op, job).await?;
        Ok(())
    }
}
