//! Job spawner for creating jobs of a specific type.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use serde::Serialize;
use std::{marker::PhantomData, sync::Arc};
use tracing::{Span, instrument};

use super::{
    Job, JobId,
    entity::{JobType, NewJob},
    error::JobError,
    repo::JobRepo,
};

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
    _phantom: PhantomData<Config>,
}

impl<Config> JobSpawner<Config>
where
    Config: Serialize + Send + Sync,
{
    pub(crate) fn new(repo: Arc<JobRepo>, job_type: JobType, clock: ClockHandle) -> Self {
        Self {
            repo,
            job_type,
            clock,
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
        fields(job_type = %self.job_type),
        err
    )]
    pub async fn spawn(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op().await?;
        let job = self.spawn_in_op(&mut op, id, config).await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type),
        err
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
        fields(job_type = %self.job_type),
        err
    )]
    pub async fn spawn_at(
        &self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op().await?;
        let job = self.spawn_at_in_op(&mut op, id, config, schedule_at).await?;
        op.commit().await?;
        Ok(job)
    }

    /// Create and spawn a job for execution at a specific time as part of an existing atomic operation.
    #[instrument(
        name = "job_spawner.spawn_at_in_op",
        skip(self, op, config),
        fields(job_type = %self.job_type),
        err
    )]
    pub async fn spawn_at_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        self.create_job_internal(op, id.into(), &self.job_type, config, schedule_at, false)
            .await
    }

    /// Create and spawn a unique job.
    ///
    /// Only one job of this type can exist at a time. This method consumes the
    /// spawner since no further jobs of this type can be created.
    ///
    /// Returns `Ok(())` whether the job was created or already exists.
    #[instrument(
        name = "job_spawner.spawn_unique",
        skip(self, config),
        fields(job_type = %self.job_type),
        err
    )]
    pub async fn spawn_unique(
        self,
        id: impl Into<JobId> + std::fmt::Debug,
        config: Config,
    ) -> Result<(), JobError> {
        let new_job = NewJob::builder()
            .id(id)
            .unique_per_type(true)
            .job_type(self.job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut op = self.repo.begin_op().await?;
        match self.repo.create_in_op(&mut op, new_job).await {
            Err(JobError::DuplicateUniqueJobType) => (),
            Err(e) => return Err(e),
            Ok(mut job) => {
                let schedule_at = op.maybe_now().unwrap_or_else(|| self.clock.now());
                self.insert_execution(&mut op, &mut job, schedule_at)
                    .await?;
                op.commit().await?;
            }
        }
        Ok(())
    }

    #[instrument(name = "job.create_internal", skip(self, op, config), fields(job_type = %job_type), err)]
    async fn create_job_internal<C: Serialize + Send>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: JobId,
        job_type: &JobType,
        config: C,
        schedule_at: DateTime<Utc>,
        unique_per_type: bool,
    ) -> Result<Job, JobError> {
        Span::current().record("job_type", tracing::field::display(job_type));

        let new_job = NewJob::builder()
            .id(id)
            .unique_per_type(unique_per_type)
            .job_type(job_type.clone())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");

        let mut job = self.repo.create_in_op(op, new_job).await?;
        self.insert_execution(op, &mut job, schedule_at).await?;
        Ok(job)
    }

    #[instrument(name = "job.insert_execution", skip_all, err)]
    async fn insert_execution(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job: &mut Job,
        schedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        sqlx::query!(
            r#"
          INSERT INTO job_executions (id, job_type, execute_at, alive_at, created_at)
          VALUES ($1, $2, $3, COALESCE($4, NOW()), COALESCE($4, NOW()))
        "#,
            job.id as JobId,
            &job.job_type as &JobType,
            schedule_at,
            op.maybe_now()
        )
        .execute(op.as_executor())
        .await?;
        job.schedule_execution(schedule_at);
        self.repo.update_in_op(op, job).await?;
        Ok(())
    }
}
