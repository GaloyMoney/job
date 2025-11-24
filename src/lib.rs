//! `job` is an async, Postgres-backed job scheduler and runner for Rust
//! applications. It coordinates distributed workers, tracks job history, and
//! handles retries with predictable backoff. Inspired by earlier systems like
//! [Sidekiq](https://sidekiq.org), it focuses on running your
//! application code asynchronously, outside of request/response paths while keeping business
//! logic in familiar Rust async functions. The crate uses [`sqlx`] for
//! database access and forbids `unsafe`.
//!
//! ## Documentation
//! - [Github repository](https://github.com/GaloyMoney/job)
//! - [Cargo package](https://crates.io/crates/job)
//!
//! ## Highlights
//! - Durable Postgres-backed storage so jobs survive restarts and crashes.
//! - Automatic exponential backoff with jitter, plus opt-in infinite retries.
//! - Concurrency controls that let many worker instances share the workload,
//!   configurable through [`JobPollerConfig`].
//! - Optional at-most-one-per-type queueing.
//! - Built-in migrations that you can run automatically or embed into your own
//!   migration workflow.
//!
//! ## Core concepts
//! - **Jobs service** – [`Jobs`] owns registration, polling, and shutdown.
//! - **Initializer** – [`JobInitializer`] registers a job type and builds a
//!   [`JobRunner`] for each execution.
//! - **Runner** – [`JobRunner`] performs the work using the provided
//!   [`CurrentJob`] context.
//! - **Current job** – [`CurrentJob`] exposes attempt counts, execution state,
//!   and access to the Postgres pool during a run.
//! - **Completion** – [`JobCompletion`] returns the outcome: finish, retry, or
//!   reschedule at a later time.
//!
//! ## Scheduling
//! Jobs run immediately once a poller claims them. If you need a future start
//! time, schedule it up front with [`Jobs::create_and_spawn_at_in_op`]. After a
//! run completes, return `JobCompletion::Complete` for one-off work or use the
//! `JobCompletion::Reschedule*` variants to book the next execution.
//!
//! ## Retries
//! Retry behaviour comes from [`JobInitializer::retry_on_error_settings`]. Once
//! attempts are exhausted the job is marked as errored and removed from the
//! queue.
//!
//! ## Uniqueness
//! For at-most-one semantics, use [`Jobs::add_initializer_and_spawn_unique`] or
//! set `unique_per_type(true)` when building jobs manually.
//!
//! ## Database migrations
//! See the [setup guide](https://github.com/GaloyMoney/job/blob/main/README.md#setup)
//! for migration options and examples.
//!
//! ## Feature flags
//! - `es-entity` enables advanced integration with the [`es_entity`] crate,
//!   allowing runners to finish with `DbOp` handles and enriching tracing/event
//!   metadata.
//! - `sim-time` swaps time and sleeping utilities for the
//!   [`sim_time`](https://docs.rs/sim-time) crate, making it easier to test
//!   long-running backoff strategies deterministically.

#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

mod config;
mod current;
mod dispatcher;
mod entity;
mod handle;
mod migrate;
mod poller;
mod registry;
mod repo;
mod runner;
mod time;
mod tracker;

pub mod error;

use chrono::{DateTime, Utc};
use tracing::{Span, instrument};

use std::sync::{Arc, Mutex};

pub use config::*;
pub use current::*;
pub use entity::{Job, JobType};
pub use migrate::*;
pub use registry::*;
pub use runner::*;

use entity::NewJob;
use error::*;
use poller::*;
use repo::*;

es_entity::entity_id! { JobId }

#[derive(Clone)]
/// Primary entry point for interacting with the Job crate. Provides APIs to register job
/// handlers, manage configuration, and control scheduling and execution.
pub struct Jobs {
    config: JobSvcConfig,
    repo: JobRepo,
    registry: Arc<Mutex<Option<JobRegistry>>>,
    poller_handle: Option<Arc<JobPollerHandle>>,
}

impl Jobs {
    /// Initialize the service using a [`JobSvcConfig`] for connection and runtime settings.
    pub async fn init(config: JobSvcConfig) -> Result<Self, JobError> {
        let pool = match (config.pool.clone(), config.pg_con.clone()) {
            (Some(pool), None) => pool,
            (None, Some(pg_con)) => {
                let mut pool_opts = sqlx::postgres::PgPoolOptions::new();
                if let Some(max_connections) = config.max_connections {
                    pool_opts = pool_opts.max_connections(max_connections);
                }
                pool_opts.connect(&pg_con).await.map_err(JobError::Sqlx)?
            }
            _ => {
                return Err(JobError::Config(
                    "One of pg_con or pool must be set".to_string(),
                ));
            }
        };

        if config.exec_migrations {
            sqlx::migrate!().run(&pool).await?;
        }

        let repo = JobRepo::new(&pool);
        let registry = Arc::new(Mutex::new(Some(JobRegistry::new())));
        Ok(Self {
            repo,
            config,
            registry,
            poller_handle: None,
        })
    }

    /// Start the background poller that fetches and dispatches jobs from Postgres.
    ///
    /// Call this only after registering every job initializer. The call consumes the internal
    /// registry; attempting to register additional initializers or starting the poller again
    /// afterwards will panic with `Registry has been consumed by executor`.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Sqlx`] if the poller cannot initialise its database listeners or
    /// supporting tasks.
    ///
    /// # Panics
    ///
    /// Panics if invoked more than once, or if [`Jobs::add_initializer`] or
    /// [`Jobs::add_initializer_and_spawn_unique`] are called after the poller has started.
    ///
    /// # Examples
    ///
    /// Register any initializers and then start the poller:
    ///
    /// ```no_run
    /// use job::{
    ///     Jobs, JobSvcConfig, Job, JobInitializer, JobRunner, JobType, JobCompletion, CurrentJob,
    /// };
    /// use job::error::JobError;
    /// use async_trait::async_trait;
    /// use sqlx::postgres::PgPoolOptions;
    /// use std::error::Error;
    ///
    /// struct MyInitializer;
    ///
    /// impl JobInitializer for MyInitializer {
    ///     fn job_type() -> JobType {
    ///         JobType::new("example")
    ///     }
    ///
    ///     fn init(&self, _job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
    ///         Ok(Box::new(MyRunner))
    ///     }
    /// }
    ///
    /// struct MyRunner;
    ///
    /// #[async_trait]
    /// impl JobRunner for MyRunner {
    ///     async fn run(
    ///         &self,
    ///         _current_job: CurrentJob,
    ///     ) -> Result<JobCompletion, Box<dyn Error>> {
    ///         Ok(JobCompletion::Complete)
    ///     }
    /// }
    ///
    /// # async fn example() -> Result<(), JobError> {
    /// let pool = PgPoolOptions::new()
    ///     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
    /// let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    /// let mut jobs = Jobs::init(config).await?;
    ///
    /// jobs.add_initializer(MyInitializer);
    ///
    /// jobs.start_poll().await?;
    /// # Ok(())
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap();
    /// ```
    ///
    /// Calling `start_poll` again, or attempting to register a new initializer afterwards,
    /// results in a panic:
    ///
    /// ```no_run
    /// use job::{
    ///     Jobs, JobSvcConfig, Job, JobInitializer, JobRunner, JobType, JobCompletion, CurrentJob,
    /// };
    /// use job::error::JobError;
    /// use async_trait::async_trait;
    /// use sqlx::postgres::PgPoolOptions;
    /// use std::error::Error;
    ///
    /// struct MyInitializer;
    ///
    /// impl JobInitializer for MyInitializer {
    ///     fn job_type() -> JobType {
    ///         JobType::new("example")
    ///     }
    ///
    ///     fn init(&self, _job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
    ///         Ok(Box::new(MyRunner))
    ///     }
    /// }
    ///
    /// struct MyRunner;
    ///
    /// #[async_trait]
    /// impl JobRunner for MyRunner {
    ///     async fn run(
    ///         &self,
    ///         _current_job: CurrentJob,
    ///     ) -> Result<JobCompletion, Box<dyn Error>> {
    ///         Ok(JobCompletion::Complete)
    ///     }
    /// }
    ///
    /// # async fn double_start() -> Result<(), JobError> {
    /// let pool = PgPoolOptions::new()
    ///     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
    /// let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    /// let mut jobs = Jobs::init(config).await?;
    ///
    /// jobs.start_poll().await?;
    ///
    /// // Panics with "Registry has been consumed by executor".
    /// // jobs.start_poll().await.unwrap();
    ///
    /// // Also panics because the registry moved into the poller.
    /// // jobs.add_initializer(MyInitializer);
    /// # Ok(())
    /// # }
    /// # tokio::runtime::Runtime::new().unwrap().block_on(double_start()).unwrap();
    /// ```
    #[instrument(name = "job.start_poll", skip(self), err)]
    pub async fn start_poll(&mut self) -> Result<(), JobError> {
        let registry = self
            .registry
            .lock()
            .expect("Couldn't lock Registry Mutex")
            .take()
            .expect("Registry has been consumed by executor");
        self.poller_handle = Some(Arc::new(
            JobPoller::new(
                self.config.poller_config.clone(),
                self.repo.clone(),
                registry,
            )
            .start()
            .await?,
        ));
        Ok(())
    }

    /// Register a [`JobInitializer`] so the service can construct runners.
    pub fn add_initializer<I: JobInitializer>(&self, initializer: I) {
        let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
        registry
            .as_mut()
            .expect("Registry has been consumed by executor")
            .add_initializer(initializer);
    }

    #[instrument(
        name = "job.add_initializer_and_spawn_unique",
        skip(self, initializer, config),
        fields(job_type, now)
    )]
    /// Register an initializer and enqueue a job only if one of that type is not already pending.
    ///
    /// Use this when you want a single outstanding job for a given [`JobType`]. The new job is
    /// created with `unique_per_type(true)`, so the insert simply succeeds without change if
    /// another job of the same type is already queued or running.
    ///
    /// # Errors
    ///
    /// Propagates [`JobError`] if the initializer cannot be stored, the config fails to serialize,
    /// or the database interaction (e.g., starting a transaction) fails.
    pub async fn add_initializer_and_spawn_unique<C: JobConfig>(
        &self,
        initializer: <C as JobConfig>::Initializer,
        config: C,
    ) -> Result<(), JobError> {
        {
            let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
            registry
                .as_mut()
                .expect("Registry has been consumed by executor")
                .add_initializer(initializer);
        }
        let job_type = <<C as JobConfig>::Initializer as JobInitializer>::job_type();
        Span::current().record("job_type", tracing::field::display(&job_type));
        let new_job = NewJob::builder()
            .id(JobId::new())
            .unique_per_type(true)
            .job_type(job_type)
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut op = self.repo.begin_op().await?;
        match self.repo.create_in_op(&mut op, new_job).await {
            Err(JobError::DuplicateUniqueJobType) => (),
            Err(e) => return Err(e),
            Ok(mut job) => {
                let schedule_at = op.now().unwrap_or_else(crate::time::now);
                self.insert_execution::<<C as JobConfig>::Initializer>(
                    &mut op,
                    &mut job,
                    schedule_at,
                )
                .await?;
                op.commit().await?;
            }
        }
        Ok(())
    }

    #[instrument(
        name = "job.create_and_spawn",
        skip(self, config),
        fields(job_type, now)
    )]
    /// Persist a job with the provided ID and schedule it for immediate execution.
    pub async fn create_and_spawn<C: JobConfig>(
        &self,
        job_id: impl Into<JobId> + std::fmt::Debug,
        config: C,
    ) -> Result<Job, JobError> {
        let mut op = self.repo.begin_op().await?;
        let job = self.create_and_spawn_in_op(&mut op, job_id, config).await?;
        op.commit().await?;
        Ok(job)
    }

    #[instrument(
        name = "job.create_and_spawn_in_op",
        skip(self, op, config),
        fields(job_type, now)
    )]
    /// Create and enqueue a job as part of an existing atomic operation.
    pub async fn create_and_spawn_in_op<C: JobConfig>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job_id: impl Into<JobId> + std::fmt::Debug,
        config: C,
    ) -> Result<Job, JobError> {
        let job_type = <<C as JobConfig>::Initializer as JobInitializer>::job_type();
        Span::current().record("job_type", tracing::field::display(&job_type));
        let new_job = NewJob::builder()
            .id(job_id.into())
            .job_type(<<C as JobConfig>::Initializer as JobInitializer>::job_type())
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut job = self.repo.create_in_op(op, new_job).await?;
        let schedule_at = op.now().unwrap_or_else(crate::time::now);
        self.insert_execution::<<C as JobConfig>::Initializer>(op, &mut job, schedule_at)
            .await?;
        Ok(job)
    }

    #[instrument(
        name = "job.create_and_spawn_at_in_op",
        skip(self, op, config),
        fields(job_type, now)
    )]
    /// Create a job and schedule it for a specific time within an atomic operation.
    pub async fn create_and_spawn_at_in_op<C: JobConfig>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job_id: impl Into<JobId> + std::fmt::Debug,
        config: C,
        schedule_at: DateTime<Utc>,
    ) -> Result<Job, JobError> {
        let job_type = <<C as JobConfig>::Initializer as JobInitializer>::job_type();
        Span::current().record("job_type", tracing::field::display(&job_type));
        let new_job = NewJob::builder()
            .id(job_id.into())
            .job_type(job_type)
            .config(config)?
            .tracing_context(es_entity::context::TracingContext::current())
            .build()
            .expect("Could not build new job");
        let mut job = self.repo.create_in_op(op, new_job).await?;
        self.insert_execution::<<C as JobConfig>::Initializer>(op, &mut job, schedule_at)
            .await?;
        Ok(job)
    }

    #[instrument(name = "job.find", skip(self))]
    /// Fetch the current snapshot of a job entity by identifier.
    pub async fn find(&self, id: JobId) -> Result<Job, JobError> {
        self.repo.find_by_id(id).await
    }

    /// Gracefully shut down the job poller.
    ///
    /// This method is idempotent and can be called multiple times safely.
    /// It will send a shutdown signal to all running jobs, wait briefly for them
    /// to complete, and reschedule any jobs still running.
    ///
    /// If not called manually, shutdown will be automatically triggered when the
    /// Jobs instance is dropped.
    #[instrument(name = "job.shutdown", skip(self), err)]
    pub async fn shutdown(&self) -> Result<(), JobError> {
        if let Some(handle) = &self.poller_handle {
            handle.shutdown().await?;
        }
        Ok(())
    }

    #[instrument(name = "job.insert_execution", skip_all, err)]
    async fn insert_execution<I: JobInitializer>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        job: &mut Job,
        schedule_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        if job.job_type != I::job_type() {
            return Err(JobError::JobTypeMismatch(
                job.job_type.clone(),
                I::job_type(),
            ));
        }
        sqlx::query!(
            r#"
          INSERT INTO job_executions (id, job_type, execute_at, alive_at, created_at)
          VALUES ($1, $2, $3, COALESCE($4, NOW()), COALESCE($4, NOW()))
        "#,
            job.id as JobId,
            &job.job_type as &JobType,
            schedule_at,
            op.now()
        )
        .execute(op.as_executor())
        .await?;
        job.schedule_execution(schedule_at);
        self.repo.update_in_op(op, job).await?;
        Ok(())
    }
}
