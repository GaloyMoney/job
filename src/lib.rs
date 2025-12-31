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
//! - Optional at-most-one-per-type queueing via [`JobSpawner::spawn_unique`].
//! - Built-in migrations that you can run automatically or embed into your own
//!   migration workflow.
//!
//! ## Core Concepts
//! - **Jobs service** – [`Jobs`] owns registration, polling, and shutdown.
//! - **Initializer** – [`JobInitializer`] registers a job type and builds a
//!   [`JobRunner`] for each execution. Defines the associated `Config` type.
//! - **Spawner** – [`JobSpawner`] is returned from registration and provides
//!   type-safe job creation methods. Parameterized by the config type.
//! - **Runner** – [`JobRunner`] performs the work using the provided
//!   [`CurrentJob`] context.
//! - **Current job** – [`CurrentJob`] exposes attempt counts, execution state,
//!   and access to the Postgres pool during a run.
//! - **Completion** – [`JobCompletion`] returns the outcome: finish, retry, or
//!   reschedule at a later time.
//!
//! ## Lifecycle
//!
//! 1. Initialize the service with [`Jobs::init`]
//! 2. Register initializers with [`Jobs::add_initializer`] – returns a [`JobSpawner`]
//! 3. Start polling with [`Jobs::start_poll`]
//! 4. Use spawners to create jobs throughout your application
//! 5. Shut down gracefully with [`Jobs::shutdown`]
//!
//! ## Example
//!
//! ```ignore
//! use async_trait::async_trait;
//! use job::{
//!     CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner,
//!     JobSpawner, JobSvcConfig, JobType, Jobs,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! // 1. Define your config (serialized to the database)
//! #[derive(Debug, Serialize, Deserialize)]
//! struct MyConfig {
//!     value: i32,
//! }
//!
//! // 2. Define your initializer
//! struct MyInitializer;
//!
//! impl JobInitializer for MyInitializer {
//!     type Config = MyConfig;
//!
//!     fn job_type(&self) -> JobType {
//!         JobType::new("my-job")
//!     }
//!
//!     fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
//!         let config: MyConfig = job.config()?;
//!         Ok(Box::new(MyRunner { value: config.value }))
//!     }
//! }
//!
//! // 3. Define your runner
//! struct MyRunner {
//!     value: i32,
//! }
//!
//! #[async_trait]
//! impl JobRunner for MyRunner {
//!     async fn run(
//!         &self,
//!         _current_job: CurrentJob,
//!     ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
//!         println!("Processing value: {}", self.value);
//!         Ok(JobCompletion::Complete)
//!     }
//! }
//!
//! // 4. Wire it up
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = JobSvcConfig::builder()
//!         .pg_con("postgres://user:pass@localhost/db")
//!         .build()?;
//!
//!     let mut jobs = Jobs::init(config).await?;
//!
//!     // Registration returns a type-safe spawner
//!     let spawner: JobSpawner<MyConfig> = jobs.add_initializer(MyInitializer);
//!
//!     jobs.start_poll().await?;
//!
//!     // Use the spawner to create jobs
//!     spawner.spawn(JobId::new(), MyConfig { value: 42 }).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Scheduling
//!
//! Jobs run immediately once a poller claims them. If you need a future start
//! time, schedule it up front with [`JobSpawner::spawn_at_in_op`]. After a
//! run completes, return [`JobCompletion::Complete`] for one-off work or use the
//! `JobCompletion::Reschedule*` variants to book the next execution.
//!
//! ## Retries
//!
//! Retry behaviour comes from [`JobInitializer::retry_on_error_settings`]. Once
//! attempts are exhausted the job is marked as errored and removed from the
//! queue.
//!
//! ```ignore
//! impl JobInitializer for MyInitializer {
//!     // ...
//!
//!     fn retry_on_error_settings(&self) -> RetrySettings {
//!         RetrySettings {
//!             n_attempts: Some(5),
//!             min_backoff: Duration::from_secs(10),
//!             max_backoff: Duration::from_secs(300),
//!             ..Default::default()
//!         }
//!     }
//! }
//! ```
//!
//! ## Uniqueness
//!
//! For at-most-one semantics, use [`JobSpawner::spawn_unique`]. This method
//! consumes the spawner, enforcing at the type level that only one job of
//! this type can exist:
//!
//! ```ignore
//! let cleanup_spawner = jobs.add_initializer(CleanupInitializer);
//!
//! // Consumes spawner - can't accidentally spawn twice
//! cleanup_spawner.spawn_unique(JobId::new(), CleanupConfig::default()).await?;
//! ```
//!
//! ## Parameterized Job Types
//!
//! For cases where the job type is configured at runtime (e.g., multi-tenant inboxes),
//! store the job type in your initializer and return it from the instance method:
//!
//! ```ignore
//! struct TenantJobInitializer {
//!     job_type: JobType,
//!     tenant_id: String,
//! }
//!
//! impl JobInitializer for TenantJobInitializer {
//!     type Config = TenantJobConfig;
//!
//!     fn job_type(&self) -> JobType {
//!         self.job_type.clone()  // From instance, not hardcoded
//!     }
//!
//!     // ...
//! }
//! ```
//!
//! ## Database migrations
//!
//! See the [setup guide](https://github.com/GaloyMoney/job/blob/main/README.md#setup)
//! for migration options and examples.
//!
//! ## Feature flags
//!
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
mod spawner;
mod time;
mod tracker;

pub mod error;

use tracing::instrument;

use std::sync::{Arc, Mutex};

pub use config::*;
pub use current::*;
pub use entity::{Job, JobType};
pub use migrate::*;
pub use registry::*;
pub use runner::*;
pub use spawner::*;

use error::*;
use poller::*;
use repo::*;

es_entity::entity_id! { JobId }

#[derive(Clone)]
/// Primary entry point for interacting with the Job crate. Provides APIs to register job
/// handlers, manage configuration, and control scheduling and execution.
pub struct Jobs {
    config: JobSvcConfig,
    repo: Arc<JobRepo>,
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

        let repo = Arc::new(JobRepo::new(&pool));
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
    /// Panics if invoked more than once, or if [`Jobs::add_initializer`] is called after the
    /// poller has started.
    ///
    /// # Examples
    ///
    /// Register any initializers and then start the poller:
    ///
    /// ```no_run
    /// use job::{
    ///     Jobs, JobSvcConfig, Job, JobId, JobInitializer, JobRunner, JobType, JobCompletion,
    ///     CurrentJob, JobSpawner,
    /// };
    /// use job::error::JobError;
    /// use async_trait::async_trait;
    /// use serde::{Serialize, Deserialize};
    /// use sqlx::postgres::PgPoolOptions;
    /// use std::error::Error;
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct MyConfig {
    ///     value: i32,
    /// }
    ///
    /// struct MyInitializer;
    ///
    /// impl JobInitializer for MyInitializer {
    ///     type Config = MyConfig;
    ///
    ///     fn job_type(&self) -> JobType {
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
    /// // Registration returns a type-safe spawner
    /// let spawner: JobSpawner<MyConfig> = jobs.add_initializer(MyInitializer);
    ///
    /// jobs.start_poll().await?;
    ///
    /// // Use the spawner to create jobs
    /// spawner.spawn(JobId::new(), MyConfig { value: 42 }).await?;
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
    ///     JobSpawner,
    /// };
    /// use job::error::JobError;
    /// use async_trait::async_trait;
    /// use serde::{Serialize, Deserialize};
    /// use sqlx::postgres::PgPoolOptions;
    /// use std::error::Error;
    ///
    /// #[derive(Debug, Serialize, Deserialize)]
    /// struct MyConfig {
    ///     value: i32,
    /// }
    ///
    /// struct MyInitializer;
    ///
    /// impl JobInitializer for MyInitializer {
    ///     type Config = MyConfig;
    ///
    ///     fn job_type(&self) -> JobType {
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
    /// let _spawner = jobs.add_initializer(MyInitializer);
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
                (*self.repo).clone(),
                registry,
            )
            .start()
            .await?,
        ));
        Ok(())
    }

    /// Register a [`JobInitializer`] and return a [`JobSpawner`] for creating jobs.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let spawner = jobs.add_initializer(MyInitializer);
    /// spawner.spawn(JobId::new(), MyConfig { value: 42 }).await?;
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called after [`start_poll`](Self::start_poll).
    pub fn add_initializer<I: JobInitializer>(&mut self, initializer: I) -> JobSpawner<I::Config> {
        let job_type = {
            let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
            registry
                .as_mut()
                .expect("Registry has been consumed by executor")
                .add_initializer(initializer)
        };
        JobSpawner::new(Arc::clone(&self.repo), job_type, self.poller_handle.clone())
    }

    /// Fetch the current snapshot of a job entity by identifier.
    #[instrument(name = "job.find", skip(self))]
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
}
