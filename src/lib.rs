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
//!
//! ## Testing with simulated time
//!
//! For deterministic testing of time-dependent behavior (e.g., backoff strategies),
//! inject an artificial clock via [`JobSvcConfig::clock`]:
//!
//! ```ignore
//! use job::{JobSvcConfig, ClockHandle};
//!
//! let (clock, controller) = ClockHandle::manual();
//! let config = JobSvcConfig::builder()
//!     .pool(pool)
//!     .clock(clock)
//!     .build()?;
//!
//! // Advance time deterministically
//! controller.advance(Duration::from_secs(60)).await;
//! ```

#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

mod config;
mod current;
mod dispatcher;
mod entity;
mod handle;
mod handle_api;
mod migrate;
mod notification_router;
mod outcome;
mod poller;
mod registry;
mod repo;
mod runner;
mod spawner;
mod tracker;

pub mod error;

use tracing::instrument;

use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use config::*;
pub use current::*;
pub use entity::{Job, JobType};
pub use es_entity::clock::{Clock, ClockController, ClockHandle};
pub use es_entity::{ListDirection, PaginatedQueryArgs, PaginatedQueryRet};
pub use handle_api::{JobHandle, JobHandles, JobStatus};
pub use migrate::*;
pub use outcome::{JobOutcome, JobOutcomes, JobReturnValue, JobTerminalState};
pub use registry::*;
pub use repo::job_cursor;
pub use runner::*;
pub use spawner::*;

use error::*;
use notification_router::*;
use poller::*;
use repo::*;
use tracker::*;

es_entity::entity_id! { JobId }

/// Sentinel interval for the deprecated wait-forever paths, which delegate to
/// the bounded-await handle surface by looping and re-awaiting on
/// [`JobError::TimedOut`]. Each iteration re-registers a fresh waiter, so a
/// lost in-memory notification self-heals within one interval instead of
/// wedging forever.
const DEPRECATED_WAIT_FOREVER_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
/// Primary entry point for interacting with the Job crate. Provides APIs to register job
/// handlers, manage configuration, and control scheduling and execution.
pub struct Jobs {
    config: JobSvcConfig,
    repo: Arc<JobRepo>,
    registry: Arc<Mutex<Option<JobRegistry>>>,
    router: Arc<JobNotificationRouter>,
    poller_handle: Option<Arc<JobPollerHandle>>,
    clock: ClockHandle,
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
                pool_opts.connect(&pg_con).await?
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
        let router = Arc::new(JobNotificationRouter::new(
            &pool,
            Arc::clone(&repo),
            config.poller_config.terminal_channel_size,
            config.poller_config.sweep_interval,
        ));
        let clock = config.clock.clone();
        Ok(Self {
            repo,
            config,
            registry,
            router,
            poller_handle: None,
            clock,
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
    ///     CurrentJob, JobSpawner
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
    ///     fn init(&self, _job: &Job, _: JobSpawner<Self::Config>) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
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
    ///     fn init(&self, _job: &Job, _spawner: JobSpawner<Self::Config>) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
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

        let tracker = Arc::new(JobTracker::new(
            self.config.poller_config.min_jobs_per_process,
            self.config.poller_config.max_jobs_per_process,
        ));

        let poller = JobPoller::new(
            self.config.poller_config.clone(),
            Arc::clone(&self.repo),
            registry,
            Arc::clone(&tracker),
            Arc::clone(&self.router),
            self.clock.clone(),
        );

        let job_types = poller.registered_job_types();

        let (listener_handle, waiter_handle) =
            self.router.start(Arc::clone(&tracker), job_types).await?;

        let poller_handle = poller.start(listener_handle, waiter_handle);
        self.poller_handle = Some(Arc::new(poller_handle));
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
        JobSpawner::new(
            Arc::clone(&self.repo),
            job_type,
            Arc::clone(&self.router),
            self.clock.clone(),
        )
    }

    /// Fetch the current snapshot of a job entity by identifier.
    #[instrument(name = "job.find", skip(self))]
    pub async fn find(&self, id: JobId) -> Result<Job, JobError> {
        Ok(self.repo.find_by_id(id).await?)
    }

    /// Mint a [`JobHandle`] for `id` — cheap and non-validating.
    ///
    /// No I/O happens until a method on the handle is called; handles hold no
    /// cached state, so every read is a live committed read. The id does not
    /// need to belong to an existing job: [`JobHandle::status`] and the
    /// awaits return [`JobError::Find`] if it never existed.
    ///
    /// See [`Jobs::handles`] for the batch mint and the
    /// persist-ids → re-mint → await pattern.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use job::{
    /// #     Jobs, JobSvcConfig, Job, JobId, JobInitializer, JobRunner, JobType, JobCompletion,
    /// #     CurrentJob, JobSpawner,
    /// # };
    /// # use job::error::JobError;
    /// # use async_trait::async_trait;
    /// # use serde::{Serialize, Deserialize};
    /// # use sqlx::postgres::PgPoolOptions;
    /// # use std::error::Error;
    /// # use std::time::Duration;
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # struct MyConfig { value: i32 }
    /// # struct MyInitializer;
    /// # impl JobInitializer for MyInitializer {
    /// #     type Config = MyConfig;
    /// #     fn job_type(&self) -> JobType { JobType::new("example") }
    /// #     fn init(&self, _job: &Job, _: JobSpawner<Self::Config>) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
    /// #         Ok(Box::new(MyRunner))
    /// #     }
    /// # }
    /// # struct MyRunner;
    /// # #[async_trait]
    /// # impl JobRunner for MyRunner {
    /// #     async fn run(&self, _current_job: CurrentJob) -> Result<JobCompletion, Box<dyn Error>> {
    /// #         Ok(JobCompletion::Complete)
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), JobError> {
    /// # let pool = PgPoolOptions::new()
    /// #     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
    /// # let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    /// # let mut jobs = Jobs::init(config).await?;
    /// # let spawner: JobSpawner<MyConfig> = jobs.add_initializer(MyInitializer);
    /// # jobs.start_poll().await?;
    /// let job_id = JobId::new();
    /// spawner.spawn(job_id, MyConfig { value: 42 }).await?;
    ///
    /// // Observe and await the job through its handle (live committed reads).
    /// let handle = jobs.handle(job_id);
    /// let status = handle.status().await?;
    /// let outcome = handle.await_completion(Duration::from_secs(60)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn handle(&self, id: impl Into<JobId>) -> JobHandle {
        JobHandle::new(
            id.into(),
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
        )
    }

    /// Mint an ordered [`JobHandles`] collection — cheap and non-validating.
    ///
    /// Results of the batch methods ([`JobHandles::await_all`],
    /// [`JobHandles::statuses`]) align positionally with the input ids.
    ///
    /// # Examples
    ///
    /// Persist the ids first (crash-safe), spawn, then re-mint handles from
    /// the persisted ids to await the batch:
    ///
    /// ```no_run
    /// # use job::{
    /// #     Jobs, JobSvcConfig, Job, JobId, JobInitializer, JobRunner, JobType, JobCompletion,
    /// #     CurrentJob, JobSpawner, JobOutcomes,
    /// # };
    /// # use job::error::JobError;
    /// # use async_trait::async_trait;
    /// # use serde::{Serialize, Deserialize};
    /// # use sqlx::postgres::PgPoolOptions;
    /// # use std::error::Error;
    /// # use std::time::Duration;
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # struct MyConfig { value: i32 }
    /// # struct MyInitializer;
    /// # impl JobInitializer for MyInitializer {
    /// #     type Config = MyConfig;
    /// #     fn job_type(&self) -> JobType { JobType::new("example") }
    /// #     fn init(&self, _job: &Job, _: JobSpawner<Self::Config>) -> Result<Box<dyn JobRunner>, Box<dyn Error>> {
    /// #         Ok(Box::new(MyRunner))
    /// #     }
    /// # }
    /// # struct MyRunner;
    /// # #[async_trait]
    /// # impl JobRunner for MyRunner {
    /// #     async fn run(&self, _current_job: CurrentJob) -> Result<JobCompletion, Box<dyn Error>> {
    /// #         Ok(JobCompletion::Complete)
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), JobError> {
    /// # let pool = PgPoolOptions::new()
    /// #     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
    /// # let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    /// # let mut jobs = Jobs::init(config).await?;
    /// # let spawner: JobSpawner<MyConfig> = jobs.add_initializer(MyInitializer);
    /// # jobs.start_poll().await?;
    /// // 1. Generate and PERSIST the ids before spawning (crash-safe: if the
    /// //    process dies mid-batch, the ids can be reloaded and re-awaited).
    /// let ids: Vec<JobId> = (0..3).map(|_| JobId::new()).collect();
    /// // ... write `ids` into your own state in the same transaction ...
    ///
    /// // 2. Spawn the jobs.
    /// for id in &ids {
    ///     spawner.spawn(*id, MyConfig { value: 42 }).await?;
    /// }
    ///
    /// // 3. Later (possibly after a restart): re-mint handles from the
    /// //    persisted ids and await the whole batch, bounded.
    /// let handles = jobs.handles(ids);
    /// let outcomes = handles.await_all(Duration::from_secs(300)).await?;
    /// assert!(outcomes.all_succeeded());
    /// # Ok(())
    /// # }
    /// ```
    pub fn handles(&self, ids: impl IntoIterator<Item = impl Into<JobId>>) -> JobHandles {
        ids.into_iter().map(|id| self.handle(id)).collect()
    }

    /// List child jobs for a parent with cursor-based pagination.
    #[instrument(name = "job.list_by_parent_job_id", skip(self))]
    pub async fn list_by_parent_job_id(
        &self,
        parent_job_id: JobId,
        query: PaginatedQueryArgs<job_cursor::JobByIdCursor>,
        direction: ListDirection,
    ) -> Result<PaginatedQueryRet<Job, job_cursor::JobByIdCursor>, JobError> {
        Ok(self
            .repo
            .list_for_parent_job_id_by_id(Some(parent_job_id), query, direction)
            .await?)
    }

    /// Convenience method that fetches all child jobs for a parent.
    #[instrument(name = "job.list_all_by_parent_job_id", skip(self))]
    pub async fn list_all_by_parent_job_id(
        &self,
        parent_job_id: JobId,
    ) -> Result<Vec<Job>, JobError> {
        let mut all_jobs = Vec::new();
        let mut query = Some(PaginatedQueryArgs::default());
        while let Some(q) = query.take() {
            let mut ret = self
                .repo
                .list_for_parent_job_id_by_id(Some(parent_job_id), q, ListDirection::Ascending)
                .await?;
            all_jobs.append(&mut ret.entities);
            query = ret.into_next_query();
        }
        Ok(all_jobs)
    }

    /// Returns a reference to the clock used by this job service.
    pub fn clock(&self) -> &ClockHandle {
        &self.clock
    }

    /// Block until the given job reaches a terminal state (completed or errored)
    /// and return the outcome together with any result value the
    /// runner attached via [`CurrentJob::set_result`].
    ///
    /// When `timeout` is `Some(duration)`, the call returns
    /// [`JobError::TimedOut`] if the job has not reached a terminal state
    /// within the specified duration. Pass `None` to wait indefinitely.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if the job does not exist.
    /// Returns [`JobError::TimedOut`] if the timeout elapses before the job
    /// reaches a terminal state.
    /// Returns [`JobError::AwaitCompletionShutdown`] if the notification channel is
    /// dropped (e.g., during shutdown) before delivering the terminal state.
    #[deprecated(note = "use jobs.handle(id).await_completion(timeout)")]
    #[instrument(name = "job.await_completion", skip(self))]
    pub async fn await_completion(
        &self,
        id: JobId,
        timeout: Option<Duration>,
    ) -> Result<JobOutcome, JobError> {
        // Preserve the historical fail-fast order: find before waiter
        // registration, so a missing job errors even before start_poll.
        self.find(id).await?;
        let handle = self.handle(id);
        let result = match timeout {
            Some(duration) => handle.await_completion(duration).await,
            None => loop {
                // Wait-forever, expressed as a bounded-await loop (the handle
                // surface has no unbounded wait). Each iteration re-registers
                // a fresh waiter, which also self-heals a lost in-memory
                // notification.
                match handle
                    .await_completion(DEPRECATED_WAIT_FOREVER_INTERVAL)
                    .await
                {
                    Err(JobError::TimedOut(_)) => continue,
                    other => break other,
                }
            },
        };
        match result {
            // Pre-handle behavior: awaiting before Jobs::start_poll panicked.
            Err(JobError::RouterNotStarted) => panic!("router not started"),
            other => other,
        }
    }

    /// Block until every job in `ids` reaches a terminal state (completed or
    /// errored) and return all outcomes together with any result values the
    /// runners attached via [`CurrentJob::set_result`].
    ///
    /// This is the batch counterpart of [`await_completion`](Self::await_completion).
    /// Each job is awaited concurrently; the call resolves once **all** jobs
    /// have finished.
    ///
    /// When `timeout` is `Some(duration)`, the call returns
    /// [`JobError::TimedOut`] if the batch has not fully resolved within the
    /// specified duration. Pass `None` to wait indefinitely.
    ///
    /// An empty `ids` slice returns an empty `Vec` immediately.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Find`] if any job in the batch does not exist.
    /// Returns [`JobError::TimedOut`] if the timeout elapses before every job
    /// reaches a terminal state.
    /// Returns [`JobError::AwaitCompletionShutdown`] if the notification channel
    /// is dropped (e.g., during shutdown) before all jobs have resolved.
    #[deprecated(note = "use jobs.handles(ids).await_all(timeout)")]
    #[instrument(name = "job.await_completions", skip(self))]
    pub async fn await_completions(
        &self,
        ids: &[JobId],
        timeout: Option<Duration>,
    ) -> Result<Vec<JobOutcome>, JobError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let handles = self.handles(ids.iter().copied());
        let result = match timeout {
            Some(duration) => handles.await_all(duration).await,
            None => loop {
                // Wait-forever, expressed as a bounded-await loop — see
                // `await_completion` above.
                match handles.await_all(DEPRECATED_WAIT_FOREVER_INTERVAL).await {
                    Err(JobError::TimedOut(_)) => continue,
                    other => break other,
                }
            },
        };
        match result {
            // Pre-handle behavior: awaiting before Jobs::start_poll panicked.
            Err(JobError::RouterNotStarted) => panic!("router not started"),
            other => other,
        }
    }

    /// Non-blocking check for job completion.
    ///
    /// Returns the terminal state immediately if the job has reached one,
    /// or `None` if it is still running.
    #[deprecated(note = "use jobs.handle(id).status()")]
    #[instrument(name = "job.poll_completion", skip(self))]
    pub async fn poll_completion(&self, id: JobId) -> Result<Option<JobTerminalState>, JobError> {
        Ok(match self.handle(id).status().await? {
            JobStatus::Completed { .. } => Some(JobTerminalState::Completed),
            JobStatus::Errored { .. } => Some(JobTerminalState::Errored),
            JobStatus::Pending { .. } | JobStatus::Running { .. } => None,
        })
    }

    /// Gracefully shut down the job poller.
    ///
    /// This method is idempotent and can be called multiple times safely.
    /// It will send a shutdown signal to all running jobs, wait briefly for them
    /// to complete, and reschedule any jobs still running.
    ///
    /// If not called manually, shutdown will be automatically triggered when the
    /// Jobs instance is dropped.
    #[instrument(name = "job.shutdown", skip(self))]
    pub async fn shutdown(&self) -> Result<(), JobError> {
        if let Some(handle) = &self.poller_handle {
            handle.shutdown().await?;
        }
        Ok(())
    }
}
