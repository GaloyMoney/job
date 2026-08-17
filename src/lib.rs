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
//!   configurable through [`JobPollerConfig`], plus per-type concurrency caps
//!   via [`JobInitializer::max_concurrent_per_process`] /
//!   [`max_concurrent_global`](JobInitializer::max_concurrent_global).
//! - Two singleton *flavors* beyond the default: keyed jobs — at most one
//!   LIVE job per `(job_type, key)`, respawnable once terminal — via
//!   [`KeyedJobSpawner`], and resident jobs — at most one job per type,
//!   ever, that never terminates — via [`ResidentJobSpawner`].
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
//! ## Batched execution
//!
//! Job types whose work is dominated by per-job transaction overhead — the
//! common "an event spawns a command job that mutates one entity" shape — can
//! opt into *batched* execution with [`Jobs::add_batched_initializer`]. The
//! poller then hands a [`BatchedJobRunner`] every job of that type it claimed in
//! one poll, so K jobs cost one transaction and one commit instead of K.
//!
//! ```ignore
//! impl BatchedJobInitializer for RevalueInitializer {
//!     type Config = RevalueConfig;
//!
//!     fn job_type(&self) -> JobType { JobType::new("command.revalue") }
//!     fn max_batch_size(&self) -> usize { 25 }
//!
//!     fn init(&self, _: JobSpawner<Self::Config>)
//!         -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn Error>>
//!     {
//!         Ok(Box::new(RevalueRunner { accounts: self.accounts.clone() }))
//!     }
//! }
//!
//! #[async_trait]
//! impl BatchedJobRunner for RevalueRunner {
//!     type Config = RevalueConfig;
//!
//!     async fn run_batch(&self, batch: CurrentBatchedJob<RevalueConfig>)
//!         -> Result<JobBatchCompletion, Box<dyn Error>>
//!     {
//!         let mut op = batch.begin_op().await?;
//!         for item in batch.items() {
//!             self.accounts.revalue_in_op(&mut op, item.config().account_id).await?;
//!         }
//!         Ok(JobBatchCompletion::CompleteAllWithOp(op))
//!     }
//! }
//! ```
//!
//! **Spawning and awaiting are unchanged.** Batched types use the same
//! [`JobSpawner`] and resolve through the same
//! [`await_completion`](Jobs::await_completion) — whether a job ran in a batch
//! is invisible from both sides.
//!
//! Points worth knowing before opting in:
//!
//! - **At most one job per `queue_id` is ever in the same batch**, because the
//!   poll query already claims at most one row per queue. Items arrive sorted by
//!   `queue_id` so concurrent batches take domain locks in a consistent order.
//! - **A batch of one is normal.** Under light load batches are size 1; write
//!   `run_batch` to be correct at any length.
//! - **Per-job outcomes** are available via
//!   [`JobBatchCompletion::WithOutcomes`]: complete some, reschedule others,
//!   fail the rest, all in one commit. Every job must get exactly one outcome —
//!   the dispatcher rejects a partial set rather than guessing.
//! - **Returning `Err` fails the whole batch**, retrying each job under the
//!   type's [`RetrySettings`]. Jobs on a second or later attempt are never
//!   batched, so a persistently failing job ends up retrying alone.
//! - **A running batch costs one unit** of `max_jobs_per_process`, not one per
//!   job — a batch is one task, one transaction, one connection.
//! - **Claims are throttled by free batch slots.** A type may have
//!   `max_concurrent_per_process` (default 2) batches running per process, and
//!   the poll query claims at most `max_batch_size × free slots` rows for it.
//!   Rows are therefore only locked when a batch is free to start on them: the
//!   rest of a backlog stays `pending` — claimable by other poller instances,
//!   and accumulating into fuller later batches. Raise the slot count to trade
//!   locked rows for more concurrency on a hot type.
//! - **Keep external calls out of batched runners.** A shared transaction held
//!   across HTTP or mail delivery is a liability; leave those types on
//!   [`JobRunner`].
//!
//! ## Per-type concurrency limits
//!
//! [`JobInitializer::max_concurrent_per_process`] bounds how many jobs of one
//! type may execute concurrently in THIS process; while every slot is busy the
//! poller stops claiming rows of that type and the backlog stays `pending` —
//! cheap database rows, visible to other poller instances — instead of
//! pinning executor slots, connections, and memory. This is the direct
//! defense against a slow external dependency (a hanging HTTP call, say)
//! confiscating [`JobPollerConfig::max_jobs_per_process`] from every other
//! job type.
//!
//! [`JobInitializer::max_concurrent_global`] additionally bounds the type's
//! concurrency across ALL poller instances — useful for rate-limiting a
//! downstream dependency itself ("at most N concurrent calls to service X").
//! It is **soft**: each instance pre-counts the type's running executions just
//! before claiming, so concurrent polls across instances can transiently
//! overshoot; steady-state stays at or below the cap. For a hard bound, pair
//! it with `max_concurrent_per_process` (hard bound = per-process cap ×
//! instance count).
//!
//! ```ignore
//! impl JobInitializer for KeycloakSyncInitializer {
//!     // ...
//!     fn max_concurrent_per_process(&self) -> Option<usize> { Some(20) }
//!     fn max_concurrent_global(&self) -> Option<usize> { Some(50) }
//! }
//! ```
//!
//! A capped type's backlog is observable the same way any pending backlog is:
//! via the crate's existing stale-pending warnings
//! (`job.check_stale_pending_jobs`), which fire regardless of *why* a type's
//! rows are sitting `pending`.
//!
//! ## Job flavors
//!
//! Every job is one of three **flavors** — regular (what [`JobInitializer`]
//! registers, optionally [`BatchedJobInitializer`] or spawned with a
//! `queue_id`), keyed, or resident. "Flavor" is this crate's umbrella term
//! for the three whenever a doc comment or diagnostic needs to name them
//! collectively.
//!
//! | flavor | admission invariant | terminates? | id |
//! |---|---|---|---|
//! | regular | none (`queue_id` = at most one RUNNING per queue at a time — a scheduling-time mutual exclusion, not a FIFO ordering guarantee) | yes | caller-chosen |
//! | keyed | at most one LIVE job per `(job_type, key)`; respawnable after terminal | yes | internal |
//! | resident | at most one job per `job_type`, EVER | cannot (no `Complete` variant exists) | internal |
//!
//! ### Keyed jobs
//!
//! For at-most-one-live-per-key semantics, register with
//! [`Jobs::add_keyed_initializer`] and spawn with [`KeyedJobSpawner::spawn`]:
//! at most one LIVE (pending/running) job of `(job_type, key)` at a time. A
//! key becomes respawnable once its job reaches a terminal state — the next
//! spawn creates a new generation under the same key. Enumerate every key of
//! a type — the entry point for a "have my listeners caught up?" check —
//! with [`Jobs::keyed_handles`].
//!
//! ```ignore
//! let shard_spawner = jobs.add_keyed_initializer(ShardListenerInitializer);
//!
//! // One job per shard; re-spawning a live shard is a no-op that returns
//! // the existing job's handle. Once a shard's job goes terminal, spawning
//! // it again starts a new generation.
//! shard_spawner.spawn(format!("shard-{shard_id}"), ShardConfig { shard_id }).await?;
//! ```
//!
//! A keyed job's execution state (see [`CurrentJob::update_execution_state`])
//! outlives its own generation: once a generation goes terminal its state
//! row is retained (readable via [`JobSnapshot::execution_state`]) rather
//! than deleted, and is compacted away at the key's next spawn. Set
//! [`KeyedJobInitializer::inherits_state`] to seed a new generation's state
//! from its predecessor's — useful when a respawn should resume from a
//! checkpoint instead of starting cold.
//!
//! ### Resident jobs
//!
//! For a process-wide singleton that just keeps running — a poller, a
//! periodic sweep — register with [`Jobs::add_resident_initializer`] and
//! spawn with [`ResidentJobSpawner::spawn`]: absolutely unique, at most one
//! job of a type EVER exists. Once created it can never be spawned again —
//! there is no respawn/new-generation escape hatch, unlike keyed jobs — and
//! it can never complete either: [`ResidentJobRunner::run`] returns
//! [`ResidentJobCompletion`], a reschedule-only mirror of [`JobCompletion`]
//! with no `Complete` variant, so a resident job finishing for good is a
//! compile error, not a runtime surprise. Registration also forces eternal
//! retry regardless of the initializer's settings, for the same reason.
//! `spawn` consumes the spawner, enforcing at the type level that only one
//! job of this type can be created:
//!
//! ```ignore
//! let cleanup_spawner = jobs.add_resident_initializer(CleanupInitializer);
//!
//! // Consumes spawner - can't accidentally spawn twice
//! cleanup_spawner.spawn(CleanupConfig::default()).await?;
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

mod batch_dispatcher;
mod batched;
mod config;
mod current;
mod dispatcher;
mod entity;
mod handle;
mod job_execution;
mod keyed;
mod migrate;
mod notification_router;
mod notifier;
mod outcome;
mod poller;
mod registry;
mod repo;
mod resident;
mod runner;
mod snapshot;
mod spawner;
mod task;
mod tracker;

pub mod error;

use tracing::instrument;

use std::sync::{Arc, Mutex};

pub use batched::{
    BatchItemOutcome, BatchOutcomes, BatchedJobInitializer, BatchedJobItem, BatchedJobRunner,
    CurrentBatchedJob, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_BATCHES, JobBatchCompletion,
};
pub use config::*;
pub use current::*;
pub use entity::{Job, JobEvent, JobType};
pub use error::JobError;
pub use es_entity::clock::{Clock, ClockController, ClockHandle};
pub use handle::{JobHandle, JobHandles};
pub use job_execution::JobStatus;
pub use keyed::{KeyedJobInitializer, KeyedJobSpawner};
pub use migrate::*;
pub use outcome::{JobOutcome, JobOutcomes, JobReturnValue, JobTerminalState};
pub use registry::*;
pub use resident::{
    ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner, ResidentJobSpawner,
};
pub use runner::*;
pub use snapshot::JobSnapshot;
pub use spawner::*;

use notification_router::*;
use poller::*;
use repo::*;
use tracker::*;

es_entity::entity_id! { JobId }

#[derive(Clone)]
/// Primary entry point for interacting with the Job crate. Provides APIs to register job
/// handlers, manage configuration, and control scheduling and execution.
pub struct Jobs {
    config: JobSvcConfig,
    repo: Arc<JobRepo>,
    registry: Arc<Mutex<Option<JobRegistry>>>,
    router: Arc<JobNotificationRouter>,
    tracker: Arc<JobTracker>,
    notifier: Arc<notifier::JobEventNotifier>,
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
        let tracker = Arc::new(JobTracker::new(
            config.poller_config.min_jobs_per_process,
            config.poller_config.max_jobs_per_process,
        ));
        let registry = Arc::new(Mutex::new(Some(JobRegistry::new(Arc::clone(&tracker)))));
        let router = Arc::new(JobNotificationRouter::new(
            &pool,
            Arc::clone(&repo),
            config.poller_config.terminal_channel_size,
            config.poller_config.sweep_interval,
        ));
        let notifier = notifier::JobEventNotifier::spawn(
            &pool,
            Arc::clone(&tracker),
            router.terminal_sender(),
        );
        let clock = config.clock.clone();
        Ok(Self {
            repo,
            config,
            registry,
            router,
            tracker,
            notifier,
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

        let tracker = Arc::clone(&self.tracker);
        // Read before `registry` moves into the poller below — capped_types()
        // is a cheap scan of the registry's own concurrency map, computed
        // here on the owned value while it's still available.
        tracker.set_capped_types(registry.capped_types().into_iter().collect());

        let poller = JobPoller::new(
            self.config.poller_config.clone(),
            Arc::clone(&self.repo),
            registry,
            Arc::clone(&tracker),
            Arc::clone(&self.router),
            Arc::clone(&self.notifier),
            self.clock.clone(),
        );

        tracker.set_job_types(poller.registered_job_types());

        let (listener_handle, waiter_handle) = self.router.start(Arc::clone(&tracker)).await?;

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
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )
    }

    /// Register a [`BatchedJobInitializer`] and return a [`JobSpawner`] for
    /// creating jobs of that type.
    ///
    /// Jobs of a batched type are executed many-at-a-time by a
    /// [`BatchedJobRunner`] instead of one task per job. **Spawning and
    /// awaiting are unchanged** — the returned spawner is the same one
    /// [`add_initializer`](Self::add_initializer) returns, and awaiting through
    /// a [`JobHandle`] resolves per job exactly as before — so whether a job
    /// ran in a batch is invisible to both sides.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let spawner = jobs.add_batched_initializer(MyBatchedInitializer);
    /// spawner.spawn_with_queue_id(JobId::new(), cfg, entity_id.to_string()).await?;
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called after [`start_poll`](Self::start_poll).
    pub fn add_batched_initializer<I: BatchedJobInitializer>(
        &mut self,
        initializer: I,
    ) -> JobSpawner<I::Config> {
        let job_type = {
            let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
            registry
                .as_mut()
                .expect("Registry has been consumed by executor")
                .add_batched_initializer(initializer)
        };
        JobSpawner::new(
            Arc::clone(&self.repo),
            job_type,
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )
    }

    /// Register a [`KeyedJobInitializer`] and return a [`KeyedJobSpawner`]
    /// for creating jobs of that type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let spawner = jobs.add_keyed_initializer(ShardListenerInitializer);
    /// spawner.spawn(format!("shard-{shard_id}"), ShardConfig { shard_id }).await?;
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called after [`start_poll`](Self::start_poll).
    pub fn add_keyed_initializer<I: KeyedJobInitializer>(
        &mut self,
        initializer: I,
    ) -> KeyedJobSpawner<I::Config> {
        // Read before `initializer` moves into the registry below — the
        // returned spawner needs its own copy to drive the seed/compact
        // machinery in `KeyedJobSpawner::spawn`.
        let inherits_state = initializer.inherits_state();
        let job_type = {
            let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
            registry
                .as_mut()
                .expect("Registry has been consumed by executor")
                .add_keyed_initializer(initializer)
        };
        KeyedJobSpawner::new(
            Arc::clone(&self.repo),
            job_type,
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
            inherits_state,
        )
    }

    /// Register a [`ResidentJobInitializer`] and return a
    /// [`ResidentJobSpawner`] for creating the (at most one) job of that
    /// type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let spawner = jobs.add_resident_initializer(CleanupInitializer);
    /// spawner.spawn(CleanupConfig::default()).await?;
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called after [`start_poll`](Self::start_poll).
    pub fn add_resident_initializer<I: ResidentJobInitializer>(
        &mut self,
        initializer: I,
    ) -> ResidentJobSpawner<I::Config> {
        let job_type = {
            let mut registry = self.registry.lock().expect("Couldn't lock Registry Mutex");
            registry
                .as_mut()
                .expect("Registry has been consumed by executor")
                .add_resident_initializer(initializer)
        };
        ResidentJobSpawner::new(
            Arc::clone(&self.repo),
            job_type,
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )
    }

    /// Mint a [`JobHandle`] for `id` — cheap and non-validating.
    ///
    /// No I/O happens until a method on the handle is called; handles hold no
    /// cached state, so every read is a live committed read. The id does not
    /// need to belong to an existing job: [`JobHandle::load`] and the
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
    /// let snapshot = handle.load().await?;
    /// let _state = snapshot.state();
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
    /// [`JobHandles::load_all`]) align positionally with the input ids.
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

    /// Mint a [`JobHandle`] for the resident job of `job_type`, if one
    /// exists.
    ///
    /// The counterpart of [`handle`](Self::handle) for resident jobs (see
    /// [`ResidentJobSpawner::spawn`]): unlike `handle(id)`, this resolves
    /// the persisted job's id from the database, so it is async and returns
    /// `None` when no resident job of that type has been spawned.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the lookup fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use job::{
    /// #     Jobs, JobSvcConfig, Job, JobId, JobType, ResidentJobInitializer,
    /// #     ResidentJobRunner, ResidentJobCompletion, CurrentJob,
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
    /// # impl ResidentJobInitializer for MyInitializer {
    /// #     type Config = MyConfig;
    /// #     fn job_type(&self) -> JobType { JobType::new("cleanup") }
    /// #     fn init(&self, _job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn Error>> {
    /// #         Ok(Box::new(MyRunner))
    /// #     }
    /// # }
    /// # struct MyRunner;
    /// # #[async_trait]
    /// # impl ResidentJobRunner for MyRunner {
    /// #     async fn run(&self, _current_job: CurrentJob) -> Result<ResidentJobCompletion, Box<dyn Error>> {
    /// #         Ok(ResidentJobCompletion::RescheduleIn(Duration::from_secs(60)))
    /// #     }
    /// # }
    /// # async fn example() -> Result<(), JobError> {
    /// # let pool = PgPoolOptions::new()
    /// #     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
    /// # let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    /// # let mut jobs = Jobs::init(config).await?;
    /// # let spawner = jobs.add_resident_initializer(MyInitializer);
    /// # jobs.start_poll().await?;
    /// // Observe the (at most one) cleanup job without knowing its id.
    /// if let Some(handle) = jobs.resident_handle(JobType::new("cleanup")).await? {
    ///     let snapshot = handle.load().await?;
    ///     let _state = snapshot.state();
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(name = "job.resident_handle", skip(self))]
    pub async fn resident_handle(
        &self,
        job_type: impl Into<JobType> + std::fmt::Debug,
    ) -> Result<Option<JobHandle>, JobError> {
        let id = self.repo.find_resident_id(&job_type.into()).await?;
        Ok(id.map(|id| self.handle(id)))
    }

    /// Mint a [`JobHandle`] for the keyed job of `(job_type, key)`, if one
    /// exists (see [`KeyedJobSpawner::spawn`]). Resolves the LIVE generation
    /// when one exists, else the latest terminal generation. `None` when no
    /// job of that type has ever been spawned under `key`.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the lookup fails.
    #[instrument(name = "job.keyed_handle", skip(self))]
    pub async fn keyed_handle(
        &self,
        job_type: impl Into<JobType> + std::fmt::Debug,
        key: impl AsRef<str> + std::fmt::Debug,
    ) -> Result<Option<JobHandle>, JobError> {
        let job = self.repo.find_keyed(&job_type.into(), key.as_ref()).await?;
        Ok(job.map(|job| self.handle(job.id)))
    }

    /// Every keyed job of `job_type`, ordered by key — one handle per key
    /// (the live-or-latest generation). The entry point for "have my
    /// listeners caught up?": [`JobHandles::load_all`] batch-loads every
    /// job's [`JobSnapshot`], and [`JobSnapshot::unique_key`] reads the key
    /// back off each one — no separate bookkeeping of which handle is which
    /// key.
    ///
    /// Returns a plain [`JobHandles`], the same type [`Jobs::handles`]
    /// returns: `await_all`'s [`JobOutcome`] doesn't carry the key (it's
    /// deliberately minimal — just state and result), so if you need the key
    /// alongside an *outcome* rather than a snapshot, zip it against the keys
    /// you already hold.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Query`] if the lookup fails.
    #[instrument(name = "job.keyed_handles", skip(self))]
    pub async fn keyed_handles(
        &self,
        job_type: impl Into<JobType> + std::fmt::Debug,
    ) -> Result<JobHandles, JobError> {
        let ids = self
            .repo
            .list_keyed_ids_by_job_type(&job_type.into())
            .await?;
        Ok(ids.into_iter().map(|(_, id)| self.handle(id)).collect())
    }

    /// Returns a reference to the clock used by this job service.
    pub fn clock(&self) -> &ClockHandle {
        &self.clock
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
