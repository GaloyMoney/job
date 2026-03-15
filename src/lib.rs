//! `job` is an async, SQLite-backed job scheduler and runner for Rust
//! applications. It coordinates distributed workers, tracks job history, and
//! handles retries with predictable backoff.

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
mod tracker;

pub mod error;

use es_entity::AtomicOperation;
use tracing::instrument;

use std::sync::{Arc, Mutex};

pub use config::*;
pub use current::*;
pub use entity::{Job, JobType};
pub use es_entity::clock::{
    ArtificialClockConfig, ArtificialMode, Clock, ClockController, ClockHandle,
};
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
    clock: ClockHandle,
}

impl Jobs {
    /// Initialize the service using a [`JobSvcConfig`] for connection and runtime settings.
    pub async fn init(config: JobSvcConfig) -> Result<Self, JobError> {
        let pool = match (config.pool.clone(), config.db_con.clone()) {
            (Some(pool), None) => pool,
            (None, Some(db_con)) => {
                let mut pool_opts = sqlx::sqlite::SqlitePoolOptions::new();
                if let Some(max_connections) = config.max_connections {
                    pool_opts = pool_opts.max_connections(max_connections);
                }
                pool_opts.connect(&db_con).await?
            }
            _ => {
                return Err(JobError::Config(
                    "One of db_con or pool must be set".to_string(),
                ));
            }
        };

        if config.exec_migrations {
            sqlx::migrate!().run(&pool).await?;
        }

        let repo = Arc::new(JobRepo::new(&pool));
        let registry = Arc::new(Mutex::new(Some(JobRegistry::new())));
        let clock = config.clock.clone();
        Ok(Self {
            repo,
            config,
            registry,
            poller_handle: None,
            clock,
        })
    }

    /// Start the background poller that fetches and dispatches jobs.
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
                Arc::clone(&self.repo),
                registry,
                self.clock.clone(),
            )
            .start()
            .await?,
        ));
        Ok(())
    }

    /// Register a [`JobInitializer`] and return a [`JobSpawner`] for creating jobs.
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
        JobSpawner::new(Arc::clone(&self.repo), job_type, self.clock.clone())
    }

    /// Fetch the current snapshot of a job entity by identifier.
    #[instrument(name = "job.find", skip(self))]
    pub async fn find(&self, id: JobId) -> Result<Job, JobError> {
        Ok(self.repo.find_by_id(id).await?)
    }

    /// Cancel a pending job, removing it from the execution queue.
    ///
    /// This operation is idempotent — calling it on an already cancelled or
    /// completed job is a no-op. If the job exists but is currently running
    /// (not pending), returns [`JobError::CannotCancelJob`].
    #[instrument(name = "job.cancel_job", skip(self))]
    pub async fn cancel_job(&self, id: JobId) -> Result<(), JobError> {
        let mut op = self.repo.begin_op_with_clock(&self.clock).await?;
        let mut job = self.repo.find_by_id(id).await?;

        if job.cancel().did_execute() {
            let result: sqlx::sqlite::SqliteQueryResult =
                sqlx::query("DELETE FROM job_executions WHERE id = ? AND state = 'pending'")
                    .bind(id.to_string())
                    .execute(op.as_executor())
                    .await?;

            if result.rows_affected() == 0 {
                return Err(JobError::CannotCancelJob);
            }

            self.repo.update_in_op(&mut op, &mut job).await?;
            op.commit().await?;
        }

        Ok(())
    }

    /// Returns a reference to the clock used by this job service.
    pub fn clock(&self) -> &ClockHandle {
        &self.clock
    }

    /// Gracefully shut down the job poller.
    #[instrument(name = "job.shutdown", skip(self), err)]
    pub async fn shutdown(&self) -> Result<(), JobError> {
        if let Some(handle) = &self.poller_handle {
            handle.shutdown().await?;
        }
        Ok(())
    }
}
