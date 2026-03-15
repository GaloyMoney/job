//! Service and poller configuration types.

use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};
use es_entity::db;
use serde::{Deserialize, Serialize};

use std::time::Duration;

#[serde_with::serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
/// Controls how the background poller balances work across processes.
pub struct JobPollerConfig {
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_job_lost_interval")]
    /// How long a job may be in a 'running' state
    pub job_lost_interval: Duration,
    #[serde(default = "default_max_jobs_per_process")]
    /// Maximum number of concurrent jobs this process will execute.
    pub max_jobs_per_process: usize,
    #[serde(default = "default_min_jobs_per_process")]
    /// Minimum number of concurrent jobs to keep running before the poller sleeps.
    pub min_jobs_per_process: usize,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_shutdown_timeout")]
    /// How long to wait for jobs to complete gracefully during shutdown before rescheduling them.
    pub shutdown_timeout: Duration,
}

impl Default for JobPollerConfig {
    fn default() -> Self {
        Self {
            job_lost_interval: default_job_lost_interval(),
            max_jobs_per_process: default_max_jobs_per_process(),
            min_jobs_per_process: default_min_jobs_per_process(),
            shutdown_timeout: default_shutdown_timeout(),
        }
    }
}

#[derive(Builder, Debug, Clone)]
#[builder(build_fn(skip))]
/// Configuration consumed by [`Jobs::init`](crate::Jobs::init).
/// Build with [`JobSvcConfig::builder`](Self::builder).
pub struct JobSvcConfig {
    #[builder(setter(into, strip_option), default)]
    /// Provide a SQLite connection string used to build an internal pool. Mutually exclusive with `pool`.
    /// When set, `exec_migrations` defaults to `true` unless overridden.
    pub(super) db_con: Option<String>,
    #[builder(setter(into, strip_option), default)]
    /// Override the maximum number of connections the internally managed pool may open. Ignored when `pool` is supplied.
    pub(super) max_connections: Option<u32>,
    #[builder(default)]
    /// Set to `true` to have `Jobs::init` run the embedded database migrations during startup.
    /// Defaults to `false`, unless `db_con` is supplied without an explicit value.
    pub(super) exec_migrations: bool,
    #[builder(setter(into, strip_option), default)]
    /// Inject an existing pool instead of letting the job service build one. Mutually exclusive with `db_con`.
    pub(super) pool: Option<db::Pool>,
    #[builder(default)]
    /// Override the defaults that control how the background poller distributes work across processes.
    pub poller_config: JobPollerConfig,
    #[builder(setter(into), default = "Clock::handle()")]
    /// Clock handle for time operations. Defaults to the global clock.
    /// The global clock is realtime unless an artificial clock was installed.
    pub clock: ClockHandle,
}

impl JobSvcConfig {
    /// Create a [`JobSvcConfigBuilder`] with defaults for all optional settings.
    pub fn builder() -> JobSvcConfigBuilder {
        JobSvcConfigBuilder::default()
    }
}

impl JobSvcConfigBuilder {
    /// Validate and construct a [`JobSvcConfig`], ensuring either `db_con` or `pool` is set.
    pub fn build(&mut self) -> Result<JobSvcConfig, String> {
        // Validate configuration
        match (self.db_con.as_ref(), self.pool.as_ref()) {
            (None, None) | (Some(None), None) | (None, Some(None)) => {
                return Err("One of db_con or pool must be set".to_string());
            }
            (Some(_), Some(_)) => return Err("Only one of db_con or pool must be set".to_string()),
            _ => (),
        }

        // If db_con is provided and exec_migrations is not explicitly set, default to true
        if matches!(self.db_con.as_ref(), Some(Some(_))) && self.exec_migrations.is_none() {
            self.exec_migrations = Some(true);
        }

        Ok(JobSvcConfig {
            db_con: self.db_con.clone().flatten(),
            max_connections: self.max_connections.flatten(),
            exec_migrations: self.exec_migrations.unwrap_or(false),
            pool: self.pool.clone().flatten(),
            poller_config: self.poller_config.clone().unwrap_or_default(),
            clock: self
                .clock
                .clone()
                .unwrap_or_else(|| Clock::handle().clone()),
        })
    }
}

fn default_job_lost_interval() -> Duration {
    Duration::from_secs(60 * 5)
}

fn default_max_jobs_per_process() -> usize {
    50
}

fn default_min_jobs_per_process() -> usize {
    30
}

fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(5)
}
