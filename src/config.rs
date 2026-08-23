//! Service and poller configuration types.

use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};
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
    #[serde(default = "default_terminal_channel_size")]
    /// Capacity of the broadcast channel used to propagate terminal-job notifications.
    pub terminal_channel_size: usize,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_sweep_interval")]
    /// How often the waiter-manager reconciles registered completion-waiters
    /// against the database. This is the backstop that resolves waiters whose
    /// terminal notification was dropped (e.g. broadcast overflow), so it must
    /// run on a predictable cadence.
    pub sweep_interval: Duration,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_pending_jobs_check_interval")]
    /// How often to check for pending jobs that are past their scheduled execution time.
    pub pending_jobs_check_interval: Duration,
    #[serde(default = "default_connections_per_job")]
    /// How many shared-pool connections one dispatched job is assumed to
    /// cost, for pool-aware claim admission: each poll's dispatch budget is
    /// `live pool headroom / connections_per_job`, rounded down. A "job"
    /// here is one dispatch unit -- a whole batch of a batched type counts
    /// once, not per row.
    ///
    /// Defaults to `1.0`, which prices exactly what the crate's own
    /// claim/dispatch machinery holds. Deliberately fractional: the crate
    /// cannot know what YOUR runners do with connections, but you can. A
    /// deployment whose jobs are mostly connection-free can set e.g. `0.5`
    /// to admit twice the headroom; one whose runners open extra
    /// connections (the non-`_in_op` convenience methods, concurrent
    /// fan-out) can set `1.5` or `2.0` to admit less and rely less on
    /// congestion reschedules. Must be finite and greater than zero
    /// (validated by [`JobSvcConfigBuilder::build`]); note that a value
    /// larger than the pool's `max_connections` means no work is ever
    /// admitted.
    pub connections_per_job: f64,
}

impl Default for JobPollerConfig {
    fn default() -> Self {
        Self {
            job_lost_interval: default_job_lost_interval(),
            max_jobs_per_process: default_max_jobs_per_process(),
            min_jobs_per_process: default_min_jobs_per_process(),
            shutdown_timeout: default_shutdown_timeout(),
            terminal_channel_size: default_terminal_channel_size(),
            sweep_interval: default_sweep_interval(),
            pending_jobs_check_interval: default_pending_jobs_check_interval(),
            connections_per_job: default_connections_per_job(),
        }
    }
}

#[derive(Builder, Debug, Clone)]
#[builder(build_fn(skip))]
/// Configuration consumed by [`Jobs::init`](crate::Jobs::init).
/// Build with [`JobSvcConfig::builder`](Self::builder).
///
/// # Examples
///
/// Build a configuration that manages its own Postgres pool from a connection string:
///
/// ```no_run
/// use job::{Jobs, JobSvcConfig};
/// use job::error::JobError;
///
/// # async fn run() -> Result<(), JobError> {
/// let config = JobSvcConfig::builder()
///     .pg_con("postgres://postgres:password@localhost/postgres")
///     .build()
///     .unwrap();
///
/// let mut jobs = Jobs::init(config).await?;
/// jobs.start_poll().await?;
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(run()).unwrap();
/// ```
///
/// Reuse an existing `sqlx::PgPool` instead:
///
/// ```no_run
/// use job::{Jobs, JobSvcConfig};
/// use job::error::JobError;
/// use sqlx::postgres::PgPoolOptions;
///
/// # async fn run() -> Result<(), JobError> {
/// let pool = PgPoolOptions::new()
///     .connect_lazy("postgres://postgres:password@localhost/postgres")?;
///
/// let config = JobSvcConfig::builder()
///     .pool(pool)
///     .exec_migrations(false) // migrations already handled elsewhere
///     .build()
///     .unwrap();
///
/// let mut jobs = Jobs::init(config).await?;
/// jobs.start_poll().await?;
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().unwrap().block_on(run()).unwrap();
/// ```
pub struct JobSvcConfig {
    #[builder(setter(into, strip_option), default)]
    /// Provide a Postgres connection string used to build an internal pool. Mutually exclusive with `pool`. When set, `exec_migrations` defaults to `true` unless overridden.
    pub(super) pg_con: Option<String>,
    #[builder(setter(into, strip_option), default)]
    /// Override the maximum number of connections the internally managed pool may open. Ignored when `pool` is supplied.
    pub(super) max_connections: Option<u32>,
    #[builder(default)]
    /// Set to `true` to have `Jobs::init` run the embedded database migrations during startup. Defaults to `false`, unless `pg_con` is supplied without an explicit value.
    pub(super) exec_migrations: bool,
    #[builder(setter(into, strip_option), default)]
    /// Inject an existing `sqlx::PgPool` instead of letting the job service build one. Mutually exclusive with `pg_con`.
    pub(super) pool: Option<sqlx::PgPool>,
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
    /// Validate and construct a [`JobSvcConfig`], ensuring either `pg_con` or `pool` is set.
    pub fn build(&mut self) -> Result<JobSvcConfig, String> {
        // Validate configuration
        match (self.pg_con.as_ref(), self.pool.as_ref()) {
            (None, None) | (Some(None), None) | (None, Some(None)) => {
                return Err("One of pg_con or pool must be set".to_string());
            }
            (Some(_), Some(_)) => return Err("Only one of pg_con or pool must be set".to_string()),
            _ => (),
        }

        // If pg_con is provided and exec_migrations is not explicitly set, default to true
        if matches!(self.pg_con.as_ref(), Some(Some(_))) && self.exec_migrations.is_none() {
            self.exec_migrations = Some(true);
        }

        if let Some(poller_config) = self.poller_config.as_ref() {
            let factor = poller_config.connections_per_job;
            // Zero/negative would turn the admission division into a claim
            // -everything (or NaN) hazard; the upper bound is a typo guard
            // (e.g. a value written in "milli-connections") -- no real
            // workload prices a single dispatch at more than a few
            // connections.
            if !(factor.is_finite() && factor > 0.0 && factor <= 100.0) {
                return Err(format!(
                    "connections_per_job must be a finite value in (0.0, 100.0], got {factor}"
                ));
            }
        }

        Ok(JobSvcConfig {
            pg_con: self.pg_con.clone().flatten(),
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

fn default_terminal_channel_size() -> usize {
    1024
}

fn default_sweep_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_pending_jobs_check_interval() -> Duration {
    Duration::from_secs(5 * 60)
}

fn default_connections_per_job() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Existing configs (no `connections_per_job` key) must keep working
    /// untouched: the field is additive with a serde default of 1.0.
    #[test]
    fn connections_per_job_serde_default_is_one() {
        let config: JobPollerConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.connections_per_job, 1.0);
        assert_eq!(JobPollerConfig::default().connections_per_job, 1.0);
    }

    /// The builder rejects factors that would break the admission division:
    /// zero, negative, non-finite, and absurdly large (typo guard).
    #[test]
    fn invalid_connections_per_job_is_rejected_at_build() {
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, 101.0] {
            let result = JobSvcConfig::builder()
                .pg_con("postgres://unused")
                .poller_config(JobPollerConfig {
                    connections_per_job: invalid,
                    ..Default::default()
                })
                .build();
            assert!(
                result.is_err(),
                "connections_per_job = {invalid} should be rejected"
            );
        }

        let ok = JobSvcConfig::builder()
            .pg_con("postgres://unused")
            .poller_config(JobPollerConfig {
                connections_per_job: 1.5,
                ..Default::default()
            })
            .build();
        assert!(ok.is_ok(), "connections_per_job = 1.5 should be accepted");
    }
}
