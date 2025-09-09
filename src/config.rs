use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use std::time::Duration;

#[serde_with::serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobPollerConfig {
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_job_lost_interval")]
    pub job_lost_interval: Duration,
    #[serde(default = "default_max_jobs_per_process")]
    pub max_jobs_per_process: usize,
    #[serde(default = "default_min_jobs_per_process")]
    pub min_jobs_per_process: usize,
}

impl Default for JobPollerConfig {
    fn default() -> Self {
        Self {
            job_lost_interval: default_job_lost_interval(),
            max_jobs_per_process: default_max_jobs_per_process(),
            min_jobs_per_process: default_min_jobs_per_process(),
        }
    }
}

#[derive(Builder, Debug, Clone)]
#[builder(build_fn(skip))]
pub struct JobSvcConfig {
    #[builder(setter(into, strip_option), default)]
    pub(super) pg_con: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub(super) max_connections: Option<u32>,
    #[builder(default)]
    pub(super) exec_migrations: bool,
    #[builder(setter(into, strip_option), default)]
    pub(super) pool: Option<sqlx::PgPool>,
    #[builder(default)]
    pub process_config: JobPollerConfig,
}

impl JobSvcConfig {
    pub fn builder() -> JobSvcConfigBuilder {
        JobSvcConfigBuilder::default()
    }
}

impl JobSvcConfigBuilder {
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

        Ok(JobSvcConfig {
            pg_con: self.pg_con.clone().flatten(),
            max_connections: self.max_connections.flatten(),
            exec_migrations: self.exec_migrations.unwrap_or(false),
            pool: self.pool.clone().flatten(),
            process_config: self.process_config.clone().unwrap_or_default(),
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
