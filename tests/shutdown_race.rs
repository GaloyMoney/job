//! Regression tests for the shutdown/`set_result` concurrent-modification race.
//!
//! `Jobs::shutdown`'s kill path used to load job entities on a second
//! connection while holding an execution-row claim in its own op; a handler's
//! `set_result` committing in that window made the kill's append collide on
//! `job_events (id, sequence)` and `shutdown()` returned
//! `JobError::Modify: ConcurrentModification` — observed end-to-end in lana's
//! `facility_matures_on_end_of_day` (the maturity job `set_result`s its failed
//! count exactly as the test tears the poller down).

use std::time::Duration;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobStatus, JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};

mod helpers;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BusyJobConfig {
    iterations: u64,
}

/// A runner that hammers `set_result` with a fresh value on every iteration —
/// each call appends a `ReturnValueUpdated` event, i.e. exactly the concurrent
/// entity write the shutdown killer used to collide with. Parks briefly at the
/// end so the shutdown timeout (10ms) reliably expires mid-flight.
struct BusyJobRunner {
    config: BusyJobConfig,
}

#[async_trait]
impl JobRunner for BusyJobRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        for i in 0..self.config.iterations {
            current_job.set_result(&i).await?;
            tokio::task::yield_now().await;
        }
        Ok(JobCompletion::Complete)
    }
}

struct BusyJobInitializer;

impl JobInitializer for BusyJobInitializer {
    type Config = BusyJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("busy-set-result-job")
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(BusyJobRunner {
            config: job.config()?,
        }))
    }
}

/// `shutdown()` must never fail with a concurrent-modification error while a
/// job is mid-`set_result`: the appends merge, the job either completes
/// gracefully or is cleanly aborted+rescheduled, and the shutdown returns Ok.
#[tokio::test]
async fn shutdown_survives_concurrent_set_result_storm() -> anyhow::Result<()> {
    // Several rounds so both interleavings (graceful completion, and kill
    // claiming a still-running row) are exercised. No assertion depends on
    // timing: each round's outcome must hold for EVERY interleaving.
    for round in 0..5 {
        let pool = helpers::init_pool().await?;
        let config = JobSvcConfig::builder()
            .pool(pool)
            .poller_config(JobPollerConfig {
                shutdown_timeout: Duration::from_millis(10),
                ..Default::default()
            })
            .build()
            .expect("Failed to build JobsConfig");

        let mut jobs = Jobs::init(config).await?;
        let spawner = jobs.add_initializer(BusyJobInitializer);
        jobs.start_poll().await?;

        let job_id = JobId::new();
        let job = spawner
            .spawn(job_id, BusyJobConfig { iterations: 2_000 })
            .await?;

        // Let the runner get well into its set_result loop, then tear the
        // poller down right on top of it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        jobs.shutdown().await?;

        // Whatever interleaving won: the job is either terminal, or cleanly
        // rescheduled (aborted, pending, unowned) for a later poller — never
        // wedged running with no owner.
        let snapshot = jobs.handle(job.id).load().await?;
        match snapshot.state() {
            JobStatus::Running { .. } => {
                panic!("round {round}: job must not be left Running after shutdown");
            }
            JobStatus::Pending { .. } | JobStatus::Completed { .. } | JobStatus::Errored { .. } => {
            }
        }
    }
    Ok(())
}
