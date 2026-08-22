//! Live-PG coverage for the pool-congestion classification
//! (`error::is_pool_congestion`, `JobDispatcher::reschedule_congestion`) on
//! the single-job (non-batched) path.
//!
//! Deliberately a separate file from `pool_congestion.rs` (the batched
//! counterpart): a solo job has no batchability to preserve, so what this
//! asserts is narrower -- attempt stays unchanged and completion isn't
//! delayed past a `RetrySettings` attempt cap.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    JobTerminalState, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    label: String,
}

/// A runner that fails its FIRST invocation with `sqlx::Error::PoolTimedOut`
/// and completes every subsequent invocation. Failing by call-count rather
/// than by `attempt` is deliberate: a congestion reschedule must NOT bump
/// `attempt` (that's exactly what this test asserts), so gating on
/// `attempt == 1` would fail forever.
struct CongestionOnceRunner {
    /// Every call's attempt number, in call order. Two calls with the SAME
    /// (unchanged) attempt number proves `attempt_index` was left alone --
    /// else the second call would read 2, not 1.
    calls: Arc<Mutex<Vec<u32>>>,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for CongestionOnceRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.calls.lock().await.push(current_job.attempt());

        if self.invocations.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Box::new(sqlx::Error::PoolTimedOut));
        }
        Ok(JobCompletion::Complete)
    }
}

struct CongestionOnceInitializer {
    job_type: JobType,
    calls: Arc<Mutex<Vec<u32>>>,
    invocations: Arc<AtomicUsize>,
}

impl JobInitializer for CongestionOnceInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        // Small `n_attempts`: if the congestion classification regressed and
        // this went through the ordinary retry policy instead, the job
        // would burn an attempt and this cap makes that failure mode
        // terminate (and therefore be caught by the completion assertion
        // below) instead of quietly retrying its way to success.
        RetrySettings {
            n_attempts: Some(2),
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &job::Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(CongestionOnceRunner {
            calls: Arc::clone(&self.calls),
            invocations: Arc::clone(&self.invocations),
        }))
    }
}

/// A solo job that fails once with `PoolTimedOut` must reschedule and
/// complete on its next dispatch WITHOUT ever going through `RetrySettings`'
/// `n_attempts` cap, and WITHOUT its `attempt_index` moving -- the
/// single-job counterpart of `pool_congestion.rs`'s
/// `congestion_reschedule_keeps_job_batchable`.
#[tokio::test]
async fn congestion_reschedule_preserves_attempt() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let invocations = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(CongestionOnceInitializer {
        job_type: helpers::job_type("pool-congestion-solo"),
        calls: Arc::clone(&calls),
        invocations: Arc::clone(&invocations),
    });

    let id = JobId::new();
    spawner
        .spawn(
            id,
            Cfg {
                label: "congestion".to_string(),
            },
        )
        .await?;

    jobs.start_poll().await?;

    // The congestion reschedule delay (2s +/- 1s jitter) is real time this
    // test has to wait through -- bounded, not blind: `await_all` returns
    // the instant the job reaches a terminal state, rather than sleeping a
    // fixed guess.
    let outcomes = jobs
        .handles(vec![id])
        .await_all(Duration::from_secs(20))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "job should complete after the congestion reschedule, got {outcomes:?}"
    );

    let calls = calls.lock().await;
    assert_eq!(
        calls.as_slice(),
        [1, 1],
        "expected exactly one congestion failure and one successful retry, both \
         at attempt 1 -- if the second entry isn't 1, `attempt_index` was bumped \
         by the congestion reschedule; saw {calls:?}"
    );

    Ok(())
}
