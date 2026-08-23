//! Live-PG coverage for the pool-congestion classification
//! (`CongestionHandler::maybe_reclassify`,
//! `BatchDispatcher::reschedule_congestion`).
//!
//! Deliberately a separate file from `batched_job.rs`: these tests assert on
//! the exact SEQUENCE and SHAPE of `run_batch` invocations (attempt numbers,
//! batch sizes) rather than just final job state, so any stray row from
//! another test's job type would corrupt the assertions the same way
//! `batched_job_bisect.rs`'s header warns about.

mod helpers;

use async_trait::async_trait;
use job::{
    BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob, JobBatchCompletion, JobId,
    JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySettings,
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

/// A runner that fails its FIRST invocation with
/// `sqlx::Error::PoolTimedOut` (the client-side "no connection available"
/// error `CongestionHandler::maybe_reclassify` classifies as congestion, never a
/// database-side error a real runner's body could hit) and completes
/// every subsequent invocation. Failing by call-count rather than by
/// `attempt` is deliberate: a congestion reschedule must NOT bump
/// `attempt` (that's exactly what this test asserts), so gating on
/// `attempt == 1` would fail forever.
struct CongestionOnceRunner {
    /// Every call's items' attempt numbers, in call order. Two calls with
    /// the SAME batch size and the SAME (unchanged) attempt numbers proves
    /// both halves of the congestion contract at once: `attempt_index` was
    /// left alone (else the second call's attempts would read 2, not 1),
    /// and the retry-solo rule did not fire on it (else N items at
    /// attempt 1 would show up as N separate one-item calls on the retry,
    /// not one N-item call).
    calls: Arc<Mutex<Vec<Vec<u32>>>>,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl BatchedJobRunner for CongestionOnceRunner {
    type Config = Cfg;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<Cfg>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let attempts: Vec<u32> = current_batch.items().iter().map(|i| i.attempt()).collect();
        self.calls.lock().await.push(attempts);

        if self.invocations.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Box::new(sqlx::Error::PoolTimedOut));
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

struct CongestionOnceInitializer {
    job_type: JobType,
    calls: Arc<Mutex<Vec<Vec<u32>>>>,
    invocations: Arc<AtomicUsize>,
}

impl BatchedJobInitializer for CongestionOnceInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        // Small `n_attempts`: if the congestion classification regressed
        // and this went through the ordinary retry policy instead, the
        // batch would burn an attempt and this cap makes that failure mode
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
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(CongestionOnceRunner {
            calls: Arc::clone(&self.calls),
            invocations: Arc::clone(&self.invocations),
        }))
    }
}

/// A batch that fails once with `PoolTimedOut` must reschedule and complete
/// on its next dispatch WITHOUT ever going through `RetrySettings`'
/// `n_attempts` cap, and WITHOUT losing its batching: the whole point of
/// leaving `attempt_index` unchanged is that the retry-solo rule
/// (`poller.rs`, `attempt > 1` is dispatched alone) never sees this batch as
/// a retry at all.
#[tokio::test]
async fn congestion_reschedule_keeps_job_batchable() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let invocations = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_batched_initializer(CongestionOnceInitializer {
        job_type: helpers::job_type("pool-congestion-batchable"),
        calls: Arc::clone(&calls),
        invocations: Arc::clone(&invocations),
    });

    // Three jobs, spawned together in one transaction (`spawn_all`) so the
    // poller's per-queue windowing claims them as one batch -- a lone job
    // can't distinguish "still batchable" from "dispatched solo".
    const N: usize = 3;
    let specs: Vec<JobSpec<Cfg>> = (0..N)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                Cfg {
                    label: format!("congestion-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    // The congestion reschedule delay (2s +/- 1s jitter) is real time this
    // test has to wait through -- bounded, not blind: `await_all` returns
    // the instant every job reaches a terminal state, whichever comes
    // first, rather than sleeping a fixed guess.
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(20))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "every job should complete after the congestion reschedule, got {outcomes:?}"
    );

    let calls = calls.lock().await;
    assert_eq!(
        calls.len(),
        2,
        "expected exactly one congestion failure and one successful retry, saw {calls:?}"
    );
    assert_eq!(
        calls[0].len(),
        N,
        "first call should see the whole batch, saw {:?}",
        calls[0]
    );
    assert!(
        calls[0].iter().all(|&a| a == 1),
        "first call's items should all be at attempt 1, saw {:?}",
        calls[0]
    );
    assert_eq!(
        calls[1].len(),
        N,
        "the congestion-rescheduled retry should still be one N-item batch, not \
         N solo dispatches -- got sizes {:?}. If this shrank to 1-item calls, \
         `attempt_index` was bumped and the retry-solo rule kicked in.",
        calls.iter().map(|c| c.len()).collect::<Vec<_>>()
    );
    assert!(
        calls[1].iter().all(|&a| a == 1),
        "the congestion-rescheduled retry's items should still be at attempt 1 \
         (unchanged), saw {:?}",
        calls[1]
    );

    Ok(())
}
