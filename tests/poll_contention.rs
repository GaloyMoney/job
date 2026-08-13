//! Reproduction for the per-type claim cap vs. concurrent pollers.
//!
//! The per-type `row_limit` must bound how many rows a poller *takes*, not
//! which rows it is *allowed to look at*. If the cap is applied before
//! `FOR UPDATE SKIP LOCKED`, every poller ranks the same global candidate set
//! and targets an identical head slice, so a poller that loses the race skips
//! all of it and falls through to nothing — despite plenty of due work.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSpec, JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    n: usize,
}

struct CountingInitializer {
    job_type: JobType,
    ran: Arc<AtomicUsize>,
}

impl JobInitializer for CountingInitializer {
    type Config = Cfg;
    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }
    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(CountingRunner {
            ran: Arc::clone(&self.ran),
        }))
    }
}

struct CountingRunner {
    ran: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for CountingRunner {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.ran.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::Complete)
    }
}

/// A poller must claim due work that a *concurrent* poller has not taken,
/// even when the concurrent poller currently holds row locks on the head of
/// the queue.
///
/// The locks held below are exactly what another instance's in-flight poll
/// transaction holds between its `FOR UPDATE SKIP LOCKED` and its commit.
#[tokio::test]
async fn poller_falls_through_locked_head_rows_to_later_due_jobs() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let job_type = JobType::new("poll-contention-fallthrough");

    // Small poll budget so `n_jobs_to_poll` (and thus a non-batched type's
    // row_limit) is 5, while far more due work exists.
    let poller_config = JobPollerConfig {
        max_jobs_per_process: 5,
        min_jobs_per_process: 5,
        ..Default::default()
    };
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(poller_config)
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let ran = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(CountingInitializer {
        job_type: job_type.clone(),
        ran: Arc::clone(&ran),
    });

    // 20 due jobs with strictly increasing execute_at, so "oldest N" is
    // unambiguous for both the lock below and the poll query.
    let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
    let specs: Vec<JobSpec<Cfg>> = (0..20)
        .map(|i| {
            JobSpec::new(JobId::new(), Cfg { n: i })
                .schedule_at(base + chrono::Duration::seconds(i as i64))
        })
        .collect();
    spawner.spawn_all(specs).await?;

    // Simulate a concurrent poller mid-claim: hold locks on the oldest 5 rows
    // (this type's entire row_limit) without changing their state.
    let mut blocking_tx = pool.begin().await?;
    let locked: Vec<uuid::Uuid> = sqlx::query_scalar(
        r#"
        SELECT id FROM job_executions
        WHERE state = 'pending' AND job_type = $1
        ORDER BY execute_at
        LIMIT 5
        FOR UPDATE
        "#,
    )
    .bind(job_type.as_str())
    .fetch_all(&mut *blocking_tx)
    .await?;
    assert_eq!(locked.len(), 5, "expected to lock this type's whole budget");

    jobs.start_poll().await?;

    // The other 15 rows are due and unlocked. A healthy poller claims 5 of
    // them immediately; it must not sit idle waiting on MAX_WAIT.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while ran.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let progressed = ran.load(Ordering::SeqCst);

    blocking_tx.rollback().await?;

    assert!(
        progressed > 0,
        "poller claimed nothing while 15 due jobs were unlocked: the per-type \
         cap was applied before FOR UPDATE SKIP LOCKED, so the poller could \
         only ever target the 5 rows another poller had locked"
    );

    Ok(())
}
