//! The two periodic checks must open a span only when they have something to
//! report.
//!
//! Both run on a fixed cadence regardless of load, and both spans are roots
//! (`parent: None` on the stale check, a detached task for the sweep), so a
//! tick that reports nothing used to export a one-span trace of its own at
//! every interval. On a quiet instance -- the common case -- that is the
//! ENTIRE trace export of a service: the spans are indistinguishable from real
//! work in a trace backend, and one instance alone emits
//! `3600 / sweep_interval` of them per hour.
//!
//! The fields these spans carry (`n_waiters`, `n_resolved`, `n_stale_pending`,
//! `max_pending_duration_secs`) are still recorded whenever there is something
//! to report; the second test pins that they did not quietly disappear along
//! with the quiet case.
//!
//! Its own binary, like the other span-capture tests: `tracing`'s per-callsite
//! interest cache is process-wide, and the first assertion below is an absence.

mod helpers;
mod span_capture;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const SWEEP_SPAN: &str = "job.notification_router.sweep_waiters";
const STALE_SPAN: &str = "job.check_stale_pending_jobs";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NoopConfig;

struct NoopInitializer {
    job_type: JobType,
}

impl JobInitializer for NoopInitializer {
    type Config = NoopConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(NoopRunner))
    }
}

struct NoopRunner;

#[async_trait]
impl JobRunner for NoopRunner {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        Ok(JobCompletion::Complete)
    }
}

/// Both periodic checks fast enough to fire several times inside the windows
/// below.
fn tight_intervals() -> JobPollerConfig {
    JobPollerConfig {
        sweep_interval: Duration::from_millis(20),
        pending_jobs_check_interval: Duration::from_millis(20),
        ..Default::default()
    }
}

#[tokio::test]
async fn an_idle_instance_opens_no_poller_spans() -> anyhow::Result<()> {
    let (fields, _guard) = span_capture::install();

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(tight_intervals())
        .build()?;
    let mut jobs = Jobs::init(config).await?;
    // No initializer (nothing claimable) and no handle (no waiter): every tick
    // of both checks finds nothing to report.
    jobs.start_poll().await?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    jobs.shutdown().await?;

    assert!(
        !fields.created(SWEEP_SPAN),
        "a sweep with no waiters must not open a span; spans seen: {}",
        fields.debug_dump()
    );
    assert!(
        !fields.created(STALE_SPAN),
        "a stale check that finds no rows must not open a span; spans seen: {}",
        fields.debug_dump()
    );
    Ok(())
}

#[tokio::test]
async fn a_tick_with_something_to_report_still_opens_its_span() -> anyhow::Result<()> {
    let (fields, _guard) = span_capture::install();

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(JobPollerConfig {
            // A zero dispatch budget claims nothing, which is what keeps the
            // row below `pending` and past due -- the state the stale check
            // reports. (`budget.rs`: "a zero budget claims nothing".) 100.0 is
            // the validated maximum, and it floors the budget to zero for as
            // long as the pool's headroom stays under 100 connections -- this
            // helper pool opens 5.
            connections_per_job: 100.0,
            ..tight_intervals()
        })
        .build()?;
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(NoopInitializer {
        job_type: helpers::job_type("poller-span"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, NoopConfig).await?;

    // Holding the completion-waiter keeps the sweep's waiter map non-empty for
    // the whole window: the job never runs, so it never goes terminal.
    let waiter = {
        let jobs = jobs.clone();
        tokio::spawn(async move {
            jobs.handle(job_id)
                .await_completion(Duration::from_secs(10))
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(300)).await;
    waiter.abort();
    jobs.shutdown().await?;

    assert!(
        fields.created(SWEEP_SPAN),
        "a sweep with a waiter must still open its span; spans seen: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get(SWEEP_SPAN, "n_waiters").as_deref(),
        Some("1"),
        "the waiter count must reach the span"
    );

    assert!(
        fields.created(STALE_SPAN),
        "a stale check that finds a row must still open its span; spans seen: {}",
        fields.debug_dump()
    );
    let stale: i64 = fields
        .get(STALE_SPAN, "n_stale_pending")
        .expect("n_stale_pending must be recorded on a reporting check")
        .parse()?;
    assert_eq!(stale, 1, "the unclaimed row must be the one counted");
    Ok(())
}
