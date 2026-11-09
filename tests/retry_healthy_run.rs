//! `attempt_reset_after_healthy_run`: the attempt counter is forgiven by
//! EXECUTION EVIDENCE — how long the failing run itself stayed up, measured
//! on a monotonic clock — and by nothing else.
//!
//! Two properties carry the feature, and each fails deterministically if
//! the mechanism is removed:
//!
//! - `a_long_running_execution_is_forgiven_and_never_terminates` — the #14
//!   case. A job whose runs stay up past the threshold has its counter held
//!   down, WITHOUT ever returning a completion. This is the shape a
//!   resident/daemon listener has (healthy means "still inside `run()`"), so
//!   an `ExecutionCompleted`-based reset cannot serve it.
//! - `a_reclaimed_attempt_is_forgiven_by_the_next_healthy_run` — the SIGKILL
//!   path. `reclaim_lost_jobs` bumps `attempt_index` in SQL with NO entity
//!   event, so it is the one real producer of an inflated counter that no
//!   event-based signal can ever forgive. The run-duration signal forgives it
//!   because it never consults history at all.
//!
//! Waiting is by the runner's own channel or by state polling, never by
//! sleeping for "long enough". The one `sleep` here is INSIDE a runner and is
//! the mechanism under test rather than synchronisation: it puts a hard lower
//! bound on how long that execution ran, which is the direction the assertion
//! needs.

mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    Backoff, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner,
    JobSpawner, JobSpec, JobSvcConfig, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// Long enough that a run clearing it cannot be an artifact of scheduling
/// noise, short enough to keep the test quick.
const HEALTHY_RUN: Duration = Duration::from_millis(200);

/// What a "healthy" run actually spends inside `run()`. Comfortably above
/// [`HEALTHY_RUN`] so a loaded machine cannot make the run look short — a
/// `sleep` can only ever make it LONGER, so the margin is one-sided.
const RUN_FOR: Duration = Duration::from_millis(500);

const POLL_TIMEOUT: Duration = Duration::from_secs(30);

// -- runners ----------------------------------------------------------------

/// Stays up `RUN_FOR`, then fails: a daemon that served traffic for a while
/// and then hit a blip. Never returns a completion, so nothing in its history
/// ever says "this job was healthy".
struct RunsLongThenFails {
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
}

#[async_trait]
impl JobRunner for RunsLongThenFails {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let _ = self.ran.send(Utc::now());
        tokio::time::sleep(RUN_FOR).await;
        Err("blip after a healthy stretch".into())
    }
}

struct Init {
    job_type: JobType,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
    retry: RetrySettings,
}

impl JobInitializer for Init {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(RunsLongThenFails {
            ran: self.ran.clone(),
        }))
    }
}

// -- helpers ----------------------------------------------------------------

/// Blocks until the runner reports its next run, or fails the test.
async fn next_run(
    ran: &mut mpsc::UnboundedReceiver<DateTime<Utc>>,
    which: &str,
) -> anyhow::Result<DateTime<Utc>> {
    tokio::time::timeout(POLL_TIMEOUT, ran.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for {which}"))?
        .ok_or_else(|| anyhow::anyhow!("runner channel closed before {which}"))
}

async fn execution_row(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<Option<(i32, String)>> {
    let row: Option<(i32, String)> =
        sqlx::query_as("SELECT attempt_index, state::text FROM job_executions WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row)
}

async fn attempt_index(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<i32> {
    Ok(execution_row(pool, id)
        .await?
        .expect("execution row must still exist")
        .0)
}

/// State-polls until `predicate` holds for the execution row, or fails.
async fn await_row<F>(
    pool: &sqlx::PgPool,
    id: JobId,
    what: &str,
    predicate: F,
) -> anyhow::Result<()>
where
    F: Fn(Option<(i32, String)>) -> bool,
{
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if predicate(execution_row(pool, id).await?) {
            return Ok(());
        }
        anyhow::ensure!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// -- tests ------------------------------------------------------------------

/// The #14 case, and the one PR #205's `ExecutionCompleted` predicate broke:
/// a job that stays up past the threshold before failing is forgiven with no
/// completion event anywhere in its history, so its counter stays held down.
///
/// Red without the mechanism: with forgiveness removed the counter climbs
/// past 2 and the row assertion never holds.
#[tokio::test]
async fn a_long_running_execution_is_forgiven_and_never_terminates() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let job_type = helpers::job_type("healthy-run-forgiven");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
        ran: tx,
        retry: RetrySettings {
            backoff: Backoff::new(
                Duration::from_millis(1),
                Duration::from_millis(5),
                Duration::from_millis(10),
                0,
            ),
            attempt_reset_after_healthy_run: Some(HEALTHY_RUN),
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn_all(vec![JobSpec::new(id, Cfg)]).await?;

    for n in 1..=5 {
        next_run(&mut ran, &format!("run {n}")).await?;
    }

    // And the counter is genuinely being held down, not merely climbing
    // slowly: a forgiven failure reschedules as attempt 2 (reset to 1, then
    // +1 for the next run), so it can never pass 2.
    await_row(
        &pool,
        id,
        "a forgiven retry row",
        |row| matches!(row, Some((attempt, _)) if attempt <= 2),
    )
    .await?;

    jobs.shutdown().await?;
    Ok(())
}

/// The SIGKILL path. `reclaim_lost_jobs` bumps `attempt_index` directly in
/// SQL and pushes NO entity event, so a job killed ungracefully returns with
/// a counter no event-based signal can account for — let alone forgive.
/// Because the run-duration signal never reads history, the reclaimed
/// attempt is forgiven by the job's next healthy run just like any other.
///
/// Red without the mechanism: the reclaimed attempt is never forgiven and the
/// row settles at `attempt_index = 4` instead of 2.
#[tokio::test]
async fn a_reclaimed_attempt_is_forgiven_by_the_next_healthy_run() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(JobPollerConfig {
            job_lost_interval: Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let job_type = helpers::job_type("reclaimed-then-forgiven");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
        ran: tx,
        retry: RetrySettings {
            // Long, so the post-failure retry row parks and the poller cannot
            // re-claim it out from under the reclaim we are about to stage.
            backoff: Backoff::new(
                Duration::from_secs(60),
                Duration::from_secs(120),
                Duration::from_secs(120),
                0,
            ),
            attempt_reset_after_healthy_run: Some(HEALTHY_RUN),
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn_all(vec![JobSpec::new(id, Cfg)]).await?;

    // First failure: attempt 1 is not forgivable (nothing has accumulated),
    // so the row parks at attempt 2, 60s out.
    next_run(&mut ran, "the first run").await?;
    await_row(
        &pool,
        id,
        "the first retry row",
        |row| matches!(row, Some((2, ref state)) if state == "pending"),
    )
    .await?;

    // Stage an ungraceful kill: the row looks `running` under a dead peer
    // whose heartbeat stopped a minute ago (wall-clock, which is what
    // liveness is measured on). The real lost-handler reclaims it.
    let dead_peer = uuid::Uuid::now_v7();
    sqlx::query(
        "UPDATE job_executions
         SET state = 'running', alive_at = $2, poller_instance_id = $3
         WHERE id = $1",
    )
    .bind(id)
    .bind(Utc::now() - chrono::Duration::seconds(60))
    .bind(dead_peer)
    .execute(&pool)
    .await?;

    // The reclaim spends an attempt with no event to show for it: 2 -> 3.
    // Then the poller re-dispatches, the runner stays up past the threshold,
    // and that failure forgives the whole accumulated count.
    next_run(&mut ran, "the run after the reclaim").await?;
    await_row(
        &pool,
        id,
        "the reclaimed attempt to be forgiven",
        |row| matches!(row, Some((2, ref state)) if state == "pending"),
    )
    .await?;

    assert_eq!(
        attempt_index(&pool, id).await?,
        2,
        "a healthy run must forgive an attempt the reclaim spent silently"
    );

    jobs.shutdown().await?;
    Ok(())
}
