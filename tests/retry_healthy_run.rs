//! `attempt_reset_after_healthy_run`: the attempt counter is forgiven by
//! EXECUTION EVIDENCE — how long the failing run itself stayed up, measured
//! on a monotonic clock — and by nothing else.
//!
//! Three properties carry the feature, and each fails deterministically if
//! the mechanism is removed:
//!
//! - `a_long_running_execution_is_forgiven_and_never_terminates` — the #14
//!   case. A job whose runs stay up past the threshold outlives
//!   `n_attempts`, WITHOUT ever returning a completion. This is the shape a
//!   resident/daemon listener has (healthy means "still inside `run()`"), so
//!   an `ExecutionCompleted`-based reset cannot serve it.
//! - `a_fast_failure_terminates_at_max_attempts_despite_clock_jumps` — the
//!   #163 case at the database level. A deterministic failure escalates to
//!   terminal even as the application clock jumps a day between attempts.
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
use es_entity::clock::ClockHandle;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSpec, JobSvcConfig, JobType, Jobs, RetrySettings,
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
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _ = self.ran.send(Utc::now());
        tokio::time::sleep(RUN_FOR).await;
        Err("blip after a healthy stretch".into())
    }
}

/// Fails immediately: a deterministically broken job, which must be allowed
/// to reach `n_attempts` and terminate.
struct FailsFast {
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
}

#[async_trait]
impl JobRunner for FailsFast {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _ = self.ran.send(Utc::now());
        Err("deterministic failure".into())
    }
}

enum Behaviour {
    RunsLongThenFails,
    FailsFast,
}

struct Init {
    job_type: JobType,
    behaviour: Behaviour,
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
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(match self.behaviour {
            Behaviour::RunsLongThenFails => Box::new(RunsLongThenFails {
                ran: self.ran.clone(),
            }) as Box<dyn JobRunner>,
            Behaviour::FailsFast => Box::new(FailsFast {
                ran: self.ran.clone(),
            }),
        })
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
/// completion event anywhere in its history, so it outlives `n_attempts`
/// indefinitely.
///
/// Red without the mechanism: with forgiveness removed the job terminates on
/// its 3rd failure and the 5th run never arrives.
#[tokio::test]
async fn a_long_running_execution_is_forgiven_and_never_terminates() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let job_type = helpers::job_type("healthy-run-forgiven");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
        behaviour: Behaviour::RunsLongThenFails,
        ran: tx,
        retry: RetrySettings {
            n_attempts: Some(3),
            n_warn_attempts: None,
            min_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_jitter_pct: 0,
            attempt_reset_after_healthy_run: Some(HEALTHY_RUN),
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn_all(vec![JobSpec::new(id, Cfg)]).await?;

    // Five runs is two more than `n_attempts` allows without forgiveness, so
    // simply arriving here is the assertion.
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

/// The #163 case at the database level, which PR #205 never had: a job that
/// fails deterministically must still reach `n_attempts` and go terminal, and
/// advancing the application clock between attempts must not change that.
///
/// Red without the mechanism: restore an elapsed-domain-time reset and the
/// clock jumps forgive every attempt, so the execution row never disappears
/// and this times out.
#[tokio::test]
async fn a_fast_failure_terminates_at_max_attempts_despite_clock_jumps() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let job_type = helpers::job_type("fast-failure-terminates");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
        behaviour: Behaviour::FailsFast,
        ran: tx,
        retry: RetrySettings {
            n_attempts: Some(3),
            n_warn_attempts: None,
            min_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_jitter_pct: 0,
            // Far longer than any run this job will ever manage.
            attempt_reset_after_healthy_run: Some(Duration::from_secs(60 * 60)),
        },
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn_all(vec![JobSpec::new(id, Cfg)]).await?;

    // Jump the domain clock by a day after each observed failure: under the
    // old elapsed-time rule this cleared the reset threshold every single
    // pass, and the job could never be exhausted.
    //
    // The wait for the retry row is load-bearing, not decoration. The runner
    // reports at the START of its run, so advancing on that signal alone can
    // land BEFORE the finalizer writes the retry row — the row would then be
    // stamped `execute_at = advanced_now + min_backoff`, a millisecond into
    // the simulated future that a frozen manual clock never reaches on its
    // own, and the next attempt would never be dispatched.
    for n in 1..=3 {
        next_run(&mut ran, &format!("attempt {n}")).await?;
        let next_attempt = n + 1;
        if next_attempt <= 3 {
            await_row(&pool, id, &format!("the retry row for attempt {next_attempt}"), |row| {
                matches!(row, Some((attempt, ref state)) if attempt == next_attempt && state == "pending")
            })
            .await?;
        }
        controller.advance(Duration::from_secs(24 * 60 * 60)).await;
    }

    await_row(
        &pool,
        id,
        "the exhausted job's execution row to be deleted",
        |row| row.is_none(),
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
        behaviour: Behaviour::RunsLongThenFails,
        ran: tx,
        retry: RetrySettings {
            n_attempts: Some(10),
            n_warn_attempts: None,
            // Long, so the post-failure retry row parks and the poller cannot
            // re-claim it out from under the reclaim we are about to stage.
            min_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(60),
            backoff_jitter_pct: 0,
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
