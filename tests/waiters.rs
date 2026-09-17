//! `job_waiters` + wake-by-id (`JobSpec::waiter`, `Jobs::pull_forward_in_op`,
//! `Jobs::register_waiter_in_op`): a job that spawns another and parks on it
//! is pulled forward when the callee goes terminal, instead of sleeping out
//! its own deadline.
//!
//! Four properties carry the feature, each with a test whose failure mode is
//! deterministic:
//!
//! - `pull_forward_in_op_moves_a_parked_row_once_and_never_a_backoff` — the
//!   hazard shared with the keyed pull-forward (`keyed_force_reschedule.rs`):
//!   a waiter already in retry backoff (`attempt_index > 1`) must never be
//!   yanked forward. Drop the `attempt_index <= 1` guard in
//!   `JobRepo::pull_forward_ids_in_op` and this fails outright — the crafted
//!   backoff row moves.
//! - `register_waiter_in_op_attaches_to_a_live_job_and_rejects_a_terminal_one`
//!   — D4's `FOR SHARE` semantics: registering on a live row succeeds,
//!   registering on a since-terminal row is an honest `false` with nothing
//!   written.
//! - `a_parked_caller_is_woken_when_its_callee_completes` — the point of the
//!   mechanism. A caller parked an HOUR out via `RescheduleAtWithOp` runs
//!   again within seconds of its callee completing, and `job_waiters` is
//!   empty for the pair afterwards (cleanup).
//! - `a_plain_rescheduled_caller_still_wakes_via_the_mark` — D10. A caller
//!   whose callee completes *while it is still running* (so the wake cannot
//!   move a `running` row) still wakes promptly once its own plain
//!   `RescheduleAt` park lands, because that write consults the mark. Remove
//!   the mark consult and this test times out deterministically — the run
//!   is an hour away otherwise, not a race.
//!
//! Waiting is by state polling (explicit conditions) or the runner's own
//! channel, never by sleeping for "long enough".
#![cfg(feature = "es-entity")]

mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, SubsecRound, Utc};
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSpec,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalleeCfg;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CallerCfg {
    callee_id: JobId,
}

/// Persisted across a caller's two runs via `execution_state`: the runner
/// instance itself is rebuilt on each claim, so "have I already spawned the
/// callee and parked" cannot live in a struct field.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CallerState {
    spawned: bool,
}

/// How far ahead a caller parks itself. Deliberately far beyond any test
/// timeout: nothing but a wake can make a second run happen inside this
/// suite.
const HOLD: chrono::TimeDelta = chrono::TimeDelta::hours(1);

// -- callee behaviours --------------------------------------------------

/// Completes on its first run.
struct CompleteImmediately;

#[async_trait]
impl JobRunner for CompleteImmediately {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

/// Blocks until told to finish, so a test can hold it `running` for as long
/// as it needs to observe a wake that cannot move a running row.
struct Held {
    release: Arc<AtomicBool>,
}

#[async_trait]
impl JobRunner for Held {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(JobCompletion::Complete)
    }
}

enum CalleeBehaviour {
    Complete,
    Held(Arc<AtomicBool>),
}

struct CalleeInit {
    job_type: JobType,
    behaviour: CalleeBehaviour,
}

impl JobInitializer for CalleeInit {
    type Config = CalleeCfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(match &self.behaviour {
            CalleeBehaviour::Complete => Box::new(CompleteImmediately) as Box<dyn JobRunner>,
            CalleeBehaviour::Held(release) => Box::new(Held {
                release: Arc::clone(release),
            }),
        })
    }
}

// -- caller ---------------------------------------------------------------

/// Caller runner parameterized with the callee id it must wait on (read from
/// the job's own `config` at `init` time) and whether the park is written
/// in-op (D-path) or as a plain `RescheduleAt` committed after the run
/// returns (D10 path).
struct Caller {
    callee_id: JobId,
    callee_spawner: JobSpawner<CalleeCfg>,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
    plain_reschedule: bool,
}

#[async_trait]
impl JobRunner for Caller {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _ = self.ran.send(Utc::now());
        let state: Option<CallerState> = current_job.execution_state()?;
        if state.is_some() {
            return Ok(JobCompletion::Complete);
        }

        if self.plain_reschedule {
            // Spawn and park as two separate commits: the callee can (and in
            // the D10 test, does) reach a terminal state while this run is
            // still executing, before the plain `RescheduleAt` below is even
            // returned, let alone applied by the dispatcher. Poll the
            // callee's handle to `Completed` before returning, so the
            // callee's terminal wake finds THIS row still `running` (the
            // case the mark exists for) rather than racing the dispatcher's
            // own park write.
            let callee = self
                .callee_spawner
                .spawn_spec(JobSpec::new(self.callee_id, CalleeCfg).waiter(*current_job.id()))
                .await?;
            loop {
                if matches!(
                    callee.load().await?.state(),
                    job::JobStatus::Completed { .. }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            current_job
                .update_execution_state(CallerState { spawned: true })
                .await?;
            Ok(JobCompletion::RescheduleAt(Utc::now() + HOLD))
        } else {
            let mut op = current_job.begin_op().await?;
            self.callee_spawner
                .spawn_spec_in_op(
                    &mut op,
                    JobSpec::new(self.callee_id, CalleeCfg).waiter(*current_job.id()),
                )
                .await?;
            current_job
                .update_execution_state_in_op(&mut op, &CallerState { spawned: true })
                .await?;
            Ok(JobCompletion::RescheduleAtWithOp(op, Utc::now() + HOLD))
        }
    }
}

struct CallerInit {
    job_type: JobType,
    callee_spawner: JobSpawner<CalleeCfg>,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
    plain_reschedule: bool,
}

impl JobInitializer for CallerInit {
    type Config = CallerCfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: CallerCfg = job.config()?;
        Ok(Box::new(Caller {
            callee_id: config.callee_id,
            callee_spawner: self.callee_spawner.clone(),
            ran: self.ran.clone(),
            plain_reschedule: self.plain_reschedule,
        }))
    }
}

// -- helpers ----------------------------------------------------------------

async fn exec_row(
    pool: &sqlx::PgPool,
    id: JobId,
) -> anyhow::Result<Option<(Option<DateTime<Utc>>, i32, String)>> {
    let row: Option<(Option<DateTime<Utc>>, i32, String)> = sqlx::query_as(
        "SELECT execute_at, attempt_index, state::text FROM job_executions WHERE id = $1",
    )
    .bind(uuid::Uuid::from(id))
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Whether the `job_waiters` EDGE exists for the pair. The table holds only
/// edges; the "you were woken" mark is a property of the waiter and lives on
/// its execution row (see [`woken_mark`]).
async fn wait_exists(pool: &sqlx::PgPool, callee: JobId, waiter: JobId) -> anyhow::Result<bool> {
    let row: Option<(uuid::Uuid,)> =
        sqlx::query_as("SELECT job_id FROM job_waiters WHERE job_id = $1 AND waiter_job_id = $2")
            .bind(uuid::Uuid::from(callee))
            .bind(uuid::Uuid::from(waiter))
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// The waiter's wake mark, off its own execution row. `None` covers both "no
/// mark" and "no execution row".
async fn woken_mark(pool: &sqlx::PgPool, waiter: JobId) -> anyhow::Result<Option<DateTime<Utc>>> {
    let row: Option<(Option<DateTime<Utc>>,)> =
        sqlx::query_as("SELECT woken_at FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(waiter))
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(w,)| w))
}

async fn next_run(
    ran: &mut mpsc::UnboundedReceiver<DateTime<Utc>>,
) -> anyhow::Result<DateTime<Utc>> {
    tokio::time::timeout(Duration::from_secs(30), ran.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for the runner to run"))?
        .ok_or_else(|| anyhow::anyhow!("runner channel closed"))
}

fn target_in(delta: chrono::TimeDelta) -> DateTime<Utc> {
    (Utc::now() + delta).trunc_subsecs(6)
}

// -- tests --------------------------------------------------------------

/// THE regression test for the retry-backoff hazard, at the by-id primitive
/// `Jobs::pull_forward_in_op` uses. Exactly the keyed pull-forward's guard
/// (`keyed_force_reschedule.rs::force_reschedule_never_shortens_a_retry_backoff`),
/// pinned directly at the row level rather than through a full retry cycle:
/// a parked row (attempt 1) moves and is idempotent; a row shaped like retry
/// backoff (attempt 2, crafted by hand — exactly `Finalizer`'s own retry
/// write shape) is never touched.
///
/// Deterministic: dropping `attempt_index <= 1` from
/// `JobRepo::pull_forward_ids_in_op` makes the crafted backoff row move on
/// the very first call — no race, no timing.
#[tokio::test]
async fn pull_forward_in_op_moves_a_parked_row_once_and_never_a_backoff() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-pull-forward");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });
    // No poller: the row must stay parked so the assertions are about
    // `pull_forward_in_op` alone.

    let id = JobId::new();
    spawner.spawn_at(id, CalleeCfg, Utc::now() + HOLD).await?;
    let parked = exec_row(&pool, id).await?.unwrap().0.unwrap();

    let mut op = es_entity::DbOp::init(&pool).await?;
    let moved = jobs.pull_forward_in_op(&mut op, id, Utc::now()).await?;
    op.commit().await?;
    assert!(moved, "a parked row on its first attempt must move");
    let after = exec_row(&pool, id).await?.unwrap().0.unwrap();
    assert!(
        after < parked && after <= Utc::now(),
        "the row must now be due: {after} (was {parked})"
    );

    let mut op2 = es_entity::DbOp::init(&pool).await?;
    let moved2 = jobs
        .pull_forward_in_op(&mut op2, id, Utc::now() + HOLD)
        .await?;
    op2.commit().await?;
    assert!(!moved2, "an already-due row is a no-op");
    assert_eq!(
        exec_row(&pool, id).await?.unwrap().0.unwrap(),
        after,
        "and must not be rewritten"
    );

    // Shape a retry-backoff row by hand: exactly what `Finalizer`'s own
    // retry write leaves behind (`state = 'pending'`, `attempt_index > 1`,
    // `execute_at` in the future).
    let backoff_target = target_in(chrono::TimeDelta::minutes(30));
    sqlx::query(
        "UPDATE job_executions SET state = 'pending', attempt_index = 2, execute_at = $2
         WHERE id = $1",
    )
    .bind(uuid::Uuid::from(id))
    .bind(backoff_target)
    .execute(&pool)
    .await?;

    let mut op3 = es_entity::DbOp::init(&pool).await?;
    let moved3 = jobs.pull_forward_in_op(&mut op3, id, Utc::now()).await?;
    op3.commit().await?;
    assert!(
        !moved3,
        "a row in retry backoff (attempt_index > 1) must never be pulled forward"
    );
    assert_eq!(
        exec_row(&pool, id).await?.unwrap().0.unwrap(),
        backoff_target,
        "the backoff deadline must be untouched"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// D4: `Jobs::register_waiter_in_op`'s `FOR SHARE` semantics. Registering on
/// a live job succeeds and writes the row; registering on a job whose
/// execution row is already gone (terminal, simulated the same way a
/// completion leaves it — the row deleted) is an honest `false` with nothing
/// written, so the caller reads the outcome instead of parking on a promise
/// that can never be kept.
#[tokio::test]
async fn register_waiter_in_op_attaches_to_a_live_job_and_rejects_a_terminal_one()
-> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-register");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    let callee = JobId::new();
    spawner
        .spawn_at(callee, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let waiter = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    let attached = jobs.register_waiter_in_op(&mut op, callee, waiter).await?;
    op.commit().await?;
    assert!(attached, "the callee is live");
    assert!(
        wait_exists(&pool, callee, waiter).await?,
        "an edge must exist for the registered wait"
    );
    assert_eq!(
        woken_mark(&pool, waiter).await?,
        None,
        "no wake has happened yet"
    );

    // Simulate the callee going terminal: its execution row is deleted,
    // exactly as `finalize_in_op` leaves it for any terminal disposition.
    sqlx::query("DELETE FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(callee))
        .execute(&pool)
        .await?;

    let other_waiter = JobId::new();
    let mut op2 = es_entity::DbOp::init(&pool).await?;
    let attached2 = jobs
        .register_waiter_in_op(&mut op2, callee, other_waiter)
        .await?;
    op2.commit().await?;
    assert!(!attached2, "a terminal callee must reject registration");
    assert!(
        !wait_exists(&pool, callee, other_waiter).await?,
        "nothing must be written for a rejected registration"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The point of the mechanism, end to end: a caller that spawns its callee
/// and parks on it IN THE SAME transaction (`RescheduleAtWithOp`) runs again
/// within seconds of the callee completing — not an hour later — and
/// `job_waiters` is left empty for the pair afterwards (cleanup: consumed by
/// the wake).
#[tokio::test]
async fn a_parked_caller_is_woken_when_its_callee_completes() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let callee_type = helpers::job_type("waiters-callee-complete");
    let caller_type = helpers::job_type("waiters-caller-complete");

    let callee_spawner = jobs.add_initializer(CalleeInit {
        job_type: callee_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });
    let (tx, mut ran) = mpsc::unbounded_channel();
    let caller_spawner = jobs.add_initializer(CallerInit {
        job_type: caller_type.clone(),
        callee_spawner,
        ran: tx,
        plain_reschedule: false,
    });
    jobs.start_poll().await?;

    let caller_id = JobId::new();
    let callee_id = JobId::new();
    caller_spawner
        .spawn(caller_id, CallerCfg { callee_id })
        .await?;

    // No intermediate check of the caller's parked row: with an immediately
    // completing callee, the whole spawn -> park -> wake -> second-run chain
    // can finish inside a single poll tick, so a transient "parked at +1h"
    // read would race the wake and is not observable deterministically. The
    // proof that the wake — not the hour-long deadline — is what produced
    // the second run is the elapsed time between the two runs.
    let first = next_run(&mut ran).await?;
    let second = next_run(&mut ran).await?;
    assert!(
        second - first < chrono::TimeDelta::seconds(10),
        "the woken run must happen within seconds, not the hour the caller parked for \
         ({} vs {first})",
        second
    );

    // Poll for the terminal write to land, then assert cleanup: no
    // `job_waiters` row survives a consumed wake.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if !wait_exists(&pool, callee_id, caller_id).await? {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the consumed wait must eventually be deleted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    jobs.shutdown().await?;
    Ok(())
}

/// D10: a caller that parks with a PLAIN `RescheduleAt` (its park written by
/// the dispatcher after `run` returns, not on the spawn's own transaction)
/// is still woken promptly even when its callee goes terminal WHILE the
/// caller is still running — the wake cannot move a `running` row, so it
/// leaves a mark instead, and the caller's own `Fresh` write consults it.
///
/// Deterministic failure mode: without the mark consult, the caller's park
/// is written for a full hour and this test times out (30s) waiting for the
/// second run — it is not a race that sometimes passes without D10, the run
/// genuinely does not happen for an hour.
#[tokio::test]
async fn a_plain_rescheduled_caller_still_wakes_via_the_mark() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let callee_type = helpers::job_type("waiters-callee-held");
    let caller_type = helpers::job_type("waiters-caller-plain");

    let release = Arc::new(AtomicBool::new(false));
    let callee_spawner = jobs.add_initializer(CalleeInit {
        job_type: callee_type.clone(),
        behaviour: CalleeBehaviour::Held(Arc::clone(&release)),
    });
    let (tx, mut ran) = mpsc::unbounded_channel();
    let caller_spawner = jobs.add_initializer(CallerInit {
        job_type: caller_type.clone(),
        callee_spawner,
        ran: tx,
        plain_reschedule: true,
    });
    jobs.start_poll().await?;

    let caller_id = JobId::new();
    let callee_id = JobId::new();
    caller_spawner
        .spawn(caller_id, CallerCfg { callee_id })
        .await?;

    let first = next_run(&mut ran).await?;

    // The caller's own run is blocked inside `Caller::run` polling the
    // callee's handle — it has spawned the callee and is now waiting for it
    // to complete, all before returning `RescheduleAt`. Release the callee
    // once we can see the wait was registered (belt-and-braces: also just
    // wait a moment for the spawn to land).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if wait_exists(&pool, callee_id, caller_id).await? {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the caller never registered its wait on the callee"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    release.store(true, Ordering::SeqCst);

    // The callee completes (and its terminal wake runs) while the caller's
    // own row is still `running` — the wake marks the wait instead of
    // moving it. Confirm the mark landed before the caller's park write.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        // Either the mark landed on the caller's execution row, or the
        // caller's Fresh write already won the race and consumed the edge --
        // both are the wake being honoured.
        if woken_mark(&pool, caller_id).await?.is_some()
            || !wait_exists(&pool, callee_id, caller_id).await?
        {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the callee's terminal wake never marked the caller's wait"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // As in the in-op test: the caller's own park is written for +1h, but
    // the mark consult pulls it back to now before the write is even
    // visible to a poller elsewhere, so the proof is the elapsed time
    // between runs, not a transient row read.
    let second = next_run(&mut ran).await?;
    assert!(
        second - first < chrono::TimeDelta::seconds(10),
        "the mark consult must land the caller due within seconds, not the hour it \
         plainly rescheduled for ({second} vs {first})"
    );

    jobs.shutdown().await?;
    Ok(())
}
