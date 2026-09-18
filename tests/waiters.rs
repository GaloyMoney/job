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
//!
//! `register_waiter_in_op_*same_op*` / `register_waiter_in_op_*no_hooks*` /
//! `a_caller_that_registers_at_park_time_is_woken` cover
//! `handoff-register-waiter-same-op.md`: every handle-based registration
//! (`JobHandle`/`JobHandles`/`Jobs::register_waiter_in_op`,
//! `CurrentJob::wait_for_in_op`) funnels to `JobWaiters::register_waiters_in_op`,
//! which must attach to a callee this SAME op is itself spawning (its
//! execution row may not exist yet — keyless rows insert at commit) instead
//! of reading the absent row as terminal. `a_caller_that_registers_at_park_time_is_woken`
//! is the actual race: a live poller completing the callee concurrently with
//! (or immediately after) the caller's commit, through the handle path that
//! was broken before this fix — not a sequential register-then-complete.
//! `register_waiter_in_op_same_op_rolls_back_with_the_op` pins the other
//! half: the registration is ordinary same-transaction writes, so a caller
//! whose op rolls back is left holding neither the spawn nor the wait.
#![cfg(feature = "es-entity")]

mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, SubsecRound, Utc};
use job::{
    CurrentJob, Job, JobCompletion, JobHandles, JobId, JobInitializer, JobRunner, JobSpawner,
    JobSpec, JobSvcConfig, JobType, Jobs,
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

/// A caller that spawns its callee KEYLESS (no `.waiter()` on the spec) and
/// registers itself through `CurrentJob::wait_for_in_op` — the handle-based
/// path `handoff-register-waiter-same-op.md` fixes — rather than through
/// `JobSpec::waiter`, which was already correct before this handoff.
struct HandleParkCaller {
    callee_id: JobId,
    callee_spawner: JobSpawner<CalleeCfg>,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
}

#[async_trait]
impl JobRunner for HandleParkCaller {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _ = self.ran.send(Utc::now());
        let state: Option<CallerState> = current_job.execution_state()?;
        if state.is_some() {
            return Ok(JobCompletion::Complete);
        }

        let mut op = current_job.begin_op().await?;
        let callee = self
            .callee_spawner
            .spawn_in_op(&mut op, self.callee_id, CalleeCfg)
            .await?;
        let handles: JobHandles = std::iter::once(callee).collect();
        current_job.wait_for_in_op(&mut op, &handles).await?;
        current_job
            .update_execution_state_in_op(&mut op, &CallerState { spawned: true })
            .await?;
        Ok(JobCompletion::RescheduleAtWithOp(op, Utc::now() + HOLD))
    }
}

struct HandleParkCallerInit {
    job_type: JobType,
    callee_spawner: JobSpawner<CalleeCfg>,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
}

impl JobInitializer for HandleParkCallerInit {
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
        Ok(Box::new(HandleParkCaller {
            callee_id: config.callee_id,
            callee_spawner: self.callee_spawner.clone(),
            ran: self.ran.clone(),
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

// -- handoff-register-waiter-same-op.md ------------------------------------

/// The defect, pinned directly: a keyless callee spawned on THIS op has no
/// execution row yet (`ExecutionInsertHook` buffers it for commit), so the
/// old `register_waiters_on_live_in_op`-only path's `FOR SHARE` finds
/// nothing and reports it terminal. Fails on the pre-fix code (`false`, no
/// `job_waiters` row); `register_waiters_in_op`'s `pending_ids` routing
/// fixes it by taking the lock-free insert form for a callee this op is
/// itself creating.
#[tokio::test]
async fn register_waiter_in_op_attaches_to_a_keyless_callee_spawned_on_the_same_op()
-> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-keyless");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });
    // No poller: the row must stay parked so the assertions are about
    // registration alone, not a race with a claim.

    let callee_id = JobId::new();
    let waiter = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner
        .spawn_at_in_op(&mut op, callee_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let attached = jobs
        .handle(callee_id)
        .register_waiter_in_op(&mut op, waiter)
        .await?;
    op.commit().await?;

    assert!(
        attached,
        "a keyless callee spawned on this same op must attach, not read as terminal"
    );
    assert!(
        wait_exists(&pool, callee_id, waiter).await?,
        "a wait edge must exist for the pair"
    );
    assert!(
        exec_row(&pool, callee_id).await?.is_some(),
        "the buffered spawn must have landed at commit"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The `JobHandles` (multi-callee) form of the same fix: every keyless
/// callee from one `spawn_all_in_op` batch on this op must attach.
#[tokio::test]
async fn register_waiter_in_op_attaches_all_of_a_keyless_spawn_all_on_the_same_op()
-> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-spawn-all");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    let ids: Vec<JobId> = (0..3).map(|_| JobId::new()).collect();
    let waiter = JobId::new();
    let specs: Vec<JobSpec<CalleeCfg>> = ids
        .iter()
        .map(|id| JobSpec::new(*id, CalleeCfg).schedule_at(Utc::now() + HOLD))
        .collect();

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner.spawn_all_in_op(&mut op, specs).await?;
    let attached = jobs
        .handles(ids.clone())
        .register_waiter_in_op(&mut op, waiter)
        .await?;
    op.commit().await?;

    let mut attached_sorted = attached;
    attached_sorted.sort_unstable();
    let mut ids_sorted = ids.clone();
    ids_sorted.sort_unstable();
    assert_eq!(
        attached_sorted, ids_sorted,
        "every keyless spawn_all callee on this op must attach"
    );
    for id in &ids {
        assert!(
            wait_exists(&pool, *id, waiter).await?,
            "a wait edge must exist for {id}"
        );
    }

    jobs.shutdown().await?;
    Ok(())
}

/// One `JobHandles::register_waiter_in_op` call spanning three callees in
/// three different states -- live (already committed), terminal (its
/// execution row deleted, as a completion leaves it), and same-op (still
/// buffered in this op's `ExecutionInsertHook`) -- attaches exactly the
/// live and same-op ones. Pins that the routing is per-callee, not
/// all-or-nothing.
#[tokio::test]
async fn register_waiter_in_op_partitions_same_op_live_and_terminal_callees() -> anyhow::Result<()>
{
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-partition");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    let live_id = JobId::new();
    spawner
        .spawn_at(live_id, CalleeCfg, Utc::now() + HOLD)
        .await?;

    let terminal_id = JobId::new();
    spawner
        .spawn_at(terminal_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    sqlx::query("DELETE FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(terminal_id))
        .execute(&pool)
        .await?;

    let same_op_id = JobId::new();
    let waiter = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner
        .spawn_at_in_op(&mut op, same_op_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let attached = jobs
        .handles([live_id, terminal_id, same_op_id])
        .register_waiter_in_op(&mut op, waiter)
        .await?;
    op.commit().await?;

    let mut attached_sorted = attached;
    attached_sorted.sort_unstable();
    let mut expected = vec![live_id, same_op_id];
    expected.sort_unstable();
    assert_eq!(
        attached_sorted, expected,
        "only the live and same-op callees attach; the terminal one is rejected"
    );
    assert!(wait_exists(&pool, live_id, waiter).await?);
    assert!(wait_exists(&pool, same_op_id, waiter).await?);
    assert!(
        !wait_exists(&pool, terminal_id, waiter).await?,
        "nothing is written for the terminal callee"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// THE race, not a sequential register-then-complete: a caller spawns its
/// callee KEYLESS and registers through `CurrentJob::wait_for_in_op` — the
/// handle path that was broken — on the SAME op as the spawn, then commits
/// and parks for an HOUR. A live poller is running the whole time and is
/// free to claim and complete the callee the instant its row lands, racing
/// the caller's own commit. On the pre-fix code this is not a close race at
/// all: `register_waiters_on_live_in_op` finds no row for the still-buffered
/// callee and reports it terminal, so no wait is ever written and the
/// caller sleeps out the full hour -- this test times out (30s) waiting for
/// the second run. On the fixed code the wait is written in the same
/// transaction as the spawn, so the callee -- however fast it completes --
/// can never observe an attached-but-un-woken state: the wake and the
/// caller's own second run land within seconds.
#[tokio::test]
async fn a_caller_that_registers_at_park_time_is_woken() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let callee_type = helpers::job_type("waiters-same-op-race-callee");
    let caller_type = helpers::job_type("waiters-same-op-race-caller");

    let callee_spawner = jobs.add_initializer(CalleeInit {
        job_type: callee_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });
    let (tx, mut ran) = mpsc::unbounded_channel();
    let caller_spawner = jobs.add_initializer(HandleParkCallerInit {
        job_type: caller_type.clone(),
        callee_spawner,
        ran: tx,
    });
    jobs.start_poll().await?;

    let caller_id = JobId::new();
    let callee_id = JobId::new();
    caller_spawner
        .spawn(caller_id, CallerCfg { callee_id })
        .await?;

    let first = next_run(&mut ran).await?;
    let second = next_run(&mut ran).await?;
    assert!(
        second - first < chrono::TimeDelta::seconds(10),
        "a caller registering through the handle path on the same op as its spawn \
         must be woken within seconds, not the hour it parked for ({second} vs {first})"
    );

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

/// `SavepointOp::commit_hook_dyn` reads its own staged buffer first and
/// falls back to its parent's -- so a callee spawned on the PARENT op must
/// still be found from a nested savepoint, and, mirrored, a callee spawned
/// INSIDE a savepoint that is then released (folding its hook into the
/// parent) must be found by a registration made on the parent afterwards.
#[tokio::test]
async fn register_waiter_in_op_sees_a_spawn_made_on_the_parent_of_a_savepoint() -> anyhow::Result<()>
{
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-savepoint");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    // Spawn on the parent, register from a savepoint nested inside it.
    let callee_id = JobId::new();
    let waiter = JobId::new();
    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner
        .spawn_at_in_op(&mut op, callee_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let attached = op
        .with_savepoint(async |sp| {
            jobs.handle(callee_id)
                .register_waiter_in_op(sp, waiter)
                .await
        })
        .await??;
    op.commit().await?;
    assert!(
        attached,
        "a spawn on the parent op must be visible from a nested savepoint"
    );
    assert!(wait_exists(&pool, callee_id, waiter).await?);

    // Spawn inside a savepoint that is released, register on the parent.
    let callee_id2 = JobId::new();
    let waiter2 = JobId::new();
    let mut op2 = es_entity::DbOp::init(&pool).await?;
    op2.with_savepoint(async |sp| -> Result<(), job::JobError> {
        spawner
            .spawn_at_in_op(sp, callee_id2, CalleeCfg, Utc::now() + HOLD)
            .await?;
        Ok(())
    })
    .await??;
    let attached2 = jobs
        .handle(callee_id2)
        .register_waiter_in_op(&mut op2, waiter2)
        .await?;
    op2.commit().await?;
    assert!(
        attached2,
        "a spawn inside a released savepoint must be visible on the parent afterwards"
    );
    assert!(wait_exists(&pool, callee_id2, waiter2).await?);

    jobs.shutdown().await?;
    Ok(())
}

/// A bare `sqlx::Transaction` reports `AtomicOperation::supports_hooks() ==
/// false`, so `ExecutionInsertHook::register` cannot buffer a keyless row on
/// it and falls back to inserting inline (`force_execute_pre_commit`) --
/// `pending_ids`'s `unwrap_or_default` on such an op is correctly empty, and
/// the row is still found by the live `FOR SHARE` form because it already
/// exists (uncommitted, but visible within the same transaction).
#[tokio::test]
async fn register_waiter_in_op_on_an_op_without_hooks_still_attaches() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-no-hooks");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    let callee_id = JobId::new();
    let waiter = JobId::new();

    let mut tx = pool.begin().await?;
    spawner
        .spawn_at_in_op(&mut tx, callee_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let attached = jobs
        .handle(callee_id)
        .register_waiter_in_op(&mut tx, waiter)
        .await?;
    tx.commit().await?;

    assert!(
        attached,
        "the inline-inserted row must be found by the live FOR SHARE form"
    );
    assert!(wait_exists(&pool, callee_id, waiter).await?);

    jobs.shutdown().await?;
    Ok(())
}

/// Rollback semantics: the registration is ordinary writes against the
/// caller's own `op`, nothing more -- so when that op rolls back (dropped
/// without `commit`), both the spawn and the wait roll back with it. The
/// caller is left holding neither a callee nor a wait; nothing to clean up,
/// nothing dangling.
#[tokio::test]
async fn register_waiter_in_op_same_op_rolls_back_with_the_op() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("waiters-same-op-rollback");
    let spawner = jobs.add_initializer(CalleeInit {
        job_type: job_type.clone(),
        behaviour: CalleeBehaviour::Complete,
    });

    let callee_id = JobId::new();
    let waiter = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner
        .spawn_at_in_op(&mut op, callee_id, CalleeCfg, Utc::now() + HOLD)
        .await?;
    let attached = jobs
        .handle(callee_id)
        .register_waiter_in_op(&mut op, waiter)
        .await?;
    assert!(
        attached,
        "the same-op callee is attached inside the still-open transaction"
    );
    drop(op);

    assert!(
        exec_row(&pool, callee_id).await?.is_none(),
        "the rolled-back spawn must never have landed"
    );
    assert!(
        !wait_exists(&pool, callee_id, waiter).await?,
        "the wait registered inside the rolled-back op must not survive it"
    );

    jobs.shutdown().await?;
    Ok(())
}
