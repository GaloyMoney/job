//! Regression test for the write-path deadlock fix (sb-max8: 21 deadlocks /
//! 25 min in production). Concurrent multi-queue spawns racing over the
//! same previously-empty queues, in opposite orders, must never deadlock --
//! `ExecutionInsertHook::pre_commit` sorts its rows by `(queue_id, id)`
//! before the arbiter insert so every concurrent batch agrees on one global
//! lock-acquisition order, matching the occupant pin
//! (`ExecutionInsertHook::lock_queue_occupants`) and the promote swap
//! (`PromoteHeadsHook::apply`), which already locked in that order.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSpec, JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct NoopRunner;

#[async_trait]
impl JobRunner for NoopRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct NoopInitializer {
    job_type: JobType,
}

impl JobInitializer for NoopInitializer {
    type Config = Cfg;
    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }
    fn short_circuit(&self) -> bool {
        // Scope this test to the insert/pin race alone -- no claim/dispatch
        // noise from the head-swap short circuit.
        false
    }
    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(NoopRunner))
    }
}

/// sb-max8 traced its 21 deadlocks/25min to the park-or-take arbiter insert
/// (`ExecutionInsertHook::insert_many`, statement 1) having no defined
/// lock-acquisition order -- unlike every OTHER multi-row locker of
/// `job_executions` (the occupant pin, the promote swap), which already
/// locked in a fixed order. Two concurrent multi-queue spawns racing over
/// the SAME previously-empty queues, in opposite orders, is the primary
/// cycle shape named in the handoff: each can grab one queue's
/// speculative-insert slot first and then block waiting for the other's.
///
/// This runs many independent iterations (fresh queue pairs each time)
/// because it is a genuine timing race, not a deterministic sequence point
/// -- Postgres's per-row speculative-insert processing inside one
/// multi-row statement isn't something application code can pause
/// mid-statement to force. Enough iterations reliably reproduce the
/// deadlock (40P01, surfaced as a `JobError`) before the fix
/// (`ExecutionInsertHook::insert_many`'s `input` CTE: `MATERIALIZED` +
/// `ORDER BY queue_id, id`, enforced in SQL rather than by pre-sorting
/// `rows` in Rust -- the DB-side order holds regardless of what order any
/// caller/accumulator happens to pass rows in) and reliably don't after it
/// -- verified by temporarily reverting that `ORDER BY`.
#[tokio::test]
async fn concurrent_multi_queue_spawns_do_not_deadlock() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = helpers::job_type("lock-order");
    let spawner = jobs.add_initializer(NoopInitializer { job_type: jt });

    for _ in 0..25 {
        // Lexicographically q1 < q2 unconditionally -- the shared prefix
        // differs before either's UUID suffix begins, so string ordering
        // (what `ExecutionInsertHook`'s sort uses) is deterministic
        // regardless of the UUIDs' own values.
        let q1 = helpers::unique("lockorder-q1");
        let q2 = helpers::unique("lockorder-q2");

        let a_specs = vec![
            JobSpec::new(JobId::new(), Cfg).queue_id(q1.clone()),
            JobSpec::new(JobId::new(), Cfg).queue_id(q2.clone()),
        ];
        // Deliberately the OPPOSITE order from `a_specs` -- the adversarial
        // shape the fix must neutralize regardless of caller/accumulator
        // order.
        let b_specs = vec![
            JobSpec::new(JobId::new(), Cfg).queue_id(q2.clone()),
            JobSpec::new(JobId::new(), Cfg).queue_id(q1.clone()),
        ];

        let (ra, rb) = tokio::join!(spawner.spawn_all(a_specs), spawner.spawn_all(b_specs));
        ra.expect("batch A must not deadlock");
        rb.expect("batch B must not deadlock");
    }

    jobs.shutdown().await?;
    Ok(())
}

/// Parks inside `run` until the test opens the gate, so its execution row
/// stays `running` -- and therefore inside the keep-alive's live set -- for
/// as long as the test needs to observe heartbeats.
struct GatedRunner {
    gate: Arc<Semaphore>,
}

#[async_trait]
impl JobRunner for GatedRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _permit = self.gate.acquire().await?;
        Ok(JobCompletion::Complete)
    }
}

struct GatedInitializer {
    job_type: JobType,
    gate: Arc<Semaphore>,
}

impl JobInitializer for GatedInitializer {
    type Config = Cfg;
    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }
    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(GatedRunner {
            gate: Arc::clone(&self.gate),
        }))
    }
}

async fn alive_at(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT alive_at FROM job_executions WHERE id = $1",
    )
    .bind(uuid::Uuid::from(id))
    .fetch_one(pool)
    .await?;
    Ok(at)
}

/// The keep-alive heartbeat must not block on a row another transaction
/// holds, and must still refresh every row it CAN take.
///
/// sb-max10 caught the heartbeat as a deadlock victim in production
/// (`keep alive error ... deadlock detected`). It fires every
/// `job_lost_interval / 4` over every running row of the instance, which
/// makes it the busiest multi-row `job_executions` writer in the crate, and
/// as a bare `UPDATE ... WHERE id = ANY(...)` it took row locks in scan
/// order -- an order no other writer agrees with. It is the prime suspect
/// for the surviving partner of the batch-seal deadlocks that stalled five
/// loan approvals for 7m40s.
///
/// The fix orders its locks `(queue_id, id)` like every other multi-row
/// locker and adds `FOR NO KEY UPDATE SKIP LOCKED`, which is what this
/// asserts: with one of two running rows pinned by a competing
/// `FOR UPDATE`, the heartbeat skips that row and refreshes the other.
///
/// Before the fix this fails on the *other* row: a bare UPDATE blocks on the
/// pinned row mid-statement, and because the statement is its own
/// transaction nothing it already wrote is visible until it completes -- so
/// the unpinned row's `alive_at` stays frozen too. That is the property
/// that matters. A heartbeat that stalls behind one contended row stops
/// refreshing every OTHER job it is responsible for, and those jobs age
/// toward the lost-handler's threshold through no fault of their own.
#[tokio::test]
async fn keep_alive_skips_a_contended_row_and_still_refreshes_the_rest() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    // The keep-alive beats at `job_lost_interval / 4`, so this gives ~4
    // beats per second -- fast enough to observe without making the test
    // slow, and far above the liveness threshold this test never reaches.
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(JobPollerConfig {
            job_lost_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let gate = Arc::new(Semaphore::new(0));
    let spawner = jobs.add_initializer(GatedInitializer {
        job_type: helpers::job_type("keepalive-skip-locked"),
        gate: Arc::clone(&gate),
    });

    // Two unqueued jobs so both run concurrently and both land in the same
    // heartbeat's live set.
    let pinned = JobId::new();
    let free = JobId::new();
    spawner
        .spawn_all(vec![JobSpec::new(pinned, Cfg), JobSpec::new(free, Cfg)])
        .await?;
    jobs.start_poll().await?;

    // Both must be `running` before pinning anything, or the heartbeat has
    // nothing to skip and nothing to refresh.
    let mut waited = 0;
    loop {
        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE id = ANY($1) AND state = 'running'",
        )
        .bind(vec![uuid::Uuid::from(pinned), uuid::Uuid::from(free)])
        .fetch_one(&pool)
        .await?;
        if running == 2 {
            break;
        }
        waited += 1;
        assert!(waited < 100, "both jobs should reach running");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A competing lock on ONE row, held across several heartbeats. Its own
    // connection, so the pool the poller uses cannot be starved by it.
    // Pinned BEFORE the alive_at snapshots below: with the lock already
    // held, no heartbeat can move the pinned row's alive_at between the
    // snapshot and the assertion -- reading first would race a beat into
    // that gap.
    let holder_pool = helpers::init_pool().await?;
    let mut holder = holder_pool.begin().await?;
    sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR UPDATE")
        .bind(uuid::Uuid::from(pinned))
        .fetch_one(&mut *holder)
        .await?;

    let pinned_before = alive_at(&pool, pinned).await?;
    let free_before = alive_at(&pool, free).await?;

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let pinned_during = alive_at(&pool, pinned).await?;
    let free_during = alive_at(&pool, free).await?;

    assert_eq!(
        pinned_during, pinned_before,
        "the pinned row must be skipped, not waited on"
    );
    assert!(
        free_during > free_before,
        "a contended row must not stop the heartbeat refreshing the rest: \
         {free_before} -> {free_during}"
    );

    // Releasing the pin lets the next beat pick the skipped row back up --
    // the reason skipping is safe: the gap is bounded by one beat, far
    // inside the liveness threshold.
    holder.rollback().await?;
    let mut waited = 0;
    loop {
        if alive_at(&pool, pinned).await? > pinned_before {
            break;
        }
        waited += 1;
        assert!(
            waited < 100,
            "a skipped row must be refreshed again once the pin is released"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    gate.add_permits(1024);
    jobs.shutdown().await?;
    Ok(())
}
