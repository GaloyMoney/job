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
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSpec,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};

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

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn job_type(prefix: &str) -> JobType {
    JobType::new(Box::leak(unique(prefix).into_boxed_str()))
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

    let jt = job_type("lock-order");
    let spawner = jobs.add_initializer(NoopInitializer { job_type: jt });

    for _ in 0..25 {
        // Lexicographically q1 < q2 unconditionally -- the shared prefix
        // differs before either's UUID suffix begins, so string ordering
        // (what `ExecutionInsertHook`'s sort uses) is deterministic
        // regardless of the UUIDs' own values.
        let q1 = unique("lockorder-q1");
        let q2 = unique("lockorder-q2");

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
