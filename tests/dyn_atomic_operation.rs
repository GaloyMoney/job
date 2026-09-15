//! Pins the actual point of relaxing every `_in_op` fn's bound to
//! `+ ?Sized`: a caller that only has a type-erased, object-safe
//! `&mut dyn es_entity::AtomicOperation` -- not a concrete, `Sized` op type
//! -- must still be able to call them. That is exactly the shape a
//! downstream object-safe boundary (e.g. lana's `SealedSpawner`) needs to
//! hand in, and the whole reason the bound was relaxed at all: before this,
//! `impl es_entity::AtomicOperation` carried an implicit `Sized` bound that
//! rejected a `dyn` op outright, at compile time, regardless of what ran at
//! runtime.
//!
//! Each test below erases a live `DbOp` to `&mut dyn AtomicOperation`
//! *before* the call -- so the compiler, not just the runtime, is checking
//! the property -- then asserts the call's ordinary durable effects, so a
//! regression that reintroduces an implicit `Sized` bound on any of these
//! signatures fails to COMPILE this file rather than merely failing some
//! other test's assertions.

mod helpers;

use async_trait::async_trait;
use es_entity::AtomicOperation;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    Jobs, KeyedJobInitializer, KeyedJobSpawner,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    marker: u32,
}

struct Runner;

#[async_trait]
impl JobRunner for Runner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct Init {
    job_type: job::JobType,
}

impl JobInitializer for Init {
    type Config = Cfg;

    fn job_type(&self) -> job::JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Runner))
    }
}

struct KeyedInit {
    job_type: job::JobType,
}

impl KeyedJobInitializer for KeyedInit {
    type Config = Cfg;

    fn job_type(&self) -> job::JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Runner))
    }
}

async fn count_jobs(
    pool: &sqlx::PgPool,
    job_type: &job::JobType,
    id: JobId,
) -> anyhow::Result<i64> {
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1 AND id = $2")
            .bind(job_type.as_str())
            .bind(uuid::Uuid::from(id))
            .fetch_one(pool)
            .await?;
    Ok(count)
}

/// `JobSpawner::spawn_in_op` -- the exact boundary lana's `SealedSpawner`
/// needs -- durably creates a job when handed a genuinely type-erased op.
/// Exercises the whole relaxed call chain from `spawn_in_op` down:
/// `spawn_at_in_op` -> `spawn_spec_in_op` -> `JobRepo::create_in_op` ->
/// `ExecutionInsertHook::register` -> `(&mut op).add_commit_hook(..)` (the
/// generic convenience method, reachable through the erased op via the
/// `&mut O` blanket impl, es-entity#222).
#[tokio::test]
async fn spawn_in_op_accepts_an_erased_dyn_atomic_operation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("dyn-op-spawn");
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    // The erasure under test: from here on, the compiler only knows `op` as
    // `&mut dyn AtomicOperation` -- no concrete, `Sized` type in sight.
    let erased: &mut dyn AtomicOperation = &mut op;
    let id = JobId::new();
    let job = spawner.spawn_in_op(erased, id, Cfg { marker: 1 }).await?;
    op.commit().await?;

    assert_eq!(job.id, id);
    assert_eq!(count_jobs(&pool, &job_type, id).await?, 1);

    jobs.shutdown().await?;
    Ok(())
}

/// [`JobSpawner::spawn_all_in_op`] against the same erased op, so the
/// bulk path (`JobRepo::create_all_in_op`'s own `&mut op` reborrow) is
/// pinned too, not just the single-spawn one.
#[tokio::test]
async fn spawn_all_in_op_accepts_an_erased_dyn_atomic_operation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("dyn-op-spawn-all");
    let spawner = jobs.add_initializer(Init {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    let erased: &mut dyn AtomicOperation = &mut op;
    let specs = vec![
        job::JobSpec::new(JobId::new(), Cfg { marker: 1 }),
        job::JobSpec::new(JobId::new(), Cfg { marker: 2 }),
    ];
    let result = spawner.spawn_all_in_op(erased, specs).await?;
    op.commit().await?;

    assert_eq!(result.jobs.len(), 2);
    for job in &result.jobs {
        assert_eq!(count_jobs(&pool, &job_type, job.id).await?, 1);
    }

    jobs.shutdown().await?;
    Ok(())
}

/// [`KeyedJobSpawner::spawn_in_op`] against an erased op -- the keyed path's
/// own chain (`JobRepo::lock_and_check_live_keys_in_op`,
/// `JobRepo::create_all_in_op`'s `&mut op` reborrow, `insert_executions_in_op`,
/// `carry_state_in_op`, `pull_forward_in_op`) all the way down.
#[tokio::test]
async fn keyed_spawn_in_op_accepts_an_erased_dyn_atomic_operation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("dyn-op-keyed-spawn");
    let spawner = jobs.add_keyed_initializer(KeyedInit {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    let erased: &mut dyn AtomicOperation = &mut op;
    let spawned = spawner
        .spawn_in_op(erased, "erased-shard", Cfg { marker: 1 })
        .await?;
    op.commit().await?;

    assert!(spawned.created);
    assert_eq!(count_jobs(&pool, &job_type, spawned.handle.id()).await?, 1);

    jobs.shutdown().await?;
    Ok(())
}
