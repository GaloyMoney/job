//! Pool-based handle getters must stay spawn-awaitable — these stop COMPILING if one regresses.

mod helpers;

use async_trait::async_trait;
use es_entity::AtomicOperation;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobSvcConfig, JobType, Jobs, KeyedJobInitializer,
    KeyedJobSpawner,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct Idle;

#[async_trait]
impl job::JobRunner for Idle {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct KeyedInit {
    job_type: JobType,
}

impl KeyedJobInitializer for KeyedInit {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Idle))
    }
}

/// `keyed_handle` awaited on a spawned task — the shape that broke downstream.
#[tokio::test]
async fn keyed_handle_is_spawnable() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("spawnable-keyed");
    let spawner = jobs.add_keyed_initializer(KeyedInit {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    let spawned = spawner.spawn_in_op(&mut op, "k", Cfg).await?;
    op.commit().await?;

    let spawned_jobs = jobs.clone();
    let spawned_type = job_type.clone();
    let found = tokio::spawn(async move { spawned_jobs.keyed_handle(spawned_type, "k").await })
        .await?
        .expect("lookup must not error")
        .expect("the committed keyed job must be found");
    assert_eq!(found.id(), spawned.id());

    jobs.shutdown().await?;
    Ok(())
}

/// The list form, same guard.
#[tokio::test]
async fn keyed_handles_is_spawnable() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("spawnable-keyed-list");
    let spawner = jobs.add_keyed_initializer(KeyedInit {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner.spawn_in_op(&mut op, "k", Cfg).await?;
    op.commit().await?;

    let spawned_jobs = jobs.clone();
    let spawned_type = job_type.clone();
    let handles = tokio::spawn(async move { spawned_jobs.keyed_handles(spawned_type).await })
        .await?
        .expect("lookup must not error");
    assert_eq!(handles.len(), 1);

    jobs.shutdown().await?;
    Ok(())
}

/// The resident form. Row written by hand, as in `handle_getters_in_op.rs`.
#[tokio::test]
async fn resident_handle_is_spawnable() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("spawnable-resident");
    let id = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    sqlx::query("INSERT INTO jobs (id, job_type, resident) VALUES ($1, $2, true)")
        .bind(uuid::Uuid::from(id))
        .bind(job_type.as_str())
        .execute(op.as_executor())
        .await?;
    op.commit().await?;

    let spawned_jobs = jobs.clone();
    let spawned_type = job_type.clone();
    let found = tokio::spawn(async move { spawned_jobs.resident_handle(spawned_type).await })
        .await?
        .expect("lookup must not error")
        .expect("the committed resident job must be found");
    assert_eq!(found.id(), id);

    Ok(())
}
