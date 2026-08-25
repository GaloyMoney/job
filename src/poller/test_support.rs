//! Shared helpers for the poller submodules' tests: a DB pool from
//! `PG_CON`, raw row seeders, and a minimally-scaffolded `JobPoller`.

use chrono::{DateTime, Utc};
use es_entity::clock::ClockHandle;
use sqlx::postgres::PgPool;

use std::sync::Arc;
use std::time::Duration;

use crate::{
    JobId, JobType, config::JobPollerConfig, entity::Job,
    notification_router::JobNotificationRouter, notifier::JobEventNotifier, registry::JobRegistry,
    repo::JobRepo, tracker::JobTracker,
};

use super::JobPoller;

pub(super) async fn init_pool() -> anyhow::Result<PgPool> {
    let pg_con = std::env::var("PG_CON").unwrap();
    Ok(sqlx::PgPool::connect(&pg_con).await?)
}

/// An uncapped ("elastic") plain type; `init` is unreachable because the
/// tests using it never let a claimed row reach dispatch.
pub(super) struct ElasticInitializer {
    pub(super) job_type: JobType,
}

impl crate::JobInitializer for ElasticInitializer {
    type Config = ();

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: crate::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn crate::JobRunner>, Box<dyn std::error::Error>> {
        unimplemented!("never invoked by this test")
    }
}

pub(super) async fn build_poller(
    pool: &PgPool,
    registry: JobRegistry,
    tracker: Arc<JobTracker>,
) -> anyhow::Result<Arc<JobPoller>> {
    let repo = Arc::new(JobRepo::new(pool));
    let router = Arc::new(JobNotificationRouter::new(
        pool,
        Arc::clone(&repo),
        16,
        Duration::from_secs(60),
    ));
    let notifier = JobEventNotifier::spawn(pool, Arc::clone(&tracker), router.terminal_sender());
    Ok(Arc::new(
        JobPoller::new(
            JobPollerConfig::default(),
            repo,
            registry,
            tracker,
            router,
            notifier,
            ClockHandle::realtime(),
        )
        .await?,
    ))
}

pub(super) async fn seed_pending_job(
    pool: &PgPool,
    job_type: &str,
    execute_at: DateTime<Utc>,
) -> anyhow::Result<JobId> {
    let id = JobId::new();
    let uuid = uuid::Uuid::from(id);
    let now = chrono::Utc::now();
    sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
        .bind(uuid)
        .bind(job_type)
        .bind(now)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, 'pending', 1, $3, $4, $5)",
    )
    .bind(uuid)
    .bind(job_type)
    .bind(execute_at)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(super) async fn seed_running_job(
    pool: &PgPool,
    job_type: &str,
    instance_id: uuid::Uuid,
    alive_at: DateTime<Utc>,
) -> anyhow::Result<JobId> {
    let id = JobId::new();
    let uuid = uuid::Uuid::from(id);
    let now = chrono::Utc::now();
    sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
        .bind(uuid)
        .bind(job_type)
        .bind(now)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
         VALUES ($1, $2, 'running', $3, 1, $4, $5)",
    )
    .bind(uuid)
    .bind(job_type)
    .bind(instance_id)
    .bind(alive_at)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(super) async fn seed_queued_job(
    pool: &PgPool,
    job_type: &str,
    queue_id: &str,
    execute_at: DateTime<Utc>,
    state: &str,
) -> anyhow::Result<JobId> {
    let id = JobId::new();
    let uuid = uuid::Uuid::from(id);
    let now = chrono::Utc::now();
    sqlx::query("INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, $4)")
        .bind(uuid)
        .bind(job_type)
        .bind(queue_id)
        .bind(now)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, \
          poller_instance_id, created_at) \
         VALUES ($1, $2, $3, $4::JobExecutionState, 1, \
                 CASE WHEN $4 = 'running' THEN NULL ELSE $5 END, $6, \
                 CASE WHEN $4 = 'running' THEN gen_random_uuid() END, $7)",
    )
    .bind(uuid)
    .bind(job_type)
    .bind(queue_id)
    .bind(state)
    .bind(execute_at)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(super) async fn row_state(pool: &PgPool, id: JobId) -> anyhow::Result<String> {
    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(state)
}
