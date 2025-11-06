mod helpers;

use anyhow::bail;
use async_trait::async_trait;

use job::{
    CurrentJob, Job, JobCompletion, JobConfig, JobId, JobInitializer, JobRunner, JobSpawnOptions,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct TestJobConfig {
    delay_ms: u64,
    #[serde(default)]
    entity_id: Option<Uuid>,
}

impl JobConfig for TestJobConfig {
    type Initializer = TestJobInitializer;

    fn spawn_options(&self) -> JobSpawnOptions {
        let mut options = JobSpawnOptions::default();
        if let Some(entity_id) = self.entity_id {
            options = options.with_entity_id(entity_id);
        }
        options
    }
}

struct TestJobInitializer;
impl JobInitializer for TestJobInitializer {
    fn job_type() -> JobType {
        JobType::new("test_job")
    }

    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: TestJobConfig = job.config()?;
        Ok(Box::new(TestJobRunner { config }))
    }
}

struct TestJobRunner {
    config: TestJobConfig,
}

#[async_trait]
impl JobRunner for TestJobRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if let Some(entity_id) = self.config.entity_id {
            let job_entity = current_job.entity_id().map(|id| Uuid::from(*id));
            assert_eq!(job_entity, Some(entity_id));
        } else {
            assert!(current_job.entity_id().is_none());
        }
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(self.config.delay_ms)).await;

        Ok(JobCompletion::Complete)
    }
}

async fn fetch_job_state(pool: &sqlx::PgPool, job_id: JobId) -> anyhow::Result<Option<String>> {
    let state =
        sqlx::query_scalar::<_, String>("SELECT state::TEXT FROM job_executions WHERE id = $1")
            .bind(job_id)
            .fetch_optional(pool)
            .await?;
    Ok(state)
}

async fn wait_for_state(
    pool: &sqlx::PgPool,
    job_id: JobId,
    expected: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(state) = fetch_job_state(pool, job_id).await? {
            if state == expected {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for job {job_id:?} to reach state {expected}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn test_create_and_run_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    jobs.add_initializer(TestJobInitializer);
    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let job_config = TestJobConfig {
        delay_ms: 50,
        entity_id: None,
    };
    let job_id = JobId::new();
    jobs.create_and_spawn(job_id, job_config)
        .await
        .expect("Failed to create and spawn job");

    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

    let job = jobs.find(job_id).await?;
    assert!(job.completed());

    Ok(())
}

#[tokio::test]
async fn jobs_with_same_entity_run_sequentially() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    jobs.add_initializer(TestJobInitializer);
    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let entity_id = Uuid::now_v7();
    let other_entity_id = Uuid::now_v7();

    let first_job_id = JobId::new();
    jobs.create_and_spawn(
        first_job_id,
        TestJobConfig {
            delay_ms: 400,
            entity_id: Some(entity_id),
        },
    )
    .await
    .expect("Failed to create first job");

    wait_for_state(&pool, first_job_id, "running", Duration::from_secs(2)).await?;

    let concurrent_job_id = JobId::new();
    jobs.create_and_spawn(
        concurrent_job_id,
        TestJobConfig {
            delay_ms: 200,
            entity_id: Some(other_entity_id),
        },
    )
    .await
    .expect("Failed to create concurrent job");

    wait_for_state(&pool, concurrent_job_id, "running", Duration::from_secs(2)).await?;

    let second_job_id = JobId::new();
    jobs.create_and_spawn(
        second_job_id,
        TestJobConfig {
            delay_ms: 50,
            entity_id: Some(entity_id),
        },
    )
    .await
    .expect("Failed to create second job");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let second_state = fetch_job_state(&pool, second_job_id).await?;
    assert_eq!(second_state.as_deref(), Some("pending"));

    tokio::time::sleep(Duration::from_millis(700)).await;

    let second_state_post = fetch_job_state(&pool, second_job_id).await?;
    assert!(
        second_state_post.is_none(),
        "second job did not complete after first finished"
    );

    let first_job = jobs.find(first_job_id).await?;
    assert!(first_job.completed());

    let second_job = jobs.find(second_job_id).await?;
    assert!(second_job.completed());

    let third_job = jobs.find(concurrent_job_id).await?;
    assert!(third_job.completed());

    jobs.shutdown().await?;

    Ok(())
}
