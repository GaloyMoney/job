mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    JobType, Jobs,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct TestJobConfig {
    delay_ms: u64,
}

struct TestJobInitializer;

impl JobInitializer for TestJobInitializer {
    type Config = TestJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("test-job")
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
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
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(self.config.delay_ms)).await;
        Ok(JobCompletion::Complete)
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

    let spawner = jobs.add_initializer(TestJobInitializer);

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let delay_ms = 50;
    let job_config = TestJobConfig { delay_ms };
    let job_id = JobId::new();

    let job = spawner
        .spawn(job_id, job_config)
        .await
        .expect("Failed to create and spawn job");

    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms * 2)).await;

    let job = jobs.find(job.id).await?;
    assert!(job.completed());

    Ok(())
}
