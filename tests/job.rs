mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    ClockHandle, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs, SimulationConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

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

// Test job that records when it was executed using the clock from CurrentJob
#[derive(Debug, Serialize, Deserialize)]
struct ScheduledJobConfig {
    expected_schedule_time: DateTime<Utc>,
}

struct ScheduledJobInitializer {
    recorded_time: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl JobInitializer for ScheduledJobInitializer {
    type Config = ScheduledJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("scheduled-job")
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(ScheduledJobRunner {
            recorded_time: Arc::clone(&self.recorded_time),
        }))
    }
}

struct ScheduledJobRunner {
    recorded_time: Arc<Mutex<Option<DateTime<Utc>>>>,
}

#[async_trait]
impl JobRunner for ScheduledJobRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Record the time from the clock when the job runs
        let now = current_job.clock().now();
        *self.recorded_time.lock().await = Some(now);
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_scheduled_job_with_artificial_clock() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    // Create an artificial clock for deterministic testing
    let (clock, controller) = ClockHandle::artificial(SimulationConfig::manual());
    let initial_time = clock.now();

    let config = JobSvcConfig::builder()
        .pool(pool)
        .clock(clock.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    // Track when the job was executed
    let recorded_time: Arc<Mutex<Option<DateTime<Utc>>>> = Arc::new(Mutex::new(None));
    let spawner = jobs.add_initializer(ScheduledJobInitializer {
        recorded_time: Arc::clone(&recorded_time),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    // Schedule a job 60 seconds in the future
    let schedule_at = initial_time + chrono::Duration::seconds(60);
    let job_config = ScheduledJobConfig {
        expected_schedule_time: schedule_at,
    };
    let job_id = JobId::new();

    let job = spawner
        .spawn_at(job_id, job_config, schedule_at)
        .await
        .expect("Failed to create and spawn job");

    // Job should not have run yet (we haven't advanced time)
    // Give a small real-time delay for the poller to poll
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert!(
        recorded_time.lock().await.is_none(),
        "Job should not have run before scheduled time"
    );

    // Advance the clock past the scheduled time
    controller
        .advance(std::time::Duration::from_secs(61))
        .await;

    // Give the poller time to pick up and execute the job
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify the job ran and recorded the correct time
    let execution_time = recorded_time
        .lock()
        .await
        .expect("Job should have recorded execution time");

    // The execution time should be at or after the scheduled time
    assert!(
        execution_time >= schedule_at,
        "Job execution time ({}) should be >= scheduled time ({})",
        execution_time,
        schedule_at
    );

    // Verify the job is completed
    let job = jobs.find(job.id).await?;
    assert!(job.completed(), "Job should be completed");

    Ok(())
}
