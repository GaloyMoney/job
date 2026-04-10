mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    ClockHandle, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobOutcomes, JobRunner,
    JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySettings,
    error::JobError,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestJobConfig {
    delay_ms: u64,
}

struct TestJobInitializer {
    job_type: JobType,
}

impl JobInitializer for TestJobInitializer {
    type Config = TestJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
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

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("create-and-run-job"),
    });

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

    let mut attempts = 0;
    let max_attempts = 50;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let job = jobs.find(job.id).await?;
        if job.completed() {
            break;
        }
        attempts += 1;
        if attempts >= max_attempts {
            panic!(
                "Job did not complete within {} attempts ({}ms)",
                max_attempts,
                max_attempts * 100
            );
        }
    }

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

    // Create a manual clock for deterministic testing
    let (clock, controller) = ClockHandle::manual();
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
    controller.advance(std::time::Duration::from_secs(61)).await;

    // Poll until the job completes (with timeout)
    let mut attempts = 0;
    let max_attempts = 50;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let job = jobs.find(job.id).await?;
        if job.completed() {
            break;
        }
        attempts += 1;
        if attempts >= max_attempts {
            panic!(
                "Job did not complete within {} attempts ({}ms)",
                max_attempts,
                max_attempts * 100
            );
        }
    }

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

    Ok(())
}

// -- Queue ID tests --

/// A job that signals when it starts, then waits for an external release before completing.
#[derive(Debug, Serialize, Deserialize)]
struct QueueJobConfig {
    label: String,
}

struct QueueJobInitializer {
    job_type: JobType,
    started: Arc<Mutex<Vec<String>>>,
    completed: Arc<Mutex<Vec<String>>>,
    release: Arc<Notify>,
}

impl JobInitializer for QueueJobInitializer {
    type Config = QueueJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: QueueJobConfig = job.config()?;
        Ok(Box::new(QueueJobRunner {
            label: config.label,
            started: Arc::clone(&self.started),
            completed: Arc::clone(&self.completed),
            release: Arc::clone(&self.release),
        }))
    }
}

struct QueueJobRunner {
    label: String,
    started: Arc<Mutex<Vec<String>>>,
    completed: Arc<Mutex<Vec<String>>>,
    release: Arc<Notify>,
}

#[async_trait]
impl JobRunner for QueueJobRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.started.lock().await.push(self.label.clone());
        self.release.notified().await;
        self.completed.lock().await.push(self.label.clone());
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_queue_id_serializes_execution() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let started = Arc::new(Mutex::new(Vec::<String>::new()));
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new(Notify::new());

    let spawner = jobs.add_initializer(QueueJobInitializer {
        job_type: JobType::new("queue-serial"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    // Spawn two jobs with the same queue_id
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "A".into() },
            "serial-queue",
        )
        .await?;
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "B".into() },
            "serial-queue",
        )
        .await?;

    // Wait for job A to start
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if !started.lock().await.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job A never started");
    }

    // Give the poller time to pick up B (if it incorrectly would)
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Only one job should have started
    assert_eq!(
        started.lock().await.len(),
        1,
        "Only 1 job should be running"
    );
    assert_eq!(started.lock().await[0], "A");

    // Release A
    release.notify_one();

    // Wait for A to complete and B to start
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if started.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job B never started after A completed");
    }

    // Release B
    release.notify_one();

    // Wait for B to complete
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if completed.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job B never completed");
    }

    assert_eq!(completed.lock().await.as_slice(), &["A", "B"]);

    Ok(())
}

#[tokio::test]
async fn test_different_queue_ids_run_concurrently() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let started = Arc::new(Mutex::new(Vec::<String>::new()));
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new(Notify::new());

    let spawner = jobs.add_initializer(QueueJobInitializer {
        job_type: JobType::new("queue-concurrent"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    // Spawn jobs with different queue_ids
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "Q1".into() },
            "queue-1",
        )
        .await?;
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "Q2".into() },
            "queue-2",
        )
        .await?;

    // Both should start concurrently
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if started.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 100,
            "Both jobs should start concurrently, started: {:?}",
            started.lock().await
        );
    }

    // Release both
    release.notify_waiters();

    // Wait for both to complete
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if completed.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Both jobs should complete");
    }

    Ok(())
}

#[tokio::test]
async fn test_non_queued_jobs_unaffected() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let started = Arc::new(Mutex::new(Vec::<String>::new()));
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new(Notify::new());

    let spawner = jobs.add_initializer(QueueJobInitializer {
        job_type: JobType::new("queue-noqueue"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    // Spawn two jobs WITHOUT queue_id — they should run concurrently
    spawner
        .spawn(
            JobId::new(),
            QueueJobConfig {
                label: "NO_Q1".into(),
            },
        )
        .await?;
    spawner
        .spawn(
            JobId::new(),
            QueueJobConfig {
                label: "NO_Q2".into(),
            },
        )
        .await?;

    // Both should start concurrently
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if started.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 100,
            "Non-queued jobs should start concurrently, started: {:?}",
            started.lock().await
        );
    }

    // Release both
    release.notify_waiters();

    // Wait for completion
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if completed.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Non-queued jobs should complete");
    }

    Ok(())
}

#[tokio::test]
async fn test_bulk_spawn_creates_and_runs_all_jobs() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("bulk-spawn-job"),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let specs: Vec<JobSpec<TestJobConfig>> = (0..5)
        .map(|i| JobSpec::new(JobId::new(), TestJobConfig { delay_ms: 10 + i }))
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();

    let spawned = spawner.spawn_all(specs).await?;
    assert_eq!(spawned.len(), 5);

    // Wait for all jobs to complete
    let mut attempts = 0;
    let max_attempts = 100;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let mut all_done = true;
        for id in &ids {
            let job = jobs.find(*id).await?;
            if !job.completed() {
                all_done = false;
                break;
            }
        }
        if all_done {
            break;
        }
        attempts += 1;
        assert!(
            attempts < max_attempts,
            "Not all bulk-spawned jobs completed in time"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_bulk_spawn_rolls_back_on_duplicate_id() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("bulk-spawn-dup-job"),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let duplicate_id = JobId::new();
    let specs = vec![
        JobSpec::new(duplicate_id, TestJobConfig { delay_ms: 10 }),
        JobSpec::new(JobId::new(), TestJobConfig { delay_ms: 10 }),
        JobSpec::new(duplicate_id, TestJobConfig { delay_ms: 10 }),
    ];

    let result = spawner.spawn_all(specs).await;
    assert!(
        matches!(result, Err(JobError::DuplicateId(_))),
        "Expected DuplicateId error, got err: {:?}",
        result.as_ref().err(),
    );

    // The first job should also not be persisted (transaction rolled back)
    let find_result = jobs.find(duplicate_id).await;
    assert!(
        find_result.is_err(),
        "No jobs should be persisted after rollback"
    );

    Ok(())
}

#[tokio::test]
async fn test_bulk_spawn_empty_batch() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("bulk-spawn-empty-job"),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let result = spawner.spawn_all(vec![]).await?;
    assert!(result.is_empty());

    Ok(())
}

// -- await_completion tests --

/// An initializer whose runner always returns an error.
struct FailingJobInitializer;

#[derive(Debug, Serialize, Deserialize)]
struct FailingJobConfig;

impl JobInitializer for FailingJobInitializer {
    type Config = FailingJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("failing-job")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(1),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(FailingJobRunner))
    }
}

struct FailingJobRunner;

#[async_trait]
impl JobRunner for FailingJobRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Err("intentional failure".into())
    }
}

#[tokio::test]
async fn test_await_completion_on_success() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-success-job"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner
        .spawn(job_id, TestJobConfig { delay_ms: 50 })
        .await?;

    let jobs_clone = jobs.clone();
    let handle = tokio::spawn(async move { jobs_clone.await_completion(job_id, None).await });

    let outcome = handle.await??;
    assert_eq!(outcome.state(), JobTerminalState::Completed);

    Ok(())
}

#[tokio::test]
async fn test_await_completion_on_error() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(FailingJobInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, FailingJobConfig).await?;

    let jobs_clone = jobs.clone();
    let handle = tokio::spawn(async move { jobs_clone.await_completion(job_id, None).await });

    let outcome = handle.await??;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    Ok(())
}

#[tokio::test]
async fn test_await_completion_already_completed() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-already-job"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner
        .spawn(job_id, TestJobConfig { delay_ms: 10 })
        .await?;

    // Wait for the job to complete via polling first
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let job = jobs.find(job_id).await?;
        if job.completed() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job never completed");
    }

    // Now call await_completion — should return immediately
    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);

    Ok(())
}

// -- Result passing tests --

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct MyResult {
    value: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResultJobConfig;

struct ResultJobInitializer;

impl JobInitializer for ResultJobInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("result-job")
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(ResultJobRunner))
    }
}

struct ResultJobRunner;

#[async_trait]
impl JobRunner for ResultJobRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Incremental updates — only the last value is persisted
        current_job.set_result(&MyResult { value: 1 }).await?;
        current_job.set_result(&MyResult { value: 42 }).await?;
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_await_completion_returns_result() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(ResultJobInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    let result: MyResult = outcome
        .result()
        .expect("deserialize result")
        .expect("result should be Some");
    assert_eq!(result, MyResult { value: 42 });

    Ok(())
}

struct PartialResultThenErrorInitializer;

impl JobInitializer for PartialResultThenErrorInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("partial-result-error-job")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(1),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(PartialResultThenErrorRunner))
    }
}

struct PartialResultThenErrorRunner;

#[async_trait]
impl JobRunner for PartialResultThenErrorRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Simulate processing 50 items then failing — partial progress preserved
        current_job.set_result(&MyResult { value: 50 }).await?;
        current_job.set_result(&MyResult { value: 99 }).await?;
        Err("intentional failure after setting result".into())
    }
}

#[tokio::test]
async fn test_await_completion_returns_partial_result_on_error() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(PartialResultThenErrorInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    let result: MyResult = outcome
        .result()
        .expect("deserialize result")
        .expect("partial result should be Some");
    assert_eq!(result, MyResult { value: 99 });

    Ok(())
}

struct NoResultJobInitializer;

impl JobInitializer for NoResultJobInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("no-result-job")
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(NoResultJobRunner))
    }
}

struct NoResultJobRunner;

#[async_trait]
impl JobRunner for NoResultJobRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_await_completion_no_result() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(NoResultJobInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    assert!(
        outcome
            .result::<serde_json::Value>()
            .expect("deserialize")
            .is_none()
    );

    Ok(())
}

// -- Incremental set_result tests --

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct BatchProgress {
    processed: u32,
    total: u32,
}

struct IncrementalResultInitializer;

impl JobInitializer for IncrementalResultInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("incremental-result-job")
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(IncrementalResultRunner))
    }
}

struct IncrementalResultRunner;

#[async_trait]
impl JobRunner for IncrementalResultRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let total = 5;
        for i in 1..=total {
            current_job
                .set_result(&BatchProgress {
                    processed: i,
                    total,
                })
                .await?;
        }
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_set_result_multiple_calls_keeps_last() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(IncrementalResultInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    let progress: BatchProgress = outcome
        .result()
        .expect("deserialize result")
        .expect("result should be Some");
    assert_eq!(
        progress,
        BatchProgress {
            processed: 5,
            total: 5
        }
    );

    Ok(())
}

struct IncrementalResultThenErrorInitializer;

impl JobInitializer for IncrementalResultThenErrorInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("incremental-error-result-job")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(1),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(IncrementalResultThenErrorRunner))
    }
}

struct IncrementalResultThenErrorRunner;

#[async_trait]
impl JobRunner for IncrementalResultThenErrorRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let total = 100;
        for i in 1..=50 {
            current_job
                .set_result(&BatchProgress {
                    processed: i,
                    total,
                })
                .await?;
        }
        Err("failed at item 51".into())
    }
}

#[tokio::test]
async fn test_set_result_partial_progress_preserved_on_error() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(IncrementalResultThenErrorInitializer);
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs.await_completion(job_id, None).await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    let progress: BatchProgress = outcome
        .result()
        .expect("deserialize result")
        .expect("partial result should be Some");
    assert_eq!(
        progress,
        BatchProgress {
            processed: 50,
            total: 100
        },
        "partial progress from before the error should be preserved"
    );

    Ok(())
}

#[tokio::test]
async fn test_poll_completion() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("poll-completion-job"),
    });
    jobs.start_poll().await?;

    // Spawn a job scheduled far in the future so it stays pending
    let job_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(job_id, TestJobConfig { delay_ms: 10 }, schedule_at)
        .await?;

    // Poll immediately — job hasn't completed yet
    let state = jobs.poll_completion(job_id).await?;
    assert_eq!(state, None, "Pending job should return None");

    // Now spawn a quick job that will complete fast
    let quick_id = JobId::new();
    spawner
        .spawn(quick_id, TestJobConfig { delay_ms: 10 })
        .await?;

    // Wait for the quick job to finish
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let job = jobs.find(quick_id).await?;
        if job.completed() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Quick job never completed");
    }

    // Poll the completed job — should return terminal state
    let state = jobs.poll_completion(quick_id).await?;
    assert_eq!(
        state,
        Some(JobTerminalState::Completed),
        "Completed job should return Some(Completed)"
    );

    Ok(())
}

#[tokio::test]
async fn test_await_completion_timeout() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-timeout-job"),
    });
    jobs.start_poll().await?;

    // Spawn a job scheduled far in the future so it never completes during the test
    let job_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(job_id, TestJobConfig { delay_ms: 50 }, schedule_at)
        .await?;

    // Call await_completion with a short timeout
    let result = jobs
        .await_completion(job_id, Some(Duration::from_millis(200)))
        .await;

    assert!(
        matches!(result, Err(JobError::TimedOut(id)) if id == job_id),
        "Expected TimedOut error, got: {:?}",
        result,
    );

    Ok(())
}

// -- Multi-day scheduling tests --

#[derive(Debug, Serialize, Deserialize)]
struct MultiDayJobConfig {
    label: String,
}

struct MultiDayJobInitializer {
    execution_times: Arc<Mutex<HashMap<JobId, DateTime<Utc>>>>,
}

impl JobInitializer for MultiDayJobInitializer {
    type Config = MultiDayJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("multi-day-job")
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(MultiDayJobRunner {
            job_id: job.id,
            execution_times: Arc::clone(&self.execution_times),
        }))
    }
}

struct MultiDayJobRunner {
    job_id: JobId,
    execution_times: Arc<Mutex<HashMap<JobId, DateTime<Utc>>>>,
}

#[async_trait]
impl JobRunner for MultiDayJobRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let now = current_job.clock().now();
        self.execution_times.lock().await.insert(self.job_id, now);
        Ok(JobCompletion::Complete)
    }
}

/// Polls until all specified jobs are marked completed in the database.
async fn wait_for_jobs_completed(jobs: &Jobs, ids: &[JobId], max_attempts: usize) {
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let mut all_done = true;
        for id in ids {
            let job = jobs.find(*id).await.expect("job should exist");
            if !job.completed() {
                all_done = false;
                break;
            }
        }
        if all_done {
            return;
        }
        attempts += 1;
        if attempts >= max_attempts {
            panic!(
                "Jobs {:?} did not all complete within {} attempts ({}ms)",
                ids,
                max_attempts,
                max_attempts * 100,
            );
        }
    }
}

/// Test that jobs scheduled across multiple days all fire correctly when the
/// manual clock is advanced one day at a time.
///
/// # What this test verifies
///
/// When `controller.advance(1 day)` is called, the manual clock jumps forward.
/// Housekeeping loops (keep-alive, lost-handler) use `sleep_coalesce()` so they
/// wake once at the final time instead of at every intermediate interval. This
/// means the polling loop can dispatch jobs promptly without being starved by
/// ~1700 housekeeping wake-ups per day-advance.
///
/// # Cross-test isolation
///
/// The lost-handler SQL is scoped to `job_type = ANY(registered_types)`, so a
/// poller with a far-future manual clock only resets its own job types. This
/// prevents cross-test interference when multiple pollers share the same DB.
#[tokio::test]
async fn test_multi_day_scheduling_with_artificial_clock() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let (clock, controller) = ClockHandle::manual();
    let initial_time = clock.now();

    let config = JobSvcConfig::builder()
        .pool(pool)
        .clock(clock.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let execution_times: Arc<Mutex<HashMap<JobId, DateTime<Utc>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let spawner = jobs.add_initializer(MultiDayJobInitializer {
        execution_times: Arc::clone(&execution_times),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    // Schedule 5 jobs at various future times
    let job_2h_a = JobId::new();
    let job_2h_b = JobId::new();
    let job_2d = JobId::new();
    let job_4d = JobId::new();
    let job_7d = JobId::new();

    let at_2h = initial_time + chrono::Duration::hours(2);
    let at_2d = initial_time + chrono::Duration::days(2);
    let at_4d = initial_time + chrono::Duration::days(4);
    let at_7d = initial_time + chrono::Duration::days(7);

    spawner
        .spawn_at(
            job_2h_a,
            MultiDayJobConfig {
                label: "2h-a".into(),
            },
            at_2h,
        )
        .await?;
    spawner
        .spawn_at(
            job_2h_b,
            MultiDayJobConfig {
                label: "2h-b".into(),
            },
            at_2h,
        )
        .await?;
    spawner
        .spawn_at(job_2d, MultiDayJobConfig { label: "2d".into() }, at_2d)
        .await?;
    spawner
        .spawn_at(job_4d, MultiDayJobConfig { label: "4d".into() }, at_4d)
        .await?;
    spawner
        .spawn_at(job_7d, MultiDayJobConfig { label: "7d".into() }, at_7d)
        .await?;

    // No jobs should have run yet
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    assert!(
        execution_times.lock().await.is_empty(),
        "No jobs should run before clock advances"
    );

    let one_day = std::time::Duration::from_secs(86_400);
    let wait_attempts = 50; // 5 seconds — plenty now that housekeeping coalesces

    // --- Day 1: the two 2-hour jobs should fire ---
    controller.advance(one_day).await;
    wait_for_jobs_completed(&jobs, &[job_2h_a, job_2h_b], wait_attempts).await;
    {
        let times = execution_times.lock().await;
        assert!(
            times.contains_key(&job_2h_a),
            "2h-a should have run after day 1"
        );
        assert!(
            times.contains_key(&job_2h_b),
            "2h-b should have run after day 1"
        );
        assert!(
            !times.contains_key(&job_2d),
            "2d should NOT have run after day 1"
        );
        assert!(
            !times.contains_key(&job_4d),
            "4d should NOT have run after day 1"
        );
        assert!(
            !times.contains_key(&job_7d),
            "7d should NOT have run after day 1"
        );
    }

    // --- Day 2: the 2-day job should fire ---
    controller.advance(one_day).await;
    wait_for_jobs_completed(&jobs, &[job_2d], wait_attempts).await;
    {
        let times = execution_times.lock().await;
        assert!(
            times.contains_key(&job_2d),
            "2d should have run after day 2"
        );
        assert!(
            !times.contains_key(&job_4d),
            "4d should NOT have run after day 2"
        );
        assert!(
            !times.contains_key(&job_7d),
            "7d should NOT have run after day 2"
        );
    }

    // --- Day 3: no new jobs ---
    controller.advance(one_day).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    {
        let times = execution_times.lock().await;
        assert_eq!(times.len(), 3, "Only 3 jobs should have run by day 3");
    }

    // --- Day 4: the 4-day job should fire ---
    controller.advance(one_day).await;
    wait_for_jobs_completed(&jobs, &[job_4d], wait_attempts).await;
    {
        let times = execution_times.lock().await;
        assert!(
            times.contains_key(&job_4d),
            "4d should have run after day 4"
        );
        assert!(
            !times.contains_key(&job_7d),
            "7d should NOT have run after day 4"
        );
    }

    // --- Days 5 and 6: no new jobs ---
    controller.advance(one_day).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    controller.advance(one_day).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    {
        let times = execution_times.lock().await;
        assert_eq!(times.len(), 4, "Only 4 jobs should have run by day 6");
    }

    // --- Day 7: the 7-day job should fire ---
    controller.advance(one_day).await;
    wait_for_jobs_completed(&jobs, &[job_7d], wait_attempts).await;

    // All 5 jobs should now be recorded
    {
        let times = execution_times.lock().await;
        assert_eq!(times.len(), 5, "All 5 jobs should have run by day 7");
    }

    // Verify every job is completed in the database
    for id in [job_2h_a, job_2h_b, job_2d, job_4d, job_7d] {
        let job = jobs.find(id).await?;
        assert!(job.completed(), "Job {id} should be completed");
    }

    // Verify execution times are at or after their scheduled times
    {
        let times = execution_times.lock().await;
        for (label, id, scheduled) in [
            ("2h-a", job_2h_a, at_2h),
            ("2h-b", job_2h_b, at_2h),
            ("2d", job_2d, at_2d),
            ("4d", job_4d, at_4d),
            ("7d", job_7d, at_7d),
        ] {
            let exec_time = times[&id];
            assert!(
                exec_time >= scheduled,
                "Job {label} executed at {exec_time} but was scheduled for {scheduled}",
            );
        }
    }

    // Explicit shutdown prevents the lost-handler (which uses our far-future
    // manual clock) from resetting other tests' running jobs to 'pending'.
    jobs.shutdown().await?;

    Ok(())
}

// -- await_completions / JobOutcomes tests --

#[tokio::test]
async fn test_await_completions_batch() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-completions-batch"),
    });
    jobs.start_poll().await?;

    let ids: Vec<JobId> = (0..3).map(|_| JobId::new()).collect();
    for id in &ids {
        spawner.spawn(*id, TestJobConfig { delay_ms: 20 }).await?;
    }

    let outcomes = jobs
        .await_completions(&ids, Some(Duration::from_secs(10)))
        .await?;
    assert_eq!(outcomes.len(), 3);
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    Ok(())
}

#[tokio::test]
async fn test_await_completions_empty_ids() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let _spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-completions-empty"),
    });
    jobs.start_poll().await?;

    let outcomes = jobs.await_completions(&[], None).await?;
    assert!(outcomes.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_await_completions_timeout() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-completions-timeout"),
    });
    jobs.start_poll().await?;

    // Schedule a job far in the future so it never completes
    let job_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(job_id, TestJobConfig { delay_ms: 50 }, schedule_at)
        .await?;

    let result = jobs
        .await_completions(&[job_id], Some(Duration::from_millis(200)))
        .await;

    assert!(
        matches!(result, Err(JobError::TimedOut(_))),
        "Expected TimedOut error, got: {:?}",
        result,
    );

    Ok(())
}

#[tokio::test]
async fn test_job_completion_results_trait() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let success_spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("trait-success"),
    });
    let fail_spawner = jobs.add_initializer(FailingJobInitializer);

    jobs.start_poll().await?;

    // Spawn 2 successful + 1 failing job
    let s1 = JobId::new();
    let s2 = JobId::new();
    let f1 = JobId::new();
    success_spawner
        .spawn(s1, TestJobConfig { delay_ms: 20 })
        .await?;
    success_spawner
        .spawn(s2, TestJobConfig { delay_ms: 20 })
        .await?;
    fail_spawner.spawn(f1, FailingJobConfig).await?;

    let outcomes = jobs
        .await_completions(&[s1, s2, f1], Some(Duration::from_secs(10)))
        .await?;

    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes.failed_count(), 1);
    assert!(!outcomes.all_succeeded());

    // Also test the slice impl
    let slice: &[_] = &outcomes;
    assert_eq!(slice.failed_count(), 1);
    assert!(!slice.all_succeeded());

    // Test all-success case
    let s3 = JobId::new();
    let s4 = JobId::new();
    success_spawner
        .spawn(s3, TestJobConfig { delay_ms: 20 })
        .await?;
    success_spawner
        .spawn(s4, TestJobConfig { delay_ms: 20 })
        .await?;

    let success_outcomes = jobs
        .await_completions(&[s3, s4], Some(Duration::from_secs(10)))
        .await?;
    assert!(success_outcomes.all_succeeded());
    assert_eq!(success_outcomes.failed_count(), 0);

    Ok(())
}

// -- Parent job id tests --

#[tokio::test]
async fn test_spawn_with_parent_job_id() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("parent-child-job"),
    });

    jobs.start_poll().await?;

    // Spawn a parent job
    let parent_id = JobId::new();
    let parent = spawner
        .spawn(parent_id, TestJobConfig { delay_ms: 10 })
        .await?;
    assert!(parent.parent_job_id.is_none());

    // Spawn a standalone job (no parent) to verify empty-list later
    let orphan_id = JobId::new();
    spawner
        .spawn(orphan_id, TestJobConfig { delay_ms: 10 })
        .await?;

    // Spawn multiple child jobs using with_parent (consumes spawner)
    let child_spawner = spawner.with_parent(parent_id);
    let child_id_1 = JobId::new();
    let child_1 = child_spawner
        .spawn(child_id_1, TestJobConfig { delay_ms: 10 })
        .await?;
    assert_eq!(child_1.parent_job_id, Some(parent_id));

    let child_id_2 = JobId::new();
    let child_2 = child_spawner
        .spawn(child_id_2, TestJobConfig { delay_ms: 10 })
        .await?;
    assert_eq!(child_2.parent_job_id, Some(parent_id));

    // Verify we can read them back from the database
    let loaded_child = jobs.find(child_id_1).await?;
    assert_eq!(loaded_child.parent_job_id, Some(parent_id));

    // List children by parent_job_id — should return both
    let children = jobs.list_all_by_parent_job_id(parent_id).await?;
    assert_eq!(children.len(), 2);
    let child_ids: Vec<JobId> = children.iter().map(|j| j.id).collect();
    assert!(child_ids.contains(&child_id_1));
    assert!(child_ids.contains(&child_id_2));

    // Job with no children should return empty list
    let no_children = jobs.list_all_by_parent_job_id(orphan_id).await?;
    assert!(no_children.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_spawn_without_parent_still_works() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("no-parent-job"),
    });

    jobs.start_poll().await?;

    let job_id = JobId::new();
    let job = spawner
        .spawn(job_id, TestJobConfig { delay_ms: 10 })
        .await?;
    assert!(job.parent_job_id.is_none());

    let loaded = jobs.find(job_id).await?;
    assert!(loaded.parent_job_id.is_none());

    Ok(())
}

// -- Automatic parent propagation tests --

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentSpawnerConfig {
    child_id: JobId,
}

struct ParentSpawnerInitializer;

impl JobInitializer for ParentSpawnerInitializer {
    type Config = ParentSpawnerConfig;

    fn job_type(&self) -> JobType {
        JobType::new("auto-parent-spawner")
    }

    fn init(
        &self,
        job: &Job,
        spawner: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: ParentSpawnerConfig = job.config()?;
        Ok(Box::new(ParentSpawnerRunner {
            child_id: config.child_id,
            spawner,
        }))
    }
}

struct ParentSpawnerRunner {
    child_id: JobId,
    spawner: JobSpawner<ParentSpawnerConfig>,
}

#[async_trait]
impl JobRunner for ParentSpawnerRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Spawn a child using the injected spawner — parent_job_id should be set automatically
        self.spawner
            .spawn(
                self.child_id,
                ParentSpawnerConfig {
                    child_id: JobId::new(),
                },
            )
            .await?;
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
async fn test_automatic_parent_propagation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_initializer(ParentSpawnerInitializer);

    jobs.start_poll().await?;

    // Spawn parent job — its runner will spawn a child using the injected spawner
    let parent_id = JobId::new();
    let child_id = JobId::new();
    spawner
        .spawn(parent_id, ParentSpawnerConfig { child_id })
        .await?;

    // Wait for the parent job to complete
    jobs.await_completion(parent_id, Some(Duration::from_secs(5)))
        .await?;

    // Wait for child job to complete too
    jobs.await_completion(child_id, Some(Duration::from_secs(5)))
        .await?;

    // Verify the child job has the parent_job_id set automatically
    let child = jobs.find(child_id).await?;
    assert_eq!(
        child.parent_job_id,
        Some(parent_id),
        "child spawned by runner should automatically get parent_job_id"
    );

    // Verify the parent itself has no parent
    let parent = jobs.find(parent_id).await?;
    assert!(
        parent.parent_job_id.is_none(),
        "externally-spawned job should have no parent"
    );

    // Verify listing children via parent_job_id works
    let children = jobs.list_all_by_parent_job_id(parent_id).await?;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, child_id);

    Ok(())
}

// -- Lost handler instance filter tests --

/// A job runner that parks indefinitely until shutdown is requested,
/// mimicking long-running listener-style jobs (e.g. outbox consumers).
#[derive(Debug, Serialize, Deserialize)]
struct InfiniteListenerConfig;

struct InfiniteListenerInitializer;

impl JobInitializer for InfiniteListenerInitializer {
    type Config = InfiniteListenerConfig;

    fn job_type(&self) -> JobType {
        JobType::new("infinite-listener")
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(InfiniteListenerRunner))
    }
}

struct InfiniteListenerRunner;

#[async_trait]
impl JobRunner for InfiniteListenerRunner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Park until shutdown, like an outbox listener would.
        current_job.shutdown_requested().await;
        Ok(JobCompletion::Complete)
    }
}

/// Verify that the lost-handler does NOT mark running jobs owned by its own
/// poller instance as lost. Without the instance filter, a race between the
/// lost-handler and the keep-alive handler can desync the in-memory job
/// counter and permanently wedge the poller.
#[tokio::test]
async fn test_lost_handler_skips_own_instance_running_jobs() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();

    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .poller_config(JobPollerConfig {
            job_lost_interval: Duration::from_secs(10),
            // Pin min == max == 2 so a single leaked counter slot would wedge
            // the poller. This makes the assertion strict.
            min_jobs_per_process: 2,
            max_jobs_per_process: 2,
            ..Default::default()
        })
        .build()
        .expect("build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    // 1) Register a long-running listener job and a short canary job.
    let listener_spawner = jobs.add_initializer(InfiniteListenerInitializer);
    let canary_spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("lost-handler-canary"),
    });

    jobs.start_poll().await?;

    // 2) Spawn the listener. It will park at shutdown_requested().
    let listener_id = JobId::new();
    listener_spawner
        .spawn(listener_id, InfiniteListenerConfig)
        .await?;

    // Wait for it to reach 'running' in the DB.
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let row: (String,) = sqlx::query_as("SELECT state::text FROM job_executions WHERE id = $1")
            .bind(listener_id)
            .fetch_one(&pool)
            .await?;
        if row.0 == "running" {
            break;
        }
        attempts += 1;
        assert!(attempts < 50, "listener job never reached running state");
    }

    // 3) Force alive_at into the past so the lost-handler would find it stale.
    let stale_time = clock.now() - chrono::Duration::seconds(60);
    sqlx::query("UPDATE job_executions SET alive_at = $1 WHERE id = $2")
        .bind(stale_time)
        .bind(listener_id)
        .execute(&pool)
        .await?;

    // 4) Advance the manual clock by 2 * job_lost_interval. This fires both
    //    the keep-alive and lost-handler coalesce wakes.
    controller.advance(Duration::from_secs(20)).await;

    // Give the loops a tick.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 5) The listener must still be 'running' — the lost-handler should have
    //    skipped it because it belongs to our own instance.
    let row: (String,) = sqlx::query_as("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(listener_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        row.0, "running",
        "own-instance running job must not be marked lost"
    );

    // 6) The poller must still be healthy — spawn a canary and confirm it runs.
    //    If the counter had leaked, the poller would be wedged and this would
    //    time out.
    let canary_id = JobId::new();
    canary_spawner
        .spawn(canary_id, TestJobConfig { delay_ms: 10 })
        .await?;
    wait_for_jobs_completed(&jobs, &[canary_id], 50).await;

    jobs.shutdown().await?;
    Ok(())
}

/// Verify that the lost-handler still rescues jobs left behind by a crashed
/// peer process (different poller_instance_id).
#[tokio::test]
async fn test_lost_handler_rescues_other_instance_jobs() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;
    let (clock, controller) = ClockHandle::manual();

    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .poller_config(JobPollerConfig {
            job_lost_interval: Duration::from_secs(10),
            ..Default::default()
        })
        .build()
        .expect("build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let _spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("orphan-rescue"),
    });

    // Insert rows directly as if left behind by a crashed peer.
    let orphan_id = JobId::new();
    let other_instance = uuid::Uuid::now_v7();
    let now = clock.now();
    let stale_alive_at = now - chrono::Duration::seconds(60);

    // Parent row in `jobs` (FK target for job_executions).
    sqlx::query(
        "INSERT INTO jobs (id, unique_per_type, job_type, created_at) VALUES ($1, false, 'orphan-rescue', $2)",
    )
    .bind(orphan_id)
    .bind(now)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO job_executions (id, job_type, state, alive_at, poller_instance_id, execution_state_json, attempt_index, created_at)
        VALUES ($1, 'orphan-rescue', 'running', $2, $3, '{}', 1, $4)
        "#,
    )
    .bind(orphan_id)
    .bind(stale_alive_at)
    .bind(other_instance)
    .bind(now)
    .execute(&pool)
    .await?;

    jobs.start_poll().await?;

    // Poll until the orphan is rescued. Each iteration advances the clock
    // by job_lost_interval so the lost-handler gets a fresh wake even if it
    // missed the previous advance (e.g. because it hadn't registered its
    // coalesce sleep yet).
    let mut attempts = 0;
    loop {
        controller.advance(Duration::from_secs(10)).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let row: (String,) = sqlx::query_as("SELECT state::text FROM job_executions WHERE id = $1")
            .bind(orphan_id)
            .fetch_one(&pool)
            .await?;
        if row.0 == "pending" {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 20,
            "orphan from another instance must be rescued"
        );
    }

    jobs.shutdown().await?;
    Ok(())
}
