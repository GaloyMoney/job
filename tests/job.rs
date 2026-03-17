mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    ClockHandle, CurrentJob, Job, JobCompletion, JobId, JobInitializer,
    JobRunner, JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySettings,
    error::JobError,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Serialize, Deserialize)]
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

#[tokio::test]
async fn test_cancel_pending_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("cancel-pending-job"),
    });

    // Spawn a job scheduled far in the future so it stays pending
    let job_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(job_id, TestJobConfig { delay_ms: 50 }, schedule_at)
        .await?;

    // Cancel the pending job
    jobs.cancel_job(job_id).await?;

    // Verify it's findable as a cancelled/completed entity
    let found = jobs.find(job_id).await?;
    assert!(found.completed(), "Cancelled job should be completed");
    assert!(found.cancelled(), "Job should be marked as cancelled");

    Ok(())
}

#[tokio::test]
async fn test_cancel_running_job_fails() -> anyhow::Result<()> {
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
        job_type: JobType::new("cancel-running-test"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let job_id = JobId::new();
    spawner
        .spawn(job_id, QueueJobConfig { label: "X".into() })
        .await?;

    // Wait for the job to start running
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if !started.lock().await.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job never started");
    }

    // Cancel on a running job should fail
    let result = jobs.cancel_job(job_id).await;
    assert!(
        matches!(result, Err(JobError::CannotCancelJob)),
        "Cancelling a running job should return JobNotPending, got err: {:?}",
        result.err(),
    );

    // Release the job so it completes normally
    release.notify_one();

    // Wait for completion
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

    Ok(())
}

#[tokio::test]
async fn test_cancel_already_completed_job_is_idempotent() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("cancel-completed-job"),
    });

    jobs.start_poll()
        .await
        .expect("Failed to start job polling");

    let job_id = JobId::new();
    spawner
        .spawn(job_id, TestJobConfig { delay_ms: 10 })
        .await?;

    // Wait for the job to complete
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

    // Cancel on an already completed job is a no-op
    jobs.cancel_job(job_id).await?;

    let job = jobs.find(job_id).await?;
    assert!(
        !job.cancelled(),
        "Completed job should not be marked cancelled"
    );
    assert!(job.completed(), "Job should still be completed");

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
    let handle = tokio::spawn(async move { jobs_clone.await_completion(job_id).await });

    let state = handle.await??;
    assert_eq!(state, JobTerminalState::Completed);

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
    let handle = tokio::spawn(async move { jobs_clone.await_completion(job_id).await });

    let state = handle.await??;
    assert_eq!(state, JobTerminalState::Errored);

    Ok(())
}

#[tokio::test]
async fn test_await_completion_on_cancel() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-cancel-job"),
    });
    jobs.start_poll().await?;

    // Spawn a job scheduled far in the future so it stays pending
    let job_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(job_id, TestJobConfig { delay_ms: 50 }, schedule_at)
        .await?;

    let jobs_clone = jobs.clone();
    let handle = tokio::spawn(async move { jobs_clone.await_completion(job_id).await });

    // Give the waiter time to register before cancelling
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    jobs.cancel_job(job_id).await?;

    let state = handle.await??;
    assert_eq!(state, JobTerminalState::Cancelled);

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
    let state = jobs.await_completion(job_id).await?;
    assert_eq!(state, JobTerminalState::Completed);

    Ok(())
}
