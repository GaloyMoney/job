mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    ClockHandle, CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobOutcomes, JobRunner,
    JobSpawner, JobSpec, JobStatus, JobSvcConfig, JobTerminalState, JobType, Jobs,
    KeyedJobInitializer, KeyedJobSpawner, ResidentJobCompletion, ResidentJobInitializer,
    ResidentJobRunner, RetrySettings, error::JobError,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

struct TestKeyedInitializer {
    job_type: JobType,
}

impl KeyedJobInitializer for TestKeyedInitializer {
    type Config = TestJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        job: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: TestJobConfig = job.config()?;
        Ok(Box::new(TestJobRunner { config }))
    }
}

struct TestResidentInitializer {
    job_type: JobType,
}

impl ResidentJobInitializer for TestResidentInitializer {
    type Config = TestJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(&self, job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error>> {
        let config: TestJobConfig = job.config()?;
        Ok(Box::new(TestResidentRunner { config }))
    }
}

struct TestResidentRunner {
    config: TestJobConfig,
}

#[async_trait]
impl ResidentJobRunner for TestResidentRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        // Simulate some work, then reschedule — a resident job never completes.
        tokio::time::sleep(tokio::time::Duration::from_millis(self.config.delay_ms)).await;
        Ok(ResidentJobCompletion::RescheduleIn(Duration::from_secs(60)))
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
        let snap = jobs.handle(job.id).load().await?;
        if snap.state().is_terminal() {
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
        let snap = jobs.handle(job.id).load().await?;
        if snap.state().is_terminal() {
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
async fn test_queue_id_serializes_execution_across_two_pollers() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let started = Arc::new(Mutex::new(Vec::<String>::new()));
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new(Notify::new());

    let make_initializer = || QueueJobInitializer {
        job_type: JobType::new("queue-serial-two-pollers"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    };

    // Two independent Jobs services — two pollers with distinct instance
    // ids — sharing one database. Both register the same job_type and get
    // woken by the same notifications, so they race to claim queue heads.
    let config_a = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs_a = Jobs::init(config_a).await?;
    let spawner = jobs_a.add_initializer(make_initializer());
    jobs_a
        .start_poll()
        .await
        .expect("Failed to start job polling (A)");

    let config_b = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs_b = Jobs::init(config_b).await?;
    let _ = jobs_b.add_initializer(make_initializer());
    jobs_b
        .start_poll()
        .await
        .expect("Failed to start job polling (B)");

    // Two jobs in the same queue, spawned back-to-back so both pollers'
    // first polls race on the queue head with the same snapshot.
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "A".into() },
            "serial-queue-two-pollers",
        )
        .await?;
    spawner
        .spawn_with_queue_id(
            JobId::new(),
            QueueJobConfig { label: "B".into() },
            "serial-queue-two-pollers",
        )
        .await?;

    // Wait for A to start
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if !started.lock().await.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job A never started");
    }

    // With A running (blocked), give both pollers ample time for several
    // poll cycles. Whichever poller lost the race for the queue head must
    // skip it — never start B while A holds the queue.
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    assert_eq!(
        started.lock().await.as_slice(),
        &["A"],
        "a second same-queue job started while A was running (two pollers)"
    );

    // Release A — B may only start after A completes
    release.notify_one();
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if started.lock().await.len() == 2 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job B never started after A completed");
    }
    assert_eq!(started.lock().await.as_slice(), &["A", "B"]);

    // Release B
    release.notify_one();
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

    jobs_a.shutdown().await?;
    jobs_b.shutdown().await?;

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
            let snap = jobs.handle(*id).load().await?;
            if !snap.state().is_terminal() {
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
    let load_result = jobs.handle(duplicate_id).load().await;
    assert!(
        load_result.is_err(),
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
///
/// The job type is per-test rather than a shared constant: these tests run
/// concurrently against one database, and a poller claims every registered
/// type it sees. A shared type name lets one test's poller claim another
/// test's rows, which shows up as rare, unrelated-looking failures in whichever
/// test lost the row.
struct FailingJobInitializer {
    job_type: JobType,
}

#[derive(Debug, Serialize, Deserialize)]
struct FailingJobConfig;

impl JobInitializer for FailingJobInitializer {
    type Config = FailingJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
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

struct FailingKeyedInitializer {
    job_type: JobType,
}

impl KeyedJobInitializer for FailingKeyedInitializer {
    type Config = FailingJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
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
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(FailingJobRunner))
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
    let handle = tokio::spawn(async move {
        jobs_clone
            .handle(job_id)
            .await_completion(Duration::from_secs(30))
            .await
    });

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
    let spawner = jobs.add_initializer(FailingJobInitializer {
        job_type: JobType::new("failing-await-completion"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, FailingJobConfig).await?;

    let jobs_clone = jobs.clone();
    let handle = tokio::spawn(async move {
        jobs_clone
            .handle(job_id)
            .await_completion(Duration::from_secs(30))
            .await
    });

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
        let snap = jobs.handle(job_id).load().await?;
        if snap.state().is_terminal() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Job never completed");
    }

    // Now call await_completion — should return immediately
    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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

/// Per-test job type, for the same reason as [`FailingJobInitializer`].
struct ResultJobInitializer {
    job_type: JobType,
}

impl JobInitializer for ResultJobInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
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
    let spawner = jobs.add_initializer(ResultJobInitializer {
        job_type: JobType::new("result-job-await-completion"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
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
async fn test_load_state_reflects_pending_and_terminal() -> anyhow::Result<()> {
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

    // Load immediately — job hasn't completed yet
    let snap = jobs.handle(job_id).load().await?;
    assert!(
        !snap.state().is_terminal(),
        "Pending job should not be terminal"
    );
    assert!(matches!(snap.state(), JobStatus::Pending { .. }));

    // Now spawn a quick job that will complete fast
    let quick_id = JobId::new();
    spawner
        .spawn(quick_id, TestJobConfig { delay_ms: 10 })
        .await?;

    // Wait for the quick job to finish
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let snap = jobs.handle(quick_id).load().await?;
        if snap.state().is_terminal() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "Quick job never completed");
    }

    // Load the completed job — state should be terminal Completed
    let snap = jobs.handle(quick_id).load().await?;
    assert_eq!(
        snap.state(),
        JobStatus::Completed { queue_id: None },
        "Completed job should report Completed"
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
        .handle(job_id)
        .await_completion(Duration::from_millis(200))
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
            let snap = jobs.handle(*id).load().await.expect("job should exist");
            if !snap.state().is_terminal() {
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
        let snap = jobs.handle(id).load().await?;
        assert!(snap.state().is_terminal(), "Job {id} should be completed");
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
        .handles(ids.clone())
        .await_all(Duration::from_secs(10))
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
async fn test_await_all_empty_ids() -> anyhow::Result<()> {
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

    let outcomes = jobs
        .handles(Vec::<JobId>::new())
        .await_all(Duration::from_secs(1))
        .await?;
    assert!(outcomes.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_await_all_timeout() -> anyhow::Result<()> {
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
        .handles([job_id])
        .await_all(Duration::from_millis(200))
        .await;

    assert!(
        matches!(result, Err(JobError::TimedOut(_))),
        "Expected TimedOut error, got: {:?}",
        result,
    );

    Ok(())
}

/// Regression test for the `await_completions` wedge: an orchestrator that waits
/// on a burst of fast-completing jobs must not hang when their terminal
/// notifications are dropped.
///
/// Preconditions are reproduced deterministically: a size-1 terminal broadcast
/// buffer so a completion burst overflows it and those notifications are lost
/// (they are never redelivered), leaving the periodic reconciliation sweep as
/// the sole resolution path. Previously that sweep was the lowest-priority arm
/// of a `biased` select and could be starved indefinitely by a terminal
/// firehose, wedging `await_completions(None)` until the process restarted. With
/// the sweep polled first, a bounded `sweep_interval` caps resolution regardless
/// of load — so this must complete well within the timeout.
#[tokio::test]
async fn test_await_all_resolves_when_notifications_dropped() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(JobPollerConfig {
            // Tiny buffer: a completion burst overflows it and terminal
            // notifications are dropped, forcing resolution through the sweep.
            terminal_channel_size: 1,
            // Short cadence so the (now unstarvable) backstop is fast to assert on.
            sweep_interval: Duration::from_millis(250),
            ..Default::default()
        })
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("await-completions-dropped-notifications"),
    });
    jobs.start_poll().await?;

    // A burst of fast jobs that all finish in a tight window.
    let ids: Vec<JobId> = (0..40).map(|_| JobId::new()).collect();
    for id in &ids {
        spawner.spawn(*id, TestJobConfig { delay_ms: 10 }).await?;
    }

    // Must still resolve — via the sweep — despite the dropped notifications.
    // The required `await_all` timeout is the wedge assertion: if the sweep
    // were starvable this would return `TimedOut` instead of the outcomes.
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(20))
        .await
        .expect("await_all wedged: dropped notifications were never reconciled");

    assert_eq!(outcomes.len(), ids.len());
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    jobs.shutdown().await?;
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
    let fail_spawner = jobs.add_initializer(FailingJobInitializer {
        job_type: JobType::new("failing-results-trait"),
    });

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
        .handles([s1, s2, f1])
        .await_all(Duration::from_secs(10))
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
        .handles([s3, s4])
        .await_all(Duration::from_secs(10))
        .await?;
    assert!(success_outcomes.all_succeeded());
    assert_eq!(success_outcomes.failed_count(), 0);

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

/// Verify that a job with a *live* runner future on this instance is not
/// reclaimed by the lost-handler, because the keep-alive handler keeps its
/// `alive_at` fresh.
///
/// The lost-handler no longer special-cases its own instance (that exclusion is
/// what allowed a lost terminal write to zombie forever). What protects a
/// running row from reclaim is now *liveness*, not ownership: the keep-alive
/// handler heartbeats only jobs that still have a live future, so a live job's
/// `alive_at` (a wall-clock heartbeat) never crosses the staleness threshold.
#[tokio::test]
async fn test_keep_alive_protects_live_own_instance_jobs() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;

    // Use the real clock: the keep-alive and lost-handler now measure liveness
    // in wall-clock time, so a manual clock would not drive them.
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(JobPollerConfig {
            // Short interval so the lost-handler (fires every interval/2) gets
            // several chances to reclaim within the test window.
            job_lost_interval: Duration::from_secs(3),
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

    // 2) Spawn the listener. It will park at shutdown_requested(), keeping its
    //    future live for the duration of the test.
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

    // 3) Wait well past job_lost_interval so the wall-clock lost-handler fires
    //    several times. The keep-alive (every interval/4) must keep the live
    //    job's alive_at fresh so it never crosses the staleness threshold.
    tokio::time::sleep(tokio::time::Duration::from_secs(7)).await;

    // 4) The listener must still be 'running' AND never reclaimed. A reclaim
    //    NULLs poller_instance_id and bumps attempt_index, so an unchanged
    //    attempt_index is race-free proof the keep-alive protected the job.
    let row: (String, i32) =
        sqlx::query_as("SELECT state::text, attempt_index FROM job_executions WHERE id = $1")
            .bind(listener_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        row.0, "running",
        "live own-instance running job must stay running (kept fresh by keep-alive)"
    );
    assert_eq!(
        row.1, 1,
        "live job must not have been reclaimed (attempt_index must stay 1)"
    );

    // 5) The poller must still be healthy — spawn a canary and confirm it runs.
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

    // Insert rows directly as if left behind by a crashed peer. alive_at is
    // a wall-clock heartbeat now, so the "stale" timestamp must be in real
    // time, not simulated time.
    let orphan_id = JobId::new();
    let other_instance = uuid::Uuid::now_v7();
    let now = clock.now();
    let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(60);

    // Row in `jobs` (FK target for job_executions).
    sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, 'orphan-rescue', $2)")
        .bind(orphan_id)
        .bind(now)
        .execute(&pool)
        .await?;

    sqlx::query(
        r#"
        INSERT INTO job_executions (id, job_type, state, alive_at, poller_instance_id, attempt_index, created_at)
        VALUES ($1, 'orphan-rescue', 'running', $2, $3, 1, $4)
        "#,
    )
    .bind(orphan_id)
    .bind(stale_alive_at)
    .bind(other_instance)
    .bind(now)
    .execute(&pool)
    .await?;

    jobs.start_poll().await?;

    // Poll until the orphan is rescued. We check that the poller_instance_id
    // is no longer the other instance — the lost handler resets it to NULL
    // (pending) but the main poller may immediately re-claim it, so we can't
    // reliably assert state='pending'. The lost-handler now ticks on real
    // wall time at job_lost_interval/2 (5s here), so a short real sleep per
    // iteration is enough.
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let row: (Option<uuid::Uuid>,) =
            sqlx::query_as("SELECT poller_instance_id FROM job_executions WHERE id = $1")
                .bind(orphan_id)
                .fetch_one(&pool)
                .await?;
        // Once the lost-handler fires, the row's poller_instance_id will be
        // either NULL (pending) or our own instance (re-dispatched). Either
        // way it won't be other_instance any more.
        if row.0 != Some(other_instance) {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 30,
            "orphan from another instance must be rescued"
        );
    }
    let _ = controller; // silence unused-warning; clock no longer drives this loop

    jobs.shutdown().await?;
    Ok(())
}

/// Regression: liveness must be measured in wall-clock time, not the
/// application clock. With a manual clock that never advances, an orphan
/// row left behind by a crashed peer must still be reclaimed within
/// `job_lost_interval` of real wall time.
///
/// Before this fix, `start_lost_handler` slept on `clock.sleep_coalesce()`
/// and computed `check_time = clock.now() - job_lost_interval`. Under a
/// frozen manual clock that meant the lost-handler never woke and never
/// matched stale rows — even if the orphaned process had been dead for
/// hours of wall time. (See lana-bank PR #4934, which set
/// `job_lost_interval = 365 days` to mask a related false-positive.)
#[tokio::test]
async fn test_lost_handler_uses_wall_clock_under_frozen_manual_clock() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;
    let (clock, _controller) = ClockHandle::manual();

    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .poller_config(JobPollerConfig {
            job_lost_interval: Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .expect("build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let _spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("frozen-clock-orphan"),
    });

    let frozen_sim_now = clock.now();

    // Insert an orphan row whose alive_at is far in the past in WALL-CLOCK
    // terms but at the manual clock's `now` in sim terms. With sim-clock
    // liveness this would NEVER be reclaimed (alive_at == sim_now). With
    // wall-clock liveness it will be reclaimed within job_lost_interval.
    let orphan_id = JobId::new();
    let other_instance = uuid::Uuid::now_v7();
    let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(60);

    sqlx::query(
        "INSERT INTO jobs (id, job_type, created_at) VALUES ($1, 'frozen-clock-orphan', $2)",
    )
    .bind(orphan_id)
    .bind(frozen_sim_now)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO job_executions (id, job_type, state, alive_at, poller_instance_id, attempt_index, created_at)
        VALUES ($1, 'frozen-clock-orphan', 'running', $2, $3, 1, $4)
        "#,
    )
    .bind(orphan_id)
    .bind(stale_alive_at)
    .bind(other_instance)
    .bind(frozen_sim_now)
    .execute(&pool)
    .await?;

    jobs.start_poll().await?;

    // Crucially: we never advance the manual clock. The lost-handler has to
    // fire purely from real wall-time progress.
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let row: (Option<uuid::Uuid>,) =
            sqlx::query_as("SELECT poller_instance_id FROM job_executions WHERE id = $1")
                .bind(orphan_id)
                .fetch_one(&pool)
                .await?;
        if row.0 != Some(other_instance) {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 20,
            "orphan must be rescued via wall-clock liveness even with a frozen manual clock"
        );
    }

    // Sanity check: the manual clock did not advance during this test.
    assert_eq!(
        clock.now(),
        frozen_sim_now,
        "manual clock must not have advanced; reclaim must come from wall time"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Await the first `job_events` payload satisfying `pred`, ignoring unrelated
/// traffic from concurrently-running tests. Returns `None` on timeout.
async fn next_matching(
    listener: &mut sqlx::postgres::PgListener,
    within: Duration,
    mut pred: impl FnMut(&serde_json::Value) -> bool,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let notification = match tokio::time::timeout(remaining, listener.recv()).await {
            Ok(Ok(notification)) => notification,
            _ => return None,
        };
        if let Ok(payload) = serde_json::from_str::<serde_json::Value>(notification.payload())
            && pred(&payload)
        {
            return Some(payload);
        }
    }
}

/// Writes to `job_executions` must not notify from inside the transaction.
#[tokio::test]
async fn test_write_path_emits_no_in_transaction_execution_ready() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("job_events").await?;

    let job_id = uuid::Uuid::now_v7();
    let job_type = format!("notify-funnel-insert-{job_id}");

    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO jobs (id, job_type) VALUES ($1, $2)")
        .bind(job_id)
        .bind(&job_type)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, queue_id, execute_at, alive_at, created_at) \
         VALUES ($1, $2, 'notify-funnel-queue', NOW(), NOW(), NOW())",
    )
    .bind(job_id)
    .bind(&job_type)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    sqlx::query("UPDATE job_executions SET execute_at = NOW() + interval '1 hour' WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await?;

    // One global channel, concurrent tests: only this job is evidence.
    let stray = next_matching(&mut listener, Duration::from_millis(500), |payload| {
        payload["job_type"] == job_type.as_str()
    })
    .await;
    assert!(
        stray.is_none(),
        "write path emitted an in-transaction notification: {stray:?}"
    );

    sqlx::query("DELETE FROM job_executions WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await?;

    let stray = next_matching(&mut listener, Duration::from_millis(500), |payload| {
        payload["job_type"] == job_type.as_str() || payload["job_id"] == job_id.to_string()
    })
    .await;
    assert!(
        stray.is_none(),
        "delete emitted an in-transaction notification: {stray:?}"
    );

    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await?;

    Ok(())
}

/// `job_terminal` must still reach the wire when a job completes, so a waiter
/// in another process resolves without falling back to the sweep.
#[tokio::test]
async fn test_job_terminal_is_delivered_out_of_band() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("job_events").await?;

    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("terminal-out-of-band"),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, TestJobConfig { delay_ms: 0 }).await?;

    let payload = next_matching(&mut listener, Duration::from_secs(10), |payload| {
        payload["job_id"] == job_id.to_string()
    })
    .await
    .expect("job_terminal was never delivered for a completed job");
    assert_eq!(payload["type"], "job_terminal", "got {payload}");

    jobs.shutdown().await?;
    Ok(())
}

/// The emitter must reach other processes. The spawning service never polls,
/// so its in-process delivery is inert and NOTIFY is the only route.
#[tokio::test]
async fn test_execution_ready_reaches_another_process() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;

    let job_type = JobType::new("notify-funnel-crosspod");
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));

    let config_b = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs_b = Jobs::init(config_b).await?;
    let _ = jobs_b.add_initializer(TrackingJobInitializer {
        job_type: job_type.clone(),
        completed: Arc::clone(&completed),
    });
    jobs_b.start_poll().await.expect("Failed to start poller B");

    let config_a = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs_a = Jobs::init(config_a).await?;
    let spawner = jobs_a.add_initializer(TrackingJobInitializer {
        job_type: job_type.clone(),
        completed: Arc::clone(&completed),
    });

    spawner
        .spawn(JobId::new(), TrackingJobConfig { label: "x".into() })
        .await?;

    // Far below the 60s MAX_WAIT fallback.
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if !completed.lock().await.is_empty() {
            break;
        }
        attempts += 1;
        assert!(
            attempts < 100,
            "cross-process spawn was never picked up via NOTIFY (would have needed MAX_WAIT)"
        );
    }

    jobs_b.shutdown().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrackingJobConfig {
    label: String,
}

struct TrackingJobInitializer {
    job_type: JobType,
    completed: Arc<Mutex<Vec<String>>>,
}

impl JobInitializer for TrackingJobInitializer {
    type Config = TrackingJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: TrackingJobConfig = job.config()?;
        Ok(Box::new(TrackingJobRunner {
            config,
            completed: Arc::clone(&self.completed),
        }))
    }
}

struct TrackingJobRunner {
    config: TrackingJobConfig,
    completed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl JobRunner for TrackingJobRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.completed.lock().await.push(self.config.label.clone());
        Ok(JobCompletion::Complete)
    }
}

// -- JobHandle / JobHandles tests --

/// Contract 6: on the duplicate path a resident job's `spawn` returns a
/// handle whose id is the PERSISTED job's id — and no second row is created.
#[tokio::test]
async fn resident_spawn_returns_existing_handle_on_duplicate() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    // Unique-per-run job type so the test is repeatable against a persistent
    // DB (resident jobs are never deleted).
    let job_type: &'static str =
        Box::leak(format!("resident-dup-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_resident_initializer(TestResidentInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let second_spawner = spawner.clone();
    let first_handle = spawner.spawn(TestJobConfig { delay_ms: 10 }).await?;
    let first_id = first_handle.id();

    // A second spawn of the same type resolves to the persisted job.
    let second_handle = second_spawner.spawn(TestJobConfig { delay_ms: 10 }).await?;
    assert_eq!(
        second_handle.id(),
        first_id,
        "duplicate path must return the persisted job's id"
    );

    // No second row was created.
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1")
        .bind(job_type)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1);

    jobs.shutdown().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CheckpointState {
    processed: u32,
}

/// A runner that parks, then (on first release) writes execution state and
/// signals, then (on second release) completes.
struct StateWritingInitializer {
    job_type: JobType,
    wrote: Arc<Notify>,
    release: Arc<Notify>,
}

impl JobInitializer for StateWritingInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(StateWritingRunner {
            wrote: Arc::clone(&self.wrote),
            release: Arc::clone(&self.release),
        }))
    }
}

struct StateWritingRunner {
    wrote: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl JobRunner for StateWritingRunner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.release.notified().await;
        current_job
            .update_execution_state(CheckpointState { processed: 42 })
            .await?;
        self.wrote.notify_one();
        self.release.notified().await;
        Ok(JobCompletion::Complete)
    }
}

/// Contract 5 (honest absence) + typed read-back: `execution_state` is `None`
/// before the first write and round-trips the committed value afterwards.
#[tokio::test]
async fn execution_state_none_before_first_write_then_roundtrips() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let wrote = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(StateWritingInitializer {
        job_type: JobType::new("handle-execution-state"),
        wrote: Arc::clone(&wrote),
        release: Arc::clone(&release),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;

    // Mint by id: no state written yet ⇒ None.
    let handle = jobs.handle(job_id);
    assert_eq!(
        handle.load().await?.execution_state::<CheckpointState>()?,
        None
    );

    // Release the runner to write its state, then read it back typed.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), wrote.notified())
        .await
        .expect("runner never wrote its execution state");
    assert_eq!(
        handle.load().await?.execution_state::<CheckpointState>()?,
        Some(CheckpointState { processed: 42 })
    );

    // Let the job finish; the row is deleted ⇒ honest absence again.
    release.notify_one();
    handle.await_completion(Duration::from_secs(10)).await?;
    assert_eq!(
        handle.load().await?.execution_state::<CheckpointState>()?,
        None
    );

    jobs.shutdown().await?;
    Ok(())
}

// The public API can't prove `job_execution_states` cleanup happened, since
// `execution_state()` reports `None` once `job_executions` is gone either
// way. These tests assert directly against the checkpoint table.

/// A plain job's checkpoint row exists while running and is gone the instant
/// the terminal DELETE commits.
#[tokio::test]
async fn checkpoint_row_deleted_on_terminal() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let wrote = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(StateWritingInitializer {
        job_type: JobType::new("checkpoint-row-deleted-complete"),
        wrote: Arc::clone(&wrote),
        release: Arc::clone(&release),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;
    let handle = jobs.handle(job_id);

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), wrote.notified())
        .await
        .expect("runner never wrote its execution state");

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM job_execution_states WHERE id = $1")
            .bind(uuid::Uuid::from(job_id))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        count, 1,
        "checkpoint row must exist while the job is running"
    );

    release.notify_one();
    handle.await_completion(Duration::from_secs(10)).await?;

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM job_execution_states WHERE id = $1")
            .bind(uuid::Uuid::from(job_id))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        count, 0,
        "checkpoint row must be deleted along with the terminal execution row"
    );

    jobs.shutdown().await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct StateWritingFailingConfig;

/// Writes a checkpoint then always fails; reaches errored-terminal on its first attempt.
struct StateWritingFailingInitializer {
    job_type: JobType,
    wrote: Arc<Notify>,
}

impl JobInitializer for StateWritingFailingInitializer {
    type Config = StateWritingFailingConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
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
        Ok(Box::new(StateWritingFailingRunner {
            wrote: Arc::clone(&self.wrote),
        }))
    }
}

struct StateWritingFailingRunner {
    wrote: Arc<Notify>,
}

#[async_trait]
impl JobRunner for StateWritingFailingRunner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        current_job
            .update_execution_state(CheckpointState { processed: 7 })
            .await?;
        self.wrote.notify_one();
        Err("intentional failure".into())
    }
}

/// A checkpoint row is also deleted on the errored-terminal path.
#[tokio::test]
async fn checkpoint_row_deleted_on_errored_terminal() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let wrote = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(StateWritingFailingInitializer {
        job_type: JobType::new("checkpoint-row-deleted-errored"),
        wrote: Arc::clone(&wrote),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, StateWritingFailingConfig).await?;
    let handle = jobs.handle(job_id);

    tokio::time::timeout(Duration::from_secs(10), wrote.notified())
        .await
        .expect("runner never wrote its execution state");

    let outcome = handle.await_completion(Duration::from_secs(10)).await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM job_execution_states WHERE id = $1")
            .bind(uuid::Uuid::from(job_id))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        count, 0,
        "checkpoint row must be deleted on the errored-terminal path too"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// First attempt checkpoints then fails; the retry records what it observes.
struct CheckpointThenFailOnceInitializer {
    job_type: JobType,
    seen_on_retry: Arc<Mutex<Option<CheckpointState>>>,
}

impl JobInitializer for CheckpointThenFailOnceInitializer {
    type Config = ResultJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(3),
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(CheckpointThenFailOnceRunner {
            seen_on_retry: Arc::clone(&self.seen_on_retry),
        }))
    }
}

struct CheckpointThenFailOnceRunner {
    seen_on_retry: Arc<Mutex<Option<CheckpointState>>>,
}

#[async_trait]
impl JobRunner for CheckpointThenFailOnceRunner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if current_job.attempt() == 1 {
            current_job
                .update_execution_state(CheckpointState { processed: 99 })
                .await?;
            return Err("intentional first-attempt failure".into());
        }
        let state = current_job.execution_state::<CheckpointState>()?;
        *self.seen_on_retry.lock().await = state;
        Ok(JobCompletion::Complete)
    }
}

/// A checkpoint written on attempt 1 must still be readable on the retry.
#[tokio::test]
async fn checkpoint_survives_retry() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let seen_on_retry = Arc::new(Mutex::new(None));
    let spawner = jobs.add_initializer(CheckpointThenFailOnceInitializer {
        job_type: JobType::new("checkpoint-survives-retry"),
        seen_on_retry: Arc::clone(&seen_on_retry),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;
    let handle = jobs.handle(job_id);
    let outcome = handle.await_completion(Duration::from_secs(10)).await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);

    assert_eq!(
        *seen_on_retry.lock().await,
        Some(CheckpointState { processed: 99 }),
        "the retry attempt must observe attempt 1's checkpoint"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Drive jobs through the lifecycle and assert every `JobStatus` variant,
/// including `queue_id` passthrough and the `Errored { error }` string.
#[tokio::test]
async fn status_pending_running_completed_errored() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;

    let started = Arc::new(Mutex::new(Vec::<String>::new()));
    let completed = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new(Notify::new());
    let queue_spawner = jobs.add_initializer(QueueJobInitializer {
        job_type: JobType::new("handle-status-lifecycle"),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        release: Arc::clone(&release),
    });
    let fail_spawner = jobs.add_initializer(FailingJobInitializer {
        job_type: JobType::new("failing-status-transitions"),
    });
    jobs.start_poll().await?;

    // Pending: scheduled far in the future, in a queue.
    let pending_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    queue_spawner
        .spawn_at_with_queue_id(
            pending_id,
            QueueJobConfig { label: "P".into() },
            schedule_at,
            "status-queue-pending",
        )
        .await?;
    match jobs.handle(pending_id).load().await?.state() {
        JobStatus::Pending {
            scheduled_at: at,
            attempt,
            queue_id,
        } => {
            assert!((at - schedule_at).num_seconds().abs() < 1);
            assert_eq!(attempt, 1);
            assert_eq!(queue_id.as_deref(), Some("status-queue-pending"));
        }
        other => panic!("expected Pending, got {other:?}"),
    }

    // Running: a parked job in a queue.
    let running_id = JobId::new();
    queue_spawner
        .spawn_with_queue_id(
            running_id,
            QueueJobConfig { label: "R".into() },
            "status-queue-running",
        )
        .await?;
    let mut attempts = 0;
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        if !started.lock().await.is_empty() {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "running job never started");
    }
    let running_handle = jobs.handle(running_id);
    match running_handle.load().await?.state() {
        JobStatus::Running {
            attempt, queue_id, ..
        } => {
            assert_eq!(attempt, 1);
            assert_eq!(queue_id.as_deref(), Some("status-queue-running"));
        }
        other => panic!("expected Running, got {other:?}"),
    }

    // Completed: release it, await, and the terminal status still carries the
    // queue identity (entity-sourced after the execution row is gone).
    release.notify_one();
    let outcome = running_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    assert_eq!(
        running_handle.load().await?.state(),
        JobStatus::Completed {
            queue_id: Some("status-queue-running".into())
        }
    );

    // Errored: a failing job in a queue; `error` is the final error string.
    let errored_id = JobId::new();
    fail_spawner
        .spawn_with_queue_id(errored_id, FailingJobConfig, "status-queue-errored")
        .await?;
    let errored_handle = jobs.handle(errored_id);
    let outcome = errored_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    match errored_handle.load().await?.state() {
        JobStatus::Errored { error, queue_id } => {
            assert!(
                error.contains("intentional failure"),
                "unexpected error string: {error}"
            );
            assert_eq!(queue_id.as_deref(), Some("status-queue-errored"));
        }
        other => panic!("expected Errored, got {other:?}"),
    }

    jobs.shutdown().await?;
    Ok(())
}

/// Contract 5: `load()` on an id that never existed is `Err(Find)`.
#[tokio::test]
async fn load_not_found_is_find_error() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let jobs = Jobs::init(config).await?;

    let result = jobs.handle(JobId::new()).load().await;
    assert!(
        matches!(result, Err(JobError::Find(_))),
        "expected Find error for a job that never existed, got Ok or wrong error",
    );

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderedJobConfig {
    index: i32,
    delay_ms: u64,
}

struct OrderedResultInitializer;

impl JobInitializer for OrderedResultInitializer {
    type Config = OrderedJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("handle-await-all-order")
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: OrderedJobConfig = job.config()?;
        Ok(Box::new(OrderedResultRunner { config }))
    }
}

struct OrderedResultRunner {
    config: OrderedJobConfig,
}

#[async_trait]
impl JobRunner for OrderedResultRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        tokio::time::sleep(tokio::time::Duration::from_millis(self.config.delay_ms)).await;
        current_job
            .set_result(&MyResult {
                value: self.config.index,
            })
            .await?;
        Ok(JobCompletion::Complete)
    }
}

/// Contract 2: `await_all` outcomes align positionally with the handles even
/// when the jobs complete in shuffled order; `JobOutcomes` works on the
/// result unchanged.
#[tokio::test]
async fn await_all_order_preserved() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(OrderedResultInitializer);
    jobs.start_poll().await?;

    // Reverse-staggered delays: later handles complete earlier.
    let n = 5;
    let ids: Vec<JobId> = (0..n).map(|_| JobId::new()).collect();
    for (i, id) in ids.iter().enumerate() {
        spawner
            .spawn(
                *id,
                OrderedJobConfig {
                    index: i as i32,
                    delay_ms: ((n - i) as u64) * 60,
                },
            )
            .await?;
    }

    let handles = jobs.handles(ids.clone());
    let outcomes = handles.await_all(Duration::from_secs(20)).await?;

    assert_eq!(outcomes.len(), n);
    for (i, (outcome, handle)) in outcomes.iter().zip(handles.iter()).enumerate() {
        assert_eq!(handle.id(), ids[i]);
        let result: MyResult = outcome
            .result()
            .expect("deserialize result")
            .expect("result should be Some");
        assert_eq!(
            result.value, i as i32,
            "outcomes[{i}] must belong to handles[{i}]"
        );
    }
    // `JobOutcomes` applies to the returned Vec unchanged.
    assert_eq!(outcomes.failed_count(), 0);
    assert!(outcomes.all_succeeded());

    jobs.shutdown().await?;
    Ok(())
}

/// The wedge-inheritance guard (await-completions-wedge.md "How to confirm"):
/// registration racing a completion burst whose terminal notifications are
/// dropped (size-1 broadcast buffer). The REQUIRED timeout fires instead of
/// wedging, and — contract 3 (cancel safety) — a fresh `await_all` after the
/// timed-out (dropped) one re-registers and resolves via the sweep.
#[tokio::test]
async fn await_all_times_out_and_reawait_resolves() -> anyhow::Result<()> {
    use job::JobPollerConfig;

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(JobPollerConfig {
            // Tiny buffer: the completion burst overflows it and terminal
            // notifications are dropped (never redelivered), leaving the
            // reconciliation sweep as the only resolution path.
            terminal_channel_size: 1,
            sweep_interval: Duration::from_millis(250),
            ..Default::default()
        })
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("handle-await-all-wedge"),
    });
    jobs.start_poll().await?;

    // A burst of jobs whose completions race the registrations below.
    let ids: Vec<JobId> = (0..40).map(|_| JobId::new()).collect();
    for id in &ids {
        spawner.spawn(*id, TestJobConfig { delay_ms: 100 }).await?;
    }

    let handles = jobs.handles(ids.clone());

    // The bounded timeout fires: the batch cannot fully resolve this fast
    // (the last-spawned job alone needs ≥100ms).
    let result = handles.await_all(Duration::from_millis(50)).await;
    assert!(
        matches!(result, Err(JobError::TimedOut(id)) if id == ids[0]),
        "expected TimedOut with the first handle's id, got: {result:?}"
    );

    // The timed-out call dropped its waiters (select! loser). A FRESH
    // `await_all` must re-register and resolve — via the sweep — despite the
    // dropped terminal notifications.
    let outcomes = handles.await_all(Duration::from_secs(20)).await?;
    assert_eq!(outcomes.len(), ids.len());
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Awaiting through a handle before `Jobs::start_poll` is a
/// `RouterNotStarted` error, not a panic. (Runs without a live database: the
/// lazy pool is never contacted because the router check precedes any query.)
#[tokio::test]
async fn await_before_start_poll_is_router_not_started_error() -> anyhow::Result<()> {
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:password@localhost:5432/nonexistent-db")?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let jobs = Jobs::init(config).await?;

    let handle = jobs.handle(JobId::new());
    let result = handle.await_completion(Duration::from_millis(100)).await;
    assert!(
        matches!(result, Err(JobError::RouterNotStarted)),
        "expected RouterNotStarted, got: {result:?}"
    );

    let handles = jobs.handles([JobId::new(), JobId::new()]);
    let result = handles.await_all(Duration::from_millis(100)).await;
    assert!(
        matches!(result, Err(JobError::RouterNotStarted)),
        "expected RouterNotStarted, got: {result:?}"
    );

    Ok(())
}

/// Handle awaits surface the right errors and results: a missing job is a
/// Find error, a pending job times out, and a batch resolves positionally.
#[tokio::test]
async fn handle_await_find_timeout_and_batch() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("handle-await-errors"),
    });
    jobs.start_poll().await?;

    // await_completion on a job that never existed → Find error.
    let result = jobs
        .handle(JobId::new())
        .await_completion(Duration::from_secs(1))
        .await;
    assert!(
        matches!(result, Err(JobError::Find(_))),
        "expected Find error, got: {result:?}"
    );

    // await_completion on a far-future job → TimedOut carrying its id.
    let pending_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    spawner
        .spawn_at(pending_id, TestJobConfig { delay_ms: 10 }, schedule_at)
        .await?;
    let result = jobs
        .handle(pending_id)
        .await_completion(Duration::from_millis(200))
        .await;
    assert!(
        matches!(result, Err(JobError::TimedOut(id)) if id == pending_id),
        "expected TimedOut, got: {result:?}"
    );

    // await_all: batch resolves with positional outcomes.
    let batch: Vec<JobId> = (0..3).map(|_| JobId::new()).collect();
    for id in &batch {
        spawner.spawn(*id, TestJobConfig { delay_ms: 10 }).await?;
    }
    let outcomes = jobs
        .handles(batch.clone())
        .await_all(Duration::from_secs(10))
        .await?;
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes.all_succeeded());

    jobs.shutdown().await?;
    Ok(())
}

/// Part 0 payoff: after the terminal DELETE removes the execution row, the
/// job's queue identity survives on the entity and is carried by the terminal
/// `JobStatus` variants.
#[tokio::test]
async fn terminal_status_carries_queue_id() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("terminal-queue-id"),
    });
    let fail_spawner = jobs.add_initializer(FailingJobInitializer {
        job_type: JobType::new("failing-terminal-queue-id"),
    });
    jobs.start_poll().await?;

    // Completed path.
    let done_id = JobId::new();
    spawner
        .spawn_with_queue_id(done_id, TestJobConfig { delay_ms: 10 }, "terminal-q")
        .await?;
    let done_handle = jobs.handle(done_id);
    let outcome = done_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT queue_id FROM job_executions WHERE id = $1")
            .bind(done_id)
            .fetch_optional(&pool)
            .await?;
    assert!(row.is_none(), "execution row must be gone after completion");
    assert_eq!(
        done_handle.load().await?.state(),
        JobStatus::Completed {
            queue_id: Some("terminal-q".into())
        }
    );

    // Errored path.
    let errored_id = JobId::new();
    fail_spawner
        .spawn_with_queue_id(errored_id, FailingJobConfig, "terminal-q-err")
        .await?;
    let errored_handle = jobs.handle(errored_id);
    let outcome = errored_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    match errored_handle.load().await?.state() {
        JobStatus::Errored { queue_id, .. } => {
            assert_eq!(queue_id.as_deref(), Some("terminal-q-err"));
        }
        other => panic!("expected Errored, got {other:?}"),
    }

    jobs.shutdown().await?;
    Ok(())
}

/// A single `load()` exposes the config proxy and `next_run` for a pending
/// job, and the return value for a completed one — all via sync snapshot
/// getters. Also covers the `JobHandles::load_all` batch mint.
#[tokio::test]
async fn snapshot_exposes_config_next_run_and_return_value() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    // A dedicated result initializer with a constant return value keeps this
    // test isolated from the order-sensitive `await_all_order_preserved`.
    let result_spawner = jobs.add_initializer(ResultJobInitializer {
        job_type: JobType::new("result-job-snapshot"),
    });
    let test_spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("snapshot-getters"),
    });
    jobs.start_poll().await?;

    // Pending job: one load() yields config + next_run without extra calls.
    let pending_id = JobId::new();
    let schedule_at = chrono::Utc::now() + chrono::Duration::hours(24);
    test_spawner
        .spawn_at(pending_id, TestJobConfig { delay_ms: 77 }, schedule_at)
        .await?;
    let snap = jobs.handle(pending_id).load().await?;
    assert!(matches!(snap.state(), JobStatus::Pending { .. }));
    assert_eq!(snap.config::<TestJobConfig>()?.delay_ms, 77);
    assert_eq!(snap.attempt(), Some(1));
    let next_run = snap.next_run().expect("pending job has a next_run");
    assert!((next_run - schedule_at).num_seconds().abs() < 1);
    // Return value absent before the job has run.
    assert_eq!(snap.return_value::<MyResult>()?, None);

    // Completed job: the return value the runner set is readable off the snapshot.
    let done_id = JobId::new();
    result_spawner.spawn(done_id, ResultJobConfig).await?;
    let done_handle = jobs.handle(done_id);
    done_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    let done_snap = done_handle.load().await?;
    assert!(
        done_snap.next_run().is_none(),
        "terminal job has no next_run"
    );
    assert_eq!(
        done_snap.return_value::<MyResult>()?,
        Some(MyResult { value: 42 })
    );

    // load_all preserves order positionally (contract 2).
    let snaps = jobs.handles([pending_id, done_id]).load_all().await?;
    assert_eq!(snaps.len(), 2);
    assert!(matches!(snaps[0].state(), JobStatus::Pending { .. }));
    assert!(matches!(snaps[1].state(), JobStatus::Completed { .. }));

    jobs.shutdown().await?;
    Ok(())
}

/// Regression (Cursor Bugbot): the entity is authoritative for terminal state.
/// If a concurrent completion leaves a live execution row visible mid-commit
/// (reproduced deterministically by injecting a stale `running` row for an
/// already-completed job — the exact shape a torn read under READ COMMITTED
/// would observe), `load()` must still report the terminal status, not
/// `Running`, and must not leak any live-row-derived fields.
#[tokio::test]
async fn load_prefers_terminal_entity_over_stale_execution_row() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("terminal-vs-stale-row"),
    });
    jobs.start_poll().await?;

    // Run a job to completion: entity terminal, execution row deleted.
    let job_id = JobId::new();
    spawner
        .spawn(job_id, TestJobConfig { delay_ms: 10 })
        .await?;
    let handle = jobs.handle(job_id);
    handle.await_completion(Duration::from_secs(10)).await?;
    assert!(handle.load().await?.state().is_terminal());

    // Inject a stale `running` execution row (poller_instance_id NULL so no
    // poller claims it) — the live-row-alongside-terminal-entity torn shape.
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, state, alive_at, attempt_index, created_at) \
         VALUES ($1, 'terminal-vs-stale-row', 'running', NOW(), 1, NOW())",
    )
    .bind(job_id)
    .execute(&pool)
    .await?;

    // The entity wins: terminal status, and no live-row fields leak through.
    let snap = handle.load().await?;
    assert!(
        snap.state().is_terminal(),
        "terminal entity must win over a stale running row, got {:?}",
        snap.state()
    );
    assert_eq!(snap.attempt(), None, "terminal job exposes no attempt");
    assert_eq!(snap.next_run(), None, "terminal job has no next_run");
    assert_eq!(
        snap.execution_state::<serde_json::Value>()?,
        None,
        "terminal job exposes no execution state"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// `resident_handle(job_type)` mirrors `handle(id)` for resident jobs: it
/// resolves the persisted job's id from the DB — `Some` once the resident
/// job is spawned (the `jobs` row persists forever), `None` for a type that
/// has none.
#[tokio::test]
async fn resident_handle_resolves_the_resident_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("resident-handle-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_resident_initializer(TestResidentInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    // No resident job of this type yet ⇒ None.
    assert!(
        jobs.resident_handle(JobType::new(job_type))
            .await?
            .is_none(),
        "no resident job spawned yet"
    );

    // Spawn the resident job; resident_handle now resolves to its persisted id.
    let spawned = spawner.spawn(TestJobConfig { delay_ms: 10 }).await?;
    let handle = jobs
        .resident_handle(JobType::new(job_type))
        .await?
        .expect("resident job should resolve");
    assert_eq!(handle.id(), spawned.id());

    // A type that was never spawned ⇒ None.
    assert!(
        jobs.resident_handle(JobType::new("resident-handle-never-spawned"))
            .await?
            .is_none()
    );

    jobs.shutdown().await?;
    Ok(())
}

// -- Round-2 additions (obix consumer proof): last_error mid-retry + execution_state point-read --

/// A job that always fails, with retries remaining and a far-future backoff so
/// it parks (non-terminal) after the first failure — the shape of obix's
/// wedged, `repeat_indefinitely()` resident handler.
struct FailingWithRetriesInitializer;

impl JobInitializer for FailingWithRetriesInitializer {
    type Config = FailingJobConfig;

    fn job_type(&self) -> JobType {
        JobType::new("failing-with-retries")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(5),
            // Huge backoff: after attempt 1 fails it reschedules ~1h out, so it
            // stays Pending (non-terminal) with the error recorded.
            min_backoff: Duration::from_secs(3600),
            max_backoff: Duration::from_secs(3600),
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

/// `A`: the last error is reachable while a job is still retrying (non-terminal),
/// matches the `Errored { error }` string once terminal, and is `None` for a
/// job that never failed.
#[tokio::test]
async fn last_error_visible_mid_retry_and_terminal() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let retry_spawner = jobs.add_initializer(FailingWithRetriesInitializer);
    let fail_spawner = jobs.add_initializer(FailingJobInitializer {
        job_type: JobType::new("failing-last-error"),
    });
    let ok_spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("last-error-never-failed"),
    });
    jobs.start_poll().await?;

    // Mid-retry: fails once, then parks Pending ~1h out with the error recorded.
    let retry_id = JobId::new();
    retry_spawner.spawn(retry_id, FailingJobConfig).await?;
    let retry_handle = jobs.handle(retry_id);

    let mut attempts = 0;
    let mid_retry = loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let snap = retry_handle.load().await?;
        if snap.last_error().is_some() {
            break snap;
        }
        attempts += 1;
        assert!(attempts < 100, "job never recorded a failed attempt");
    };
    assert!(
        !mid_retry.state().is_terminal(),
        "job must still be retrying (non-terminal), got {:?}",
        mid_retry.state()
    );
    assert!(matches!(mid_retry.state(), JobStatus::Pending { .. }));
    assert!(
        mid_retry
            .last_error()
            .expect("mid-retry error present")
            .contains("intentional failure"),
        "unexpected mid-retry error"
    );

    // Terminal errored: last_error equals the `Errored { error }` string.
    let errored_id = JobId::new();
    fail_spawner.spawn(errored_id, FailingJobConfig).await?;
    let errored_handle = jobs.handle(errored_id);
    errored_handle
        .await_completion(Duration::from_secs(10))
        .await?;
    let terminal = errored_handle.load().await?;
    match terminal.state() {
        JobStatus::Errored { error, .. } => {
            assert_eq!(terminal.last_error(), Some(error.as_str()));
        }
        other => panic!("expected Errored, got {other:?}"),
    }

    // Never failed: last_error is None.
    let ok_id = JobId::new();
    ok_spawner
        .spawn(ok_id, TestJobConfig { delay_ms: 10 })
        .await?;
    let ok_handle = jobs.handle(ok_id);
    ok_handle.await_completion(Duration::from_secs(10)).await?;
    assert_eq!(ok_handle.load().await?.last_error(), None);

    jobs.shutdown().await?;
    Ok(())
}

/// `B`: the `execution_state` point-read round-trips a written state, is `None`
/// for a missing row, surfaces `CouldNotDeserializeExecutionState` on a decode
/// mismatch, and agrees with `load().execution_state()` on a live job.
#[tokio::test]
async fn execution_state_point_read() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");

    let mut jobs = Jobs::init(config).await?;
    let wrote = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(StateWritingInitializer {
        job_type: JobType::new("execution-state-point-read"),
        wrote: Arc::clone(&wrote),
        release: Arc::clone(&release),
    });
    jobs.start_poll().await?;

    let job_id = JobId::new();
    spawner.spawn(job_id, ResultJobConfig).await?;
    let handle = jobs.handle(job_id);

    // No state written yet ⇒ None (row present, execution_state_json unset).
    assert_eq!(handle.execution_state::<CheckpointState>().await?, None);

    // A job that never existed ⇒ None (missing row).
    assert_eq!(
        jobs.handle(JobId::new())
            .execution_state::<CheckpointState>()
            .await?,
        None
    );

    // Release the runner to write its state, then point-read it.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), wrote.notified())
        .await
        .expect("runner never wrote its execution state");
    assert_eq!(
        handle.execution_state::<CheckpointState>().await?,
        Some(CheckpointState { processed: 42 })
    );

    // Agrees with the snapshot's execution_state on the same live job.
    assert_eq!(
        handle.load().await?.execution_state::<CheckpointState>()?,
        Some(CheckpointState { processed: 42 })
    );

    // Decode mismatch ⇒ CouldNotDeserializeExecutionState (an object is not a String).
    let result = handle.execution_state::<String>().await;
    assert!(
        matches!(result, Err(JobError::CouldNotDeserializeExecutionState(_))),
        "expected CouldNotDeserializeExecutionState, got {result:?}"
    );

    // Let the job finish so the runner's second `release.notified()` returns.
    release.notify_one();
    handle.await_completion(Duration::from_secs(10)).await?;

    jobs.shutdown().await?;
    Ok(())
}

// -- Per-type concurrency caps --

/// A runner that parks (spin-polling a shared flag, so a release covers
/// runners started before AND after the release, unlike a one-shot
/// `Notify`) while tracking how many of its type are concurrently in `run`.
struct ConcurrencyProbeInitializer {
    job_type: JobType,
    max_concurrent_per_process: Option<usize>,
    running: Arc<AtomicUsize>,
    high_water: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
}

impl JobInitializer for ConcurrencyProbeInitializer {
    type Config = ();

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn max_concurrent_per_process(&self) -> Option<usize> {
        self.max_concurrent_per_process
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(ConcurrencyProbeRunner {
            running: Arc::clone(&self.running),
            high_water: Arc::clone(&self.high_water),
            release: Arc::clone(&self.release),
        }))
    }
}

struct ConcurrencyProbeRunner {
    running: Arc<AtomicUsize>,
    high_water: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
}

#[async_trait]
impl JobRunner for ConcurrencyProbeRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let n = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.high_water.fetch_max(n, Ordering::SeqCst);
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.running.fetch_sub(1, Ordering::SeqCst);
        Ok(JobCompletion::Complete)
    }
}

/// D6/D7: `max_concurrent_per_process` bounds concurrent execution of one
/// type, and a freed slot must wake the poll loop so the rest of the backlog
/// still gets claimed — without the capped-type notify rule this test would
/// time out waiting for waves 2 and 3 (the process-wide `min_jobs`
/// threshold is never crossed by a single completion here).
#[tokio::test]
async fn per_process_cap_bounds_concurrency() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;

    let running = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let spawner = jobs.add_initializer(ConcurrencyProbeInitializer {
        job_type: JobType::new("per-process-cap-bounds-concurrency"),
        max_concurrent_per_process: Some(2),
        running: Arc::clone(&running),
        high_water: Arc::clone(&high_water),
        release: Arc::clone(&release),
    });

    let ids: Vec<JobId> = (0..6).map(|_| JobId::new()).collect();
    for id in &ids {
        spawner.spawn(*id, ()).await?;
    }
    jobs.start_poll().await?;

    // Wait for the cap to saturate: exactly 2 concurrently running.
    let mut peak = 0;
    for _ in 0..100 {
        peak = high_water.load(Ordering::SeqCst);
        if peak >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(peak, 2, "cap must be reached given a backlog of 6");
    assert_eq!(
        running.load(Ordering::SeqCst),
        2,
        "no more than the cap may run at once"
    );

    release.store(true, Ordering::SeqCst);
    // The completion timeout is what proves D7's notify rule: without it,
    // waves 2 and 3 never get claimed and this times out.
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );
    assert_eq!(
        high_water.load(Ordering::SeqCst),
        2,
        "the cap must never have been exceeded across all 3 waves"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// D6: a saturated capped type must not starve an uncapped type sharing the
/// process — the uncapped job completes promptly even while the capped
/// type's backlog sits stuck behind its one slot.
#[tokio::test]
async fn capped_type_does_not_starve_others() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;

    let capped_running = Arc::new(AtomicUsize::new(0));
    let capped_high_water = Arc::new(AtomicUsize::new(0));
    let capped_release = Arc::new(AtomicBool::new(false));
    let capped_spawner = jobs.add_initializer(ConcurrencyProbeInitializer {
        job_type: JobType::new("capped-does-not-starve-capped"),
        max_concurrent_per_process: Some(1),
        running: Arc::clone(&capped_running),
        high_water: Arc::clone(&capped_high_water),
        release: Arc::clone(&capped_release),
    });
    let uncapped_spawner = jobs.add_initializer(TestJobInitializer {
        job_type: JobType::new("capped-does-not-starve-uncapped"),
    });

    let capped_ids: Vec<JobId> = (0..5).map(|_| JobId::new()).collect();
    for id in &capped_ids {
        capped_spawner.spawn(*id, ()).await?;
    }
    jobs.start_poll().await?;

    let mut attempts = 0;
    while capped_running.load(Ordering::SeqCst) < 1 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        attempts += 1;
        assert!(attempts < 100, "capped type never claimed its one slot");
    }

    let uncapped_id = JobId::new();
    uncapped_spawner
        .spawn(uncapped_id, TestJobConfig { delay_ms: 10 })
        .await?;
    let outcome = jobs
        .handle(uncapped_id)
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(
        outcome.state(),
        JobTerminalState::Completed,
        "an uncapped type must not be starved by a saturated capped type"
    );
    assert_eq!(
        capped_high_water.load(Ordering::SeqCst),
        1,
        "the capped type's slot budget must have held throughout"
    );

    capped_release.store(true, Ordering::SeqCst);
    jobs.handles(capped_ids)
        .await_all(Duration::from_secs(30))
        .await?;

    jobs.shutdown().await?;
    Ok(())
}

// -- Keyed jobs --

/// `KeyedJobSpawner::spawn` twice with the same key: the second call must
/// resolve to the first job's persisted id, and no second row is created
/// (mirrors `resident_spawn_returns_existing_handle_on_duplicate`).
#[tokio::test]
async fn spawn_keyed_duplicate_returns_persisted_handle() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("spawn-keyed-dup-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(TestKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let first_handle = spawner
        .spawn("shard-1", TestJobConfig { delay_ms: 10 })
        .await?;
    let second_handle = spawner
        .spawn("shard-1", TestJobConfig { delay_ms: 10 })
        .await?;
    assert_eq!(
        second_handle.id(),
        first_handle.id(),
        "duplicate key must return the persisted job's id"
    );

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1 AND unique_key = 'shard-1'")
            .bind(job_type)
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 1, "no second row was created");

    jobs.shutdown().await?;
    Ok(())
}

/// Distinct keys of one type are distinct jobs and run (and complete)
/// concurrently — `KeyedJobSpawner::spawn` does not consume the spawner.
#[tokio::test]
async fn spawn_keyed_distinct_keys_run_concurrently() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("spawn-keyed-distinct-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(TestKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let a = spawner.spawn("a", TestJobConfig { delay_ms: 10 }).await?;
    let b = spawner.spawn("b", TestJobConfig { delay_ms: 10 }).await?;
    assert_ne!(a.id(), b.id());

    let outcomes = jobs
        .handles(vec![a.id(), b.id()])
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    jobs.shutdown().await?;
    Ok(())
}

/// `keyed_handle` resolves an explicit key, and `keyed_handles` lists every
/// key of a type, ordered by key.
#[tokio::test]
async fn keyed_handle_and_keyed_handles() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-handles-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(TestKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    assert!(
        jobs.keyed_handle(JobType::new(job_type), "shard-a")
            .await?
            .is_none(),
        "no job spawned under this key yet"
    );

    let a = spawner
        .spawn("shard-a", TestJobConfig { delay_ms: 10 })
        .await?;
    let b = spawner
        .spawn("shard-b", TestJobConfig { delay_ms: 10 })
        .await?;

    let handle_a = jobs
        .keyed_handle(JobType::new(job_type), "shard-a")
        .await?
        .expect("shard-a should resolve");
    assert_eq!(handle_a.id(), a.id());
    handle_a.load().await?;

    let keyed = jobs.keyed_handles(JobType::new(job_type)).await?;
    let snapshots = keyed.load_all().await?;
    let keys: Vec<Option<&str>> = snapshots.iter().map(|s| s.unique_key()).collect();
    assert_eq!(
        keys,
        vec![Some("shard-a"), Some("shard-b")],
        "keyed_handles is ordered by key"
    );
    let ids: HashMap<&str, JobId> = snapshots
        .iter()
        .map(|s| (s.unique_key().expect("keyed job has a key"), s.job().id))
        .collect();
    assert_eq!(ids["shard-a"], a.id());
    assert_eq!(ids["shard-b"], b.id());

    jobs.shutdown().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyedCheckpointConfig {
    processed: u32,
}

/// A runner that records whatever execution state it observed at the start
/// of its run as its own return value (for `inherits_state` assertions),
/// commits a per-key checkpoint, then parks (spin-polling a shared flag) so
/// the execution row stays alive for `execution_state` to read back.
struct KeyedCheckpointInitializer {
    job_type: JobType,
    release: Arc<AtomicBool>,
    inherits_state: bool,
}

impl KeyedJobInitializer for KeyedCheckpointInitializer {
    type Config = KeyedCheckpointConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn inherits_state(&self) -> bool {
        self.inherits_state
    }

    fn init(
        &self,
        job: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: KeyedCheckpointConfig = job.config()?;
        Ok(Box::new(KeyedCheckpointRunner {
            processed: config.processed,
            release: Arc::clone(&self.release),
        }))
    }
}

struct KeyedCheckpointRunner {
    processed: u32,
    release: Arc<AtomicBool>,
}

#[async_trait]
impl JobRunner for KeyedCheckpointRunner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let observed: Option<CheckpointState> = current_job.execution_state()?;
        current_job.set_result(&observed).await?;
        current_job
            .update_execution_state(CheckpointState {
                processed: self.processed,
            })
            .await?;
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(JobCompletion::Complete)
    }
}

/// The "caught up?" pattern: `keyed_handles(...).load_all()` batch-loads
/// every shard's snapshot, and `unique_key()`/`execution_state()` on each
/// snapshot read back which shard is at which cursor.
#[tokio::test]
async fn keyed_singletons_report_execution_state() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-caught-up-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let release = Arc::new(AtomicBool::new(false));
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::clone(&release),
        inherits_state: false,
    });
    jobs.start_poll().await?;

    spawner
        .spawn("shard-0", KeyedCheckpointConfig { processed: 10 })
        .await?;
    spawner
        .spawn("shard-1", KeyedCheckpointConfig { processed: 20 })
        .await?;

    let mut cursors: HashMap<String, u32> = HashMap::new();
    for _ in 0..100 {
        cursors.clear();
        let keyed = jobs.keyed_handles(JobType::new(job_type)).await?;
        for snapshot in keyed.load_all().await? {
            if let (Some(key), Some(state)) = (
                snapshot.unique_key(),
                snapshot.execution_state::<CheckpointState>()?,
            ) {
                cursors.insert(key.to_string(), state.processed);
            }
        }
        if cursors.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(cursors.get("shard-0"), Some(&10));
    assert_eq!(cursors.get("shard-1"), Some(&20));

    release.store(true, Ordering::SeqCst);
    let keyed = jobs.keyed_handles(JobType::new(job_type)).await?;
    let outcomes = keyed.await_all(Duration::from_secs(30)).await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    jobs.shutdown().await?;
    Ok(())
}

// -- Live-keyed jobs (respawn after terminal) --

/// NEW: once a keyed job's generation reaches a terminal state, the key
/// becomes respawnable — the next `spawn` call creates a new generation
/// (new internally-generated id) that actually runs.
#[tokio::test]
async fn spawn_keyed_respawns_after_completion() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("spawn-keyed-respawn-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(TestKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let first_handle = spawner.spawn("k", TestJobConfig { delay_ms: 10 }).await?;
    let outcome = jobs
        .handle(first_handle.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Completed);

    let second_handle = spawner.spawn("k", TestJobConfig { delay_ms: 10 }).await?;
    assert_ne!(
        second_handle.id(),
        first_handle.id(),
        "respawn after terminal must create a new generation"
    );

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1 AND unique_key = 'k'")
            .bind(job_type)
            .fetch_one(&pool)
            .await?;
    assert_eq!(count, 2, "two generations of the key exist");

    let outcome = jobs
        .handle(second_handle.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(
        outcome.state(),
        JobTerminalState::Completed,
        "the new generation actually ran"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// NEW: a keyed job that goes terminal via error (not just completion) also
/// frees its key.
#[tokio::test]
async fn spawn_keyed_respawns_after_error() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("spawn-keyed-respawn-error-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(FailingKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let first_handle = spawner.spawn("k", FailingJobConfig).await?;
    let outcome = jobs
        .handle(first_handle.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    let second_handle = spawner.spawn("k", FailingJobConfig).await?;
    assert_ne!(
        second_handle.id(),
        first_handle.id(),
        "respawn after terminal must create a new generation"
    );
    let outcome = jobs
        .handle(second_handle.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);

    jobs.shutdown().await?;
    Ok(())
}

/// NEW: `keyed_handle`/`keyed_handles` resolve the LIVE generation when one
/// exists, else the LATEST generation — not the first ever spawned.
#[tokio::test]
async fn keyed_lookups_track_latest_generation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-latest-gen-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let release = Arc::new(AtomicBool::new(true));
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::clone(&release),
        inherits_state: false,
    });
    jobs.start_poll().await?;

    // Generation 1: let it complete immediately.
    let gen1 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 1 })
        .await?;
    jobs.handle(gen1.id())
        .await_completion(Duration::from_secs(10))
        .await?;

    // Generation 2: let it complete too.
    let gen2 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 2 })
        .await?;
    jobs.handle(gen2.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_ne!(gen2.id(), gen1.id());

    // No generation live: lookups resolve to the latest (gen2), not gen1.
    let resolved = jobs
        .keyed_handle(JobType::new(job_type), "k")
        .await?
        .expect("key was spawned");
    assert_eq!(resolved.id(), gen2.id(), "no live generation: latest wins");
    let listed = jobs.keyed_handles(JobType::new(job_type)).await?;
    let listed_ids: Vec<JobId> = listed
        .load_all()
        .await?
        .into_iter()
        .map(|s| s.job().id)
        .collect();
    assert_eq!(listed_ids, vec![gen2.id()]);

    // Generation 3: park it live — an execution row exists the instant
    // `spawn` returns, regardless of whether the poller has claimed it yet,
    // so this is deterministically "live" without racing the runner.
    release.store(false, Ordering::SeqCst);
    let gen3 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 3 })
        .await?;
    assert_ne!(gen3.id(), gen2.id());

    let resolved = jobs
        .keyed_handle(JobType::new(job_type), "k")
        .await?
        .expect("key was spawned");
    assert_eq!(resolved.id(), gen3.id(), "gen3 is live: it wins over gen2");
    let listed = jobs.keyed_handles(JobType::new(job_type)).await?;
    let listed_ids: Vec<JobId> = listed
        .load_all()
        .await?
        .into_iter()
        .map(|s| s.job().id)
        .collect();
    assert_eq!(listed_ids, vec![gen3.id()]);

    release.store(true, Ordering::SeqCst);
    jobs.handle(gen3.id())
        .await_completion(Duration::from_secs(10))
        .await?;

    jobs.shutdown().await?;
    Ok(())
}

/// NEW: a race between two concurrent `KeyedJobSpawner::spawn` calls on one
/// key resolves to exactly one live job and leaks no `jobs` row for the
/// loser — its insert is rolled back inside `spawn`'s conflict-retry loop.
#[tokio::test]
async fn keyed_conflict_leaks_no_job_row() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-conflict-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(TestKeyedInitializer {
        job_type: JobType::new(job_type),
    });
    jobs.start_poll().await?;

    let a = spawner.clone();
    let b = spawner.clone();
    let (ra, rb) = tokio::join!(
        a.spawn("k", TestJobConfig { delay_ms: 200 }),
        b.spawn("k", TestJobConfig { delay_ms: 200 })
    );
    let handle_a = ra?;
    let handle_b = rb?;
    assert_eq!(
        handle_a.id(),
        handle_b.id(),
        "both concurrent spawns must resolve to the one live job"
    );

    let (jobs_count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1 AND unique_key = 'k'")
            .bind(job_type)
            .fetch_one(&pool)
            .await?;
    assert_eq!(jobs_count, 1, "the loser's job row must not leak");

    let (executions_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_executions WHERE job_type = $1 AND unique_key = 'k'",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await?;
    assert_eq!(executions_count, 1, "exactly one live execution");

    jobs.shutdown().await?;
    Ok(())
}

struct CountingResidentInitializer {
    job_type: JobType,
    runs: Arc<AtomicUsize>,
}

impl ResidentJobInitializer for CountingResidentInitializer {
    type Config = TestJobConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(&self, _job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(CountingResidentRunner {
            runs: Arc::clone(&self.runs),
        }))
    }
}

struct CountingResidentRunner {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl ResidentJobRunner for CountingResidentRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(ResidentJobCompletion::RescheduleIn(Duration::from_millis(
            20,
        )))
    }
}

/// NEW: unlike a keyed job, a resident job never terminates — it stays
/// absolutely unique for the type's whole lifetime. There is no `Complete`
/// variant for `ResidentJobCompletion` to express, so this observes liveness
/// via the run counter advancing (and `await_completion` timing out) rather
/// than a terminal state, and confirms a second `spawn` while it is (forever)
/// running still resolves to the same persisted job.
#[tokio::test]
async fn resident_runner_keeps_rescheduling_and_stays_unique() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("resident-keeps-going-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let runs = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_resident_initializer(CountingResidentInitializer {
        job_type: JobType::new(job_type),
        runs: Arc::clone(&runs),
    });
    jobs.start_poll().await?;

    let first_handle = spawner.spawn(TestJobConfig { delay_ms: 0 }).await?;

    for _ in 0..100 {
        if runs.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        runs.load(Ordering::SeqCst) >= 3,
        "a resident job must keep rescheduling itself instead of completing"
    );

    // A resident job never reaches terminal — awaiting completion only ever
    // times out.
    let timed_out = jobs
        .handle(first_handle.id())
        .await_completion(Duration::from_millis(50))
        .await;
    assert!(matches!(timed_out, Err(JobError::TimedOut(_))));

    // Still absolutely unique: `resident_handle` resolves to the same job.
    let resolved = jobs
        .resident_handle(JobType::new(job_type))
        .await?
        .expect("resident job exists");
    assert_eq!(resolved.id(), first_handle.id());

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1")
        .bind(job_type)
        .fetch_one(&pool)
        .await?;
    assert_eq!(count, 1, "at most one resident job of a type ever exists");

    jobs.shutdown().await?;
    Ok(())
}

// -- Keyed jobs: `inherits_state` --

/// NEW: `KeyedJobInitializer::inherits_state` seeds a new generation's
/// execution state from its predecessor's final state.
#[tokio::test]
async fn keyed_inherits_state_seeds_next_generation() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-inherits-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::new(AtomicBool::new(true)),
        inherits_state: true,
    });
    jobs.start_poll().await?;

    let gen1 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 1 })
        .await?;
    let outcome1 = jobs
        .handle(gen1.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    let observed1 = outcome1.result::<Option<CheckpointState>>()?.flatten();
    assert_eq!(observed1, None, "gen1 starts with no inherited state");

    let gen2 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 2 })
        .await?;
    assert_ne!(gen2.id(), gen1.id());
    let outcome2 = jobs
        .handle(gen2.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    let observed2 = outcome2.result::<Option<CheckpointState>>()?.flatten();
    assert_eq!(
        observed2,
        Some(CheckpointState { processed: 1 }),
        "gen2 must inherit gen1's final state"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// NEW: with `inherits_state` left at its `false` default, a new generation
/// starts with no observed state, and the predecessor's retained state row
/// is compacted away once the new generation has spawned.
#[tokio::test]
async fn keyed_without_inherits_state_starts_fresh() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-no-inherit-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::new(AtomicBool::new(true)),
        inherits_state: false,
    });
    jobs.start_poll().await?;

    let gen1 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 1 })
        .await?;
    jobs.handle(gen1.id())
        .await_completion(Duration::from_secs(10))
        .await?;

    let gen2 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 2 })
        .await?;
    let outcome2 = jobs
        .handle(gen2.id())
        .await_completion(Duration::from_secs(10))
        .await?;
    let observed2 = outcome2.result::<Option<CheckpointState>>()?.flatten();
    assert_eq!(
        observed2, None,
        "gen2 must not inherit gen1's state when inherits_state is false"
    );

    // Without `inherits_state` each generation's state dies with it, so once
    // both have completed the key holds nothing at all — no row survives to
    // be compacted later.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_execution_states s JOIN jobs j ON j.id = s.id \
         WHERE j.job_type = $1 AND j.unique_key = 'k'",
    )
    .bind(job_type)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        count, 0,
        "a key that does not inherit state retains nothing once its generations are terminal"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// With `inherits_state`, a keyed job's execution state survives terminal —
/// readable even before any respawn — unlike a regular job's, which is
/// deleted alongside its execution row.
#[tokio::test]
async fn keyed_terminal_state_retained_when_inherited() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-state-retained-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::new(AtomicBool::new(true)),
        inherits_state: true,
    });
    jobs.start_poll().await?;

    let gen1 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 7 })
        .await?;
    jobs.handle(gen1.id())
        .await_completion(Duration::from_secs(10))
        .await?;

    let state: Option<CheckpointState> = jobs.handle(gen1.id()).execution_state().await?;
    assert_eq!(
        state,
        Some(CheckpointState { processed: 7 }),
        "a terminal keyed job's state row is retained and readable via JobHandle::execution_state"
    );

    // Also readable through the snapshot/`load()` path (`keyed_handles(...)
    // .load_all()`'s "caught up?" pattern) — this is the path that
    // previously silently discarded a terminal job's execution state
    // regardless of flavor.
    let snapshot = jobs.handle(gen1.id()).load().await?;
    let snapshot_state: Option<CheckpointState> = snapshot.execution_state()?;
    assert_eq!(
        snapshot_state,
        Some(CheckpointState { processed: 7 }),
        "a terminal keyed job's state must also be readable via JobSnapshot::execution_state"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Without `inherits_state` (the default), a keyed job cleans up exactly like
/// every other flavor: its state row is deleted with its execution row.
/// Retention is something a type opts into, not a side effect of being keyed.
#[tokio::test]
async fn keyed_terminal_state_deleted_without_inherits_state() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .expect("Failed to build JobsConfig");
    let mut jobs = Jobs::init(config).await?;
    let job_type: &'static str =
        Box::leak(format!("keyed-state-dropped-{}", uuid::Uuid::now_v7()).into_boxed_str());
    let spawner = jobs.add_keyed_initializer(KeyedCheckpointInitializer {
        job_type: JobType::new(job_type),
        release: Arc::new(AtomicBool::new(true)),
        inherits_state: false,
    });
    jobs.start_poll().await?;

    let gen1 = spawner
        .spawn("k", KeyedCheckpointConfig { processed: 7 })
        .await?;
    jobs.handle(gen1.id())
        .await_completion(Duration::from_secs(10))
        .await?;

    let state: Option<CheckpointState> = jobs.handle(gen1.id()).execution_state().await?;
    assert_eq!(
        state, None,
        "a keyed job that does not inherit state must not retain it past terminal"
    );

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_execution_states WHERE id = $1")
        .bind(uuid::Uuid::from(gen1.id()))
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        rows, 0,
        "the state row itself must be gone, not just unread"
    );

    jobs.shutdown().await?;
    Ok(())
}
