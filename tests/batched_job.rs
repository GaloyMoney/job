//! Live-PG coverage for batched job execution.
//!
//! The contract under test is that batching changes *how* claimed rows are
//! executed and nothing else: spawning, awaiting, per-job results, retries and
//! queue exclusion all behave exactly as they do for per-job runners.

mod helpers;

use async_trait::async_trait;
use job::{
    BatchItemOutcome, BatchOutcomes, BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob,
    JobBatchCompletion, JobId, JobPollerConfig, JobSpawner, JobSpec, JobSvcConfig,
    JobTerminalState, JobType, Jobs, RetrySettings, error::JobError,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchConfig {
    label: String,
}

/// Records the size and contents of every batch a runner received.
#[derive(Default)]
struct BatchLog {
    sizes: Vec<usize>,
    labels: Vec<String>,
    /// Queue ids as observed per batch, to assert the batch invariant.
    per_batch_queue_ids: Vec<Vec<Option<String>>>,
}

impl BatchLog {
    fn total_items(&self) -> usize {
        self.sizes.iter().sum()
    }
    fn max_batch(&self) -> usize {
        self.sizes.iter().copied().max().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// A runner that completes every item and logs what it saw.
// ---------------------------------------------------------------------------

struct RecordingInitializer {
    job_type: JobType,
    log: Arc<Mutex<BatchLog>>,
    max_batch_size: usize,
}

impl BatchedJobInitializer for RecordingInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(RecordingRunner {
            log: Arc::clone(&self.log),
        }))
    }
}

struct RecordingRunner {
    log: Arc<Mutex<BatchLog>>,
}

#[async_trait]
impl BatchedJobRunner for RecordingRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        {
            let mut log = self.log.lock().await;
            log.sizes.push(current_batch.len());
            log.per_batch_queue_ids.push(
                current_batch
                    .items()
                    .iter()
                    .map(|i| i.queue_id().map(str::to_string))
                    .collect(),
            );
            for item in current_batch.items() {
                log.labels.push(item.config().label.clone());
            }
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

#[tokio::test]
async fn batched_jobs_complete_and_are_awaitable() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let log = Arc::new(Mutex::new(BatchLog::default()));
    let spawner = jobs.add_batched_initializer(RecordingInitializer {
        job_type: helpers::job_type("batched-complete-all"),
        log: Arc::clone(&log),
        max_batch_size: 25,
    });
    jobs.start_poll().await?;

    let specs: Vec<JobSpec<BatchConfig>> = (0..10)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("item-{i}"),
                },
            )
            // Namespaced per test: the poll query's "is this queue busy"
            // anti-join matches on queue_id across *every* job type, so a
            // generic name here would block an unrelated test's job of a
            // different type that happened to pick the same one.
            .queue_id(format!("batched-complete-all-q{i}"))
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    // Awaiting is unchanged: every job resolves individually even though the
    // terminal writes were made by one batched commit.
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;
    assert_eq!(outcomes.len(), 10);
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    let log = log.lock().await;
    assert_eq!(log.total_items(), 10, "every job must be executed once");
    let seen: HashSet<&String> = log.labels.iter().collect();
    assert_eq!(seen.len(), 10, "each job executed exactly once");

    // No batch may contain two jobs from the same queue.
    for batch in &log.per_batch_queue_ids {
        let queued: Vec<&String> = batch.iter().flatten().collect();
        let distinct: HashSet<&&String> = queued.iter().collect();
        assert_eq!(
            queued.len(),
            distinct.len(),
            "a batch contained two jobs sharing a queue_id: {batch:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn batching_actually_groups_jobs() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let log = Arc::new(Mutex::new(BatchLog::default()));
    let spawner = jobs.add_batched_initializer(RecordingInitializer {
        job_type: helpers::job_type("batched-grouping"),
        log: Arc::clone(&log),
        max_batch_size: 25,
    });

    // Spawn a burst *before* polling starts so the first poll claims them all
    // at once — this is exactly the "arrived together" condition batching is
    // meant to exploit.
    let specs: Vec<JobSpec<BatchConfig>> = (0..20)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("burst-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    let log = log.lock().await;
    assert_eq!(log.total_items(), 20);
    assert!(
        log.max_batch() > 1,
        "a burst of 20 simultaneously-ready jobs should have produced at least \
         one multi-job batch, got batch sizes {:?}",
        log.sizes
    );
    Ok(())
}

#[tokio::test]
async fn max_batch_size_is_respected() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let log = Arc::new(Mutex::new(BatchLog::default()));
    let spawner = jobs.add_batched_initializer(RecordingInitializer {
        job_type: helpers::job_type("batched-max-size"),
        log: Arc::clone(&log),
        max_batch_size: 3,
    });

    let specs: Vec<JobSpec<BatchConfig>> = (0..20)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("capped-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;
    jobs.handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;

    let log = log.lock().await;
    assert_eq!(log.total_items(), 20);
    assert!(
        log.max_batch() <= 3,
        "max_batch_size(3) violated, batch sizes: {:?}",
        log.sizes
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-item outcomes
// ---------------------------------------------------------------------------

/// Completes items labelled `ok-*` and fails the rest, in one transaction.
struct MixedOutcomeInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for MixedOutcomeInitializer {
    type Config = BatchConfig;

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
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(MixedOutcomeRunner))
    }
}

struct MixedOutcomeRunner;

#[async_trait]
impl BatchedJobRunner for MixedOutcomeRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let outcomes: BatchOutcomes = current_batch.outcomes_for_each(|item| {
            if item.config().label.starts_with("ok") {
                BatchItemOutcome::Complete
            } else {
                BatchItemOutcome::Fail(format!("rejected {}", item.config().label))
            }
        });
        Ok(JobBatchCompletion::WithOutcomes(outcomes))
    }
}

#[tokio::test]
async fn per_item_outcomes_are_applied_independently() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(MixedOutcomeInitializer {
        job_type: helpers::job_type("batched-mixed-outcomes"),
    });

    let ok_ids: Vec<JobId> = (0..4).map(|_| JobId::new()).collect();
    let bad_ids: Vec<JobId> = (0..4).map(|_| JobId::new()).collect();

    let mut specs: Vec<JobSpec<BatchConfig>> = ok_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            JobSpec::new(
                *id,
                BatchConfig {
                    label: format!("ok-{i}"),
                },
            )
        })
        .collect();
    specs.extend(bad_ids.iter().enumerate().map(|(i, id)| {
        JobSpec::new(
            *id,
            BatchConfig {
                label: format!("bad-{i}"),
            },
        )
    }));
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    let all: Vec<JobId> = ok_ids.iter().chain(bad_ids.iter()).copied().collect();
    let outcomes = jobs
        .handles(all.clone())
        .await_all(Duration::from_secs(30))
        .await?;

    // Positional: first four are the `ok-*` jobs, last four the `bad-*` ones.
    for (i, outcome) in outcomes.iter().enumerate() {
        let expected = if i < 4 {
            JobTerminalState::Completed
        } else {
            JobTerminalState::Errored
        };
        assert_eq!(
            outcome.state(),
            expected,
            "job at index {i} had unexpected terminal state"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Failure semantics
// ---------------------------------------------------------------------------

#[derive(Default)]
struct AttemptLog {
    /// Batch sizes seen, in order, to prove retries are dispatched alone.
    sizes: Vec<usize>,
}

/// Fails the whole batch the first time each job is seen; succeeds afterwards.
/// Records batch sizes so the retry-isolation rule can be asserted.
struct FailFirstInitializer {
    job_type: JobType,
    log: Arc<Mutex<AttemptLog>>,
}

impl BatchedJobInitializer for FailFirstInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: Some(5),
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(FailFirstRunner {
            log: Arc::clone(&self.log),
        }))
    }
}

struct FailFirstRunner {
    log: Arc<Mutex<AttemptLog>>,
}

#[async_trait]
impl BatchedJobRunner for FailFirstRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        self.log.lock().await.sizes.push(current_batch.len());
        // Every item on its first attempt => fail the batch. Retries (attempt
        // > 1) succeed, so the batch drains.
        if current_batch.items().iter().all(|i| i.attempt() == 1) {
            return Err("whole batch fails on first attempt".into());
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

#[tokio::test]
async fn batch_error_retries_every_item_and_retries_run_alone() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let log = Arc::new(Mutex::new(AttemptLog::default()));
    let spawner = jobs.add_batched_initializer(FailFirstInitializer {
        // This test asserts on the shape of EVERY batch its type produces, so
        // any row of the type left behind by an earlier run against the same
        // persistent dev database would get claimed by this run's poller and
        // show up as an extra batch (observed as `saw [1, 1, 5, 1, ...]`, the
        // two leading singletons being ids this test never spawned).
        job_type: helpers::job_type("batched-fail-first"),
        log: Arc::clone(&log),
    });

    let specs: Vec<JobSpec<BatchConfig>> = (0..5)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("retry-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    // A failed batch fails *every* member, and each is retried under the type's
    // policy — so all of them still reach Completed.
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "all items should complete after their retry"
    );

    let log = log.lock().await;
    assert!(
        log.sizes.len() >= 2,
        "expected a failing batch followed by retries, saw {:?}",
        log.sizes
    );
    // Retries are dispatched one per batch — a poison item can never take
    // another job's first attempt down with it more than once.
    assert!(
        log.sizes[1..].iter().all(|&n| n == 1),
        "retry batches must contain exactly one job, saw {:?}",
        log.sizes
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCheckpoint {
    processed: u32,
}

/// Checkpoints every item, then completes `ok-*` labels and terminally fails `bad-*` ones.
struct CheckpointBatchInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for CheckpointBatchInitializer {
    type Config = BatchConfig;

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
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(CheckpointBatchRunner))
    }
}

struct CheckpointBatchRunner;

#[async_trait]
impl BatchedJobRunner for CheckpointBatchRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        mut current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        for item in current_batch.items_mut() {
            item.update_execution_state(&BatchCheckpoint { processed: 1 })
                .await?;
        }
        let outcomes = current_batch.outcomes_for_each(|item| {
            if item.config().label.starts_with("ok") {
                BatchItemOutcome::Complete
            } else {
                BatchItemOutcome::Fail("intentional terminal failure".to_string())
            }
        });
        Ok(JobBatchCompletion::WithOutcomes(outcomes))
    }
}

/// The batch dispatcher's two delete sites (complete and terminal-fail) must
/// both clean up `job_execution_states`.
#[tokio::test]
async fn checkpoint_rows_deleted_on_batched_terminal() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(CheckpointBatchInitializer {
        job_type: helpers::job_type("batched-checkpoint-cleanup"),
    });

    let ok_id = JobId::new();
    let bad_id = JobId::new();
    let specs = vec![
        JobSpec::new(
            ok_id,
            BatchConfig {
                label: "ok".to_string(),
            },
        ),
        JobSpec::new(
            bad_id,
            BatchConfig {
                label: "bad".to_string(),
            },
        ),
    ];
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    let outcomes = jobs
        .handles(vec![ok_id, bad_id])
        .await_all(Duration::from_secs(30))
        .await?;
    assert_eq!(outcomes[0].state(), JobTerminalState::Completed);
    assert_eq!(outcomes[1].state(), JobTerminalState::Errored);

    let ids = [uuid::Uuid::from(ok_id), uuid::Uuid::from(bad_id)];
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM job_execution_states WHERE id = ANY($1)")
            .bind(&ids[..])
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        count, 0,
        "checkpoint rows for both the completed and the errored-terminal \
         batch items must be gone"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Seal validation
// ---------------------------------------------------------------------------

/// Returns outcomes for only *some* of its items — a contract violation.
struct IncompleteOutcomeInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for IncompleteOutcomeInitializer {
    type Config = BatchConfig;

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
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(IncompleteOutcomeRunner))
    }
}

struct IncompleteOutcomeRunner;

#[async_trait]
impl BatchedJobRunner for IncompleteOutcomeRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        _current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        // Deliberately empty: no outcome for any item.
        Ok(JobBatchCompletion::WithOutcomes(Vec::new()))
    }
}

#[tokio::test]
async fn undispositioned_items_fail_the_batch_rather_than_being_guessed() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(IncompleteOutcomeInitializer {
        job_type: helpers::job_type("batched-incomplete-outcomes"),
    });

    let job_id = JobId::new();
    spawner
        .spawn(
            job_id,
            BatchConfig {
                label: "orphan".into(),
            },
        )
        .await?;

    jobs.start_poll().await?;

    // n_attempts = 1, so the seal failure is terminal rather than retried
    // forever: the job surfaces as Errored instead of silently completing.
    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
    assert_eq!(outcome.state(), JobTerminalState::Errored);
    Ok(())
}

// ---------------------------------------------------------------------------
// Queue exclusion still holds for batched types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_queue_jobs_are_never_in_one_batch() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let log = Arc::new(Mutex::new(BatchLog::default()));
    let spawner = jobs.add_batched_initializer(RecordingInitializer {
        job_type: helpers::job_type("batched-queue-exclusion"),
        log: Arc::clone(&log),
        max_batch_size: 25,
    });

    // Ten jobs, all on ONE queue: the poll query may claim at most one of them
    // at a time, so every batch must have exactly one item. The queue id is
    // process-unique because `idx_job_executions_queue_active` is on
    // `queue_id` alone — a queued row left by an earlier run would block the
    // spawn no matter how unique the job type is.
    let queue_id = helpers::unique("batched-same-queue-one-queue");
    let specs: Vec<JobSpec<BatchConfig>> = (0..10)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("serial-{i}"),
                },
            )
            .queue_id(queue_id.clone())
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(60))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );

    let log = log.lock().await;
    assert_eq!(log.total_items(), 10);
    assert_eq!(
        log.max_batch(),
        1,
        "jobs sharing a queue_id must never be batched together, sizes: {:?}",
        log.sizes
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-item results and reschedule
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ItemResult {
    label: String,
}

/// Sets a distinct result per item, then completes the batch, proving results
/// stay per-job through a shared commit.
struct ResultPerItemInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for ResultPerItemInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(ResultPerItemRunner))
    }
}

struct ResultPerItemRunner;

#[async_trait]
impl BatchedJobRunner for ResultPerItemRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        for item in current_batch.items() {
            item.set_result(&ItemResult {
                label: item.config().label.clone(),
            })
            .await?;
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

#[tokio::test]
async fn results_remain_per_job_across_a_batched_commit() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(ResultPerItemInitializer {
        job_type: helpers::job_type("batched-per-item-results"),
    });

    let specs: Vec<JobSpec<BatchConfig>> = (0..6)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("res-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;

    for (i, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome.state(), JobTerminalState::Completed);
        let result: ItemResult = outcome
            .result()
            .expect("deserialize result")
            .expect("each job should carry its own result");
        assert_eq!(
            result,
            ItemResult {
                label: format!("res-{i}")
            }
        );
    }
    Ok(())
}

/// Reschedules each item once, then completes it — exercising per-item
/// `RescheduleIn` inside the shared commit.
struct RescheduleOnceInitializer {
    job_type: JobType,
    seen: Arc<Mutex<HashSet<JobId>>>,
}

impl BatchedJobInitializer for RescheduleOnceInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(RescheduleOnceRunner {
            seen: Arc::clone(&self.seen),
        }))
    }
}

struct RescheduleOnceRunner {
    seen: Arc<Mutex<HashSet<JobId>>>,
}

#[async_trait]
impl BatchedJobRunner for RescheduleOnceRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let mut seen = self.seen.lock().await;
        let outcomes: BatchOutcomes = current_batch
            .items()
            .iter()
            .map(|item| {
                if seen.insert(item.id()) {
                    (
                        item.id(),
                        BatchItemOutcome::RescheduleIn(Duration::from_millis(10)),
                    )
                } else {
                    (item.id(), BatchItemOutcome::Complete)
                }
            })
            .collect();
        Ok(JobBatchCompletion::WithOutcomes(outcomes))
    }
}

#[tokio::test]
async fn per_item_reschedule_runs_the_job_again() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(JobPollerConfig::default())
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let spawner = jobs.add_batched_initializer(RescheduleOnceInitializer {
        job_type: helpers::job_type("batched-reschedule-once"),
        seen: Arc::clone(&seen),
    });

    let specs: Vec<JobSpec<BatchConfig>> = (0..4)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("resched-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "every job should complete on its second run"
    );
    assert_eq!(seen.lock().await.len(), 4, "each job ran at least twice");
    Ok(())
}

// ---------------------------------------------------------------------------
// Batched and non-batched types coexist
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_outcome_id_is_rejected() -> anyhow::Result<()> {
    // A runner naming a job outside its batch is a contract violation; the
    // dispatcher must reject it rather than write to an unrelated job.
    struct StrayInitializer {
        job_type: JobType,
    }
    impl BatchedJobInitializer for StrayInitializer {
        type Config = BatchConfig;
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
            _: JobSpawner<Self::Config>,
        ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>>
        {
            Ok(Box::new(StrayRunner))
        }
    }
    struct StrayRunner;
    #[async_trait]
    impl BatchedJobRunner for StrayRunner {
        type Config = BatchConfig;
        async fn run_batch(
            &self,
            current_batch: CurrentBatchedJob<BatchConfig>,
        ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
            let mut outcomes = current_batch.outcomes_for_each(|_| BatchItemOutcome::Complete);
            // A job id that was never part of this batch.
            outcomes.push((JobId::new(), BatchItemOutcome::Complete));
            Ok(JobBatchCompletion::WithOutcomes(outcomes))
        }
    }

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(StrayInitializer {
        job_type: helpers::job_type("batched-stray-outcome"),
    });

    let job_id = JobId::new();
    spawner
        .spawn(
            job_id,
            BatchConfig {
                label: "stray".into(),
            },
        )
        .await?;
    jobs.start_poll().await?;

    let outcome = jobs
        .handle(job_id)
        .await_completion(Duration::from_secs(30))
        .await?;
    assert_eq!(
        outcome.state(),
        JobTerminalState::Errored,
        "an outcome naming a job outside the batch must fail the batch"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Poller saturation accounting
// ---------------------------------------------------------------------------

/// Blocks every batch until the latch opens, recording which items it saw.
///
/// A latch rather than a notification: batches dispatched *after* the release
/// must also be let through, and a one-shot wake-up would strand them.
struct BlockingInitializer {
    job_type: JobType,
    seen: Arc<Mutex<HashSet<String>>>,
    release: Arc<AtomicBool>,
    max_batch_size: usize,
    max_concurrent_per_process: usize,
}

impl BatchedJobInitializer for BlockingInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn max_concurrent_per_process(&self) -> usize {
        self.max_concurrent_per_process
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(BlockingRunner {
            seen: Arc::clone(&self.seen),
            release: Arc::clone(&self.release),
        }))
    }
}

struct BlockingRunner {
    seen: Arc<Mutex<HashSet<String>>>,
    release: Arc<AtomicBool>,
}

#[async_trait]
impl BatchedJobRunner for BlockingRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        {
            let mut seen = self.seen.lock().await;
            for item in current_batch.items() {
                seen.insert(item.config().label.clone());
            }
        }
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(JobBatchCompletion::CompleteAll)
    }
}

/// A running batch occupies **one** unit of poller capacity, not one per job.
///
/// With `max_jobs_per_process = 3` the poller may hold three concurrent
/// execution units. If a batch were charged per row, the first batch of three
/// would exhaust the budget and no further rows could be claimed while it ran —
/// capping in-flight rows at 3. Charging one unit per batch lets the poller keep
/// claiming, so strictly more than 3 rows are in flight at once.
#[tokio::test]
async fn a_running_batch_costs_one_unit_of_poller_capacity() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder()
        .pool(pool)
        .poller_config(JobPollerConfig {
            min_jobs_per_process: 3,
            max_jobs_per_process: 3,
            ..Default::default()
        })
        .build()
        .unwrap();
    let mut jobs = Jobs::init(config).await?;

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let release = Arc::new(AtomicBool::new(false));
    let spawner = jobs.add_batched_initializer(BlockingInitializer {
        job_type: helpers::job_type("batched-unit-accounting"),
        seen: Arc::clone(&seen),
        release: Arc::clone(&release),
        max_batch_size: 3,
        max_concurrent_per_process: 2,
    });

    let specs: Vec<JobSpec<BatchConfig>> = (0..9)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("unit-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    // Every batch is blocked, so anything observed here is concurrently
    // in-flight. Per-row accounting could never exceed 3.
    let mut in_flight = 0;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        in_flight = seen.lock().await.len();
        if in_flight > 3 {
            break;
        }
    }
    assert!(
        in_flight > 3,
        "expected more than max_jobs_per_process (3) rows in flight at once, \
         which only holds if a batch costs one unit rather than one per job; \
         saw {in_flight}"
    );

    release.store(true, Ordering::SeqCst);
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(60))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed)
    );
    Ok(())
}

/// The poller claims at most `max_batch_size x max_concurrent_per_process`
/// rows of a type — it never locks rows that no batch slot is free to execute.
///
/// This is the property that keeps a large backlog *claimable*: rows the
/// process cannot start on stay `pending`, visible to other poller instances
/// and accumulating into fuller later batches, instead of sitting `running`
/// behind a saturated process.
#[tokio::test]
async fn claims_are_capped_by_free_batch_slots() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    // Default poller config: min 30 / max 50 units, so the global budget is
    // deliberately *not* the binding constraint here — the slot budget is.
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let release = Arc::new(AtomicBool::new(false));
    let spawner = jobs.add_batched_initializer(BlockingInitializer {
        job_type: helpers::job_type("batched-slot-capped-claims"),
        seen: Arc::clone(&seen),
        release: Arc::clone(&release),
        max_batch_size: 5,
        max_concurrent_per_process: 2,
    });

    // A backlog far larger than the slot budget.
    let specs: Vec<JobSpec<BatchConfig>> = (0..200)
        .map(|i| {
            JobSpec::new(
                JobId::new(),
                BatchConfig {
                    label: format!("slot-{i}"),
                },
            )
        })
        .collect();
    let ids: Vec<JobId> = specs.iter().map(|s| s.id).collect();
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    // Every batch blocks, so nothing frees a slot: whatever is observed is the
    // steady-state claim. Give the poll loop ample time to over-claim if it can.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let claimed = seen.lock().await.len();

    assert!(
        claimed <= 10,
        "claimed {claimed} rows, but max_batch_size(5) x max_concurrent_per_process(2) \
         allows at most 10 — the poller is locking rows no batch can execute"
    );
    assert!(
        claimed > 5,
        "claimed {claimed} rows: both batch slots should be filled, giving \
         pipelining across 2 concurrent batches"
    );

    release.store(true, Ordering::SeqCst);
    let outcomes = jobs
        .handles(ids.clone())
        .await_all(Duration::from_secs(120))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "the whole backlog should still drain once slots free up"
    );
    Ok(())
}

#[tokio::test]
async fn missing_initializer_surfaces_for_unregistered_batched_type() -> anyhow::Result<()> {
    // Sanity: registering a batched type does not disturb the error surface of
    // the ordinary registry lookup.
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let jobs = Jobs::init(config).await?;
    let missing = jobs.handle(JobId::new()).load().await;
    assert!(matches!(missing, Err(JobError::Find(_))));
    Ok(())
}
