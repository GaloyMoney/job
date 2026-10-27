//! Span-assertion coverage for a batch item's `BatchItemOutcome::Fail`: the
//! verdict is recorded on that item's own `job.conclude_failed` span, never on
//! the enclosing `job.execute_batch` span its siblings share.
//!
//! Its own file/binary for the same reason as `tests/lanes_denied_span.rs`:
//! `tracing`'s per-callsite interest cache is process-wide, so span capture is
//! only reliable isolated in its own test binary.

mod helpers;
mod span_capture;

use async_trait::async_trait;
use es_entity::errlanes::Denied;
use job::{
    BatchItemOutcome, BatchOutcomes, BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob,
    JobBatchCompletion, JobId, JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchConfig {
    fail: bool,
}

struct FailOneInitializer {
    job_type: JobType,
}

impl BatchedJobInitializer for FailOneInitializer {
    type Config = BatchConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<
        Box<dyn BatchedJobRunner<Config = Self::Config>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(Box::new(FailOneRunner))
    }
}

struct FailOneRunner;

#[async_trait]
impl BatchedJobRunner for FailOneRunner {
    type Config = BatchConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BatchConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let outcomes: BatchOutcomes = current_batch.outcomes_for_each(|item| {
            if item.config().fail {
                BatchItemOutcome::Fail {
                    error: Box::new(Denied::default()),
                }
            } else {
                BatchItemOutcome::Complete
            }
        });
        Ok(JobBatchCompletion::WithOutcomes(outcomes))
    }
}

#[tokio::test]
async fn a_batch_item_fail_is_recorded_on_its_own_conclude_failed_span() -> anyhow::Result<()> {
    let (fields, _guard) = span_capture::install();

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(FailOneInitializer {
        job_type: helpers::job_type("batch-fail-span"),
    });

    let ok_id = JobId::new();
    let failed_id = JobId::new();
    spawner
        .spawn_all(vec![
            JobSpec::new(ok_id, BatchConfig { fail: false }),
            JobSpec::new(failed_id, BatchConfig { fail: true }),
        ])
        .await?;
    jobs.start_poll().await?;

    let outcomes = jobs
        .handles(vec![ok_id, failed_id])
        .await_all(Duration::from_secs(30))
        .await?;
    assert_eq!(outcomes[0].state(), JobTerminalState::Completed);
    assert_eq!(outcomes[1].state(), JobTerminalState::Failed);

    assert_eq!(
        fields.get("job.conclude_failed", "id"),
        Some(failed_id.to_string()),
        "captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.conclude_failed", "error.lane").as_deref(),
        Some("denied"),
        "a verdict's lane is not narrowed; captured: {}",
        fields.debug_dump()
    );
    assert_eq!(
        fields.get("job.conclude_failed", "error.level").as_deref(),
        Some("WARN")
    );
    assert_eq!(
        fields.get("job.conclude_failed", "will_retry").as_deref(),
        Some("false")
    );
    assert_eq!(
        fields.get("job.execute_batch", "error.lane"),
        None,
        "the batch span covers a completed sibling and must not carry the \
         item's verdict; captured: {}",
        fields.debug_dump()
    );
    assert_eq!(fields.get("job.execute_batch", "will_retry"), None);

    jobs.shutdown().await?;
    Ok(())
}
