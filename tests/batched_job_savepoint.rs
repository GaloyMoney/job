//! Live-PG coverage for `CurrentBatchedJob::run_isolated` — per-item
//! `SAVEPOINT` isolation inside a shared batch transaction (es-entity
//! 0.12.8's `DbOp::with_savepoint`).
//!
//! The contract under test: a per-item **DB** error no longer poisons the
//! whole batch transaction. Before this, any error surfacing from a
//! statement run directly against a batch's `op` aborted the entire shared
//! transaction — every item in the batch, including ones that had nothing to
//! do with the failure, went through the whole-batch retry path. With
//! `run_isolated`, a failing item's writes unwind to its own savepoint and
//! the rest of the batch still commits in the same transaction.
#![cfg(feature = "es-entity")]

mod helpers;

use async_trait::async_trait;
use es_entity::AtomicOperation;
use job::{
    BatchItemOutcome, BatchedJobInitializer, BatchedJobRunner, CurrentBatchedJob,
    JobBatchCompletion, JobId, JobSpawner, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs,
    RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavepointConfig {
    label: String,
    /// Scratch table this item writes into. Carried on the config (rather
    /// than captured from the runner) so the closure passed to
    /// `run_isolated` captures nothing from `run_batch`'s environment —
    /// everything it needs arrives through `item`.
    table: String,
    /// Items sharing the same key race for the same row in `table`: the
    /// first insert wins, every later one hits a genuine Postgres
    /// unique-violation inside its own savepoint.
    conflict_key: Option<i32>,
}

/// Inserts `config.conflict_key` into `config.table` through
/// [`CurrentBatchedJob::run_isolated`] — only on an item's first attempt, so
/// a retried item (always dispatched solo, one attempt later) finds nothing
/// standing in its way, the same way a real transient DB failure would clear
/// by the next try.
struct SavepointInitializer {
    job_type: JobType,
    n_attempts: Option<u32>,
}

impl BatchedJobInitializer for SavepointInitializer {
    type Config = SavepointConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings {
            n_attempts: self.n_attempts,
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            ..Default::default()
        }
    }

    fn init(
        &self,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(SavepointRunner))
    }
}

struct SavepointRunner;

#[async_trait]
impl BatchedJobRunner for SavepointRunner {
    type Config = SavepointConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<SavepointConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let mut op = current_batch.begin_op().await?;
        let outcomes = current_batch
            .run_isolated(&mut op, async |sp, item| {
                if item.attempt() == 1
                    && let Some(key) = item.config().conflict_key
                {
                    sqlx::query(&format!(
                        "INSERT INTO {} (v) VALUES ($1)",
                        item.config().table
                    ))
                    .bind(key)
                    .execute(sp.as_executor())
                    .await?;
                }
                Ok::<_, sqlx::Error>(BatchItemOutcome::Complete)
            })
            .await?;
        Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
    }
}

/// Drops and recreates a scratch table with a `PRIMARY KEY` constraint, so
/// each test starts from a clean slate regardless of leftovers from a
/// previous run. A real (non-temporary) table on purpose: it must produce a
/// genuine conflict even if the racing items land in different batches on
/// different pooled connections, not just within one transaction.
async fn reset_scratch_table(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await?;
    sqlx::query(&format!("CREATE TABLE {table} (v INT PRIMARY KEY)"))
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn run_isolated_keeps_batch_mates_committing_after_a_per_item_db_error() -> anyhow::Result<()>
{
    let table = "batched_savepoint_test_conflict";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;

    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(SavepointInitializer {
        job_type: JobType::new("batched-savepoint-conflict"),
        // Terminal on the first failure: keeps this test's assertions about
        // the *first pass* unambiguous (no retry churn to account for).
        n_attempts: Some(1),
    });

    // Three items race for the same row: exactly one insert can win.
    let conflict_ids: Vec<JobId> = (0..3).map(|_| JobId::new()).collect();
    // Two conflict-free controls: their only job is to prove the shared
    // transaction stayed usable after the conflicting items' savepoints
    // rolled back.
    let ok_ids: Vec<JobId> = (0..2).map(|_| JobId::new()).collect();

    let mut specs: Vec<JobSpec<SavepointConfig>> = conflict_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            JobSpec::new(
                *id,
                SavepointConfig {
                    label: format!("conflict-{i}"),
                    table: table.to_string(),
                    conflict_key: Some(1),
                },
            )
        })
        .collect();
    specs.extend(ok_ids.iter().enumerate().map(|(i, id)| {
        JobSpec::new(
            *id,
            SavepointConfig {
                label: format!("ok-{i}"),
                table: table.to_string(),
                conflict_key: None,
            },
        )
    }));
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    let all: Vec<JobId> = conflict_ids.iter().chain(ok_ids.iter()).copied().collect();
    let outcomes = jobs
        .handles(all.clone())
        .await_all(Duration::from_secs(30))
        .await?;

    // The conflict-free controls must complete. Pre-savepoint, a per-item DB
    // error run straight against the batch's `op` would abort the whole
    // transaction and take these down with the conflicting items on every
    // attempt (the batch keeps re-forming with the same losers) — this is
    // the assertion that actually distinguishes isolation from poisoning.
    for outcome in outcomes.iter().skip(3) {
        assert_eq!(
            outcome.state(),
            JobTerminalState::Completed,
            "a conflict-free batch-mate must complete regardless of another \
             item's DB error"
        );
    }

    // Of the three racing items, exactly one wins the insert and completes;
    // the other two roll back to their own savepoint and, with n_attempts=1,
    // surface as Errored rather than dragging the batch down.
    let conflict_states: Vec<_> = outcomes.iter().take(3).map(|o| o.state()).collect();
    let completed = conflict_states
        .iter()
        .filter(|s| **s == JobTerminalState::Completed)
        .count();
    let errored = conflict_states
        .iter()
        .filter(|s| **s == JobTerminalState::Errored)
        .count();
    assert_eq!(
        completed, 1,
        "exactly one conflicting item should win the insert, saw {conflict_states:?}"
    );
    assert_eq!(
        errored, 2,
        "the losing items must be isolated failures, not a batch-wide one, \
         saw {conflict_states:?}"
    );

    Ok(())
}

#[tokio::test]
async fn run_isolated_failed_item_retries_solo_and_recovers() -> anyhow::Result<()> {
    let table = "batched_savepoint_test_retry";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;

    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let spawner = jobs.add_batched_initializer(SavepointInitializer {
        job_type: JobType::new("batched-savepoint-retry"),
        n_attempts: Some(5),
    });

    // Two items share a key: the loser's first attempt hits a real
    // unique-violation inside its savepoint, is translated to `Fail`, and is
    // retried per the type's `RetrySettings` — same as any other batched
    // failure, proving savepoint isolation doesn't disturb the existing
    // retry-solo rule (attempt > 1 is always dispatched alone). Its second
    // attempt skips the insert (see `SavepointRunner`) and completes.
    let winner_id = JobId::new();
    let loser_id = JobId::new();
    let specs = vec![
        JobSpec::new(
            winner_id,
            SavepointConfig {
                label: "winner".into(),
                table: table.to_string(),
                conflict_key: Some(7),
            },
        ),
        JobSpec::new(
            loser_id,
            SavepointConfig {
                label: "loser".into(),
                table: table.to_string(),
                conflict_key: Some(7),
            },
        ),
    ];
    spawner.spawn_all(specs).await?;

    jobs.start_poll().await?;

    let outcomes = jobs
        .handles(vec![winner_id, loser_id])
        .await_all(Duration::from_secs(30))
        .await?;
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "both the immediate winner and the retried loser should complete, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );

    Ok(())
}
