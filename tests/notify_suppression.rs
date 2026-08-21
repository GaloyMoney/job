//! Regression tests for Fix 3 of the sb-max8 handoff: `ExecutionInsertHook`
//! used to notify every type that landed a `pending` row unconditionally,
//! even when the SAME transaction's head-swap `ClaimHook` immediately
//! self-claimed it -- a redundant wake for work already stolen, measured at
//! 19-23% of all DB exec time in production. `ExecutionReadyNotifyHook`
//! nets each type's `adds` against `suppress` (and always fires for
//! `forces`) and only calls `execution_ready_in_op` when residue remains.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig,
    JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct NoopRunner;

#[async_trait]
impl JobRunner for NoopRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct NoopInitializer {
    job_type: JobType,
    short_circuit: bool,
}

impl JobInitializer for NoopInitializer {
    type Config = Cfg;
    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }
    fn short_circuit(&self) -> bool {
        self.short_circuit
    }
    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(NoopRunner))
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn job_type(prefix: &str) -> JobType {
    JobType::new(Box::leak(unique(prefix).into_boxed_str()))
}

/// Waits up to `within` for a `job_events` notification matching `pred`,
/// `None` if nothing matched in time. Modeled on
/// `tests/job.rs`'s `next_matching` (same channel, same shape) -- an
/// absence assertion has no sequence point to gate on, so this is the
/// established pattern in this crate for "prove nothing arrived": a
/// generous bound (well past the 25ms notify debounce window) rather than
/// a guessed sleep.
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

/// A due-now spawn of a `short_circuit` type with free capacity is claimed
/// synchronously off its own commit (see
/// `parked_rows.rs::short_circuit_spawn_lands_running_immediately_on_commit`).
/// That claim must suppress the spawn's own `execution_ready` notify --
/// nothing is left for it to wake a poll to do.
#[tokio::test]
async fn self_claimed_spawn_suppresses_its_own_notify() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("job_events").await?;

    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let jt = job_type("notify-suppress-claimed");
    let spawner = jobs.add_initializer(NoopInitializer {
        job_type: jt.clone(),
        short_circuit: true,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    // Precondition: the row really was self-claimed synchronously -- if
    // this ever fails, the test below would be vacuously true for the
    // wrong reason (nothing to suppress in the first place).
    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
    assert_eq!(state, "running", "precondition: spawn must self-claim");

    let stray = next_matching(&mut listener, Duration::from_millis(500), |payload| {
        payload["job_type"] == jt.to_string()
    })
    .await;
    assert!(
        stray.is_none(),
        "a self-claimed spawn must not also notify: {stray:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The contrasting case: an op with no claim coverage at all -- deliberately
/// never calling `start_poll()`, so the spawner's `PollerHandle` never
/// populates and `ExecutionInsertHook` never registers a `ClaimHook` --
/// must still notify unconditionally, matching the handoff's own stated
/// non-special-case: "no `ClaimHook` ⇒ no `suppress` entries ⇒ the full
/// `adds` fire". This also sidesteps racing a live background poll for the
/// state assertion below: with no poller at all, nothing but this test can
/// touch the row.
#[tokio::test]
async fn unclaimed_spawn_still_notifies() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("job_events").await?;

    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let jt = job_type("notify-unclaimed");
    let spawner = jobs.add_initializer(NoopInitializer {
        job_type: jt.clone(),
        short_circuit: false,
    });
    // No `start_poll()`: no poller, no `ClaimHook`, nothing to suppress with.

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        state, "pending",
        "precondition: no poller running, nothing could have claimed it"
    );

    let found = next_matching(&mut listener, Duration::from_millis(500), |payload| {
        payload["job_type"] == jt.to_string()
    })
    .await;
    assert!(found.is_some(), "an unclaimed spawn must still notify");

    jobs.shutdown().await?;
    Ok(())
}
