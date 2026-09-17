#![cfg(feature = "es-entity")]
//! The two waiter cases that a `pending`-only wake cannot serve on its own:
//! a waiter sitting `parked` behind a queue sibling, and a spawn batch that
//! presents the same `(callee, waiter)` pair twice.

mod helpers;

use std::time::Duration;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobSpawner, JobSpec, JobSvcConfig,
    JobType, Jobs,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// Completes on sight. Used both as the callee a waiter parks on and as the
/// queue sibling whose completion frees the queue.
struct Instant;

#[async_trait]
impl job::JobRunner for Instant {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct InstantInit {
    job_type: JobType,
}

impl JobInitializer for InstantInit {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Instant))
    }
}

/// Run 1 parks an hour out (the wait itself is registered from the test, so
/// it can be timed against the callee's liveness). Run 2 -- which only ever
/// happens because something woke it -- completes.
struct Waiter;

#[async_trait]
impl job::JobRunner for Waiter {
    async fn run(
        &self,
        mut current: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if current.execution_state::<bool>()?.is_some() {
            return Ok(JobCompletion::Complete);
        }
        let mut op = current.begin_op().await?;
        current.update_execution_state_in_op(&mut op, &true).await?;
        let now = current.clock().now();
        Ok(JobCompletion::RescheduleAtWithOp(
            op,
            now + chrono::Duration::hours(1),
        ))
    }
}

struct WaiterInit {
    job_type: JobType,
}

impl JobInitializer for WaiterInit {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Waiter))
    }
}

/// A waiter that parks itself while holding a queue slot gets demoted to
/// `parked` the moment an earlier-scheduled sibling exists (the
/// `PromoteHeadsHook` swap). A wake can only MOVE a `pending` row, so the
/// callee's completion can do nothing but mark the wait -- and the mark's
/// other reader, the waiter's own `Fresh` disposition write, never runs for
/// a job that is not running. Promotion is the last chance to honour it.
///
/// Determinism: the sibling is scheduled 30 minutes out and is moved only by
/// an explicit `pull_forward_in_op`, so it cannot free the queue before the
/// callee completes; and the waiter parks a full HOUR out, so a promote that
/// ignores the mark leaves it unrunnable for an hour -- never merely slow
/// relative to the 30s assertion window below.
#[tokio::test]
async fn a_waiter_parked_behind_a_queue_sibling_is_woken_when_promoted() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let callee_type = helpers::job_type("queued-wake-callee");
    let sibling_type = helpers::job_type("queued-wake-sibling");
    let waiter_type = helpers::job_type("queued-wake-waiter");

    let callee_id = JobId::new();
    let callee_spawner = jobs.add_initializer(InstantInit {
        job_type: callee_type.clone(),
    });
    let sibling_spawner = jobs.add_initializer(InstantInit {
        job_type: sibling_type.clone(),
    });
    let waiter_spawner = jobs.add_initializer(WaiterInit {
        job_type: waiter_type.clone(),
    });
    jobs.start_poll().await?;

    let now = chrono::Utc::now();
    // Live but not due: something for the waiter to attach to.
    let callee = callee_spawner
        .spawn_at(callee_id, Cfg, now + chrono::Duration::hours(1))
        .await?;

    let queue = helpers::unique("q");
    let waiter = waiter_spawner
        .spawn_spec(JobSpec::new(JobId::new(), Cfg).queue_id(queue.clone()))
        .await?;
    // Earlier than the waiter's park (+1h) but far from due: the swap that
    // demotes the waiter to `parked` needs a strictly older sibling, and this
    // one must not run until told.
    let sibling = sibling_spawner
        .spawn_spec(
            JobSpec::new(JobId::new(), Cfg)
                .queue_id(queue.clone())
                .schedule_at(now + chrono::Duration::minutes(30)),
        )
        .await?;

    // The waiter has to be parked BEFORE the callee goes terminal -- that is
    // the whole point. Wait for the row to actually reach `parked`.
    helpers::await_state(&pool, waiter.id(), "parked", Duration::from_secs(30)).await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    callee.register_waiter_in_op(&mut op, waiter.id()).await?;
    op.commit().await?;

    // Callee completes while the waiter is parked: the wake can only mark.
    let mut op = es_entity::DbOp::init(&pool).await?;
    jobs.pull_forward_in_op(&mut op, callee_id, chrono::Utc::now())
        .await?;
    op.commit().await?;
    callee.await_completion(Duration::from_secs(30)).await?;

    // Freeing the queue promotes the waiter. Its own `execute_at` still says
    // +1h; only the mark can bring it forward.
    let mut op = es_entity::DbOp::init(&pool).await?;
    sibling
        .pull_forward_in_op(&mut op, chrono::Utc::now())
        .await?;
    op.commit().await?;
    sibling.await_completion(Duration::from_secs(30)).await?;

    waiter.await_completion(Duration::from_secs(30)).await?;

    jobs.shutdown().await?;
    Ok(())
}

/// A wake must never shorten a retry backoff -- the guard the by-id and
/// keyed pull-forwards both carry -- and the promote-time consult is no
/// exception. Same construction as the test above, except the waiter is put
/// into backoff (`attempt_index = 2`) while it sits `parked`, so the mark is
/// present and the ONLY thing standing between it and a pulled-forward row
/// is the guard.
///
/// Determinism: the waiter parks an hour out and the assertion is that it is
/// still scheduled more than 30 minutes out after being promoted. An
/// unguarded promote sets it to `now`, which is not near that boundary.
#[tokio::test]
async fn a_wake_never_shortens_a_backoff_at_promote() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let callee_spawner = jobs.add_initializer(InstantInit {
        job_type: helpers::job_type("backoff-wake-callee"),
    });
    let sibling_spawner = jobs.add_initializer(InstantInit {
        job_type: helpers::job_type("backoff-wake-sibling"),
    });
    let waiter_spawner = jobs.add_initializer(WaiterInit {
        job_type: helpers::job_type("backoff-wake-waiter"),
    });
    jobs.start_poll().await?;

    let now = chrono::Utc::now();
    let callee_id = JobId::new();
    let callee = callee_spawner
        .spawn_at(callee_id, Cfg, now + chrono::Duration::hours(1))
        .await?;

    let queue = helpers::unique("q");
    let waiter = waiter_spawner
        .spawn_spec(JobSpec::new(JobId::new(), Cfg).queue_id(queue.clone()))
        .await?;
    let sibling = sibling_spawner
        .spawn_spec(
            JobSpec::new(JobId::new(), Cfg)
                .queue_id(queue.clone())
                .schedule_at(now + chrono::Duration::minutes(30)),
        )
        .await?;

    helpers::await_state(&pool, waiter.id(), "parked", Duration::from_secs(30)).await?;

    // Put the parked waiter into retry backoff. Done directly because no
    // public path parks a row and then fails it -- the state is reachable in
    // production (a queued job that fails, backs off, and is then overtaken
    // by an older sibling) but not constructible through the API in one test.
    sqlx::query("UPDATE job_executions SET attempt_index = 2 WHERE id = $1")
        .bind(uuid::Uuid::from(waiter.id()))
        .execute(&pool)
        .await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    callee.register_waiter_in_op(&mut op, waiter.id()).await?;
    op.commit().await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    jobs.pull_forward_in_op(&mut op, callee_id, chrono::Utc::now())
        .await?;
    op.commit().await?;
    callee.await_completion(Duration::from_secs(30)).await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    sibling
        .pull_forward_in_op(&mut op, chrono::Utc::now())
        .await?;
    op.commit().await?;
    sibling.await_completion(Duration::from_secs(30)).await?;

    helpers::await_state(&pool, waiter.id(), "pending", Duration::from_secs(30)).await?;
    let execute_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT execute_at FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(waiter.id()))
            .fetch_one(&pool)
            .await?;
    let execute_at = execute_at.expect("a promoted row is pending and so has an execute_at");
    assert!(
        execute_at > chrono::Utc::now() + chrono::Duration::minutes(30),
        "a wake must not shorten a retry backoff at promote; execute_at was {execute_at}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Two specs in one batch sharing BOTH a `dedup_key` and a waiter present
/// the identical `(job_id, waiter_job_id)` pair twice to one statement,
/// which `ON CONFLICT DO UPDATE` rejects outright ("cannot affect row a
/// second time"). Deterministic: it is a hard error from Postgres on every
/// run, not a race.
#[tokio::test]
async fn a_batch_repeating_one_callee_and_waiter_pair_still_spawns() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("dup-waiter-pair");
    let spawner = jobs.add_initializer(InstantInit {
        job_type: job_type.clone(),
    });
    jobs.start_poll().await?;

    let waiter = JobId::new();
    let key = helpers::unique("k");
    let handles = spawner
        .spawn_all(vec![
            JobSpec::new(JobId::new(), Cfg)
                .dedup_key(key.clone())
                .waiter(waiter),
            JobSpec::new(JobId::new(), Cfg)
                .dedup_key(key.clone())
                .waiter(waiter),
        ])
        .await?;

    assert_eq!(handles.len(), 2);
    assert_eq!(
        handles[0].id(),
        handles[1].id(),
        "both specs share a dedup key, so both resolve to the one job"
    );
    assert!(handles[0].created(), "the first spec mints it");
    assert!(!handles[1].created(), "the second coalesces onto it");

    jobs.shutdown().await?;
    Ok(())
}
