//! Tests for the in-op and bulk keyed spawn surface:
//! `KeyedJobSpawner::spawn_in_op` / `spawn_all` / `spawn_all_in_op` and
//! `KeyedSpawn`.
//!
//! The semantic under test throughout is keyed spawn's own, which differs
//! from `JobSpec::dedup_key`'s (`bulk_spawn_dedup_key.rs`): a collision
//! RESOLVES to the live holder rather than dropping the spec, so every
//! requested key yields a handle and `KeyedSpawn::created` says which
//! generation it names.
//!
//! The case worth the most attention is
//! `two_spawn_in_op_calls_on_one_op_resolve_to_the_same_job`: keyed spawn
//! inserts its execution rows inline rather than deferring them to
//! `ExecutionInsertHook`, which is what lets the second call's live-check see
//! the first call's uncommitted row. Deferring would silently collapse the
//! two in `insert_many`'s `DISTINCT ON` and strand an orphan `jobs` row
//! behind a handle that never runs.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobRunner, JobSvcConfig, JobType, Jobs, KeyedJobInitializer,
    KeyedJobSpawner, KeyedJobSpec,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    marker: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct State {
    seen: u32,
}

/// Completes immediately, recording whatever execution state it observed at
/// the start of the run as its return value, then writing its own. That pair
/// is what the `inherits_state` assertions read. The observation is recorded
/// as a `seen` number (`INHERITED_NOTHING` when there was no predecessor
/// state) rather than an `Option`, because a JSON `null` result is
/// indistinguishable from "no result set" through `JobOutcome::result`.
struct Runner {
    marker: u32,
    hold: Option<Arc<AtomicBool>>,
}

/// Sentinel `State::seen` for a generation that inherited nothing.
const INHERITED_NOTHING: u32 = 0;

#[async_trait]
impl JobRunner for Runner {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let observed: Option<State> = current_job.execution_state()?;
        current_job
            .set_result(&State {
                seen: observed.map(|s| s.seen).unwrap_or(INHERITED_NOTHING),
            })
            .await?;
        current_job
            .update_execution_state(State { seen: self.marker })
            .await?;
        if let Some(hold) = &self.hold {
            while !hold.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(JobCompletion::Complete)
    }
}

struct Init {
    job_type: JobType,
    inherits_state: bool,
    hold: Option<Arc<AtomicBool>>,
}

impl Init {
    fn plain(job_type: &JobType) -> Self {
        Self {
            job_type: job_type.clone(),
            inherits_state: false,
            hold: None,
        }
    }
}

impl KeyedJobInitializer for Init {
    type Config = Cfg;

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
        let config: Cfg = job.config()?;
        Ok(Box::new(Runner {
            marker: config.marker,
            hold: self.hold.clone(),
        }))
    }
}

fn unique_job_type(prefix: &str) -> JobType {
    JobType::new(Box::leak(
        format!("{prefix}-{}", uuid::Uuid::now_v7()).into_boxed_str(),
    ))
}

async fn count_jobs(pool: &sqlx::PgPool, job_type: &JobType, key: &str) -> anyhow::Result<i64> {
    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1 AND unique_key = $2")
            .bind(job_type.as_str())
            .bind(key)
            .fetch_one(pool)
            .await?;
    Ok(count)
}

/// The happy path: a keyed job created inside the caller's transaction is
/// durable once that transaction commits, and reports `created`.
#[tokio::test]
async fn spawn_in_op_commits_with_the_caller() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-in-op-commit");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));
    // Deliberately not polling: the assertions are spawn-time properties of a
    // LIVE key, and a running poller could complete the job and free the key
    // mid-test. Same reasoning as `spawn_keyed_duplicate_returns_persisted_handle`.

    let mut op = es_entity::DbOp::init(&pool).await?;
    let spawned = spawner
        .spawn_in_op(&mut op, "shard-a", Cfg { marker: 1 })
        .await?;
    assert!(spawned.created, "a free key must report created");
    assert_eq!(spawned.key, "shard-a");
    op.commit().await?;

    assert_eq!(count_jobs(&pool, &job_type, "shard-a").await?, 1);
    assert_eq!(jobs.handle(spawned.handle.id()).load().await?.job().id, {
        spawned.handle.id()
    });

    jobs.shutdown().await?;
    Ok(())
}

/// The point of an in-op spawn: rolling the caller's transaction back takes
/// the job with it, leaving neither a `jobs` row nor a claimed key.
#[tokio::test]
async fn spawn_in_op_rolls_back_with_the_caller() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-in-op-rollback");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let mut op = es_entity::DbOp::init(&pool).await?;
    spawner
        .spawn_in_op(&mut op, "shard-a", Cfg { marker: 1 })
        .await?;
    drop(op);

    assert_eq!(
        count_jobs(&pool, &job_type, "shard-a").await?,
        0,
        "a rolled-back op must leave no jobs row"
    );
    let (execs,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_executions WHERE job_type = $1 AND unique_key = 'shard-a'",
    )
    .bind(job_type.as_str())
    .fetch_one(&pool)
    .await?;
    assert_eq!(execs, 0, "and no claimed key");

    // The key is genuinely free afterwards, not merely un-rowed.
    let again = spawner.spawn("shard-a", Cfg { marker: 2 }).await?;
    assert_eq!(count_jobs(&pool, &job_type, "shard-a").await?, 1);
    assert_eq!(again.id(), again.id());

    jobs.shutdown().await?;
    Ok(())
}

/// Two `spawn_in_op` calls sharing ONE op and one key: the second must
/// resolve to the first's job, create no second `jobs` row, and report
/// `created == false`.
///
/// This is the case an `ExecutionInsertHook`-deferred insert could not get
/// right: with the row buffered until commit, the second call's live-check
/// would find the key free, create a second `jobs` row, and hand back a
/// handle for it — then `insert_many`'s `DISTINCT ON` would drop one of the
/// two execution rows at commit, stranding a `jobs` row whose job never runs.
/// Inserting inline is what makes the live-check see it, since a transaction
/// sees its own uncommitted writes.
#[tokio::test]
async fn two_spawn_in_op_calls_on_one_op_resolve_to_the_same_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-in-op-same-op");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let mut op = es_entity::DbOp::init(&pool).await?;
    let first = spawner
        .spawn_in_op(&mut op, "shard-a", Cfg { marker: 1 })
        .await?;
    let second = spawner
        .spawn_in_op(&mut op, "shard-a", Cfg { marker: 2 })
        .await?;
    op.commit().await?;

    assert!(first.created);
    assert!(
        !second.created,
        "the second call on the same op must resolve, not create"
    );
    assert_eq!(
        second.handle.id(),
        first.handle.id(),
        "both calls must name the job that actually runs"
    );
    assert_eq!(
        count_jobs(&pool, &job_type, "shard-a").await?,
        1,
        "no orphan jobs row"
    );

    let (execs,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_executions WHERE job_type = $1 AND unique_key = 'shard-a'",
    )
    .bind(job_type.as_str())
    .fetch_one(&pool)
    .await?;
    assert_eq!(execs, 1);

    jobs.shutdown().await?;
    Ok(())
}

/// A key already held by a committed LIVE job resolves to that job, with
/// `created == false` — the in-op mirror of
/// `spawn_keyed_duplicate_returns_persisted_handle`.
#[tokio::test]
async fn spawn_in_op_resolves_to_a_committed_live_holder() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-in-op-holder");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let held = spawner.spawn("shard-a", Cfg { marker: 1 }).await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    let spawned = spawner
        .spawn_in_op(&mut op, "shard-a", Cfg { marker: 2 })
        .await?;
    op.commit().await?;

    assert!(!spawned.created);
    assert_eq!(spawned.handle.id(), held.id());
    assert_eq!(count_jobs(&pool, &job_type, "shard-a").await?, 1);

    jobs.shutdown().await?;
    Ok(())
}

/// `spawn_all` returns one outcome per spec, in input order, so results can
/// be zipped straight back against the inputs.
#[tokio::test]
async fn spawn_all_returns_one_outcome_per_spec_in_order() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-order");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let keys = ["shard-c", "shard-a", "shard-b"];
    let spawned = spawner
        .spawn_all(
            keys.iter()
                .enumerate()
                .map(|(i, k)| KeyedJobSpec::new(*k, Cfg { marker: i as u32 }))
                .collect(),
        )
        .await?;

    assert_eq!(spawned.len(), keys.len());
    assert_eq!(
        spawned.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(),
        keys,
        "outcomes must come back in input order, not key order"
    );
    assert!(spawned.iter().all(|s| s.created));

    let mut ids: Vec<_> = spawned.iter().map(|s| s.handle.id()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), keys.len(), "distinct keys are distinct jobs");

    for key in keys {
        assert_eq!(count_jobs(&pool, &job_type, key).await?, 1);
    }

    jobs.shutdown().await?;
    Ok(())
}

/// One key repeated WITHIN a single `spawn_all` call: neither spec is
/// inserted when the live-check runs, so the in-call `seen` map is what
/// collapses them. The first spec wins and the second resolves to it.
#[tokio::test]
async fn spawn_all_collapses_a_key_repeated_within_one_call() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-repeat");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let spawned = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("shard-a", Cfg { marker: 1 }),
            KeyedJobSpec::new("shard-b", Cfg { marker: 2 }),
            KeyedJobSpec::new("shard-a", Cfg { marker: 3 }),
        ])
        .await?;

    assert_eq!(spawned.len(), 3, "every spec still yields an outcome");
    assert!(spawned[0].created);
    assert!(spawned[1].created);
    assert!(!spawned[2].created, "the repeat must resolve, not create");
    assert_eq!(spawned[2].handle.id(), spawned[0].handle.id());
    assert_eq!(count_jobs(&pool, &job_type, "shard-a").await?, 1);

    jobs.shutdown().await?;
    Ok(())
}

/// A bulk call spanning both outcomes: free keys are created, held ones
/// resolve, and the two are reported per spec.
#[tokio::test]
async fn spawn_all_mixes_created_and_resolved() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-mixed");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let held = spawner.spawn("shard-a", Cfg { marker: 1 }).await?;

    let spawned = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("shard-a", Cfg { marker: 2 }),
            KeyedJobSpec::new("shard-b", Cfg { marker: 3 }),
        ])
        .await?;

    assert!(!spawned[0].created);
    assert_eq!(spawned[0].handle.id(), held.id());
    assert!(spawned[1].created);
    assert_eq!(count_jobs(&pool, &job_type, "shard-a").await?, 1);
    assert_eq!(count_jobs(&pool, &job_type, "shard-b").await?, 1);

    jobs.shutdown().await?;
    Ok(())
}

/// An empty spec list is a no-op, not an error or a stray statement.
#[tokio::test]
async fn spawn_all_with_no_specs_is_a_noop() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-empty");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    assert!(spawner.spawn_all(Vec::new()).await?.is_empty());

    jobs.shutdown().await?;
    Ok(())
}

/// `inherits_state` across a BULK respawn: each new generation must be
/// seeded from its OWN key's predecessor (the per-key `LATERAL`), and older
/// generations compacted away, with several keys in one statement.
#[tokio::test]
async fn spawn_all_carries_state_per_key_across_generations() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-inherit");
    let spawner = jobs.add_keyed_initializer(Init {
        job_type: job_type.clone(),
        inherits_state: true,
        hold: None,
    });
    jobs.start_poll().await?;

    // Generation 1: distinct markers per key, so a cross-key mix-up in the
    // seeding LATERAL would show up as the wrong `seen` value below.
    let gen1 = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("shard-a", Cfg { marker: 10 }),
            KeyedJobSpec::new("shard-b", Cfg { marker: 20 }),
        ])
        .await?;
    for spawned in &gen1 {
        let outcome = spawned
            .handle
            .await_completion(Duration::from_secs(30))
            .await?;
        let observed: Option<State> = outcome.result()?;
        assert_eq!(
            observed,
            Some(State {
                seen: INHERITED_NOTHING
            }),
            "the first generation inherits nothing"
        );
    }

    // Generation 2: same keys, now free again.
    let gen2 = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("shard-a", Cfg { marker: 11 }),
            KeyedJobSpec::new("shard-b", Cfg { marker: 21 }),
        ])
        .await?;
    assert!(
        gen2.iter().all(|s| s.created),
        "a terminal key is respawnable"
    );
    assert_ne!(gen2[0].handle.id(), gen1[0].handle.id());

    for (spawned, expected) in gen2.iter().zip([10, 20]) {
        let outcome = spawned
            .handle
            .await_completion(Duration::from_secs(30))
            .await?;
        let observed: Option<State> = outcome.result()?;
        assert_eq!(
            observed,
            Some(State { seen: expected }),
            "each key must inherit from its OWN predecessor"
        );
    }

    // Compaction: one retained state row per key, not one per generation.
    for key in ["shard-a", "shard-b"] {
        let (states,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM job_execution_states s JOIN jobs j ON j.id = s.id
             WHERE j.job_type = $1 AND j.unique_key = $2",
        )
        .bind(job_type.as_str())
        .bind(key)
        .fetch_one(&pool)
        .await?;
        assert_eq!(states, 1, "a key holds at most one retained state row");
    }

    jobs.shutdown().await?;
    Ok(())
}

/// Without `inherits_state` (the default) a bulk respawn starts clean and the
/// predecessor's state row is compacted away rather than seeded.
#[tokio::test]
async fn spawn_all_without_inherits_state_starts_each_generation_clean() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-no-inherit");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));
    jobs.start_poll().await?;

    let gen1 = spawner
        .spawn_all(vec![KeyedJobSpec::new("shard-a", Cfg { marker: 10 })])
        .await?;
    gen1[0]
        .handle
        .await_completion(Duration::from_secs(30))
        .await?;

    let gen2 = spawner
        .spawn_all(vec![KeyedJobSpec::new("shard-a", Cfg { marker: 11 })])
        .await?;
    let outcome = gen2[0]
        .handle
        .await_completion(Duration::from_secs(30))
        .await?;
    let observed: Option<State> = outcome.result()?;
    assert_eq!(
        observed,
        Some(State {
            seen: INHERITED_NOTHING
        }),
        "without inherits_state each generation starts fresh"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Two concurrent spawns of one key, in separate transactions: exactly one
/// creates, the other resolves to it, and both name the same job.
///
/// This is the race the old three-attempt `KeyedSpawnRace` retry loop existed
/// to survive. It no longer needs retrying: the key's advisory lock is taken
/// BEFORE the liveness check and held to commit, so the loser blocks at its
/// own lock acquisition and its check — a separate statement, hence a fresh
/// READ COMMITTED snapshot — observes whatever the winner committed. What the
/// old `ON CONFLICT` design could not distinguish (a holder that went
/// terminal between the conflict and the read) cannot arise here.
#[tokio::test]
async fn concurrent_spawns_of_one_key_agree_on_the_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-concurrent");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));
    // No poller: the key must stay held for the duration, so the race is
    // decided by the spawn path rather than by a runner freeing the key.

    for i in 0..20 {
        let key = format!("race-{i}");
        let (a, b) = tokio::join!(
            spawner.spawn(key.clone(), Cfg { marker: 1 }),
            spawner.spawn(key.clone(), Cfg { marker: 2 })
        );
        let a = a.expect("a lost race must resolve, never error");
        let b = b.expect("a lost race must resolve, never error");
        assert_eq!(
            a.id(),
            b.id(),
            "both callers must observe the job that actually runs"
        );
        assert_eq!(
            count_jobs(&pool, &job_type, &key).await?,
            1,
            "the loser must leave no orphan jobs row"
        );
    }

    jobs.shutdown().await?;
    Ok(())
}

/// `schedule_at` on a spec defers that key without holding up its siblings,
/// and the deferred job still holds its key meanwhile.
#[tokio::test]
async fn spawn_all_honors_per_spec_schedule_at() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = unique_job_type("keyed-bulk-schedule");
    let spawner = jobs.add_keyed_initializer(Init::plain(&job_type));

    let far_future = chrono::Utc::now() + chrono::Duration::hours(1);
    let spawned = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("now", Cfg { marker: 1 }),
            KeyedJobSpec::new("later", Cfg { marker: 2 }).schedule_at(far_future),
        ])
        .await?;
    assert!(spawned.iter().all(|s| s.created));

    let (execute_at,): (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(
        "SELECT execute_at FROM job_executions WHERE job_type = $1 AND unique_key = 'later'",
    )
    .bind(job_type.as_str())
    .fetch_one(&pool)
    .await?;
    assert!(
        execute_at > chrono::Utc::now() + chrono::Duration::minutes(30),
        "per-spec schedule_at must reach the execution row"
    );

    // A not-yet-due generation still holds its key.
    let again = spawner.spawn("later", Cfg { marker: 3 }).await?;
    assert_eq!(again.id(), spawned[1].handle.id());

    jobs.shutdown().await?;
    Ok(())
}
