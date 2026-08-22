//! Tests for `JobSpec::dedup_key`: opt-in live-window dedup on the bulk/
//! regular spawn path. The mechanism is the SAME `(job_type, unique_key)`
//! live-window `idx_job_executions_job_type_unique_key` already enforces for
//! keyed jobs — these tests pin the NEW surface (`JobSpec`/`spawn_all`/
//! `spawn_all_in_op`/`BulkSpawnResult`), not the underlying index (that's
//! `repo::tests::unique_per_job_type_and_key`).

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSpec,
    JobSvcConfig, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// A runner controllable from the test: blocks in `run` until released, so a
/// spawn's LIVE window can be held open on purpose. Optionally fails its
/// first N attempts before completing, to exercise the retry path. Mirrors
/// `parked_rows.rs`'s `HoldableRunner`.
struct HoldableRunner {
    started: Arc<Notify>,
    release: Arc<Notify>,
    fail_first_n: usize,
    attempts_so_far: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for HoldableRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let attempt = self.attempts_so_far.fetch_add(1, Ordering::SeqCst) + 1;
        self.started.notify_one();
        if attempt <= self.fail_first_n {
            return Err("intentional failure to exercise retry backoff".into());
        }
        self.release.notified().await;
        Ok(JobCompletion::Complete)
    }
}

struct HoldableInitializer {
    job_type: JobType,
    started: Arc<Notify>,
    release: Arc<Notify>,
    fail_first_n: usize,
    attempts_so_far: Arc<AtomicUsize>,
    retry_settings: RetrySettings,
}

impl JobInitializer for HoldableInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(HoldableRunner {
            started: Arc::clone(&self.started),
            release: Arc::clone(&self.release),
            fail_first_n: self.fail_first_n,
            attempts_so_far: Arc::clone(&self.attempts_so_far),
        }))
    }
}

/// A fresh, process-unique string for `prefix`, so re-running the suite
/// against the same persistent dev DB never collides with a previous run's
/// leftover rows. Mirrors `parked_rows.rs::unique`.
fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn job_type(prefix: &str) -> JobType {
    JobType::new(Box::leak(unique(prefix).into_boxed_str()))
}

async fn jobs_row_count(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<i64> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(n)
}

async fn execution_row_count(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<i64> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(n)
}

async fn execution_row_exists(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<bool> {
    Ok(execution_row_count(pool, id).await? > 0)
}

async fn execution_row_unique_key(
    pool: &sqlx::PgPool,
    id: JobId,
) -> anyhow::Result<Option<String>> {
    let key: Option<String> =
        sqlx::query_scalar("SELECT unique_key FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_one(pool)
            .await?;
    Ok(key)
}

async fn execution_row_state(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<String> {
    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(state)
}

async fn attempt_index(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<i32> {
    let n: i32 = sqlx::query_scalar("SELECT attempt_index FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Poll `f` until it returns `true` or the attempt budget is exhausted --
/// state polling, not a blind sleep. Mirrors `parked_rows.rs::wait_until`.
async fn wait_until(
    mut f: impl AsyncFnMut() -> anyhow::Result<bool>,
    what: &str,
) -> anyhow::Result<()> {
    for _ in 0..800 {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    anyhow::bail!("timed out waiting for: {what}");
}

/// AC1: a spec whose `dedup_key` is already held by a LIVE execution creates
/// NO `jobs` row and NO execution row, the rest of the batch lands
/// normally, and the no-op is reported via `BulkSpawnResult::deduped`.
///
/// The live holder is seeded directly (mirrors
/// `parked_rows.rs::keyed_spawn_is_blocked_by_a_parked_row_with_the_same_key`)
/// rather than run through a real job, since only the LIVE row's existence
/// matters here, not its runner.
#[tokio::test]
async fn dedup_key_no_ops_against_a_live_row() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-live");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt.clone(),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let key = unique("held-key");
    let holder_id = JobId::new();
    sqlx::query(
        "INSERT INTO jobs (id, unique_key, job_type, created_at) VALUES ($1, $2, $3, NOW())",
    )
    .bind(uuid::Uuid::from(holder_id))
    .bind(&key)
    .bind(jt.as_str())
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, unique_key, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, $3, 'running', 1, NOW(), NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(holder_id))
    .bind(jt.as_str())
    .bind(&key)
    .execute(&pool)
    .await?;

    let deduped_id = JobId::new();
    let survivor_id = JobId::new();
    let result = spawner
        .spawn_all(vec![
            JobSpec::new(deduped_id, Cfg).dedup_key(key.clone()),
            JobSpec::new(survivor_id, Cfg),
        ])
        .await?;

    assert_eq!(result.deduped, vec![deduped_id]);
    assert_eq!(result.jobs.len(), 1);
    assert_eq!(result.jobs[0].id, survivor_id);

    assert_eq!(
        jobs_row_count(&pool, deduped_id).await?,
        0,
        "a deduped spec must create no `jobs` row"
    );
    assert_eq!(
        execution_row_count(&pool, deduped_id).await?,
        0,
        "a deduped spec must create no execution row"
    );
    assert!(execution_row_exists(&pool, survivor_id).await?);

    Ok(())
}

/// AC2: two specs sharing one dedup key in ONE `spawn_all` call collapse to
/// exactly one landed job — the loser reported via `deduped`, not a
/// constraint violation.
#[tokio::test]
async fn dedup_key_intra_batch_collapses_to_one() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-intra-batch");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let key = unique("shared-key");
    let first_id = JobId::new();
    let second_id = JobId::new();
    let result = spawner
        .spawn_all(vec![
            JobSpec::new(first_id, Cfg).dedup_key(key.clone()),
            JobSpec::new(second_id, Cfg).dedup_key(key.clone()),
        ])
        .await?;

    assert_eq!(result.jobs.len(), 1, "exactly one of the two must land");
    assert_eq!(
        result.jobs[0].id, first_id,
        "the first-listed spec wins the collapse"
    );
    assert_eq!(result.deduped, vec![second_id]);
    assert_eq!(jobs_row_count(&pool, second_id).await?, 0);
    assert_eq!(execution_row_count(&pool, second_id).await?, 0);
    assert!(execution_row_exists(&pool, first_id).await?);

    Ok(())
}

/// AC3 (respawn half): once the holder goes terminal its execution row is
/// deleted (`dispatcher.rs`), so the key becomes respawnable on the very
/// next `spawn_all` call — no special-casing needed, it falls out of the
/// live-window definition.
#[tokio::test]
async fn dedup_key_becomes_respawnable_after_terminal() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-respawn");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let key = unique("respawn-key");
    let first_id = JobId::new();
    let result = spawner
        .spawn_all(vec![JobSpec::new(first_id, Cfg).dedup_key(key.clone())])
        .await?;
    assert_eq!(result.jobs.len(), 1);
    assert!(result.deduped.is_empty());

    started.notified().await;

    // Still LIVE (running, holding the key): a second spawn no-ops.
    let blocked_id = JobId::new();
    let result = spawner
        .spawn_all(vec![JobSpec::new(blocked_id, Cfg).dedup_key(key.clone())])
        .await?;
    assert!(result.jobs.is_empty());
    assert_eq!(result.deduped, vec![blocked_id]);

    // Release the holder and wait for its execution row to vanish (terminal).
    release.notify_one();
    wait_until(
        || async { Ok(!execution_row_exists(&pool, first_id).await?) },
        "first holder to go terminal",
    )
    .await?;

    // Key is free again: a third spawn lands.
    let respawned_id = JobId::new();
    let result = spawner
        .spawn_all(vec![JobSpec::new(respawned_id, Cfg).dedup_key(key.clone())])
        .await?;
    assert_eq!(result.jobs.len(), 1);
    assert_eq!(result.jobs[0].id, respawned_id);
    assert!(result.deduped.is_empty());

    Ok(())
}

/// AC3 (swap-path half): a retry's `UPDATE ... SET state = 'pending'`
/// touches the SAME row rather than deleting and reinserting it, so
/// `unique_key` survives mid-lifecycle without any dedicated carry-forward
/// code. Pins the `dispatcher.rs`/`poller.rs` audit from the PR body with a
/// live assertion rather than just a grep.
#[tokio::test]
async fn dedup_key_survives_a_retry() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-retry");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 1,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings {
            n_attempts: Some(5),
            n_warn_attempts: None,
            min_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_jitter_pct: 0,
            attempt_reset_after_backoff_multiples: 100,
        },
    });
    jobs.start_poll().await?;

    let key = unique("retry-key");
    let id = JobId::new();
    let result = spawner
        .spawn_all(vec![JobSpec::new(id, Cfg).dedup_key(key.clone())])
        .await?;
    assert_eq!(result.jobs.len(), 1);

    // Wait for the first (failing) attempt to be recorded, i.e. the retry
    // reschedule write has happened -- the SAME row, per `dispatcher.rs`.
    wait_until(
        || async { Ok(attempt_index(&pool, id).await? >= 2) },
        "retry to reschedule the row",
    )
    .await?;
    assert_eq!(
        execution_row_unique_key(&pool, id).await?,
        Some(key.clone()),
        "a retry's reschedule must not clear unique_key"
    );

    // A concurrent spawn against the same key must still see it live.
    let blocked_id = JobId::new();
    let result = spawner
        .spawn_all(vec![JobSpec::new(blocked_id, Cfg).dedup_key(key.clone())])
        .await?;
    assert!(result.jobs.is_empty());
    assert_eq!(result.deduped, vec![blocked_id]);

    release.notify_one();
    Ok(())
}

/// A dedup-keyed spec that ALSO carries a `queue_id` must still respect
/// queue exclusion (park behind an existing occupant) when its key is free
/// -- dedup and queue-exclusion are independent constraints on the same
/// row, and only a LIVE-key collision should ever suppress a row outright.
#[tokio::test]
async fn dedup_key_composes_with_queue_id_park_or_take() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-plus-queue");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let queue = unique("dedup-queue");
    let occupant_id = JobId::new();
    spawner
        .spawn_all(vec![JobSpec::new(occupant_id, Cfg).queue_id(queue.clone())])
        .await?;
    started.notified().await;
    assert_eq!(execution_row_state(&pool, occupant_id).await?, "running");

    let key = unique("dedup-plus-queue-key");
    let parked_id = JobId::new();
    let result = spawner
        .spawn_all(vec![
            JobSpec::new(parked_id, Cfg)
                .queue_id(queue.clone())
                .dedup_key(key.clone()),
        ])
        .await?;

    assert_eq!(
        result.jobs.len(),
        1,
        "a free key must never be suppressed by queue occupancy"
    );
    assert_eq!(execution_row_state(&pool, parked_id).await?, "parked");
    assert_eq!(
        execution_row_unique_key(&pool, parked_id).await?,
        Some(key),
        "a parked row must still carry its dedup key"
    );

    release.notify_one();
    Ok(())
}

/// Regression guard: a batch mixing dedup-keyed and keyless specs must leave
/// the keyless specs byte-for-byte unaffected -- no cross-contamination
/// from the `deduped` CTE's `COALESCE(unique_key, id::text)` fallback.
#[tokio::test]
async fn dedup_key_batch_leaves_keyless_specs_unaffected() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-mixed-batch");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let keyless_ids: Vec<JobId> = (0..5).map(|_| JobId::new()).collect();
    let keyed_id = JobId::new();
    let mut specs: Vec<JobSpec<Cfg>> = keyless_ids
        .iter()
        .map(|id| JobSpec::new(*id, Cfg))
        .collect();
    specs.push(JobSpec::new(keyed_id, Cfg).dedup_key(unique("mixed-batch-key")));

    let result = spawner.spawn_all(specs).await?;
    assert_eq!(result.jobs.len(), 6);
    assert!(result.deduped.is_empty());
    for id in keyless_ids {
        assert!(execution_row_exists(&pool, id).await?);
    }
    assert!(execution_row_exists(&pool, keyed_id).await?);

    Ok(())
}

/// AC5: two processes bulk-spawning the same, previously-unheld dedup key
/// simultaneously must resolve to exactly one landing and no statement
/// abort surfaced to either caller. Simulated the same way
/// `lock_ordering.rs::concurrent_multi_queue_spawns_do_not_deadlock` does --
/// two `tokio::join!`ed `spawn_all` calls sharing one pool.
#[tokio::test]
async fn concurrent_bulk_spawn_same_dedup_key_exactly_one_lands() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("dedup-concurrent");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt.clone(),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    for _ in 0..20 {
        let key = unique("race-key");
        let a_id = JobId::new();
        let b_id = JobId::new();

        let (ra, rb) = tokio::join!(
            spawner.spawn_all(vec![JobSpec::new(a_id, Cfg).dedup_key(key.clone())]),
            spawner.spawn_all(vec![JobSpec::new(b_id, Cfg).dedup_key(key.clone())])
        );
        let ra = ra.expect("no statement-abort must ever surface to the caller");
        let rb = rb.expect("no statement-abort must ever surface to the caller");

        let landed = ra.jobs.len() + rb.jobs.len();
        let deduped = ra.deduped.len() + rb.deduped.len();
        assert_eq!(landed, 1, "exactly one of the two concurrent spawns lands");
        assert_eq!(deduped, 1, "the other must be reported, not silently lost");

        let live_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE job_type = $1 AND unique_key = $2",
        )
        .bind(jt.as_str())
        .bind(&key)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            live_count, 1,
            "at most one live row per key, even under a race"
        );
    }

    Ok(())
}

/// `JobSpawner::spawn_spec` -- the single-spawn entry point every other
/// `spawn*` convenience method now delegates to -- honors `dedup_key`
/// exactly like `spawn_all` does: `Some(job)` for a free key.
#[tokio::test]
async fn spawn_spec_without_dedup_key_creates_a_job() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("spawn-spec-plain");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt,
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    let job = spawner
        .spawn_spec(JobSpec::new(id, Cfg))
        .await?
        .expect("a spec without dedup_key must always create a job");
    assert_eq!(job.id, id);
    assert!(execution_row_exists(&pool, id).await?);

    Ok(())
}

/// AC1 via the single-spawn path: `spawn_spec` against a dedup key already
/// held by a LIVE execution returns `Ok(None)` -- no `jobs` row, no
/// execution row -- mirroring `dedup_key_no_ops_against_a_live_row`'s
/// `spawn_all` case exactly, but for the delegation chain every other
/// `spawn*` convenience method now runs through.
#[tokio::test]
async fn spawn_spec_dedup_key_no_ops_against_a_live_row() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("spawn-spec-dedup-live");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: jt.clone(),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let key = unique("spawn-spec-held-key");
    let holder_id = JobId::new();
    sqlx::query(
        "INSERT INTO jobs (id, unique_key, job_type, created_at) VALUES ($1, $2, $3, NOW())",
    )
    .bind(uuid::Uuid::from(holder_id))
    .bind(&key)
    .bind(jt.as_str())
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, unique_key, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, $3, 'running', 1, NOW(), NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(holder_id))
    .bind(jt.as_str())
    .bind(&key)
    .execute(&pool)
    .await?;

    let deduped_id = JobId::new();
    let result = spawner
        .spawn_spec(JobSpec::new(deduped_id, Cfg).dedup_key(key.clone()))
        .await?;
    assert!(
        result.is_none(),
        "spawn_spec must report None for a key already held live"
    );
    assert_eq!(jobs_row_count(&pool, deduped_id).await?, 0);
    assert_eq!(execution_row_count(&pool, deduped_id).await?, 0);

    Ok(())
}

/// Regression for a Cursor Bugbot finding on PR #185: `insert_many`'s
/// cross-call dedup backstop (`DISTINCT ON (COALESCE(unique_key,
/// id::text))`) was NOT `job_type`-qualified, even though the live index it
/// backstops (`idx_job_executions_job_type_unique_key`) is `(job_type,
/// unique_key)`. Two DIFFERENT job types sharing one dedup_key STRING in
/// one `op` -- e.g. facility-scoped cross-type work using the facility id
/// as the key -- collapsed to one execution row even though the index would
/// happily hold both. Worse than a lost row: BOTH `spawn_spec_in_op` calls
/// report `Ok(Some(job))` (each passes its own job_type-scoped live-check
/// independently, before either row reaches the merged insert), so the
/// caller believes both spawned -- a silent, corrupting false success plus
/// an orphan `jobs` row for whichever lost the collapse.
#[tokio::test]
async fn dedup_key_cross_call_collapse_is_scoped_by_job_type() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt_a = job_type("dedup-cross-type-a");
    let jt_b = job_type("dedup-cross-type-b");
    let spawner_a = jobs.add_initializer(HoldableInitializer {
        job_type: jt_a,
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    let spawner_b = jobs.add_initializer(HoldableInitializer {
        job_type: jt_b,
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    // Same dedup_key STRING, different job_type -- the exact pattern the
    // index (job_type, unique_key) is scoped to permit.
    let key = unique("cross-type-key");
    let id_a = JobId::new();
    let id_b = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    let a = spawner_a
        .spawn_spec_in_op(&mut op, JobSpec::new(id_a, Cfg).dedup_key(key.clone()))
        .await?;
    let b = spawner_b
        .spawn_spec_in_op(&mut op, JobSpec::new(id_b, Cfg).dedup_key(key.clone()))
        .await?;
    op.commit().await?;

    assert!(
        a.is_some(),
        "job_type A's own per-type live-check must pass -- the key is free under A"
    );
    assert!(
        b.is_some(),
        "job_type B's own per-type live-check must pass -- the key is free under B"
    );

    assert!(
        execution_row_exists(&pool, id_a).await?,
        "job_type A's execution row must exist -- a different job_type sharing the \
         dedup_key string must never collapse against it"
    );
    assert!(
        execution_row_exists(&pool, id_b).await?,
        "job_type B's execution row must exist -- a different job_type sharing the \
         dedup_key string must never collapse against it"
    );
    // The orphan check matters as much as the count: a `jobs` row with no
    // matching execution row is exactly the silent false-success state the
    // finding warns about.
    assert_eq!(jobs_row_count(&pool, id_a).await?, 1);
    assert_eq!(jobs_row_count(&pool, id_b).await?, 1);

    Ok(())
}
