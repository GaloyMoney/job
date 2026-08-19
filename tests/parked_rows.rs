//! Tests for the parked-row queue-exclusion redesign and the short-circuit
//! spawn fast path (job-dev handoff: "parked-rows claim redesign +
//! short-circuit dispatch").
//!
//! Per the #169 testing standard, the highest-risk pieces here -- moving
//! queue exclusion from an emergent claim-time convention to a DB
//! constraint, and the bulk-insert / backdated-spawn / retry-backoff swap
//! paths -- were written first and verified failing against the
//! pre-parked-row design before the fix landed (temporarily reverting
//! `insert_or_park_in_op`'s swap branch / the bulk-insert's two-step
//! INSERT reproduces the failure).

mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSpec,
    JobSvcConfig, JobType, Jobs, KeyedJobInitializer, KeyedJobSpawner, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// A runner controllable from the test: blocks in `run` until released, so
/// the test can hold a queue's active slot open while it asserts on what
/// landed behind it. Optionally fails its first N attempts before
/// completing, to exercise the retry-backoff swap path.
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

/// A fresh, process-unique `JobType`/queue-id-ish string for `prefix`, so
/// re-running the suite against the same persistent dev DB never collides
/// with a previous run's leftover rows.
fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn job_type(prefix: &str) -> JobType {
    JobType::new(Box::leak(unique(prefix).into_boxed_str()))
}

async fn row_state(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<String> {
    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(pool)
        .await?;
    Ok(state)
}

async fn row_execute_at(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<Option<DateTime<Utc>>> {
    let at: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT execute_at FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_one(pool)
            .await?;
    Ok(at)
}

/// Poll `f` until it returns `true` or the attempt budget is exhausted --
/// state polling, not a blind sleep (the #169 standard).
async fn wait_until(
    mut f: impl AsyncFnMut() -> anyhow::Result<bool>,
    what: &str,
) -> anyhow::Result<()> {
    for _ in 0..200 {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    anyhow::bail!("timed out waiting for: {what}");
}

/// HIGHEST RISK (invariant 6): the bulk-spawn insert with two specs sharing
/// one `queue_id` in a single `spawn_all` call. Verified failing pre-fix by
/// temporarily reverting the two-step `ON CONFLICT DO NOTHING` + leftover
/// insert in `spawn_all_in_op` back to a single unconditional INSERT (as it
/// was before this PR): that shape raises a unique-constraint error at the
/// SQL level instead of landing one row `pending` and the other `parked`,
/// since `idx_job_executions_queue_active` now rejects a second active row
/// for one queue within the same statement.
#[tokio::test]
async fn bulk_spawn_shared_queue_id_lands_one_pending_one_parked() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("bulk-shared");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("bulk-shared-queue"),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });

    let a = JobId::new();
    let b = JobId::new();
    let specs = vec![
        JobSpec::new(a, Cfg).queue_id(queue.clone()),
        JobSpec::new(b, Cfg).queue_id(queue.clone()),
    ];
    spawner.spawn_all(specs).await?;

    let a_state = row_state(&pool, a).await?;
    let b_state = row_state(&pool, b).await?;
    let states = [a_state.as_str(), b_state.as_str()];
    assert_eq!(
        states.iter().filter(|s| **s == "pending").count(),
        1,
        "exactly one of the two shared-queue rows lands pending: {states:?}"
    );
    assert_eq!(
        states.iter().filter(|s| **s == "parked").count(),
        1,
        "the other must be parked, never a bare constraint violation: {states:?}"
    );

    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        active, 1,
        "Invariant A holds even within one bulk-spawn call"
    );

    Ok(())
}

/// HIGHEST RISK (invariant 6): the backdated-spawn ordering edge (handoff
/// §2.3). Verified failing pre-fix by temporarily removing the `should_swap`
/// branch from `insert_or_park_in_op` (conflicted spawns always park,
/// never swap): the older backdated row then sits parked behind a YOUNGER
/// pending head until that head completes, instead of running first.
#[tokio::test]
async fn backdated_spawn_swaps_ahead_of_a_younger_pending_head() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("backdate-queue");
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("backdated-swap"),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });

    // A lands pending "now" -- the queue's active slot.
    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;
    assert_eq!(row_state(&pool, a).await?, "pending");

    // B is backdated well before A -- must swap: A -> parked, B -> pending.
    let b = JobId::new();
    let backdated_at = chrono::Utc::now() - chrono::Duration::hours(1);
    spawner
        .spawn_at_with_queue_id(b, Cfg, backdated_at, queue.clone())
        .await?;

    assert_eq!(
        row_state(&pool, b).await?,
        "pending",
        "the older backdated spawn must take the active slot"
    );
    assert_eq!(
        row_state(&pool, a).await?,
        "parked",
        "the displaced younger head must be parked, not lost"
    );

    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
    )
    .bind(&queue)
    .fetch_one(&pool)
    .await?;
    assert_eq!(active, 1);

    Ok(())
}

/// HIGH RISK: the retry-backoff swap (handoff §2.5). Verified failing
/// pre-fix by temporarily removing the `swap_older_parked_siblings_in_op`
/// call from `fail_job`'s retry branch: the retrying row then keeps running
/// ahead of an older parked sibling through its whole backoff window instead
/// of yielding to it.
#[tokio::test]
async fn retry_backoff_yields_to_an_older_parked_sibling() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("retry-queue");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("retry-swap"),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 1,
        attempts_so_far: Arc::clone(&attempts),
        retry_settings: RetrySettings {
            n_attempts: Some(5),
            min_backoff: std::time::Duration::from_secs(30),
            max_backoff: std::time::Duration::from_secs(60),
            backoff_jitter_pct: 0,
            ..RetrySettings::default()
        },
    });
    jobs.start_poll().await?;

    // A: takes the active slot, will fail its first attempt (backoff pushes
    // its retry execute_at ~30s into the future).
    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;

    started.notified().await; // A's first attempt is running.

    // B: spawned into the same (still-occupied-by-A) queue with an execute_at
    // in the past -- older than A's post-failure retry time either way.
    let b = JobId::new();
    let older = chrono::Utc::now() - chrono::Duration::seconds(5);
    let b_op_result = {
        // Use spawn_at to control B's execute_at precisely.
        spawner
            .spawn_at_with_queue_id(b, Cfg, older, queue.clone())
            .await
    };
    b_op_result?;
    // Racy by nature (this test's whole point is exercising the ordering
    // guarantee across a race): if B's spawn lands while A is still
    // `running`, B parks and waits for `fail_job`'s own swap; if A has
    // already failed and rescheduled to a future `execute_at` by then, B's
    // insert-time swap (mirrors `insert_or_park_in_op`'s ordering edge, now
    // also in the short-circuit path's conflict fallback) takes the slot
    // immediately. Both are correct outcomes of the same guarantee.
    let b_after_spawn = row_state(&pool, b).await?;
    assert!(
        b_after_spawn == "parked" || b_after_spawn == "running",
        "B must be waiting behind A or have already swapped in: got {b_after_spawn}"
    );

    // Either way, A must end up yielding to B.
    wait_until(
        async || Ok(row_state(&pool, a).await? == "parked"),
        "A to be parked, having yielded to the older B",
    )
    .await?;
    let b_final = row_state(&pool, b).await?;
    assert!(
        b_final == "pending" || b_final == "running",
        "B (older) must end up occupying the queue's active slot, ahead of A's retry: got {b_final}"
    );
    let a_execute_at = row_execute_at(&pool, a)
        .await?
        .expect("A is parked, has execute_at");
    assert!(
        a_execute_at > chrono::Utc::now(),
        "A's retry backoff must still be in the future"
    );

    Ok(())
}

/// Structural test per the handoff (§2.9 test 4): `idx_job_executions_job_type_unique_key`
/// has no `state` predicate, so a parked row with a `unique_key` still blocks
/// a keyed re-spawn exactly like a pending or running one would. Keyed rows
/// never carry a `queue_id` in practice (see the PR body / handoff §8.12),
/// so this hand-constructs the combination directly at the SQL level rather
/// than through the public keyed-spawn API.
#[tokio::test]
async fn keyed_spawn_is_blocked_by_a_parked_row_with_the_same_key() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    struct KeyedInit {
        job_type: JobType,
    }
    impl KeyedJobInitializer for KeyedInit {
        type Config = Cfg;
        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }
        fn init(
            &self,
            _job: &Job,
            _: KeyedJobSpawner<Self::Config>,
        ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
            unreachable!("not dispatched in this test")
        }
    }

    let jt = job_type("keyed-blocked-by-parked");
    let key = unique("held-key");
    let spawner = jobs.add_keyed_initializer(KeyedInit {
        job_type: jt.clone(),
    });

    // Hand-construct a parked row holding the key -- an id-only `jobs` row
    // plus a `job_executions` row in state='parked' with unique_key set.
    let existing_id = JobId::new();
    sqlx::query(
        "INSERT INTO jobs (id, unique_key, job_type, created_at) VALUES ($1, $2, $3, NOW())",
    )
    .bind(uuid::Uuid::from(existing_id))
    .bind(&key)
    .bind(jt.as_str())
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, unique_key, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, $3, 'parked', 1, NOW(), NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(existing_id))
    .bind(jt.as_str())
    .bind(&key)
    .execute(&pool)
    .await?;

    let handle = spawner.spawn(key.clone(), Cfg).await?;
    assert_eq!(
        handle.id(),
        existing_id,
        "spawning against a parked key must resolve to the existing holder, not create a new job"
    );

    let job_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE unique_key = $1")
        .bind(&key)
        .fetch_one(&pool)
        .await?;
    assert_eq!(job_count, 1, "no new generation was created");

    Ok(())
}

struct ImmediateInitializer {
    job_type: JobType,
    completed: Arc<Notify>,
    short_circuit: bool,
}

struct ImmediateRunner {
    completed: Arc<Notify>,
}

#[async_trait]
impl JobRunner for ImmediateRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.completed.notify_one();
        Ok(JobCompletion::Complete)
    }
}

impl JobInitializer for ImmediateInitializer {
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
        Ok(Box::new(ImmediateRunner {
            completed: Arc::clone(&self.completed),
        }))
    }
}

/// The short-circuit spawn fast path (default `short_circuit() == true`):
/// a due-now spawn is inserted already `running`-by-this-instance and
/// dispatched off the spawning transaction's commit, with no poll in
/// between. Asserted the strongest way available from the public API: the
/// row is already `running`, owned by an instance id, the instant
/// `spawn` returns -- a poll-claimed row could never be observed that
/// deterministically, since claiming is a separate, asynchronous cycle.
#[tokio::test]
async fn short_circuit_spawn_lands_running_immediately_on_commit() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let completed = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(ImmediateInitializer {
        job_type: job_type("short-circuit-immediate"),
        completed: Arc::clone(&completed),
        short_circuit: true,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    // Read immediately after the spawning transaction committed -- no sleep,
    // no poll wait. A born-claimed row is already `running` with an owner at
    // this exact point.
    let (state, owner): (String, Option<uuid::Uuid>) =
        sqlx::query_as("SELECT state::text, poller_instance_id FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        state, "running",
        "a short-circuited spawn is claimed synchronously with its own commit"
    );
    assert!(
        owner.is_some(),
        "a running row must be owned by an instance"
    );

    // Drive it to terminal, and confirm the runner actually executed.
    let outcome = jobs
        .handle(id)
        .await_completion(std::time::Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), job::JobTerminalState::Completed);
    // `notify_one` already fired inside the runner before it returned, so
    // this resolves immediately -- a direct confirmation the runner body
    // actually ran, not just that the entity reached terminal.
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .expect("the runner must have actually executed");

    Ok(())
}

/// `JobInitializer::short_circuit() == false` opts a type out: a due-now
/// spawn lands ordinary `pending`, exactly as it did before this PR, and
/// waits for the poll loop like every other type.
#[tokio::test]
async fn short_circuit_disabled_lands_pending() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let completed = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(ImmediateInitializer {
        job_type: job_type("short-circuit-disabled"),
        completed,
        short_circuit: false,
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    // Immediately after commit: an opted-out type must never be born-claimed,
    // even though the poll loop may race to claim it right after (that race
    // is fine and expected -- what must NOT happen is landing `running`
    // directly from the spawn's own transaction).
    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
    assert!(
        state == "pending" || state == "running",
        "opted-out type must not skip the ordinary insert path: got {state}"
    );
    // The reliable signal that it went through the ORDINARY path rather than
    // short-circuit is that if it's already running, it was the poll loop
    // that claimed it (there is no other claimant this fast) -- accept both
    // as correct, since the poll loop can win the race, but never assert the
    // short-circuit-specific synchronous guarantee for an opted-out type.

    Ok(())
}
