//! Tests for parked-row queue exclusion and the short-circuit spawn fast
//! path: queue exclusion (at most one live row per `queue_id`) is enforced
//! as a database constraint, and a due-now spawn or a completion that frees
//! capacity can claim and dispatch immediately instead of waiting for the
//! next poll.

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
use tokio::sync::{Notify, Semaphore};

/// Holds a runner inside `run` until the test opens the gate.
///
/// An execution row exists iff its job is pending/parked/running -- it is
/// DELETED on terminal. So any test that spawns an instantly-completing job
/// and then reads that row is racing its own job: claim -> dispatch -> run
/// -> complete -> delete can finish before the read, and the read then
/// fails with "no rows returned by a query that expected to return at least
/// one row" rather than any assertion. The window is exactly the
/// claim-to-completion latency, so every improvement to the claim path
/// (#177's type-leading index, #180's `enable_seqscan` pin) narrows it;
/// #180 narrowed it enough to turn this into a reproducible CI failure.
/// Gating the runner removes the race outright instead of widening the
/// window again: the row cannot reach terminal, so it cannot be deleted,
/// so the assertion reads exactly the state the code under test produced.
///
/// A `Semaphore` rather than the `Notify` handshake `HoldableRunner` uses:
/// these types can have more than one row in flight, and permits added
/// BEFORE a runner parks still release it, so a dispatch that lands late
/// cannot strand a test's `shutdown()`.
type Gate = Arc<Semaphore>;

fn closed_gate() -> Gate {
    Arc::new(Semaphore::new(0))
}

fn open(gate: &Gate) {
    gate.add_permits(1024);
}

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
/// state polling, not a blind sleep.
async fn wait_until(
    mut f: impl AsyncFnMut() -> anyhow::Result<bool>,
    what: &str,
) -> anyhow::Result<()> {
    // 800 x 25ms = 20s: several tests in this file race a spawn's
    // insert-time swap against a retry's own swap, and a contended CI
    // runner can push those races close to a tighter budget even though
    // the invariant itself holds.
    for _ in 0..800 {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    anyhow::bail!("timed out waiting for: {what}");
}

/// A bulk `spawn_all` call with two specs sharing one `queue_id` must
/// resolve the conflict between them: exactly one lands `pending`, the
/// other `parked` -- never a raw unique-constraint violation.
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

/// A spawn backdated well before a queue's current `pending` head must
/// swap ahead of it -- take the active slot itself, parking the younger
/// head instead of queuing behind it.
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

    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;
    assert_eq!(row_state(&pool, a).await?, "pending");

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

/// A row scheduled for retry backoff must yield its queue's active slot to
/// an older parked sibling, rather than keep it through the whole backoff
/// window.
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

    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;

    started.notified().await;

    let b = JobId::new();
    let older = chrono::Utc::now() - chrono::Duration::seconds(5);
    let b_op_result = spawner
        .spawn_at_with_queue_id(b, Cfg, older, queue.clone())
        .await;
    b_op_result?;
    // Racy by construction: if B's spawn lands while A is still running, B
    // parks and waits for the retry's own swap; if A has already failed and
    // rescheduled by then, B's insert-time swap takes the slot immediately.
    // Both are correct.
    let b_after_spawn = row_state(&pool, b).await?;
    assert!(
        b_after_spawn == "parked" || b_after_spawn == "running",
        "B must be waiting behind A or have already swapped in: got {b_after_spawn}"
    );

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

/// `idx_job_executions_job_type_unique_key` has no `state` predicate, so a
/// `parked` row with a `unique_key` still blocks a keyed re-spawn exactly
/// like a `pending` or `running` one would. Keyed rows never carry a
/// `queue_id` in practice, so this hand-constructs the combination directly
/// at the SQL level rather than through the public keyed-spawn API.
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
    /// `Some` for every test that asserts on this type's execution row:
    /// see [`Gate`]. `None` completes immediately, as the name suggests.
    gate: Option<Gate>,
}

struct ImmediateRunner {
    completed: Arc<Notify>,
    gate: Option<Gate>,
}

#[async_trait]
impl JobRunner for ImmediateRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Before the gate, so `completed` still proves the runner body ran
        // even while it is being held.
        self.completed.notify_one();
        if let Some(gate) = &self.gate {
            let _permit = gate.acquire().await?;
        }
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
            gate: self.gate.clone(),
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
    let gate = closed_gate();
    let spawner = jobs.add_initializer(ImmediateInitializer {
        job_type: job_type("short-circuit-immediate"),
        completed: Arc::clone(&completed),
        short_circuit: true,
        gate: Some(Arc::clone(&gate)),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

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

    // The row has been observed; let it reach terminal (and be deleted).
    open(&gate);
    let outcome = jobs
        .handle(id)
        .await_completion(std::time::Duration::from_secs(10))
        .await?;
    assert_eq!(outcome.state(), job::JobTerminalState::Completed);
    // Resolves immediately: `completed` was notified before the runner
    // returned, confirming the runner body actually executed.
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .expect("the runner must have actually executed");

    Ok(())
}

/// `JobInitializer::short_circuit() == false` opts a type out: a due-now
/// spawn lands ordinary `pending` and waits for the poll loop like every
/// other type.
#[tokio::test]
async fn short_circuit_disabled_lands_pending() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let completed = Arc::new(Notify::new());
    let gate = closed_gate();
    let spawner = jobs.add_initializer(ImmediateInitializer {
        job_type: job_type("short-circuit-disabled"),
        completed,
        short_circuit: false,
        gate: Some(Arc::clone(&gate)),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    spawner.spawn(id, Cfg).await?;

    let state: String = sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
    assert!(
        state == "pending" || state == "running",
        "opted-out type must not skip the ordinary insert path: got {state}"
    );
    // The poll loop may independently win a race to claim it right after
    // commit; what matters is that it never lands `running` directly from
    // the spawn's own transaction, the way a short-circuited type would.
    // `running` is only reachable here via that claim, which is why the
    // gate above is what keeps this readable at all: without it the row is
    // deleted on completion and the read fails outright.
    open(&gate);

    Ok(())
}

/// The short-circuit claim must serve an OLDER due row of the same type
/// before the row a fresh spawn just inserted -- a spawn is never
/// guaranteed to dispatch itself.
#[tokio::test]
async fn spawn_yields_to_an_older_pending_row_of_the_same_type() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let jt = job_type("fairness");
    let gate = closed_gate();
    let spawner = jobs.add_initializer(ImmediateInitializer {
        job_type: jt.clone(),
        completed: Arc::new(Notify::new()),
        short_circuit: true,
        gate: Some(Arc::clone(&gate)),
    });
    jobs.start_poll().await?;

    // Hand-construct an older due `pending` row directly (unqueued, so it
    // can never conflict at insert time) -- an ordinary spawn of an
    // uncapped type always claims itself immediately when nothing else is
    // due, so this is the only way to put backlog in front of it.
    let old_id = JobId::new();
    let old_at = chrono::Utc::now() - chrono::Duration::seconds(30);
    sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, NOW())")
        .bind(uuid::Uuid::from(old_id))
        .bind(jt.as_str())
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, 'pending', 1, $3, NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(old_id))
    .bind(jt.as_str())
    .bind(old_at)
    .execute(&pool)
    .await?;

    let new_id = JobId::new();
    spawner.spawn(new_id, Cfg).await?;

    // Read both rows in ONE query so the two states come from one snapshot.
    let (old_state, new_state): (String, String) = sqlx::query_as(
        "SELECT \
           (SELECT state::text FROM job_executions WHERE id = $1), \
           (SELECT state::text FROM job_executions WHERE id = $2)",
    )
    .bind(uuid::Uuid::from(old_id))
    .bind(uuid::Uuid::from(new_id))
    .fetch_one(&pool)
    .await?;
    // `old_state` carries the whole fairness claim, and carries it on its
    // own: the spawn's transaction claims exactly one unit for the one row
    // it added, and this asserts that the unit went to the OLDER row rather
    // than to `new_id`. The violation this guards against -- the spawn
    // dispatching itself past older backlog -- necessarily leaves `old_id`
    // `pending`, so it cannot hide behind a passing assertion here.
    //
    // What is deliberately NOT asserted is `new_state == "pending"`. That
    // used to be here and was the source of this test's flakiness, without
    // fairness ever having broken: having correctly claimed the older row,
    // the spawn's commit legitimately emits `execution_ready` for the type
    // (`new_id` was added and NOT claimed), the poll loop wakes on it, and
    // claims `new_id` on a later pass. Both rows end up `running` and the
    // old assertion read red on a system that did exactly the right thing.
    // Capping the type's concurrency does not close the window either --
    // units are counted at DISPATCH, after the claiming transaction commits,
    // so a poll landing in between still sees a free slot.
    assert_eq!(
        old_state, "running",
        "the OLDER due row must be the one the spawn's own claim served \
         (new_id is {new_state})"
    );
    // Both rows are read as scalar subqueries, so a deleted row would
    // arrive as NULL and fail to decode into `String` rather than as a
    // missing row -- same race, different symptom. The gate keeps both
    // alive for the snapshot.
    open(&gate);

    Ok(())
}

/// Completing A must not just promote its queue's parked sibling B to
/// `pending` -- the completion-time recycle claim must dispatch it
/// synchronously, in the same transaction, with no poll needed in between.
#[tokio::test]
async fn completion_recycles_into_a_promoted_sibling_with_no_poll_needed() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("chain-hop-queue");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("chain-hop"),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;
    started.notified().await;
    assert_eq!(row_state(&pool, a).await?, "running");

    let b = JobId::new();
    spawner.spawn_with_queue_id(b, Cfg, queue.clone()).await?;
    assert_eq!(
        row_state(&pool, b).await?,
        "parked",
        "B must park behind A's active slot"
    );

    release.notify_one();

    wait_until(
        async || Ok(row_state(&pool, b).await? == "running"),
        "B to be recycled straight to running once A completes",
    )
    .await?;

    let a_gone: i64 = sqlx::query_scalar("SELECT count(*) FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(a))
        .fetch_one(&pool)
        .await?;
    assert_eq!(a_gone, 0, "A must be deleted on completion");
    let b_owner: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT poller_instance_id FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(b))
            .fetch_one(&pool)
            .await?;
    assert!(
        b_owner.is_some(),
        "a recycled claim must be owned by an instance, byte-identical to a poll claim"
    );

    // Drive B to completion too, to confirm the recycled dispatch is a
    // real dispatcher, not just a state flip.
    started.notified().await;
    release.notify_one();
    wait_until(
        async || {
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM job_executions WHERE id = $1")
                    .bind(uuid::Uuid::from(b))
                    .fetch_one(&pool)
                    .await?;
            Ok(remaining == 0)
        },
        "B to complete",
    )
    .await?;

    Ok(())
}

/// A completion during an in-flight shutdown drain must NOT recycle into
/// new work -- doing so would re-admit a dispatch after the drain has
/// started collecting acks, and a task spawned that late never subscribes
/// to the shutdown broadcast in time, so it gets force-aborted instead of
/// drained.
#[tokio::test]
async fn completion_during_shutdown_does_not_recycle_into_new_work() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("shutdown-recycle-queue");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("shutdown-recycle"),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let a = JobId::new();
    spawner.spawn_with_queue_id(a, Cfg, queue.clone()).await?;
    started.notified().await;
    assert_eq!(row_state(&pool, a).await?, "running");

    let b = JobId::new();
    spawner.spawn_with_queue_id(b, Cfg, queue.clone()).await?;
    assert_eq!(row_state(&pool, b).await?, "parked");

    let shutdown_task = tokio::spawn(async move { jobs.shutdown().await });
    // Calibration only: give the shutdown task a moment to actually flip
    // its "started" flag before A is released. Every assertion below is
    // state-based, taken only after `shutdown_task` is fully awaited.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    release.notify_one();

    shutdown_task
        .await?
        .expect("shutdown must drain A and return cleanly");

    // If the recycle were not gated on shutdown, B would already be
    // `running`; it must instead sit where the ordinary promote leaves it.
    assert_eq!(
        row_state(&pool, b).await?,
        "pending",
        "a completion during shutdown must promote its sibling but not \
         recycle-dispatch it"
    );
    let b_owner: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT poller_instance_id FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(b))
            .fetch_one(&pool)
            .await?;
    assert!(
        b_owner.is_none(),
        "B must not have been claimed by the shutting-down instance"
    );

    Ok(())
}

/// A short-circuit-dispatched execution's shutdown-coordination
/// `broadcast::Receiver`s must be subscribed BEFORE the claiming
/// transaction commits, not inside the task spawned to run it afterward. A
/// late subscribe races the shutdown broadcast -- `tokio::sync::broadcast`
/// never delivers to a subscriber that arrives after `send` -- and a
/// dispatch that loses that race is never acked, never waited for, and gets
/// force-aborted mid-flight instead of drained.
#[tokio::test]
async fn short_circuit_spawn_dispatch_survives_a_shutdown_race() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("shutdown-race"),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let id = JobId::new();
    // No gap between spawn and the shutdown race -- deliberately stressing
    // the commit-to-subscribe window.
    spawner.spawn(id, Cfg).await?;
    started.notified().await;

    let shutdown_task = tokio::spawn(async move { jobs.shutdown().await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    release.notify_one();

    shutdown_task
        .await?
        .expect("shutdown must drain the short-circuited execution, not abort it");

    let aborted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_events WHERE id = $1 AND event->>'type' = 'execution_aborted'",
    )
    .bind(uuid::Uuid::from(id))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        aborted, 0,
        "the short-circuited execution must be drained, not force-aborted"
    );

    Ok(())
}

/// Seed a `running` execution row (plus the `jobs` row its FK needs) holding
/// `queue`'s active slot — the occupant a racing spawn parks behind.
async fn seed_running_occupant(
    pool: &sqlx::PgPool,
    job_type: &str,
    queue: &str,
) -> anyhow::Result<JobId> {
    let id = JobId::new();
    let uuid = uuid::Uuid::from(id);
    sqlx::query("INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, NOW())")
        .bind(uuid)
        .bind(job_type)
        .bind(queue)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, \
          poller_instance_id, created_at) \
         VALUES ($1, $2, $3, 'running', 1, NULL, NOW(), gen_random_uuid(), NOW())",
    )
    .bind(uuid)
    .bind(job_type)
    .bind(queue)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Pins the lock-strength choice made by `ExecutionInsertHook::
/// lock_queue_occupants` against the real schema, since the whole cost
/// argument for taking a spawn-side lock at all rests on it: `FOR KEY SHARE`
/// must block a completion's `DELETE` and nothing else.
///
/// The `state` probe is the interesting one. `state` appears in
/// `idx_job_executions_queue_active`'s PREDICATE, so it would be reasonable
/// to guess Postgres treats it as a key column and blocks retry / reschedule
/// / reclaim / promote too. It does not — key columns are the indexed
/// columns themselves — and if that ever changed, the spawn lock would start
/// blocking far more than intended. Hence a test rather than a comment.
#[tokio::test]
async fn key_share_blocks_only_the_delete() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let queue = unique("keyshare-matrix");
    let id = seed_running_occupant(&pool, &unique("keyshare"), &queue).await?;
    let uuid = uuid::Uuid::from(id);

    // Probe `sql` from a second connection while a `FOR KEY SHARE` holder is
    // open, and report whether it had to wait for that holder.
    async fn blocked_by_key_share(
        pool: &sqlx::PgPool,
        queue: &str,
        sql: &str,
        uuid: uuid::Uuid,
    ) -> anyhow::Result<bool> {
        let mut holder = pool.begin().await?;
        sqlx::query(
            "SELECT id FROM job_executions WHERE queue_id = $1 \
             AND state IN ('pending','running') ORDER BY id FOR KEY SHARE",
        )
        .bind(queue)
        .fetch_all(&mut *holder)
        .await?;

        let mut probe = pool.begin().await?;
        sqlx::query("SET LOCAL lock_timeout = '750ms'")
            .execute(&mut *probe)
            .await?;
        let outcome = sqlx::query(sql).bind(uuid).execute(&mut *probe).await;
        let blocked = match outcome {
            Ok(_) => false,
            Err(e) => {
                let db_err = e
                    .as_database_error()
                    .expect("probe must fail only on a lock timeout");
                assert_eq!(
                    db_err.code().as_deref(),
                    Some("55P03"),
                    "unexpected probe failure: {e}"
                );
                true
            }
        };
        probe.rollback().await?;
        holder.rollback().await?;
        Ok(blocked)
    }

    assert!(
        !blocked_by_key_share(
            &pool,
            &queue,
            "UPDATE job_executions SET alive_at = NOW() WHERE id = $1",
            uuid
        )
        .await?,
        "the keep-alive heartbeat is one bulk statement across every live job on \
         the instance; a spawn must never stall it"
    );
    assert!(
        !blocked_by_key_share(
            &pool,
            &queue,
            "UPDATE job_executions SET state = 'pending' WHERE id = $1",
            uuid
        )
        .await?,
        "`state` sits in idx_job_executions_queue_active's predicate, not its key, \
         so retry/reschedule/reclaim/promote stay unblocked"
    );
    assert!(
        !blocked_by_key_share(
            &pool,
            &queue,
            "SELECT id FROM job_executions WHERE id = $1 FOR KEY SHARE",
            uuid
        )
        .await?,
        "concurrent spawns into one queue must not serialize against each other"
    );
    assert!(
        blocked_by_key_share(
            &pool,
            &queue,
            "DELETE FROM job_executions WHERE id = $1",
            uuid
        )
        .await?,
        "a completion's DELETE is the one operation that can orphan a parked row, \
         and the one this lock exists to make wait"
    );

    Ok(())
}

/// The orphan race, driven for real rather than hand-constructed: a spawn
/// parks behind an occupant that a concurrent completion is deleting right
/// now. The spawn's insert sees the occupant live (the delete is
/// uncommitted), so the row parks; the completion's own promote pass cannot
/// see that parked row, so it promotes nothing. Before the spawn-side lock,
/// this left the queue with a parked backlog and no active row — invisible
/// to the claim scan until the sweep ran, up to `job_lost_interval / 2`
/// later.
///
/// The spawn must now come out the other side `pending`: `lock_queue_occupants`
/// blocks on the uncommitted `DELETE`, and once it commits, EPQ reports the
/// occupant gone and `adopt_orphaned_queues` re-arbitrates the queue in the
/// same transaction.
#[tokio::test]
async fn spawn_racing_a_completion_adopts_the_freed_queue() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("adopt-queue");
    let occupant_type = unique("adopt-occupant");
    let occupant = seed_running_occupant(&pool, &occupant_type, &queue).await?;

    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("adopt-spawn"),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });

    // The completion, held open mid-transaction: the occupant row is deleted
    // but not yet committed, exactly the window the race needs.
    let mut completion = pool.begin().await?;
    sqlx::query("DELETE FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(occupant))
        .execute(&mut *completion)
        .await?;

    let id = JobId::new();
    let spawn = tokio::spawn(async move { spawner.spawn_with_queue_id(id, Cfg, queue).await });

    // Let the spawn reach its lock statement and block there, then release
    // the completion.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!spawn.is_finished(), "the spawn must wait on the occupant");
    completion.commit().await?;

    spawn
        .await?
        .expect("the spawn must succeed, not fail on the vanished occupant");

    assert_eq!(
        row_state(&pool, id).await?,
        "pending",
        "a spawn whose occupant completed underneath it must adopt the freed \
         queue, never commit an orphaned parked row"
    );

    Ok(())
}

/// Two spawns that both lose their occupant in the same window both reach
/// the adopt path. Each can only see its OWN uncommitted parked row, so a
/// naive "promote my own head" would have both of them promote a different
/// row into one queue's active slot — a bare unique-index violation, which
/// would surface as a failed business transaction rather than a delayed job.
///
/// Routing the adopt back through `insert_many`'s `ON CONFLICT` arbiter is
/// what makes this safe: exactly one lands `pending`, the other parks, and
/// neither errors.
#[tokio::test]
async fn concurrent_adopts_of_one_freed_queue_do_not_conflict() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("adopt-race-queue");
    let occupant = seed_running_occupant(&pool, &unique("adopt-race-occ"), &queue).await?;

    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("adopt-race"),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });

    let mut completion = pool.begin().await?;
    sqlx::query("DELETE FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(occupant))
        .execute(&mut *completion)
        .await?;

    let a = JobId::new();
    let b = JobId::new();
    let spawns: Vec<_> = [a, b]
        .into_iter()
        .map(|id| {
            let spawner = spawner.clone();
            let queue = queue.clone();
            tokio::spawn(async move { spawner.spawn_with_queue_id(id, Cfg, queue).await })
        })
        .collect();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    completion.commit().await?;

    for spawn in spawns {
        spawn
            .await?
            .expect("neither adopt may fail on a unique-index violation");
    }

    let states = [row_state(&pool, a).await?, row_state(&pool, b).await?];
    assert_eq!(
        states.iter().filter(|s| *s == "pending").count(),
        1,
        "exactly one adopter takes the freed slot: {states:?}"
    );
    assert_eq!(
        states.iter().filter(|s| *s == "parked").count(),
        1,
        "the loser parks behind it rather than erroring: {states:?}"
    );

    Ok(())
}

/// Await the first `job_events` payload satisfying `pred`, ignoring unrelated
/// traffic from concurrently-running tests. Returns `None` on timeout.
async fn next_matching(
    listener: &mut sqlx::postgres::PgListener,
    within: std::time::Duration,
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

/// Pinning a queue's occupant costs something the other lock probes don't
/// cover: the claim takes `FOR UPDATE SKIP LOCKED`, which DOES conflict with
/// `FOR KEY SHARE`, so a poll running inside a parking spawn's commit tail
/// skips a `pending` occupant it would otherwise have claimed.
///
/// That skip is not self-healing. `poll_jobs`' `min_wait` only considers rows
/// with `execute_at > now`, so a skipped DUE head contributes no
/// `next_due_at`; with `may_have_more` false the type can sleep up to
/// `MAX_WAIT` on work that is sitting right there, and a bare parked row
/// notifies nothing that would wake it.
///
/// So a spawn that parks behind a `pending` occupant must notify that
/// OCCUPANT's type — which is generally not the spawning type, and gets no
/// notify from any other rule in `notify_types`.
#[tokio::test]
async fn parking_behind_a_pending_occupant_wakes_the_occupants_type() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("wake-queue");
    let occupant_type = unique("wake-occupant");

    // A due `pending` occupant, left unclaimed (no poller is started here).
    let occupant = JobId::new();
    sqlx::query("INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, NOW())")
        .bind(uuid::Uuid::from(occupant))
        .bind(&occupant_type)
        .bind(&queue)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, $3, 'pending', 1, NOW(), NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(occupant))
    .bind(&occupant_type)
    .bind(&queue)
    .execute(&pool)
    .await?;

    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("wake-spawn"),
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });

    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen("job_events").await?;

    let id = JobId::new();
    spawner.spawn_with_queue_id(id, Cfg, queue.clone()).await?;
    assert_eq!(
        row_state(&pool, id).await?,
        "parked",
        "the spawn must park behind the pending occupant"
    );

    // One global channel, concurrent tests: only this occupant is evidence.
    let woken = next_matching(
        &mut listener,
        std::time::Duration::from_secs(5),
        |payload| payload["job_type"] == occupant_type.as_str(),
    )
    .await;
    assert!(
        woken.is_some(),
        "parking behind a pending occupant must wake that occupant's type, or a \
         poll that skipped it under SKIP LOCKED sleeps on claimable work"
    );

    Ok(())
}

/// The REVERSE ordering of `spawn_racing_a_completion_adopts_the_freed_queue`:
/// the spawn parks its row and pins the occupant FIRST, and the completion's
/// `DELETE` starts while that pin is held. The `DELETE` blocks and, once the
/// spawn commits, proceeds — but under READ COMMITTED a blocked statement
/// resumes with its ORIGINAL snapshot (EvalPlanQual re-checks nothing beyond
/// the conflicting row itself), in which the freshly committed parked row
/// does not yet exist. A parked-sibling promote folded into that SAME
/// statement as a CTE therefore promotes nothing and orphans the queue until
/// `sweep_orphaned_parked_rows`, up to `job_lost_interval / 2` later. The
/// promote must instead run as its own later statement (the completer's
/// `PromoteHeadsHook` freed-queue registration), whose fresh snapshot sees
/// the row.
///
/// Driven through the real completion path: a holdable occupant job runs on
/// the poller; the spawn side is hand-rolled SQL replicating exactly what
/// `ExecutionInsertHook` emits (parked insert + `FOR KEY SHARE` occupant
/// pin), held open across the occupant's completion.
#[tokio::test]
async fn completion_blocked_on_a_spawn_pin_promotes_the_parked_row() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;

    let queue = unique("pin-first-queue");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let spawner = jobs.add_initializer(HoldableInitializer {
        job_type: job_type("pin-first-occupant"),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        fail_first_n: 0,
        attempts_so_far: Arc::new(AtomicUsize::new(0)),
        retry_settings: RetrySettings::default(),
    });
    jobs.start_poll().await?;

    let occupant = JobId::new();
    spawner
        .spawn_with_queue_id(occupant, Cfg, queue.clone())
        .await?;
    started.notified().await;

    // The spawn side, held open: a parked row plus the occupant pin, the
    // statements `ExecutionInsertHook` runs in a spawning transaction's
    // commit tail. `execute_at` is an hour out so the row, once promoted,
    // stays `pending` (nothing claims it) and the assertion below is not
    // racing the runner.
    let parked = JobId::new();
    let parked_uuid = uuid::Uuid::from(parked);
    let parked_type = unique("pin-first-parked");
    let mut spawn_tx = pool.begin().await?;
    sqlx::query("INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, NOW())")
        .bind(parked_uuid)
        .bind(&parked_type)
        .bind(&queue)
        .execute(&mut *spawn_tx)
        .await?;
    sqlx::query(
        "INSERT INTO job_executions \
         (id, job_type, queue_id, unique_key, state, attempt_index, execute_at, alive_at, created_at) \
         VALUES ($1, $2, $3, NULL, 'parked', 1, NOW() + INTERVAL '1 hour', NOW(), NOW())",
    )
    .bind(parked_uuid)
    .bind(&parked_type)
    .bind(&queue)
    .execute(&mut *spawn_tx)
    .await?;
    sqlx::query(
        "SELECT id FROM job_executions WHERE queue_id = $1 \
         AND state IN ('pending','running') ORDER BY id FOR KEY SHARE",
    )
    .bind(&queue)
    .fetch_all(&mut *spawn_tx)
    .await?;

    // Release the occupant: the dispatcher's completion DELETE now blocks on
    // the pin. Give it time to actually reach and block on it, then commit
    // the spawn.
    release.notify_one();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    spawn_tx.commit().await?;

    // The completion itself must promote the parked row — within the
    // completion transaction, not minutes later via the orphan sweep (the
    // default `job_lost_interval` keeps the sweep far beyond this budget).
    wait_until(
        async || Ok(row_state(&pool, parked).await? == "pending"),
        "the parked row must be promoted by the blocked completion itself",
    )
    .await?;

    Ok(())
}
