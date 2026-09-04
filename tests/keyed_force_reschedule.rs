//! `KeyedJobSpec::force_reschedule`: a respawn against a LIVE key that is
//! parked in the future pulls that key's `execute_at` forward to now, instead
//! of resolving to the holder and doing nothing.
//!
//! Two properties carry the whole feature, and each has a test whose failure
//! mode is deterministic:
//!
//! - `a_held_runner_is_woken_by_a_force_reschedule_respawn` — the point of
//!   the mechanism. A job holding a one-HOUR deadline runs within seconds of
//!   a respawn. Remove the pull-forward and the test cannot pass by luck: it
//!   would have to wait an hour.
//! - `force_reschedule_never_shortens_a_retry_backoff` — the hazard. A keyed
//!   job spawned on every upstream event (lana's price-shock sweeps are the
//!   live shape) must not have its exponential backoff erased by the next
//!   event while it is failing. Drop the `attempt_index <= 1` guard and every
//!   respawn in the burst reports `pulled_forward` and the observed run gaps
//!   stop growing.
//!
//! Waiting is by state polling or by the runner's own channel, never by
//! sleeping for "long enough".

mod helpers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job::{
    CurrentJob, Job, JobCompletion, JobRunner, JobSvcConfig, JobType, Jobs, KeyedJobInitializer,
    KeyedJobSpawner, KeyedJobSpec, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

/// How far ahead the held generations park themselves. Deliberately far
/// beyond any test timeout: nothing but a pull-forward can make such a job
/// run inside this suite.
const HOLD: chrono::TimeDelta = chrono::TimeDelta::hours(1);

/// Runs `Jobs` sees but never has work for. lana's staging registry carries
/// 65 types (36 of them keyed/resident outbox residents), and the wake path
/// has to survive the poller's rotation across a registry that size — a
/// two-type test never exercises the rotation window at all.
const FILLER_TYPES: usize = 40;

// -- runners ----------------------------------------------------------------

/// Holds `HOLD` on its first run, then completes. Every run's start instant
/// goes down `runs`.
struct HoldThenComplete {
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
    n: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for HoldThenComplete {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.ran.send(Utc::now());
        if n == 1 {
            Ok(JobCompletion::RescheduleAt(Utc::now() + HOLD))
        } else {
            Ok(JobCompletion::Complete)
        }
    }
}

/// Always fails, so the type's `RetryPolicy` keeps rescheduling it with a
/// growing backoff. Every run's start instant goes down `runs`.
struct AlwaysFails {
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
}

#[async_trait]
impl JobRunner for AlwaysFails {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let _ = self.ran.send(Utc::now());
        Err("deliberate failure".into())
    }
}

/// Never runs — these types exist only to give the poller a realistically
/// sized registry to rotate over.
struct Idle;

#[async_trait]
impl JobRunner for Idle {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

enum Behaviour {
    HoldThenComplete,
    AlwaysFails,
    Idle,
}

struct Init {
    job_type: JobType,
    behaviour: Behaviour,
    ran: mpsc::UnboundedSender<DateTime<Utc>>,
    retry: RetrySettings,
    n: Arc<AtomicUsize>,
}

impl Init {
    fn new(job_type: &JobType, behaviour: Behaviour) -> Self {
        Self {
            job_type: job_type.clone(),
            behaviour,
            ran: mpsc::unbounded_channel().0,
            retry: RetrySettings::default(),
            n: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reporting_to(mut self, ran: mpsc::UnboundedSender<DateTime<Utc>>) -> Self {
        self.ran = ran;
        self
    }

    fn with_retry(mut self, retry: RetrySettings) -> Self {
        self.retry = retry;
        self
    }
}

impl KeyedJobInitializer for Init {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(match self.behaviour {
            Behaviour::HoldThenComplete => Box::new(HoldThenComplete {
                ran: self.ran.clone(),
                n: Arc::clone(&self.n),
            }) as Box<dyn JobRunner>,
            Behaviour::AlwaysFails => Box::new(AlwaysFails {
                ran: self.ran.clone(),
            }),
            Behaviour::Idle => Box::new(Idle),
        })
    }
}

// -- helpers ----------------------------------------------------------------

/// Registers [`FILLER_TYPES`] idle keyed types alongside the one under test.
fn register_filler(jobs: &mut Jobs) {
    for _ in 0..FILLER_TYPES {
        let job_type = helpers::job_type("force-reschedule-filler");
        let _ = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));
    }
}

async fn row(
    pool: &sqlx::PgPool,
    job_type: &JobType,
    key: &str,
) -> anyhow::Result<Option<(Option<DateTime<Utc>>, i32, String)>> {
    let row: Option<(Option<DateTime<Utc>>, i32, String)> = sqlx::query_as(
        "SELECT execute_at, attempt_index, state::text FROM job_executions
         WHERE job_type = $1 AND unique_key = $2",
    )
    .bind(job_type.as_str())
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

async fn execute_at(
    pool: &sqlx::PgPool,
    job_type: &JobType,
    key: &str,
) -> anyhow::Result<DateTime<Utc>> {
    Ok(row(pool, job_type, key)
        .await?
        .expect("the key must still be live")
        .0
        .expect("a pending row always carries execute_at"))
}

/// State-polls until `(job_type, key)` is back to `pending` at
/// `attempt_index = attempt`, and returns that row's `execute_at`.
///
/// This is the handshake between "the runner told us it failed" and "the
/// finalizer has written the retry row": respawning before that write lands
/// would test the `state = 'pending'` guard by accident instead of the
/// backoff guard on purpose.
async fn await_pending_at_attempt(
    pool: &sqlx::PgPool,
    job_type: &JobType,
    key: &str,
    attempt: i32,
) -> anyhow::Result<DateTime<Utc>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((Some(execute_at), got, state)) = row(pool, job_type, key).await?
            && state == "pending"
            && got == attempt
        {
            return Ok(execute_at);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {key} to be pending at attempt {attempt}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn next_run(
    ran: &mut mpsc::UnboundedReceiver<DateTime<Utc>>,
) -> anyhow::Result<DateTime<Utc>> {
    tokio::time::timeout(Duration::from_secs(30), ran.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for the runner to run"))?
        .ok_or_else(|| anyhow::anyhow!("runner channel closed"))
}

// -- tests ------------------------------------------------------------------

/// The core write. A key held far in the future moves to now, reports
/// `pulled_forward`, and a second respawn against the now-due row is a
/// no-op — nothing is written twice and nothing moves back.
///
/// No poller: the row must stay parked so the assertions are about the spawn
/// path alone.
#[tokio::test]
async fn force_reschedule_pulls_a_future_hold_forward_exactly_once() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-pull");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    let held = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("k", Cfg).schedule_at(Utc::now() + HOLD),
        ])
        .await?;
    assert!(held[0].created);
    assert!(
        !held[0].pulled_forward,
        "a job created now is already due at the time it asked for"
    );
    let parked = execute_at(&pool, &job_type, "k").await?;

    let woken = spawner
        .spawn_all(vec![KeyedJobSpec::new("k", Cfg).force_reschedule()])
        .await?;
    assert!(!woken[0].created, "the key is still live");
    assert!(woken[0].pulled_forward, "the hold must have been moved");
    assert_eq!(woken[0].handle.id(), held[0].handle.id());

    let after = execute_at(&pool, &job_type, "k").await?;
    assert!(
        after < parked && after <= Utc::now(),
        "the row must now be due: {after} (was {parked})"
    );

    let again = spawner
        .spawn_all(vec![KeyedJobSpec::new("k", Cfg).force_reschedule()])
        .await?;
    assert!(
        !again[0].pulled_forward,
        "a second respawn against an already-due row must be a no-op"
    );
    assert_eq!(
        execute_at(&pool, &job_type, "k").await?,
        after,
        "and must not rewrite execute_at"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// End to end, with a poller and a realistically sized registry: a generation
/// that parked itself an HOUR out via `RescheduleAt` runs within the test's
/// timeout once a respawn asks for it.
///
/// Nothing about this can pass without the pull-forward — the second run is
/// an hour away otherwise — and both wake signals are exercised: the
/// `ExecutionReady` notify and the local poller's claim demand.
#[tokio::test]
async fn a_held_runner_is_woken_by_a_force_reschedule_respawn() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-wake");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs
        .add_keyed_initializer(Init::new(&job_type, Behaviour::HoldThenComplete).reporting_to(tx));
    register_filler(&mut jobs);
    jobs.start_poll().await?;

    let spawned = spawner.spawn_all(vec![KeyedJobSpec::new("k", Cfg)]).await?;
    let first = next_run(&mut ran).await?;

    // The runner has returned `RescheduleAt(+1h)`; wait for the finalizer's
    // write rather than assuming it has landed.
    let parked = await_pending_at_attempt(&pool, &job_type, "k", 1).await?;
    assert!(
        parked > first + chrono::TimeDelta::minutes(30),
        "the generation must really be parked an hour out, got {parked}"
    );

    let woken = spawner
        .spawn_all(vec![KeyedJobSpec::new("k", Cfg).force_reschedule()])
        .await?;
    assert!(!woken[0].created);
    assert!(woken[0].pulled_forward);
    assert_eq!(woken[0].handle.id(), spawned[0].handle.id());

    let second = next_run(&mut ran).await?;
    assert!(
        second < parked,
        "the woken run must happen long before its own deadline ({second} vs {parked})"
    );

    woken[0]
        .handle
        .await_completion(Duration::from_secs(30))
        .await?;

    jobs.shutdown().await?;
    Ok(())
}

/// THE regression test. A permanently failing keyed job, hammered with
/// `force_reschedule` respawns between every attempt, must keep its
/// exponential backoff: each respawn reports `pulled_forward == false`,
/// leaves `execute_at` byte-for-byte alone, and the observed run gaps keep
/// growing.
///
/// Without the `attempt_index <= 1` guard this is a hot loop at the respawn
/// rate — the price-shock retry storm the design exists to avoid.
#[tokio::test]
async fn force_reschedule_never_shortens_a_retry_backoff() -> anyhow::Result<()> {
    const BURST: usize = 5;
    const ATTEMPTS: i32 = 4;
    let min_backoff = Duration::from_millis(400);

    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-backoff");
    let (tx, mut ran) = mpsc::unbounded_channel();
    let spawner = jobs.add_keyed_initializer(
        Init::new(&job_type, Behaviour::AlwaysFails)
            .reporting_to(tx)
            .with_retry(RetrySettings {
                n_attempts: None,
                min_backoff,
                max_backoff: Duration::from_secs(60),
                // Jitter off so the schedule is exactly doubling, and the
                // attempt counter pinned so a slow machine cannot reset it
                // to 1 mid-test (which would hand the row to the guard for
                // the wrong reason).
                backoff_jitter_pct: 0,
                attempt_reset_after_backoff_multiples: 1_000,
                ..Default::default()
            }),
    );
    register_filler(&mut jobs);
    jobs.start_poll().await?;

    spawner.spawn_all(vec![KeyedJobSpec::new("k", Cfg)]).await?;

    let mut starts = Vec::new();
    for attempt in 1..=ATTEMPTS {
        starts.push(next_run(&mut ran).await?);

        // The failed run is now backing off at `attempt + 1`.
        let scheduled = await_pending_at_attempt(&pool, &job_type, "k", attempt + 1).await?;
        for _ in 0..BURST {
            let respawn = spawner
                .spawn_all(vec![KeyedJobSpec::new("k", Cfg).force_reschedule()])
                .await?;
            assert!(!respawn[0].created, "the failing job still holds the key");
            assert!(
                !respawn[0].pulled_forward,
                "a backing-off row (attempt {}) must never be pulled forward",
                attempt + 1
            );
        }
        assert_eq!(
            execute_at(&pool, &job_type, "k").await?,
            scheduled,
            "the backoff deadline must be untouched after {BURST} respawns"
        );
    }

    let gaps: Vec<chrono::TimeDelta> = starts.windows(2).map(|w| w[1] - w[0]).collect();
    for (i, gap) in gaps.iter().enumerate() {
        let expected = min_backoff.as_millis() as i64 * (1 << i);
        assert!(
            gap.num_milliseconds() >= expected * 3 / 4,
            "gap {i} was {gap:?}, well under the {expected}ms backoff it should have waited"
        );
    }
    assert!(
        gaps[gaps.len() - 1] > gaps[0] * 2,
        "the backoff must still be growing across attempts: {gaps:?}"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A respawn can only ever move `execute_at` EARLIER. A due row asked to
/// reschedule far into the future stays due — `schedule_at` is a property of
/// a job being CREATED, and a resolved spec never rewrites the holder.
#[tokio::test]
async fn force_reschedule_never_pushes_execute_at_later() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-monotone");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    spawner.spawn_all(vec![KeyedJobSpec::new("k", Cfg)]).await?;
    let due = execute_at(&pool, &job_type, "k").await?;

    let respawn = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("k", Cfg)
                .schedule_at(Utc::now() + HOLD)
                .force_reschedule(),
        ])
        .await?;
    assert!(!respawn[0].pulled_forward);
    assert_eq!(
        execute_at(&pool, &job_type, "k").await?,
        due,
        "a respawn must never delay a live job"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// The flag is opt-in: without it a respawn against a future-dated holder is
/// exactly what it always was — resolve, change nothing.
#[tokio::test]
async fn a_respawn_without_the_flag_leaves_a_hold_alone() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-opt-in");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    let held = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("k", Cfg).schedule_at(Utc::now() + HOLD),
        ])
        .await?;
    let parked = execute_at(&pool, &job_type, "k").await?;

    let plain = spawner.spawn_all(vec![KeyedJobSpec::new("k", Cfg)]).await?;
    assert!(!plain[0].created);
    assert!(!plain[0].pulled_forward);
    assert_eq!(plain[0].handle.id(), held[0].handle.id());
    assert_eq!(
        execute_at(&pool, &job_type, "k").await?,
        parked,
        "an ordinary keyed respawn must still be a pure no-op"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// One key, two specs in one call, only one of them asking for a wake: the
/// row moves once and only the asking spec reports it. The plain spec still
/// reads as the no-op it requested.
#[tokio::test]
async fn only_the_specs_that_asked_report_pulled_forward() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-mixed");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    spawner
        .spawn_all(vec![
            KeyedJobSpec::new("k", Cfg).schedule_at(Utc::now() + HOLD),
        ])
        .await?;

    let mixed = spawner
        .spawn_all(vec![
            KeyedJobSpec::new("k", Cfg),
            KeyedJobSpec::new("k", Cfg).force_reschedule(),
        ])
        .await?;
    assert!(mixed.iter().all(|s| !s.created));
    assert!(!mixed[0].pulled_forward, "this spec never asked for a wake");
    assert!(mixed[1].pulled_forward);
    assert!(execute_at(&pool, &job_type, "k").await? <= Utc::now());

    jobs.shutdown().await?;
    Ok(())
}

/// Two transactions waking one key at the same time: both resolve to the same
/// job, neither errors or deadlocks (they serialize on the key's advisory
/// lock), and the row ends up due exactly once — never later than either
/// caller asked for.
#[tokio::test]
async fn concurrent_force_reschedules_of_one_key_agree() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-race");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    for i in 0..10 {
        let key = format!("race-{i}");
        let held = spawner
            .spawn_all(vec![
                KeyedJobSpec::new(key.clone(), Cfg).schedule_at(Utc::now() + HOLD),
            ])
            .await?;

        let started = Utc::now();
        let (a, b) = tokio::join!(
            spawner.spawn_all(vec![KeyedJobSpec::new(key.clone(), Cfg).force_reschedule()]),
            spawner.spawn_all(vec![KeyedJobSpec::new(key.clone(), Cfg).force_reschedule()]),
        );
        let a = a.expect("a lost race must resolve, never error");
        let b = b.expect("a lost race must resolve, never error");

        assert_eq!(a[0].handle.id(), held[0].handle.id());
        assert_eq!(b[0].handle.id(), held[0].handle.id());
        assert!(
            a[0].pulled_forward || b[0].pulled_forward,
            "one of the two must have moved the row"
        );
        let after = execute_at(&pool, &job_type, &key).await?;
        assert!(
            after <= Utc::now() && after < started + HOLD,
            "the row must be due afterwards, not still parked"
        );
    }

    jobs.shutdown().await?;
    Ok(())
}

/// KNOWN GAP, pinned deliberately (`handoff-keyed-wake-pull-forward.md` Q1,
/// still open): a congestion reschedule leaves `attempt_index` untouched, so
/// a row delayed by pool congestion on its FIRST attempt looks exactly like a
/// deliberate hold and IS pulled forward.
///
/// The row is shaped by hand rather than by starving a real pool — what
/// matters is the row shape the finalizer's congestion write leaves behind
/// (`state = 'pending'`, `attempt_index` UNCHANGED, `execute_at = now + 2s
/// ± 1s`), which is what this path reads.
///
/// If Q1 is later decided the other way — a congestion marker on the row, or
/// congestion advancing `attempt_index` — this test flips to asserting the
/// row is left alone. It exists so the choice is explicit either way.
#[tokio::test]
async fn a_congestion_shaped_row_is_indistinguishable_from_a_hold() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("force-reschedule-congestion");
    let spawner = jobs.add_keyed_initializer(Init::new(&job_type, Behaviour::Idle));

    spawner.spawn_all(vec![KeyedJobSpec::new("k", Cfg)]).await?;

    // Exactly `Finalizer::reschedule_congested`'s row shape: back to pending
    // ~2s out, attempt_index left where it was (1, a first attempt).
    sqlx::query(
        "UPDATE job_executions SET state = 'pending', execute_at = NOW() + INTERVAL '2 seconds'
         WHERE job_type = $1 AND unique_key = 'k'",
    )
    .bind(job_type.as_str())
    .execute(&pool)
    .await?;
    let (_, attempt, _) = row(&pool, &job_type, "k").await?.expect("the key is live");
    assert_eq!(attempt, 1, "congestion leaves attempt_index untouched");

    let woken = spawner
        .spawn_all(vec![KeyedJobSpec::new("k", Cfg).force_reschedule()])
        .await?;
    assert!(
        woken[0].pulled_forward,
        "Q1 gap: a congestion-delayed first attempt is currently pulled forward"
    );

    jobs.shutdown().await?;
    Ok(())
}
