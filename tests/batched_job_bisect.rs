//! Live-PG coverage for `CurrentBatchedJob::run_bisected` — auto-bisecting
//! failure isolation for true-batch runners (probe the whole batch in one
//! savepoint; on failure, roll back and probe smaller contiguous sub-ranges,
//! largest pending range first, until each culprit is isolated).
//!
//! Deliberately a separate file from `batched_job_savepoint.rs`: every test
//! here uses its own `JobType`, matching the shared-job-type flake class
//! fixed in d1c58c2.
#![cfg(feature = "es-entity")]

mod helpers;

use async_trait::async_trait;
use es_entity::AtomicOperation;
use job::{
    BatchedJobInitializer, BatchedJobRunner, BisectBudget, CurrentBatchedJob, JobBatchCompletion,
    JobId, JobSpec, JobSvcConfig, JobTerminalState, JobType, Jobs, RetrySettings,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Every item carries a small integer key. `key = SENTINEL_KEY` items are
/// culprits: their key is pre-seeded into the scratch table before dispatch,
/// so ANY probe touching one hits a genuine Postgres unique-violation
/// regardless of who else is in the slice — deterministic, no racing.
/// Clean items carry a key unique to their position in the batch's
/// `queue_id` sort order, so they never collide with each other.
const SENTINEL_KEY: i32 = 999;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BisectConfig {
    table: String,
    key: i32,
    /// Culprits only insert on their first attempt: a retried culprit is
    /// always dispatched solo (attempt > 1 is never batched), and by then
    /// the test has cleared the pre-seeded sentinel row, so skipping the
    /// insert entirely on attempt > 1 lets it complete instead of
    /// colliding with itself.
    culprit: bool,
}

/// A *true batch* runner: one multi-row `INSERT ... SELECT UNNEST(...)` per
/// probe slice, not a per-item loop (that shape is `run_isolated`'s job).
/// Counts probe invocations via `probes` so tests can assert exact counts.
struct BisectRunner {
    table: String,
    budget: BisectBudget,
    probes: Arc<AtomicUsize>,
}

#[async_trait]
impl BatchedJobRunner for BisectRunner {
    type Config = BisectConfig;

    async fn run_batch(
        &self,
        current_batch: CurrentBatchedJob<BisectConfig>,
    ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
        let mut op = current_batch.begin_op().await?;
        let probes = Arc::clone(&self.probes);
        let table = self.table.clone();
        let outcomes = current_batch
            .run_bisected_with(&mut op, self.budget, async move |sp, slice| {
                probes.fetch_add(1, Ordering::SeqCst);
                let keys: Vec<i32> = slice
                    .iter()
                    .filter(|item| !item.config().culprit || item.attempt() == 1)
                    .map(|item| item.config().key)
                    .collect();
                sqlx::query(&format!(
                    "INSERT INTO {table} (v) SELECT * FROM UNNEST($1::int[])"
                ))
                .bind(&keys)
                .execute(sp.as_executor())
                .await?;
                Ok::<_, sqlx::Error>(())
            })
            .await?;
        Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
    }
}

struct BisectInitializer {
    job_type: JobType,
    table: String,
    budget: BisectBudget,
    probes: Arc<AtomicUsize>,
    n_attempts: Option<u32>,
}

impl BatchedJobInitializer for BisectInitializer {
    type Config = BisectConfig;

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
        _: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>> {
        Ok(Box::new(BisectRunner {
            table: self.table.clone(),
            budget: self.budget,
            probes: Arc::clone(&self.probes),
        }))
    }
}

async fn reset_scratch_table(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<()> {
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(pool)
        .await?;
    sqlx::query(&format!("CREATE TABLE {table} (v INT PRIMARY KEY)"))
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_sentinel(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<()> {
    sqlx::query(&format!("INSERT INTO {table} (v) VALUES ($1)"))
        .bind(SENTINEL_KEY)
        .execute(pool)
        .await?;
    Ok(())
}

async fn scratch_values(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<Vec<i32>> {
    let rows: Vec<(i32,)> = sqlx::query_as(&format!("SELECT v FROM {table} ORDER BY v"))
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(v,)| v).collect())
}

/// Spawns `n` unqueued items whose batch position is fully controlled
/// without depending on `JobId`'s generation-time ordering (`JobId::new()`
/// is UUIDv7 — time-ordered across milliseconds, but NOT guaranteed
/// monotonic for ids minted in the same millisecond, which a tight spawn
/// loop easily does). Instead: generate `n` ids, sort them, and assign
/// roles by SORTED position — exactly the order the module's `(queue_id,
/// id)` fallback sort will place them in when `queue_id` is `None`, so the
/// returned `Vec<JobId>` IS the batch's probe order.
///
/// `culprit_indices` marks which sorted positions are pre-seeded-key
/// culprits; every other position gets a clean key equal to its own index.
async fn spawn_batch(
    spawner: &job::JobSpawner<BisectConfig>,
    table: &str,
    n: usize,
    culprit_indices: &[usize],
) -> anyhow::Result<Vec<JobId>> {
    let mut ids: Vec<JobId> = (0..n).map(|_| JobId::new()).collect();
    ids.sort();

    let specs: Vec<JobSpec<BisectConfig>> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let culprit = culprit_indices.contains(&i);
            JobSpec::new(
                *id,
                BisectConfig {
                    table: table.to_string(),
                    key: if culprit { SENTINEL_KEY } else { i as i32 },
                    culprit,
                },
            )
        })
        .collect();
    spawner.spawn_all(specs).await?;
    Ok(ids)
}

#[tokio::test]
async fn happy_path_probes_once() -> anyhow::Result<()> {
    let table = "bisect_happy_path";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-happy-path"),
        table: table.to_string(),
        budget: BisectBudget::default(),
        probes: Arc::clone(&probes),
        n_attempts: Some(1),
    });

    let ids = spawn_batch(&spawner, table, 5, &[]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "a clean batch must fully complete, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    assert_eq!(
        probes.load(Ordering::SeqCst),
        1,
        "a healthy batch must resolve in exactly one probe"
    );
    assert_eq!(scratch_values(&pool, table).await?, vec![0, 1, 2, 3, 4]);

    Ok(())
}

#[tokio::test]
async fn single_culprit_isolates_under_default_auto_budget() -> anyhow::Result<()> {
    let table = "bisect_single_culprit_auto";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    seed_sentinel(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-single-culprit-auto"),
        table: table.to_string(),
        budget: BisectBudget::default(),
        probes: Arc::clone(&probes),
        n_attempts: Some(2),
    });

    // Culprit at position 0 of 10 — Auto's cap (2*ceil(log2(10))+1 = 9)
    // must be enough to fully isolate one culprit.
    let ids = spawn_batch(&spawner, table, 10, &[0]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "the culprit must recover on its solo retry, batch-mates must complete \
         in the first dispatch, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    // Items 1..10 (their clean keys) commit in the bisecting dispatch; the
    // culprit's retry inserts nothing (skips past attempt 1) but the
    // pre-seeded sentinel row from `seed_sentinel` is a real, separate
    // commit made before the batch ever ran, so it's still there too.
    assert_eq!(
        scratch_values(&pool, table).await?,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, SENTINEL_KEY]
    );
    // Auto's cap is 2*ceil(log2(N))+1 (internal, non-contractual formula —
    // spelled out here rather than exposed as public API): 2*4+1 = 9 at N=10.
    let used = probes.load(Ordering::SeqCst);
    assert!(
        used <= 9,
        "Auto must not exceed its own computed cap of 9 at N=10, saw {used} probes"
    );

    Ok(())
}

#[tokio::test]
async fn largest_first_salvage_under_tight_budget() -> anyhow::Result<()> {
    let table = "bisect_tight_budget_salvage";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    seed_sentinel(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-tight-budget-salvage"),
        table: table.to_string(),
        budget: BisectBudget::MaxProbes(4),
        probes: Arc::clone(&probes),
        n_attempts: Some(2),
    });

    let ids = spawn_batch(&spawner, table, 10, &[0]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "budget-failed items must still recover through solo retry, \
         saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    // The first dispatch probes largest-first — [0,10)F, [0,5)F, [5,10)OK,
    // [2,5)OK — exactly 4 (the budget), salvaging items 2..9; items 0,1
    // budget-fail and retry solo, each contributing 1 more probe to the
    // SHARED counter (it accumulates across every dispatch of this job
    // type): 4 + 2 = 6.
    assert_eq!(
        probes.load(Ordering::SeqCst),
        6,
        "expected the 4-probe tight-budget dispatch plus 2 solo-retry probes \
         (items 0 and 1)"
    );

    Ok(())
}

#[tokio::test]
async fn all_bad_batch_resolves_every_item_under_full_resolution() -> anyhow::Result<()> {
    let table = "bisect_all_bad_full_resolution";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    seed_sentinel(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-all-bad-full-resolution"),
        table: table.to_string(),
        budget: BisectBudget::FullResolution,
        probes: Arc::clone(&probes),
        // Terminal on the first failure, deliberately: solo-retry recovery
        // is already covered (with less simultaneous retry churn) by
        // `single_culprit_isolates_under_default_auto_budget` and
        // `largest_first_salvage_under_tight_budget`. This test's only job
        // is the search's shape on the FIRST dispatch, so it doesn't need
        // 6 concurrent backoff+reclaim round trips to prove that — those
        // round trips were observed to make the test load-sensitive (timed
        // out under full-suite concurrency even at 45s) without adding any
        // coverage this test is actually responsible for.
        n_attempts: Some(1),
    });

    let culprits: Vec<usize> = (0..6).collect();
    let ids = spawn_batch(&spawner, table, 6, &culprits).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Errored),
        "every item exhausts its single attempt and errors out, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    assert_eq!(scratch_values(&pool, table).await?, vec![SENTINEL_KEY]);
    // The pathological case: `FullResolution` keeps halving until every
    // culprit sits in its own singleton, so an all-bad batch costs the full
    // `2N-1`. First (only) dispatch, largest-first: [0,6)F, [0,3)F, [3,6)F,
    // [1,3)F, [4,6)F, then the 6 singletons = 11 probes. This is the cost
    // the `# Cost` docs warn about — a type whose dominant failure mode is
    // "everything fails together" should return `Err` from `run_batch`
    // rather than reach for this helper.
    assert_eq!(
        probes.load(Ordering::SeqCst),
        11,
        "expected full resolution's exact probe count; a different total \
         means the search's shape changed"
    );

    Ok(())
}

/// Regression test for the false-escalation bug: a *streak* of consecutive
/// failing probes was once read as "the whole batch is bad" and used to
/// shred every pending range — including unprobed, entirely clean ones —
/// into singletons.
///
/// The signal was invalid. Largest-first probing walks the frontier
/// breadth-first, so a run of failures means the culprits are widely
/// *spread*, not that they are *dense*: with N=16 and just two culprits
/// (one per half) the run reached 4 before a single clean range had been
/// tried. Escalation then fired and burned the remaining budget on
/// singletons, completing 4 items instead of 10.
#[tokio::test]
async fn scattered_culprits_do_not_poison_their_clean_siblings() -> anyhow::Result<()> {
    let table = "bisect_scattered_culprits";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    seed_sentinel(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-scattered-culprits"),
        table: table.to_string(),
        budget: BisectBudget::default(),
        probes: Arc::clone(&probes),
        // Terminal on the first failure: this test asserts the salvage of
        // the FIRST dispatch exactly, so no retry churn to account for.
        n_attempts: Some(1),
    });

    // One culprit per half of a 16-item batch — the minimum that used to
    // trip the old streak threshold.
    let ids = spawn_batch(&spawner, table, 16, &[0, 8]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    // Largest-first under Auto (cap 2*ceil(log2 16)+1 = 9):
    //   [0,16)F [0,8)F [8,16)F [0,4)F [4,8)OK [8,12)F [12,16)OK [0,2)F [2,4)OK
    // salvaging 2..7 and 12..15 = 10 items; 0,1,8,9,10,11 budget-fail.
    // Under the old escalation heuristic this dispatch completed only 4.
    let completed = outcomes
        .iter()
        .filter(|o| o.state() == JobTerminalState::Completed)
        .count();
    assert_eq!(
        completed,
        10,
        "two scattered culprits must not cost their clean siblings a \
         completion, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    assert_eq!(probes.load(Ordering::SeqCst), 9, "Auto's cap at N=16");
    assert_eq!(
        scratch_values(&pool, table).await?,
        vec![2, 3, 4, 5, 6, 7, 12, 13, 14, 15, SENTINEL_KEY],
        "exactly the salvaged items' keys must be committed"
    );

    Ok(())
}

#[tokio::test]
async fn domain_error_isolates_without_touching_the_db() -> anyhow::Result<()> {
    // Not a unique-violation: `f` returns `Err` for flagged items purely in
    // Rust, proving the bisect path isn't specific to Postgres errors.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FlagConfig {
        bad: bool,
    }

    /// A domain error with nothing underneath it. `run_bisected` inspects
    /// `E`'s source chain to spot retryable conflicts, so this exercises the
    /// case where the chain bottoms out immediately and the probe is a plain
    /// attributable failure.
    #[derive(Debug)]
    struct FlaggedItem;

    impl std::fmt::Display for FlaggedItem {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("flagged item in slice")
        }
    }

    impl std::error::Error for FlaggedItem {}

    struct FlagRunner {
        probes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BatchedJobRunner for FlagRunner {
        type Config = FlagConfig;

        async fn run_batch(
            &self,
            current_batch: CurrentBatchedJob<FlagConfig>,
        ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
            let mut op = current_batch.begin_op().await?;
            let probes = Arc::clone(&self.probes);
            let outcomes = current_batch
                .run_bisected(&mut op, async move |_sp, slice| {
                    probes.fetch_add(1, Ordering::SeqCst);
                    if slice.iter().any(|item| item.config().bad) {
                        Err(FlaggedItem)
                    } else {
                        Ok(())
                    }
                })
                .await?;
            Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
        }
    }

    struct FlagInitializer {
        job_type: JobType,
        probes: Arc<AtomicUsize>,
    }

    impl BatchedJobInitializer for FlagInitializer {
        type Config = FlagConfig;

        fn job_type(&self) -> JobType {
            self.job_type.clone()
        }

        fn retry_on_error_settings(&self) -> RetrySettings {
            RetrySettings {
                n_attempts: Some(1),
                min_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                ..Default::default()
            }
        }

        fn init(
            &self,
            _: job::JobSpawner<Self::Config>,
        ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>>
        {
            Ok(Box::new(FlagRunner {
                probes: Arc::clone(&self.probes),
            }))
        }
    }

    let pool = helpers::init_pool().await?;
    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(FlagInitializer {
        job_type: helpers::job_type("bisect-domain-error"),
        probes: Arc::clone(&probes),
    });

    let mut ids = Vec::new();
    let mut specs = Vec::new();
    for i in 0..5 {
        let id = JobId::new();
        ids.push(id);
        specs.push(JobSpec::new(id, FlagConfig { bad: i == 2 }));
    }
    spawner.spawn_all(specs).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    let completed = outcomes
        .iter()
        .filter(|o| o.state() == JobTerminalState::Completed)
        .count();
    let errored = outcomes
        .iter()
        .filter(|o| o.state() == JobTerminalState::Errored)
        .count();
    assert_eq!(
        completed, 4,
        "the four flag-free items must isolate and complete"
    );
    assert_eq!(
        errored, 1,
        "the single flagged item must isolate and error out"
    );
    assert!(probes.load(Ordering::SeqCst) > 1);

    Ok(())
}

#[tokio::test]
async fn max_probes_one_is_equivalent_to_fail_all() -> anyhow::Result<()> {
    let table = "bisect_max_probes_one";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    seed_sentinel(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-max-probes-one"),
        table: table.to_string(),
        budget: BisectBudget::MaxProbes(1),
        probes: Arc::clone(&probes),
        // Terminal on the first failure. Recovery of budget-failed items
        // through solo retry is covered by
        // `largest_first_salvage_under_tight_budget`; asserting it here too
        // would mean five simultaneous backoff+reclaim round trips, and
        // retry churn of that size is what makes a test load-sensitive
        // under full-suite concurrency. What this test is responsible for
        // is the degenerate budget itself, which the first dispatch settles
        // on its own.
        n_attempts: Some(1),
    });

    let ids = spawn_batch(&spawner, table, 5, &[0]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    // MaxProbes(1) probes the whole batch exactly once and, since it fails,
    // budget-fails every item in that same dispatch — no bisection at all.
    // This is the interpolation endpoint: equivalent to returning `Err` from
    // `run_batch` directly, just routed through the helper.
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Errored),
        "a single probe cannot attribute the failure, so every item fails, \
         saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    assert_eq!(
        probes.load(Ordering::SeqCst),
        1,
        "MaxProbes(1) must probe exactly once and never split"
    );
    assert_eq!(
        scratch_values(&pool, table).await?,
        vec![SENTINEL_KEY],
        "the sole probe rolled back, so nothing but the seed is committed"
    );

    Ok(())
}

#[tokio::test]
async fn max_probes_zero_clamps_to_one() -> anyhow::Result<()> {
    let table = "bisect_max_probes_zero";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-max-probes-zero"),
        table: table.to_string(),
        budget: BisectBudget::MaxProbes(0),
        probes: Arc::clone(&probes),
        n_attempts: Some(1),
    });

    let ids = spawn_batch(&spawner, table, 3, &[]).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );
    assert_eq!(
        probes.load(Ordering::SeqCst),
        1,
        "MaxProbes(0) must clamp to 1"
    );

    Ok(())
}

#[tokio::test]
async fn coupled_pair_resolves_deterministically_by_probe_order() -> anyhow::Result<()> {
    let table = "bisect_coupled_pair";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;

    let probes = Arc::new(AtomicUsize::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(BisectInitializer {
        job_type: helpers::job_type("bisect-coupled-pair"),
        table: table.to_string(),
        budget: BisectBudget::default(),
        probes: Arc::clone(&probes),
        n_attempts: Some(1),
    });

    // Two items race for the SAME key (not the sentinel table) — a real
    // intra-batch coupling. Not marked `culprit` (that flag means
    // "pre-seeded"); both share `key: 42` instead.
    let mut ids = Vec::new();
    let mut specs = Vec::new();
    for _ in 0..2 {
        let id = JobId::new();
        ids.push(id);
        specs.push(JobSpec::new(
            id,
            BisectConfig {
                table: table.to_string(),
                key: 42,
                culprit: false,
            },
        ));
    }
    spawner.spawn_all(specs).await?;
    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;

    let completed = outcomes
        .iter()
        .filter(|o| o.state() == JobTerminalState::Completed)
        .count();
    let errored = outcomes
        .iter()
        .filter(|o| o.state() == JobTerminalState::Errored)
        .count();
    assert_eq!(completed, 1, "exactly one of the coupled pair wins the key");
    assert_eq!(
        errored, 1,
        "the other resolves as an isolated failure, not a batch-wide one"
    );
    assert_eq!(scratch_values(&pool, table).await?, vec![42]);

    Ok(())
}

/// A `40P01` deadlock inside a probe must re-run that probe, never split on it.
///
/// A deadlock is not attributable to any item, so splitting on one cannot
/// isolate anything. Worse, it can provoke: lock sets exist that succeed taken
/// at once and deadlock taken in halves. Left to drive the search it would end
/// up blaming individual jobs -- and burning their retry attempts -- for a
/// conflict the server invented to break a cycle.
///
/// The deadlock is manufactured, and which side loses is made deterministic
/// rather than left to chance. A helper transaction takes the two scratch rows
/// in the opposite order to the probe, and the two are stepped through the
/// interleaving with semaphore gates (permits, so neither can miss a signal it
/// arrives late for):
///
/// 1. helper locks row 2, then opens the probe's gate
/// 2. probe locks row 1, opens the helper's gate, and reaches for row 2 --
///    blocking, because the helper holds it
/// 3. helper waits out a margin, then reaches for row 1 -- closing the cycle
///
/// Postgres aborts whichever waiter's `deadlock_timeout` expires first and
/// finds a cycle. The probe starts waiting a clear margin before the helper
/// does, so the probe is the victim -- which is the side under test.
#[tokio::test]
async fn a_deadlocked_probe_is_re_run_whole_never_split() -> anyhow::Result<()> {
    let table = "bisect_deadlock_abort";
    let pool = helpers::init_pool().await?;
    reset_scratch_table(&pool, table).await?;
    // Two rows the probe and the helper will contend over, in opposite orders.
    for v in [1, 2] {
        sqlx::query(&format!("INSERT INTO {table} (v) VALUES ($1)"))
            .bind(v)
            .execute(&pool)
            .await?;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DlConfig {
        table: String,
    }

    struct DlRunner {
        table: String,
        probes: Arc<AtomicUsize>,
        /// Opened once the helper holds row 2.
        helper_ready: Arc<tokio::sync::Semaphore>,
        /// Opened once the probe holds row 1.
        probe_ready: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait]
    impl BatchedJobRunner for DlRunner {
        type Config = DlConfig;

        async fn run_batch(
            &self,
            current_batch: CurrentBatchedJob<DlConfig>,
        ) -> Result<JobBatchCompletion, Box<dyn std::error::Error>> {
            let mut op = current_batch.begin_op().await?;
            let probes = Arc::clone(&self.probes);
            let helper_ready = Arc::clone(&self.helper_ready);
            let probe_ready = Arc::clone(&self.probe_ready);
            let table = self.table.clone();
            let outcomes = current_batch
                .run_bisected(&mut op, async move |sp, _slice| {
                    probes.fetch_add(1, Ordering::SeqCst);
                    // Wait for the helper to hold row 2, take row 1, then
                    // reach for row 2 and block. Blocking here first is what
                    // makes this side the deadlock victim.
                    let _ = helper_ready.acquire().await;
                    sqlx::query(&format!("SELECT v FROM {table} WHERE v = 1 FOR UPDATE"))
                        .fetch_one(sp.as_executor())
                        .await?;
                    probe_ready.add_permits(4096);
                    sqlx::query(&format!("SELECT v FROM {table} WHERE v = 2 FOR UPDATE"))
                        .fetch_one(sp.as_executor())
                        .await?;
                    Ok::<_, sqlx::Error>(())
                })
                .await?;
            Ok(JobBatchCompletion::WithOutcomesWithOp(op, outcomes))
        }
    }

    struct DlInitializer {
        job_type: JobType,
        table: String,
        probes: Arc<AtomicUsize>,
        helper_ready: Arc<tokio::sync::Semaphore>,
        probe_ready: Arc<tokio::sync::Semaphore>,
    }

    impl BatchedJobInitializer for DlInitializer {
        type Config = DlConfig;
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
            _: job::JobSpawner<Self::Config>,
        ) -> Result<Box<dyn BatchedJobRunner<Config = Self::Config>>, Box<dyn std::error::Error>>
        {
            Ok(Box::new(DlRunner {
                table: self.table.clone(),
                probes: Arc::clone(&self.probes),
                helper_ready: Arc::clone(&self.helper_ready),
                probe_ready: Arc::clone(&self.probe_ready),
            }))
        }
    }

    let probes = Arc::new(AtomicUsize::new(0));
    let helper_ready = Arc::new(tokio::sync::Semaphore::new(0));
    let probe_ready = Arc::new(tokio::sync::Semaphore::new(0));
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let spawner = jobs.add_batched_initializer(DlInitializer {
        job_type: helpers::job_type("bisect-deadlock-abort"),
        table: table.to_string(),
        probes: Arc::clone(&probes),
        helper_ready: Arc::clone(&helper_ready),
        probe_ready: Arc::clone(&probe_ready),
    });

    // Eight items, so a bisect that treated the deadlock as a culprit signal
    // would be plainly visible: it would split to 15 probes rather than stop.
    let ids: Vec<JobId> = (0..8).map(|_| JobId::new()).collect();
    spawner
        .spawn_all(
            ids.iter()
                .map(|id| {
                    JobSpec::new(
                        *id,
                        DlConfig {
                            table: table.to_string(),
                        },
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

    let helper_pool = helpers::init_pool().await?;
    let helper_table = table.to_string();
    let helper = tokio::spawn(async move {
        let mut tx = helper_pool.begin().await?;
        sqlx::query(&format!(
            "SELECT v FROM {helper_table} WHERE v = 2 FOR UPDATE"
        ))
        .fetch_one(&mut *tx)
        .await?;
        helper_ready.add_permits(4096);

        // Wait for the probe to hold row 1, then give it a clear margin to
        // start waiting on row 2 before closing the cycle from this side.
        // Whichever waiter's `deadlock_timeout` expires first is the one
        // Postgres aborts, and the probe is the side under test.
        let _ = probe_ready.acquire().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = sqlx::query(&format!(
            "SELECT v FROM {helper_table} WHERE v = 1 FOR UPDATE"
        ))
        .fetch_one(&mut *tx)
        .await;
        let _ = tx.rollback().await;
        Ok::<_, sqlx::Error>(())
    });

    jobs.start_poll().await?;
    let outcomes = jobs.handles(ids).await_all(Duration::from_secs(30)).await?;
    let _ = helper.await;

    // The conflict was transient, so the re-probe wins and the batch settles
    // in this dispatch -- no item blamed, no retry attempt burnt, even though
    // `n_attempts` is 1 and there is no second chance.
    assert!(
        outcomes
            .iter()
            .all(|o| o.state() == JobTerminalState::Completed),
        "a transient conflict must not cost the batch its dispatch, saw {:?}",
        outcomes.iter().map(|o| o.state()).collect::<Vec<_>>()
    );

    // The assertion that pins the mechanism. Two probes means the deadlocked
    // range was re-run whole: [0,8) deadlocks, [0,8) succeeds. Splitting on
    // the conflict instead -- the behaviour this guards against -- reaches the
    // same completed outcome by a different route, [0,8) then [0,4) then
    // [4,8), and is caught here as three.
    let used = probes.load(Ordering::SeqCst);
    assert_eq!(
        used, 2,
        "a retryable conflict must re-probe the same range, not split it; \
         saw {used} probes"
    );

    Ok(())
}
