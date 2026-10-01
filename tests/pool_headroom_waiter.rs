//! Live-PG coverage for the zero-headroom claim behaviour: a poll whose
//! shared pool has no headroom claims NOTHING (rows stay `pending`, where a
//! healthy peer instance can take them) and arms the pool-headroom waiter
//! (`JobPoller::arm_pool_headroom_waiter`), which wakes the poll loop on a
//! backoff schedule once a connection frees.
//!
//! Deliberately its own file, same reason as
//! `pool_terminal_write_safety*.rs`: these tests exhaust the dedicated
//! pools they hand to `Jobs`, which would starve any test sharing them.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobSvcConfig, JobType,
    Jobs,
};
use serde::{Deserialize, Serialize};
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg {
    label: String,
}

/// Completes immediately, counting each run -- the counter identifies WHICH
/// `Jobs` instance ran the job, since each instance registers its own
/// `CountingInitializer` with its own counter.
struct CountingRunner {
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl JobRunner for CountingRunner {
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::Complete)
    }
}

struct CountingInitializer {
    job_type: JobType,
    runs: Arc<AtomicUsize>,
}

impl JobInitializer for CountingInitializer {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &job::Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(CountingRunner {
            runs: Arc::clone(&self.runs),
        }))
    }
}

/// Drain `pool` of every connection it will give up, holding them.
///
/// Stricter than `pool_terminal_write_safety.rs`'s acquire-until-timeout
/// drain, because these tests need saturation to be STABLE, not just
/// momentary: a plain timeout-break can fire while one of the poller's
/// background monitors transiently holds a connection, which then frees
/// AFTER the drain and silently restores headroom. So this only stops once
/// this function holds every connection except the router's one permanent
/// `LISTEN` connection AND the pool reports fully checked out with zero
/// idle -- from that state nothing can free a connection this test does
/// not itself hold. Bounded so a real bug fails the test instead of
/// hanging it.
async fn drain_pool(pool: &sqlx::PgPool) -> anyhow::Result<Vec<PoolConnection<Postgres>>> {
    let max = pool.options().get_max_connections();
    let mut held: Vec<PoolConnection<Postgres>> = Vec::new();
    for _ in 0..100 {
        if held.len() + 1 >= max as usize && pool.size() == max && pool.num_idle() == 0 {
            return Ok(held);
        }
        match tokio::time::timeout(Duration::from_millis(200), pool.acquire()).await {
            Ok(Ok(conn)) => held.push(conn),
            Ok(Err(e)) => return Err(e.into()),
            // Nothing available within the timeout -- re-check the
            // saturation condition and keep trying.
            Err(_) => {}
        }
    }
    anyhow::bail!("pool never reached stable saturation");
}

/// Poll `f` until it returns `true` or the attempt budget is exhausted --
/// state polling, not a blind sleep. 400 x 25ms = 10s: generous against CI
/// load, far below both the poller's 60s idle fallback and the shared
/// pool's ~30s acquire timeout, so a regression to either shows up as a
/// clean timeout here.
async fn wait_until(
    mut f: impl AsyncFnMut() -> anyhow::Result<bool>,
    what: &str,
) -> anyhow::Result<()> {
    for _ in 0..400 {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("timed out waiting for: {what}");
}

async fn row_state(pool: &sqlx::PgPool, id: JobId) -> anyhow::Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
            .bind(uuid::Uuid::from(id))
            .fetch_optional(pool)
            .await?,
    )
}

/// The multi-instance regression the zero-headroom rule exists for: an
/// instance whose shared pool is saturated must NOT claim due rows, because
/// a claimed row sits `running`, locked to the saturated claimer and
/// invisible to `FOR UPDATE SKIP LOCKED` on every healthy peer -- while a
/// `pending` row is claimable by whichever instance can actually run it.
///
/// Instance A's pool is fully drained before any work exists; instance B
/// is healthy. A is polling first and alone when the job is spawned, so
/// under a claim-anyway design (an earlier revision floored the unit budget
/// at 1) A would claim the row and block in `acquire()` for the pool's
/// ~30s timeout -- B could then never take it, and the completion below
/// would blow its 10s budget. With the zero-headroom rule, A leaves the row
/// `pending` and B completes it as soon as it polls.
#[tokio::test]
async fn saturated_instance_leaves_rows_pending_for_healthy_peer() -> anyhow::Result<()> {
    let observer_pool = helpers::init_pool().await?;
    let job_type = helpers::job_type("pool-peer-takeover");

    // Instance A: small dedicated pool, drained below.
    let pg_con = std::env::var("PG_CON").unwrap();
    let pool_a = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_con)
        .await?;
    let config_a = JobSvcConfig::builder()
        .pool(pool_a.clone())
        .build()
        .unwrap();
    let mut jobs_a = Jobs::init(config_a).await?;
    let runs_a = Arc::new(AtomicUsize::new(0));
    let _spawner_a = jobs_a.add_initializer(CountingInitializer {
        job_type: job_type.clone(),
        runs: Arc::clone(&runs_a),
    });

    // Instance B: healthy pool, same job type.
    let pool_b = helpers::init_pool().await?;
    let config_b = JobSvcConfig::builder()
        .pool(pool_b.clone())
        .build()
        .unwrap();
    let mut jobs_b = Jobs::init(config_b).await?;
    let runs_b = Arc::new(AtomicUsize::new(0));
    let spawner_b = jobs_b.add_initializer(CountingInitializer {
        job_type,
        runs: Arc::clone(&runs_b),
    });

    // Start ONLY A's poller, then saturate its pool completely. Ordering
    // matters: `start_poll` itself needs shared-pool connections, but no
    // job exists yet, so nothing can be claimed before the drain lands --
    // and every poll A runs once work exists sees zero headroom.
    jobs_a.start_poll().await?;
    let held = drain_pool(&pool_a).await?;

    // Spawn while A is the only instance polling -- the LISTEN/NOTIFY wake
    // reaches A immediately, so A gets every chance to (wrongly) claim
    // before B even starts. The insert itself goes through B's healthy
    // pool.
    let id = JobId::new();
    spawner_b
        .spawn(
            id,
            Cfg {
                label: "peer-takeover".to_string(),
            },
        )
        .await?;

    // The row must be claimable when B arrives: still `pending`, never
    // `running` under A.
    wait_until(
        async || Ok(row_state(&observer_pool, id).await? == Some("pending".to_string())),
        "the spawned row to be visible as pending",
    )
    .await?;

    jobs_b.start_poll().await?;

    // B (healthy) completes it; the execution row is deleted on completion.
    wait_until(
        async || Ok(runs_b.load(Ordering::SeqCst) == 1),
        "the healthy peer to run the job",
    )
    .await?;
    wait_until(
        async || Ok(row_state(&observer_pool, id).await?.is_none()),
        "the completed row to be deleted",
    )
    .await?;

    assert_eq!(
        runs_a.load(Ordering::SeqCst),
        0,
        "the saturated instance must never have claimed or run the job"
    );
    // The saturation was real the whole time.
    assert_eq!(
        pool_a.num_idle(),
        0,
        "instance A's pool should still be fully held"
    );

    drop(held);
    Ok(())
}

/// The single-instance half of the zero-headroom design: claiming nothing
/// must not mean sleeping through the pool's recovery. A poll that finds
/// due work but zero headroom arms the pool-headroom waiter, which watches
/// `pool_connection_headroom` on a 10ms-to-1s backoff and wakes the poll
/// loop as soon as a connection frees.
///
/// Without the waiter, the clamped poll's only revisit would be the 60s
/// idle fallback (`MAX_WAIT`) -- releasing a connection below would then
/// not lead to a claim within this test's 10s budget, and the completion
/// wait would time out.
///
/// (Interleaving note: the spawn's NOTIFY triggers the clamped poll within
/// milliseconds, while the release below happens at least one 25ms
/// `wait_until` round trip later -- so the clamped-then-woken path is the
/// overwhelmingly common interleaving this pins. In the rare inversion the
/// poll runs after the release and claims directly, which satisfies the
/// same no-stall property.)
#[tokio::test]
async fn waiter_wakes_poller_when_headroom_returns() -> anyhow::Result<()> {
    let observer_pool = helpers::init_pool().await?;

    let pg_con = std::env::var("PG_CON").unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_con)
        .await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let runs = Arc::new(AtomicUsize::new(0));
    let spawner = jobs.add_initializer(CountingInitializer {
        job_type: helpers::job_type("pool-headroom-waiter"),
        runs: Arc::clone(&runs),
    });

    // Start polling (needs shared-pool connections; nothing is due yet),
    // then saturate the pool.
    jobs.start_poll().await?;
    let mut held = drain_pool(&pool).await?;

    // Spawn through the OBSERVER pool -- the drained shared pool cannot
    // serve the insert. The commit's NOTIFY reaches this instance's
    // listener regardless of which pool wrote it, so the poller wakes,
    // sees due work and zero headroom, claims nothing, and arms the
    // waiter.
    let id = JobId::new();
    let mut op = es_entity::DbOp::init(&observer_pool).await?;
    spawner
        .spawn_in_op(
            &mut op,
            id,
            Cfg {
                label: "waiter".to_string(),
            },
        )
        .await?;
    op.commit().await?;

    // The clamped polls must leave the row untouched.
    wait_until(
        async || Ok(row_state(&observer_pool, id).await? == Some("pending".to_string())),
        "the row to sit pending under zero headroom",
    )
    .await?;

    // Free exactly one connection. The waiter's next backoff check sees
    // headroom, wakes the poll loop, and the claim + run + completion write
    // all proceed on that one freed connection.
    drop(held.pop());

    wait_until(
        async || Ok(runs.load(Ordering::SeqCst) == 1),
        "the job to run after a connection freed",
    )
    .await?;
    wait_until(
        async || Ok(row_state(&observer_pool, id).await?.is_none()),
        "the completed row to be deleted",
    )
    .await?;

    drop(held);
    Ok(())
}
