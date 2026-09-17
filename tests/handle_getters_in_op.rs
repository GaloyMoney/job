//! `_in_op` twins of the handle-getters that do a DB read to resolve a
//! handle (`Jobs::keyed_handle`, `Jobs::keyed_handles`,
//! `Jobs::resident_handle`): each takes any `es_entity::IntoOneTimeExecutor`
//! (a pool reference, or an in-flight operation) instead of always reading
//! through the pool.
//!
//! `Jobs::handle`/`Jobs::handles` are deliberately not given `_in_op` twins:
//! they mint a handle in memory with no I/O at all (see their own doc
//! comments), so there is no executor for an `_in_op` variant to use.
//!
//! The property under test is the same for every twin, and it is the whole
//! point of taking `IntoOneTimeExecutor` rather than defaulting to the pool
//! internally: the `_in_op` read genuinely runs on the SAME connection the
//! caller is transacting on, so it sees that transaction's own uncommitted
//! writes, which a pool-based read (a separate connection, READ COMMITTED)
//! cannot. Proven directly rather than assumed: read via the pool first
//! (must miss), read via the op (must hit), commit, read via the pool again
//! (must hit).
//!
//! Each test also pins `JobHandle::created()` on the `_in_op` read: `false`,
//! same as every other lookup path in `JobHandle::created`'s doc table --
//! these are lookups, not spawns, so nothing was created by the call that
//! produced the handle.

mod helpers;

use async_trait::async_trait;
use es_entity::AtomicOperation;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobSvcConfig, JobType, Jobs, KeyedJobInitializer,
    KeyedJobSpawner,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cfg;

struct Idle;

#[async_trait]
impl job::JobRunner for Idle {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        Ok(JobCompletion::Complete)
    }
}

struct Init {
    job_type: JobType,
}

impl KeyedJobInitializer for Init {
    type Config = Cfg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _: &Job,
        _: KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(Idle))
    }
}

/// `keyed_handle_in_op` sees a keyed spawn made earlier in the SAME
/// uncommitted operation; `keyed_handle` (pool-based) does not, until that
/// operation commits.
#[tokio::test]
async fn keyed_handle_in_op_sees_its_own_uncommitted_transaction() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("handle-in-op-keyed");
    let spawner = jobs.add_keyed_initializer(Init {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    let spawned = spawner.spawn_in_op(&mut op, "k", Cfg).await?;

    assert!(
        jobs.keyed_handle(job_type.clone(), "k").await?.is_none(),
        "a pool-based read must not see the still-open transaction's write"
    );
    let seen_in_op = jobs
        .keyed_handle_in_op(&mut op, job_type.clone(), "k")
        .await?
        .expect("the in-op read must see its own transaction's uncommitted write");
    assert_eq!(seen_in_op.id(), spawned.id());
    assert!(
        !seen_in_op.created(),
        "a lookup handle must report created() == false, same as every other lookup path"
    );

    op.commit().await?;

    let seen_after_commit = jobs
        .keyed_handle(job_type.clone(), "k")
        .await?
        .expect("now committed, the pool-based read must see it too");
    assert_eq!(seen_after_commit.id(), spawned.id());

    jobs.shutdown().await?;
    Ok(())
}

/// Same property, for the list form: `keyed_handles_in_op` sees an
/// uncommitted spawn from the same operation; `keyed_handles` does not until
/// it commits.
#[tokio::test]
async fn keyed_handles_in_op_sees_its_own_uncommitted_transaction() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let mut jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("handle-in-op-keyed-list");
    let spawner = jobs.add_keyed_initializer(Init {
        job_type: job_type.clone(),
    });

    let mut op = es_entity::DbOp::init(&pool).await?;
    let spawned = spawner.spawn_in_op(&mut op, "k", Cfg).await?;

    let before = jobs.keyed_handles(job_type.clone()).await?;
    assert!(
        before.is_empty(),
        "a pool-based read must not see the still-open transaction's write"
    );

    let in_op = jobs.keyed_handles_in_op(&mut op, job_type.clone()).await?;
    assert_eq!(in_op.len(), 1);
    assert_eq!(in_op[0].id(), spawned.id());
    assert!(
        !in_op[0].created(),
        "a lookup handle must report created() == false, same as every other lookup path"
    );

    op.commit().await?;

    let after = jobs.keyed_handles(job_type.clone()).await?;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id(), spawned.id());

    jobs.shutdown().await?;
    Ok(())
}

/// Same property for `resident_handle_in_op`. There is no in-op resident
/// spawn to exercise (`ResidentJobSpawner::spawn` always commits its own
/// operation), so the row is written by hand inside an open op, matching
/// exactly the shape a real resident spawn leaves in `jobs`
/// (`id`, `job_type`, `resident = true`).
#[tokio::test]
async fn resident_handle_in_op_sees_its_own_uncommitted_transaction() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let config = JobSvcConfig::builder().pool(pool.clone()).build().unwrap();
    let jobs = Jobs::init(config).await?;
    let job_type = helpers::job_type("handle-in-op-resident");
    let id = JobId::new();

    let mut op = es_entity::DbOp::init(&pool).await?;
    sqlx::query("INSERT INTO jobs (id, job_type, resident) VALUES ($1, $2, true)")
        .bind(uuid::Uuid::from(id))
        .bind(job_type.as_str())
        .execute(op.as_executor())
        .await?;

    assert!(
        jobs.resident_handle(job_type.clone()).await?.is_none(),
        "a pool-based read must not see the still-open transaction's write"
    );
    let seen_in_op = jobs
        .resident_handle_in_op(&mut op, job_type.clone())
        .await?
        .expect("the in-op read must see its own transaction's uncommitted write");
    assert_eq!(seen_in_op.id(), id);
    assert!(
        !seen_in_op.created(),
        "a lookup handle must report created() == false, same as every other lookup path"
    );

    op.commit().await?;

    let seen_after_commit = jobs
        .resident_handle(job_type.clone())
        .await?
        .expect("now committed, the pool-based read must see it too");
    assert_eq!(seen_after_commit.id(), id);

    Ok(())
}
