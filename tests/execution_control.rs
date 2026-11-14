use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use es_entity::{DbOp, clock::ClockHandle};
use job::*;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::sync::Semaphore;

struct Initializer {
    job_type: JobType,
    started: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}
impl JobInitializer for Initializer {
    type Config = ();
    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }
    fn init(
        &self,
        _: &Job,
        _: JobSpawner<()>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(Runner {
            started: self.started.clone(),
            release: self.release.clone(),
        }))
    }
}
struct Runner {
    started: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl JobRunner for Runner {
    async fn run(
        &self,
        _: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.acquire().await?.forget();
        Ok(JobCompletion::Complete)
    }
}

async fn worker(
    pool: &PgPool,
    started: &Arc<AtomicUsize>,
    release: &Arc<Semaphore>,
) -> anyhow::Result<(Jobs, JobSpawner<()>, JobSpawner<()>)> {
    let mut jobs = Jobs::init(
        JobSvcConfig::builder()
            .pool(pool.clone())
            .execution_control(true)
            .build()?,
    )
    .await?;
    let business = jobs.add_initializer(Initializer {
        job_type: JobType::new("business"),
        started: started.clone(),
        release: release.clone(),
    });
    let infra = jobs.add_initializer(Initializer {
        job_type: JobType::new("relay"),
        started: Arc::new(AtomicUsize::new(0)),
        release: Arc::new(Semaphore::new(100)),
    });
    jobs.start_poll().await?;
    Ok((jobs, business, infra))
}

async fn status(jobs: &Jobs, pool: &PgPool) -> anyhow::Result<QuiescenceStatus> {
    let mut op = DbOp::init_with_clock(pool, &ClockHandle::realtime()).await?;
    let status = jobs
        .execution_control()
        .unwrap()
        .status_in_op(&mut op)
        .await?;
    op.commit().await?;
    Ok(status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selective_pause_is_durable_drains_running_work_and_does_not_spend_attempts()
-> anyhow::Result<()> {
    let options: PgConnectOptions = std::env::var("PG_CON")?.parse()?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await?;
    let database = format!("job_quiescence_{}", uuid::Uuid::now_v7().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect_with(options.database(&database))
        .await?;
    let setup = Jobs::init(
        JobSvcConfig::builder()
            .pool(pool.clone())
            .exec_migrations(true)
            .build()?,
    )
    .await?;
    drop(setup);
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let (jobs, business, infra) = worker(&pool, &started, &release).await?;
    let (peer, _, _) = worker(&pool, &started, &release).await?;
    let first = business.spawn(JobId::new(), ()).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    let mut op = DbOp::init_with_clock(&pool, &ClockHandle::realtime()).await?;
    jobs.execution_control()
        .unwrap()
        .pause_in_op(&mut op, &[JobType::new("relay")])
        .await?;
    // The spawn-side short circuit must wait for the pause transaction,
    // then observe its new policy rather than a pre-wait snapshot.
    let raced = {
        let racing = business.spawn(JobId::new(), ());
        tokio::pin!(racing);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut racing)
                .await
                .is_err()
        );
        op.commit().await?;
        racing.await?
    };
    assert!(!status(&jobs, &pool).await?.is_quiesced());
    let mut queued = vec![raced];
    for _ in 0..2 {
        queued.push(business.spawn(JobId::new(), ()).await?);
    }
    infra
        .spawn(JobId::new(), ())
        .await?
        .await_completion(Duration::from_secs(10))
        .await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    for handle in &queued {
        assert!(matches!(
            handle.load().await?.state(),
            JobStatus::Pending {
                attempt: 1,
                last_error: None,
                ..
            }
        ));
    }

    release.add_permits(1);
    first.await_completion(Duration::from_secs(10)).await?;
    assert!(status(&jobs, &pool).await?.is_quiesced());
    let (restarted, _, _) = worker(&pool, &started, &release).await?;
    infra
        .spawn(JobId::new(), ())
        .await?
        .await_completion(Duration::from_secs(10))
        .await?;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert!(status(&restarted, &pool).await?.is_quiesced());

    let mut op = DbOp::init_with_clock(&pool, &ClockHandle::realtime()).await?;
    jobs.execution_control()
        .unwrap()
        .resume_in_op(&mut op)
        .await?;
    op.commit().await?;
    release.add_permits(3);
    for handle in queued {
        assert!(
            handle
                .await_completion(Duration::from_secs(10))
                .await?
                .is_completed()
        );
    }
    assert_eq!(started.load(Ordering::SeqCst), 4);
    assert!(!status(&jobs, &pool).await?.is_quiesced());
    jobs.shutdown().await?;
    peer.shutdown().await?;
    restarted.shutdown().await?;
    drop((jobs, peer, restarted, business, infra));
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await?;
    admin.close().await;
    Ok(())
}
