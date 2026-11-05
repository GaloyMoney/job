//! Diagnostic reproduction for the remaining listener-admission gap in #200.
//! This pins the current starvation behavior; it is not the proposed fix.

mod helpers;

use async_trait::async_trait;
use job::*;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::sync::{Notify, mpsc};

type RunnerError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Serialize, Deserialize)]
struct Config;

struct ListenerInit {
    kind: JobType,
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

impl KeyedJobInitializer for ListenerInit {
    type Config = Config;
    fn job_type(&self) -> JobType {
        self.kind.clone()
    }
    fn init(&self, _: &Job, _: KeyedJobSpawner<Config>) -> Result<Box<dyn JobRunner>, RunnerError> {
        Ok(Box::new(Listener {
            started: self.started.clone(),
            release: self.release.clone(),
        }))
    }
}

struct Listener {
    started: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

#[async_trait]
impl JobRunner for Listener {
    async fn run(&self, mut current: CurrentJob) -> Result<JobCompletion, RunnerError> {
        self.started.send(())?;
        tokio::select! {
            _ = self.release.notified() => {},
            _ = current.shutdown_requested() => {},
        }
        Ok(JobCompletion::Complete)
    }
}

struct CanaryInit {
    kind: JobType,
    ran: Arc<Notify>,
}
impl JobInitializer for CanaryInit {
    type Config = Config;
    fn job_type(&self) -> JobType {
        self.kind.clone()
    }
    fn init(&self, _: &Job, _: JobSpawner<Config>) -> Result<Box<dyn JobRunner>, RunnerError> {
        Ok(Box::new(Canary(self.ran.clone())))
    }
}
struct Canary(Arc<Notify>);
#[async_trait]
impl JobRunner for Canary {
    async fn run(&self, _: CurrentJob) -> Result<JobCompletion, RunnerError> {
        self.0.notify_one();
        Ok(JobCompletion::Complete)
    }
}

#[tokio::test]
#[ignore = "diagnostic for #200; records starvation until a listener-admission design is agreed"]
async fn keyed_listeners_block_transient_work_until_one_exits() -> anyhow::Result<()> {
    let base = std::env::var("PG_CON")?;
    let database = format!("job_listener_repro_{}", uuid::Uuid::now_v7().simple());
    let admin = sqlx::PgPool::connect(&base).await?;
    sqlx::query(&format!("CREATE DATABASE \"{database}\""))
        .execute(&admin)
        .await?;
    admin.close().await;
    let options = sqlx::postgres::PgConnectOptions::from_str(&base)?.database(&database);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect_with(options)
        .await?;
    let config = JobSvcConfig::builder()
        .pool(pool.clone())
        .exec_migrations(true)
        .poller_config(JobPollerConfig {
            min_jobs_per_process: 2,
            max_jobs_per_process: 2,
            ..Default::default()
        })
        .build()?;
    let mut jobs = Jobs::init(config).await?;
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let canary_ran = Arc::new(Notify::new());
    let listeners = jobs.add_keyed_initializer(ListenerInit {
        kind: helpers::job_type("listener-repro"),
        started: started_tx,
        release: release.clone(),
    });
    let canary = jobs.add_initializer(CanaryInit {
        kind: helpers::job_type("canary-repro"),
        ran: canary_ran.clone(),
    });
    jobs.start_poll().await?;
    for key in ["endpoint-a", "endpoint-b"] {
        listeners.spawn(key, Config).await?;
        tokio::time::timeout(Duration::from_secs(10), started_rx.recv())
            .await?
            .unwrap();
    }
    let canary_id = JobId::new();
    canary.spawn(canary_id, Config).await?;
    let blocked = tokio::time::timeout(Duration::from_secs(2), canary_ran.notified())
        .await
        .is_err();
    let (state,): (String,) =
        sqlx::query_as("SELECT state::text FROM job_executions WHERE id = $1")
            .bind(canary_id)
            .fetch_one(&pool)
            .await?;
    let idle = pool.num_idle();
    println!(
        "listeners_running=2, transient_executed={}, transient_state={state}, idle_connections={idle}",
        !blocked
    );
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), canary_ran.notified()).await?;
    println!("after releasing one listener: transient executed");
    jobs.shutdown().await?;
    assert!(
        blocked,
        "the current behavior should reproduce the reported starvation"
    );
    assert_eq!(state, "pending");
    assert!(
        idle > 0,
        "the reproduction must not depend on database pool exhaustion"
    );
    Ok(())
}
