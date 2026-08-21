//! `Jobs::shutdown()` must fully drain: once it returns, no execution of this
//! instance may still be running, and none may have been force-aborted
//! mid-flight.
//!
//! The hazard is the poll loop, not the monitors. A `poll_and_dispatch()` in
//! flight when shutdown starts used to keep claiming and dispatching rows; a
//! generation created that late subscribes to the shutdown broadcast *after*
//! the `send`, and `tokio::sync::broadcast` never delivers to late subscribers
//! — so it never acked, was never waited for, and got force-aborted by
//! `kill_remaining_jobs` while its future was still live. Both writes then
//! landed on the same `Job` aggregate and one of them lost with
//! `ConcurrentModification`: either the execution's own completion (its work
//! silently discarded) or the kill itself (the error escaping
//! `Jobs::shutdown()`, as seen in lana PR #8282).
//!
//! Self-rescheduling jobs make that window easy to hit: they hand a row back to
//! `pending` and notify, so the loop is polling essentially continuously.

mod helpers;

use async_trait::async_trait;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobPollerConfig, JobRunner, JobSpawner,
    JobSvcConfig, JobType, Jobs,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Shared with the runners so they can report work done after `shutdown()`
/// already returned.
#[derive(Default)]
struct DrainWatch {
    shutdown_returned: AtomicBool,
    ran_after_shutdown: AtomicUsize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChurnConfig;

struct ChurnInitializer {
    job_type: JobType,
    watch: Arc<DrainWatch>,
}

impl JobInitializer for ChurnInitializer {
    type Config = ChurnConfig;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(ChurnRunner {
            watch: Arc::clone(&self.watch),
        }))
    }
}

struct ChurnRunner {
    watch: Arc<DrainWatch>,
}

#[async_trait]
impl JobRunner for ChurnRunner {
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // `set_result` is an ordinary entity write that does NOT touch the
        // `job_executions` row, so it is not serialized behind the row locks
        // `kill_remaining_jobs` holds — it is the write that collides.
        for i in 0..8u32 {
            current_job.set_result(&i).await?;
            if self.watch.shutdown_returned.load(Ordering::SeqCst) {
                self.watch.ran_after_shutdown.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(JobCompletion::RescheduleNow)
    }
}

/// Shutting down under continuous self-rescheduling churn must leave nothing
/// running, abort nothing mid-flight, and return `Ok`.
///
/// Before the poll loop was stopped and drained ahead of the monitor broadcast,
/// each iteration force-aborted roughly half of the live executions
/// (`aborted_events` 18-22 of 40) and logged one `ConcurrentModification` per
/// aborted generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drains_self_rescheduling_jobs() -> anyhow::Result<()> {
    let pg_con = std::env::var("PG_CON").unwrap();
    // Every job holds a connection for its own writes, so this pool is sized
    // for the whole churn set rather than taking the shared test default.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(25)
        .connect(&pg_con)
        .await?;

    const N_JOBS: usize = 20;

    for iteration in 0..5u32 {
        let watch = Arc::new(DrainWatch::default());

        let config = JobSvcConfig::builder()
            .pool(pool.clone())
            .poller_config(JobPollerConfig {
                // Poll as fast as the churn allows: the tighter the cadence,
                // the likelier a poll is in flight when shutdown starts.
                ..Default::default()
            })
            .build()
            .expect("build JobSvcConfig");

        let mut jobs = Jobs::init(config).await?;
        // Unique per iteration: rows left pending by an earlier iteration (or
        // an earlier run) must not be re-claimed here.
        let job_type = helpers::job_type("shutdown-drain");
        let spawner = jobs.add_initializer(ChurnInitializer {
            job_type,
            watch: Arc::clone(&watch),
        });
        jobs.start_poll().await?;

        let mut ids = Vec::with_capacity(N_JOBS);
        for _ in 0..N_JOBS {
            let id = JobId::new();
            spawner.spawn(id, ChurnConfig).await?;
            ids.push(uuid::Uuid::from(id));
        }

        // Let the churn reach a steady state, so the poll loop is busy rather
        // than parked when shutdown starts.
        tokio::time::sleep(Duration::from_millis(300)).await;

        jobs.shutdown()
            .await
            .unwrap_or_else(|e| panic!("iteration {iteration}: shutdown failed: {e}"));
        watch.shutdown_returned.store(true, Ordering::SeqCst);

        let still_running: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM job_executions WHERE id = ANY($1) AND state = 'running'",
        )
        .bind(&ids)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            still_running, 0,
            "iteration {iteration}: executions left running after shutdown"
        );

        // A forced abort means the drain missed a live execution: every
        // generation should have acked and finished on its own.
        let aborted: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM job_events \
             WHERE id = ANY($1) AND event->>'type' = 'execution_aborted'",
        )
        .bind(&ids)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            aborted, 0,
            "iteration {iteration}: {aborted} execution(s) were force-aborted mid-flight \
             instead of being drained"
        );

        // Any work observed after `shutdown()` returned means an execution
        // outlived the shutdown it was supposed to be waited for.
        let events_at_return: i64 =
            sqlx::query_scalar("SELECT count(*) FROM job_events WHERE id = ANY($1)")
                .bind(&ids)
                .fetch_one(&pool)
                .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let events_after: i64 =
            sqlx::query_scalar("SELECT count(*) FROM job_events WHERE id = ANY($1)")
                .bind(&ids)
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            events_at_return, events_after,
            "iteration {iteration}: job events kept being written after shutdown returned"
        );
        assert_eq!(
            watch.ran_after_shutdown.load(Ordering::SeqCst),
            0,
            "iteration {iteration}: a job runner was still executing after shutdown returned"
        );
    }

    Ok(())
}
