//! Drain-then-kill shutdown: stop the poll loop and wait for it to actually
//! exit -- since `tokio::sync::broadcast` only reaches receivers subscribed
//! before the send, this makes the set of live executions final before
//! broadcasting a shutdown signal to their monitors and collecting acks.
//! After waiting out the shutdown timeout, `kill_remaining_jobs` force-
//! releases whatever is still `running`, using a savepoint per entity
//! append so one lost audit race cannot fail the whole shutdown (the row
//! release itself is already durable).
//!
//! The sequence is idempotent via a CAS on `shutdown_called`, shared with
//! the poller's `shutdown_started` so completion-time recycle claims stop
//! re-admitting work mid-drain; dropping the handle runs the same
//! sequence.

use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use tracing::instrument;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::{error::JobError, repo::JobRepo};

use super::{JobId, JobPollerHandle};

pub(super) struct ShutdownCoordinator {
    pub(super) shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    pub(super) poll_stop_tx: tokio::sync::watch::Sender<bool>,
    pub(super) poll_exited_rx: tokio::sync::watch::Receiver<bool>,
    pub(super) shutdown_called: Arc<AtomicBool>,
    pub(super) shutdown_timeout: Duration,
    pub(super) max_jobs_per_process: usize,
    pub(super) repo: Arc<JobRepo>,
    pub(super) instance_id: uuid::Uuid,
    pub(super) clock: ClockHandle,
}

impl JobPollerHandle {
    pub async fn shutdown(&self) -> Result<(), JobError> {
        self.shutdown.perform().await
    }
}

impl Drop for JobPollerHandle {
    fn drop(&mut self) {
        let shutdown = Arc::clone(&self.shutdown);
        spawn_named_task!("job-poller-shutdown-on-drop", async move {
            let _ = shutdown.perform().await;
        });
    }
}

impl ShutdownCoordinator {
    #[instrument(
        name = "jobs.perform_shutdown",
        skip(self),
        fields(
            instance_id = %self.instance_id,
            poll_loop_stopped,
            broadcast_ok,
            n_responses
        )
    )]
    pub(super) async fn perform(&self) -> Result<(), JobError> {
        if self
            .shutdown_called
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let poll_loop_stopped = self.stop_poll_loop().await;
        tracing::Span::current().record("poll_loop_stopped", poll_loop_stopped);

        let (send, mut recv) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Receiver<()>>(
            self.max_jobs_per_process,
        );

        let broadcast_ok = self.shutdown_tx.send(send).is_ok();
        tracing::Span::current().record("broadcast_ok", broadcast_ok);

        if broadcast_ok {
            let mut receivers = Vec::with_capacity(self.max_jobs_per_process);
            let receive_timeout = Duration::from_millis(100);

            tracing::info!("Starting to collect shutdown acknowledgements from job monitors");

            loop {
                match tokio::time::timeout(receive_timeout, recv.recv()).await {
                    Ok(Some(oneshot_rx)) => {
                        receivers.push(oneshot_rx);
                        tracing::info!(
                            n_collected = receivers.len(),
                            "Received acknowledgement from monitor task"
                        );
                    }
                    Ok(None) => {
                        tracing::info!(
                            n_collected = receivers.len(),
                            "Channel closed, all monitors responded"
                        );
                        break;
                    }
                    Err(_) => {
                        tracing::warn!(
                            n_collected = receivers.len(),
                            "Receive timeout expired, moving on with collected responses"
                        );
                        break;
                    }
                }
            }

            tracing::Span::current().record("n_responses", receivers.len());

            tracing::info!(
                n_responses = receivers.len(),
                "Waiting for all acknowledged jobs to complete"
            );

            if tokio::time::timeout(self.shutdown_timeout, futures::future::join_all(receivers))
                .await
                .is_err()
            {
                tracing::warn!("Some jobs did not signal completion within shutdown timeout");
            } else {
                tracing::info!("All acknowledged jobs completed");
            }
        } else {
            tracing::info!("No live job monitors at shutdown, nothing to drain");
        }

        kill_remaining_jobs(Arc::clone(&self.repo), self.instance_id, self.clock.clone()).await
    }

    async fn stop_poll_loop(&self) -> bool {
        let _ = self.poll_stop_tx.send(true);

        let mut exited_rx = self.poll_exited_rx.clone();
        let exited = async {
            loop {
                let already_exited = *exited_rx.borrow_and_update();
                if already_exited {
                    return;
                }
                if exited_rx.changed().await.is_err() {
                    return;
                }
            }
        };

        match tokio::time::timeout(self.shutdown_timeout, exited).await {
            Ok(()) => {
                tracing::info!("Poll loop stopped, no further jobs will be dispatched");
                true
            }
            Err(_) => {
                tracing::warn!(
                    "Poll loop did not stop within shutdown timeout, continuing shutdown"
                );
                false
            }
        }
    }
}

#[instrument(name = "jobs.kill_remaining_jobs", skip(repo, clock), fields(instance_id = %instance_id, n_killed = tracing::field::Empty, n_conflicts = tracing::field::Empty))]
async fn kill_remaining_jobs(
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
) -> Result<(), JobError> {
    let mut op = repo.begin_op_with_clock(&clock).await?;
    let now = clock.now();
    let rows = sqlx::query!(
        r#"
        WITH locked AS MATERIALIZED (
            SELECT je.id FROM job_executions je
            WHERE je.poller_instance_id = $2 AND je.state = 'running'
            ORDER BY je.queue_id, je.id
            FOR NO KEY UPDATE
        )
        UPDATE job_executions je
        SET state = 'pending',
            execute_at = $1,
            poller_instance_id = NULL
        FROM locked l WHERE je.id = l.id
        RETURNING je.id as "id!: JobId", je.attempt_index
        "#,
        now,
        instance_id
    )
    .fetch_all(op.as_executor())
    .await?;

    let n_killed = rows.len();
    tracing::Span::current().record("n_killed", n_killed);

    if n_killed == 0 {
        return Ok(());
    }

    let attempt_map: std::collections::HashMap<JobId, u32> = rows
        .into_iter()
        .map(|r| (r.id, r.attempt_index as u32))
        .collect();

    let ids: Vec<JobId> = attempt_map.keys().copied().collect();
    let entities: std::collections::HashMap<JobId, crate::Job> =
        repo.find_all_in_op(&mut op, &ids).await?;

    let mut n_conflicts = 0usize;
    for (job_id, mut job) in entities {
        let attempt_index = attempt_map[&job_id];

        tracing::warn!(
            job_id = %job_id,
            job_type = %job.job_type,
            attempt = attempt_index,
            "Job still running after shutdown timeout, forcing reschedule"
        );

        job.abort_execution("killed job".to_string(), now, attempt_index);
        if let Err(e) = op
            .with_savepoint(async |sp| repo.update_in_op(sp, &mut job).await)
            .await?
        {
            n_conflicts += 1;
            tracing::warn!(
                job_id = %job_id,
                attempt = attempt_index,
                exception.message = %e,
                exception.type = std::any::type_name_of_val(&e),
                "Could not record forced reschedule; execution row released anyway"
            );
        }
    }
    tracing::Span::current().record("n_conflicts", n_conflicts);
    op.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::test_support::init_pool;
    use super::*;
    use crate::JobType;
    use sqlx::postgres::PgPool;

    async fn seed_running_entity(
        pool: &PgPool,
        repo: &JobRepo,
        job_type: &str,
        instance_id: uuid::Uuid,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let new_job = crate::entity::NewJob::builder()
            .id(id)
            .job_type(JobType::from_owned(job_type.to_string()))
            .config(serde_json::json!({}))?
            .schedule_at(chrono::Utc::now())
            .build()
            .expect("build NewJob");
        repo.create(new_job).await?;

        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
             VALUES ($1, $2, 'running', $3, 1, $4, $4)",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type)
        .bind(instance_id)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    async fn wait_for_blocked_backend(pool: &PgPool) -> anyhow::Result<()> {
        for _ in 0..600 {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(pool)
            .await?;
            if blocked > 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::bail!("no backend ever blocked on a lock");
    }

    #[tokio::test]
    async fn kill_remaining_jobs_survives_losing_a_concurrent_entity_write() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = Arc::new(JobRepo::new(&pool));
        let clock = ClockHandle::realtime();
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("kill-race-{}", uuid::Uuid::now_v7());

        let id = seed_running_entity(&pool, &repo, &job_type, instance_id).await?;

        let mut writer_op = repo.begin_op_with_clock(&clock).await?;
        let mut job = repo.find_by_id_in_op(&mut writer_op, id).await?;
        let return_value = crate::outcome::JobReturnValue::try_from(&"progress")?;
        assert!(job.update_return_value(return_value).did_execute());
        repo.update_in_op(&mut writer_op, &mut job).await?;

        let kill = tokio::spawn(kill_remaining_jobs(
            Arc::clone(&repo),
            instance_id,
            clock.clone(),
        ));
        wait_for_blocked_backend(&pool).await?;

        writer_op.commit().await?;

        kill.await?
            .expect("shutdown must not fail because a forced-reschedule append lost a race");

        let row: (String, Option<uuid::Uuid>) = sqlx::query_as(
            "SELECT state::text, poller_instance_id FROM job_executions WHERE id = $1",
        )
        .bind(uuid::Uuid::from(id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, "pending", "execution must be released for reclaim");
        assert_eq!(row.1, None, "released execution must not stay owned");

        Ok(())
    }
}
