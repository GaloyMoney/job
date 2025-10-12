use es_entity::AtomicOperation;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgListener, PgPool, types::PgInterval};
use tracing::{Span, instrument};

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{
    JobId, config::JobPollerConfig, dispatcher::*, error::JobError, handle::OwnedTaskHandle,
    registry::JobRegistry, repo::JobRepo, tracker::JobTracker,
};

pub(crate) struct JobPoller {
    config: JobPollerConfig,
    repo: JobRepo,
    registry: JobRegistry,
    tracker: Arc<JobTracker>,
    instance_id: uuid::Uuid,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

pub(crate) struct JobPollerHandle {
    #[allow(dead_code)]
    poller: Arc<JobPoller>,
    #[allow(dead_code)]
    handle: OwnedTaskHandle,
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    shutdown_called: Arc<AtomicBool>,
    repo: JobRepo,
    instance_id: uuid::Uuid,
}

const MAX_WAIT: Duration = Duration::from_secs(60);

impl JobPoller {
    pub fn new(config: JobPollerConfig, repo: JobRepo, registry: JobRegistry) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        Self {
            tracker: Arc::new(JobTracker::new(
                config.min_jobs_per_process,
                config.max_jobs_per_process,
            )),
            repo,
            config,
            registry,
            instance_id: uuid::Uuid::now_v7(),
            shutdown_tx,
        }
    }

    pub async fn start(self) -> Result<JobPollerHandle, sqlx::Error> {
        let listener_handle = self.start_listener().await?;
        let lost_handle = self.start_lost_handler();
        let keep_alive_handle = self.start_keep_alive_handler();
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = self.repo.clone();
        let instance_id = self.instance_id;
        let executor = Arc::new(self);
        let handle = OwnedTaskHandle::new(tokio::task::spawn(Self::main_loop(
            Arc::clone(&executor),
            listener_handle,
            lost_handle,
            keep_alive_handle,
        )));
        Ok(JobPollerHandle {
            poller: executor,
            handle,
            shutdown_tx,
            shutdown_called: Arc::new(AtomicBool::new(false)),
            repo,
            instance_id,
        })
    }

    async fn main_loop(
        self: Arc<Self>,
        _listener_task: OwnedTaskHandle,
        _lost_task: OwnedTaskHandle,
        _keep_alive_task: OwnedTaskHandle,
    ) {
        let mut failures = 0;
        let mut woken_up = false;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            let timeout = match self.poll_and_dispatch(woken_up).await {
                Ok(duration) => {
                    failures = 0;
                    duration
                }
                Err(e) => {
                    failures += 1;
                    eprintln!("job.main_loop errored {e} ({failures})");
                    Duration::from_millis(50 << failures)
                }
            };

            tokio::select! {
                biased;

                _ = shutdown_rx.recv() => {
                    break;
                }
                result = crate::time::timeout(timeout, self.tracker.notified()) => {
                    woken_up = result.is_ok();
                }
            }
        }
    }

    #[instrument(
        name = "job.poll_and_dispatch",
        level = "trace",
        skip(self),
        fields(n_jobs_running, n_jobs_to_start, now, next_poll_in),
        err
    )]
    async fn poll_and_dispatch(self: &Arc<Self>, woken_up: bool) -> Result<Duration, JobError> {
        let span = Span::current();
        let Some(n_jobs_to_poll) = self.tracker.next_batch_size() else {
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        };
        let rows = match poll_jobs(self.repo.pool(), n_jobs_to_poll, self.instance_id).await? {
            JobPollResult::WaitTillNextJob(duration) => {
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs(jobs) => jobs,
        };
        span.record("n_jobs_to_start", rows.len());
        if !rows.is_empty() {
            for row in rows {
                self.dispatch_job(row).await?;
            }
        }

        span.record("next_poll_in", tracing::field::debug(Duration::ZERO));
        Ok(Duration::ZERO)
    }

    async fn start_listener(&self) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = PgListener::connect_with(self.repo.pool()).await?;
        listener.listen("job_execution").await?;
        let tracker = self.tracker.clone();
        Ok(OwnedTaskHandle::new(tokio::task::spawn(async move {
            loop {
                if listener.recv().await.is_ok() {
                    tracker.job_execution_inserted();
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        })))
    }

    fn start_lost_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        OwnedTaskHandle::new(tokio::task::spawn(async move {
            loop {
                crate::time::sleep(job_lost_interval / 2).await;
                let now = crate::time::now();
                let check_time = now - job_lost_interval;
                if let Ok(rows) = sqlx::query!(
                    r#"
            UPDATE job_executions
            SET state = 'pending', execute_at = $1, attempt_index = attempt_index + 1, poller_instance_id = NULL
            WHERE state = 'running' AND alive_at < $1::timestamptz
            RETURNING id as id
            "#,
                    check_time,
                )
                .fetch_all(&pool)
                .await
                    && !rows.is_empty()
                {
                    eprintln!(
                        "job.lost_job: {}",
                        rows.into_iter()
                            .map(|r| r.id.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }))
    }

    fn start_keep_alive_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let instance_id = self.instance_id;
        OwnedTaskHandle::new(tokio::task::spawn(async move {
            let mut failures = 0;
            loop {
                let now = crate::time::now();
                let timeout = match sqlx::query!(
                    r#"
                    UPDATE job_executions
                    SET alive_at = $1
                    WHERE poller_instance_id = $2 AND state = 'running'
                    "#,
                    now,
                    instance_id,
                )
                .execute(&pool)
                .await
                {
                    Ok(_) => {
                        failures = 0;
                        job_lost_interval / 4
                    }
                    Err(e) => {
                        failures += 1;
                        eprintln!("Keep alive for instance {instance_id} errored: {e}");
                        Duration::from_millis(50 << failures)
                    }
                };
                crate::time::sleep(timeout).await;
            }
        }))
    }

    #[instrument(
        name = "job.dispatch_job",
        skip(self, polled_job),
        fields(job_id, job_type, attempt, now),
        err
    )]
    async fn dispatch_job(&self, polled_job: PolledJob) -> Result<(), JobError> {
        let span = Span::current();
        span.record("attempt", polled_job.attempt);
        let job = self.repo.find_by_id(polled_job.id).await?;
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        let runner = self.registry.init_job(&job)?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let repo = self.repo.clone();
        let tracker = self.tracker.clone();
        let instance_id = self.instance_id;
        span.record("now", tracing::field::display(crate::time::now()));

        let job_handle = tokio::spawn(async move {
            let id = job.id;
            let attempt = polled_job.attempt;
            if let Err(e) =
                JobDispatcher::new(repo, tracker, retry_settings, job.id, runner, instance_id)
                    .execute_job(polled_job)
                    .await
            {
                eprintln!("JobDispatcher.execute_job {id} ({attempt}) returned error {e}")
            }
        });

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            if shutdown_rx.recv().await.is_ok() {
                job_handle.abort();
            }
        });

        Ok(())
    }
}

async fn poll_jobs(
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
) -> Result<JobPollResult, sqlx::Error> {
    let now = crate::time::now();
    Span::current().record("now", tracing::field::display(now));

    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        WITH min_wait AS (
            SELECT MIN(execute_at) - $2::timestamptz AS wait_time
            FROM job_executions
            WHERE state = 'pending'
            AND execute_at > $2::timestamptz
        ),
        selected_jobs AS (
            SELECT je.id, je.execution_state_json AS data_json, je.attempt_index
            FROM job_executions je
            JOIN jobs ON je.id = jobs.id
            WHERE execute_at <= $2::timestamptz
            AND je.state = 'pending'
            ORDER BY execute_at ASC
            LIMIT $1
            FOR UPDATE
        ),
        updated AS (
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $2, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
            RETURNING je.id, selected_jobs.data_json, je.attempt_index
        )
        SELECT * FROM (
            SELECT 
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                NULL::INTERVAL AS "max_wait?: PgInterval"
            FROM updated u
            UNION ALL
            SELECT 
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                mw.wait_time AS "max_wait?: PgInterval"
            FROM min_wait mw
            WHERE NOT EXISTS (SELECT 1 FROM updated)
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        now,
        instance_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(JobPollResult::from_rows(rows))
}

#[derive(Debug)]
enum JobPollResult {
    Jobs(Vec<PolledJob>),
    WaitTillNextJob(Duration),
}

#[derive(Debug)]
struct JobPollRow {
    id: Option<JobId>,
    data_json: Option<JsonValue>,
    attempt_index: Option<i32>,
    max_wait: Option<PgInterval>,
}

impl JobPollResult {
    /// Convert raw query rows into a JobPollResult
    pub fn from_rows(rows: Vec<JobPollRow>) -> Self {
        if rows.is_empty() {
            JobPollResult::WaitTillNextJob(MAX_WAIT)
        } else if rows.len() == 1 && rows[0].id.is_none() {
            if let Some(interval) = &rows[0].max_wait {
                JobPollResult::WaitTillNextJob(pg_interval_to_duration(interval))
            } else {
                JobPollResult::WaitTillNextJob(MAX_WAIT)
            }
        } else {
            let jobs = rows
                .into_iter()
                .filter_map(|row| {
                    if let (Some(id), Some(attempt_index)) = (row.id, row.attempt_index) {
                        Some(PolledJob {
                            id,
                            data_json: row.data_json,
                            attempt: attempt_index as u32,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            JobPollResult::Jobs(jobs)
        }
    }
}

fn pg_interval_to_duration(interval: &PgInterval) -> Duration {
    const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
    if interval.microseconds < 0 || interval.days < 0 || interval.months < 0 {
        Duration::default()
    } else {
        let days = (interval.days as u64) + (interval.months as u64) * 30;
        Duration::from_micros(interval.microseconds as u64)
            + Duration::from_secs(days * SECONDS_PER_DAY)
    }
}

impl JobPollerHandle {
    /// Gracefully shut down the job poller.
    ///
    /// This method is idempotent and can be called multiple times safely.
    /// It will:
    /// 1. Send shutdown signal to all running job tasks
    /// 2. Wait briefly for tasks to complete naturally
    /// 3. Reschedule any jobs still running for this instance
    ///
    /// If not called manually, it will be called automatically when the handle is dropped.
    pub async fn shutdown(&self) -> Result<(), JobError> {
        perform_shutdown(
            self.shutdown_tx.clone(),
            self.repo.clone(),
            self.instance_id,
            self.shutdown_called.clone(),
        )
        .await
    }
}

impl Drop for JobPollerHandle {
    fn drop(&mut self) {
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = self.repo.clone();
        let instance_id = self.instance_id;
        let shutdown_called = self.shutdown_called.clone();

        tokio::spawn(async move {
            let _ = perform_shutdown(shutdown_tx, repo, instance_id, shutdown_called).await;
        });
    }
}

#[instrument(name = "jobs.shutdown", skip(shutdown_tx, repo), fields(n_jobs))]
async fn perform_shutdown(
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    repo: JobRepo,
    instance_id: uuid::Uuid,
    shutdown_called: Arc<AtomicBool>,
) -> Result<(), JobError> {
    if shutdown_called
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Ok(());
    }

    let _ = shutdown_tx.send(());

    tokio::time::sleep(Duration::from_millis(100)).await;

    reschedule_running_jobs(repo, instance_id).await
}

async fn reschedule_running_jobs(repo: JobRepo, instance_id: uuid::Uuid) -> Result<(), JobError> {
    let mut op = repo.begin_op().await?;
    let now = crate::time::now();
    let rows = sqlx::query!(
        r#"
            UPDATE job_executions
            SET state = 'pending',
                execute_at = $1,
                poller_instance_id = NULL
            WHERE poller_instance_id = $2 AND state = 'running'
            RETURNING id as "id!: JobId", attempt_index
            "#,
        now,
        instance_id
    )
    .fetch_all(op.as_executor())
    .await?;

    tracing::Span::current().record("n_jobs", rows.len());

    let attempt_map: std::collections::HashMap<JobId, u32> = rows
        .into_iter()
        .map(|r| (r.id, r.attempt_index as u32))
        .collect();

    let ids: Vec<JobId> = attempt_map.keys().copied().collect();
    let entities = repo.find_all::<crate::Job>(&ids).await?;

    for (job_id, mut job) in entities {
        let attempt_index = attempt_map[&job_id];
        job.execution_aborted("graceful shutdown".to_string(), now, attempt_index);
        repo.update_in_op(&mut op, &mut job).await?;
    }
    op.commit().await?;
    Ok(())
}
