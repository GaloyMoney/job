use es_entity::AtomicOperation;
use futures::future::join_all;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgListener, PgPool, types::PgInterval};
use tracing::{Span, info, instrument};

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
    job_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
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
    job_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
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
            job_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub async fn start(self) -> Result<JobPollerHandle, sqlx::Error> {
        let listener_handle = self.start_listener().await?;
        let lost_handle = self.start_lost_handler();
        let keep_alive_handle = self.start_keep_alive_handler();
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = self.repo.clone();
        let instance_id = self.instance_id;
        let job_handles = self.job_handles.clone();
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
            job_handles,
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
                    tracing::error!(error = %e, failures, "main loop error");
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
        level = "debug",
        skip(self),
        fields(poller_id, n_jobs_running, n_jobs_to_start, now, next_poll_in),
        err
    )]
    async fn poll_and_dispatch(self: &Arc<Self>, woken_up: bool) -> Result<Duration, JobError> {
        Span::current().record("poller_id", tracing::field::display(self.instance_id));
        let Some(n_jobs_to_poll) = self.tracker.next_batch_size() else {
            Span::current().record("next_poll_in", tracing::field::debug(MAX_WAIT));
            Span::current().record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        };
        let rows = match poll_jobs(self.repo.pool(), n_jobs_to_poll, self.instance_id).await? {
            JobPollResult::WaitTillNextJob(duration) => {
                Span::current().record("next_poll_in", tracing::field::debug(duration));
                Span::current().record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs(jobs) => jobs,
        };
        Span::current().record("n_jobs_to_start", rows.len());
        if !rows.is_empty() {
            for row in rows {
                self.dispatch_job(row).await?;
            }
        }

        Span::current().record("next_poll_in", tracing::field::debug(Duration::ZERO));
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

                let span = tracing::debug_span!(
                    parent: None,
                    "job.detect_lost_jobs",
                    check_time = %check_time,
                    n_lost_jobs = tracing::field::Empty,
                );
                let _guard = span.enter();

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
                    Span::current().record("n_lost_jobs", rows.len());
                    for row in rows {
                        tracing::error!(job_id = %row.id, "lost job");
                    }
                } else {
                    Span::current().record("n_lost_jobs", 0);
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
                let span = tracing::debug_span!(
                    parent: None,
                    "job.keep_alive",
                    instance_id = %instance_id,
                    now = %now,
                    failures
                );
                let _guard = span.enter();

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
                        tracing::error!(instance_id = %instance_id, error = %e, "keep alive error");
                        Duration::from_millis(50 << failures)
                    }
                };
                drop(_guard);
                crate::time::sleep(timeout).await;
            }
        }))
    }

    #[instrument(
        name = "job.dispatch_job",
        skip(self, polled_job),
        fields(job_id, job_type, poller_id, attempt, now),
        err
    )]
    async fn dispatch_job(&self, polled_job: PolledJob) -> Result<(), JobError> {
        Span::current().record("attempt", polled_job.attempt);
        let job = self.repo.find_by_id(polled_job.id).await?;
        Span::current().record("job_id", tracing::field::display(job.id));
        Span::current().record("job_type", tracing::field::display(&job.job_type));
        let runner = self.registry.init_job(&job)?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let repo = self.repo.clone();
        let tracker = self.tracker.clone();
        let instance_id = self.instance_id;
        let shutdown_tx = self.shutdown_tx.clone();
        Span::current().record("now", tracing::field::display(crate::time::now()));
        Span::current().record("poller_id", tracing::field::display(instance_id));

        let job_id = job.id;
        let job_type = job.job_type.clone();
        let task_name = format!("job-{}-{}", job_type, job_id);

        let job_handle = tokio::task::Builder::new()
            .name(&task_name)
            .spawn(async move {
                let id = job.id;
                let attempt = polled_job.attempt;
                if let Err(e) = JobDispatcher::new(
                    repo,
                    tracker,
                    retry_settings,
                    job.id,
                    runner,
                    instance_id,
                    shutdown_tx,
                )
                .execute_job(polled_job)
                .await
                {
                    tracing::error!(job_id = %id, attempt, error = %e, "job dispatcher error");
                }
            })
            .expect("failed to spawn job task");

        // Store the handle for graceful shutdown
        self.job_handles
            .lock()
            .map_err(|e| JobError::JobExecutionError(e.to_string()))?
            .push(job_handle);

        Ok(())
    }
}

#[instrument(name = "job.poll_jobs", level = "debug", skip(pool), fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty), err)]
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

    Span::current().record("n_jobs_found", rows.len());
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
    /// 2. Wait for jobs to complete naturally (with timeout)
    /// 3. Reschedule any jobs still running for this instance
    ///
    /// If not called manually, it will be called automatically when the handle is dropped.
    pub async fn shutdown(&self) -> Result<(), JobError> {
        perform_shutdown(
            self.shutdown_tx.clone(),
            self.repo.clone(),
            self.instance_id,
            self.shutdown_called.clone(),
            self.job_handles.clone(),
            self.poller.config.shutdown_timeout,
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
        let job_handles = self.job_handles.clone();
        let shutdown_timeout = self.poller.config.shutdown_timeout;

        tokio::spawn(async move {
            let _ = perform_shutdown(
                shutdown_tx,
                repo,
                instance_id,
                shutdown_called,
                job_handles,
                shutdown_timeout,
            )
            .await;
        });
    }
}

#[instrument(
    name = "jobs.perform_shutdown",
    skip(shutdown_tx, repo, job_handles, shutdown_timeout, shutdown_called),
    fields(n_jobs, timeout_secs),
    err
)]
async fn perform_shutdown(
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    repo: JobRepo,
    instance_id: uuid::Uuid,
    shutdown_called: Arc<AtomicBool>,
    job_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    shutdown_timeout: Duration,
) -> Result<(), JobError> {
    if shutdown_called
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Ok(());
    }

    Span::current().record("timeout_secs", shutdown_timeout.as_secs());

    // Take ownership of job handles
    let handles = {
        let mut handles_guard = job_handles
            .lock()
            .map_err(|e| JobError::JobExecutionError(e.to_string()))?;
        std::mem::take(&mut *handles_guard)
    };

    let n_jobs = handles.len();
    Span::current().record("n_jobs", n_jobs);

    match shutdown_tx.send(()) {
        Ok(n_receivers) => tracing::info!(n_receivers, "Shutdown signal sent to receivers"),
        Err(_) => tracing::warn!("No receivers for shutdown signal"),
    }

    match tokio::time::timeout(shutdown_timeout, join_all(handles)).await {
        Ok(_) => {
            tracing::info!("All jobs completed gracefully");
        }
        Err(_) => {
            tracing::error!("Shutdown timeout reached, killing remaining jobs");
            kill_remaining_jobs(repo, instance_id).await?;
        }
    }

    Ok(())
}

#[instrument(name = "jobs.kill_remaining_jobs", skip(repo), fields(instance_id = %instance_id, n_killed), err)]
async fn kill_remaining_jobs(repo: JobRepo, instance_id: uuid::Uuid) -> Result<(), JobError> {
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

    let n_killed = rows.len();
    Span::current().record("n_killed", n_killed);
    info!("Killing {} jobs", n_killed);

    let attempt_map: std::collections::HashMap<JobId, u32> = rows
        .into_iter()
        .map(|r| (r.id, r.attempt_index as u32))
        .collect();

    let ids: Vec<JobId> = attempt_map.keys().copied().collect();
    let entities = repo.find_all::<crate::Job>(&ids).await?;

    for (job_id, mut job) in entities {
        let attempt_index = attempt_map[&job_id];
        job.abort_execution("killed job".to_string(), now, attempt_index);
        repo.update_in_op(&mut op, &mut job).await?;
    }
    op.commit().await?;

    Ok(())
}
