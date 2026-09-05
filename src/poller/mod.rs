//! The claim-and-dispatch poller: one `main_loop` per process parks on
//! "notification OR deadline" (`clock.timeout` against the application
//! clock, so manual-clock tests can time-travel, while `JobTracker::wake`
//! wakes it instantly and clock-independently). Each pass prices a
//! per-type claim plan against live pool headroom (`JobRegistry::plan_claim`),
//! claims due rows in one SQL statement, and dispatches them. Correctness of
//! the sleep is the load-bearing concern: the poller may only park on a
//! deadline that honestly reflects every row it is responsible for, and each
//! submodule protects that honesty for one mechanism -- `budget` (pool
//! headroom admission plus a real-time recovery waiter), `recheck` (bounded
//! zero-sleep re-polls for due rows the elastic rotation window hides from a
//! poll), `claim_query` (the claim statement and its sleep window), `hook`
//! (short-circuit claims on the spawner's/completer's own commit, plus
//! rollback reconciliation), and `recovery`/`shutdown` (real-time
//! heartbeats, lost-job reclaim, orphan sweep, and drain-then-kill).
//!
//! Dispatch (claimed row to running task) lives here, on the poller itself:
//! a batched type's claims split into canonical-order batches (sorted by
//! `queue_id` so concurrent batch transactions lock shared domain rows in
//! one order), with retries always batched alone. Dispatchers are built
//! synchronously with the poll loop, since construction claims the type's
//! tracker slot (which the very next poll's plan reads) and subscribes
//! shutdown receivers a later broadcast would never reach; each task pairs
//! the execution future with a monitor that acks shutdown and grants the
//! drain timeout. The `_from_reservation` entry points are the
//! short-circuit path's (`hook`), dispatching through an already-taken
//! reservation.

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{Span, instrument};

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use super::{
    JobId,
    batch_dispatcher::BatchDispatcher,
    batched::{RawBatchItem, ShutdownRx},
    config::JobPollerConfig,
    dispatcher::*,
    entity::{Job, JobType},
    error::JobError,
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    registry::JobRegistry,
    repo::JobRepo,
    task::OwnedTaskHandle,
    tracker::{JobTracker, UnitReservation},
};

#[cfg(all(feature = "tokio-task-names", tokio_unstable))]
macro_rules! spawn_named_task {
    ($name:expr, $future:expr) => {
        tokio::task::Builder::new()
            .name($name)
            .spawn($future)
            .expect("failed to spawn task")
    };
}

#[cfg(not(all(feature = "tokio-task-names", tokio_unstable)))]
macro_rules! spawn_named_task {
    ($name:expr, $future:expr) => {
        tokio::spawn($future)
    };
}

mod budget;
mod claim_query;
mod hook;
mod plan;
mod recheck;
mod recovery;
mod shutdown;
#[cfg(test)]
mod test_support;

pub(crate) use budget::pool_connection_headroom;
pub(crate) use hook::ClaimHook;

use budget::PoolBudget;
use claim_query::{CONTENTION_HEADROOM, JobPollResult, poll_jobs};
use hook::{ClaimedRow, DispatchTarget, claim_due_heads_in_op};
use plan::ClaimPlanner;
use recheck::Recheck;
use recovery::Recovery;
use shutdown::ShutdownCoordinator;

const MAX_WAIT: Duration = Duration::from_secs(60);

struct ShutdownSubs {
    job: ShutdownRx,
    monitor: ShutdownRx,
}

pub(crate) struct JobPoller {
    config: JobPollerConfig,
    repo: Arc<JobRepo>,
    registry: Arc<JobRegistry>,
    planner: ClaimPlanner,
    tracker: Arc<JobTracker>,
    router: Arc<JobNotificationRouter>,
    notifier: Arc<JobEventNotifier>,
    instance_id: uuid::Uuid,
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    clock: ClockHandle,
    internal_pool: PgPool,
    shutdown_started: Arc<AtomicBool>,
    budget: PoolBudget,
    recheck: Recheck,
    recovery: Recovery,
}

async fn build_internal_pool(main_pool: &PgPool) -> Result<PgPool, sqlx::Error> {
    let options: PgConnectOptions = (*main_pool.connect_options()).clone();
    PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET plan_cache_mode = force_generic_plan")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET enable_bitmapscan = off")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET enable_seqscan = off")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await
}

pub(crate) struct JobPollerHandle {
    poller: Arc<JobPoller>,
    #[allow(dead_code)]
    handle: OwnedTaskHandle,
    #[allow(dead_code)]
    router_listener_handle: OwnedTaskHandle,
    #[allow(dead_code)]
    router_waiter_handle: OwnedTaskHandle,
    shutdown: Arc<ShutdownCoordinator>,
}

impl JobPollerHandle {
    pub(crate) fn poller(&self) -> &Arc<JobPoller> {
        &self.poller
    }
}

pub(crate) type PollerHandle = Arc<std::sync::OnceLock<std::sync::Weak<JobPoller>>>;

impl JobPoller {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: JobPollerConfig,
        repo: Arc<JobRepo>,
        registry: JobRegistry,
        tracker: Arc<JobTracker>,
        router: Arc<JobNotificationRouter>,
        notifier: Arc<JobEventNotifier>,
        clock: ClockHandle,
    ) -> Result<Self, sqlx::Error> {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<
            tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
        >(1);
        let internal_pool = build_internal_pool(repo.pool()).await?;
        let instance_id = uuid::Uuid::now_v7();
        let registry = Arc::new(registry);
        Ok(Self {
            planner: ClaimPlanner::new(Arc::clone(&registry), Arc::clone(&tracker)),
            recheck: Recheck::new(Arc::clone(&tracker)),
            budget: PoolBudget::new(
                repo.pool(),
                config.connections_per_job,
                Arc::clone(&tracker),
            ),
            recovery: Recovery {
                pool: repo.pool().clone(),
                clock: clock.clone(),
                supported_job_types: registry.registered_job_types(),
                instance_id,
                tracker: Arc::clone(&tracker),
                notifier: Arc::clone(&notifier),
                job_lost_interval: config.job_lost_interval,
                pending_jobs_check_interval: config.pending_jobs_check_interval,
            },
            tracker,
            notifier,
            repo,
            config,
            registry,
            router,
            instance_id,
            shutdown_tx,
            clock,
            internal_pool,
            shutdown_started: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown_started.load(Ordering::SeqCst)
    }

    pub(crate) fn internal_pool(&self) -> &PgPool {
        &self.internal_pool
    }

    pub fn registered_job_types(&self) -> Vec<JobType> {
        self.registry.registered_job_types()
    }

    pub fn start(
        self,
        router_listener_handle: OwnedTaskHandle,
        router_waiter_handle: OwnedTaskHandle,
    ) -> JobPollerHandle {
        let lost_handle = self.recovery.spawn_lost_handler();
        let keep_alive_handle = self.recovery.spawn_keep_alive_handler();
        let stale_jobs_handle = self.recovery.spawn_stale_jobs_handler();
        let shutdown_tx = self.shutdown_tx.clone();
        let repo = Arc::clone(&self.repo);
        let instance_id = self.instance_id;
        let shutdown_timeout = self.config.shutdown_timeout;
        let max_jobs_per_process = self.config.max_jobs_per_process;
        let clock = self.clock.clone();
        let shutdown_started = Arc::clone(&self.shutdown_started);
        let (poll_stop_tx, poll_stop_rx) = tokio::sync::watch::channel(false);
        let (poll_exited_tx, poll_exited_rx) = tokio::sync::watch::channel(false);
        let executor = Arc::new(self);
        let handle = OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-main-loop",
            Self::main_loop(
                Arc::clone(&executor),
                poll_stop_rx,
                poll_exited_tx,
                lost_handle,
                keep_alive_handle,
                stale_jobs_handle,
            )
        ));
        JobPollerHandle {
            poller: executor,
            handle,
            router_listener_handle,
            router_waiter_handle,
            shutdown: Arc::new(ShutdownCoordinator {
                shutdown_tx,
                poll_stop_tx,
                poll_exited_rx,
                shutdown_called: shutdown_started,
                repo,
                instance_id,
                shutdown_timeout,
                max_jobs_per_process,
                clock,
            }),
        }
    }

    async fn main_loop(
        self: Arc<Self>,
        mut poll_stop_rx: tokio::sync::watch::Receiver<bool>,
        poll_exited_tx: tokio::sync::watch::Sender<bool>,
        _lost_task: OwnedTaskHandle,
        _keep_alive_task: OwnedTaskHandle,
        _stale_jobs_task: OwnedTaskHandle,
    ) {
        let mut failures = 0;
        let mut woken_up = false;
        loop {
            if *poll_stop_rx.borrow_and_update() {
                break;
            }

            let timeout = match self.poll_and_dispatch(woken_up).await {
                Ok(duration) => {
                    failures = 0;
                    duration
                }
                Err(e) => {
                    failures += 1;
                    tracing::error!(
                        exception.message = %e,
                        exception.type = std::any::type_name_of_val(&e),
                        failures,
                        "main loop error"
                    );
                    Duration::from_millis(50 << failures.min(12))
                }
            };

            tokio::select! {
                biased;

                _ = poll_stop_rx.changed() => {
                    break;
                }
                result = self.clock.timeout(timeout, self.tracker.notified()) => {
                    woken_up = result.is_ok();
                }
            }
        }

        let _ = poll_exited_tx.send(true);
    }

    #[instrument(
        name = "job.poll_and_dispatch",
        level = "debug",
        skip(self),
        fields(
            poller_id,
            n_jobs_running,
            n_jobs_to_start,
            now,
            next_poll_in,
            n_claim_clamped_by_pool
        )
    )]
    async fn poll_and_dispatch(self: &Arc<Self>, woken_up: bool) -> Result<Duration, JobError> {
        let span = Span::current();
        span.record("poller_id", tracing::field::display(self.instance_id));
        let Some(n_jobs_to_poll) = self.tracker.next_batch_size() else {
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        };
        let unit_budget = self.budget.unit_budget();
        let plan = self.planner.plan(n_jobs_to_poll, unit_budget);
        span.record("n_claim_clamped_by_pool", plan.clamped_by_pool);
        if plan.types.is_empty() {
            if plan.clamped_by_pool {
                self.budget.arm_waiter();
            }
            span.record("next_poll_in", tracing::field::debug(MAX_WAIT));
            span.record("n_jobs_to_start", 0);
            return Ok(MAX_WAIT);
        }

        let result = poll_jobs(
            &self.internal_pool,
            n_jobs_to_poll,
            self.instance_id,
            &plan.types,
            &plan.rotation_excluded,
            &plan.row_limits,
            CONTENTION_HEADROOM,
            &self.clock,
        )
        .await?;

        let (rows, window) = match result {
            JobPollResult::WaitTillNextJob(window) => {
                let duration = self.recheck.bounded_sleep(
                    window.sleep_for(self.clock.now()),
                    0,
                    window.excluded_due,
                    plan.rotation_lap,
                );
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs { jobs, window } => (jobs, window),
        };
        let jobs_len = rows.len();
        span.record("n_jobs_to_start", jobs_len);

        let base = if jobs_len == n_jobs_to_poll {
            Duration::ZERO
        } else {
            window.sleep_for(self.clock.now())
        };
        let next_poll_in =
            self.recheck
                .bounded_sleep(base, jobs_len, window.excluded_due, plan.rotation_lap);
        span.record("next_poll_in", tracing::field::debug(next_poll_in));

        if !rows.is_empty() {
            self.load_and_dispatch_claimed(rows).await?;
        }

        Ok(next_poll_in)
    }

    async fn load_and_dispatch_claimed(
        self: &Arc<Self>,
        rows: Vec<PolledJob>,
    ) -> Result<(), JobError> {
        let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let mut entities = self.repo.find_all::<Job>(&ids).await?;
        let mut batched: HashMap<JobType, Vec<RawBatchItem>> = HashMap::new();
        for row in rows {
            let Some(job) = entities.remove(&row.id) else {
                tracing::error!(
                    job_id = %row.id,
                    "claimed job row has no entity; skipping dispatch"
                );
                continue;
            };
            if self.registry.is_batched(&job.job_type) {
                batched
                    .entry(job.job_type.clone())
                    .or_default()
                    .push(RawBatchItem {
                        attempt: row.attempt,
                        queue_id: row.queue_id,
                        execution_state_json: row.data_json,
                        job,
                    });
            } else {
                self.dispatch_job(job, row).await?;
            }
        }
        for (job_type, items) in batched {
            self.dispatch_batches(job_type, items).await?;
        }
        Ok(())
    }

    #[instrument(
        name = "job.dispatch_batches",
        skip(self, items),
        fields(job_type = %job_type, n_items = items.len(), max_batch_size, n_batches)
    )]
    async fn dispatch_batches(
        self: &Arc<Self>,
        job_type: JobType,
        mut items: Vec<RawBatchItem>,
    ) -> Result<(), JobError> {
        let span = Span::current();
        let max_batch_size = self.registry.max_batch_size(&job_type);
        span.record("max_batch_size", max_batch_size);

        items.sort_by(
            |a, b| match (a.queue_id.as_deref(), b.queue_id.as_deref()) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => uuid::Uuid::from(a.job.id).cmp(&uuid::Uuid::from(b.job.id)),
            },
        );

        let (retries, mut fresh): (Vec<_>, Vec<_>) =
            items.into_iter().partition(|item| item.attempt > 1);

        let mut n_batches = 0;
        for retry in retries {
            n_batches += 1;
            self.dispatch_batch(job_type.clone(), vec![retry]).await?;
        }
        while !fresh.is_empty() {
            let take = max_batch_size.min(fresh.len());
            let chunk: Vec<RawBatchItem> = fresh.drain(..take).collect();
            n_batches += 1;
            self.dispatch_batch(job_type.clone(), chunk).await?;
        }
        span.record("n_batches", n_batches);
        Ok(())
    }

    #[instrument(
        name = "job.dispatch_batch",
        skip(self, items),
        fields(job_type = %job_type, n_items = items.len(), poller_id, now)
    )]
    async fn dispatch_batch(
        self: &Arc<Self>,
        job_type: JobType,
        items: Vec<RawBatchItem>,
    ) -> Result<(), JobError> {
        if items.is_empty() {
            return Ok(());
        }
        let span = Span::current();
        let runner = self.registry.init_batch(
            &job_type,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job_type).clone();
        span.record("now", tracing::field::display(self.clock.now()));
        span.record("poller_id", tracing::field::display(self.instance_id));

        let dispatcher = BatchDispatcher::new(
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job_type,
            runner,
            self.instance_id,
            self.clock.clone(),
            &items,
        );
        let subs = ShutdownSubs {
            job: self.shutdown_tx.subscribe(),
            monitor: self.shutdown_tx.subscribe(),
        };
        self.spawn_batch_dispatch_task(dispatcher, items, subs);
        Ok(())
    }

    fn spawn_batch_dispatch_task(
        &self,
        dispatcher: BatchDispatcher,
        items: Vec<RawBatchItem>,
        subs: ShutdownSubs,
    ) {
        let job_type = dispatcher.job_type().clone();
        let ShutdownSubs {
            job: shutdown_rx_batch,
            monitor: mut shutdown_rx_monitor,
        } = subs;
        let shutdown_timeout = self.config.shutdown_timeout;
        let n_items = items.len();
        let dispatched_type = job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-batch-{}-{}", job_type, n_items);

        spawn_named_task!(&task_name, async move {
            use tracing::Instrument;

            let batch_fut = dispatcher.execute_batch(items, shutdown_rx_batch);
            tokio::pin!(batch_fut);

            tokio::select! {
                res = &mut batch_fut => {
                    let _ = res;
                }
                Ok(shutdown_notifier) = shutdown_rx_monitor.recv() => {
                    let (send, recv) = tokio::sync::oneshot::channel();

                    async {
                        match shutdown_notifier.send(recv).await {
                            Ok(()) => {
                                tracing::Span::current().record("ack_sent", true);
                                tracing::info!("Acknowledgement sent, waiting for batch completion");
                                drop(shutdown_notifier);

                                match tokio::time::timeout(shutdown_timeout, &mut batch_fut).await {
                                    Ok(res) => {
                                        tracing::Span::current().record("job_completed", true);
                                        tracing::info!("Batch completed gracefully");
                                        let _ = res;
                                    }
                                    Err(_) => {
                                        tracing::Span::current().record("job_completed", false);
                                        tracing::warn!("Batch exceeded timeout, aborting");
                                    }
                                }

                                let _ = send.send(());
                                tracing::info!("Final completion signal sent");
                            }
                            Err(_) => {
                                tracing::Span::current().record("ack_sent", false);
                                tracing::error!("Failed to send acknowledgement - stopped listening");
                            }
                        }
                    }.instrument(tracing::info_span!(
                            parent: None,
                            "job.shutdown_coordination",
                            job_type = %dispatched_type,
                            n_items,
                            coordination_path = "shutdown_first",
                            ack_sent = tracing::field::Empty,
                            job_completed = tracing::field::Empty,
                        )
                    ).await;
                }
            }
        });
    }

    #[instrument(
        name = "job.dispatch_job",
        skip(self, job, polled_job),
        fields(job_id, job_type, poller_id, attempt, now)
    )]
    async fn dispatch_job(
        self: &Arc<Self>,
        job: Job,
        polled_job: PolledJob,
    ) -> Result<(), JobError> {
        let span = Span::current();
        span.record("attempt", polled_job.attempt);
        span.record("job_id", tracing::field::display(job.id));
        span.record("job_type", tracing::field::display(&job.job_type));
        let runner = self.registry.init_job(
            &job,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let retains_state = self.registry.retains_state(&job.job_type);
        span.record("now", tracing::field::display(self.clock.now()));
        span.record("poller_id", tracing::field::display(self.instance_id));

        let dispatcher = JobDispatcher::new(
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job.id,
            job.job_type.clone(),
            retains_state,
            runner,
            self.instance_id,
            self.clock.clone(),
        );
        let subs = ShutdownSubs {
            job: self.shutdown_tx.subscribe(),
            monitor: self.shutdown_tx.subscribe(),
        };
        self.spawn_dispatch_task(dispatcher, job, polled_job, subs);
        Ok(())
    }

    async fn dispatch_job_from_reservation(
        self: &Arc<Self>,
        reservation: UnitReservation,
        row: ClaimedRow,
        subs: ShutdownSubs,
    ) -> Result<(), JobError> {
        let job = self.repo.find_by_id(row.id).await?;
        let polled_job = PolledJob {
            id: row.id,
            data_json: row.data_json,
            attempt: row.attempt as u32,
            queue_id: row.queue_id,
        };
        let runner = self.registry.init_job(
            &job,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job.job_type).clone();
        let retains_state = self.registry.retains_state(&job.job_type);
        let dispatcher = JobDispatcher::from_reservation(
            reservation,
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job.id,
            job.job_type.clone(),
            retains_state,
            runner,
            self.instance_id,
            self.clock.clone(),
        );
        self.spawn_dispatch_task(dispatcher, job, polled_job, subs);
        Ok(())
    }

    async fn dispatch_batch_from_reservation(
        self: &Arc<Self>,
        reservation: UnitReservation,
        job_type: JobType,
        rows: Vec<ClaimedRow>,
        subs: ShutdownSubs,
    ) -> Result<(), JobError> {
        let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let mut entities = self.repo.find_all::<Job>(&ids).await?;
        let mut items: Vec<RawBatchItem> = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(job) = entities.remove(&row.id) else {
                tracing::error!(
                    job_id = %row.id,
                    "claimed job row has no entity; skipping dispatch"
                );
                continue;
            };
            items.push(RawBatchItem {
                attempt: row.attempt as u32,
                queue_id: row.queue_id,
                execution_state_json: row.data_json,
                job,
            });
        }
        if items.is_empty() {
            reservation.release();
            return Ok(());
        }
        let runner = self.registry.init_batch(
            &job_type,
            Arc::clone(&self.repo),
            Arc::clone(&self.router),
            self.clock.clone(),
            Arc::clone(&self.notifier),
        )?;
        let retry_settings = self.registry.retry_settings(&job_type).clone();
        let dispatcher = BatchDispatcher::from_reservation(
            reservation,
            Arc::downgrade(self),
            Arc::clone(&self.repo),
            Arc::clone(&self.tracker),
            Arc::clone(&self.notifier),
            retry_settings,
            job_type,
            runner,
            self.instance_id,
            self.clock.clone(),
            &items,
        );
        self.spawn_batch_dispatch_task(dispatcher, items, subs);
        Ok(())
    }

    fn spawn_dispatch_task(
        &self,
        dispatcher: JobDispatcher,
        job: Job,
        polled_job: PolledJob,
        subs: ShutdownSubs,
    ) {
        let ShutdownSubs {
            job: shutdown_rx_job,
            monitor: mut shutdown_rx_monitor,
        } = subs;
        let shutdown_timeout = self.config.shutdown_timeout;
        let job_id = job.id;
        let job_type = job.job_type.clone();
        #[cfg_attr(
            not(all(feature = "tokio-task-names", tokio_unstable)),
            allow(unused_variables)
        )]
        let task_name = format!("job-{}-{}", job_type, job_id);

        spawn_named_task!(&task_name, async move {
            use tracing::Instrument;

            let attempt = polled_job.attempt;
            let job_fut = dispatcher.execute_job(job, polled_job, shutdown_rx_job);
            tokio::pin!(job_fut);

            tokio::select! {
                res = &mut job_fut => {
                    if let Err(e) = res {
                        tracing::error!(
                            job_id = %job_id,
                            attempt,
                            exception.message = %e,
                            exception.type = std::any::type_name_of_val(&e),
                            "job dispatcher error"
                        );
                    }
                }
                Ok(shutdown_notifier) = shutdown_rx_monitor.recv() => {
                    let (send, recv) = tokio::sync::oneshot::channel();

                    async {
                        match shutdown_notifier.send(recv).await {
                            Ok(()) => {
                                tracing::Span::current().record("ack_sent", true);
                                tracing::info!("Acknowledgement sent, waiting for job completion");
                                drop(shutdown_notifier);

                                match tokio::time::timeout(shutdown_timeout, &mut job_fut).await {
                                    Ok(res) => {
                                        tracing::Span::current().record("job_completed", true);
                                        tracing::info!("Job completed gracefully");
                                        if let Err(e) = res {
                                            tracing::error!(
                                                job_id = %job_id,
                                                attempt,
                                                exception.message = %e,
                                                exception.type = std::any::type_name_of_val(&e),
                                                "job dispatcher error"
                                            );
                                        }
                                    }
                                    Err(_) => {
                                        tracing::Span::current().record("job_completed", false);
                                        tracing::warn!("Job exceeded timeout, aborting");
                                    }
                                }

                                let _ = send.send(());
                                tracing::info!("Final completion signal sent");
                            }
                            Err(_) => {
                                tracing::Span::current().record("ack_sent", false);
                                tracing::error!("Failed to send acknowledgement - stopped listening");
                            }
                        }
                    }.instrument(tracing::info_span!(
                            parent: None,
                            "job.shutdown_coordination",
                            job_id = %job_id,
                            job_type = %job_type,
                            coordination_path = "shutdown_first",
                            ack_sent = tracing::field::Empty,
                            job_completed = tracing::field::Empty,
                        )
                    ).await;
                }
            }
        });
    }

    fn try_reserve(self: &Arc<Self>, job_type: &JobType) -> Option<UnitReservation> {
        let cap = match self.registry.batch_policy(job_type) {
            Some(policy) => Some(policy.max_concurrent_per_process),
            None => self.registry.per_process_cap(job_type),
        };
        self.tracker.try_reserve(job_type, cap)
    }

    fn claim_shape(&self, job_type: &JobType) -> (i64, bool) {
        match self.registry.batch_policy(job_type) {
            Some(policy) => (policy.max_batch_size as i64, true),
            None => (1, false),
        }
    }

    async fn claim_after_many(
        self: &Arc<Self>,
        op: &mut (impl AtomicOperation + ?Sized),
        job_type: &JobType,
        now: DateTime<Utc>,
        n_units: usize,
    ) -> Result<Vec<DispatchTarget>, sqlx::Error> {
        if n_units == 0 {
            return Ok(Vec::new());
        }
        let (per_unit_limit, fresh_only) = self.claim_shape(job_type);
        let limit = per_unit_limit * n_units as i64;
        let rows =
            claim_due_heads_in_op(op, job_type, self.instance_id, now, limit, fresh_only).await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if self.registry.batch_policy(job_type).is_some() {
            let mut rows = rows.into_iter();
            let mut targets = Vec::new();
            loop {
                let chunk: Vec<ClaimedRow> = (&mut rows).take(per_unit_limit as usize).collect();
                if chunk.is_empty() {
                    break;
                }
                targets.push(DispatchTarget::Batch(job_type.clone(), chunk));
            }
            Ok(targets)
        } else {
            Ok(rows.into_iter().map(DispatchTarget::Single).collect())
        }
    }

    pub(crate) fn register_claim_demand(
        self: &Arc<Self>,
        op: &mut (impl AtomicOperation + ?Sized),
        job_type: &JobType,
        n_due: usize,
    ) {
        if n_due == 0 {
            return;
        }
        let hook = ClaimHook::for_demand(Arc::downgrade(self), job_type.clone(), n_due);
        Self::register_claim_hook(op, hook);
    }

    pub(crate) fn register_claim_recycle(
        self: &Arc<Self>,
        op: &mut (impl AtomicOperation + ?Sized),
        job_type: &JobType,
        reservation: UnitReservation,
    ) {
        let hook = ClaimHook::for_recycle(Arc::downgrade(self), job_type.clone(), reservation);
        Self::register_claim_hook(op, hook);
    }

    fn register_claim_hook(mut op: &mut (impl AtomicOperation + ?Sized), hook: ClaimHook) {
        if (&mut op).add_commit_hook(hook).is_err() {
            tracing::error!(
                "short-circuit claim could not register its commit hook; \
                 any recycled unit released normally, any fresh demand is simply not claimed \
                 -- the ordinary poll covers both"
            );
        }
    }
}
