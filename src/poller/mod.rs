//! The claim-and-dispatch poller: one `main_loop` per process parks on
//! "notification OR deadline" (`clock.timeout(deadline,
//! tracker.notified())` -- deadlines run on the application clock so
//! manual-clock tests can time-travel, while lifecycle events wake the
//! loop instantly and clock-independently via `JobTracker::wake`). Each
//! pass prices a per-type claim plan against live pool headroom
//! (`JobRegistry::plan_claim`), claims due rows in one SQL statement, and
//! dispatches them. Correctness of the sleep is the load-bearing concern:
//! the poller may only park on a deadline that honestly reflects every row
//! it is responsible for, and every mechanism that could make that
//! deadline dishonest gets its own submodule.
//!
//! `budget`: pool headroom gates admission, with a real-time waiter for
//! recovery the tracker cannot observe. `recheck`: the elastic rotation
//! window hides some types from a poll entirely; due rows there force
//! bounded zero-sleep re-polls. `claim_query`: the claim statement and the
//! sleep window it computes. `hook`: short-circuit claims on the
//! spawner's/completer's own commit, plus their rollback reconciler.
//! `dispatch`: claimed row to running task, batching and shutdown
//! monitors. `recovery`: real-time heartbeats, lost-job reclaim, orphan
//! sweep. `shutdown`: drain-then-kill.

use es_entity::clock::ClockHandle;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{Span, instrument};

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use super::{
    JobId, config::JobPollerConfig, entity::JobType, error::JobError,
    notification_router::JobNotificationRouter, notifier::JobEventNotifier, registry::JobRegistry,
    repo::JobRepo, task::OwnedTaskHandle, tracker::JobTracker,
};

/// Helper macro to spawn tasks with optional names based on the tokio-task-names feature
/// Note: Requires both the feature AND tokio_unstable cfg to be set
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
mod dispatch;
mod hook;
mod recheck;
mod recovery;
mod shutdown;
#[cfg(test)]
mod test_support;

pub(crate) use budget::pool_connection_headroom;
pub(crate) use hook::ClaimHook;

use budget::PoolBudget;
use claim_query::{CONTENTION_HEADROOM, JobPollResult, poll_jobs};
use recheck::Recheck;
use recovery::Recovery;
use shutdown::ShutdownCoordinator;

const MAX_WAIT: Duration = Duration::from_secs(60);

pub(crate) struct JobPoller {
    config: JobPollerConfig,
    repo: Arc<JobRepo>,
    registry: JobRegistry,
    tracker: Arc<JobTracker>,
    router: Arc<JobNotificationRouter>,
    notifier: Arc<JobEventNotifier>,
    instance_id: uuid::Uuid,
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    clock: ClockHandle,
    internal_pool: PgPool,
    /// Shared with [`ShutdownCoordinator`]; see `shutdown`.
    shutdown_started: Arc<AtomicBool>,
    budget: PoolBudget,
    recheck: Recheck,
    recovery: Recovery,
}

/// A small dedicated pool reusing the main pool's connect options, serving
/// the claim query and `BatchDispatcher`'s terminal writes -- neither may
/// compete with the shared application pool for a connection. Session GUCs
/// (`force_generic_plan`, bitmap/seq scans off) make the claim a single
/// autocommit statement on guaranteed ordered index access; see
/// PERFORMANCE.md, "Ordered index access is mandatory".
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
    /// This process's poller, for populating [`PollerHandle`]s.
    pub(crate) fn poller(&self) -> &Arc<JobPoller> {
        &self.poller
    }
}

/// A late-bound `Weak` handle to this process's poller, shared by every
/// [`crate::JobSpawner`]: empty until `Jobs::start_poll` (a spawn before
/// that falls back to the ordinary insert), never re-set, and never keeps
/// the poller alive.
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
        Ok(Self {
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

    /// The dedicated pool backing the claim query and `BatchDispatcher`'s
    /// terminal writes; see [`build_internal_pool`].
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

    /// Stopping latches via the `watch` (an in-flight poll sees it on its
    /// next check, and every row it claimed still gets dispatched), then
    /// flips `poll_exited_tx` for the shutdown sequence to wait on.
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
        let plan = self.registry.plan_claim(n_jobs_to_poll, unit_budget);
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
                    plan.n_elastic,
                );
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs { jobs, window } => (jobs, window),
        };
        let jobs_len = rows.len();
        span.record("n_jobs_to_start", jobs_len);

        // Full claim -> drain immediately; otherwise the window's honest
        // sleep, with `recheck` deciding what `excluded_due` may shorten.
        let base = if jobs_len == n_jobs_to_poll {
            Duration::ZERO
        } else {
            window.sleep_for(self.clock.now())
        };
        let next_poll_in =
            self.recheck
                .bounded_sleep(base, jobs_len, window.excluded_due, plan.n_elastic);
        span.record("next_poll_in", tracing::field::debug(next_poll_in));

        // Deliberately NOT detached: dispatching claims tracker slots the
        // very next poll's plan reads, and subscribes shutdown receivers a
        // broadcast would otherwise never reach (see `dispatch`).
        if !rows.is_empty() {
            self.load_and_dispatch_claimed(rows).await?;
        }

        Ok(next_poll_in)
    }
}
