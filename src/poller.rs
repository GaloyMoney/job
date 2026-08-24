use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use tracing::{Instrument, Span, instrument};

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
// Only referenced by the #[cfg(test)]-gated waiter-spawn counter below --
// unused (and denied by fail-on-warnings) in a normal build otherwise.
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use super::{
    JobId,
    batch_dispatcher::BatchDispatcher,
    batched::{RawBatchItem, ShutdownRx},
    config::JobPollerConfig,
    dispatcher::*,
    entity::{Job, JobType},
    error::JobError,
    execution_hooks::{PromoteHeadsHook, PromotedRow},
    notification_router::JobNotificationRouter,
    notifier::JobEventNotifier,
    registry::JobRegistry,
    repo::JobRepo,
    task::OwnedTaskHandle,
    tracker::{JobTracker, UnitReservation},
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
    /// Dedicated small pool (see [`build_internal_pool`]), serving two
    /// tenants that must never compete with the shared application pool for
    /// a connection: the claim query, whose session-level plan-cache/scan
    /// overrides (see `build_internal_pool`) must never leak onto `repo`'s
    /// pool; and `BatchDispatcher`'s terminal writes (`fail_batch`,
    /// `rescue_claimed_rows`) -- see those methods' doc comments for why a
    /// terminal write cannot afford to compete with the shared pool it is
    /// often trying to write *because* that pool is under pressure.
    internal_pool: PgPool,
    /// Set once `ShutdownCoordinator::perform` begins (shared with it, not
    /// owned -- both must observe the SAME flip). Checked by the
    /// completion-time recycle claim (`ClaimHook::pre_commit`) so a
    /// completing job/batch never re-admits new work once the drain is
    /// underway: recycling during shutdown would let a self-rescheduling job
    /// keep the process alive past the drain.
    shutdown_started: Arc<AtomicBool>,
    /// Whether a pool-headroom waiter task (see
    /// [`Self::arm_pool_headroom_waiter`]) is currently live. At most one at
    /// a time: a clamped-to-zero poll arms it, and it disarms itself right
    /// before waking the poll loop, so the poll that wakeup triggers can arm
    /// a fresh one if the headroom is already gone again.
    pool_waiter_armed: AtomicBool,
    /// Whether an elastic-rotation waiter task (see
    /// [`Self::arm_elastic_rotation_waiter`]) is currently live. At most one
    /// at a time: a poll whose plan left `ClaimPlan::elastic_rotation_partial`
    /// set arms it, and it disarms itself right before waking the poll loop,
    /// so the poll that wakeup triggers can arm a fresh one if rotation still
    /// hasn't covered every elastic type.
    elastic_rotation_waiter_armed: AtomicBool,
    /// Test-only instrumentation: counts every waiter task actually spawned
    /// by [`Self::arm_elastic_rotation_waiter`] (i.e. every time the guard
    /// above was NOT already held). Proves the guard collapses repeated/
    /// concurrent arm calls into at most one live task rather than one per
    /// call -- see `elastic_rotation_waiter_arm_is_idempotent_while_pending`.
    #[cfg(test)]
    elastic_rotation_waiter_spawns: AtomicUsize,
}

/// A small dedicated pool reusing the main pool's connect options, serving
/// two tenants: [`poll_jobs`], and `BatchDispatcher`'s terminal writes
/// (`fail_batch`, `rescue_claimed_rows`).
///
/// The claim query needs `plan_cache_mode = force_generic_plan`,
/// `enable_bitmapscan = off`, and `enable_seqscan = off` (see
/// PERFORMANCE.md, "Ordered index access is mandatory") on every connection
/// it runs on; setting them once per connection here — instead of `SET
/// LOCAL` inside a `BEGIN`/`COMMIT` on every poll — turns the claim into a
/// single autocommit statement (5 round trips down to 1) without ever
/// touching a connection the application pool might hand to unrelated
/// queries.
///
/// `enable_seqscan = off` completes the same contract as `enable_bitmapscan
/// = off`: under a generic plan built while table stats read near-empty,
/// the planner otherwise falls back to one heap seq scan per type probe
/// (~57 per poll on a registry this crate's size) because index bloat
/// inflates the index path's cost estimate. Forcing ordered index access
/// keeps the poll's cost O(claimed) + O(registered types), independent of
/// heap and index bloat. Measured: idle poll 3,192 -> 59 shared
/// blocks/call; no regression in any claiming regime (sb-max9 evidence,
/// 2026-08-21).
///
/// These same GUCs are safe for the terminal-write tenant even though it
/// runs none of the claim's SELECT statements: `fail_batch`/`rescue_claimed_rows`
/// issue fully-parameterized, PK- or `poller_instance_id`-targeted
/// `UPDATE`/`DELETE` statements, so a generic plan is what they would get
/// anyway (no literal to specialize on), and bitmap/seq-scan suppression is
/// simply irrelevant to a single-row index lookup. There is no session
/// state here that could leak between the two tenants in a way that
/// matters.
///
/// Sized for both tenants: 2 connections stay effectively reserved for poll
/// cadence, 2 more absorb terminal-write bursts. Under a mass-failure storm,
/// fail-writes could momentarily queue behind each other here -- acceptable,
/// they're short PK-targeted statements, and the alternative (borrowing the
/// shared pool) is the multi-minute claimed-row strand this pool exists to
/// prevent (see `handoff-pool-aware-claiming-and-fail-path.md` §5, and
/// `BatchDispatcher::fail_batch`/`rescue_claimed_rows`).
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
    /// This process's poller, for populating [`PollerHandle`]s (the
    /// short-circuit spawn fast path).
    pub(crate) fn poller(&self) -> &Arc<JobPoller> {
        &self.poller
    }
}

/// Drives the shutdown sequence for one poller instance.
///
/// Shared behind an `Arc` so the explicit [`JobPollerHandle::shutdown`] call and
/// the drop path run the identical sequence, guarded by the same
/// `shutdown_called` flag.
struct ShutdownCoordinator {
    shutdown_tx: tokio::sync::broadcast::Sender<
        tokio::sync::mpsc::Sender<tokio::sync::oneshot::Receiver<()>>,
    >,
    /// Tells `main_loop` to stop polling. Separate from `shutdown_tx` on
    /// purpose: the poll loop must be stopped and *drained* before the monitors
    /// are signalled (see [`ShutdownCoordinator::perform`]).
    poll_stop_tx: tokio::sync::watch::Sender<bool>,
    /// Flipped by `main_loop` once it has left the loop. A dropped sender (the
    /// task was aborted or panicked) counts as "exited" too.
    poll_exited_rx: tokio::sync::watch::Receiver<bool>,
    /// Shared with `JobPoller::shutdown_started` -- see that field's doc.
    /// `perform` CAS-flips it as its very first step, guarding both
    /// idempotency here and the completion-time recycle claim's
    /// no-recycle-during-drain rule.
    shutdown_called: Arc<AtomicBool>,
    shutdown_timeout: Duration,
    max_jobs_per_process: usize,
    repo: Arc<JobRepo>,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
}

/// A late-bound handle to this process's `JobPoller`, shared by every
/// [`crate::JobSpawner`] minted via `Jobs::add_initializer`/
/// `add_batched_initializer`. Empty until [`crate::Jobs::start_poll`] runs
/// (a spawn attempted before that has no poller to short-circuit through and
/// simply falls back to the ordinary insert — there is nothing to dispatch
/// with yet), and never re-set afterward. `Weak` so a spawner never keeps
/// the poller alive on its own.
pub(crate) type PollerHandle = Arc<std::sync::OnceLock<std::sync::Weak<JobPoller>>>;

const MAX_WAIT: Duration = Duration::from_secs(60);

/// Backoff schedule for the pool-headroom waiter
/// ([`JobPoller::arm_pool_headroom_waiter`]): the first re-check lands 10ms
/// after a clamped-to-zero poll, doubling per check up to a 1s ceiling.
///
/// Doubling, not linear: pool exhaustion is usually a query-duration-scale
/// blip, so the fine-grained early checks (10/20/40/80ms) catch the common
/// case within tens of milliseconds of a connection freeing, while the
/// geometric growth caps the waiter's total activity at ~8 checks (~1.3s)
/// before settling into the 1s steady state a genuinely stuck pool deserves
/// -- one cheap in-process counter read per second, no SQL, no connection.
const POOL_WAITER_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const POOL_WAITER_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Re-check delay for the elastic-rotation waiter
/// ([`JobPoller::arm_elastic_rotation_waiter`]): armed whenever a poll's
/// plan left `ClaimPlan::elastic_rotation_partial` set -- i.e. the elastic
/// floor window excluded at least one registered elastic type this poll
/// (see `JobRegistry::plan_claim`).
///
/// An excluded type's due rows are invisible to BOTH this poll's claim
/// query and `min_wait`'s next-due computation (both are scoped to
/// `plan.types`), so the window-derived sleep can't be trusted to reflect
/// them. That alone would just mean a wrong number -- the reason it needs
/// its OWN waiter, not a smaller number plugged into the existing sleep, is
/// that the existing sleep is `self.clock.timeout(..)`-based (`main_loop`):
/// under a manual/paused clock (exactly the shape this bug was found under
/// -- lana's tests run on one) that timeout never elapses on its own no
/// matter how small it is, only `tracker.notified()` firing does. This
/// waiter uses plain `tokio::time::sleep`, real wall-clock time regardless
/// of which clock the application clock is, and explicitly calls
/// `tracker.wake()` -- the same reason [`Self::arm_pool_headroom_waiter`]
/// does, not a coincidence of style.
///
/// A single fixed delay, not a doubling backoff like the pool-headroom
/// waiter: that waiter loops internally until ITS condition (headroom
/// returning) clears, which is transient. Elastic types outnumbering their
/// tier budget is a STANDING condition under load, not transient, so this
/// waiter is one-shot per arm instead -- it wakes once and exits, and the
/// next poll re-arms it if `elastic_rotation_partial` is still set. Every
/// registered elastic type enters the rotation window within `n` polls
/// regardless of `take` (see
/// `elastic_types_rotate_through_a_scarce_floor_across_polls`), so
/// convergence is bounded at `n` re-checks; 1s keeps that bounded in
/// seconds while holding indefinitely cheaply on an app whose elastic type
/// count structurally exceeds its tier's share -- same order of magnitude
/// as `POOL_WAITER_MAX_BACKOFF`'s own steady state, for the same reason.
const ELASTIC_ROTATION_RECHECK: Duration = Duration::from_secs(1);

/// How far past its admission budget a poll gathers candidates, so
/// `FOR UPDATE ... SKIP LOCKED` has somewhere to fall through when a peer
/// instance holds locks on the rows this poll would target. Sized for
/// contention, not for filtering.
///
/// Fixed, not adaptive: `state = 'pending'`
/// contains only already-claimable rows (one per queue at most), so
/// every window row is a candidate, and a queue's blocked backlog never
/// enters the window at all (it is `parked`). This constant only needs to
/// survive `SKIP LOCKED` fall-through, which a small fixed overscan does.
/// See PERFORMANCE.md, "Contention headroom".
const CONTENTION_HEADROOM: i32 = 4;

/// Live headroom on the shared pool: how many more connections it could
/// hand out right now without exceeding `max_connections`. Factored out as
/// a free function of `&PgPool` so it's directly unit-testable against a
/// real pool without needing a full `JobPoller`. See
/// `JobPoller::pool_unit_budget`, the only caller, for how this becomes a
/// dispatch-unit budget.
///
/// This is the raw capacity signal and deliberately reads 0 when the pool
/// is fully checked out: a zero budget means the poll claims nothing (see
/// `JobPoller::pool_unit_budget` for why claiming into a saturated pool is
/// worse than leaving rows `pending`, especially with peer instances) and
/// the pool-headroom waiter (`JobPoller::arm_pool_headroom_waiter`) takes
/// over watching for recovery.
///
/// The `size()`/`num_idle()` read is instantaneous and racy -- headroom can
/// change the instant after it's read. That's fine: this is a soft budget
/// re-evaluated every poll, not an invariant anything else depends on for
/// correctness.
pub(crate) fn pool_connection_headroom(main_pool: &PgPool) -> usize {
    let max_connections = main_pool.options().get_max_connections() as usize;
    let in_use = (main_pool.size() as usize).saturating_sub(main_pool.num_idle());
    max_connections.saturating_sub(in_use)
}

/// Convert live connection headroom into a dispatch-unit budget at
/// `connections_per_job` connections per unit
/// (`JobPollerConfig::connections_per_job`; validated finite and positive
/// at config build). Free function of its two inputs so the arithmetic is
/// unit-testable without a `JobPoller`.
///
/// Rounds DOWN (`floor`): the conservative reading, and with a factor above
/// 1.0 it means a budget of e.g. `floor(1 / 1.5) = 0` -- one free
/// connection is deliberately NOT enough when the operator has declared
/// each dispatch costs one and a half. That truncated-to-zero budget cannot
/// silently stall claiming, because everything that waits on admission
/// waits on THIS number, not on raw headroom: the pool-headroom waiter
/// (`JobPoller::arm_pool_headroom_waiter`) re-checks `pool_unit_budget() >
/// 0` and so wakes the poll loop exactly when enough headroom for one
/// whole unit has accumulated. (Watching raw headroom there instead would
/// spin: wake at one free connection, claim nothing, re-arm, wake again.)
fn unit_budget(headroom: usize, connections_per_job: f64) -> usize {
    (headroom as f64 / connections_per_job).floor() as usize
}

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
        Ok(Self {
            tracker,
            notifier,
            repo,
            config,
            registry,
            router,
            instance_id: uuid::Uuid::now_v7(),
            shutdown_tx,
            clock,
            internal_pool,
            shutdown_started: Arc::new(AtomicBool::new(false)),
            pool_waiter_armed: AtomicBool::new(false),
            elastic_rotation_waiter_armed: AtomicBool::new(false),
            #[cfg(test)]
            elastic_rotation_waiter_spawns: AtomicUsize::new(0),
        })
    }

    /// Whether this instance's shutdown sequence has started. See the
    /// `shutdown_started` field doc.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown_started.load(Ordering::SeqCst)
    }

    /// The dedicated pool backing the claim query and `BatchDispatcher`'s
    /// terminal writes. See the `internal_pool` field doc and
    /// `build_internal_pool`.
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
        let lost_handle = self.start_lost_handler();
        let keep_alive_handle = self.start_keep_alive_handler();
        let stale_jobs_handle = self.start_stale_jobs_handler();
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

    /// Claim-and-dispatch loop.
    ///
    /// Stopping is driven by `poll_stop_rx` — a `watch`, not the
    /// `shutdown_tx` broadcast, because the stop must *latch*: a
    /// `poll_and_dispatch()` already in flight when the signal lands has to see
    /// it on the very next check rather than miss a one-shot notification. The
    /// loop leaves an in-flight poll intact (every row it claimed still gets
    /// dispatched, so no claim is stranded in `state='running'`) and then flips
    /// `poll_exited_tx`, which is what
    /// [`ShutdownCoordinator::perform`] waits for before signalling the
    /// monitors.
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

    /// This poll's dispatch-unit budget, from the shared pool's live
    /// headroom -- fed into [`crate::registry::JobRegistry::plan_claim`],
    /// which does the actual per-type spending. NOT a row-count clamp: see
    /// `plan_claim`'s doc comment for why a unit (roughly one shared-pool
    /// connection's worth of work) is a *dispatch*, not a *row* -- a
    /// batched type's whole claim, however many rows, becomes as few as one
    /// `run_batch` call.
    ///
    /// One dispatch unit is priced at exactly one connection -- the crate's
    /// own claim/dispatch machinery (`run_batch`, `run_isolated`,
    /// `run_bisected`, and the `begin_op` + `_in_op` method family) never
    /// holds more than one at a time, confirmed by reading
    /// `batched.rs`/`current.rs`. That is the only cost this budget can
    /// know: what a *runner* does inside its own code is opaque to the
    /// poller and cannot be priced from here. A runner might open zero
    /// connections of its own (e.g. it does no persistence at all, or reads
    /// through a permanently-held listener connection like
    /// `JobNotificationRouter`'s), or it might open several -- the
    /// non-`_in_op` convenience methods (`BatchedJobItem::update_execution_state`/
    /// `set_result`, `CurrentJob::update_execution_state`/`set_result`)
    /// commit on a second connection while the runner's own `op` is still
    /// open, and nothing stops a runner from fanning work out
    /// *concurrently* (e.g. via `join_all`) for an unbounded number more.
    /// No uniform per-unit price is correct for both ends of that range, and
    /// guessing high to cover the worst case taxes the common (cheap or
    /// free) case for nothing.
    ///
    /// So this is a heuristic against the crate's own baseline cost, not a
    /// hard cap on what a poll's dispatched work can actually consume --
    /// same as before this budget existed, admission just used to be
    /// unbounded instead of loosely bounded. A runner that needs more than
    /// its unit's one connection can still make the shared pool run dry;
    /// what changed in this same feature is that the consequence is now
    /// cheap: `Finalizer::maybe_reclassify` classifies the
    /// resulting `PoolTimedOut` and the fail path reschedules the job a few
    /// seconds out rather than burning a retry attempt (see
    /// `Finalizer::reschedule_congested`). That asymmetry is why
    /// undercharging here is an acceptable trade: an occasional real
    /// `PoolTimedOut` costs a short reschedule delay, while a per-type or
    /// per-runner declared cost would ask every job author to reason about
    /// a number they usually cannot know either -- the same "we don't know
    /// how many connections a runner opens" problem, just pushed onto every
    /// caller instead of accepted once here.
    ///
    /// The `1.0` above is only the DEFAULT price:
    /// `JobPollerConfig::connections_per_job` lets a deployment tune it,
    /// fractionally, in either direction -- the operator knows their
    /// workload's real shape where the crate cannot (mostly connection-free
    /// runners: set e.g. `0.5` and admit twice the headroom; fan-out-heavy
    /// runners: `1.5`/`2.0` and admit less, leaning less on congestion
    /// reschedules). See [`unit_budget`] for the arithmetic and rounding.
    ///
    /// A zero budget means: claim NOTHING and arm the pool-headroom waiter
    /// ([`Self::arm_pool_headroom_waiter`], done by `poll_and_dispatch`'s
    /// clamped-empty branch) instead of claiming a probe job into the
    /// saturated pool. An earlier revision floored this at one unit "to
    /// keep making progress"; that floor was wrong for the multi-instance
    /// case: a row claimed by a saturated instance sits `running`, locked
    /// to that instance while its dispatch blocks in `acquire()` (up to the
    /// pool's acquire timeout, ~30s by default) -- invisible to `FOR UPDATE
    /// SKIP LOCKED` on every PEER instance whose pool is perfectly healthy
    /// and could have run it immediately. A row left `pending` is claimable
    /// by whichever instance recovers first; a row claimed by a saturated
    /// instance is claimable by no one. The single-instance liveness the
    /// floor bought (the blocked `acquire()` doubling as the wake signal
    /// for a freed connection) is bought instead by the waiter, at
    /// claim-nothing prices.
    fn pool_unit_budget(&self) -> usize {
        unit_budget(
            pool_connection_headroom(self.repo.pool()),
            self.config.connections_per_job,
        )
    }

    /// Spawn (at most one) background task that re-checks shared-pool
    /// headroom on a backoff schedule ([`POOL_WAITER_INITIAL_BACKOFF`]
    /// doubling to [`POOL_WAITER_MAX_BACKOFF`]) and wakes the poll loop
    /// ([`JobTracker::wake`]) the moment headroom returns. Armed by a poll
    /// that had due work but a zero unit budget
    /// ([`Self::pool_unit_budget`]).
    ///
    /// This exists because the tracker's `notified()` wake only fires on
    /// this crate's own lifecycle events -- it cannot observe a connection
    /// freed by OTHER users of a shared pool, so a poll that claimed
    /// nothing would otherwise sleep its full fallback wait (`MAX_WAIT`)
    /// past a recovery. A fixed retry interval (an earlier revision used
    /// 2s) misses most connection churn -- real release-to-reacquire gaps
    /// are milliseconds wide -- while the backoff's early 10-80ms checks
    /// catch them and its 1s ceiling keeps a genuinely stuck pool cheap to
    /// watch.
    ///
    /// The check is [`Self::pool_unit_budget`]` > 0` -- not `num_idle() >=
    /// 1`, for two reasons: headroom also counts capacity the pool has
    /// never opened (`size() < max_connections`), where an acquire would
    /// succeed by opening a fresh connection despite zero idle ones; and
    /// with `connections_per_job` above 1.0 a single freed connection can
    /// still mean a zero budget, so waking on raw headroom would claim
    /// nothing and spin (see [`unit_budget`]).
    ///
    /// The waiter holds only a `Weak` on the poller: it can never keep the
    /// instance alive, and it exits within one backoff step of the poller
    /// dropping. Disarm-then-wake ordering matters: the flag clears BEFORE
    /// `wake()`, so the poll the wake triggers can arm a fresh waiter if
    /// the headroom has already been snatched away again -- were it the
    /// other way around, that poll's arm attempt would see the flag still
    /// set, skip, and leave no waiter at all.
    fn arm_pool_headroom_waiter(self: &Arc<Self>) {
        if self.pool_waiter_armed.swap(true, Ordering::AcqRel) {
            return;
        }
        let poller = Arc::downgrade(self);
        spawn_named_task!("job-pool-headroom-waiter", async move {
            let mut backoff = POOL_WAITER_INITIAL_BACKOFF;
            loop {
                // Scoped so the upgraded `Arc` is dropped before the sleep:
                // the waiter must not extend the poller's lifetime across a
                // backoff step.
                {
                    let Some(poller) = poller.upgrade() else {
                        return;
                    };
                    // The wake condition is the unit BUDGET, not raw
                    // headroom: with `connections_per_job` above 1.0 a
                    // single freed connection can still mean a zero budget,
                    // and waking the poll loop on it would claim nothing
                    // and spin re-arming. See `unit_budget`.
                    if poller.pool_unit_budget() > 0 {
                        poller.pool_waiter_armed.store(false, Ordering::Release);
                        poller.tracker.wake();
                        return;
                    }
                };
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(POOL_WAITER_MAX_BACKOFF);
            }
        });
    }

    /// Spawn (at most one) background task that sleeps
    /// [`ELASTIC_ROTATION_RECHECK`] on real wall-clock time, then wakes the
    /// poll loop ([`JobTracker::wake`]). Armed by a poll whose plan left
    /// `ClaimPlan::elastic_rotation_partial` set (`JobRegistry::plan_claim`'s
    /// floor window excluded a registered elastic type this poll).
    ///
    /// One-shot, not a condition-checking loop like
    /// [`Self::arm_pool_headroom_waiter`]: there is no cheap external signal
    /// to poll for ("has rotation reached the excluded type yet" is only
    /// answerable by actually polling), and the underlying condition --
    /// elastic types outnumbering their tier's budget -- is a standing
    /// property of the app's registered types under load, not a transient
    /// one that resolves on its own. So this just forces the next poll to
    /// happen soon, in real time; that poll's own `plan_claim` call decides
    /// whether to arm another one.
    ///
    /// Plain `tokio::time::sleep`, not `self.clock.sleep`/`self.clock.timeout`:
    /// `main_loop`'s own sleep between polls IS clock-based
    /// (`self.clock.timeout(timeout, self.tracker.notified())`), which is
    /// exactly the problem -- under a manual/paused clock that timeout never
    /// elapses no matter how short, so a poll that only shortened its
    /// returned duration would still park forever. This waiter runs on real
    /// time regardless of which clock the application uses, and calling
    /// `tracker.wake()` resolves `notified()` immediately, independent of
    /// the outer clock-based timeout wrapping it -- same mechanism
    /// `arm_pool_headroom_waiter` already relies on for the same reason.
    fn arm_elastic_rotation_waiter(self: &Arc<Self>) {
        if self
            .elastic_rotation_waiter_armed
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        #[cfg(test)]
        self.elastic_rotation_waiter_spawns
            .fetch_add(1, Ordering::SeqCst);
        let poller = Arc::downgrade(self);
        spawn_named_task!("job-elastic-rotation-waiter", async move {
            tokio::time::sleep(ELASTIC_ROTATION_RECHECK).await;
            let Some(poller) = poller.upgrade() else {
                return;
            };
            poller
                .elastic_rotation_waiter_armed
                .store(false, Ordering::Release);
            poller.tracker.wake();
        });
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
        let unit_budget = self.pool_unit_budget();
        let plan = self.registry.plan_claim(n_jobs_to_poll, unit_budget);
        span.record("n_claim_clamped_by_pool", plan.clamped_by_pool);
        if plan.elastic_rotation_partial {
            self.arm_elastic_rotation_waiter();
        }
        if plan.types.is_empty() {
            if plan.clamped_by_pool {
                self.arm_pool_headroom_waiter();
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
            &plan.row_limits,
            CONTENTION_HEADROOM,
            &self.clock,
        )
        .await?;

        let (rows, window) = match result {
            JobPollResult::WaitTillNextJob(window) => {
                let duration = window.sleep_for(self.clock.now());
                span.record("next_poll_in", tracing::field::debug(duration));
                span.record("n_jobs_to_start", 0);
                return Ok(duration);
            }
            JobPollResult::Jobs { jobs, window } => (jobs, window),
        };
        let jobs_len = rows.len();
        span.record("n_jobs_to_start", jobs_len);

        // Full claim: budget was the limit, drain immediately. Partial claim: sleep,
        // unless `may_have_more` says the due backlog wasn't fully seen this poll.
        let next_poll_in = if jobs_len == n_jobs_to_poll {
            Duration::ZERO
        } else {
            window.sleep_for(self.clock.now())
        };
        span.record("next_poll_in", tracing::field::debug(next_poll_in));

        // Deliberately not detached from the poll loop: `dispatch_job`/
        // `dispatch_batches` do two things synchronously that a detached
        // continuation would make late. (a) `tracker.dispatch_job`/
        // `dispatch_batch`, which `plan_claim` reads on the NEXT poll --
        // late, a poll immediately following a full claim (`next_poll_in`
        // is `Duration::ZERO`, the common case under load) can race ahead
        // of the continuation and claim a second full batch against a slot
        // budget the tracker hasn't heard is spoken for yet (see
        // `claims_are_capped_by_free_batch_slots`). (b) the dispatch
        // task's `shutdown_tx.subscribe()`, which `tokio::sync::broadcast`
        // only delivers to if it happened before the shutdown broadcast --
        // late, a shutdown landing between this poll returning and the
        // continuation actually running broadcasts to no one, force-
        // aborting rather than draining the execution (see
        // `shutdown_drains_self_rescheduling_jobs`). Both require the slot
        // claim and the shutdown subscription to stay synchronous with the
        // poll loop.
        if !rows.is_empty() {
            self.load_and_dispatch_claimed(rows).await?;
        }

        Ok(next_poll_in)
    }

    /// Load the entities for a poll's claimed rows and hand each off to the
    /// per-job or batched dispatcher.
    async fn load_and_dispatch_claimed(
        self: &Arc<Self>,
        rows: Vec<PolledJob>,
    ) -> Result<(), JobError> {
        let ids: Vec<JobId> = rows.iter().map(|row| row.id).collect();
        let mut entities = self.repo.find_all::<Job>(&ids).await?;
        // Claims for batched types are collected here rather than
        // dispatched one by one: the poll query guarantees at most one row
        // per queue_id, so a type's claims from a single poll are exactly
        // the set that may be executed together.
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

    fn start_lost_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let clock = self.clock.clone();
        let supported_job_types = self.registry.registered_job_types();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        let notifier = Arc::clone(&self.notifier);
        OwnedTaskHandle::new(spawn_named_task!("job-poller-lost-handler", async move {
            loop {
                // Liveness is a wall-clock question, independent of any
                // manual application clock.
                tokio::time::sleep(job_lost_interval / 2).await;
                let alive_threshold = chrono::Utc::now() - job_lost_interval;
                let reschedule_at = clock.now();

                let self_live_ids = tracker.live_job_ids();

                let span = tracing::debug_span!(
                    parent: None,
                    "job.detect_lost_jobs",
                    alive_threshold = %alive_threshold,
                    reschedule_at = %reschedule_at,
                    instance_id = %instance_id,
                    n_live_jobs = self_live_ids.len(),
                    n_lost_jobs = tracing::field::Empty,
                    n_orphaned_parked = tracing::field::Empty,
                );

                async {
                    match reclaim_lost_jobs(
                        &pool,
                        instance_id,
                        &supported_job_types,
                        alive_threshold,
                        reschedule_at,
                        &self_live_ids,
                    )
                    .await
                    {
                        Ok((reclaimed, promoted)) => {
                            Span::current().record("n_lost_jobs", reclaimed.len());
                            let mut reported: HashSet<String> = HashSet::new();
                            let reclaimed_at = chrono::Utc::now();
                            for job in &reclaimed {
                                // `job_type` and the stall age turn this from
                                // "something was lost" into an attributable
                                // event: the age says how long the job sat
                                // unheartbeaten, which is what identifies the
                                // dispatcher failure that stranded it.
                                tracing::error!(
                                    job_id = %job.id,
                                    job_type = %job.job_type,
                                    stall_secs = (reclaimed_at - job.alive_at).num_seconds(),
                                    "lost job"
                                );
                                if reported.insert(job.job_type.to_string()) {
                                    notifier.execution_ready(&job.job_type);
                                }
                            }
                            for promoted_type in promoted {
                                if reported.insert(promoted_type.clone()) {
                                    notifier.execution_ready(&JobType::from_owned(promoted_type));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "lost-handler failed to reclaim lost jobs"
                            );
                            Span::current().record("n_lost_jobs", 0);
                        }
                    }

                    // Piggybacked on this same cadence: recover any queue
                    // whose parked backlog has no active (pending/running)
                    // row.
                    match sweep_orphaned_parked_rows(&pool).await {
                        Ok(promoted) => {
                            Span::current().record("n_orphaned_parked", promoted.len());
                            if !promoted.is_empty() {
                                tracing::warn!(
                                    n_orphaned_parked = promoted.len(),
                                    "recovered orphaned parked rows"
                                );
                            }
                            let mut reported: HashSet<String> = HashSet::new();
                            for job_type in promoted {
                                if reported.insert(job_type.clone()) {
                                    notifier.execution_ready(&JobType::from_owned(job_type));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "lost-handler failed to sweep orphaned parked rows"
                            );
                            Span::current().record("n_orphaned_parked", 0);
                        }
                    }
                }
                .instrument(span)
                .await;
            }
        }))
    }

    fn start_keep_alive_handler(&self) -> OwnedTaskHandle {
        let job_lost_interval = self.config.job_lost_interval;
        let pool = self.repo.pool().clone();
        let instance_id = self.instance_id;
        let tracker = Arc::clone(&self.tracker);
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-keep-alive-handler",
            async move {
                let mut failures = 0;
                loop {
                    let live_ids = tracker.live_job_ids();

                    // alive_at is a wall-clock liveness heartbeat (see lost-handler).
                    let now = chrono::Utc::now();
                    let span = tracing::debug_span!(
                        parent: None,
                        "job.keep_alive",
                        instance_id = %instance_id,
                        now = %now,
                        n_live_jobs = live_ids.len(),
                        failures
                    );

                    let timeout = async {
                        if live_ids.is_empty() {
                            failures = 0;
                            return job_lost_interval / 4;
                        }
                        // Ordered and non-blocking, for deadlock avoidance.
                        //
                        // This fires every `job_lost_interval / 4` over every
                        // running row of the instance, which made it the
                        // busiest multi-row `job_executions` writer in the
                        // crate -- and, as a bare `id = ANY(...)` UPDATE, the
                        // last big one still taking row locks in scan order.
                        // It was caught as a deadlock victim in production
                        // (stress run sb-max10) and is the prime suspect for
                        // the surviving partner of the batch-seal deadlocks
                        // there and in sb-max9. One unordered writer
                        // re-poisons every ordered one it overlaps, so this
                        // adopts the same `MATERIALIZED` + `ORDER BY
                        // queue_id, id` lock pattern the finalizer's
                        // disposition writes use -- see
                        // `Finalizer::finalize_in_op` for why the ordering
                        // (and leading with `queue_id`) is load-bearing.
                        //
                        // `SKIP LOCKED` on top: a heartbeat has nothing to
                        // gain by waiting for a contended row. Whoever holds
                        // it is either deleting the row (terminal) or about to
                        // release it, in which case the next beat refreshes
                        // it. Worst-case gap is 2 * (interval / 4) = half the
                        // liveness threshold, so a skipped row can never be
                        // mistaken for lost. `FOR NO KEY UPDATE` rather than
                        // `FOR UPDATE` so the heartbeat never blocks a
                        // key-level locker.
                        match sqlx::query!(
                            r#"
                        WITH to_touch AS MATERIALIZED (
                            SELECT id FROM job_executions
                            WHERE poller_instance_id = $2
                              AND state = 'running'
                              AND id = ANY($3)
                            ORDER BY queue_id, id
                            FOR NO KEY UPDATE SKIP LOCKED
                        )
                        UPDATE job_executions je
                        SET alive_at = $1
                        FROM to_touch t
                        WHERE je.id = t.id
                        "#,
                            now,
                            instance_id,
                            &live_ids,
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
                                tracing::error!(
                                    instance_id = %instance_id,
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "keep alive error"
                                );
                                Duration::from_millis(50 << failures.min(12))
                            }
                        }
                    }
                    .instrument(span)
                    .await;

                    tokio::time::sleep(timeout).await;
                }
            }
        ))
    }

    fn start_stale_jobs_handler(&self) -> OwnedTaskHandle {
        let pending_jobs_check_interval = self.config.pending_jobs_check_interval;
        let pool = self.repo.pool().clone();
        let clock = self.clock.clone();
        let supported_job_types = self.registry.registered_job_types();
        OwnedTaskHandle::new(spawn_named_task!(
            "job-poller-stale-jobs-handler",
            async move {
                loop {
                    tokio::time::sleep(pending_jobs_check_interval).await;
                    let now = clock.now();

                    let span = tracing::info_span!(
                        parent: None,
                        "job.check_stale_pending_jobs",
                        n_stale_pending = tracing::field::Empty,
                        max_pending_duration_secs = tracing::field::Empty,
                    );

                    async {
                        match sqlx::query!(
                            r#"
                        SELECT
                            job_type,
                            COUNT(*)::INT4 AS "count!: i32",
                            EXTRACT(EPOCH FROM ($1::timestamptz - MIN(execute_at)))::FLOAT8
                                AS "max_pending_duration_secs!: f64"
                        FROM job_executions
                        WHERE state = 'pending'
                        AND execute_at <= $1::timestamptz
                        AND job_type = ANY($2)
                        GROUP BY job_type
                        "#,
                            now,
                            &supported_job_types as _,
                        )
                        .fetch_all(&pool)
                        .await
                        {
                            Ok(rows) => {
                                let mut total_stale: i64 = 0;
                                let mut max_pending_secs: f64 = 0.0;

                                for row in &rows {
                                    total_stale += row.count as i64;
                                    if row.max_pending_duration_secs > max_pending_secs {
                                        max_pending_secs = row.max_pending_duration_secs;
                                    }
                                    tracing::warn!(
                                        job_type = %row.job_type,
                                        count = row.count,
                                        max_pending_duration_secs = row.max_pending_duration_secs,
                                        "stale pending jobs detected"
                                    );
                                }

                                Span::current().record("n_stale_pending", total_stale);
                                Span::current()
                                    .record("max_pending_duration_secs", max_pending_secs);
                            }
                            Err(e) => {
                                tracing::error!(
                                    exception.message = %e,
                                    exception.type = std::any::type_name_of_val(&e),
                                    "failed to check stale pending jobs"
                                );
                            }
                        }
                    }
                    .instrument(span)
                    .await;
                }
            }
        ))
    }

    /// Split one poll's claims for a batched type into batches, then dispatch
    /// each batch as a single unit of work.
    ///
    /// Two rules are encoded here:
    ///
    /// - **Canonical order.** Items are sorted by `queue_id` (job id when
    ///   unqueued) so that every batch in the process reaches shared domain
    ///   rows in the same order, which is what keeps concurrent batch
    ///   transactions from deadlocking against each other.
    /// - **Retries run alone.** A job on its second or later attempt is
    ///   dispatched as a batch of one. The first failure of a poisonous job is
    ///   shared with its batch-mates (they are all retried), but from then on
    ///   it can only ever fail by itself.
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

        // Built here, not in the task: constructing the dispatcher claims the
        // type's batch slot, and that must happen before the poll loop's next
        // iteration or it would claim rows against a slot already spoken for.
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

    /// Spawn the batch execution task and its shutdown-coordination monitor
    /// for an already-built [`BatchDispatcher`]. Shared by [`Self::dispatch_batch`]
    /// and [`Self::dispatch_batch_from_reservation`] so the shutdown
    /// handshake (`job.shutdown_coordination`) has exactly one
    /// implementation, mirroring [`Self::spawn_dispatch_task`]. Takes
    /// already-subscribed receivers for the same reason that one does.
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
                // `execute_batch` emits `batch dispatcher error` itself --
                // it is the only scope that knows which job ids were
                // affected and how their claimed rows were disposed of.
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
                                        // See the note above: the error log
                                        // is `execute_batch`'s to emit.
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

        // Built here, not in the task: constructing the dispatcher claims the
        // type's per-process slot, and that must happen before the poll
        // loop's next iteration or it would claim rows against a slot
        // already spoken for (mirrors `dispatch_batch`).
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

    /// Reserve capacity for a due-now event of `job_type`: the type's
    /// per-process cap for a plain type, or one batch slot
    /// (`max_concurrent_per_process`, counted per BATCH not per row -- see
    /// `JobTracker::dispatch_batch`) for a batched type. See
    /// [`JobTracker::try_reserve`].
    fn try_reserve(self: &Arc<Self>, job_type: &JobType) -> Option<UnitReservation> {
        let cap = match self.registry.batch_policy(job_type) {
            Some(policy) => Some(policy.max_concurrent_per_process),
            None => self.registry.per_process_cap(job_type),
        };
        self.tracker.try_reserve(job_type, cap)
    }

    /// Claim shape for `job_type`: `(limit, fresh_only)`. A batched type
    /// claims up to `max_batch_size` rows, retries excluded (they always run
    /// alone -- see `dispatch_batches`); a plain type claims exactly one row,
    /// any attempt (it dispatches alone regardless).
    fn claim_shape(&self, job_type: &JobType) -> (i64, bool) {
        match self.registry.batch_policy(job_type) {
            Some(policy) => (policy.max_batch_size as i64, true),
            None => (1, false),
        }
    }

    /// The head-swap kernel's claim step: claims, in ONE statement,
    /// up to `n_units * claim_shape(job_type).0` of `job_type`'s due backlog
    /// and splits the result into that many [`DispatchTarget`]s -- one row
    /// per reservation for a plain type, up to `max_batch_size` rows per
    /// reservation for a batched type. `now` must be the SAME instant the
    /// caller's own write in this `op` used -- see the sim-clock hazard note
    /// on [`claim_due_heads_in_op`]. Returns fewer than `n_units` targets --
    /// down to zero -- whenever fewer than that many units' worth of due
    /// work exists; callers must release whatever reservation has no
    /// matching target.
    async fn claim_after_many(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
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

    /// Registers spawn-side claim demand for `job_type`. `n_due` is a count
    /// of due ROWS, not reservations -- at commit
    /// time [`ClaimHook::pre_commit`] translates it into
    /// `n_due.div_ceil(per_reservation)` fresh reservations (one row per
    /// reservation for a plain type, up to `max_batch_size` rows per
    /// reservation for a batched type -- see [`Self::claim_shape`]) and
    /// claims after each. Call AFTER the caller's ordinary insert has
    /// already landed its own row(s) pending/parked in the SAME `op` -- the
    /// claim is a later statement in the same transaction, so it sees that
    /// insert with guaranteed ordering. A no-op if `n_due == 0` -- nothing
    /// to claim for zero due rows.
    pub(crate) fn register_claim_demand(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        job_type: &JobType,
        n_due: usize,
    ) {
        if n_due == 0 {
            return;
        }
        let hook = ClaimHook {
            poller: Arc::downgrade(self),
            fresh_demand: HashMap::from([(job_type.clone(), n_due)]),
            recycled: Vec::new(),
            claimed: Vec::new(),
        };
        Self::register_claim_hook(op, hook);
    }

    /// Registers a completion-side recycled unit of `job_type`'s capacity:
    /// the caller already owns this unit (it just
    /// called [`JobTracker::recycle`], having first detached its own
    /// Drop-triggered release) and is about to lose it. At commit time,
    /// [`ClaimHook::pre_commit`] tries to spend it on this type's own oldest
    /// due backlog; if nothing is due (or shutdown is underway, or the type
    /// opted out), the reservation simply drops and performs the ordinary
    /// release the caller's own Drop would have -- no claim-first-then-decide
    /// at the call site, the hook decides.
    pub(crate) fn register_claim_recycle(
        self: &Arc<Self>,
        op: &mut impl es_entity::AtomicOperation,
        job_type: &JobType,
        reservation: UnitReservation,
    ) {
        let hook = ClaimHook {
            poller: Arc::downgrade(self),
            fresh_demand: HashMap::new(),
            recycled: vec![(job_type.clone(), reservation)],
            claimed: Vec::new(),
        };
        Self::register_claim_hook(op, hook);
    }

    /// Shared registration tail for [`Self::register_claim_demand`]/
    /// [`Self::register_claim_recycle`]. `add_commit_hook` can only fail if
    /// `op` carries no commit-hook buffer at all. `ClaimHook` must NEVER
    /// `force_execute_pre_commit` in that case, unlike a hook whose work
    /// must not be dropped -- forcing this one inline would claim rows with
    /// no `post_commit` pass ever running to dispatch them, stranding them
    /// `running` until `reclaim_lost_jobs` eventually recovers them on a
    /// stale `alive_at`. Dropping the hook here is strictly the safer
    /// failure mode: no claim happens at all, and any recycled reservation
    /// it carried releases via `UnitReservation::Drop` exactly as an
    /// ordinary completion would.
    fn register_claim_hook(op: &mut impl es_entity::AtomicOperation, hook: ClaimHook) {
        if op.add_commit_hook(hook).is_err() {
            tracing::error!(
                "short-circuit claim could not register its commit hook; \
                 any recycled unit released normally, any fresh demand is simply not claimed \
                 -- the ordinary poll covers both"
            );
        }
    }

    /// The head-swap dispatch fast path's SINGLE-job entry point: called
    /// from [`ClaimHook::post_commit`] once the claiming transaction has
    /// committed. Re-reads the entity by id rather than carrying it across
    /// the commit boundary -- one extra point read for the fast path only,
    /// not the `find_all` batch the ordinary poll claim needs. Builds the
    /// runner and dispatcher exactly like [`Self::dispatch_job`], but from
    /// an already-taken [`UnitReservation`] rather than claiming a fresh
    /// slot, and using the claimed row's real attempt/queue_id/data_json
    /// rather than assuming a fresh spawn's defaults -- a recycled claim can
    /// land a retry (`attempt > 1`) just as easily as a fresh row.
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

    /// The head-swap dispatch fast path's BATCH entry point: mirrors
    /// [`Self::dispatch_job_from_reservation`] for a [`DispatchTarget::Batch`]
    /// -- re-fetches every claimed row's entity, builds the same
    /// [`RawBatchItem`]s an ordinary poll claim would, and dispatches through
    /// an already-taken reservation via [`BatchDispatcher::from_reservation`].
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
            // Every claimed row's entity vanished between the claim and this
            // fetch (extremely unlikely -- would mean the row was deleted in
            // between). Release rather than leak the reservation.
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

    /// Spawn the execution task and its shutdown-coordination monitor for an
    /// already-built [`JobDispatcher`]. Shared by [`Self::dispatch_job`] and
    /// [`Self::dispatch_job_from_reservation`] so the shutdown handshake
    /// (`job.shutdown_coordination`) has exactly one implementation.
    /// Takes already-subscribed receivers rather than subscribing itself:
    /// the head-swap caller must subscribe before its claiming `op` commits
    /// (see [`ClaimHook::pre_commit`]'s doc comment) -- subscribing
    /// in here would be too late for that path.
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
}

/// One row claimed by [`claim_due_heads_in_op`]: everything a dispatcher
/// needs besides the `Job` entity itself (which the caller still re-fetches
/// by id -- see [`JobPoller::dispatch_job_from_reservation`]'s doc comment
/// for why).
pub(crate) struct ClaimedRow {
    pub id: JobId,
    pub attempt: i32,
    pub queue_id: Option<String>,
    pub data_json: Option<JsonValue>,
    /// This row's `execute_at` immediately BEFORE the claim nulled it, and
    /// its `job_type` (constant across one [`claim_due_heads_in_op`] call,
    /// carried per-row so a flattened `Vec<ClaimedRow>` is self-contained).
    /// Both unused by the ordinary dispatch path; carried for
    /// [`ClaimReconciler`], which needs them to restore a row's true
    /// oldest-first position if the claiming transaction's `COMMIT` fails
    /// after landing (see [`ClaimHook::on_rollback`]).
    pub execute_at: DateTime<Utc>,
    pub job_type: JobType,
}

/// Claim up to `limit` of the oldest due `pending` rows of `job_type`,
/// landing them `running`-by-`instance_id` -- byte-identical to what a poll
/// claim would produce (same `state`/`poller_instance_id`/`alive_at`/
/// `execute_at` columns, same tiebreak). The head-swap kernel: a
/// short-circuit event (spawn or completion) never dispatches a specific
/// row, it claims whichever oldest due row(s) of `job_type` exist right
/// now, so admission within one type stays oldest-first even on the fast
/// path.
///
/// Always a LATER statement in the SAME transaction as the caller's write
/// (insert, promote, reclaim) -- sees that write's uncommitted rows with
/// guaranteed statement ordering, which is what keeps this immune to the
/// independent-CTE ordering hazard documented on
/// [`crate::execution_hooks::PromoteHeadsHook::apply`].
///
/// `FOR UPDATE SKIP LOCKED` races the ordinary poll claim (and any other
/// concurrent caller of this function) harmlessly: each skips whatever the
/// other already holds. Returns fewer than `limit` rows -- down to zero --
/// whenever fewer than `limit` due rows exist; callers must treat a short
/// claim as fully expected, not an error.
///
/// `fresh_only` excludes `attempt_index > 1` rows: required for a batched
/// claim (retries always run alone -- see `dispatch_batches`' identical
/// split of an ordinary poll claim) and irrelevant for a plain-job claim
/// (`limit = 1`, dispatched alone regardless of attempt either way).
async fn claim_due_heads_in_op(
    op: &mut impl es_entity::AtomicOperation,
    job_type: &JobType,
    instance_id: uuid::Uuid,
    now: DateTime<Utc>,
    limit: i64,
    fresh_only: bool,
) -> Result<Vec<ClaimedRow>, sqlx::Error> {
    if limit <= 0 {
        return Ok(Vec::new());
    }
    // wall_now drives alive_at, exactly like poll_jobs -- liveness is always
    // measured in real time, independent of manual-clock advances.
    let wall_now = chrono::Utc::now();
    sqlx::query_as!(
        ClaimedRow,
        r#"
        WITH heads AS (
            SELECT id, execute_at FROM job_executions
            WHERE job_type = $1 AND state = 'pending' AND execute_at <= $2
              AND (NOT $6 OR attempt_index = 1)
            ORDER BY execute_at, id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        ),
        updated AS (
            UPDATE job_executions je
            SET state = 'running', poller_instance_id = $4, alive_at = $5, execute_at = NULL
            FROM heads WHERE je.id = heads.id
            RETURNING je.id, je.queue_id, je.attempt_index, heads.execute_at AS original_execute_at
        )
        SELECT u.id AS "id!: JobId", u.attempt_index AS "attempt!", u.queue_id AS "queue_id?",
               s.execution_state_json AS "data_json?", u.original_execute_at AS "execute_at!",
               $1 AS "job_type!: JobType"
        FROM updated u
        LEFT JOIN job_execution_states s ON s.id = u.id
        "#,
        job_type as &JobType,
        now,
        limit,
        instance_id,
        wall_now,
        fresh_only,
    )
    .fetch_all(op.as_executor())
    .await
}

/// One unit of head-swap claimed work, ready to dispatch once its claiming
/// transaction commits. Carries everything
/// [`JobPoller::dispatch_job_from_reservation`]/
/// [`JobPoller::dispatch_batch_from_reservation`] need besides the poller
/// itself -- see [`ClaimHook`].
pub(crate) enum DispatchTarget {
    Single(ClaimedRow),
    Batch(JobType, Vec<ClaimedRow>),
}

/// The two independently-subscribed shutdown receivers one dispatch task
/// needs (`job` for the execution future, `monitor` for the shutdown-signal
/// arm of its `tokio::select!`) -- see [`ClaimHook::pre_commit`]'s doc
/// comment for why a head-swap dispatch must obtain these BEFORE its
/// claiming transaction commits, not inside the task that finally runs it.
struct ShutdownSubs {
    job: ShutdownRx,
    monitor: ShutdownRx,
}

/// The head-swap short-circuit's commit hook: does
/// its DB work -- reserving capacity and claiming due heads -- in
/// `pre_commit`, so the `FOR UPDATE SKIP LOCKED` head lock is held only from
/// the claim to commit, not for the remainder of whatever transaction the
/// caller's own write runs in.
///
/// Two kinds of demand merge into one instance per `op` (via [`Self::merge`],
/// which is why registration order across call sites never matters):
/// - `fresh_demand`: a spawn-side call reports `job_type` has `n_due` more
///   due ROWS worth claiming. Translated into `n_due.div_ceil(per_reservation)` NEW
///   reservations of `job_type`'s capacity in `pre_commit` -- taken there,
///   not at registration, so a reservation is never held open across the
///   rest of the caller's transaction.
/// - `recycled`: a completion-side call already owns a unit of `job_type`'s
///   capacity (its own dispatcher is about to release it) and hands it over
///   instead of releasing outright -- see [`JobTracker::recycle`].
pub(crate) struct ClaimHook {
    poller: std::sync::Weak<JobPoller>,
    fresh_demand: HashMap<JobType, usize>,
    recycled: Vec<(JobType, UnitReservation)>,
    claimed: Vec<(UnitReservation, DispatchTarget, ShutdownSubs)>,
}

impl ClaimHook {
    /// [`Self::runs_after`]'s dependency list -- an associated const rather
    /// than a value built inline: `TypeId::of` is a `const fn`, so this is
    /// fully compile-time-determined, and `&Self::RUNS_AFTER` promotes to a
    /// `'static` reference for free (no `OnceLock`, no runtime init check).
    ///
    /// Extended from `[PromoteHeadsHook]` to also name `ExecutionInsertHook`:
    /// a claim must never run before the inserts that create the rows it
    /// would claim. In practice `ClaimHook` is always registered
    /// re-entrantly FROM `ExecutionInsertHook::pre_commit`, which already
    /// guarantees the insert ran first -- but declaring the dependency makes
    /// that hold for every call site (present or future) that hand-composes
    /// both hooks on one `op`, not just the ones where registration happens
    /// to occur in the right order today. See the crate-level hook-DAG note
    /// on [`crate::execution_hooks`] for the full picture.
    pub(crate) const RUNS_AFTER: [std::any::TypeId; 2] = [
        std::any::TypeId::of::<crate::execution_hooks::ExecutionInsertHook>(),
        std::any::TypeId::of::<PromoteHeadsHook>(),
    ];
}

impl es_entity::operation::hooks::CommitHook for ClaimHook {
    /// Subscribes to `shutdown_tx` HERE -- synchronously, inside this
    /// pre-commit pass, before `op` even commits -- rather than inside the
    /// post-commit spawned task. A shutdown that broadcasts between commit
    /// and that task actually running would never be seen by a receiver
    /// that only subscribes inside it (`tokio::sync::broadcast` never
    /// delivers to a late subscriber), and the execution would be
    /// force-aborted instead of drained. Subscribing here is the earliest
    /// point architecturally available for a head-swap dispatch, mirroring
    /// how `dispatch_job`/`dispatch_batch` subscribe synchronously before
    /// ever spawning their task.
    async fn pre_commit(
        mut self,
        mut op: es_entity::operation::hooks::HookOperation<'_>,
    ) -> Result<es_entity::operation::hooks::PreCommitRet<'_, Self>, sqlx::Error> {
        let Some(poller) = self.poller.upgrade() else {
            // The poller is gone (process tearing down). Every recycled
            // reservation just drops with `self` below and releases via
            // `UnitReservation::Drop`; fresh demand was only a count, never
            // a reservation, so there is nothing to release for it.
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        };

        if poller.is_shutting_down() {
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        }

        // The short-circuit is subject to the SAME pool-aware admission as
        // the ordinary poll (`pool_unit_budget`): claiming on an instance
        // with zero shared-pool headroom strands the rows `running` here --
        // locked away from every healthy peer -- instead of leaving them
        // `pending` for whoever can actually run them. With zero budget,
        // claim nothing: recycled reservations drop with `self` and release
        // via `UnitReservation::Drop` exactly as an ordinary completion
        // would, fresh demand stays pending, and the insert's own notify
        // wakes the pool-aware poll, whose clamped-empty pass arms the
        // headroom waiter. A partial budget bounds the claim below.
        let unit_budget = poller.pool_unit_budget();
        if unit_budget == 0 {
            return es_entity::operation::hooks::PreCommitRet::ok(self, op);
        }

        let mut units_by_type: HashMap<JobType, Vec<UnitReservation>> = HashMap::new();
        for (job_type, reservation) in self.recycled.drain(..) {
            units_by_type.entry(job_type).or_default().push(reservation);
        }
        for (job_type, n_due) in self.fresh_demand.drain() {
            if !poller.registry.short_circuit(&job_type) {
                continue;
            }
            let per_reservation = poller.claim_shape(&job_type).0.max(1) as usize;
            let n_reservations = n_due.div_ceil(per_reservation);
            let entry = units_by_type.entry(job_type.clone()).or_default();
            for _ in 0..n_reservations {
                match poller.try_reserve(&job_type) {
                    Some(reservation) => entry.push(reservation),
                    None => break,
                }
            }
        }

        // Bound the total claim by the unit budget, one connection per
        // reservation, same pricing as `plan_claim`. Iteration order (and
        // so which type loses out under a scarce budget) is `HashMap`
        // -arbitrary -- acceptable here, unlike in `plan_claim`: this path
        // carries at most a handful of reservations from one commit, and
        // whatever gets truncated simply releases and stays claimable by
        // the ordinary smallest-demand-first poll.
        //
        // Excess reservations are released via `UnitReservation::release`
        // -- the QUIET release -- not by dropping them: the truncation
        // happens precisely because the budget just ran out, so `Drop`'s
        // poll-loop wake would immediately start a poll that re-reads live
        // headroom -- which cannot yet see the connections this hook's own
        // remaining claims are about to consume -- and admit MORE work on
        // top of them, double-spending the very budget this bound
        // enforces. The truncated units' backlog is re-examined at the
        // next natural wake instead (this hook's surviving dispatches
        // completing, at the latest). Contrast the zero-budget gate above,
        // where the wake IS wanted: there the woken poll's own budget is
        // also zero, so it cannot over-admit -- it just arms the headroom
        // waiter.
        let mut remaining_units = unit_budget;
        for reservations in units_by_type.values_mut() {
            if reservations.len() > remaining_units {
                for reservation in reservations.drain(remaining_units..) {
                    reservation.release();
                }
            }
            remaining_units -= reservations.len();
        }

        let now = op.maybe_now().unwrap_or_else(|| poller.clock.now());
        for (job_type, reservations) in units_by_type {
            if reservations.is_empty() {
                continue;
            }
            if !poller.registry.short_circuit(&job_type) {
                continue;
            }
            let targets = poller
                .claim_after_many(&mut op, &job_type, now, reservations.len())
                .await?;
            for (reservation, target) in reservations.into_iter().zip(targets) {
                let subs = ShutdownSubs {
                    job: poller.shutdown_tx.subscribe(),
                    monitor: poller.shutdown_tx.subscribe(),
                };
                self.claimed.push((reservation, target, subs));
            }
        }

        // Fix 3 (sb-max8): report exactly WHICH rows this pass claimed, per
        // type, so the `NotifierHook` instance `ExecutionInsertHook` staged
        // (deferred behind this hook -- see `RUNS_AFTER`) can check per-id
        // coverage against its `added` and skip the notify only when this
        // claim actually carried THOSE SAME freshly landed rows off -- not
        // merely as many rows of the type. `claim_due_heads_in_op` claims a
        // type's OLDEST due row, which can be pre-existing backlog rather
        // than one of `added`'s ids, so a count match here would be unsound
        // (see `ExecutionInsertHook::due_now_landed_ids_by_type`'s doc
        // comment). Ids come from `self.claimed` (what was actually
        // claimed), not `fresh_demand`/`recycled` (what was asked for) -- a
        // claim can come back short of its reservations.
        let mut claimed_ids: HashMap<JobType, HashSet<JobId>> = HashMap::new();
        for (_, target, _) in &self.claimed {
            match target {
                DispatchTarget::Single(row) => {
                    claimed_ids
                        .entry(row.job_type.clone())
                        .or_default()
                        .insert(row.id);
                }
                DispatchTarget::Batch(job_type, rows) => {
                    let entry = claimed_ids.entry(job_type.clone()).or_default();
                    entry.extend(rows.iter().map(|row| row.id));
                }
            }
        }
        poller.notifier.register_execution_ready_in_op(
            &mut op,
            HashMap::new(),
            claimed_ids,
            HashSet::new(),
        );

        es_entity::operation::hooks::PreCommitRet::ok(self, op)
    }

    /// Fires once a head-swap claim's `op` commits: hands every claimed unit
    /// of work off to [`JobPoller::dispatch_job_from_reservation`]/
    /// [`JobPoller::dispatch_batch_from_reservation`]. Dispatching from here
    /// rather than inline in `pre_commit` because `post_commit` runs
    /// synchronously (mirrors `notifier.rs`'s `NotifierHook`) — the actual
    /// dispatch needs `.await`, so this spawns a detached task per entry,
    /// same as the ordinary poll-claim path already does off the poll loop.
    fn post_commit(self) {
        let Some(poller) = self.poller.upgrade() else {
            // The poller (and with it, the whole process's job service) is
            // gone. Every claimed row is already committed `running`;
            // nothing here can dispatch it, but nothing needs to release the
            // reservations either -- the tracker they belonged to no longer
            // exists.
            return;
        };
        for (reservation, target, subs) in self.claimed {
            let poller = Arc::clone(&poller);
            tokio::spawn(async move {
                match target {
                    DispatchTarget::Single(row) => {
                        let id = row.id;
                        if let Err(e) = poller
                            .dispatch_job_from_reservation(reservation, row, subs)
                            .await
                        {
                            tracing::error!(
                                job_id = %id,
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "failed to dispatch a short-circuit-claimed job"
                            );
                        }
                    }
                    DispatchTarget::Batch(job_type, rows) => {
                        let n_items = rows.len();
                        if let Err(e) = poller
                            .dispatch_batch_from_reservation(
                                reservation,
                                job_type.clone(),
                                rows,
                                subs,
                            )
                            .await
                        {
                            tracing::error!(
                                job_type = %job_type,
                                n_items,
                                exception.message = %e,
                                exception.type = std::any::type_name_of_val(&e),
                                "failed to dispatch a short-circuit-claimed batch"
                            );
                        }
                    }
                }
            });
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        for (job_type, demand) in other.fresh_demand.drain() {
            *self.fresh_demand.entry(job_type).or_insert(0) += demand;
        }
        self.recycled.append(&mut other.recycled);
        self.claimed.append(&mut other.claimed);
        true
    }

    /// Deferred behind every still-pending [`PromoteHeadsHook`] instance: a
    /// caller that hand-composes a promote AND a claim into one transaction
    /// must see the promote's effect before this claim runs, or a row it just moved
    /// to `pending` could be missed. Registration order across the two
    /// hooks never needs to match call order for this to hold -- that is
    /// the entire point of declaring the dependency instead of relying on
    /// it. A type that never registers `PromoteHeadsHook` on this `op`, or
    /// whose instance already executed, imposes no delay.
    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }

    /// Fires when this hook's `pre_commit` succeeded but the commit pass
    /// still failed -- either a LATER hook's `pre_commit` errored (the
    /// transaction is already rolled back; every row this claimed reverted
    /// with it) or the `COMMIT` itself errored (the transaction "may have
    /// landed despite the client error" per the trait's own docs -- this
    /// claim's rows possibly committed `running`, with no `post_commit`
    /// pass ever going to run to dispatch them). `on_rollback` cannot tell
    /// these two cases apart; [`ClaimReconciler`] resolves the ambiguity by
    /// checking, rather than assuming either. Signal-only and sync per the
    /// trait's contract, so the actual DB work is handed to a detached task
    /// (the sanctioned pattern the trait docs name) instead of run here.
    ///
    /// Every `UnitReservation` in `self.claimed`/`self.recycled` still
    /// releases normally via `Drop` when `self` goes out of scope at the
    /// end of this call -- capacity accounting stays on `Drop`
    /// unconditionally (see `ClaimHook`'s own doc comment); this method
    /// only extracts the (id, execute_at, job_type) triples it needs BEFORE
    /// that drop, it never touches the reservations themselves.
    fn on_rollback(self) {
        let Some(poller) = self.poller.upgrade() else {
            return;
        };
        let rows: Vec<(JobId, DateTime<Utc>, JobType)> = self
            .claimed
            .iter()
            .flat_map(|(_, target, _)| match target {
                DispatchTarget::Single(row) => {
                    vec![(row.id, row.execute_at, row.job_type.clone())]
                }
                DispatchTarget::Batch(_, rows) => rows
                    .iter()
                    .map(|row| (row.id, row.execute_at, row.job_type.clone()))
                    .collect(),
            })
            .collect();
        if rows.is_empty() {
            return;
        }
        tokio::spawn(ClaimReconciler::run(poller, rows));
    }
}

/// Detached best-effort recovery for [`ClaimHook::on_rollback`]: resolves an
/// ambiguity `on_rollback` can't itself distinguish -- a rollback where
/// nothing landed versus a `COMMIT` that errored after actually landing --
/// by checking, and restores working state rather than merely logging the
/// degradation. Cost is zero on every successful commit (nothing spawns) and, on the rare
/// rollback of a claiming transaction, one indexed statement in the common
/// case (nothing landed) or one reset statement plus a notify in the rare
/// landed-but-errored case.
struct ClaimReconciler;

impl ClaimReconciler {
    /// 250ms / 1s / 4s -- a pool that just failed a `COMMIT` may be
    /// transiently unreachable; this is generous enough to ride out a blip
    /// without holding the reconciler open indefinitely.
    const BACKOFF: [Duration; 3] = [
        Duration::from_millis(250),
        Duration::from_secs(1),
        Duration::from_secs(4),
    ];

    /// Spawned from `on_rollback`. Retries [`Self::reconcile_unclaimed`] a
    /// bounded number of times against a pool that just failed a commit,
    /// notifies every type it actually reset, then gives up loudly --
    /// `reclaim_lost_jobs`' slower, guaranteed-eventual sweep is the
    /// backstop either way, so abandoning here is a latency regression for
    /// these specific rows, never a correctness one.
    async fn run(poller: Arc<JobPoller>, rows: Vec<(JobId, DateTime<Utc>, JobType)>) {
        for (attempt, backoff) in Self::BACKOFF.into_iter().enumerate() {
            match Self::reconcile_unclaimed(poller.repo.pool(), poller.instance_id, &rows).await {
                Ok((reset_ids, promoted)) if reset_ids.is_empty() && promoted.is_empty() => {
                    return; // the common case: a real rollback, nothing landed
                }
                Ok((reset_ids, promoted)) => {
                    let reset_id_set: HashSet<JobId> = reset_ids.into_iter().collect();
                    let mut notify_types: HashSet<JobType> = rows
                        .iter()
                        .filter(|(id, _, _)| reset_id_set.contains(id))
                        .map(|(_, _, job_type)| job_type.clone())
                        .collect();
                    notify_types.extend(
                        promoted
                            .into_iter()
                            .map(|row| JobType::from_owned(row.job_type)),
                    );
                    for job_type in notify_types {
                        poller.notifier.execution_ready(&job_type);
                    }
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        exception.message = %error,
                        "claim reconciler retrying after a transient error"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        tracing::error!(
            n_rows = rows.len(),
            "claim reconciler exhausted its retries; abandoning to reclaim_lost_jobs' slower backstop"
        );
    }

    /// The actual DB work, in one transaction: un-claims whichever of `rows`
    /// genuinely landed `running` under `instance_id` (the COMMIT-errored
    /// case), restoring each to `pending` at its ORIGINAL `execute_at` -- no
    /// `attempt_index` bump. Unlike `reclaim_lost_jobs`, which must assume a
    /// reclaimed job MAY have executed (a dead poller, unknown progress),
    /// this claim provably never dispatched: dispatch only ever happens
    /// from `ClaimHook::post_commit`, and the hook system fired
    /// `on_rollback` INSTEAD of `post_commit` for this very reason. A pure
    /// un-claim, not a retry.
    ///
    /// The `state = 'running' AND poller_instance_id = $3` guard makes the
    /// reset a no-op against anything that beat it here -- another
    /// instance's `reclaim_lost_jobs` already reset it, or (the common
    /// case) this was a genuine rollback and these rows were never
    /// committed `running` at all. Nothing else can be legitimately
    /// EXECUTING these rows: only this never-run `post_commit` could have
    /// dispatched them.
    ///
    /// A reset row is not automatically its queue's rightful head again --
    /// while it sat `running`, an insert could have landed an OLDER
    /// backdated sibling into the same queue: a `running` occupant blocks a
    /// swap at insert time exactly like an unbeaten `pending` one does (see
    /// `ExecutionInsertHook::insert_many`'s doc comment; only a `pending`
    /// occupant is swap-eligible), so that sibling would be sitting
    /// `parked`, older than this row, right now. `reclaim_lost_jobs` will
    /// never revisit this to fix it -- the row is `pending` with a fresh
    /// `alive_at`, not a stale `running` one -- so `PromoteHeadsHook::apply`
    /// runs here, in the SAME transaction as the reset, exactly like every
    /// other "rows just moved to pending" call site in this crate.
    ///
    /// Returns the ids actually reset and the siblings actually promoted,
    /// so `run` knows both whether anything happened and which types to
    /// notify.
    async fn reconcile_unclaimed(
        pool: &PgPool,
        instance_id: uuid::Uuid,
        rows: &[(JobId, DateTime<Utc>, JobType)],
    ) -> Result<(Vec<JobId>, Vec<PromotedRow>), sqlx::Error> {
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let ids: Vec<uuid::Uuid> = rows
            .iter()
            .map(|(id, _, _)| uuid::Uuid::from(*id))
            .collect();
        let execute_ats: Vec<DateTime<Utc>> = rows.iter().map(|(_, at, _)| *at).collect();

        let mut tx = pool.begin().await?;
        let reset = sqlx::query_scalar!(
            r#"
            -- `(queue_id, id)`-ordered lock first, for the same reason
            -- `reclaim_lost_jobs` takes one: this transaction goes on to call
            -- `PromoteHeadsHook::apply`, which locks in that order, over
            -- queues this reset just touched.
            WITH locked AS MATERIALIZED (
                SELECT je.id, u.execute_at FROM job_executions je
                JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                  ON je.id = u.id
                WHERE je.state = 'running' AND je.poller_instance_id = $3
                ORDER BY je.queue_id, je.id
                FOR NO KEY UPDATE OF je
            )
            UPDATE job_executions je
            SET state = 'pending', poller_instance_id = NULL, execute_at = l.execute_at
            FROM locked l WHERE je.id = l.id
            RETURNING je.id AS "id!: JobId"
            "#,
            &ids,
            &execute_ats,
            instance_id,
        )
        .fetch_all(&mut *tx)
        .await?;

        let reset_uuids: Vec<uuid::Uuid> = reset.iter().map(|id| uuid::Uuid::from(*id)).collect();
        let promoted = PromoteHeadsHook::apply(&mut tx, &reset_uuids).await?;

        tx.commit().await?;
        Ok((reset, promoted))
    }
}

/// Resets every `running` row of `supported_job_types` whose `alive_at` is
/// older than `alive_threshold` (and isn't one of `self_live_ids`, this
/// instance's own currently-live jobs) back to `pending` for a retry, then
/// restores Invariant B for every queue those rows occupy: an older parked
/// sibling should run first during the reclaimed row's backoff, and that
/// sibling can be a different type than the row it displaced -- reported
/// separately, so callers can wake the right pollers.
///
/// Takes its locks in `(queue_id, id)` order for the same reason
/// [`sweep_orphaned_parked_rows`] does, and more urgently: this transaction
/// goes on to call [`PromoteHeadsHook::apply`], which locks in exactly that
/// order, so leaving the reclaim `UPDATE` in planner scan order would make one
/// transaction acquire two overlapping sets of locks in two different orders.
/// A queue whose row this reclaims is very often the same queue `apply` then
/// swaps.
/// A row the lost-handler took back from a poller that stopped heartbeating.
///
/// Carries `alive_at` -- the heartbeat that froze -- so the `lost job` log can
/// report how long the job actually stalled. Two production runs of lost-job
/// bursts went undiagnosed because that log was id-only: with neither the
/// type nor the stall age, there was no way to tie a burst back to the
/// batch-seal failures that caused it.
struct ReclaimedJob {
    id: JobId,
    job_type: JobType,
    alive_at: DateTime<Utc>,
}

async fn reclaim_lost_jobs(
    pool: &PgPool,
    instance_id: uuid::Uuid,
    supported_job_types: &[JobType],
    alive_threshold: DateTime<Utc>,
    reschedule_at: DateTime<Utc>,
    self_live_ids: &[uuid::Uuid],
) -> Result<(Vec<ReclaimedJob>, Vec<String>), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        WITH locked AS MATERIALIZED (
            SELECT je.id FROM job_executions je
            WHERE je.state = 'running'
              AND je.alive_at < $1::timestamptz
              AND je.job_type = ANY($2)
              AND (je.poller_instance_id IS DISTINCT FROM $4 OR je.id <> ALL($5))
            ORDER BY je.queue_id, je.id
            FOR NO KEY UPDATE
        )
        UPDATE job_executions je
        SET state = 'pending', execute_at = $3, attempt_index = attempt_index + 1, poller_instance_id = NULL
        FROM locked l WHERE je.id = l.id
        RETURNING je.id AS "id!: JobId", je.job_type AS "job_type!: JobType",
                  je.alive_at AS "alive_at!"
        "#,
        alive_threshold,
        supported_job_types as _,
        reschedule_at,
        instance_id,
        self_live_ids,
    )
    .fetch_all(&mut *tx)
    .await?;

    let reclaimed_uuids: Vec<uuid::Uuid> = rows.iter().map(|r| uuid::Uuid::from(r.id)).collect();
    let promoted = PromoteHeadsHook::apply(&mut tx, &reclaimed_uuids).await?;

    tx.commit().await?;
    Ok((
        rows.into_iter()
            .map(|r| ReclaimedJob {
                id: r.id,
                job_type: r.job_type,
                alive_at: r.alive_at,
            })
            .collect(),
        promoted.into_iter().map(|row| row.job_type).collect(),
    ))
}

/// Recover parked rows whose queue has no active (`pending`/`running`) row:
/// a spawn conflicted against a queue's active slot and landed `parked`, but
/// the occupant completed (and promoted nothing, since the parked row wasn't
/// visible to it yet) before the parked insert committed. Piggybacked on the
/// lost-handler's cadence (same task, one extra statement) rather than its
/// own timer.
///
/// A **backstop, no longer the mechanism**. That race is closed at the
/// source by [`crate::execution_hooks::ExecutionInsertHook`], which pins each
/// parked queue's occupant with `FOR KEY SHARE` and adopts the queue outright
/// if the occupant is already gone (see its `lock_queue_occupants` /
/// `adopt_orphaned_queues`). What remains for this sweep is rows orphaned by
/// a peer still running a pre-lock build -- a rolling deploy, most obviously
/// -- plus defence in depth against any future write path that frees a
/// queue's active row without promoting behind it. In steady state on a
/// fully-upgraded fleet it finds nothing, which is why its cadence can stay
/// piggybacked rather than tightened.
///
/// Returns the job type of every row promoted, so the caller can wake the
/// pollers that cover it.
///
/// Locks every head in `(queue_id, id)` order before promoting any of them --
/// the one global order every multi-row WAITING locker of this table agrees
/// on (`ExecutionInsertHook::lock_queue_occupants`, `PromoteHeadsHook::apply`
/// and `apply_freed`, both `batch_dispatcher` completers' `to_delete`). This
/// sweep is the widest locker of the lot: unlike those, its row set is
/// unscoped -- every orphaned queue in the table, across every type and every
/// process -- so it overlaps whatever any concurrent writer happens to be
/// touching. Without the ordered `locked` CTE the bare `UPDATE ... FROM heads`
/// acquires in planner scan order (`heads`'s row order is not a lock order),
/// which agrees with the rest of the table's writers only by accident, and
/// stops agreeing whenever a plan changes underneath it.
async fn sweep_orphaned_parked_rows(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar!(
        r#"
        WITH orphan_queues AS (
            SELECT DISTINCT p.queue_id
            FROM job_executions p
            WHERE p.state = 'parked'
              AND NOT EXISTS (
                  SELECT 1 FROM job_executions a
                  WHERE a.queue_id = p.queue_id AND a.state IN ('pending', 'running')
              )
        ), heads AS (
            SELECT h.id FROM orphan_queues oq
            CROSS JOIN LATERAL (
                SELECT id FROM job_executions
                WHERE state = 'parked' AND queue_id = oq.queue_id
                ORDER BY execute_at, id
                LIMIT 1
            ) h
        ), locked AS MATERIALIZED (
            SELECT je.id FROM job_executions je
            WHERE je.id IN (SELECT id FROM heads)
            ORDER BY je.queue_id, je.id
            FOR NO KEY UPDATE
        )
        UPDATE job_executions je SET state = 'pending'
        FROM locked l WHERE je.id = l.id
        RETURNING je.job_type
        "#,
    )
    .fetch_all(pool)
    .await
}

#[instrument(
    name = "job.poll_jobs",
    level = "debug",
    skip(pool, pollable_types, row_limits, clock),
    fields(n_jobs_to_poll, instance_id = %instance_id, n_jobs_found = tracing::field::Empty)
)]
#[allow(clippy::too_many_arguments)]
async fn poll_jobs(
    pool: &PgPool,
    n_jobs_to_poll: usize,
    instance_id: uuid::Uuid,
    pollable_types: &[super::entity::JobType],
    row_limits: &[i32],
    headroom: i32,
    clock: &ClockHandle,
) -> Result<JobPollResult, sqlx::Error> {
    // sim_now drives execute_at scheduling (whatever clock the application uses);
    // wall_now drives the initial alive_at heartbeat so liveness is always
    // measured in real time, independent of manual-clock advances.
    let sim_now = clock.now();
    let wall_now = chrono::Utc::now();
    Span::current().record("now", tracing::field::display(sim_now));

    // A single autocommit statement on `pool` -- a dedicated pool
    // (`build_internal_pool`) whose connections already carry
    // `plan_cache_mode = force_generic_plan` / `enable_bitmapscan = off` from
    // `after_connect`, so there is no `BEGIN`/`SET LOCAL`/`COMMIT` here to pay
    // for on every poll (5 round trips -> 1). See PERFORMANCE.md, "Ordered
    // index access is mandatory", for why both overrides are needed at all.
    let rows = sqlx::query_as!(
        JobPollRow,
        r#"
        -- Claim admission, head-only. `state = 'pending'` contains ONLY
        -- already-claimable rows: a queue's blocked backlog is `parked`
        -- instead (promoted at completion time -- see the freed-queue
        -- promotion in `finalizer.rs`), so queued and unqueued
        -- rows share one ordered scan with no anti-join and no per-queue
        -- LATERAL. See PERFORMANCE.md ("Claim admission") for the shape this
        -- replaced and the measurements behind this one.
        WITH limits AS (
            -- `type_window_limit` is each type's OWN admission budget for
            -- step 1, not a shared global one: a type's window is bounded by
            -- its own row_limit (never by another type's backlog), which is
            -- what stops one backlogged type from crowding a due row of
            -- another type out of the window entirely. Pinned by
            -- `capped_type_backlog_does_not_starve_another_type` and (for
            -- batched types) `claims_are_capped_by_free_batch_slots`.
            SELECT l.job_type, l.row_limit,
                   LEAST(l.row_limit, $1::int4) * $7::int4 AS type_window_limit
            FROM UNNEST($4::text[], $6::int4[]) AS l(job_type, row_limit)
            WHERE l.row_limit > 0
        ),
        window_rows AS (
            -- One LATERAL probe per type, each bounded by ITS OWN budget
            -- ($1 x $7 rows capped at that type's own row_limit), never by
            -- how much is pending: cost is O(budget), flat in backlog --
            -- true only because `idx_job_executions_pending_execute_at`
            -- leads with `job_type`, so this probe is an index descent into
            -- that type's own slice rather than a filter-scan of every
            -- other type's pending rows too (see PERFORMANCE.md, "Claim
            -- admission"). Ordering is `(execute_at, id)` within that slice
            -- and the tiebreak is load-bearing for the same reason it
            -- always was -- a total order, so each type's window is a
            -- well-defined prefix rather than an arbitrary cut through a
            -- group of rows sharing a timestamp.
            SELECT d.id, d.execute_at, d.job_type
            FROM limits t
            CROSS JOIN LATERAL (
                SELECT je.id, je.execute_at, je.job_type
                FROM job_executions je
                WHERE je.state = 'pending'
                  AND je.job_type = t.job_type
                  AND je.execute_at <= $2::timestamptz
                ORDER BY je.execute_at, je.id
                LIMIT t.type_window_limit
            ) d
        ),
        ordered_candidates AS (
            -- Interleave types round-robin: every type's oldest candidate
            -- ranks ahead of any type's second, so the global LIMIT below
            -- cannot be consumed end-to-end by one backlogged type. Within a
            -- rank it is still oldest-first.
            SELECT id, execute_at, job_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY job_type ORDER BY execute_at
                   ) AS type_rn
            FROM window_rows
        ),
        locked AS (
            -- The join to job_executions sits BELOW the LIMIT so it runs
            -- lazily: only rows LockRows actually pulls get probed. The sort
            -- above is a blocking node, so the full candidate set is still
            -- materialised and SKIP LOCKED falls through a contended row
            -- exactly as before -- `headroom` still exists solely to give it
            -- somewhere to fall through to; see the constant's doc comment.
            --
            -- FOR UPDATE OF je: bare FOR UPDATE errors on a nullable join side.
            SELECT je.id, je.attempt_index, c.job_type, c.execute_at
            FROM ordered_candidates c
            JOIN job_executions je ON je.id = c.id
            ORDER BY c.type_rn ASC, c.execute_at ASC
            LIMIT $1
            FOR UPDATE OF je SKIP LOCKED
        ),
        selected_jobs AS (
            -- The budget is enforced HERE, on rows actually held: the window
            -- deliberately over-gathers (see $7) so there is something to
            -- fall through to when a peer holds a row. Rows over a type's cap
            -- are simply not claimed; their locks release at commit.
            -- execution_state_json is joined after the LIMIT, so it is
            -- fetched only for winners.
            SELECT t.id, cp.execution_state_json AS data_json, t.attempt_index
            FROM (
                SELECT l.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY l.job_type ORDER BY l.execute_at
                       ) AS type_rn
                FROM locked l
            ) t
            JOIN limits lim ON lim.job_type = t.job_type
            LEFT JOIN job_execution_states cp ON cp.id = t.id
            WHERE t.type_rn <= lim.row_limit
        ),
        updated AS (
            UPDATE job_executions AS je
            SET state = 'running', alive_at = $5, execute_at = NULL, poller_instance_id = $3
            FROM selected_jobs
            WHERE je.id = selected_jobs.id
              AND je.state = 'pending'
            RETURNING je.id, selected_jobs.data_json, je.attempt_index, je.queue_id
        ),
        min_wait AS (
            SELECT MIN(execute_at) AS next_due_at
            FROM job_executions
            WHERE state = 'pending'
            AND job_type = ANY($4)
            AND execute_at > $2::timestamptz
        ),
        window_counts AS (
            SELECT job_type, COUNT(*) AS cnt FROM window_rows GROUP BY job_type
        ),
        poll_status AS (
            -- Re-poll immediately only when this poll provably left claimable
            -- work behind: it filled its budget, or at least one type's OWN
            -- window came back full while still yielding at least one
            -- pollable candidate overall (rows past that type's window are
            -- unseen and already due). A window that came back short for
            -- every type means every claimable due row was examined, so
            -- `next_due_at` is the honest next deadline -- exact now, not
            -- merely a heuristic: every window row is by construction
            -- already a candidate. Blocked queues are
            -- covered by a wake rather than a spin: the finalizer's
            -- terminal delete / the orphan sweeper report `execution_ready`
            -- when a queue's head is promoted.
            SELECT ((SELECT COUNT(*) FROM locked) >= $1
                 OR (EXISTS (
                        SELECT 1 FROM window_counts wc
                        JOIN limits t ON t.job_type = wc.job_type
                        WHERE wc.cnt >= t.type_window_limit
                     )
                     AND (SELECT COUNT(*) FROM ordered_candidates) > 0)) AS may_have_more
        )
        SELECT * FROM (
            SELECT
                u.id AS "id?: JobId",
                u.data_json AS "data_json?: JsonValue",
                u.attempt_index AS "attempt_index?",
                u.queue_id AS "queue_id?",
                NULL::TIMESTAMPTZ AS "next_due_at?",
                ps.may_have_more AS "may_have_more!"
            FROM updated u, poll_status ps
            UNION ALL
            SELECT
                NULL::UUID AS "id?: JobId",
                NULL::JSONB AS "data_json?: JsonValue",
                NULL::INT AS "attempt_index?",
                NULL::VARCHAR AS "queue_id?",
                mw.next_due_at AS "next_due_at?",
                ps.may_have_more AS "may_have_more!"
            FROM min_wait mw, poll_status ps
        ) AS result
        "#,
        n_jobs_to_poll as i32,
        sim_now,
        instance_id,
        pollable_types as _,
        wall_now,
        row_limits,
        headroom,
    )
    .fetch_all(pool)
    .await?;

    Span::current().record("n_jobs_found", rows.len());
    Ok(JobPollResult::from_rows(rows))
}

/// Whether the poller may sleep on `next_due_at`, or must re-poll
/// immediately because this poll couldn't see the full due backlog.
#[derive(Debug, Clone, Copy)]
struct PollWindow {
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
}

impl PollWindow {
    /// Sleep duration for a fresh `now` read at the point of use.
    fn sleep_for(&self, now: DateTime<Utc>) -> Duration {
        if self.may_have_more {
            Duration::ZERO
        } else {
            duration_until(self.next_due_at, now)
        }
    }
}

#[derive(Debug)]
enum JobPollResult {
    /// `window.next_due_at` is `None` when nothing else is pending for these types.
    Jobs {
        jobs: Vec<PolledJob>,
        window: PollWindow,
    },
    WaitTillNextJob(PollWindow),
}

#[derive(Debug)]
struct JobPollRow {
    id: Option<JobId>,
    data_json: Option<JsonValue>,
    attempt_index: Option<i32>,
    queue_id: Option<String>,
    next_due_at: Option<DateTime<Utc>>,
    may_have_more: bool,
}

impl JobPollResult {
    /// Convert raw query rows into a JobPollResult. The min-wait row (`id`
    /// NULL) is present in every result set; row order is not assumed.
    pub fn from_rows(rows: Vec<JobPollRow>) -> Self {
        let mut jobs = Vec::with_capacity(rows.len());
        let mut window = PollWindow {
            next_due_at: None,
            may_have_more: false,
        };
        for row in rows {
            window.may_have_more = row.may_have_more;
            match (row.id, row.attempt_index) {
                (Some(id), Some(attempt_index)) => jobs.push(PolledJob {
                    id,
                    data_json: row.data_json,
                    attempt: attempt_index as u32,
                    queue_id: row.queue_id,
                }),
                _ => window.next_due_at = row.next_due_at,
            }
        }
        if jobs.is_empty() {
            JobPollResult::WaitTillNextJob(window)
        } else {
            JobPollResult::Jobs { jobs, window }
        }
    }
}

/// Sleep duration until `deadline`, clamped to zero if already past. `None`
/// falls back to `MAX_WAIT`. Pass a `now` read as close as possible to use.
fn duration_until(deadline: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    match deadline {
        Some(at) => (at - now).to_std().unwrap_or(Duration::ZERO),
        None => MAX_WAIT,
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
    /// Shut this instance's poller down, in an order that keeps the drain
    /// honest:
    ///
    /// 1. **Stop the poll loop and wait for it to exit.** Nothing new can be
    ///    claimed or dispatched after this point, so the set of live executions
    ///    is final. Doing this *before* step 2 is what makes the ack collection
    ///    complete: `tokio::sync::broadcast` only delivers to receivers that
    ///    subscribed before `send`, so a generation dispatched after the
    ///    broadcast would never see the signal, never ack, never be waited
    ///    for — and would then be force-aborted mid-flight by
    ///    [`kill_remaining_jobs`], racing its own completion write on the same
    ///    `Job` aggregate (`ConcurrentModification`, with the loser's execution
    ///    outcome discarded).
    /// 2. Broadcast to the monitor tasks and collect their acks.
    /// 3. Wait for every acked execution to finish.
    /// 4. Force-reschedule whatever is genuinely still `running`.
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
    async fn perform(&self) -> Result<(), JobError> {
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
            // No subscribers left. With the poll loop already stopped and
            // drained (step 1) that means there is no live execution to wait
            // for — every monitor task holds a subscription for as long as its
            // execution runs — so there is nothing to give a grace period to.
            tracing::info!("No live job monitors at shutdown, nothing to drain");
        }

        kill_remaining_jobs(Arc::clone(&self.repo), self.instance_id, self.clock.clone()).await
    }

    /// Signal `main_loop` to stop and wait until it has actually exited.
    ///
    /// Returns `false` if the loop did not report back within
    /// `shutdown_timeout` — the shutdown then continues regardless, since
    /// [`kill_remaining_jobs`] still releases whatever the poller left claimed;
    /// a wedged poll must not wedge shutdown.
    ///
    /// A dropped `poll_exited` sender resolves this immediately: the loop's task
    /// is gone (aborted with the handle, or panicked), which is as stopped as it
    /// gets.
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

/// Release every execution this instance still holds, and record the forced
/// reschedule on each `Job`.
///
/// The `UPDATE` runs first and inside `op`, so by the time anything is read the
/// transaction already holds a row lock on every execution it is about to
/// abort. The entity read then happens **in the same op** (not on a separate
/// pool connection): every execution-path writer — `complete_job`,
/// `reschedule_job`, the retry branch of `fail_job` — writes its
/// `job_executions` row before appending its events, so those locks fence them
/// out and the version snapshot read here cannot go stale under them.
///
/// Writers that touch a `Job` *without* its execution row (`set_result`) are not
/// fenced by those locks, so each entity write additionally gets its own
/// `SAVEPOINT`: a lost race rolls back that one row's audit events instead of
/// failing the whole shutdown, and the release itself — the part that decides
/// whether the job is schedulable again — is already durable in the same
/// transaction either way.
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
        -- `(queue_id, id)`-ordered, like every other multi-row waiting locker
        -- of this table: shutdown releases an unbounded set of rows spread
        -- across arbitrary queues while peers are still spawning, completing
        -- and sweeping against the same ones.
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
            // The row is released regardless (that write is outside this
            // savepoint), so the job stays schedulable; only its abort audit
            // trail is missing.
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
    use super::*;

    async fn init_pool() -> anyhow::Result<PgPool> {
        let pg_con = std::env::var("PG_CON").unwrap();
        Ok(sqlx::PgPool::connect(&pg_con).await?)
    }

    /// `PoolConnection`'s `Drop` spawns a task to actually return the
    /// connection to the pool (sqlx-core's `return_to_pool`) instead of
    /// doing it inline, so `size()`/`num_idle()` don't reflect a `drop()`
    /// until that task gets a turn on the executor. Yields until they do,
    /// bounded so a real bug shows up as a failed assertion rather than a
    /// hang -- not a sleep, since nothing here waits on real time.
    async fn settle(pool: &PgPool, expected_idle: u32) {
        for _ in 0..1000 {
            if pool.num_idle() as u32 == expected_idle {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// `pool_connection_headroom` reads 0 when the pool has no headroom
    /// left, and resumes reading the true count the instant a connection
    /// frees -- work item A of `handoff-pool-aware-claiming-and-fail-path.md`.
    /// A direct call rather than driving it through
    /// `poll_and_dispatch`/`main_loop`: those go through the tokio
    /// timer/notify machinery, which would make a negative assertion
    /// ("claims stayed 0") either a blind sleep or a second, separate piece
    /// of test infrastructure this crate's test suite doesn't otherwise
    /// have. `pool_connection_headroom` is exactly the new mechanism worth
    /// pinning; this proves it without either -- including that the read
    /// reaches 0 when the pool is fully checked out, the condition under
    /// which `pool_unit_budget` claims nothing and the headroom waiter
    /// takes over (covered end to end by `tests/pool_headroom_waiter.rs`,
    /// with `tests/pool_congestion.rs`/`tests/pool_terminal_write_safety.rs`
    /// covering the unit-budget conversion and `JobRegistry::plan_claim`'s
    /// per-type spending).
    #[tokio::test]
    async fn pool_headroom_tracks_live_connections() -> anyhow::Result<()> {
        let pg_con = std::env::var("PG_CON").unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(&pg_con)
            .await?;

        // Full headroom: never exceeds `max_connections`.
        assert_eq!(pool_connection_headroom(&pool), 3);

        // Hold 2 of 3 connections -> headroom 1.
        let c1 = pool.acquire().await?;
        let c2 = pool.acquire().await?;
        assert_eq!(pool_connection_headroom(&pool), 1);

        // Hold the last one too -> headroom 0.
        let c3 = pool.acquire().await?;
        assert_eq!(pool_connection_headroom(&pool), 0);

        // Releasing restores headroom -- this is a live read, not something
        // latched from a prior poll. `PoolConnection`'s `Drop` spawns a task
        // to actually hand the connection back (sqlx-core's
        // `return_to_pool`) rather than doing it inline, so
        // `size()`/`num_idle()` only reflect a `drop()` once that task gets
        // a turn on the executor -- `settle` yields until they do.
        drop(c3);
        settle(&pool, 1).await;
        assert_eq!(pool_connection_headroom(&pool), 1);

        drop(c1);
        drop(c2);
        settle(&pool, 3).await;
        assert_eq!(pool_connection_headroom(&pool), 3);

        Ok(())
    }

    /// The guard behind `arm_elastic_rotation_waiter` -- an `AtomicBool` CAS,
    /// the same shape `arm_pool_headroom_waiter` uses -- must collapse many
    /// concurrent/repeated arm calls into AT MOST ONE live waiter task, not
    /// one per call. Without it, every poll that observed
    /// `elastic_rotation_partial` would start its own independent
    /// wake-in-`ELASTIC_ROTATION_RECHECK` chain -- and since that condition
    /// is STANDING under load (elastic types outnumbering their tier
    /// budget), unlike the pool-headroom waiter's transient one, nothing
    /// would ever stop new chains from starting: the number of live chains
    /// would grow without bound over the process lifetime, against the
    /// dedicated poll pool.
    ///
    /// Proven by direct observation, not by re-reading the guard: calls
    /// `arm_elastic_rotation_waiter` many times back to back with no
    /// `.await` between them -- simulating a burst of notify-driven polls
    /// all observing the same standing condition, the exact shape a
    /// production accumulation would start from -- and asserts exactly one
    /// task was ever spawned.
    #[tokio::test]
    async fn elastic_rotation_waiter_arm_is_idempotent_while_pending() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = Arc::new(JobRepo::new(&pool));
        let tracker = Arc::new(JobTracker::new(0, 10));
        let registry = JobRegistry::new(Arc::clone(&tracker));
        let router = Arc::new(JobNotificationRouter::new(
            &pool,
            Arc::clone(&repo),
            16,
            Duration::from_secs(60),
        ));
        let notifier =
            JobEventNotifier::spawn(&pool, Arc::clone(&tracker), router.terminal_sender());
        let poller = Arc::new(
            JobPoller::new(
                JobPollerConfig::default(),
                repo,
                registry,
                tracker,
                router,
                notifier,
                ClockHandle::realtime(),
            )
            .await?,
        );

        for _ in 0..50 {
            poller.arm_elastic_rotation_waiter();
        }

        assert_eq!(
            poller.elastic_rotation_waiter_spawns.load(Ordering::SeqCst),
            1,
            "many arm calls while a waiter is already pending must spawn \
             at most one task, not one per call"
        );

        Ok(())
    }

    /// `unit_budget`'s factor arithmetic: the default 1.0 is the identity
    /// (current behaviour exactly), fractional factors shift admission in
    /// the expected direction, and rounding is `floor` -- including the
    /// deliberate `floor(1 / 1.5) = 0` case the pool-headroom waiter's
    /// budget-not-headroom wake condition exists for.
    #[test]
    fn unit_budget_applies_connections_per_job_factor() {
        // Default 1.0: identity.
        for headroom in [0, 1, 5, 50] {
            assert_eq!(unit_budget(headroom, 1.0), headroom);
        }
        // Cheaper jobs admit more...
        assert_eq!(unit_budget(5, 0.5), 10);
        // ...expensive jobs admit less, rounding down.
        assert_eq!(unit_budget(5, 1.5), 3);
        assert_eq!(unit_budget(5, 2.0), 2);
        assert_eq!(unit_budget(1, 1.5), 0);
        // Zero headroom is a zero budget at any price.
        assert_eq!(unit_budget(0, 0.5), 0);
        assert_eq!(unit_budget(0, 2.0), 0);
    }

    /// Seed a real `Job` aggregate (events and all) plus a `running` execution
    /// row owned by `instance_id` — what a live execution looks like to
    /// [`kill_remaining_jobs`].
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

    /// Block until some backend on this database is waiting on a lock.
    ///
    /// The synchronisation point for the test below: it makes the interleaving
    /// an observed fact rather than a timing assumption — no sleeping until the
    /// race "probably" happened.
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

    /// `kill_remaining_jobs` must survive losing a version race on a `Job` it is
    /// force-rescheduling, rather than letting it escape `Jobs::shutdown()`
    /// as `JobModifyError - ConcurrentModification`.
    ///
    /// The concurrent writer here has `set_result`'s shape: it appends to the
    /// `Job` without touching the execution row, so the row locks
    /// `kill_remaining_jobs` holds do not fence it out. It claims the entity's
    /// next event sequence first and commits while the kill is mid-flight, so
    /// the kill's own append is the one that collides.
    ///
    /// Releasing the execution row is what must survive: the job has to stay
    /// schedulable, and shutdown must not fail because one audit append lost a
    /// race.
    #[tokio::test]
    async fn kill_remaining_jobs_survives_losing_a_concurrent_entity_write() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = Arc::new(JobRepo::new(&pool));
        let clock = ClockHandle::realtime();
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("kill-race-{}", uuid::Uuid::now_v7());

        let id = seed_running_entity(&pool, &repo, &job_type, instance_id).await?;

        // The competing writer: entity append staged, not yet committed, so it
        // owns the next event sequence.
        let mut writer_op = repo.begin_op_with_clock(&clock).await?;
        let mut job = repo.find_by_id_in_op(&mut writer_op, id).await?;
        let return_value = crate::outcome::JobReturnValue::try_from(&"progress")?;
        assert!(job.update_return_value(return_value).did_execute());
        repo.update_in_op(&mut writer_op, &mut job).await?;

        // The kill blocks on that staged sequence...
        let kill = tokio::spawn(kill_remaining_jobs(
            Arc::clone(&repo),
            instance_id,
            clock.clone(),
        ));
        wait_for_blocked_backend(&pool).await?;

        // ...and only now does the competing write become the winner.
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

    async fn seed_running_job(
        pool: &PgPool,
        job_type: &str,
        instance_id: uuid::Uuid,
        alive_at: DateTime<Utc>,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
            .bind(uuid)
            .bind(job_type)
            .bind(now)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, poller_instance_id, attempt_index, alive_at, created_at) \
             VALUES ($1, $2, 'running', $3, 1, $4, $5)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(instance_id)
        .bind(alive_at)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    async fn seed_pending_job(
        pool: &PgPool,
        job_type: &str,
        execute_at: DateTime<Utc>,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query("INSERT INTO jobs (id, job_type, created_at) VALUES ($1, $2, $3)")
            .bind(uuid)
            .bind(job_type)
            .bind(now)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, state, attempt_index, execute_at, alive_at, created_at) \
             VALUES ($1, $2, 'pending', 1, $3, $4, $5)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(execute_at)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// A backlogged capped type must NOT be able to consume another type's
    /// admission budget: each type scans under its own budget, so a type
    /// with 10 older due rows and a second type with one younger due row
    /// both get claimed in the same poll -- the first type's backlog never
    /// crowds the second type's row out of a shared candidate window.
    #[tokio::test]
    async fn capped_type_backlog_does_not_starve_another_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let type_a = format!("budget-dominant-{}", uuid::Uuid::now_v7());
        let type_b = format!("budget-starved-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;

        // A: 10 due rows, ALL older than B's single row.
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);
        let mut a_ids = Vec::new();
        for i in 0..10i64 {
            a_ids
                .push(seed_pending_job(&pool, &type_a, base + chrono::Duration::seconds(i)).await?);
        }
        // B: one due row, younger than all of A's but still due.
        let b_id = seed_pending_job(
            &pool,
            &type_b,
            chrono::Utc::now() - chrono::Duration::seconds(1),
        )
        .await?;

        let pollable_types = vec![
            JobType::from_owned(type_a.clone()),
            JobType::from_owned(type_b.clone()),
        ];
        // A capped to 1; B uncapped (row_limit = n_jobs_to_poll).
        let row_limits = vec![1, n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert!(
                    claimed.contains(&b_id),
                    "B's due row must be claimed: A's older backlog can no \
                     longer consume the window B's budget entitles it to"
                );
                assert_eq!(
                    claimed.iter().filter(|id| a_ids.contains(id)).count(),
                    1,
                    "A is capped at 1 and must claim exactly one row"
                );
                assert_eq!(claimed.len(), 2, "one row per type, both claimed");
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn self_reclaim_skips_live_jobs() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let self_id = uuid::Uuid::now_v7();
        let other_id = uuid::Uuid::now_v7();
        let job_type = format!("reclaim-gate-{}", uuid::Uuid::now_v7());
        let stale = chrono::Utc::now() - chrono::Duration::seconds(600);

        let live_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let orphan_self = seed_running_job(&pool, &job_type, self_id, stale).await?;
        let other_instance = seed_running_job(&pool, &job_type, other_id, stale).await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(60);
        let reschedule_at = chrono::Utc::now();
        let self_live_ids = vec![uuid::Uuid::from(live_self)];
        let types = vec![JobType::from_owned(job_type.clone())];

        let reclaimed: std::collections::HashSet<JobId> = reclaim_lost_jobs(
            &pool,
            self_id,
            &types,
            threshold,
            reschedule_at,
            &self_live_ids,
        )
        .await?
        .0
        .into_iter()
        .map(|job| job.id)
        .collect();

        assert!(
            reclaimed.contains(&orphan_self),
            "self-owned orphan (no live future) must be reclaimed"
        );
        assert!(
            reclaimed.contains(&other_instance),
            "another instance's stale row must be reclaimed"
        );
        assert!(
            !reclaimed.contains(&live_self),
            "self-owned row with a live runner must NOT be reclaimed"
        );

        let row: (String, Option<uuid::Uuid>, i32) = sqlx::query_as(
            "SELECT state::text, poller_instance_id, attempt_index \
             FROM job_executions WHERE id = $1",
        )
        .bind(uuid::Uuid::from(live_self))
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.0, "running");
        assert_eq!(row.1, Some(self_id));
        assert_eq!(row.2, 1);

        Ok(())
    }
    async fn seed_queued_job(
        pool: &PgPool,
        job_type: &str,
        queue_id: &str,
        execute_at: DateTime<Utc>,
        state: &str,
    ) -> anyhow::Result<JobId> {
        let id = JobId::new();
        let uuid = uuid::Uuid::from(id);
        let now = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(queue_id)
        .bind(now)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, \
              poller_instance_id, created_at) \
             VALUES ($1, $2, $3, $4::JobExecutionState, 1, \
                     CASE WHEN $4 = 'running' THEN NULL ELSE $5 END, $6, \
                     CASE WHEN $4 = 'running' THEN gen_random_uuid() END, $7)",
        )
        .bind(uuid)
        .bind(job_type)
        .bind(queue_id)
        .bind(state)
        .bind(execute_at)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// A blocked queue's backlog must not consume the admission budget: a
    /// blocked queue's backlog is `parked`, not `pending` -- it never enters
    /// `state = 'pending'` at all, so it structurally cannot appear in the
    /// claim window no matter how deep it is. See PERFORMANCE.md,
    /// "Claim admission".
    #[tokio::test]
    async fn blocked_queue_backlog_does_not_consume_the_budget() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("cliff-{}", uuid::Uuid::now_v7());
        let hot_queue = format!("hot-{}", uuid::Uuid::now_v7());

        let n_jobs_to_poll = 2usize;
        let base = chrono::Utc::now() - chrono::Duration::seconds(3600);

        // The blocked queue already has a job running, and holds far more due
        // rows than the whole candidate window (n * CONTENTION_HEADROOM), all
        // of them OLDER than the claimable work below -- parked, per
        // Invariant A (only one pending/running row may exist per queue).
        seed_queued_job(&pool, &job_type, &hot_queue, base, "running").await?;
        for i in 0..(n_jobs_to_poll as i64 * CONTENTION_HEADROOM as i64 * 3) {
            seed_queued_job(
                &pool,
                &job_type,
                &hot_queue,
                base + chrono::Duration::seconds(i),
                "parked",
            )
            .await?;
        }

        // Two ordinary queues, younger than every row above.
        let recent = chrono::Utc::now() - chrono::Duration::seconds(1);
        let mut claimable = Vec::new();
        for _ in 0..2 {
            let q = format!("cold-{}", uuid::Uuid::now_v7());
            claimable.push(seed_queued_job(&pool, &job_type, &q, recent, "pending").await?);
        }

        let pollable_types = vec![JobType::from_owned(job_type.clone())];
        let row_limits = vec![n_jobs_to_poll as i32];
        let clock = ClockHandle::realtime();

        let result = poll_jobs(
            &pool,
            n_jobs_to_poll,
            instance_id,
            &pollable_types,
            &row_limits,
            CONTENTION_HEADROOM,
            &clock,
        )
        .await?;

        match result {
            JobPollResult::Jobs { jobs, .. } => {
                let claimed: std::collections::HashSet<JobId> = jobs.iter().map(|j| j.id).collect();
                assert_eq!(
                    claimed.len(),
                    2,
                    "the blocked queue's backlog must not crowd out claimable queues"
                );
                for id in &claimable {
                    assert!(
                        claimed.contains(id),
                        "every unblocked queue head is claimed"
                    );
                }
            }
            other => panic!("expected a Jobs claim, got {other:?}"),
        }

        Ok(())
    }

    /// Invariant A, at the schema level: `idx_job_executions_queue_active`
    /// (`UNIQUE (queue_id) WHERE state IN ('pending','running') AND queue_id
    /// IS NOT NULL`) is the ONLY enforcement of queue exclusion. With at
    /// most one pending/running row per queue possible in the first place,
    /// there is no "which row is the head" question left for differently
    /// saturated instances to disagree about.
    ///
    /// Exercises the constraint directly at the SQL level (not through the
    /// application's own insert path, which is covered by `spawner`'s own
    /// tests): two concurrent raw inserts racing for one queue's active slot
    /// must leave exactly one `pending` row, with the loser's insert failing
    /// on this exact index.
    #[tokio::test]
    async fn queue_active_unique_index_enforces_exclusion() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("excl-{}", uuid::Uuid::now_v7());
        let queue = format!("excl-queue-{}", uuid::Uuid::now_v7());
        let now = chrono::Utc::now();

        let insert = |id: uuid::Uuid| {
            let pool = pool.clone();
            let job_type = job_type.clone();
            let queue = queue.clone();
            async move {
                sqlx::query(
                    "INSERT INTO jobs (id, job_type, queue_id, created_at) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(id)
                .bind(&job_type)
                .bind(&queue)
                .bind(now)
                .execute(&pool)
                .await?;
                sqlx::query(
                    "INSERT INTO job_executions \
                     (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
                     VALUES ($1, $2, $3, 'pending', 1, $4, $4, $4)",
                )
                .bind(id)
                .bind(&job_type)
                .bind(&queue)
                .bind(now)
                .execute(&pool)
                .await
            }
        };

        let (a, b) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let (ra, rb) = tokio::join!(insert(a), insert(b));

        let results = [ra, rb];
        let n_ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            n_ok, 1,
            "exactly one concurrent insert must win the queue's active slot"
        );
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("exactly one insert must fail");
        assert_eq!(
            err.as_database_error().and_then(|d| d.constraint()),
            Some("idx_job_executions_queue_active"),
            "the loser must fail specifically on the exclusion index"
        );

        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active, 1, "at most one active row per queue, ever");

        Ok(())
    }

    async fn row_state(pool: &PgPool, id: JobId) -> anyhow::Result<String> {
        let state: String =
            sqlx::query_scalar("SELECT state::text FROM job_executions WHERE id = $1")
                .bind(uuid::Uuid::from(id))
                .fetch_one(pool)
                .await?;
        Ok(state)
    }

    /// Seeds one orphaned queue (a `parked` head, no active sibling) with a
    /// caller-chosen `queue_id` and `id`, so a test can make plain `id` order
    /// and `(queue_id, id)` order disagree on purpose.
    async fn seed_orphan_with_id(
        pool: &PgPool,
        job_type: &str,
        queue_id: &str,
        id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO jobs (id, job_type, queue_id, created_at) VALUES ($1, $2, $3, NOW())",
        )
        .bind(id)
        .bind(job_type)
        .bind(queue_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, state, attempt_index, execute_at, alive_at, created_at) \
             VALUES ($1, $2, $3, 'parked', 1, NOW() - INTERVAL '600 seconds', NOW(), NOW())",
        )
        .bind(id)
        .bind(job_type)
        .bind(queue_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// `sweep_orphaned_parked_rows` is the widest multi-row WAITING locker of
    /// `job_executions` in the crate -- its row set is every orphaned queue in
    /// the table, unscoped by type or process -- so it owes the same
    /// `(queue_id, id)` lock-acquisition order as every other one
    /// (`lock_queue_occupants`, `PromoteHeadsHook::apply`/`apply_freed`, both
    /// completers' `to_delete`). It did not have it: the bare
    /// `UPDATE ... FROM heads` acquired in planner scan order, which is a
    /// deadlock (40P01) waiting for a plan change -- observed in CI as
    /// `orphan_sweeper_recovers_orphaned_parked_row` failing with "deadlock
    /// detected" once the rest of the table's writers were pinned to
    /// `(queue_id, id)`.
    ///
    /// Deterministic, not a timing race. Two orphaned queues are built so the
    /// two orderings DISAGREE: `qa` sorts before `qb` by `queue_id`, but `qa`'s
    /// head has the HIGHER `id`. A holder transaction then takes the row the
    /// correctly-ordered sweep must lock FIRST (`qa`'s), and only afterwards
    /// reaches for the one it must lock second (`qb`'s).
    ///
    /// Note what this can and cannot pin. Restoring the bare, UNORDERED
    /// `UPDATE ... FROM heads` does not reliably fail here: on a small local
    /// database the planner happens to feed `heads` in `queue_id` order
    /// anyway. That is exactly the defect -- the order was never guaranteed,
    /// only inherited from a plan -- and it is why this asserts the ORDER
    /// rather than the absence of a plan. Swapping the `locked` CTE's
    /// `ORDER BY` to bare `je.id` reproduces CI's 40P01 on every run.
    ///
    /// - Ordered (fixed): the sweep blocks on `qa` immediately, holding
    ///   nothing, so the holder takes `qb` and both finish.
    /// - Any order that takes `qb` before `qa` (`id` order, and whatever the
    ///   planner picks): the sweep holds `qb` and waits for `qa` while the
    ///   holder holds `qa` and waits for `qb` -- a cycle, and Postgres aborts
    ///   one side.
    #[tokio::test]
    async fn orphan_sweep_locks_heads_in_queue_id_order() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-lockorder-{}", uuid::Uuid::now_v7());

        // Shared prefix differs before either suffix, so `qa < qb` as strings
        // regardless of the UUIDs' own values.
        let qa = format!("orphan-lockorder-qa-{}", uuid::Uuid::now_v7());
        let qb = format!("orphan-lockorder-qb-{}", uuid::Uuid::now_v7());

        // ... but `qa`'s head sorts AFTER `qb`'s by bare `id`: v7 UUIDs are
        // time-ordered, so minting `qb`'s first is enough.
        let id_b = uuid::Uuid::now_v7();
        let id_a = uuid::Uuid::now_v7();
        assert!(qa < qb && id_b < id_a, "the two orderings must disagree");

        seed_orphan_with_id(&pool, &job_type, &qb, id_b).await?;
        seed_orphan_with_id(&pool, &job_type, &qa, id_a).await?;

        // Holder: takes `qa`'s head (what the ORDERED sweep locks first), then
        // reaches for `qb`'s (what it locks second).
        let holder_pool = pool.clone();
        let holder = tokio::spawn(async move {
            let mut tx = holder_pool.begin().await?;
            sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR NO KEY UPDATE")
                .bind(id_a)
                .fetch_one(&mut *tx)
                .await?;
            // Long enough that the sweep is reliably already blocked (ordered)
            // or already holding `qb` (unordered) before this second lock is
            // requested -- the sweep is a single fast statement either way.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR NO KEY UPDATE")
                .bind(id_b)
                .fetch_one(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(())
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let sweep = sweep_orphaned_parked_rows(&pool).await;

        // Neither side may be a deadlock victim. Assert on both: Postgres
        // picks whichever it likes.
        sweep.expect("orphan sweep must not deadlock against an ordered holder");
        holder
            .await?
            .expect("holder must not deadlock against the sweep");

        Ok(())
    }

    /// A parked row whose queue has no active row (hand-constructed here,
    /// rather than actually racing the insert-vs-complete window, which
    /// `ExecutionInsertHook` now closes at the source) must still be
    /// recovered by the backstop sweep piggybacked on the lost-handler
    /// cadence -- that is what covers a peer running a pre-lock build.
    #[tokio::test]
    async fn orphan_sweeper_recovers_orphaned_parked_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-{}", uuid::Uuid::now_v7());
        let queue = format!("orphan-queue-{}", uuid::Uuid::now_v7());
        let base = chrono::Utc::now() - chrono::Duration::seconds(600);

        // Parked, no active sibling for this queue -- an orphan.
        let orphan = seed_queued_job(&pool, &job_type, &queue, base, "parked").await?;

        // This call races every live `Jobs` instance's own lost-handler
        // background sweep elsewhere in the suite -- if one of those wins
        // first, this call's own return value can legitimately come back
        // without `orphan`, even though the row still ends up correctly
        // `pending`. Assert only on `row_state`.
        sweep_orphaned_parked_rows(&pool).await?;
        assert_eq!(row_state(&pool, orphan).await?, "pending");

        Ok(())
    }

    /// Among several parked rows orphaned in one queue, the sweep must
    /// promote the OLDEST by `(execute_at, id)` -- the same tiebreak the
    /// claim query and the completion-time promote CTE use, so every
    /// mechanism that ever resolves a queue's head agrees on which row it is.
    #[tokio::test]
    async fn orphan_sweeper_promotes_the_oldest_parked_sibling() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("orphan-multi-{}", uuid::Uuid::now_v7());
        let queue = format!("orphan-multi-queue-{}", uuid::Uuid::now_v7());
        let base = chrono::Utc::now() - chrono::Duration::seconds(600);

        let oldest = seed_queued_job(&pool, &job_type, &queue, base, "parked").await?;
        let _middle = seed_queued_job(
            &pool,
            &job_type,
            &queue,
            base + chrono::Duration::seconds(10),
            "parked",
        )
        .await?;
        let _youngest = seed_queued_job(
            &pool,
            &job_type,
            &queue,
            base + chrono::Duration::seconds(20),
            "parked",
        )
        .await?;

        sweep_orphaned_parked_rows(&pool).await?;

        assert_eq!(row_state(&pool, oldest).await?, "pending");
        let still_parked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state = 'parked'",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(
            still_parked, 2,
            "only the oldest sibling is promoted; the rest stay parked"
        );

        Ok(())
    }

    /// Invariant B on the reclaim path: a lost job's row keeps
    /// its queue's active slot on reclaim (it was already the sole `running`
    /// occupant), but an older parked sibling must run first during the
    /// reclaimed row's backoff.
    #[tokio::test]
    async fn reclaim_lets_an_older_parked_sibling_run_first() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let job_type = format!("reclaim-swap-{}", uuid::Uuid::now_v7());
        let queue = format!("reclaim-swap-queue-{}", uuid::Uuid::now_v7());
        let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(600);
        let older = stale_alive_at - chrono::Duration::seconds(60);

        // The lost job: running, alive_at stale enough to be reclaimed.
        let lost = seed_queued_job(&pool, &job_type, &queue, stale_alive_at, "running").await?;
        sqlx::query(
            "UPDATE job_executions SET poller_instance_id = $2, alive_at = $3 WHERE id = $1",
        )
        .bind(uuid::Uuid::from(lost))
        .bind(instance_id)
        .bind(stale_alive_at)
        .execute(&pool)
        .await?;
        // An older parked sibling -- must run before the reclaimed row's
        // retried attempt.
        let sibling = seed_queued_job(&pool, &job_type, &queue, older, "parked").await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(300);
        let reschedule_at = chrono::Utc::now();
        let (reclaimed, promoted) = reclaim_lost_jobs(
            &pool,
            instance_id,
            &[JobType::from_owned(job_type.clone())],
            threshold,
            reschedule_at,
            &[],
        )
        .await?;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, lost);
        assert_eq!(
            promoted,
            vec![job_type],
            "the reclaim must report the promoted sibling's type so its poller \
             can be woken -- even here, where it happens to match the reclaimed \
             row's own type"
        );

        assert_eq!(
            row_state(&pool, sibling).await?,
            "pending",
            "the older parked sibling must be promoted"
        );
        assert_eq!(
            row_state(&pool, lost).await?,
            "parked",
            "the reclaimed row must yield its slot to the older sibling"
        );

        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_executions WHERE queue_id = $1 AND state IN ('pending','running')",
        )
        .bind(&queue)
        .fetch_one(&pool)
        .await?;
        assert_eq!(active, 1, "Invariant A must still hold after the swap");

        Ok(())
    }

    /// A reclaim's own swap can
    /// promote an OLDER parked sibling of a DIFFERENT type than the
    /// reclaimed row's own type (one `queue_id` can be shared across
    /// types) -- `reclaim_lost_jobs` must report that type too, not just
    /// the reclaimed types, or the promoted sibling's poller is never
    /// woken.
    #[tokio::test]
    async fn reclaim_reports_a_promoted_sibling_of_a_different_type() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let instance_id = uuid::Uuid::now_v7();
        let lost_type = format!("reclaim-cross-lost-{}", uuid::Uuid::now_v7());
        let sibling_type = format!("reclaim-cross-sibling-{}", uuid::Uuid::now_v7());
        let queue = format!("reclaim-cross-queue-{}", uuid::Uuid::now_v7());
        let stale_alive_at = chrono::Utc::now() - chrono::Duration::seconds(600);
        let older = stale_alive_at - chrono::Duration::seconds(60);

        let lost = seed_queued_job(&pool, &lost_type, &queue, stale_alive_at, "running").await?;
        sqlx::query(
            "UPDATE job_executions SET poller_instance_id = $2, alive_at = $3 WHERE id = $1",
        )
        .bind(uuid::Uuid::from(lost))
        .bind(instance_id)
        .bind(stale_alive_at)
        .execute(&pool)
        .await?;
        // Same queue, a DIFFERENT type -- the reclaim only scans for
        // `lost_type`, but the swap must still find and promote this.
        let sibling = seed_queued_job(&pool, &sibling_type, &queue, older, "parked").await?;

        let threshold = chrono::Utc::now() - chrono::Duration::seconds(300);
        let reschedule_at = chrono::Utc::now();
        let (reclaimed, promoted) = reclaim_lost_jobs(
            &pool,
            instance_id,
            &[JobType::from_owned(lost_type)],
            threshold,
            reschedule_at,
            &[],
        )
        .await?;
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, lost);
        assert_eq!(
            promoted,
            vec![sibling_type],
            "the reclaim must report the promoted sibling's OWN type, distinct \
             from every reclaimed row's type, so its poller can be woken"
        );
        assert_eq!(row_state(&pool, sibling).await?, "pending");
        assert_eq!(row_state(&pool, lost).await?, "parked");

        Ok(())
    }

    /// Seed a `job_executions` row already `running` under `instance_id`,
    /// `execute_at` NULL exactly like a real claim leaves it -- what a claim
    /// that landed but whose commit then errored looks like to
    /// [`ClaimReconciler`]. The row's ORIGINAL `execute_at` (what
    /// `reconcile_unclaimed` should restore) is tracked only in the caller's
    /// test, mirroring how `ClaimedRow::execute_at` carries it in the real
    /// path -- the DB row itself no longer has it once claimed.
    async fn seed_landed_running_row(
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
             (id, job_type, state, attempt_index, execute_at, alive_at, poller_instance_id, created_at) \
             VALUES ($1, $2, 'running', 1, NULL, $3, $4, $3)",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type)
        .bind(now)
        .bind(instance_id)
        .execute(pool)
        .await?;
        Ok(id)
    }

    /// The landed case: a row genuinely committed `running` under this
    /// instance (mirrors a claim that landed but whose `COMMIT` then
    /// errored). `reconcile_unclaimed` must reset it to `pending`, restore
    /// its ORIGINAL `execute_at`, clear `poller_instance_id`, and leave
    /// `attempt_index` untouched -- it never ran, so this is an un-claim,
    /// not a retry.
    #[tokio::test]
    async fn reconciler_resets_a_row_that_actually_landed_running() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-landed-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        let original_execute_at = chrono::Utc::now() - chrono::Duration::seconds(30);

        let id = seed_landed_running_row(&pool, &repo, &job_type, instance_id).await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(id, original_execute_at, JobType::from_owned(job_type))],
        )
        .await?;
        assert_eq!(reset, vec![id]);
        assert!(promoted.is_empty(), "no parked sibling exists in this test");

        let row = sqlx::query!(
            r#"SELECT state::text AS "state!", poller_instance_id, attempt_index, execute_at
               FROM job_executions WHERE id = $1"#,
            uuid::Uuid::from(id),
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(row.state, "pending");
        assert!(row.poller_instance_id.is_none());
        assert_eq!(
            row.attempt_index, 1,
            "no attempt bump -- this row never ran"
        );
        assert_eq!(
            row.execute_at.map(|at| at.timestamp_millis()),
            Some(original_execute_at.timestamp_millis()),
            "must restore the row's original execute_at, not re-timestamp it to now"
        );

        Ok(())
    }

    /// The common case: the transaction genuinely rolled back, so the row
    /// this claim would have carried never landed at all.
    /// `reconcile_unclaimed` must report zero resets and touch nothing.
    #[tokio::test]
    async fn reconciler_is_a_noop_for_a_row_that_never_landed() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let job_type = format!("reconciler-never-landed-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        // No row inserted at all -- the transaction that would have claimed
        // it rolled back before ever committing anything.
        let phantom_id = JobId::new();

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(
                phantom_id,
                chrono::Utc::now(),
                JobType::from_owned(job_type),
            )],
        )
        .await?;
        assert!(reset.is_empty());
        assert!(promoted.is_empty());

        Ok(())
    }

    /// The guard case: a row `running` under a DIFFERENT instance (already
    /// reclaimed by a peer, or simply never ours) must be left alone --
    /// `reconcile_unclaimed` only touches rows still `running` under ITS
    /// OWN `instance_id`.
    #[tokio::test]
    async fn reconciler_does_not_touch_a_different_instances_row() -> anyhow::Result<()> {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-guard-{}", uuid::Uuid::now_v7());
        let owner_instance = uuid::Uuid::now_v7();
        let our_instance = uuid::Uuid::now_v7();
        let original_execute_at = chrono::Utc::now() - chrono::Duration::seconds(10);

        let id = seed_landed_running_row(&pool, &repo, &job_type, owner_instance).await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            our_instance,
            &[(id, original_execute_at, JobType::from_owned(job_type))],
        )
        .await?;
        assert!(
            reset.is_empty(),
            "must not reset a row owned by a different instance"
        );
        assert!(promoted.is_empty());
        assert_eq!(row_state(&pool, id).await?, "running");

        Ok(())
    }

    /// A reset row is not automatically
    /// its queue's rightful head again. While it sat `running`, a backdated
    /// sibling could have landed `parked` in the same queue (a `running`
    /// occupant blocks a swap at insert time exactly like a `pending` one
    /// that sorts after it). `reconcile_unclaimed` must re-check and swap,
    /// or the younger row squats on the slot until it runs to completion --
    /// `reclaim_lost_jobs` never revisits it, since it is `pending` with a
    /// fresh `alive_at`, not stale `running`.
    #[tokio::test]
    async fn reconciler_swaps_an_older_parked_sibling_ahead_of_the_reset_row() -> anyhow::Result<()>
    {
        let pool = init_pool().await?;
        let repo = JobRepo::new(&pool);
        let job_type = format!("reconciler-swap-{}", uuid::Uuid::now_v7());
        let instance_id = uuid::Uuid::now_v7();
        let queue = format!("reconciler-swap-queue-{}", uuid::Uuid::now_v7());
        let younger_execute_at = chrono::Utc::now() - chrono::Duration::seconds(5);
        let older_execute_at = chrono::Utc::now() - chrono::Duration::seconds(60);

        // The row this reconciler call is about to reset -- it was `running`
        // (claimed) with `younger_execute_at` before it was claimed.
        let running_id = seed_landed_running_row(&pool, &repo, &job_type, instance_id).await?;
        sqlx::query("UPDATE job_executions SET queue_id = $2 WHERE id = $1")
            .bind(uuid::Uuid::from(running_id))
            .bind(&queue)
            .execute(&pool)
            .await?;

        // An OLDER sibling, already sitting `parked` in the same queue --
        // it landed there because the queue's slot was occupied by the
        // `running` row above at insert time.
        let older_sibling =
            seed_queued_job(&pool, &job_type, &queue, older_execute_at, "parked").await?;

        let (reset, promoted) = ClaimReconciler::reconcile_unclaimed(
            &pool,
            instance_id,
            &[(
                running_id,
                younger_execute_at,
                JobType::from_owned(job_type),
            )],
        )
        .await?;
        assert_eq!(reset, vec![running_id]);
        assert_eq!(
            promoted.len(),
            1,
            "the older sibling must be promoted in the SAME call"
        );

        assert_eq!(
            row_state(&pool, older_sibling).await?,
            "pending",
            "the older sibling must now hold the queue's active slot"
        );
        assert_eq!(
            row_state(&pool, running_id).await?,
            "parked",
            "the reset row must yield to the genuinely older sibling"
        );

        Ok(())
    }
}
