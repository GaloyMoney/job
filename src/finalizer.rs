//! End-of-job finalization: the one place a claimed row's disposition is
//! written, shared by both dispatchers. A batch is just N ids and a single
//! job is N = 1 -- classification, the ordered row writes, the entity
//! events, the promote/notify hooks, the pool choice, and the abort-retry
//! loop are identical, so they live here once, as [`Finalizer`].
//!
//! Every disposition ([`Disposition`]) flows through the same five-phase
//! write ([`Finalizer::finalize_in_op`]): load entities -> decide + push
//! events -> `(queue_id, id)`-ordered row writes -> hook registrations ->
//! entity updates. What varies per disposition is only the `SET` list and
//! the entity event:
//!
//! - [`Disposition::Complete`]: delete the execution row (plus its
//!   `job_execution_states` row unless the type retains state), promote the
//!   freed queue's oldest parked sibling, emit the terminal notification,
//!   push the completion event -- the event only when this instance
//!   actually deleted the row, so a row already dispositioned elsewhere is
//!   never double-completed.
//! - [`Disposition::Fail`]: run the type's `RetryPolicy`
//!   (`Job::maybe_schedule_retry`) -- a retry goes back to `pending` at the
//!   policy's backoff with the NEXT `attempt_index`; exhausted attempts
//!   delete the row like a completion (terminal notification included) but
//!   with the error recorded on the entity.
//! - [`Disposition::Fresh`]: back to `pending` at the caller's time with
//!   `attempt_index = 1` -- a runner-requested reschedule (which has no
//!   notion of "which attempt") or a rescue's "we don't know what happened,
//!   start fresh" last resort.
//! - [`Disposition::Congestion`]: back to `pending` with `attempt_index`
//!   UNTOUCHED, on a `CongestionRescheduled` event -- see "Why congestion
//!   is its own path" below.
//!
//! # Pool choice
//!
//! Disposition writes in a dispatcher-owned transaction
//! ([`Finalizer::finalize`]) pick their pool per attempt: the FIRST attempt
//! uses the shared pool when it has live headroom -- keeping the small
//! internal pool unloaded in the healthy case -- and any failure of that
//! shared attempt (not just retryable aborts: a `PoolTimedOut` must fall
//! back too) switches every further attempt to `JobPoller::internal_pool`,
//! the dedicated pool the claim query uses. A shared pool with zero
//! headroom is skipped outright rather than burning its ~30s acquire
//! timeout on a doomed acquire: these are the LAST writes deciding a job's
//! disposition, and they often run precisely because the shared pool is the
//! thing under pressure. Only the OP carries the connection -- callers (and
//! this module) keep using their ordinary repos' `_in_op` methods against
//! it.
//!
//! # Why congestion is its own path, not a retry
//!
//! - **`attempt_index` stays UNCHANGED**, in the row and in the entity
//!   event. Congestion carries no evidence the JOB is broken -- the pool
//!   could not hand out a connection, a pool-wide condition -- so it must
//!   not spend a `RetryPolicy` attempt and walk the job toward
//!   `max_attempts`. That's load-bearing beyond the retry budget: the
//!   poller's retry-solo rule (`poller.rs`, `attempt > 1` is dispatched
//!   alone, never batched) reads the same column, so resetting or bumping
//!   it here would silently change how the job is dispatched on its next
//!   claim. See `congestion_reschedule_keeps_job_batchable` in
//!   `tests/batched_job.rs`.
//! - **Fixed short delay +/- jitter**, not `RetryPolicy`'s exponential
//!   schedule: the pool that just timed out needs a moment to drain, and
//!   the jitter keeps every job congested in the same poll from
//!   synchronizing on the exact same next claim instant.
//! - **A `CongestionRescheduled` entity event**, not `ExecutionErrored`
//!   (see [`Job::reschedule_congestion`]), which is also how the
//!   consecutive-congestion streak is counted for the stuck-forever WARN.

use chrono::{DateTime, Utc};
use es_entity::AtomicOperation;
use es_entity::clock::ClockHandle;
use es_entity::errlanes;
use es_entity::errlanes::Fault;
use rand::{RngExt, rng};
use tracing::{Span, instrument};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use super::{
    JobId,
    entity::{Job, JobType, RetryPolicy},
    error::{JobFault, TX_ABORT_MAX_ATTEMPTS},
    execution_hooks::PromoteHeadsHook,
    notifier::JobEventNotifier,
    poller::{JobPoller, pool_connection_headroom},
    repo::JobRepo,
    runner::RetrySettings,
};

/// A runner's failure, classified at the job boundary.
///
/// The runner traits return a plain `Box<dyn Error>` (not `Send + Sync` --
/// see the es-entity addendum). `Fault::classify` borrows it, so the box
/// is classified before the next `.await` and dropped there; the `Fault`
/// is `Send + Sync` by construction. Job is not an authorization boundary,
/// so a `Denied` found in the chain is narrowed to `Fatal(Denied)` here:
/// nobody is on the other end of a job to be told no.
///
/// So: classify at the boundary, before the next `.await`, and let the box
/// drop there. See `dispatcher.rs::dispatch_job` and
/// `batch_dispatcher.rs::dispatch_batch`.
///
/// Classification never comes back empty. `Fault::classify` falls back to
/// `Fatal(Dependency)` carrying the error's whole `Display` chain as
/// context, so a runner that knows nothing of errlanes still arrives as
/// something an operator can page on -- see `RunFailure::is_fatal` (via
/// `Laned`/the inherent method) for why that does not, by itself, end the
/// job.
///
/// **Job does not act on `is_fatal` by default.**
/// [`RetrySettings::terminal_on_fatal`] defaults to `false`, so a `Fatal`
/// runner error (including a narrowed `Denied`) is retried on the ordinary
/// attempt-count policy exactly like a `Transient` one -- while still being
/// *reported* as fatal on the span (`error.lane`, `error.code`,
/// `exception.message`, written by `Laned::record` the moment it is
/// classified).
///
/// The split is deliberate: observability should reflect what the runner
/// said immediately, but acting on it ends a job after one attempt, and we
/// have no live experience yet with how faithfully the crates upstream of
/// job lane their errors. A `Fatal` that is really transient would turn a
/// blip into a dead job. Until that confidence exists, trusting the lane
/// that far is opt-in per job type.
pub(crate) type RunFailure = JobFault;

#[cfg(test)]
mod run_failure_tests {
    use super::*;
    use es_entity::errlanes::{Denied, Fatal, FatalKind, Lane, Transient, TransientKind};
    use std::error::Error;

    #[derive(Debug)]
    struct Wrapped<E>(E);
    impl<E: std::fmt::Display> std::fmt::Display for Wrapped<E> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped: {}", self.0)
        }
    }
    impl<E: Error + 'static> Error for Wrapped<E> {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }

    /// Boxes as the runner traits do -- `Box<dyn Error>`, with no
    /// `Send + Sync`. If this ever needs those bounds to compile, the
    /// boundary has regressed back to the widened trait signature.
    fn boxed<E: Error + 'static>(e: E) -> Box<dyn Error> {
        Box::new(e)
    }

    /// A runner that knows nothing of errlanes gets `Fault::classify`'s
    /// `Fatal(Dependency)` safety default rather than vanishing -- but it
    /// is NOT terminal on its own: `terminal_on_fatal` is `false` by
    /// default, so it retries per policy. See
    /// `tests/lanes.rs::unclassified_string_error_retries_per_policy`.
    #[test]
    fn classifies_a_bare_string_as_fatal_dependency_carrying_its_message() {
        let e = boxed(std::io::Error::other("boom"));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        match failure {
            Fault::Fatal(f) => {
                assert_eq!(f.kind, FatalKind::Dependency);
                assert_eq!(f.context.as_deref(), Some("boom"));
            }
            _ => panic!("expected Fatal(Dependency)"),
        }
    }

    /// A runner not built on errlanes at all returns a raw, never-laned
    /// `sqlx::Error::PoolTimedOut` directly, and it must still reach the
    /// congestion path rather than spend an ordinary retry attempt: a walk
    /// that looked only for an already-laned payload would drop it.
    /// `Fault::classify`'s second rule covers the shape, using errlanes' own
    /// sqlx table -- job keeps no second copy. See
    /// `tests/pool_congestion.rs::congestion_reschedule_keeps_job_batchable`
    /// for the end-to-end sibling.
    #[test]
    fn classify_detects_congestion_from_a_raw_unlaned_sqlx_pool_timed_out() {
        let e = boxed(sqlx::Error::PoolTimedOut);
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        assert!(failure.is_congestion());
    }

    /// The same raw-chain walk classifies the rest of the sqlx table too,
    /// not just `PoolTimedOut` -- the general form, not only the one kind
    /// the regression above pins.
    #[test]
    fn classify_reads_the_rest_of_the_raw_sqlx_table() {
        let connection_lost = boxed(sqlx::Error::Io(std::io::Error::other("conn reset")));
        let failure: RunFailure = Fault::classify(&*connection_lost).narrow_denied();
        match failure {
            Fault::Transient(t) => assert_eq!(t.kind, TransientKind::ConnectionLost),
            _ => panic!("expected Transient(ConnectionLost)"),
        }

        let protocol = boxed(sqlx::Error::Protocol("synthesized".into()));
        let failure: RunFailure = Fault::classify(&*protocol).narrow_denied();
        match failure {
            Fault::Fatal(f) => assert_eq!(f.kind, FatalKind::Dependency),
            _ => panic!("expected Fatal(Dependency)"),
        }
    }

    /// A laned payload elsewhere in the chain wins over an incidental raw
    /// `sqlx::Error` deeper in the same chain -- the errlanes-based
    /// classification takes precedence, not merely whichever happens to sit
    /// nearer the top.
    #[test]
    fn a_laned_fatal_outranks_a_raw_sqlx_error_in_its_own_source_chain() {
        // `Fatal::from_error` attaches the sqlx error as this Fatal's own
        // source -- a raw `sqlx::Error::PoolTimedOut` genuinely further
        // down the same chain, which must not flip the classification to
        // congestion once the laned walk has already found the Fatal.
        let e = boxed(Fatal::from_error(
            FatalKind::Invariant,
            sqlx::Error::PoolTimedOut,
        ));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        match failure {
            Fault::Fatal(f) => assert_eq!(f.kind, FatalKind::Invariant),
            _ => panic!("expected the laned Fatal to win over the raw sqlx::Error in its source"),
        }
    }

    #[test]
    fn classifies_a_transient_three_hops_deep() {
        let e = boxed(Wrapped(Wrapped(Transient::new(TransientKind::Deadlock))));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        match failure {
            Fault::Transient(t) => assert_eq!(t.kind, TransientKind::Deadlock),
            _ => panic!("expected Transient, got a different classification"),
        }
    }

    #[test]
    fn classifies_a_fatal() {
        let e = boxed(Fatal::new(FatalKind::CorruptState));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        match failure {
            Fault::Fatal(f) => assert_eq!(f.kind, FatalKind::CorruptState),
            _ => panic!("expected Fatal"),
        }
    }

    /// A `Denied` anywhere in the chain is narrowed at the job boundary:
    /// job is not an authorization boundary, so there is nobody left to
    /// tell no. It becomes `Fatal(Denied)`, with the `Denied` as its
    /// source.
    #[test]
    fn classifies_a_denied_narrowed_to_fatal_denied() {
        let e = boxed(Denied::default());
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        match failure {
            Fault::Fatal(f) => {
                assert_eq!(f.kind, FatalKind::Denied);
                assert!(f.source().unwrap().downcast_ref::<Denied>().is_some());
            }
            _ => panic!("expected Fatal(Denied)"),
        }
    }

    /// A boxed `Fail::Rejected(_)` carries no marker type that survives
    /// erasure, so there is nothing for the lane walk to find. It must land
    /// on the `Fatal(Dependency)` default -- reported, retried per policy,
    /// and above all never silently read as congestion (which would skip
    /// the attempt counter) or as a `Denied`.
    #[test]
    fn a_boxed_rejected_fail_falls_back_to_the_default_not_a_guessed_lane() {
        use crate::error::{AwaitError, AwaitTimeout};
        let fail: AwaitError = AwaitError::Rejected(AwaitTimeout {
            pending: vec![crate::JobId::new()],
        });
        let e: Box<dyn Error> = Box::new(fail);
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        assert!(matches!(failure, Fault::Fatal(_)));
        assert!(!failure.is_congestion());
    }

    /// An already-laned `Transient::new(TransientKind::PoolTimeout)` with no
    /// `sqlx::Error` source attached -- exactly what a runner built on the
    /// errlanes boundary returns -- takes the congestion path. Matching only
    /// a raw `sqlx::Error::PoolTimedOut` by downcasting the chain would miss
    /// it, and every such pool timeout would spend a `RetryPolicy` attempt
    /// instead.
    #[test]
    fn classify_detects_congestion_from_a_bare_laned_transient_with_no_sqlx_source() {
        let e = boxed(Transient::new(TransientKind::PoolTimeout));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        assert!(failure.is_congestion());
    }

    /// The contract this whole boundary exists to keep: a runner error that
    /// is NOT `Send` (the runner traits do not require it) classifies by
    /// reference, and the resulting `Fault` crosses a thread boundary --
    /// which is what the dispatcher does with it, across the `.await` that
    /// writes the disposition. If this stops compiling, the `Send + Sync`
    /// bound has crept back onto the runner traits.
    #[test]
    fn a_non_send_runner_error_classifies_and_its_fault_crosses_a_spawn() {
        #[derive(Debug)]
        struct NotSend(#[allow(dead_code)] std::rc::Rc<()>);
        impl std::fmt::Display for NotSend {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "not send")
            }
        }
        impl Error for NotSend {}

        let e: Box<dyn Error> = Box::new(NotSend(std::rc::Rc::new(())));
        let failure: RunFailure = Fault::classify(&*e).narrow_denied();
        drop(e);
        let lane = std::thread::spawn(move || failure.lane()).join().unwrap();
        assert_eq!(lane, Lane::Fatal);
    }
}

/// Base delay before a pool-congestion reschedule becomes due again. Fixed
/// and short, not the type's exponential `RetryPolicy` schedule: congestion
/// is a pool-wide condition expected to clear on a query-duration timescale,
/// not a per-job failure that compounds.
const CONGESTION_DELAY_MS: i64 = 2_000;

/// +/- jitter applied to `CONGESTION_DELAY_MS`, so every job congestion hit
/// in the same poll doesn't come due on the exact same next claim instant.
const CONGESTION_JITTER_MS: i64 = 1_000;

/// A job whose consecutive congestion-reschedule streak exceeds this gets a
/// WARN: rescheduling stays non-punitive (this is a signal, not a cap), but
/// "stuck in congestion forever" is no longer invisible. Counted from the
/// event stream by [`Job::consecutive_congestion_reschedules`].
const CONGESTION_WARN_STREAK: u32 = 10;

/// Deadline on ACQUIRING the shared-pool connection for
/// [`Finalizer::finalize`]'s first attempt -- deliberately much shorter
/// than any plausible pool `acquire_timeout` (sqlx default 30s) and
/// independent of pool config: a disposition write is the last write
/// deciding a job's fate, and if the shared pool cannot hand out a
/// connection within a second, the internal pool exists precisely to take
/// over. It covers only the acquire ([`Finalizer::begin_op`]): once a
/// connection is held the writes no longer compete for pool capacity, so
/// they run undeadlined -- and cancelling mid-`COMMIT` would be AMBIGUOUS
/// (the server may have committed first), so the commit must never sit
/// under a timeout at all (see `finalize`'s commit handling). Enforced with
/// `tokio::time::timeout`, NOT the injected [`ClockHandle`]: this deadline
/// exists to bound real waiting on a real pool, and a manual test clock
/// that never advances must not be able to hold it open forever.
/// Applies only to the shared attempt; internal-pool acquires run uncapped
/// (that pool is dedicated and its statements are short).
const SHARED_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// What should become of one claimed row. See the module doc for the exact
/// row write, entity event, and hook each variant produces.
#[derive(Clone)]
pub(crate) enum Disposition {
    /// The job ran to completion: execution row deleted, completion event.
    Complete,
    /// The job failed with `failure` on its `attempt`-th attempt: the
    /// type's `RetryPolicy` decides between a backoff retry (next
    /// `attempt_index`) and terminal deletion.
    ///
    /// `run_duration` is how long the failing execution actually ran, on a
    /// monotonic clock. The retry policy forgives the accumulated attempt
    /// count when it clears `attempt_reset_after_healthy_run` -- a run that
    /// stayed up that long is evidence the job had recovered, whether or not
    /// it ever returned a completion.
    Fail {
        /// The classified failure. The entity reads its lane for the
        /// `terminal_on_fatal` decision and narrows it on exhaustion.
        failure: RunFailure,
        attempt: u32,
        run_duration: std::time::Duration,
    },
    /// Back to `pending` at `at` with `attempt_index = 1`: a
    /// runner-requested reschedule or a rescue.
    Fresh { at: DateTime<Utc> },
    /// Back to `pending` at `at` with `attempt_index` untouched, on a
    /// `CongestionRescheduled` event carrying `message`. Built by
    /// [`Finalizer::reschedule_congested`], not by dispatchers directly.
    Congestion {
        at: DateTime<Utc>,
        attempt: u32,
        message: String,
    },
}

/// What [`Finalizer::finalize_in_op`] actually did, for the caller's span
/// fields and flag updates. `retried` carries each retry's NEXT
/// `attempt_index` (for warn-threshold escalation); `errored_terminal`
/// counts `Fail` DECISIONS that went terminal (whether or not the row was
/// still this instance's to delete), mirroring the batch's `n_errored`
/// accounting; `completed` counts only rows this instance actually deleted.
#[derive(Default)]
pub(crate) struct FinalizeOutcome {
    pub(crate) completed: Vec<JobId>,
    pub(crate) retried: Vec<(JobId, u32)>,
    pub(crate) errored_terminal: Vec<JobId>,
    /// Highest post-reschedule consecutive-congestion streak across the
    /// items, for [`Finalizer::reschedule_congested`]'s stuck-forever WARN.
    pub(crate) congestion_streak: u32,
    /// Whether any `Fresh`/`Congestion` item went back to `pending` (the
    /// `Fail`-retry case is visible via `retried`).
    rescheduled_pending: bool,
}

impl FinalizeOutcome {
    /// Whether any row went back to `pending` (retry, fresh, or congestion)
    /// -- what the dispatchers' `rescheduled` flag tracks.
    pub(crate) fn any_rescheduled(&self) -> bool {
        !self.retried.is_empty() || self.rescheduled_pending
    }
}

/// What happened to claimed rows after a dispatcher failed terminally.
/// Reported on the dispatchers' error logs so an operator can tell a
/// self-healing blip from a genuine stall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimDisposition {
    /// Rows were handed back as `pending` with `execute_at = now`; the next
    /// poll re-dispatches them.
    Rescheduled,
    /// The dispatcher had already dispositioned its rows -- nothing was
    /// left claimed.
    AlreadyDisposed,
    /// The rescue itself failed. Rows stay `running` under this instance
    /// and only the lost-handler will recover them, one `job_lost_interval`
    /// later.
    Leaked,
}

impl std::fmt::Display for ClaimDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ClaimDisposition::Rescheduled => "rescheduled",
            ClaimDisposition::AlreadyDisposed => "already-disposed",
            ClaimDisposition::Leaked => "leaked",
        };
        f.write_str(s)
    }
}

/// The stateful home of end-of-job finalization: built once per dispatcher
/// from state it already holds, so the call sites only pass what varies per
/// job end (ids and dispositions). Cheap to clone -- every field is a
/// handle (`Weak`, `Arc`, pool-handle clones) or small copy.
#[derive(Clone)]
pub(crate) struct Finalizer {
    /// Reaches this process's poller for the internal pool. `Weak` for the
    /// same reason the dispatchers hold their poller `Weak`: a dispatcher
    /// must never keep the poller alive on its own.
    poller: Weak<JobPoller>,
    /// The shared-pool repo: first-attempt pool (see the module doc's "Pool
    /// choice"), shutdown fallback, and the repo instance every `_in_op`
    /// entity call goes through (the op carries the connection, so which
    /// repo instance is irrelevant there).
    repo: Arc<JobRepo>,
    waiters: crate::waiters::JobWaiters,
    notifier: Arc<JobEventNotifier>,
    retry_settings: RetrySettings,
    /// Whether this type keeps its `job_execution_states` row past terminal
    /// (keyed with `inherits_state`). Always `false` for batched types,
    /// which are never keyed.
    retains_state: bool,
    instance_id: uuid::Uuid,
    clock: ClockHandle,
}

impl Finalizer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        poller: Weak<JobPoller>,
        repo: Arc<JobRepo>,
        notifier: Arc<JobEventNotifier>,
        retry_settings: RetrySettings,
        retains_state: bool,
        instance_id: uuid::Uuid,
        clock: ClockHandle,
    ) -> Self {
        // Derived from the repo's pool rather than threaded down, the same
        // way the poller derives its own (`poller::JobPoller::new`): a
        // finalizer is built deep inside the poller, and its waiters must be
        // on the same pool as its repo by construction.
        let waiters = crate::waiters::JobWaiters::new(repo.pool());
        Self {
            poller,
            repo,
            waiters,
            notifier,
            retry_settings,
            retains_state,
            instance_id,
            clock,
        }
    }

    /// Reschedule `ids` after a congestion classification: every row
    /// goes back to `pending` at now + [`CONGESTION_DELAY_MS`] +/-
    /// [`CONGESTION_JITTER_MS`], `attempt_index` untouched, on a fresh
    /// `CongestionRescheduled` entity event -- one [`Disposition::Congestion`]
    /// per id through the ordinary [`Self::finalize`] machinery.
    ///
    /// `attempts` maps each id to its in-flight attempt number, recorded
    /// unchanged on the entity's next `ExecutionScheduled` event; an id
    /// missing from the map defaults to attempt 1.
    #[errlanes::instrument(name = "job.congestion_reschedule", skip_all,
        fields(n_jobs = ids.len(), congestion_streak)
    )]
    pub(crate) async fn reschedule_congested(
        &self,
        ids: &[JobId],
        attempts: &HashMap<JobId, u32>,
        message: String,
    ) -> Result<(), JobFault> {
        let jitter_ms = rng().random_range(-CONGESTION_JITTER_MS..=CONGESTION_JITTER_MS);
        let at = self.clock.now() + chrono::Duration::milliseconds(CONGESTION_DELAY_MS + jitter_ms);
        let items: Vec<(JobId, Disposition)> = ids
            .iter()
            .map(|id| {
                (
                    *id,
                    Disposition::Congestion {
                        at,
                        attempt: attempts.get(id).copied().unwrap_or(1),
                        message: message.clone(),
                    },
                )
            })
            .collect();
        let outcome = self.finalize(&items, |_, _| {}).await?;
        let streak = outcome.congestion_streak;
        Span::current().record("congestion_streak", streak);
        if streak > CONGESTION_WARN_STREAK {
            tracing::warn!(
                job_ids = %Self::display_ids(ids),
                streak,
                "stuck in congestion-reschedule; the pool may not be recovering"
            );
        }
        Ok(())
    }

    /// [`Self::reschedule_congested`] for a single job -- a batch of one.
    pub(crate) async fn reschedule_congested_one(
        &self,
        id: JobId,
        attempt: u32,
        message: String,
    ) -> Result<(), JobFault> {
        let attempts = HashMap::from([(id, attempt)]);
        self.reschedule_congested(&[id], &attempts, message).await
    }

    /// Run `items` through [`Self::finalize_in_op`] in a transaction this
    /// finalizer owns, with the pool choice and abort-retry policy from the
    /// module doc: first attempt on the shared pool when it has headroom,
    /// any first-attempt failure there switches to the internal pool, and
    /// internal-pool attempts retry transient aborts
    /// (transient aborts) up to [`TX_ABORT_MAX_ATTEMPTS`].
    /// Retrying is sound because the transaction is the finalizer's own:
    /// it holds nothing but this job end's bookkeeping, an abort rolled all
    /// of it back, and `items` is plain data that re-applies identically.
    ///
    /// `after_write` runs after the disposition writes, before commit, once
    /// per attempt -- the dispatchers hang their completion-recycle
    /// registration here, which must land in the SAME transaction as the
    /// row writes: `BatchDispatcher::seal_in_own_op` /
    /// `rescue_claimed_rows` pass `try_recycle_own_type`, and
    /// `JobDispatcher::fail_job` passes `recycle_into_claim` for the
    /// exhausted-retries terminal delete (each guarded exactly-once on
    /// their side, since a rolled-back attempt's dropped reservation
    /// already released the unit).
    pub(crate) async fn finalize(
        &self,
        items: &[(JobId, Disposition)],
        mut after_write: impl FnMut(&mut es_entity::DbOp<'static>, &FinalizeOutcome),
    ) -> Result<FinalizeOutcome, JobFault> {
        let mut attempt_no = 1;
        let mut use_internal = !self.shared_pool_has_headroom();
        loop {
            // Phase 1a -- acquire. Only the shared acquire sits under the
            // hard [`SHARED_ATTEMPT_TIMEOUT`] deadline (wall-clock, see its
            // doc): if the shared pool can hand out a connection quickly we
            // use it, otherwise we go internal without burning the pool's
            // own ~30s acquire timeout.
            let acquired = if use_internal {
                self.begin_op(true).await
            } else {
                match tokio::time::timeout(SHARED_ATTEMPT_TIMEOUT, self.begin_op(false)).await {
                    Ok(acquired) => acquired,
                    Err(_elapsed) => {
                        tracing::warn!(
                            job_ids = %Self::display_ids_of(items),
                            "shared-pool acquire for the disposition write \
                             exceeded 1s; retrying on the internal pool"
                        );
                        use_internal = true;
                        continue;
                    }
                }
            };
            // Phase 1b -- the writes, NOT the commit. Undeadlined: the held
            // connection no longer competes for pool capacity. Everything
            // here is unambiguous on failure: nothing has committed, so
            // re-running on another pool re-applies plain data.
            let prepared = match acquired {
                Ok(mut op) => {
                    let written = match Self::pin_index_plans(&mut op, use_internal).await {
                        Ok(()) => self.finalize_in_op(&mut op, items).await,
                        Err(e) => Err(e.into()),
                    };
                    match written {
                        Ok(outcome) => {
                            after_write(&mut op, &outcome);
                            Ok((op, outcome))
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e.into()),
            };
            let (op, outcome) = match prepared {
                Ok(prepared) => prepared,
                // Any pre-commit failure of a shared-pool attempt -- not
                // just a retryable abort: a `PoolTimedOut` there is
                // precisely the case the internal pool exists for -- falls
                // back to the internal pool without spending an abort-retry
                // attempt.
                Err(e) if !use_internal => {
                    tracing::warn!(
                        job_ids = %Self::display_ids_of(items),
                        exception.message = %e,
                        "disposition write failed on the shared pool; \
                         retrying on the internal pool"
                    );
                    use_internal = true;
                    continue;
                }
                Err(e) if attempt_no < TX_ABORT_MAX_ATTEMPTS && e.is_transient() => {
                    tracing::warn!(
                        job_ids = %Self::display_ids_of(items),
                        attempt_no,
                        exception.message = %e,
                        "disposition write lost a lock conflict; retrying"
                    );
                    attempt_no += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Phase 2 -- the commit, uncapped and retried ONLY on a
            // server-reported abort (deadlock
            // victim / serialization failure), which guarantees the
            // transaction rolled back. Every other commit error is
            // AMBIGUOUS -- the server may have committed before the
            // connection died -- and re-running an ambiguous attempt could
            // double-apply the dispositions' entity events, so it
            // propagates instead (the row writes' `poller_instance_id`
            // filter plus `finalize_in_op`'s applied-row gating make any
            // later rescue of an actually-committed attempt a no-op).
            match op.commit().await {
                Ok(()) => return Ok(outcome),
                Err(e)
                    if attempt_no < TX_ABORT_MAX_ATTEMPTS
                        && Fault::classify(&e).is_contention() =>
                {
                    tracing::warn!(
                        job_ids = %Self::display_ids_of(items),
                        attempt_no,
                        exception.message = %e,
                        "disposition commit lost a lock conflict; retrying"
                    );
                    use_internal = true;
                    attempt_no += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// One pass of the five-phase disposition write, on the caller's `op`
    /// (a runner's own transaction on the seal path, or [`Self::finalize`]'s
    /// owned one): load entities -> decide + push events -> ordered row
    /// writes -> hook registrations -> entity updates. Every row write
    /// filters on `poller_instance_id`, so rows another instance has since
    /// taken over match nothing; every multi-row write takes its
    /// `(queue_id, id)`-ordered `MATERIALIZED` lock first, the crate-wide
    /// deadlock-avoidance order (see `PromoteHeadsHook`).
    pub(crate) async fn finalize_in_op(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
        items: &[(JobId, Disposition)],
    ) -> Result<FinalizeOutcome, JobFault> {
        let mut outcome = FinalizeOutcome::default();
        if items.is_empty() {
            return Ok(outcome);
        }
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let retry_policy = RetryPolicy::from(&self.retry_settings);

        let ids: Vec<JobId> = items.iter().map(|(id, _)| *id).collect();
        let mut entities: HashMap<JobId, Job> = if let [id] = ids.as_slice() {
            let job = self.repo.find_by_id_in_op(&mut *op, id).await?;
            HashMap::from([(*id, job)])
        } else {
            self.repo.find_all_in_op::<Job>(&mut *op, &ids).await?
        };

        // Decision phase: push events on the IN-MEMORY entities and bucket
        // the row transitions. Nothing staged here persists on its own --
        // an entity's events only reach `update_all_in_op` below if this
        // instance's row write actually applied (the "applied-row gating"
        // below), which is what makes re-running this whole pass over an
        // already-committed attempt (an ambiguous commit's rescue, a row
        // another instance took over) a no-op instead of a double-write.
        let mut staged: HashMap<JobId, Job> = HashMap::new();
        let mut own_types: HashSet<JobType> = HashSet::new();

        let mut fresh_uuids: Vec<uuid::Uuid> = Vec::new();
        let mut fresh_times: Vec<DateTime<Utc>> = Vec::new();
        let mut congestion_uuids: Vec<uuid::Uuid> = Vec::new();
        let mut congestion_times: Vec<DateTime<Utc>> = Vec::new();
        let mut congestion_streaks: HashMap<JobId, u32> = HashMap::new();
        let mut retry_uuids: Vec<uuid::Uuid> = Vec::new();
        let mut retry_times: Vec<DateTime<Utc>> = Vec::new();
        let mut retry_attempts: Vec<i32> = Vec::new();
        let mut retry_next: HashMap<JobId, u32> = HashMap::new();
        let mut delete_uuids: Vec<uuid::Uuid> = Vec::new();
        let mut complete_ids: HashSet<JobId> = HashSet::new();
        let mut fail_terminal_ids: HashSet<JobId> = HashSet::new();

        for (id, disposition) in items {
            let Some(mut job) = entities.remove(id) else {
                continue;
            };
            own_types.insert(job.job_type.clone());
            match disposition {
                Disposition::Complete => {
                    delete_uuids.push(uuid::Uuid::from(*id));
                    complete_ids.insert(*id);
                }
                Disposition::Fresh { at } => {
                    fresh_uuids.push(uuid::Uuid::from(*id));
                    fresh_times.push(*at);
                    job.reschedule_execution(*at);
                }
                Disposition::Congestion {
                    at,
                    attempt,
                    message,
                } => {
                    congestion_uuids.push(uuid::Uuid::from(*id));
                    congestion_times.push(*at);
                    let streak = job.reschedule_congestion(message.clone(), *at, *attempt);
                    congestion_streaks.insert(*id, streak);
                }
                Disposition::Fail {
                    failure,
                    attempt,
                    run_duration,
                } => {
                    match job.maybe_schedule_retry(
                        now,
                        *attempt,
                        *run_duration,
                        &retry_policy,
                        failure,
                    ) {
                        Some((reschedule_at, next_attempt)) => {
                            retry_uuids.push(uuid::Uuid::from(*id));
                            retry_times.push(reschedule_at);
                            retry_attempts.push(next_attempt as i32);
                            retry_next.insert(*id, next_attempt);
                        }
                        None => {
                            delete_uuids.push(uuid::Uuid::from(*id));
                            fail_terminal_ids.insert(*id);
                        }
                    }
                }
            }
            staged.insert(*id, job);
        }

        // Applied-row gating: every row write returns (`RETURNING`) the ids
        // it actually
        // transitioned -- the `poller_instance_id` filter drops rows this
        // instance no longer owns -- and only those ids feed the outcome,
        // the promote registrations, and the entity persistence below.
        let mut applied: HashSet<JobId> = HashSet::new();
        let mut applied_pending_uuids: Vec<uuid::Uuid> = Vec::new();

        if !fresh_uuids.is_empty() {
            let rows = sqlx::query!(
                r#"
                WITH to_reschedule AS MATERIALIZED (
                    SELECT je.id, u.execute_at, je.woken_at
                    FROM job_executions je
                    JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                      ON je.id = u.id
                    WHERE je.poller_instance_id = $3
                    ORDER BY je.queue_id, je.id
                    FOR UPDATE
                )
                -- `t.woken_at` is the PRE-update value (the CTE read it
                -- before this statement's SET), which is what lets the same
                -- statement both honour the mark and clear it.
                UPDATE job_executions AS je
                SET state = 'pending', attempt_index = 1, poller_instance_id = NULL,
                    -- A park a wake already overtook: the callee landed while
                    -- this job was still running, so the wake could not move
                    -- the row (a running row has no `execute_at`) and left a
                    -- mark. Honouring it HERE, in the park write itself, is
                    -- what guarantees the row is never visible to
                    -- `PromoteHeadsHook` carrying the deadline the mark
                    -- overrides -- the hook would otherwise swap a queued
                    -- waiter out to `parked` on that stale time, and lowering
                    -- a parked row's `execute_at` breaks Invariant B.
                    execute_at = CASE WHEN t.woken_at IS NOT NULL
                                      THEN LEAST(t.execute_at, $4)
                                      ELSE t.execute_at END,
                    woken_at = NULL
                FROM to_reschedule t
                WHERE je.id = t.id
                RETURNING je.id AS "id!: JobId", je.job_type,
                          (t.woken_at IS NOT NULL) AS "was_woken!"
                "#,
                &fresh_uuids,
                &fresh_times,
                self.instance_id,
                now,
            )
            .fetch_all(op.as_executor())
            .await?;
            let mut overtaken: Vec<(JobId, JobType)> = Vec::new();
            for row in rows {
                applied.insert(row.id);
                applied_pending_uuids.push(uuid::Uuid::from(row.id));
                if row.was_woken {
                    overtaken.push((row.id, JobType::from_owned(row.job_type)));
                }
                outcome.rescheduled_pending = true;
            }
            // Rows the mark landed due now rather than at the deadline they
            // asked for are announced exactly as a spawn would announce them.
            self.announce_pulled_forward(op, overtaken).await?;
        }

        if !congestion_uuids.is_empty() {
            // `attempt_index` is deliberately absent from the `SET` list:
            // this write must not touch it either way (see the module doc).
            let rows = sqlx::query!(
                r#"
                WITH to_reschedule AS MATERIALIZED (
                    SELECT je.id, u.execute_at
                    FROM job_executions je
                    JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                      ON je.id = u.id
                    WHERE je.poller_instance_id = $3
                    ORDER BY je.queue_id, je.id
                    FOR UPDATE
                )
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = t.execute_at, poller_instance_id = NULL
                FROM to_reschedule t
                WHERE je.id = t.id
                RETURNING je.id AS "id!: JobId"
                "#,
                &congestion_uuids,
                &congestion_times,
                self.instance_id,
            )
            .fetch_all(op.as_executor())
            .await?;
            for row in rows {
                applied.insert(row.id);
                applied_pending_uuids.push(uuid::Uuid::from(row.id));
                outcome.rescheduled_pending = true;
                if let Some(streak) = congestion_streaks.get(&row.id) {
                    outcome.congestion_streak = outcome.congestion_streak.max(*streak);
                }
            }
        }

        if !retry_uuids.is_empty() {
            let rows = sqlx::query!(
                r#"
                WITH to_retry AS MATERIALIZED (
                    SELECT je.id, u.execute_at, u.attempt_index
                    FROM job_executions je
                    JOIN UNNEST($1::uuid[], $2::timestamptz[], $3::int4[])
                        AS u(id, execute_at, attempt_index)
                      ON je.id = u.id
                    WHERE je.poller_instance_id = $4
                    ORDER BY je.queue_id, je.id
                    FOR UPDATE
                )
                -- `woken_at` is CLEARED but deliberately NOT honoured: a wake
                -- must never shorten a retry backoff (the same guard the by-id
                -- and keyed pull-forwards carry). The mark is spent all the
                -- same -- the callee did finish, and this row will run at its
                -- backoff -- so leaving it would let attempt-count
                -- forgiveness (`attempt_reset_after_healthy_run`) resurrect a
                -- long-stale wake and yank the job due much later.
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = t.execute_at,
                    attempt_index = t.attempt_index, poller_instance_id = NULL,
                    woken_at = NULL
                FROM to_retry t
                WHERE je.id = t.id
                RETURNING je.id AS "id!: JobId"
                "#,
                &retry_uuids,
                &retry_times,
                &retry_attempts,
                self.instance_id,
            )
            .fetch_all(op.as_executor())
            .await?;
            for row in rows {
                applied.insert(row.id);
                applied_pending_uuids.push(uuid::Uuid::from(row.id));
                if let Some(next_attempt) = retry_next.get(&row.id) {
                    outcome.retried.push((row.id, *next_attempt));
                }
            }
        }

        let mut freed_queues: Vec<String> = Vec::new();
        let mut deleted_ids: HashSet<JobId> = HashSet::new();
        if !delete_uuids.is_empty() {
            // One delete serves completions and exhausted retries alike --
            // the `cleanup` CTE also drops the `job_execution_states` row
            // unless this type retains state past terminal (keyed with
            // `inherits_state`; batched types never are). The freed-queue
            // promote runs as the hook's OWN later statement, never a CTE
            // of this DELETE -- see `PromoteHeadsHook` for why folding it
            // in silently orphans a freshly parked row.
            let rows: Vec<(JobId, Option<String>)> = if let [id] = delete_uuids.as_slice() {
                sqlx::query!(
                    r#"
                    WITH deleted AS (
                        DELETE FROM job_executions
                        WHERE id = $1 AND poller_instance_id = $2
                        RETURNING id, queue_id
                    ), cleanup AS (
                        DELETE FROM job_execution_states s USING deleted d
                        WHERE s.id = d.id AND NOT $3::boolean
                    )
                    SELECT id AS "id!: JobId", queue_id AS "queue_id?"
                    FROM deleted
                    "#,
                    *id,
                    self.instance_id,
                    self.retains_state,
                )
                .fetch_all(op.as_executor())
                .await?
                .into_iter()
                .map(|row| (row.id, row.queue_id))
                .collect()
            } else {
                sqlx::query!(
                    r#"
                    WITH to_delete AS MATERIALIZED (
                        SELECT id FROM job_executions
                        WHERE id = ANY($1) AND poller_instance_id = $2
                        ORDER BY queue_id, id
                        FOR UPDATE
                    ), deleted AS (
                        DELETE FROM job_executions je USING to_delete t WHERE je.id = t.id
                        RETURNING je.id, je.queue_id
                    ), cleanup AS (
                        DELETE FROM job_execution_states s USING deleted d
                        WHERE s.id = d.id AND NOT $3::boolean
                    )
                    SELECT id AS "id!: JobId", queue_id AS "queue_id?"
                    FROM deleted
                    "#,
                    &delete_uuids,
                    self.instance_id,
                    self.retains_state,
                )
                .fetch_all(op.as_executor())
                .await?
                .into_iter()
                .map(|row| (row.id, row.queue_id))
                .collect()
            };
            for (id, queue_id) in rows {
                applied.insert(id);
                deleted_ids.insert(id);
                if let Some(queue_id) = queue_id {
                    freed_queues.push(queue_id);
                }
                if complete_ids.contains(&id) {
                    if let Some(job) = staged.get_mut(&id) {
                        job.complete_job();
                    }
                    outcome.completed.push(id);
                } else if fail_terminal_ids.contains(&id) {
                    outcome.errored_terminal.push(id);
                }
            }
        }

        // Invariant B for every row that ACTUALLY went back to `pending`:
        // it keeps its queue's active slot, but an older parked sibling
        // should run first during the backoff/delay. Multiple registrations
        // on one op merge into a single hook execution.
        if !applied_pending_uuids.is_empty() {
            PromoteHeadsHook::register(op, &self.notifier, own_types, applied_pending_uuids)
                .await?;
        }
        if !freed_queues.is_empty() {
            PromoteHeadsHook::register_freed_queues(op, &self.notifier, freed_queues).await?;
        }
        for id in &deleted_ids {
            self.notifier.job_terminal_in_op(op, *id).await?;
        }
        if !deleted_ids.is_empty() {
            let terminal: Vec<JobId> = deleted_ids.iter().copied().collect();
            self.wake_waiters_in_op(op, &terminal, now).await?;
        }

        // Persist only entities whose row transition this instance actually
        // performed -- staged events for unapplied ids are discarded with
        // their entities.
        let mut jobs: Vec<Job> = applied.iter().filter_map(|id| staged.remove(id)).collect();
        self.repo.update_all_in_op(op, &mut jobs).await?;
        Ok(outcome)
    }

    /// Wake every job parked on one of `terminal` (`job_waiters`, see
    /// `JobSpec::waiter`) and announce the rows that moved exactly as a
    /// spawn would. The waits `terminal` jobs themselves registered are
    /// deleted unconditionally: a terminal job waits on nothing.
    ///
    /// Two statements: the waits BY these jobs, then
    /// [`JobWaiters::wake_in_op`], which fuses the pull-forward with its
    /// `job_waiters` bookkeeping. The first stays separate because a
    /// terminal job waiting on another terminal job in the same batch is a
    /// row both would touch, and one statement may not write the same row
    /// twice.
    #[instrument(
        name = "job.wake_waiters",
        skip_all,
        fields(n_terminal = terminal.len(), n_moved)
    )]
    async fn wake_waiters_in_op(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
        terminal: &[JobId],
        now: DateTime<Utc>,
    ) -> Result<(), JobFault> {
        self.waiters.delete_waits_of_in_op(op, terminal).await?;
        let moved = self.waiters.wake_in_op(op, terminal, now).await?;
        Span::current().record("n_moved", moved.len());
        self.announce_pulled_forward(op, moved).await
    }

    /// The two signals a spawn fires, for rows a pull-forward just made
    /// due: the `ExecutionReady` notify per type (a peer poller may be
    /// asleep on a deadline computed before this write) and this process's
    /// claim demand. The target is always `now` here, so every moved row
    /// is due.
    async fn announce_pulled_forward(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
        moved: Vec<(JobId, JobType)>,
    ) -> Result<(), JobFault> {
        if moved.is_empty() {
            return Ok(());
        }
        let mut per_type: HashMap<JobType, usize> = HashMap::new();
        for (_, job_type) in moved {
            *per_type.entry(job_type).or_default() += 1;
        }
        let poller = self.poller.upgrade();
        for (job_type, n_due) in per_type {
            self.notifier.execution_ready_in_op(op, &job_type).await?;
            if let Some(poller) = &poller {
                poller.register_claim_demand(op, &job_type, n_due);
            }
        }
        Ok(())
    }

    /// `SET LOCAL enable_seqscan = off` for a job-end transaction on the
    /// shared pool (the internal pool pins it per connection): `job_executions`
    /// keeps a small heap under bloated indexes, so the planner otherwise
    /// seq-scans it on every completion. Only ever applied to transactions
    /// this crate owns.
    pub(crate) async fn pin_index_plans(
        op: &mut es_entity::DbOp<'static>,
        use_internal: bool,
    ) -> Result<(), sqlx::Error> {
        if use_internal {
            return Ok(());
        }
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(op.as_executor())
            .await?;
        Ok(())
    }

    /// Begin one attempt's op on the pool [`Self::finalize`]'s policy
    /// picked. The internal-pool branch falls back to the shared pool only
    /// if the poller has already been dropped: at that point the process is
    /// shutting down and the write is best-effort either way.
    async fn begin_op(&self, use_internal: bool) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        let repo = if use_internal {
            match self.poller.upgrade() {
                Some(poller) => JobRepo::new(poller.internal_pool()),
                None => (*self.repo).clone(),
            }
        } else {
            (*self.repo).clone()
        };
        repo.begin_op_with_clock(&self.clock).await
    }

    /// Whether the shared pool could hand out a connection right now --
    /// gates whether [`Self::finalize`]'s first attempt is worth pointing
    /// at it at all (see the module doc's "Pool choice").
    fn shared_pool_has_headroom(&self) -> bool {
        pool_connection_headroom(self.repo.pool()) > 0
    }

    /// Renders ids as a comma-separated list for one log field, so a warn
    /// line can be tied to the jobs it concerns.
    fn display_ids(ids: &[JobId]) -> String {
        let mut out = String::new();
        for (i, id) in ids.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&id.to_string());
        }
        out
    }

    /// [`Self::display_ids`] over finalize items.
    fn display_ids_of(items: &[(JobId, Disposition)]) -> String {
        let ids: Vec<JobId> = items.iter().map(|(id, _)| *id).collect();
        Self::display_ids(&ids)
    }
}

/// End-to-end cover for `Finalizer::finalize`'s abort-retry guard
/// (`finalize`, phase 1b): a deadlock or serialization abort on the
/// finalizer's OWN disposition write arrives as a `Transient`, through
/// errlanes' blanket `From` for `sqlx::Error`, so a real deadlock injected
/// on that write is retried up to `TX_ABORT_MAX_ATTEMPTS` rather than
/// propagating. An opaque error type here would classify as neither, and
/// the guard would never fire.
///
/// Needs a live Postgres (`PG_CON`); not run as part of `cargo test --lib`
/// without one, same as every other DB-backed unit test in this crate.
#[cfg(test)]
mod deadlock_regression_tests {
    use super::*;
    use crate::{
        entity::NewJob, notification_router::JobNotificationRouter, notifier::JobEventNotifier,
        repo::JobRepo, tracker::JobTracker,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing_subscriber::layer::SubscriberExt;

    async fn init_pool() -> sqlx::PgPool {
        let pg_con = std::env::var("PG_CON").expect("PG_CON must be set for this test");
        sqlx::PgPool::connect(&pg_con).await.expect("connect")
    }

    /// Observes whether `Finalizer::finalize`'s own
    /// "disposition write lost a lock conflict; retrying" warning fired --
    /// the one direct signal, short of parsing logs, that the abort-retry
    /// branch was actually taken rather than the write simply succeeding
    /// because no conflict ever materialised.
    struct RetryWarnObserved(Arc<AtomicBool>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RetryWarnObserved {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Finder(bool);
            impl tracing::field::Visit for Finder {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" && format!("{value:?}").contains("lock conflict") {
                        self.0 = true;
                    }
                }
            }
            let mut finder = Finder(false);
            event.record(&mut finder);
            if finder.0 {
                self.0.store(true, Ordering::SeqCst);
            }
        }
    }

    /// One row of a job that actually exists (a `jobs` entity plus its
    /// `job_executions` row, "claimed" by `instance_id`) so
    /// `Finalizer::finalize_in_op`'s entity load and row writes both find
    /// something real to work with.
    async fn seed_claimed_job(
        repo: &JobRepo,
        job_type: &JobType,
        queue_id: &str,
        instance_id: uuid::Uuid,
    ) -> JobId {
        let id = JobId::new();
        let mut op = repo
            .begin_op_with_clock(&ClockHandle::realtime())
            .await
            .unwrap();
        let new_job = NewJob::builder()
            .id(id)
            .job_type(job_type.clone())
            .config(serde_json::json!({}))
            .unwrap()
            .queue_id(Some(queue_id.to_string()))
            .schedule_at(chrono::Utc::now())
            .build()
            .expect("build NewJob");
        repo.create_in_op(&mut op, new_job)
            .await
            .expect("create job");
        op.commit().await.expect("commit job create");

        sqlx::query(
            "INSERT INTO job_executions \
             (id, job_type, queue_id, poller_instance_id, attempt_index, state, alive_at, created_at) \
             VALUES ($1, $2, $3, $4, 1, 'running', NOW(), NOW())",
        )
        .bind(uuid::Uuid::from(id))
        .bind(job_type.as_str())
        .bind(queue_id)
        .bind(instance_id)
        .execute(repo.pool())
        .await
        .expect("insert job_executions row");
        id
    }

    /// Holds a `FOR UPDATE` lock on `first`, signals, waits, then tries to
    /// lock `second` -- the reverse of the order `Finalizer::finalize`'s own
    /// CTE takes (`ORDER BY queue_id, id`), so a cycle forms once the
    /// finalizer's write is also mid-flight. Retries the whole attempt if
    /// IT is the deadlock victim, so the loop always converges once the
    /// finalizer (if it was instead the victim) has released and retried.
    /// One contestant in the reverse-order lock dance: holds `first`, waits
    /// for the finalizer to be mid-flight, then tries `second`. On a
    /// deadlock loss it just ends (dropping the transaction rolls back and
    /// releases `first`) rather than retrying its own attempt from scratch
    /// -- several of these are spawned PRE-QUEUED on `first` instead (see
    /// the caller), so the next contestant in Postgres's own lock wait
    /// queue takes over with no re-acquisition latency, which is what lets
    /// the deadlock recur on the finalizer's later (`use_internal = true`,
    /// attempt-counted) tries and not just its first (shared-pool,
    /// uncounted-fallback) one.
    async fn contest_once(
        pool: sqlx::PgPool,
        first: uuid::Uuid,
        second: uuid::Uuid,
        on_holding_first: Option<Arc<tokio::sync::Notify>>,
    ) {
        let Ok(mut tx) = pool.begin().await else {
            return;
        };
        if sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR UPDATE")
            .bind(first)
            .execute(&mut *tx)
            .await
            .is_err()
        {
            return;
        }
        if let Some(ready) = on_holding_first {
            ready.notify_one();
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = sqlx::query("SELECT id FROM job_executions WHERE id = $1 FOR UPDATE")
            .bind(second)
            .execute(&mut *tx)
            .await;
        // Releases `first` (and `second`, if won) either way: these probes
        // never write anything, so a rollback is exactly as good as a
        // commit for the purpose of freeing the locks.
        let _ = tx.rollback().await;
    }

    #[tokio::test]
    async fn finalize_retries_a_server_confirmed_deadlock_on_its_own_disposition_write() {
        let observed = Arc::new(AtomicBool::new(false));
        let subscriber =
            tracing_subscriber::registry().with(RetryWarnObserved(Arc::clone(&observed)));
        let _guard = tracing::subscriber::set_default(subscriber);

        let pool = init_pool().await;
        let repo = Arc::new(JobRepo::new(&pool));
        let tracker = Arc::new(JobTracker::new(1, 1));
        let router = Arc::new(JobNotificationRouter::new(
            &pool,
            Arc::clone(&repo),
            16,
            std::time::Duration::from_secs(60),
        ));
        let notifier =
            JobEventNotifier::spawn(&pool, Arc::clone(&tracker), router.terminal_sender());
        let instance_id = uuid::Uuid::now_v7();
        let job_type = JobType::new(Box::leak(
            format!("deadlock-regression-{}", uuid::Uuid::now_v7()).into_boxed_str(),
        ));

        // Bounded attempts at reproducing the cycle: Postgres's victim
        // choice between two symmetric waiters isn't guaranteed to pick the
        // finalizer every round, so retry the whole scenario with fresh
        // rows until the retry warning is actually observed (or give up
        // with a clear failure rather than flake silently).
        for round in 0..8 {
            if observed.load(Ordering::SeqCst) {
                break;
            }
            // `idx_job_executions_queue_active` is a unique index on
            // `queue_id` for any `pending`/`running` row, so each round
            // needs its own queue ids -- the "a-"/"z-" prefixes are what
            // matters (guarantees `ORDER BY queue_id` sorts A before B),
            // the suffix just keeps rounds from colliding.
            let suffix = uuid::Uuid::now_v7();
            let id_a =
                seed_claimed_job(&repo, &job_type, &format!("a-queue-{suffix}"), instance_id).await;
            let id_b =
                seed_claimed_job(&repo, &job_type, &format!("z-queue-{suffix}"), instance_id).await;

            let finalizer = Finalizer::new(
                Weak::new(),
                Arc::clone(&repo),
                Arc::clone(&notifier),
                RetrySettings::default(),
                false,
                instance_id,
                ClockHandle::realtime(),
            );

            // Several contestants, pre-queued on `first` (= B) before
            // `finalize()` even starts: once the leader releases (deadlock
            // loss or a clean win), the next one in Postgres's own lock
            // wait queue takes over with no re-acquisition latency, so a
            // fresh contestant is already in position for whichever
            // finalizer attempt (shared-pool first, or an internal-pool
            // retry after it) comes next.
            let ready = Arc::new(tokio::sync::Notify::new());
            let contestants: Vec<_> = (0..4)
                .map(|i| {
                    tokio::spawn(contest_once(
                        pool.clone(),
                        uuid::Uuid::from(id_b),
                        uuid::Uuid::from(id_a),
                        (i == 0).then(|| Arc::clone(&ready)),
                    ))
                })
                .collect();
            ready.notified().await;

            let now = chrono::Utc::now();
            let items = [
                (id_a, Disposition::Fresh { at: now }),
                (id_b, Disposition::Fresh { at: now }),
            ];
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                finalizer.finalize(&items, |_, _| {}),
            )
            .await
            .unwrap_or_else(|_| panic!("round {round}: finalize() did not return within 15s"));
            result.unwrap_or_else(|e| {
                panic!("round {round}: finalize() should retry a server-confirmed deadlock and succeed, got: {e}")
            });

            for c in contestants {
                let _ = c.await;
            }
        }

        assert!(
            observed.load(Ordering::SeqCst),
            "finalize() never hit its own abort-retry warning across 8 rounds of a genuine \
             two-row deadlock -- either the retry path regressed, or this environment's \
             deadlock detector never picked the finalizer as victim in any round"
        );
    }
}
