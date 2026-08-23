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
use rand::{RngExt, rng};
use tracing::{Span, instrument};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use super::{
    JobId,
    entity::{Job, JobType, RetryPolicy},
    error::{JobError, TX_ABORT_MAX_ATTEMPTS, is_retryable_conflict},
    execution_hooks::PromoteHeadsHook,
    notifier::JobEventNotifier,
    poller::{JobPoller, pool_connection_headroom},
    repo::JobRepo,
    runner::RetrySettings,
};

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
    /// The job failed with `error` on its `attempt`-th attempt: the type's
    /// `RetryPolicy` decides between a backoff retry (next `attempt_index`)
    /// and terminal deletion.
    Fail { error: String, attempt: u32 },
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
/// still this instance's to delete), mirroring the batch's historical
/// `n_errored` accounting; `completed` counts only rows this instance
/// actually deleted.
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
    /// later. This is the case that used to be silent.
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
        Self {
            poller,
            repo,
            notifier,
            retry_settings,
            retains_state,
            instance_id,
            clock,
        }
    }

    /// Classify a runner's error and convert it into the right `JobError`,
    /// recording the `error`/`error.message`/`error.level` fields on the
    /// CURRENT span. Classification happens BEFORE stringifying: `error` is
    /// the runner's own boxed error, the only point where an underlying
    /// `sqlx::Error::PoolTimedOut` still has its structure to downcast --
    /// `.to_string()` is a one-way trip into `JobExecutionError(String)`,
    /// and a plain `String` has no `.source()` chain for
    /// [`Self::is_pool_congestion`] to walk afterward.
    ///
    /// Congestion logs at INFO -- it is the expected, non-punitive signal
    /// this module exists for (see the module doc) -- real errors at WARN.
    pub(crate) fn maybe_reclassify(&self, error: Box<dyn std::error::Error>) -> JobError {
        let span = Span::current();
        let congestion = Self::is_pool_congestion(error.as_ref());
        let error = error.to_string();
        span.record("error", true);
        span.record("error.message", tracing::field::display(&error));
        span.record(
            "error.level",
            tracing::field::display(if congestion {
                tracing::Level::INFO
            } else {
                tracing::Level::WARN
            }),
        );
        if congestion {
            JobError::PoolCongestion(error)
        } else {
            JobError::JobExecutionError(error)
        }
    }

    /// Reschedule `ids` after a `PoolCongestion` classification: every row
    /// goes back to `pending` at now + [`CONGESTION_DELAY_MS`] +/-
    /// [`CONGESTION_JITTER_MS`], `attempt_index` untouched, on a fresh
    /// `CongestionRescheduled` entity event -- one [`Disposition::Congestion`]
    /// per id through the ordinary [`Self::finalize`] machinery.
    ///
    /// `attempts` maps each id to its in-flight attempt number, recorded
    /// unchanged on the entity's next `ExecutionScheduled` event; an id
    /// missing from the map defaults to attempt 1.
    #[instrument(name = "job.congestion_reschedule", skip_all,
        fields(n_jobs = ids.len(), congestion_streak)
    )]
    pub(crate) async fn reschedule_congested(
        &self,
        ids: &[JobId],
        attempts: &HashMap<JobId, u32>,
        message: String,
    ) -> Result<(), JobError> {
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
    ) -> Result<(), JobError> {
        let attempts = HashMap::from([(id, attempt)]);
        self.reschedule_congested(&[id], &attempts, message).await
    }

    /// Run `items` through [`Self::finalize_in_op`] in a transaction this
    /// finalizer owns, with the pool choice and abort-retry policy from the
    /// module doc: first attempt on the shared pool when it has headroom,
    /// any first-attempt failure there switches to the internal pool, and
    /// internal-pool attempts retry transient aborts
    /// ([`is_retryable_conflict`]) up to [`TX_ABORT_MAX_ATTEMPTS`].
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
    ) -> Result<FinalizeOutcome, JobError> {
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
                Ok(mut op) => match self.finalize_in_op(&mut op, items).await {
                    Ok(outcome) => {
                        after_write(&mut op, &outcome);
                        Ok((op, outcome))
                    }
                    Err(e) => Err(e),
                },
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
                Err(e) if attempt_no < TX_ABORT_MAX_ATTEMPTS && is_retryable_conflict(&e) => {
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
            // server-reported abort (`is_retryable_conflict`: deadlock
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
                Err(e) if attempt_no < TX_ABORT_MAX_ATTEMPTS && is_retryable_conflict(&e) => {
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
        op: &mut impl AtomicOperation,
        items: &[(JobId, Disposition)],
    ) -> Result<FinalizeOutcome, JobError> {
        let mut outcome = FinalizeOutcome::default();
        if items.is_empty() {
            return Ok(outcome);
        }
        let now = op.maybe_now().unwrap_or_else(|| self.clock.now());
        let retry_policy = RetryPolicy::from(&self.retry_settings);

        let ids: Vec<JobId> = items.iter().map(|(id, _)| *id).collect();
        let mut entities = self.repo.find_all_in_op::<Job>(&mut *op, &ids).await?;

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
                Disposition::Fail { error, attempt } => {
                    match job.maybe_schedule_retry(now, *attempt, &retry_policy, error.clone()) {
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
                    SELECT je.id, u.execute_at
                    FROM job_executions je
                    JOIN UNNEST($1::uuid[], $2::timestamptz[]) AS u(id, execute_at)
                      ON je.id = u.id
                    WHERE je.poller_instance_id = $3
                    ORDER BY je.queue_id, je.id
                    FOR UPDATE
                )
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = t.execute_at, attempt_index = 1,
                    poller_instance_id = NULL
                FROM to_reschedule t
                WHERE je.id = t.id
                RETURNING je.id AS "id!: JobId"
                "#,
                &fresh_uuids,
                &fresh_times,
                self.instance_id,
            )
            .fetch_all(op.as_executor())
            .await?;
            for row in rows {
                applied.insert(row.id);
                applied_pending_uuids.push(uuid::Uuid::from(row.id));
                outcome.rescheduled_pending = true;
            }
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
                UPDATE job_executions AS je
                SET state = 'pending', execute_at = t.execute_at,
                    attempt_index = t.attempt_index, poller_instance_id = NULL
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
            let rows = sqlx::query!(
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
            .await?;
            for row in rows {
                applied.insert(row.id);
                deleted_ids.insert(row.id);
                if let Some(queue_id) = row.queue_id {
                    freed_queues.push(queue_id);
                }
                if complete_ids.contains(&row.id) {
                    if let Some(job) = staged.get_mut(&row.id) {
                        job.complete_job();
                    }
                    outcome.completed.push(row.id);
                } else if fail_terminal_ids.contains(&row.id) {
                    outcome.errored_terminal.push(row.id);
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

        // Persist only entities whose row transition this instance actually
        // performed -- staged events for unapplied ids are discarded with
        // their entities.
        let mut jobs: Vec<Job> = applied.iter().filter_map(|id| staged.remove(id)).collect();
        self.repo.update_all_in_op(op, &mut jobs).await?;
        Ok(outcome)
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

    /// Whether this error (or anything it wraps) is
    /// `sqlx::Error::PoolTimedOut` -- the shared pool had no connection to
    /// hand out within its acquire timeout. This carries no evidence the
    /// job is broken: it says the pool was busy, not that the work is
    /// wrong.
    ///
    /// Walks the `source()` chain because a runner's error crosses an
    /// object-erasure boundary (`run`/`run_batch_erased` return
    /// `Box<dyn std::error::Error>`) before it reaches this crate's own
    /// error handling, so the check has to happen on the *original* error
    /// -- [`Self::maybe_reclassify`], the only caller, does exactly that
    /// before stringifying.
    fn is_pool_congestion(err: &(dyn std::error::Error + 'static)) -> bool {
        let mut source = Some(err);
        while let Some(e) = source {
            if let Some(sqlx::Error::PoolTimedOut) = e.downcast_ref::<sqlx::Error>() {
                return true;
            }
            source = e.source();
        }
        false
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
