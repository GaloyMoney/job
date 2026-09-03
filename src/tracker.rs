use tokio::sync::Notify;

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use super::JobId;
use crate::entity::JobType;

#[derive(Default)]
struct LiveJobs {
    counts: HashMap<JobId, usize>,
}

impl LiveJobs {
    fn started(&mut self, id: JobId) {
        *self.counts.entry(id).or_insert(0) += 1;
    }

    fn finished(&mut self, id: JobId) {
        if let Some(n) = self.counts.get_mut(&id) {
            *n -= 1;
            if *n == 0 {
                self.counts.remove(&id);
            }
        }
    }

    fn ids(&self) -> Vec<uuid::Uuid> {
        self.counts.keys().map(|id| uuid::Uuid::from(*id)).collect()
    }
}

pub(crate) struct JobTracker {
    min_jobs: usize,
    max_jobs: usize,
    running_jobs: AtomicUsize,
    notify: Notify,
    live_jobs: Mutex<LiveJobs>,
    /// Execution units currently in flight, per job type. A unit is one
    /// dispatched job for a plain type or one dispatched batch for a batched
    /// type. Read by the poller to size each type's claim so it never locks
    /// rows no free slot can start on.
    units_in_flight: Mutex<HashMap<JobType, usize>>,
    /// The job types this process polls for. Readiness reports from every
    /// source are filtered against these. Unset until polling starts, so
    /// earlier reports are dropped rather than queued.
    job_types: OnceLock<Vec<JobType>>,
    /// Job types with a per-process and/or global concurrency cap. Set once,
    /// when polling starts (mirrors `job_types`): a freed slot on one of
    /// these types is what makes its backlog claimable again, so
    /// `job_completed` notifies the poll loop for them even when the
    /// process-wide min/max thresholds wouldn't otherwise trigger a wake.
    capped_types: OnceLock<HashSet<JobType>>,
    /// Per-type "claimable work may exist" hint, set by
    /// [`Self::job_execution_inserted`] (spawns, promotes, and retries all
    /// funnel through it already) and by `ClaimHook::pre_commit` whenever a
    /// probe comes back full (more may remain behind it). Consumed by
    /// [`Self::consume_due_hint`] -- the completion-recycle claim probe's
    /// gate: at steady state a type's queue is usually empty, so an
    /// unconditional post-completion probe pays full claim-probe cost to
    /// find nothing.
    /// Fresh-demand probes (this op's own spawn/promote) never consult this
    /// -- their due rows are guaranteed by construction.
    maybe_due: Mutex<HashSet<JobType>>,
}

impl JobTracker {
    pub fn new(min_jobs: usize, max_jobs: usize) -> Self {
        Self {
            min_jobs,
            max_jobs,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
            live_jobs: Mutex::new(LiveJobs::default()),
            units_in_flight: Mutex::new(HashMap::new()),
            job_types: OnceLock::new(),
            capped_types: OnceLock::new(),
            maybe_due: Mutex::new(HashSet::new()),
        }
    }

    /// Called once, when polling starts.
    pub fn set_job_types(&self, job_types: Vec<JobType>) {
        let _ = self.job_types.set(job_types);
    }

    /// Called once, when polling starts, with every job type carrying a
    /// per-process and/or global concurrency cap.
    pub fn set_capped_types(&self, capped_types: HashSet<JobType>) {
        let _ = self.capped_types.set(capped_types);
    }

    pub fn next_batch_size(&self) -> Option<usize> {
        let n_running = self.running_jobs.load(Ordering::SeqCst);
        tracing::Span::current().record("n_jobs_running", n_running);
        if n_running < self.min_jobs {
            Some(self.max_jobs - n_running)
        } else {
            None
        }
    }

    pub fn dispatch_job(&self, id: JobId, job_type: &JobType) {
        self.running_jobs.fetch_add(1, Ordering::SeqCst);
        self.live_jobs
            .lock()
            .expect("live_jobs poisoned")
            .started(id);
        *self
            .units_in_flight
            .lock()
            .expect("units_in_flight poisoned")
            .entry(job_type.clone())
            .or_insert(0) += 1;
    }

    /// Account for a batch as a **single** unit of saturation while keeping
    /// every job in it individually live.
    ///
    /// `running_jobs` bounds concurrent execution units — tasks, transactions,
    /// pool connections — and a batch is exactly one of each no matter how many
    /// rows it carries. Counting rows instead would let one batch consume the
    /// whole process budget and stall polling until it commits. Liveness is a
    /// per-row question (the keep-alive heartbeat and lost-job reclaim both work
    /// on ids), so those stay unbatched.
    pub fn dispatch_batch(&self, job_type: &JobType, ids: &[JobId]) {
        self.running_jobs.fetch_add(1, Ordering::SeqCst);
        {
            let mut live = self.live_jobs.lock().expect("live_jobs poisoned");
            for id in ids {
                live.started(*id);
            }
        }
        *self
            .units_in_flight
            .lock()
            .expect("units_in_flight poisoned")
            .entry(job_type.clone())
            .or_insert(0) += 1;
    }

    /// Execution units of `job_type` in flight right now — batches for a
    /// batched type, jobs for a plain type. The poller subtracts this from
    /// the type's slot count to decide how many rows it may claim.
    pub fn units_in_flight(&self, job_type: &JobType) -> usize {
        self.units_in_flight
            .lock()
            .expect("units_in_flight poisoned")
            .get(job_type)
            .copied()
            .unwrap_or(0)
    }

    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    /// Wake the poll loop unconditionally. Used by the poller's
    /// pool-headroom waiter (`JobPoller::arm_pool_headroom_waiter`), whose
    /// wake condition -- shared-pool headroom returning -- is not a job
    /// lifecycle event and so has no other path to this `Notify`.
    /// `notify_one` holds at most one permit, so a wake landing while the
    /// loop is mid-poll is not lost: the next `notified()` returns
    /// immediately.
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Wake the poll loop if this process polls `job_type`. `notify_one` holds
    /// at most one permit, so repeated reports collapse into one wake-up.
    /// Also arms [`Self::consume_due_hint`]'s hint for the type, so a
    /// completion's recycle probe that runs after this report finds it.
    pub fn job_execution_inserted(&self, job_type: &str) {
        let Some(job_types) = self.job_types.get() else {
            return;
        };
        if let Some(job_type) = job_types.iter().find(|jt| jt.as_str() == job_type) {
            self.set_due_hint(job_type);
            self.notify.notify_one();
        }
    }

    /// Records that `job_type` may have claimable work waiting. See the
    /// `maybe_due` field doc for the two call sites that set it.
    pub(crate) fn set_due_hint(&self, job_type: &JobType) {
        self.maybe_due
            .lock()
            .expect("maybe_due poisoned")
            .insert(job_type.clone());
    }

    /// Consumes (removes) `job_type`'s due-hint, returning whether it was
    /// set. Consume-before-probe: a concurrent spawn's hint that lands AFTER
    /// this read is not lost -- it survives for the next completion's probe.
    /// A hint consumed with nothing actually due just costs one empty probe,
    /// same as today's unconditional behavior; a skipped claim is always
    /// backstopped by the ordinary poll.
    pub(crate) fn consume_due_hint(&self, job_type: &JobType) -> bool {
        self.maybe_due
            .lock()
            .expect("maybe_due poisoned")
            .remove(job_type)
    }

    /// Release the unit [`dispatch_job`](Self::dispatch_job) took for
    /// `job_type` and wake the poll loop if that matters: the process-wide
    /// min-jobs threshold was crossed, the job is being retried, or
    /// `job_type` carries a concurrency cap — in which case a freed slot is
    /// what makes its backlog claimable again, and nothing else would
    /// trigger that poll (mirrors [`batch_completed`](Self::batch_completed)'s
    /// always-notify).
    pub fn job_completed(&self, id: JobId, job_type: &JobType, rescheduled: bool) {
        let n_running_jobs = self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        self.live_jobs
            .lock()
            .expect("live_jobs poisoned")
            .finished(id);
        self.release_unit(job_type);
        if rescheduled
            || n_running_jobs == self.min_jobs
            || self
                .capped_types
                .get()
                .is_some_and(|types| types.contains(job_type))
        {
            self.notify.notify_one();
        }
    }

    /// Release the single unit taken by [`dispatch_batch`](Self::dispatch_batch),
    /// clear every job it held live, and free its type's batch slot.
    ///
    /// Always wakes the poll loop: a freed slot is what makes the type's
    /// backlog claimable again, and nothing else would trigger that poll.
    pub fn batch_completed(&self, job_type: &JobType, ids: &[JobId], _rescheduled: bool) {
        self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        {
            let mut live = self.live_jobs.lock().expect("live_jobs poisoned");
            for id in ids {
                live.finished(*id);
            }
        }
        self.release_unit(job_type);
        self.notify.notify_one();
    }

    /// Decrement `job_type`'s in-flight unit count, dropping the entry once
    /// it reaches zero (shared by [`job_completed`](Self::job_completed) and
    /// [`batch_completed`](Self::batch_completed)).
    fn release_unit(&self, job_type: &JobType) {
        let mut counts = self
            .units_in_flight
            .lock()
            .expect("units_in_flight poisoned");
        if let Some(n) = counts.get_mut(job_type) {
            *n -= 1;
            if *n == 0 {
                counts.remove(job_type);
            }
        }
    }

    pub fn live_job_ids(&self) -> Vec<uuid::Uuid> {
        self.live_jobs.lock().expect("live_jobs poisoned").ids()
    }

    /// Reserve one execution unit of `job_type` BEFORE the DB write that
    /// would consume it. Mirrors `dispatch_job`'s accounting exactly -- a
    /// reservation IS a unit in flight, the same way `dispatch_job`'s claim
    /// is -- so `next_batch_size`/`plan_claim` see it immediately, for the
    /// same reason `JobDispatcher::new` claims its slot synchronously rather
    /// than inside the execution task (see its doc comment).
    ///
    /// `None` if the process is already at `max_jobs`, or `per_type_cap` is
    /// `Some` and `job_type` is already at it. Reserving does not require
    /// knowing the job's id yet -- see [`UnitReservation::into_live`].
    ///
    /// **Known bounded race**: a reservation taken here between
    /// `JobRegistry::plan_claim` reading `units_in_flight` and the poll's
    /// claim query actually running against Postgres is invisible to that
    /// poll -- its `row_limit` was already baked in as a query parameter
    /// from the stale snapshot, so it can claim up to that many rows even
    /// though this reservation just took some of the capacity it assumed
    /// was free. The overshoot is bounded (at most however many concurrent
    /// short-circuit reservations land inside one poll's snapshot-to-claim
    /// window) and self-corrects on the very next poll cycle, since
    /// `units_in_flight` is authoritative again by then -- the same class of
    /// soft, self-correcting accounting imperfection `max_jobs`' own
    /// unit-vs-row mismatch already carries. A tighter fix needs either a
    /// lock spanning the whole plan-to-claim window (serializing every
    /// short-circuit reservation behind poll latency -- a real regression
    /// for exactly the paths this design exists to speed up) or a
    /// post-claim backstop that re-validates and releases any row a poll
    /// over-claimed before dispatching it. A per-type cap that must never be
    /// exceeded even transiently should not rely on this path alone.
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        job_type: &JobType,
        per_type_cap: Option<usize>,
    ) -> Option<UnitReservation> {
        if self.running_jobs.load(Ordering::SeqCst) >= self.max_jobs {
            return None;
        }
        {
            let mut units = self
                .units_in_flight
                .lock()
                .expect("units_in_flight poisoned");
            if let Some(cap) = per_type_cap {
                let current = units.get(job_type).copied().unwrap_or(0);
                if current >= cap {
                    return None;
                }
            }
            *units.entry(job_type.clone()).or_insert(0) += 1;
        }
        self.running_jobs.fetch_add(1, Ordering::SeqCst);
        Some(UnitReservation {
            tracker: Arc::clone(self),
            job_type: job_type.clone(),
            resolved: false,
        })
    }

    /// Transfer an already-accounted-for unit of `job_type`'s capacity from a
    /// job/batch that is completing to whatever this reservation goes on to
    /// dispatch next, instead of releasing it outright -- the completion-time
    /// counterpart of [`Self::try_reserve`].
    ///
    /// Unlike `try_reserve`, this can never fail and never touches
    /// `running_jobs`/`units_in_flight`: the unit is already counted (the
    /// completing job/batch's own `dispatch_job`/`dispatch_batch` call
    /// counted it), so one running unit of `job_type` being replaced by
    /// another leaves both caps unchanged -- recycling nets to zero on the
    /// counters by construction.
    ///
    /// The CALLER must ensure the completing dispatcher's own ordinary
    /// release (`job_completed`/`batch_completed`, fired from `Drop`) is
    /// skipped for this same unit -- see
    /// [`Self::mark_finished_without_releasing_unit`] and
    /// `JobDispatcher::recycle_unit`/`BatchDispatcher::recycle_unit` -- or the
    /// unit is released twice.
    pub(crate) fn recycle(self: &Arc<Self>, job_type: &JobType) -> UnitReservation {
        UnitReservation {
            tracker: Arc::clone(self),
            job_type: job_type.clone(),
            resolved: false,
        }
    }

    /// Clear `ids` from the live-job liveness set WITHOUT touching
    /// `running_jobs`/`units_in_flight` -- the counterpart to a completion
    /// that [`Self::recycle`]s its unit into a fresh dispatch instead of
    /// releasing it. The reclaim/shutdown-drain liveness bookkeeping must
    /// still see these jobs finish; the capacity accounting must not, since
    /// the unit stays claimed (by whatever the recycled reservation goes on
    /// to dispatch).
    pub(crate) fn mark_finished_without_releasing_unit(&self, ids: &[JobId]) {
        let mut live = self.live_jobs.lock().expect("live_jobs poisoned");
        for id in ids {
            live.finished(*id);
        }
    }
}

/// A pre-claimed unit of `job_type`'s capacity, taken via
/// [`JobTracker::try_reserve`] before the DB write that would consume it.
/// Exactly one of [`Self::into_live`]/[`Self::release`] should be called once
/// the write's outcome is known; a reservation dropped without either is
/// released automatically -- a leaked reservation would permanently eat a
/// slot.
pub(crate) struct UnitReservation {
    tracker: Arc<JobTracker>,
    job_type: JobType,
    resolved: bool,
}

impl UnitReservation {
    /// The reserved write landed and `id` is about to be dispatched.
    /// Registers `id` in the same live-job bookkeeping [`JobTracker::dispatch_job`]
    /// uses, WITHOUT re-incrementing the counters `try_reserve` already
    /// incremented -- the caller must build its `JobDispatcher` from this
    /// reservation (not via the ordinary `dispatch_job`-calling constructor)
    /// or the unit is counted twice.
    pub(crate) fn into_live(mut self, id: JobId) {
        self.resolved = true;
        self.tracker
            .live_jobs
            .lock()
            .expect("live_jobs poisoned")
            .started(id);
    }

    /// The unit is given back WITHOUT waking the poll loop -- the quiet
    /// counterpart to just dropping the reservation (whose `Drop` releases
    /// AND wakes). For a write that did not land running (conflicted and
    /// parked instead, or the operation rolled back), and for
    /// `ClaimHook::pre_commit`'s budget truncation, where the wake would be
    /// actively wrong: the truncation happens because the pool budget just
    /// ran out, and a poll woken at that instant re-reads headroom that
    /// cannot yet see the hook's own surviving claims and over-admits on
    /// top of them.
    pub(crate) fn release(mut self) {
        self.resolved = true;
        self.tracker.running_jobs.fetch_sub(1, Ordering::SeqCst);
        self.tracker.release_unit(&self.job_type);
    }

    /// Quiet release for a completion-side path that bypassed
    /// [`JobTracker::job_completed`]; returns whether the poll loop must be
    /// woken under that method's rule (the `min_jobs` crossing or a capped
    /// type). The caller defers the wake past its own in-flight claims.
    #[must_use = "the returned flag says whether the poll loop must be woken"]
    pub(crate) fn hand_back(mut self) -> bool {
        self.resolved = true;
        let n_running_jobs = self.tracker.running_jobs.fetch_sub(1, Ordering::SeqCst);
        self.tracker.release_unit(&self.job_type);
        n_running_jobs == self.tracker.min_jobs
            || self
                .tracker
                .capped_types
                .get()
                .is_some_and(|types| types.contains(&self.job_type))
    }

    /// Batch counterpart of [`Self::into_live`]: the claimed batch landed and
    /// every id in it is about to be dispatched together as ONE unit
    /// (mirrors [`JobTracker::dispatch_batch`]'s per-id liveness / single-unit
    /// counter split) -- WITHOUT re-incrementing the counters `try_reserve`/
    /// `recycle` already accounted for.
    pub(crate) fn into_live_batch(mut self, ids: &[JobId]) {
        self.resolved = true;
        let mut live = self.tracker.live_jobs.lock().expect("live_jobs poisoned");
        for id in ids {
            live.started(*id);
        }
    }
}

impl Drop for UnitReservation {
    fn drop(&mut self) {
        if !self.resolved {
            self.tracker.running_jobs.fetch_sub(1, Ordering::SeqCst);
            self.tracker.release_unit(&self.job_type);
            // Always wake the poll loop, for the same reason
            // [`JobTracker::batch_completed`] always does: a freed unit is
            // what makes this type's backlog claimable again, and nothing
            // else would trigger that poll. Concretely load-bearing for the
            // short-circuit's pool-aware gate: when `ClaimHook::pre_commit`
            // skips a recycled reservation because the pool has no
            // headroom, THIS drop is the only release that unit gets -- its
            // dispatcher already detached from the ordinary Drop-triggered
            // `batch_completed`/`job_completed` release (and their
            // notifies) via `recycle_unit`. Without a wake here the freed
            // slot sits invisible until the idle-poll fallback (up to
            // `MAX_WAIT`), turning every gate-skipped recycle into a
            // minute-long stall of a drainable backlog. (That zero-budget
            // wake cannot over-admit -- the woken poll's budget is also
            // zero, so it only arms the headroom waiter. A release where
            // the wake WOULD over-admit -- the gate's partial-budget
            // truncation -- goes through [`UnitReservation::release`], the
            // quiet path, instead of this drop.)
            self.tracker.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_id_until_last_runner_finishes() {
        let id = JobId::new();
        let mut live = LiveJobs::default();

        live.started(id);
        live.started(id);
        assert_eq!(live.ids().len(), 1);

        live.finished(id);
        assert_eq!(
            live.ids().len(),
            1,
            "id dropped while a runner is still live"
        );

        live.finished(id);
        assert!(live.ids().is_empty());
    }

    #[test]
    fn ignores_unmatched_finished() {
        let id = JobId::new();
        let mut live = LiveJobs::default();
        live.finished(id);
        assert!(live.ids().is_empty());
    }

    /// `consume_due_hint` is a one-shot read: unset until something reports
    /// the type, then cleared by the read itself.
    #[test]
    fn due_hint_is_consumed_exactly_once() {
        let tracker = JobTracker::new(0, 10);
        let job_type = JobType::from_owned("due-hint-consume".to_string());

        assert!(
            !tracker.consume_due_hint(&job_type),
            "unset until something reports it"
        );

        tracker.set_due_hint(&job_type);
        assert!(tracker.consume_due_hint(&job_type), "reports once set");
        assert!(
            !tracker.consume_due_hint(&job_type),
            "consuming clears it -- a second read finds nothing"
        );
    }

    /// `job_execution_inserted` only arms the hint for a type this process
    /// actually polls -- an unpolled type's report must not linger.
    #[tokio::test]
    async fn job_execution_inserted_arms_the_due_hint_for_polled_types_only() {
        let tracker = JobTracker::new(0, 10);
        let polled = JobType::from_owned("due-hint-polled".to_string());
        let unpolled = JobType::from_owned("due-hint-unpolled".to_string());
        tracker.set_job_types(vec![polled.clone()]);

        tracker.job_execution_inserted(unpolled.as_str());
        assert!(
            !tracker.consume_due_hint(&unpolled),
            "an unpolled type's report must not arm the hint"
        );

        tracker.job_execution_inserted(polled.as_str());
        assert!(
            tracker.consume_due_hint(&polled),
            "a polled type's report must arm the hint"
        );
    }

    #[tokio::test]
    async fn dispatch_job_tracks_units_in_flight_per_type() {
        let tracker = JobTracker::new(0, 10);
        let type_a = JobType::from_owned("units-in-flight-a".to_string());
        let type_b = JobType::from_owned("units-in-flight-b".to_string());

        let id1 = JobId::new();
        let id2 = JobId::new();
        let id3 = JobId::new();
        tracker.dispatch_job(id1, &type_a);
        tracker.dispatch_job(id2, &type_a);
        tracker.dispatch_job(id3, &type_b);

        assert_eq!(tracker.units_in_flight(&type_a), 2);
        assert_eq!(tracker.units_in_flight(&type_b), 1);

        tracker.job_completed(id1, &type_a, false);
        assert_eq!(tracker.units_in_flight(&type_a), 1);

        tracker.job_completed(id2, &type_a, false);
        assert_eq!(
            tracker.units_in_flight(&type_a),
            0,
            "the entry is dropped once its count reaches zero"
        );
    }

    /// Without the capped-type notify rule this would time out — a
    /// single completion never crosses `min_jobs` (set high here) or carries
    /// `rescheduled`, so the capped-type check is the only thing that can
    /// wake the poll loop.
    #[tokio::test]
    async fn job_completed_notifies_for_capped_type_below_min_jobs() {
        let tracker = JobTracker::new(10, 20);
        let capped = JobType::from_owned("capped-notify".to_string());
        tracker.set_capped_types(HashSet::from([capped.clone()]));

        let id = JobId::new();
        tracker.dispatch_job(id, &capped);

        let notified = tracker.notified();
        tracker.job_completed(id, &capped, false);

        tokio::time::timeout(std::time::Duration::from_millis(100), notified)
            .await
            .expect("a freed slot on a capped type must wake the poll loop");
    }

    /// The two `UnitReservation` release paths differ in exactly one
    /// observable: `release()` is QUIET (used by `ClaimHook::pre_commit`'s
    /// budget truncation, where a wake would let a woken poll over-admit
    /// against headroom the hook's surviving claims are about to consume),
    /// while dropping unresolved wakes the poll loop (load-bearing for the
    /// gate's zero-budget skip, where the freed unit otherwise sits
    /// invisible until the idle-poll fallback). Both must release the unit.
    #[tokio::test]
    async fn reservation_release_is_quiet_but_unresolved_drop_wakes() {
        let tracker = Arc::new(JobTracker::new(0, 10));
        let job_type = JobType::from_owned("quiet-release".to_string());

        tracker.dispatch_job(JobId::new(), &job_type);
        tracker.dispatch_job(JobId::new(), &job_type);
        assert_eq!(tracker.units_in_flight(&job_type), 2);

        tracker.recycle(&job_type).release();
        assert_eq!(
            tracker.units_in_flight(&job_type),
            1,
            "release() must free the unit"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), tracker.notified())
                .await
                .is_err(),
            "release() must NOT wake the poll loop"
        );

        drop(tracker.recycle(&job_type));
        assert_eq!(
            tracker.units_in_flight(&job_type),
            0,
            "drop must free the unit"
        );
        tokio::time::timeout(std::time::Duration::from_millis(100), tracker.notified())
            .await
            .expect("an unresolved drop must wake the poll loop");
    }
}
