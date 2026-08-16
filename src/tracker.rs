use tokio::sync::Notify;

use std::collections::{HashMap, HashSet};
use std::sync::{
    Mutex, OnceLock,
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

    /// Wake the poll loop if this process polls `job_type`. `notify_one` holds
    /// at most one permit, so repeated reports collapse into one wake-up.
    pub fn job_execution_inserted(&self, job_type: &str) {
        let Some(job_types) = self.job_types.get() else {
            return;
        };
        if job_types.iter().any(|jt| jt.as_str() == job_type) {
            self.notify.notify_one();
        }
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

    /// D7: without the capped-type notify rule this would time out — a
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
}
