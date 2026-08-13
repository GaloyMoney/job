use tokio::sync::Notify;

use std::collections::HashMap;
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
    /// Batches currently executing, per job type. Read by the poller to size
    /// each batched type's claim so it never locks rows no batch can start on.
    batches_in_flight: Mutex<HashMap<JobType, usize>>,
    /// The job types this process polls for. Readiness reports from every
    /// source are filtered against these. Unset until polling starts, so
    /// earlier reports are dropped rather than queued.
    job_types: OnceLock<Vec<JobType>>,
}

impl JobTracker {
    pub fn new(min_jobs: usize, max_jobs: usize) -> Self {
        Self {
            min_jobs,
            max_jobs,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
            live_jobs: Mutex::new(LiveJobs::default()),
            batches_in_flight: Mutex::new(HashMap::new()),
            job_types: OnceLock::new(),
        }
    }

    /// Called once, when polling starts.
    pub fn set_job_types(&self, job_types: Vec<JobType>) {
        let _ = self.job_types.set(job_types);
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

    pub fn dispatch_job(&self, id: JobId) {
        self.running_jobs.fetch_add(1, Ordering::SeqCst);
        self.live_jobs
            .lock()
            .expect("live_jobs poisoned")
            .started(id);
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
            .batches_in_flight
            .lock()
            .expect("batches_in_flight poisoned")
            .entry(job_type.clone())
            .or_insert(0) += 1;
    }

    /// Batches of `job_type` executing right now. The poller subtracts this
    /// from the type's slot count to decide how many rows it may claim.
    pub fn batches_in_flight(&self, job_type: &JobType) -> usize {
        self.batches_in_flight
            .lock()
            .expect("batches_in_flight poisoned")
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

    pub fn job_completed(&self, id: JobId, rescheduled: bool) {
        let n_running_jobs = self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        self.live_jobs
            .lock()
            .expect("live_jobs poisoned")
            .finished(id);
        if rescheduled || n_running_jobs == self.min_jobs {
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
        {
            let mut counts = self
                .batches_in_flight
                .lock()
                .expect("batches_in_flight poisoned");
            if let Some(n) = counts.get_mut(job_type) {
                *n -= 1;
                if *n == 0 {
                    counts.remove(job_type);
                }
            }
        }
        self.notify.notify_one();
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
}
