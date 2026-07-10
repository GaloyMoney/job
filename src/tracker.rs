use tokio::sync::Notify;

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::JobId;

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
}

impl JobTracker {
    pub fn new(min_jobs: usize, max_jobs: usize) -> Self {
        Self {
            min_jobs,
            max_jobs,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
            live_jobs: Mutex::new(LiveJobs::default()),
        }
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

    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub fn job_execution_inserted(&self) {
        self.notify.notify_one()
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
