use tokio::sync::Notify;

use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct JobTracker {
    min_jobs: usize,
    max_jobs: usize,
    running_jobs: AtomicUsize,
    notify: Notify,
}

impl JobTracker {
    pub fn new(min_jobs: usize, max_jobs: usize) -> Self {
        Self {
            min_jobs,
            max_jobs,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
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

    pub fn dispatch_job(&self) {
        self.running_jobs.fetch_add(1, Ordering::SeqCst);
    }

    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub fn job_completed(&self, _rescheduled: bool) {
        self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        // Without PgListener (LISTEN/NOTIFY), always wake the poller so it
        // can pick up queued jobs that may have been blocked by this one.
        self.notify.notify_one();
    }
}
