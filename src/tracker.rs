use tokio::sync::{Notify, mpsc};

use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use super::JobId;

/// Lifecycle event for a job future, used to heartbeat only live jobs.
pub(crate) enum LiveJobEvent {
    Started(JobId),
    Finished(JobId),
}

pub(crate) struct JobTracker {
    min_jobs: usize,
    max_jobs: usize,
    running_jobs: AtomicUsize,
    notify: Notify,
    live_events_tx: mpsc::UnboundedSender<LiveJobEvent>,
    live_events_rx: Mutex<Option<mpsc::UnboundedReceiver<LiveJobEvent>>>,
}

impl JobTracker {
    pub fn new(min_jobs: usize, max_jobs: usize) -> Self {
        let (live_events_tx, live_events_rx) = mpsc::unbounded_channel();
        Self {
            min_jobs,
            max_jobs,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
            live_events_tx,
            live_events_rx: Mutex::new(Some(live_events_rx)),
        }
    }

    /// Take exclusive ownership of the event stream (once, at poller startup).
    pub fn take_live_job_events(&self) -> mpsc::UnboundedReceiver<LiveJobEvent> {
        self.live_events_rx
            .lock()
            .expect("live_events_rx poisoned")
            .take()
            .expect("live-job event stream already taken")
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
        let _ = self.live_events_tx.send(LiveJobEvent::Started(id));
    }

    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    pub fn job_execution_inserted(&self) {
        self.notify.notify_one()
    }

    pub fn job_completed(&self, id: JobId, rescheduled: bool) {
        let n_running_jobs = self.running_jobs.fetch_sub(1, Ordering::SeqCst);
        let _ = self.live_events_tx.send(LiveJobEvent::Finished(id));
        if rescheduled || n_running_jobs == self.min_jobs {
            self.notify.notify_one();
        }
    }
}
