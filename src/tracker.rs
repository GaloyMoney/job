use tokio::sync::{Notify, mpsc};

use std::collections::HashMap;
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

/// Ref-counted set of jobs with a live runner future on this instance, owned by
/// the keep-alive handler.
///
/// A single job id can have more than one live runner at once on the same
/// instance: a self-reclaim (the lost-handler reclaiming a stale-but-still-live
/// row) lets the poller re-dispatch a job while its previous future is still
/// running. A plain set would drop the id on the *first* `Finished`, leaving the
/// other still-live runner un-heartbeated. Counting keeps the id alive until the
/// last runner finishes.
#[derive(Default)]
pub(crate) struct LiveJobs {
    counts: HashMap<JobId, usize>,
}

impl LiveJobs {
    pub fn apply(&mut self, event: LiveJobEvent) {
        match event {
            LiveJobEvent::Started(id) => {
                *self.counts.entry(id).or_insert(0) += 1;
            }
            LiveJobEvent::Finished(id) => {
                // `get_mut` guards an unmatched `Finished` (a runner that errored
                // before it registered): decrement nothing rather than evict a
                // different runner's still-live id.
                if let Some(n) = self.counts.get_mut(&id) {
                    *n -= 1;
                    if *n == 0 {
                        self.counts.remove(&id);
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// The distinct live job ids as raw UUIDs, for binding to `id = ANY($n)`.
    pub fn ids(&self) -> Vec<uuid::Uuid> {
        self.counts.keys().map(|id| uuid::Uuid::from(*id)).collect()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_id_until_last_runner_finishes() {
        let id = JobId::new();
        let mut live = LiveJobs::default();

        // Two concurrent runners for the same id (self-reclaim re-dispatch).
        live.apply(LiveJobEvent::Started(id));
        live.apply(LiveJobEvent::Started(id));
        assert_eq!(live.len(), 1);

        // First runner finishes: the id must stay live for the second.
        live.apply(LiveJobEvent::Finished(id));
        assert!(!live.is_empty(), "id dropped while a runner is still live");
        assert_eq!(live.ids().len(), 1);

        // Second runner finishes: only now is the id removed.
        live.apply(LiveJobEvent::Finished(id));
        assert!(live.is_empty());
    }

    #[test]
    fn ignores_unmatched_finished() {
        let id = JobId::new();
        let mut live = LiveJobs::default();
        live.apply(LiveJobEvent::Finished(id));
        assert!(live.is_empty());
    }
}
