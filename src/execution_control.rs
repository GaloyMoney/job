//! Durable, database-wide claim admission and cooperative draining.

use std::time::Duration;

use es_entity::AtomicOperation;
use sqlx::PgPool;

use crate::{JobType, error::JobFault};

pub(crate) const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct ExecutionControl {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
pub(crate) struct AdmissionPolicy {
    paused: bool,
    allowed_job_types: Vec<String>,
}

impl AdmissionPolicy {
    pub(crate) fn allows(&self, job_type: &JobType) -> bool {
        !self.paused
            || self
                .allowed_job_types
                .iter()
                .any(|allowed| allowed == job_type.as_str())
    }
}

/// A point-in-time status under the same admission lock claims take.
/// Pending/parked work is not running and is preserved for resume.
pub struct QuiescenceStatus {
    pub paused: bool,
    pub running: i64,
}

impl QuiescenceStatus {
    pub fn is_quiesced(&self) -> bool {
        self.paused && self.running == 0
    }
}

impl ExecutionControl {
    pub(crate) fn new(pool: &PgPool) -> Self {
        Self { pool: pool.clone() }
    }

    /// Stops new claims except for explicitly allowed infrastructure types.
    /// The row's exclusive update lock waits for admitted claim transactions;
    /// the pause and the application's maintenance state may commit together.
    pub async fn pause_in_op(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
        allowed: &[JobType],
    ) -> Result<(), JobFault> {
        let mut allowed: Vec<String> = allowed.iter().map(ToString::to_string).collect();
        allowed.sort();
        allowed.dedup();
        sqlx::query("UPDATE job_execution_control SET paused = TRUE, allowed_job_types = $1 WHERE id = 1 RETURNING id")
            .bind(allowed)
            .fetch_one(op.as_executor())
            .await?;
        Ok(())
    }

    pub async fn resume_in_op(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
    ) -> Result<(), JobFault> {
        sqlx::query("UPDATE job_execution_control SET paused = FALSE, allowed_job_types = '{}' WHERE id = 1 RETURNING id")
            .fetch_one(op.as_executor())
            .await?;
        Ok(())
    }

    pub async fn status(&self) -> Result<QuiescenceStatus, JobFault> {
        let mut op = es_entity::DbOp::init_with_clock(
            &self.pool,
            &es_entity::clock::ClockHandle::realtime(),
        )
        .await?;
        let status = self.status_in_op(&mut op).await?;
        op.commit().await?;
        Ok(status)
    }

    pub async fn status_in_op(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
    ) -> Result<QuiescenceStatus, JobFault> {
        let policy = self.lock_policy(op).await?;
        let running = sqlx::query_scalar("SELECT COUNT(*) FROM job_executions WHERE state = 'running' AND NOT (job_type = ANY($1))")
            .bind(&policy.allowed_job_types)
            .fetch_one(op.as_executor())
            .await?;
        Ok(QuiescenceStatus {
            paused: policy.paused,
            running,
        })
    }

    pub(crate) async fn lock_policy(
        &self,
        op: &mut (impl AtomicOperation + ?Sized),
    ) -> Result<AdmissionPolicy, sqlx::Error> {
        // FOR SHARE is held until the claim commits. READ COMMITTED returns
        // the updated row after waiting for a concurrent pause/resume writer.
        sqlx::query_as(
            "SELECT paused, allowed_job_types FROM job_execution_control WHERE id = 1 FOR SHARE",
        )
        .fetch_one(op.as_executor())
        .await
    }

    async fn requested(&self, job_type: &JobType) -> Result<bool, JobFault> {
        let policy: AdmissionPolicy = sqlx::query_as(
            "SELECT paused, allowed_job_types FROM job_execution_control WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(!policy.allows(job_type))
    }
}

/// Clone before entering a select with `CurrentJob::shutdown_requested`.
/// This signal never cancels work: long-lived runners must finish their
/// admitted batch, checkpoint, then return a successful reschedule.
#[derive(Clone)]
pub struct QuiescenceSignal {
    pub(crate) control: Option<ExecutionControl>,
    pub(crate) job_type: JobType,
}

impl QuiescenceSignal {
    pub async fn is_requested(&self) -> Result<bool, JobFault> {
        match &self.control {
            Some(control) => control.requested(&self.job_type).await,
            None => Ok(false),
        }
    }

    pub async fn requested(&self) -> Result<(), JobFault> {
        if self.control.is_none() {
            return std::future::pending().await;
        }
        loop {
            if self.is_requested().await? {
                return Ok(());
            }
            tokio::time::sleep(CONTROL_POLL_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_defaults_to_running_and_pause_is_an_explicit_allow_list() {
        let business = JobType::new("business");
        let relay = JobType::new("relay");
        assert!(
            AdmissionPolicy {
                paused: false,
                allowed_job_types: vec![]
            }
            .allows(&business)
        );
        let paused = AdmissionPolicy {
            paused: true,
            allowed_job_types: vec!["relay".into()],
        };
        assert!(!paused.allows(&business));
        assert!(paused.allows(&relay));
    }

    #[test]
    fn only_a_paused_and_drained_service_is_quiesced() {
        assert!(
            !QuiescenceStatus {
                paused: false,
                running: 0
            }
            .is_quiesced()
        );
        assert!(
            !QuiescenceStatus {
                paused: true,
                running: 1
            }
            .is_quiesced()
        );
        assert!(
            QuiescenceStatus {
                paused: true,
                running: 0
            }
            .is_quiesced()
        );
    }
}
