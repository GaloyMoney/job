//! Error type returned by the job service and helpers.

use thiserror::Error;

use super::entity::JobType;
use super::repo::JobConstraintViolation;
use crate::JobId;

use es_entity::errlanes;
use es_entity::errlanes::{Fail, lanes};

#[derive(Error, Debug, errlanes::Rejection, errlanes::Lift)]
// `JobConstraintViolation` carries a synthetic conventional-name variant per
// column (`{table}_{column}_key`) in addition to the real constraints the
// migrations define, so the mapping here is necessarily partial: only the
// two constraints that actually exist (`jobs_pkey`,
// `idx_jobs_job_type_resident`) map to a domain rejection; every other,
// never-fired variant demotes to `Fatal(Invariant)` via the unmapped path.
#[lift(JobConstraintViolation, unhandled = fatal)]
/// Caller-correctable outcomes the job service can report. Everything else
/// -- infrastructure failures, bugs, exhausted retries -- travels as
/// `Transient`/`Fatal` in [`JobError`] instead of as a variant here.
pub enum JobRejection {
    #[error("duplicate job id")]
    #[rejection(code = "JOB_DUPLICATE_ID")]
    #[lift(JobConstraintViolation::Pkey)]
    DuplicateId(es_entity::ConstraintConflict<JobId>),
    /// Returned when a resident job type already has a live job (#170).
    #[error("resident job of this type already exists")]
    #[rejection(code = "JOB_DUPLICATE_RESIDENT")]
    #[lift(JobConstraintViolation::IdxJobsJobTypeResident)]
    DuplicateResident(es_entity::ConstraintConflict<JobType>),
    #[error("job {0} did not reach a terminal state within the timeout")]
    #[rejection(code = "JOB_AWAIT_TIMED_OUT")]
    TimedOut(JobId),
    #[error("await of job {0} interrupted: notification channel closed")]
    #[rejection(code = "JOB_AWAIT_INTERRUPTED")]
    AwaitInterrupted(JobId),
}

/// Native errlanes error for the job service: a `Rejected` domain outcome,
/// or a `Transient`/`Fatal` fault. Job never denies.
pub type JobError = Fail<JobRejection, lanes!(Transient, Fatal)>;

/// A `serde_json::Error` raised encoding a value job itself produced
/// (config, execution state, a return value) rather than decoding stored
/// bytes. Deliberately `Fatal(Invariant)`, not the `classify-serde-json`
/// default of `Fatal(CorruptState)`: a value we built that fails to
/// serialize is a bug in the type, not corrupt persisted state.
#[derive(Debug, Error, errlanes::Classify)]
#[error("failed to encode value as JSON: {0}")]
#[classify(fatal(Invariant), from)]
pub struct Encode(#[source] serde_json::Error);

/// The embedded-migration failure from [`Jobs::init`](crate::Jobs::init).
#[derive(Debug, Error, errlanes::Classify)]
#[error("job service migration failed: {0}")]
#[classify(fatal(Config), from)]
pub(crate) struct Migrate(#[source] sqlx::migrate::MigrateError);

/// Total attempts a crate-owned bookkeeping transaction (batch seal / fail,
/// congestion reschedule) gets when Postgres keeps ABORTING it as a
/// deadlock victim or serialization failure --
/// transient aborts where the transaction lost to a concurrent partner and
/// is safe to simply re-run. Counted as attempts, not retries: `3` means
/// the original try plus two re-runs.
///
/// Small on purpose: these aborts are resolved by whichever partner
/// survives, so a re-attempt normally succeeds immediately. If three in a
/// row lose, something is wrong beyond ordinary contention and the work is
/// better off going through the rescue path than spinning here.
pub(crate) const TX_ABORT_MAX_ATTEMPTS: u32 = 3;
