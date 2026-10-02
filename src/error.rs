//! Error type returned by the job service and helpers.

use super::entity::JobType;
use super::repo::JobConstraintViolation;
use crate::JobId;

use es_entity::errlanes;
use es_entity::errlanes::{Fail, lanes};

#[derive(Debug, errlanes::Rejection, errlanes::Lift)]
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

/// A value the job service was asked to persist did not serialize. Named
/// per payload rather than delegating to `serde_json::Error`'s own
/// classification, so the operator-facing chain says *which* value failed:
/// the whole variant is the `Fatal`'s source and the `serde_json::Error` is
/// the variant's, which keeps the serde detail exactly once.
///
/// `Fatal(Invariant)` because `to_value` fails on properties of the type the
/// job implementor wrote -- a map with non-string keys, a `Serialize` impl
/// that errors -- which no caller can correct and no retry can fix.
#[derive(Debug, errlanes::Classify)]
pub enum CouldNotSerialize {
    #[error("could not serialize job config")]
    #[classify(fatal(Invariant))]
    Config(#[source] serde_json::Error),
    #[error("could not serialize job execution state")]
    #[classify(fatal(Invariant))]
    ExecutionState(#[source] serde_json::Error),
    #[error("could not serialize job return value")]
    #[classify(fatal(Invariant))]
    ReturnValue(#[source] serde_json::Error),
}

/// Persisted execution state that no longer decodes into the type
/// [`JobHandle::execution_state`](crate::JobHandle::execution_state) was
/// asked for.
///
/// Pins `Fatal(CorruptState)` instead of taking `serde_json::Error`'s
/// `Fatal(Invariant)` default: these bytes came out of the store, so an
/// operator needs to look at the row, not at the code. The same call
/// errlanes' own hydration path makes for a persisted event or snapshot.
#[derive(Debug, errlanes::Classify)]
#[error("could not deserialize job execution state")]
#[classify(fatal(CorruptState))]
pub struct CouldNotDeserializeExecutionState(#[source] pub(crate) serde_json::Error);

/// The embedded-migration failure from [`Jobs::init`](crate::Jobs::init).
#[derive(Debug, errlanes::Classify)]
#[error("job service migration failed")]
#[classify(fatal(Config), from)]
pub(crate) struct Migrate(sqlx::migrate::MigrateError);

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

#[cfg(test)]
mod tests {
    use super::*;
    use es_entity::errlanes::{Fail, FatalKind};
    use std::{collections::HashMap, error::Error as _};

    /// A `to_value` failure that depends on nothing but the type: a map
    /// whose key is not a string.
    fn encode_failure() -> serde_json::Error {
        let mut unencodable = HashMap::new();
        unencodable.insert((1u8, 2u8), 3u8);
        serde_json::to_value(&unencodable).expect_err("non-string map key must not encode")
    }

    /// The reason these variants exist instead of a bare `?`: the operator's
    /// chain names the payload, and the serde detail appears exactly once
    /// below it.
    #[test]
    fn a_serialize_failure_is_fatal_invariant_naming_its_payload() {
        let err: JobError = CouldNotSerialize::ExecutionState(encode_failure()).into();
        match err {
            Fail::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::Invariant);
                let wrapper = fatal.source().expect("the variant is the Fatal's source");
                assert_eq!(
                    wrapper.to_string(),
                    "could not serialize job execution state"
                );
                let serde = wrapper
                    .source()
                    .expect("serde error is the variant's source");
                assert!(serde.to_string().contains("key must be a string"));
            }
            other => panic!("expected Fatal(Invariant), got {other:?}"),
        }
    }

    /// A persisted row that no longer decodes is stored-data corruption, not
    /// a code-level invariant -- the one place job overrides errlanes' serde
    /// default, which classifies every decode failure as `Invariant`.
    #[test]
    fn a_persisted_state_decode_failure_is_fatal_corrupt_state() {
        let decode_failure =
            serde_json::from_value::<String>(serde_json::json!({})).expect_err("object is not str");
        let err: JobError = CouldNotDeserializeExecutionState(decode_failure).into();
        match err {
            Fail::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::CorruptState);
                let wrapper = fatal.source().expect("the wrapper is the Fatal's source");
                assert_eq!(
                    wrapper.to_string(),
                    "could not deserialize job execution state"
                );
                assert!(wrapper.source().is_some(), "serde error stays in the chain");
            }
            other => panic!("expected Fatal(CorruptState), got {other:?}"),
        }
    }
}
