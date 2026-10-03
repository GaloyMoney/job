//! Error types returned by the job service.
//!
//! Lanes, carriers, lifting and narrowing are `errlanes` vocabulary; this
//! module only applies it. For the model itself see the
//! [errlanes README](https://github.com/GaloyMoney/es-entity/blob/main/errlanes/README.md).
//!
//! How job adopts it:
//!
//! - Rejections are scoped to the methods that can produce them, never
//!   pooled service-wide. Only the id-choosing `JobSpawner` methods can
//!   reject (a caller-chosen duplicate id), so only they return
//!   [`JobError`]; the awaits return [`AwaitError`] for their timeout;
//!   everything else cannot reject and returns [`JobFault`].
//! - Foreign error types stay raw on a `pub fn` when they are the only
//!   error that site can produce (the serde accessors on
//!   [`JobOutcome`](crate::JobOutcome), [`JobSnapshot`](crate::JobSnapshot)
//!   and [`CurrentJob`](crate::CurrentJob)). They get a laned wrapper only
//!   where they are folded into a laned result.
//! - Library code never inspects a `Fatal`'s payload and never asks callers
//!   to. Where a caller needs to know, the API offers a value instead (see
//!   [`JobHandle::maybe_load`](crate::JobHandle::maybe_load)).

use super::repo::JobConstraintViolation;
use crate::JobId;

use es_entity::errlanes;
use es_entity::errlanes::{Fail, Fault, lanes};

/// The carrier for every method that cannot reject: lifecycle, handles,
/// awaits' faults, writes from inside a runner, keyed and resident spawns.
pub type JobFault = Fault<lanes!(Transient, Fatal)>;

/// The carrier for the id-choosing spawn paths on
/// [`JobSpawner`](crate::JobSpawner), the only methods that can reject.
pub type JobError = Fail<JobRejection, lanes!(Transient, Fatal)>;

/// The carrier for [`JobHandle::await_completion`](crate::JobHandle::await_completion)
/// and [`JobHandles::await_all`](crate::JobHandles::await_all).
pub type AwaitError = Fail<AwaitTimeout, lanes!(Transient, Fatal)>;

#[derive(Debug, errlanes::Rejection, errlanes::Lift)]
// `JobConstraintViolation` carries a synthetic conventional-name variant per
// column (`{table}_{column}_key`) in addition to the real constraints the
// migrations define, so the mapping here is necessarily partial: only
// `jobs_pkey` maps to a domain rejection; every other, never-fired variant
// demotes to `Fatal(Invariant)` via the unmapped path -- including
// `idx_jobs_job_type_resident`, which `ResidentJobSpawner::spawn`
// (`src/resident.rs`) always absorbs by resolving to the existing job before
// a violation could ever reach here.
#[lift(JobConstraintViolation, unhandled = fatal)]
/// Caller-correctable outcomes the job service can report. Everything else
/// -- infrastructure failures, bugs, exhausted retries -- travels as
/// `Transient`/`Fatal` in [`JobError`] instead of as a variant here.
pub enum JobRejection {
    #[error("duplicate job id")]
    #[rejection(code = "JOB_DUPLICATE_ID")]
    // The id is projected straight out of es_entity's `IdConflict`, which
    // attributes it on every create path (single and batch), so the
    // rejection carries the caller's own id and nothing of the driver's.
    #[lift(JobConstraintViolation::Pkey, field = attempted)]
    DuplicateId(JobId),
}

/// The await's deadline passed before every awaited job went terminal.
/// `pending` is exactly the set still running; one element for
/// [`JobHandle::await_completion`](crate::JobHandle::await_completion).
#[derive(Debug, errlanes::Rejection)]
#[rejection(code = "JOB_AWAIT_TIMED_OUT")]
#[error("await timed out; still pending: {pending:?}")]
pub struct AwaitTimeout {
    pub pending: Vec<JobId>,
}

/// The waiter's notification channel closed before delivering a terminal
/// state. The only producer is the job service stopping
/// ([`Jobs::shutdown`](crate::Jobs::shutdown) or drop) while a handle was
/// awaited.
#[derive(Debug, errlanes::Classify)]
#[error("await of job {id} interrupted: the job service stopped")]
#[classify(fatal(Invariant))]
pub struct AwaitInterrupted {
    pub id: JobId,
}

/// [`JobSvcConfigBuilder::build`](crate::JobSvcConfigBuilder::build)
/// validation failure.
#[derive(Debug, errlanes::Classify)]
#[error("invalid job service config: {message}")]
#[classify(fatal(Config))]
pub struct InvalidConfig {
    pub message: String,
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use es_entity::errlanes::{FatalKind, Fault};
    use std::{collections::HashMap, error::Error as _};

    /// A `to_value` failure that depends on nothing but the type: a map
    /// whose key is not a string.
    fn encode_failure() -> serde_json::Error {
        let mut unencodable = HashMap::new();
        unencodable.insert((1u8, 2u8), 3u8);
        serde_json::to_value(&unencodable).expect_err("non-string map key must not encode")
    }

    /// Same chain contract as the serde wrappers, on the one wrapper whose
    /// payload is a driver error rather than a `serde_json::Error`: the
    /// wrapper names the stage that failed and the `MigrateError` below it
    /// says what actually went wrong. Without that second hop a failed
    /// `Jobs::init` reports only "job service migration failed", which
    /// names no migration and no SQL.
    #[test]
    fn a_migration_failure_is_fatal_config_keeping_the_driver_error() {
        let err: JobFault = Migrate(sqlx::migrate::MigrateError::VersionMissing(7)).into();
        match err {
            Fault::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::Config);
                let wrapper = fatal.source().expect("the wrapper is the Fatal's source");
                assert_eq!(wrapper.to_string(), "job service migration failed");
                let driver = wrapper
                    .source()
                    .expect("the MigrateError is the wrapper's source");
                assert!(
                    driver.to_string().contains("migration 7"),
                    "driver error lost from the chain, got {driver}"
                );
            }
            other => panic!("expected Fatal(Config), got {other:?}"),
        }
    }

    /// The reason these variants exist instead of a bare `?`: the operator's
    /// chain names the payload, and the serde detail appears exactly once
    /// below it.
    #[test]
    fn a_serialize_failure_is_fatal_invariant_naming_its_payload() {
        let err: JobFault = CouldNotSerialize::ExecutionState(encode_failure()).into();
        match err {
            Fault::Fatal(fatal) => {
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
        let err: JobFault = CouldNotDeserializeExecutionState(decode_failure).into();
        match err {
            Fault::Fatal(fatal) => {
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
