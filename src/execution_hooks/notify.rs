use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use es_entity::operation::hooks::{CommitHook, HookOperation, PreCommitRet};

use crate::entity::JobType;
use crate::notifier::JobEventNotifier;

use super::insert::ExecutionInsertHook;
use super::promote::PromoteHeadsHook;

/// A standalone, always-merging notify hook: every phase of a spawn/claim
/// commit pass contributes `adds`/`suppress`/`forces` re-entrantly, from its
/// own `pre_commit`, with the pass's ACTUAL outcomes -- no registration-time
/// guessing -- and this hook fires the net result once, last.
///
/// **Why this exists**: `ExecutionInsertHook` used to notify every type that
/// landed a `pending` row directly, unconditionally -- including rows the
/// SAME transaction's `ClaimHook` immediately self-claims via the head-swap
/// short circuit (`register_claim_demand`, deferred behind this hook by
/// `ClaimHook::RUNS_AFTER`). sb-max8 measured that self-claimed rows were
/// ~4% of what those notifies woke a poll for, at 19-23% of all DB exec
/// time -- a redundant wake whose work was already stolen.
///
/// **Arithmetic, not per-row attribution**: after a merge there is no way to
/// attribute "this add corresponds to THAT claim", so the policy is
/// aggregate per type: notify iff `forces` names the type, or `adds >
/// suppress` (residue exists -- local capacity was short, or claims lost a
/// race to a peer instance). A process with no spare capacity claims 0,
/// residue is the full `adds`, the notify fires and another instance's
/// poller picks the work up -- multi-instance correctness falls straight
/// out of the arithmetic.
///
/// **`forces` exists because not every notify-worthy event is claim-shaped**:
/// a landed row that isn't due yet, a promoted sibling (which the SAME op's
/// head-swap claim may or may not reach -- claiming targets the type's
/// oldest due row, not specifically the promoted one), and a pinned
/// `pending` occupant a spawn's `FOR KEY SHARE` made a concurrent poll skip
/// (`ExecutionInsertHook::lock_queue_occupants`) all need a notify
/// regardless of what this pass's `ClaimHook` did.
///
/// pg `NOTIFY` semantics (delivered at commit, after every lock in this
/// pass releases) are unaffected -- this hook still runs inside
/// `pre_commit`, registering the SAME `execution_ready_in_op` that ran
/// unconditionally before; only the DECISION of whether to call it changes.
///
/// Ops with no claim coverage (a runner-internal `JobSpawner` handed to
/// `init()` has no poller handle, or a `short_circuit() = false` type) need
/// no special casing: no `ClaimHook` registers, so `suppress` stays empty
/// and every `adds` entry fires -- today's behavior, unchanged.
pub(crate) struct ExecutionReadyNotifyHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) adds: HashMap<JobType, usize>,
    pub(crate) suppress: HashMap<JobType, usize>,
    pub(crate) forces: HashSet<JobType>,
}

impl ExecutionReadyNotifyHook {
    /// [`Self::runs_after`]'s dependency list -- see [`ClaimHook::RUNS_AFTER`]
    /// for why an associated const rather than a value built inline.
    /// Over-declared rather than under: a listed type that never registers
    /// on an op, or whose instances already executed, imposes no delay (see
    /// the `es_entity` hook-ordering contract), so there is no cost to
    /// naming every producer here even though `ClaimHook` alone transitively
    /// implies `PromoteHeadsHook` via its OWN `runs_after`.
    ///
    /// [`ClaimHook::RUNS_AFTER`]: crate::poller::ClaimHook::RUNS_AFTER
    const RUNS_AFTER: [std::any::TypeId; 3] = [
        std::any::TypeId::of::<ExecutionInsertHook>(),
        std::any::TypeId::of::<PromoteHeadsHook>(),
        std::any::TypeId::of::<crate::poller::ClaimHook>(),
    ];

    /// Registers one phase's contribution, merging into any still-pending
    /// instance on `op` (or starting a fresh one -- see the `es_entity`
    /// re-entrant-registration contract). `add_commit_hook` can only fail if
    /// `op` carries no commit-hook buffer at all, which cannot happen when
    /// called (as every call site here is) from inside another hook's own
    /// `pre_commit` -- logged rather than force-executed if it ever does:
    /// forcing this one inline would fire (or skip) a notify with no
    /// suppression pass ever running to net it against, which is worse than
    /// simply not registering.
    pub(crate) fn register(
        op: &mut impl es_entity::AtomicOperation,
        notifier: &Arc<JobEventNotifier>,
        adds: HashMap<JobType, usize>,
        suppress: HashMap<JobType, usize>,
        forces: HashSet<JobType>,
    ) {
        if adds.is_empty() && suppress.is_empty() && forces.is_empty() {
            return;
        }
        let hook = ExecutionReadyNotifyHook {
            notifier: Arc::clone(notifier),
            adds,
            suppress,
            forces,
        };
        if op.add_commit_hook(hook).is_err() {
            tracing::error!(
                "execution-ready notify hook could not register; \
                 its contribution is dropped rather than fired unsuppressed or \
                 silently swallowed -- the ordinary poll still covers the type"
            );
        }
    }
}

impl CommitHook for ExecutionReadyNotifyHook {
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let mut types: HashSet<JobType> = std::mem::take(&mut self.forces);
        for (job_type, added) in self.adds.drain() {
            let suppressed = self.suppress.get(&job_type).copied().unwrap_or(0);
            if added > suppressed {
                types.insert(job_type);
            }
        }
        for job_type in types {
            self.notifier
                .execution_ready_in_op(&mut op, &job_type)
                .await?;
        }
        PreCommitRet::ok(self, op)
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        for (job_type, n) in other.adds.drain() {
            *self.adds.entry(job_type).or_insert(0) += n;
        }
        for (job_type, n) in other.suppress.drain() {
            *self.suppress.entry(job_type).or_insert(0) += n;
        }
        self.forces.extend(other.forces.drain());
        true
    }

    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }
}
