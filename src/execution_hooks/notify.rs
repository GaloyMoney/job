use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use es_entity::operation::hooks::{CommitHook, HookOperation, PreCommitRet};

use crate::JobId;
use crate::entity::JobType;
use crate::notifier::JobEventNotifier;

use super::insert::ExecutionInsertHook;
use super::promote::PromoteHeadsHook;

/// A standalone, always-merging notify hook: every phase of a spawn/claim
/// commit pass contributes `added`/`claimed`/`forces` re-entrantly, from its
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
/// **Per-row-id attribution, not counts**: a type's suppression depends on
/// this exact question -- did this pass's own claim cover EVERY due-now row
/// `added` this pass, specifically? A count comparison (`added.len() >
/// claimed.len()`) is unsound: `ClaimHook` always claims a type's OLDEST due
/// row via `claim_due_heads_in_op`, which can be a PRE-EXISTING backlog row
/// rather than the one THIS pass just landed. If older backlog already
/// existed for the type, a claim can satisfy the count while leaving this
/// pass's own new row un-claimed and un-notified -- silently stuck until the
/// next poll wake, however far off `next_due_at` is. Comparing `added` and
/// `claimed` as PER-TYPE SETS OF IDS closes this: a type notifies unless
/// every id in `added[type]` is also in `claimed[type]`.
///
/// **`forces` exists because not every notify-worthy event is claim-shaped**:
/// a landed row that isn't due yet, a promoted sibling (which the SAME op's
/// head-swap claim may or may not reach -- claiming targets the type's
/// oldest due row, not specifically the promoted one, and `PromotedRow`
/// carries no id to attribute against anyway), and a pinned `pending`
/// occupant a spawn's `FOR KEY SHARE` made a concurrent poll skip
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
/// no special casing: no `ClaimHook` registers, so `claimed` stays empty and
/// every `added` entry is un-covered, so it fires -- today's behavior,
/// unchanged.
pub(crate) struct ExecutionReadyNotifyHook {
    pub(crate) notifier: Arc<JobEventNotifier>,
    pub(crate) added: HashMap<JobType, HashSet<JobId>>,
    pub(crate) claimed: HashMap<JobType, HashSet<JobId>>,
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
        added: HashMap<JobType, HashSet<JobId>>,
        claimed: HashMap<JobType, HashSet<JobId>>,
        forces: HashSet<JobType>,
    ) {
        if added.is_empty() && claimed.is_empty() && forces.is_empty() {
            return;
        }
        let hook = ExecutionReadyNotifyHook {
            notifier: Arc::clone(notifier),
            added,
            claimed,
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
        for (job_type, added_ids) in self.added.drain() {
            let claimed_ids = self.claimed.get(&job_type);
            let fully_covered =
                claimed_ids.is_some_and(|claimed_ids| added_ids.is_subset(claimed_ids));
            if !fully_covered {
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
        for (job_type, ids) in other.added.drain() {
            self.added.entry(job_type).or_default().extend(ids);
        }
        for (job_type, ids) in other.claimed.drain() {
            self.claimed.entry(job_type).or_default().extend(ids);
        }
        self.forces.extend(other.forces.drain());
        true
    }

    fn runs_after(&self) -> &[std::any::TypeId] {
        &Self::RUNS_AFTER
    }
}
