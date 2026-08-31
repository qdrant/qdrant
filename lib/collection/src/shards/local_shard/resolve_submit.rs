//! Fenced submission of filter/condition-resolving update operations.
//!
//! Filter-carrying operations must never reach the WAL: they are rewritten to
//! their id-based form at submit time, so WAL replay applies the exact same
//! point set as the original run (issue #9575). Every replica resolves the
//! operation against its own state; replicas that hold the same data resolve
//! the same filter to the same point set.
//!
//! Resolution is only sound when the scanned segment state reflects exactly
//! the operations that precede the rewritten one in WAL order. The fence
//! reconstructs that guarantee at submit time:
//!
//! 1. `update_lock.write()` — waits out in-flight submits (they hold `read`
//!    across their append+enqueue) and blocks new ones.
//! 2. A `Plunger` through the update queue — all already-appended operations
//!    are applied once it is answered.
//! 3. Resolve the filter against segments and rewrite the operation.
//! 4. Append + dispatch the rewritten operation; only then release the fence.
//!
//! The WAL lock is deliberately *not* held across the drain: the update
//! worker takes it to re-read overflowed operations and to flush for
//! `wait=true` operations — holding it here would deadlock.
//!
//! The resolved operation is always **one** WAL record, however many points
//! the filter matched: it reuses the incoming operation's single clock tag,
//! and one tag can cover only one record — untagged or tag-sharing records
//! break WAL-delta recovery. Splitting oversized resolutions is a follow-up.
//!
//! `max_update_by_filter_limit` (strict mode) is enforced here, on the exact
//! point set the scan resolved to. Each shard gates its own share of the
//! request: the check runs after the filter is resolved and before the WAL
//! append, so a rejected operation leaves no trace on this shard. Only filter
//! scans are gated, and only clients issue those: internal operations resolve
//! no filters, so they need no exemption.

use common::counter::hardware_accumulator::HwMeasurementAcc;
use shard::resolve::{ResolvedOperation, resolve_operation};
use tokio::sync::oneshot;

use crate::operations::OperationWithClockTag;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::LocalShard;
use crate::shards::local_shard::shard_ops::SubmitOutcome;
use crate::shards::shard_trait::WaitUntil;
use crate::update_handler::UpdateSignal;

#[cfg(test)]
impl LocalShard {
    /// Test helper: append a record to the WAL as-is, bypassing submit-time
    /// resolution. Emulates a WAL written before filter operations were
    /// resolved at submit (old-version WALs).
    pub(crate) async fn append_raw_wal_operation(&self, operation: &OperationWithClockTag) -> u64 {
        let record =
            shard::wal::WalRawRecord::new(operation).expect("failed to serialize WAL record");
        self.wal
            .wal
            .lock()
            .await
            .write(&record)
            .expect("failed to write WAL record")
    }

    /// Test helper: read every record currently in the WAL.
    pub(crate) async fn read_all_wal_operations(&self) -> Vec<(u64, OperationWithClockTag)> {
        let wal = self.wal.wal.lock().await;
        let from = wal.first_index();
        let to = from + wal.len(false);
        wal.read_range(from..to)
            .map(|entry| entry.expect("failed to read WAL record"))
            .collect()
    }
}

impl LocalShard {
    /// Resolve a filter-resolving operation to its id-based form under the
    /// update fence and append + dispatch it as a single WAL record reusing
    /// the operation's clock tag.
    pub(super) async fn submit_update_filter_resolving(
        &self,
        operation: OperationWithClockTag,
        wait: WaitUntil,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<SubmitOutcome> {
        let OperationWithClockTag {
            operation,
            clock_tag,
        } = operation;

        self.check_wal_disk_space().await?;

        let update_by_filter_limit = self.max_update_by_filter_limit().await;

        // 1. Fence: block new submits; in-flight ones (holding `read`) have
        // already appended and enqueued by the time `write` is granted.
        let _fence = self.update_lock.write().await;

        // 2. Drain: everything appended so far is applied once the plunger
        // is answered, so resolution sees exactly the ops that precede this
        // one in WAL order.
        let (plunger_sender, plunger_receiver) = oneshot::channel();
        self.update_sender
            .load()
            .send(UpdateSignal::Plunger(plunger_sender))
            .await?;
        plunger_receiver.await.map_err(|_| {
            CollectionError::service_error(
                "Can't resolve filter operation: update worker stopped before draining the queue",
            )
        })?;

        // 3. Resolve the filter against segment state and rewrite the
        // operation to its id-based form.
        let segments = self.segments.clone();
        let hw_acc = hw_measurement_acc.clone();
        let ResolvedOperation {
            operation: resolved,
            scanned_points,
        } = tokio::task::spawn_blocking(move || {
            let segments = segments.read();
            resolve_operation(&segments, operation, &hw_acc.get_counter_cell())
        })
        .await??;

        // Gate on the exact number of points the scan selected in this shard,
        // before anything is written.
        if let Some(limit) = update_by_filter_limit
            && let Some(matched) = scanned_points
            && matched > limit
        {
            return Err(update_by_filter_limit_error(matched, limit));
        }

        // Guard against `is_filter_resolving` and `resolve_operation` drifting
        // apart: a resolved operation must never classify as filter-resolving,
        // or a filter could reach the WAL again (issue #9575).
        debug_assert!(
            !shard::resolve::is_filter_resolving(&resolved),
            "resolve_operation left a filter-resolving operation unresolved: {resolved:?}",
        );

        // 4. Append + dispatch, still inside the fence so no foreign
        // operation can slip into the WAL between resolution and the append.
        self.append_and_dispatch(
            OperationWithClockTag::new(resolved, clock_tag),
            wait,
            hw_measurement_acc,
        )
        .await
    }

    /// Configured `max_update_by_filter_limit`, if strict mode is enabled.
    async fn max_update_by_filter_limit(&self) -> Option<usize> {
        self.collection_config
            .read()
            .await
            .strict_mode_config
            .as_ref()
            .filter(|config| config.enabled == Some(true))
            .and_then(|config| config.max_update_by_filter_limit)
    }
}

/// Reject an update whose filter selected more points than strict mode allows,
/// pointing at the `slice` condition as the way to split it up.
fn update_by_filter_limit_error(matched: usize, limit: usize) -> CollectionError {
    // `matched / limit` slices only make the *average* slice fit, and slices
    // are hash-based rather than balanced, so suggest twice that as a starting
    // point. Another shard may have matched more points than this one, so the
    // suggestion can still fall short; the message says so.
    let slices = (2 * matched).div_ceil(limit.max(1));

    CollectionError::strict_mode(
        format!(
            "Update by filter matches {matched} points in one shard, \
             exceeding the per-shard limit of {limit}",
        ),
        format!(
            "Split the update into disjoint slices and send one request per slice: add \
             `{{\"slice\": {{\"total\": {slices}, \"index\": 0}}}}` to your filter and repeat \
             it for every index in `0..{slices}`. Together the slices cover all matching points. \
             Slices are hash-based and not exactly balanced, so raise `total` further if a slice \
             is still rejected. Alternatively, raise `max_update_by_filter_limit` in the strict \
             mode config.",
        ),
    )
}
