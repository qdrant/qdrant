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

use common::counter::hardware_accumulator::HwMeasurementAcc;
use shard::resolve::resolve_operation;
use tokio::sync::oneshot;

use crate::operations::OperationWithClockTag;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::LocalShard;
use crate::shards::local_shard::shard_ops::SubmitOutcome;
use crate::shards::shard_trait::WaitUntil;
use crate::update_handler::UpdateSignal;

#[cfg(test)]
impl LocalShard {
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
        let resolved = tokio::task::spawn_blocking(move || {
            let segments = segments.read();
            resolve_operation(&segments, operation, &hw_acc.get_counter_cell())
        })
        .await??;

        // 4. Append + dispatch, still inside the fence so no foreign
        // operation can slip into the WAL between resolution and the append.
        self.append_and_dispatch(
            OperationWithClockTag::new(resolved, clock_tag),
            wait,
            hw_measurement_acc,
        )
        .await
    }
}
