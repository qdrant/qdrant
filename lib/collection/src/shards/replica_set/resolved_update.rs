//! Coordinator-side dispatch of filter/condition-resolving update operations.
//!
//! A filter-resolving operation (delete-by-filter, conditional upsert, the
//! `*-by-filter` payload/vector ops) decides which points it touches by
//! reading live data. Persisting the filter makes WAL replay nondeterministic
//! (issue #9575). So the coordinating replica — the one executing this
//! dispatch, which must hold a plain updatable local shard — resolves the
//! operation once under its local shard's update fence into a single
//! id-based record, and forwards that resolved record to all updatable
//! remotes, so every replica applies the same point set.

use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::FutureExt as _;
use tokio::sync::RwLockReadGuard;

use super::clock_set::ClockGuard;
use super::update::run_update_futures;
use super::{RemoteShard, ShardReplicaSet};
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult, UpdateStatus};
use crate::operations::{ClockTag, CollectionUpdateOperations, OperationWithClockTag};
use crate::shards::local_shard::resolve_submit::ResolvedSubmit;
use crate::shards::local_shard::shard_ops::{SubmitOutcome, await_update_result};
use crate::shards::shard::{PeerId, Shard};
use crate::shards::shard_trait::{ShardOperation as _, WaitUntil};

/// Per-replica results, same shape as the regular `update_impl` dispatch.
type ReplicaResults = Vec<Result<(PeerId, UpdateResult), (PeerId, CollectionError)>>;

impl ShardReplicaSet {
    /// Resolve a filter-resolving operation on this (coordinating) replica
    /// and dispatch the resulting id-based operation to the local shard and
    /// all updatable remotes.
    ///
    /// The caller must have verified that `local` holds a plain, updatable
    /// [`Shard::Local`]. Consumes the `local` read guard and drops it as soon
    /// as the local submission completed, before awaiting apply results.
    ///
    /// Returns the operation's (corrected) clock tag for the caller's
    /// clock-echo bookkeeping, and one result per replica.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn dispatch_resolved_update(
        &self,
        operation: CollectionUpdateOperations,
        local: RwLockReadGuard<'_, Option<Shard>>,
        updatable_remote_shards: Vec<RemoteShard>,
        wait: WaitUntil,
        local_wait: WaitUntil,
        timeout: Option<Duration>,
        clock: &mut ClockGuard,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<(ClockTag, ReplicaResults)> {
        let this_peer_id = self.this_peer_id();

        // Rate-limit against the original operation: its strict-mode cost is
        // estimated from the filter, which is about to be resolved away.
        let operation = if self.peer_is_write_rate_limitable(this_peer_id)
            && let Some(shard) = local.as_ref()
        {
            let wrapped = OperationWithClockTag::new(operation, None);
            self.check_operation_write_rate_limiter(&hw_measurement_acc, shard, &wrapped)
                .await?;
            wrapped.operation
        } else {
            operation
        };

        let Some(Shard::Local(local_shard)) = local.as_ref() else {
            return Err(CollectionError::service_error(
                "Resolved update dispatch requires a plain local shard",
            ));
        };

        // Tick once, exactly as the regular path does: the resolved
        // operation is a single record carrying this one tag.
        let current_clock_tick = clock.tick_once();
        let clock_tag = ClockTag::new(this_peer_id, clock.id() as _, current_clock_tick);

        // Resolve + append under the local shard's update fence.
        let ResolvedSubmit { operation, outcome } = local_shard
            .resolve_and_submit(
                operation,
                Some(clock_tag),
                local_wait,
                hw_measurement_acc.clone(),
            )
            .await?;
        drop(local);

        let representative_tag = operation.clock_tag.unwrap_or(clock_tag);

        // Forward the resolved operation only if it was actually appended;
        // on a clock rejection nothing was applied anywhere and the caller
        // retries the whole original operation with an advanced clock.
        let forward_operation = match &outcome {
            SubmitOutcome::Submitted { .. } => Some(operation),
            SubmitOutcome::ClockRejected { .. } => None,
        };

        let local_update = async move {
            await_update_result(outcome, timeout)
                .await
                .map(|ok| (this_peer_id, ok))
                .map_err(|err| (this_peer_id, err))
        };

        let remote_futures: Vec<_> = updatable_remote_shards
            .into_iter()
            .map(|remote| {
                let forward_operation = forward_operation.clone();
                let hw_acc = hw_measurement_acc.clone();
                async move {
                    let peer_id = remote.peer_id;
                    match forward_operation {
                        Some(operation) => remote
                            .update(operation, wait, timeout, hw_acc)
                            .await
                            .map(|ok| (peer_id, ok))
                            .map_err(|err| (peer_id, err)),
                        // Local clock rejection: nothing to forward; echo the
                        // rejection so the caller retries.
                        None => Ok((
                            peer_id,
                            UpdateResult {
                                operation_id: None,
                                status: UpdateStatus::ClockRejected,
                                clock_tag: None,
                            },
                        )),
                    }
                }
            })
            .collect();

        let mut update_futures = Vec::with_capacity(remote_futures.len() + 1);
        update_futures.push(local_update.left_future());
        update_futures.extend(remote_futures.into_iter().map(|f| f.right_future()));

        let update_concurrency = self.shared_storage_config.update_concurrency;
        let all_res = run_update_futures(update_futures, update_concurrency).await;

        Ok((representative_tag, all_res))
    }
}
