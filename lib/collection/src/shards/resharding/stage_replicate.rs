use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tokio::time::sleep;

use super::driver::{PersistedState, Stage};
use super::tasks_pool::ReshardTaskProgress;
use super::ReshardKey;
use crate::config::CollectionConfigInternal;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::resharding::driver::{
    await_transfer_success, SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL,
};
use crate::shards::shard_holder::LockedShardHolder;
use crate::shards::transfer::{ShardTransfer, ShardTransferConsensus};
use crate::shards::CollectionId;

/// Maximum time a shard replication transfer might take.
const REPLICATE_TRANSFER_MAX_DURATION: Duration = Duration::from_secs(24 * 60 * 60);

/// Stage 3: replicate to match replication factor
///
/// Check whether we need to replicate to match replication factor.
pub(super) async fn is_completed(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    shard_holder: &Arc<LockedShardHolder>,
    collection_config: &Arc<RwLock<CollectionConfigInternal>>,
) -> CollectionResult<bool> {
    Ok(state.read().all_peers_completed(Stage::S3_Replicate)
        && has_enough_replicas(reshard_key, shard_holder, collection_config).await?)
}

/// Stage 3: replicate to match replication factor
///
/// Do replicate replicate to match replication factor.
#[allow(clippy::too_many_arguments)]
pub(super) async fn drive(
    reshard_key: &ReshardKey,
    state: &PersistedState,
    progress: &Mutex<ReshardTaskProgress>,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: &CollectionId,
    collection_config: Arc<RwLock<CollectionConfigInternal>>,
    shared_storage_config: &SharedStorageConfig,
) -> CollectionResult<()> {
    let this_peer_id = consensus.this_peer_id();

    while !has_enough_replicas(reshard_key, &shard_holder, &collection_config).await? {
        // Find peer candidates to replicate to
        let candidate_peers = {
            let incoming_limit = shared_storage_config
                .incoming_shard_transfers_limit
                .unwrap_or(usize::MAX);
            let outgoing_limit = shared_storage_config
                .outgoing_shard_transfers_limit
                .unwrap_or(usize::MAX);

            // Ensure we don't exceed the outgoing transfer limits
            let shard_holder = shard_holder.read().await;
            let (_, outgoing) = shard_holder.count_shard_transfer_io(this_peer_id);
            if outgoing >= outgoing_limit {
                log::trace!("Postponing resharding replication transfer to stay below transfer limit (outgoing: {outgoing})");
                sleep(SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL).await;
                continue;
            }

            // Select peers that don't have this replica yet
            let Some(replica_set) = shard_holder.get_shard(reshard_key.shard_id) else {
                return Err(CollectionError::service_error(format!(
                    "Shard {} not found in the shard holder for resharding",
                    reshard_key.shard_id,
                )));
            };
            let occupied_peers = replica_set.peers().into_keys().collect();
            let all_peers = consensus.peers().into_iter().collect::<HashSet<_>>();
            let candidate_peers: Vec<_> = all_peers
                .difference(&occupied_peers)
                .map(|peer_id| (*peer_id, shard_holder.count_peer_shards(*peer_id)))
                .collect();
            if candidate_peers.is_empty() {
                log::warn!("Resharding could not match desired replication factors as all peers are occupied, continuing with lower replication factor");
                break;
            };

            // To balance, only keep candidates with lowest number of shards
            // Peers must have room for an incoming transfer
            let lowest_shard_count = *candidate_peers
                .iter()
                .map(|(_, count)| count)
                .min()
                .unwrap();
            let candidate_peers: Vec<_> = candidate_peers
                .into_iter()
                .filter(|&(peer_id, shard_count)| {
                    let (incoming, _) = shard_holder.count_shard_transfer_io(peer_id);
                    lowest_shard_count == shard_count && incoming < incoming_limit
                })
                .map(|(peer_id, _)| peer_id)
                .collect();
            if candidate_peers.is_empty() {
                log::trace!("Postponing resharding replication transfer to stay below transfer limit on peers");
                sleep(SHARD_TRANSFER_IO_LIMIT_RETRY_INTERVAL).await;
                continue;
            };

            candidate_peers
        };

        let target_peer = *candidate_peers.choose(&mut rand::thread_rng()).unwrap();
        let transfer = ShardTransfer {
            shard_id: reshard_key.shard_id,
            to_shard_id: None,
            from: this_peer_id,
            to: target_peer,
            sync: true,
            method: Some(
                shared_storage_config
                    .default_shard_transfer_method
                    .unwrap_or_default(),
            ),
        };

        // Create listener for transfer end before proposing to start the transfer
        // That way we're sure we receive all transfer related messages
        let await_transfer_end = shard_holder
            .read()
            .await
            .await_shard_transfer_end(transfer.key(), REPLICATE_TRANSFER_MAX_DURATION);

        consensus
            .start_shard_transfer_confirm_and_retry(&transfer, collection_id)
            .await?;

        // Await transfer success
        await_transfer_success(
            reshard_key,
            &transfer,
            &shard_holder,
            collection_id,
            consensus,
            await_transfer_end,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to replicate shard {} to peer {target_peer} for resharding: {err}",
                reshard_key.shard_id
            ))
        })?;
        log::debug!(
            "Shard {} successfully replicated to peer {target_peer} for resharding",
            reshard_key.shard_id,
        );
    }

    state.write(|data| {
        data.complete_for_all_peers(Stage::S3_Replicate);
        data.update(progress, consensus);
    })?;

    Ok(())
}

/// Check whether we have the desired number of replicas for our new shard.
async fn has_enough_replicas(
    reshard_key: &ReshardKey,
    shard_holder: &Arc<LockedShardHolder>,
    collection_config: &Arc<RwLock<CollectionConfigInternal>>,
) -> CollectionResult<bool> {
    // We don't need to replicate when scaling down
    if reshard_key.direction == ReshardingDirection::Down {
        return Ok(true);
    }

    let desired_replication_factor = collection_config
        .read()
        .await
        .params
        .replication_factor
        .get();
    let current_replication_factor = {
        let shard_holder_read = shard_holder.read().await;
        let Some(replica_set) = shard_holder_read.get_shard(reshard_key.shard_id) else {
            return Err(CollectionError::service_error(format!(
                "Shard {} not found in the shard holder for resharding",
                reshard_key.shard_id,
            )));
        };
        replica_set.peers().len() as u32
    };

    Ok(current_replication_factor >= desired_replication_factor)
}
