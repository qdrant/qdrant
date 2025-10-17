use std::collections::{HashMap, HashSet};

use super::{ShardTransfer, ShardTransferKey, ShardTransferMethod};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;

pub fn validate_transfer_exists(
    transfer_key: &ShardTransferKey,
    current_transfers: &HashSet<ShardTransfer>,
) -> CollectionResult<()> {
    if !current_transfers.iter().any(|t| &t.key() == transfer_key) {
        return Err(CollectionError::bad_request(format!(
            "There is no transfer for shard {} from {} to {}",
            transfer_key.shard_id, transfer_key.from, transfer_key.to,
        )));
    }

    Ok(())
}

pub fn get_transfer(
    transfer_key: &ShardTransferKey,
    current_transfers: &HashSet<ShardTransfer>,
) -> Option<ShardTransfer> {
    current_transfers
        .iter()
        .find(|t| &t.key() == transfer_key)
        .cloned()
}

/// Confirms that the transfer does not conflict with any other active transfers
///
/// returns `None` if there is no conflicts, otherwise returns conflicting transfer
pub fn check_transfer_conflicts<'a, I>(
    transfer: &ShardTransfer,
    current_transfers: I,
) -> Option<ShardTransfer>
where
    I: Iterator<Item = &'a ShardTransfer>,
{
    let res = current_transfers
        .filter(|t| t.shard_id == transfer.shard_id)
        .find(|t| {
            t.from == transfer.from
                || t.to == transfer.from
                || t.from == transfer.to
                || t.to == transfer.to
        });
    res.cloned()
}

/// Same as `check_transfer_conflicts` but doesn't allow transfers to/from the same peer
/// more than once for the whole collection
pub fn check_transfer_conflicts_strict<'a, I>(
    transfer: &ShardTransfer,
    mut current_transfers: I,
) -> Option<ShardTransfer>
where
    I: Iterator<Item = &'a ShardTransfer>,
{
    let res = current_transfers.find(|t| {
        t.from == transfer.from
            || t.to == transfer.from
            || t.from == transfer.to
            || t.to == transfer.to
    });
    res.cloned()
}

/// Confirms that the transfer makes sense with the current state cluster
///
/// Checks:
/// 1. If `from` and `to` exists
/// 2. If `from` have local shard and it is active
/// 3. If there is no active transfers which involve `from` or `to`
/// 4. If a target shard is only set for resharding transfers
///
/// For resharding transfers this also checks:
/// 1. If the source and target shards are different
/// 2. If the source and target shardsd share the same shard key
///
/// If validation fails, return `BadRequest` error.
pub fn validate_transfer(
    transfer: &ShardTransfer,
    all_peers: &HashSet<PeerId>,
    source_replicas: Option<&HashMap<PeerId, ReplicaState>>,
    destination_replicas: Option<&HashMap<PeerId, ReplicaState>>,
    current_transfers: &HashSet<ShardTransfer>,
    shards_key_mapping: &ShardKeyMapping,
) -> CollectionResult<()> {
    let Some(source_replicas) = source_replicas else {
        return Err(CollectionError::service_error(format!(
            "Shard {} does not exist",
            transfer.shard_id,
        )));
    };

    if !all_peers.contains(&transfer.from) {
        return Err(CollectionError::bad_request(format!(
            "Peer {} does not exist",
            transfer.from,
        )));
    }

    if !all_peers.contains(&transfer.to) {
        return Err(CollectionError::bad_request(format!(
            "Peer {} does not exist",
            transfer.to,
        )));
    }

    // We allow transfers *from* `ReshardingScaleDown` replicas, because they contain a *superset*
    // of points in a regular replica
    let is_active = matches!(
        source_replicas.get(&transfer.from),
        Some(ReplicaState::Active | ReplicaState::ReshardingScaleDown),
    );

    if !is_active {
        return Err(CollectionError::bad_request(format!(
            "Shard {} is not active on peer {}",
            transfer.shard_id, transfer.from,
        )));
    }

    if let Some(existing_transfer) = check_transfer_conflicts(transfer, current_transfers.iter()) {
        return Err(CollectionError::bad_request(format!(
            "Shard {} is already involved in transfer {} -> {}",
            transfer.shard_id, existing_transfer.from, existing_transfer.to,
        )));
    }

    if transfer.method == Some(ShardTransferMethod::ReshardingStreamRecords) {
        let Some(destination_replicas) = destination_replicas else {
            return Err(CollectionError::service_error(format!(
                "Destination shard {} does not exist",
                transfer.shard_id,
            )));
        };

        let Some(to_shard_id) = transfer.to_shard_id else {
            return Err(CollectionError::bad_request(
                "Target shard is not set for resharding transfer",
            ));
        };

        if transfer.shard_id == to_shard_id {
            return Err(CollectionError::bad_request(format!(
                "Source and target shard must be different for resharding transfer, both are {to_shard_id}",
            )));
        }

        if let Some(ReplicaState::Dead) = destination_replicas.get(&transfer.to) {
            return Err(CollectionError::bad_request(format!(
                "Resharding shard transfer can't be started, \
                 because destination shard {}/{to_shard_id} is dead",
                transfer.to,
            )));
        }

        // Both shard IDs must share the same shard key
        let source_shard_key = shards_key_mapping
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&to_shard_id))
            .map(|(key, _)| key);
        let target_shard_key = shards_key_mapping
            .iter()
            .find(|(_, shard_ids)| shard_ids.contains(&to_shard_id))
            .map(|(key, _)| key);
        if source_shard_key != target_shard_key {
            return Err(CollectionError::bad_request(format!(
                "Source and target shard must have the same shard key, but they have {source_shard_key:?} and {target_shard_key:?}",
            )));
        }
    } else if transfer.filter.is_some() {
        let Some(destination_replicas) = destination_replicas else {
            return Err(CollectionError::service_error(format!(
                "Destination shard {} does not exist",
                transfer.shard_id,
            )));
        };

        let Some(to_shard_id) = transfer.to_shard_id else {
            return Err(CollectionError::bad_request(
                "Target shard is not set for filtered points transfer",
            ));
        };

        if transfer.shard_id == to_shard_id {
            return Err(CollectionError::bad_request(format!(
                "Source and target shard must be different for filtered points transfer, both are {to_shard_id}",
            )));
        }

        if let Some(ReplicaState::Dead) = destination_replicas.get(&transfer.to) {
            return Err(CollectionError::bad_request(format!(
                "Filtered shard transfer can't be started, \
                     because destination shard {}/{to_shard_id} is dead",
                transfer.to,
            )));
        }
    } else if let Some(to_shard_id) = transfer.to_shard_id {
        return Err(CollectionError::bad_request(format!(
            "Target shard {to_shard_id} can only be set for {:?} or filtered streaming records transfers",
            ShardTransferMethod::ReshardingStreamRecords,
        )));
    }

    Ok(())
}

/// Selects a best peer to transfer shard from.
///
/// Requirements:
/// 1. Peer should have an active replica of the shard
/// 2. There should be no active transfers from this peer with the same shard
/// 3. Prefer peer with the lowest number of active transfers
///
/// If there are no peers that satisfy the requirements, returns `None`.
pub fn suggest_transfer_source(
    shard_id: ShardId,
    target_peer: PeerId,
    current_transfers: &[ShardTransfer],
    shard_peers: &HashMap<PeerId, ReplicaState>,
) -> Option<PeerId> {
    let mut candidates = HashSet::new();

    for (&peer_id, &state) in shard_peers {
        // We allow transfers *from* `ReshardingScaleDown` replicas, because they contain a *superset*
        // of points in a regular replica
        let is_active = matches!(
            state,
            ReplicaState::Active | ReplicaState::ReshardingScaleDown
        );

        if is_active && peer_id != target_peer {
            candidates.insert(peer_id);
        }
    }

    let currently_transferring = current_transfers
        .iter()
        .filter(|transfer| transfer.shard_id == shard_id)
        .flat_map(|transfer| [transfer.from, transfer.to])
        .collect::<HashSet<PeerId>>();

    candidates = candidates
        .difference(&currently_transferring)
        .cloned()
        .collect();

    let transfer_counts = current_transfers
        .iter()
        .fold(HashMap::new(), |mut counts, transfer| {
            *counts.entry(transfer.from).or_insert(0_usize) += 1;
            counts
        });

    // Sort candidates by the number of active transfers
    let mut candidates = candidates
        .into_iter()
        .map(|peer_id| (peer_id, transfer_counts.get(&peer_id).unwrap_or(&0)))
        .collect::<Vec<(PeerId, &usize)>>();
    candidates.sort_unstable_by_key(|(_, count)| **count);

    candidates.first().map(|(peer_id, _)| *peer_id)
}
