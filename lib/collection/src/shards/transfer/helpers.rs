use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::{ShardTransfer, ShardTransferKey, ShardTransferMethod};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};

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
/// 4. If a target shard is only set for resharding  transfers
///
/// If validation fails, return `BadRequest` error.
pub fn validate_transfer(
    transfer: &ShardTransfer,
    all_peers: &HashSet<PeerId>,
    shard_state: Option<&HashMap<PeerId, ReplicaState>>,
    current_transfers: &HashSet<ShardTransfer>,
) -> CollectionResult<()> {
    let shard_state = if let Some(shard_state) = shard_state {
        shard_state
    } else {
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

    if shard_state.get(&transfer.from) != Some(&ReplicaState::Active) {
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
        match transfer.to_shard_id {
            Some(to_shard_id) if transfer.shard_id != to_shard_id => {}
            Some(to_shard_id) => {
                return Err(CollectionError::bad_request(format!(
                    "Source and target shard must be different for resharding transfer, both are {to_shard_id}",
                )));
            }
            None => {
                return Err(CollectionError::bad_request(
                    "Target shard is not set for resharding transfer".into(),
                ));
            }
        }
    } else if let Some(to_shard_id) = transfer.to_shard_id {
        return Err(CollectionError::bad_request(format!(
            "Target shard {to_shard_id} can only be set for {:?} transfers",
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
    for (peer_id, state) in shard_peers {
        if *state == ReplicaState::Active && *peer_id != target_peer {
            candidates.insert(*peer_id);
        }
    }

    let currently_transferring = current_transfers
        .iter()
        .filter(|transfer| transfer.shard_id == shard_id)
        .map(|transfer| transfer.from)
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

/// Selects the best peer to add a replica to.
///
/// Requirements:
/// 1. Peer should not have an active replica of the shard
/// 2. Peer should have minimal number of active transfers
pub fn suggest_peer_to_add_replica(
    shard_id: ShardId,
    shard_distribution: HashMap<ShardId, HashSet<PeerId>>,
) -> Option<PeerId> {
    let mut peer_loads: HashMap<PeerId, usize> = HashMap::new();
    for peers in shard_distribution.values() {
        for peer_id in peers {
            *peer_loads.entry(*peer_id).or_insert(0_usize) += 1;
        }
    }
    let peers_with_shard = shard_distribution
        .get(&shard_id)
        .cloned()
        .unwrap_or_default();
    for peer_with_shard in peers_with_shard {
        peer_loads.remove(&peer_with_shard);
    }

    let mut candidates = peer_loads.into_iter().collect::<Vec<(PeerId, usize)>>();
    candidates.sort_unstable_by_key(|(_, count)| *count);
    candidates.first().map(|(peer_id, _)| *peer_id)
}

/// Selects the best peer to remove a replica from.
///
/// Requirements:
/// 1. Peer should have a replica of the shard
/// 2. Peer should maximal number of active shards
/// 3. Shard replica should preferably be non-active
pub fn suggest_peer_to_remove_replica(
    shard_distribution: HashMap<ShardId, HashSet<PeerId>>,
    shard_peers: HashMap<PeerId, ReplicaState>,
) -> Option<PeerId> {
    let mut peer_loads: HashMap<PeerId, usize> = HashMap::new();
    for (_, peers) in shard_distribution {
        for peer_id in peers {
            *peer_loads.entry(peer_id).or_insert(0_usize) += 1;
        }
    }

    let mut candidates: Vec<_> = shard_peers
        .into_iter()
        .map(|(peer_id, status)| {
            (
                peer_id,
                status,
                peer_loads.get(&peer_id).copied().unwrap_or(0),
            )
        })
        .collect();

    candidates.sort_unstable_by(|(_, status1, count1), (_, status2, count2)| {
        match (status1, status2) {
            (ReplicaState::Active, ReplicaState::Active) => count2.cmp(count1),
            (ReplicaState::Active, _) => Ordering::Less,
            (_, ReplicaState::Active) => Ordering::Greater,
            (_, _) => count2.cmp(count1),
        }
    });

    candidates.first().map(|(peer_id, _, _)| *peer_id)
}
