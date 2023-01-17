use std::collections::HashMap;

use collection::collection::Collection;
use collection::operations::types::{CollectionError, CollectionResult};
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};

/// Handlers for migration data from one collection into another within single cluster


/// Get a list of local shards, which can be used for migration
///
/// For each shard, it should present on local peer, it should be active, it should have maximal peer id.
/// Selection of max peer id guarantees that only one shard will be migrated from one peer.
async fn get_local_source_shards(source: &Collection, this_peer_id: PeerId) -> CollectionResult<Vec<ShardId>> {
    let collection_state = source.state().await;

    let mut local_responsible_shards = Vec::new();

    // Find max replica peer id for each shard
    for (shard_id, shard_info) in collection_state.shards.iter() {
        let responsible_shard_opt = shard_info
            .replicas
            .iter()
            .filter(|(_, replica_state)| replica_state == ReplicaState::Active)
            .max_by_key(|(peer_id, _)| *peer_id)
            .map(|(peer_id, _)| *peer_id);

        let responsible_shard = match responsible_shard_opt {
            None => {
                return Err(CollectionError::service_error(format!(
                    "No active replica for shard {}, collection initialization is cancelled",
                    shard_id
                )));
            }
            Some(responsible_shard) => responsible_shard,
        };

        if responsible_shard == this_peer_id {
            local_responsible_shards.push(*shard_id);
        }
    }

    Ok(local_responsible_shards)
}


/// Spawns a task which will retrieve data from appropriate local shards of the `source` collection
/// into target collection.
pub async fn migrate(
    source: &Collection,
    target: &Collection,
    this_peer_id: PeerId,
) -> CollectionResult<()> {
    let local_responsible_shards = get_local_source_shards(source, this_peer_id).await?;

    Ok(())
}
