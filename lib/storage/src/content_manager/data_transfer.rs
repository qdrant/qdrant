use std::sync::Arc;
use std::time::Duration;

use collection::collection::Collection;
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointStruct};
use collection::operations::types::{CollectionError, CollectionResult, ScrollRequest};
use collection::operations::CollectionUpdateOperations;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::CollectionId;
use segment::types::{WithPayloadInterface, WithVector};
use tokio::sync::RwLock;

use crate::content_manager::collections_ops::Collections;

const MIGRATION_BATCH_SIZE: usize = 1000;
const COLLECTION_INITIATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Handlers for transferring data from one collection into another within single cluster

/// Get a list of local shards, which can be used for migration
///
/// For each shard, it should present on local peer, it should be active, it should have maximal peer id.
/// Selection of max peer id guarantees that only one shard will be migrated from one peer.
async fn get_local_source_shards(
    source: &Collection,
    this_peer_id: PeerId,
) -> CollectionResult<Vec<ShardId>> {
    let collection_state = source.state().await;

    let mut local_responsible_shards = Vec::new();

    // Find max replica peer id for each shard
    for (shard_id, shard_info) in collection_state.shards.iter() {
        let responsible_shard_opt = shard_info
            .replicas
            .iter()
            .filter(|(_, replica_state)| **replica_state == ReplicaState::Active)
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

fn handle_get_collection(collection: Option<&Collection>) -> CollectionResult<&Collection> {
    match collection {
        Some(collection) => Ok(collection),
        None => Err(CollectionError::service_error(
            "Collection is not found".to_string(),
        )),
    }
}

async fn replicate_shard_data(
    collections: Arc<RwLock<Collections>>,
    source_collection_name: &CollectionId,
    target_collection_name: &CollectionId,
    shard_id: ShardId,
) -> CollectionResult<()> {
    let mut offset = None;
    let limit = MIGRATION_BATCH_SIZE;

    loop {
        let request = ScrollRequest {
            offset,
            limit: Some(limit),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: WithVector::Bool(true),
        };

        let collections_read = collections.read().await;

        let source_collection =
            handle_get_collection(collections_read.get(source_collection_name))?;
        let _updates_guard = source_collection.lock_updates().await;
        let scroll_result = source_collection.scroll_by(request, Some(shard_id)).await?;

        offset = scroll_result.next_page_offset;

        if scroll_result.points.is_empty() {
            break;
        }

        let records = scroll_result
            .points
            .into_iter()
            .map(|point| PointStruct {
                id: point.id,
                vector: point.vector.unwrap(),
                payload: point.payload,
            })
            .collect();

        let upsert_request = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::PointsList(records)),
        );

        let target_collection =
            handle_get_collection(collections_read.get(target_collection_name))?;

        target_collection
            .update_from_client(upsert_request, false)
            .await?;

        if offset.is_none() {
            break;
        }
    }
    Ok(())
}

/// Spawns a task which will retrieve data from appropriate local shards of the `source` collection
/// into target collection.
pub async fn populate_collection(
    collections: Arc<RwLock<Collections>>,
    source_collection: &CollectionId,
    target_collection: &CollectionId,
    this_peer_id: PeerId,
) -> CollectionResult<()> {
    let collections_read = collections.read().await;
    let collection = handle_get_collection(collections_read.get(source_collection))?;
    let local_responsible_shards = get_local_source_shards(collection, this_peer_id).await?;

    log::debug!(
        "Migrating shards {:?} from collection {} to collection {}",
        local_responsible_shards,
        source_collection,
        target_collection
    );

    // Wait for all shards to be active
    {
        let collections_read = collections.read().await;
        let collection = handle_get_collection(collections_read.get(target_collection))?;
        let is_initialized = collection.wait_collection_initiated(COLLECTION_INITIATION_TIMEOUT);
        if !is_initialized {
            return Err(CollectionError::service_error(format!(
                "Collection {} was not initialized within {} sec timeout",
                target_collection,
                COLLECTION_INITIATION_TIMEOUT.as_secs()
            )));
        }
    }

    for shard_id in local_responsible_shards {
        replicate_shard_data(
            collections.clone(),
            source_collection,
            target_collection,
            shard_id,
        )
        .await?;
    }

    Ok(())
}
