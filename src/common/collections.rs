use std::time::Duration;

use api::grpc::models::{CollectionDescription, CollectionsResponse};
use collection::operations::cluster_ops::ClusterOperations;
use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::types::{CollectionClusterInfo, CollectionInfo};
use collection::shard::{ShardId, ShardTransfer};
use itertools::Itertools;
use storage::content_manager::collection_meta_ops::CollectionMetaOperations;
use storage::content_manager::collection_meta_ops::ShardTransferOperations::Start;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;

pub async fn do_get_collection(
    toc: &TableOfContent,
    name: &str,
    shard_selection: Option<ShardId>,
) -> Result<CollectionInfo, StorageError> {
    let collection = toc.get_collection(name).await?;
    Ok(collection.info(shard_selection).await?)
}

pub async fn do_list_collections(toc: &TableOfContent) -> CollectionsResponse {
    let collections = toc
        .all_collections()
        .await
        .into_iter()
        .map(|name| CollectionDescription { name })
        .collect_vec();

    CollectionsResponse { collections }
}

pub async fn do_list_snapshots(
    toc: &TableOfContent,
    collection_name: &str,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    Ok(toc
        .get_collection(collection_name)
        .await?
        .list_snapshots()
        .await?)
}

pub async fn do_create_snapshot(
    toc: &TableOfContent,
    collection_name: &str,
) -> Result<SnapshotDescription, StorageError> {
    toc.create_snapshot(collection_name).await
}

pub async fn do_get_collection_cluster(
    toc: &TableOfContent,
    name: &str,
) -> Result<CollectionClusterInfo, StorageError> {
    let collection = toc.get_collection(name).await?;
    Ok(collection.cluster_info(toc.this_peer_id).await?)
}

pub async fn do_update_collection_cluster(
    toc: &TableOfContent,
    collection_name: String,
    operation: ClusterOperations,
    dispatcher: &Dispatcher,
    wait_timeout: Option<Duration>,
) -> Result<bool, StorageError> {
    if dispatcher.consensus_state().is_none() {
        return Err(StorageError::BadRequest {
            description: "Distributed mode disabled".to_string(),
        });
    }
    let consensus_state = dispatcher.consensus_state().unwrap();

    let collection = toc.get_collection(&collection_name).await?;
    match operation {
        ClusterOperations::MoveShard(move_shard) => {
            // validate shard to move
            let shard_exists = collection.contains_shard(&move_shard.shard_id).await;
            if !shard_exists {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Shard {} for collection {} does not exist",
                        move_shard.to_peer_id, collection_name
                    ),
                });
            }

            // validate target peer exists
            let target_peer_exist = consensus_state
                .persistent
                .read()
                .peer_address_by_id
                .read()
                .contains_key(&move_shard.to_peer_id);
            if !target_peer_exist {
                return Err(StorageError::BadRequest {
                    description: format!("Target peer {} does not exist", move_shard.to_peer_id),
                });
            }

            // validate source peer exists
            let target_peer_exist = consensus_state
                .persistent
                .read()
                .peer_address_by_id
                .read()
                .contains_key(&move_shard.from_peer_id);
            if !target_peer_exist {
                return Err(StorageError::BadRequest {
                    description: format!("Source peer {} does not exist", move_shard.from_peer_id),
                });
            }

            // submit operation to consensus
            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        Start(ShardTransfer {
                            shard_id: move_shard.shard_id,
                            to: move_shard.to_peer_id,
                            from: move_shard.from_peer_id,
                        }),
                    ),
                    wait_timeout,
                )
                .await
        }
    }
}
