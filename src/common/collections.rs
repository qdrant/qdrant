use std::time::Duration;

use api::grpc::models::{CollectionDescription, CollectionsResponse};
use collection::operations::cluster_ops::{
    AbortTransferOperation, ClusterOperations, DropReplicaOperation, MoveShardOperation,
    ReplicateShardOperation,
};
use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::types::{
    AliasDescription, CollectionClusterInfo, CollectionInfo, CollectionsAliasesResponse,
};
use collection::shards::replica_set;
use collection::shards::shard::ShardId;
use collection::shards::transfer::shard_transfer::{ShardTransfer, ShardTransferKey};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use storage::content_manager::collection_meta_ops::ShardTransferOperations::{Abort, Start};
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateShardKey, DropShardKey, UpdateCollectionOperation,
};
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

pub async fn do_list_collection_aliases(
    toc: &TableOfContent,
    collection_name: &str,
) -> Result<CollectionsAliasesResponse, StorageError> {
    let mut aliases: Vec<AliasDescription> = Default::default();
    for alias in toc.collection_aliases(collection_name).await? {
        aliases.push(AliasDescription {
            alias_name: alias.to_string(),
            collection_name: collection_name.to_string(),
        });
    }
    Ok(CollectionsAliasesResponse { aliases })
}

pub async fn do_list_aliases(
    toc: &TableOfContent,
) -> Result<CollectionsAliasesResponse, StorageError> {
    let aliases = toc.list_aliases().await?;
    Ok(CollectionsAliasesResponse { aliases })
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
    dispatcher: &Dispatcher,
    collection_name: &str,
    wait: bool,
) -> Result<SnapshotDescription, StorageError> {
    let collection = collection_name.to_string();
    let dispatcher = dispatcher.clone();
    let snapshot = tokio::spawn(async move { dispatcher.create_snapshot(&collection).await });
    if wait {
        Ok(snapshot.await??)
    } else {
        Ok(SnapshotDescription {
            name: "".to_string(),
            creation_time: None,
            size: 0,
        })
    }
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

    let get_all_peer_ids = || {
        consensus_state
            .persistent
            .read()
            .peer_address_by_id
            .read()
            .keys()
            .cloned()
            .collect_vec()
    };

    let validate_peer_exists = |peer_id| {
        let target_peer_exist = consensus_state
            .persistent
            .read()
            .peer_address_by_id
            .read()
            .contains_key(&peer_id);
        if !target_peer_exist {
            return Err(StorageError::BadRequest {
                description: format!("Peer {peer_id} does not exist"),
            });
        }
        Ok(())
    };

    let collection = toc.get_collection(&collection_name).await?;

    match operation {
        ClusterOperations::MoveShard(MoveShardOperation { move_shard }) => {
            // validate shard to move
            if !collection.contains_shard(move_shard.shard_id).await {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Shard {} of {} does not exist",
                        move_shard.shard_id, collection_name
                    ),
                });
            };

            // validate target and source peer exists
            validate_peer_exists(move_shard.to_peer_id)?;
            validate_peer_exists(move_shard.from_peer_id)?;

            // submit operation to consensus
            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        Start(ShardTransfer {
                            shard_id: move_shard.shard_id,
                            to: move_shard.to_peer_id,
                            from: move_shard.from_peer_id,
                            sync: false,
                            method: move_shard.method,
                        }),
                    ),
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::ReplicateShard(ReplicateShardOperation { replicate_shard }) => {
            // validate shard to move
            if !collection.contains_shard(replicate_shard.shard_id).await {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Shard {} of {} does not exist",
                        replicate_shard.shard_id, collection_name
                    ),
                });
            };

            // validate target peer exists
            validate_peer_exists(replicate_shard.to_peer_id)?;

            // validate source peer exists
            validate_peer_exists(replicate_shard.from_peer_id)?;

            // submit operation to consensus
            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        Start(ShardTransfer {
                            shard_id: replicate_shard.shard_id,
                            to: replicate_shard.to_peer_id,
                            from: replicate_shard.from_peer_id,
                            sync: true,
                            method: replicate_shard.method,
                        }),
                    ),
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::AbortTransfer(AbortTransferOperation { abort_transfer }) => {
            let transfer = ShardTransferKey {
                shard_id: abort_transfer.shard_id,
                to: abort_transfer.to_peer_id,
                from: abort_transfer.from_peer_id,
            };

            if !collection.check_transfer_exists(&transfer).await {
                return Err(StorageError::NotFound {
                    description: format!(
                        "Shard transfer {} -> {} for collection {}:{} does not exist",
                        transfer.from, transfer.to, collection_name, transfer.shard_id
                    ),
                });
            }

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        Abort {
                            transfer,
                            reason: "user request".to_string(),
                        },
                    ),
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::DropReplica(DropReplicaOperation { drop_replica }) => {
            if !collection.contains_shard(drop_replica.shard_id).await {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Shard {} of {} does not exist",
                        drop_replica.shard_id, collection_name
                    ),
                });
            };

            validate_peer_exists(drop_replica.peer_id)?;

            let mut update_operation = UpdateCollectionOperation::new_empty(collection_name);

            update_operation.set_shard_replica_changes(vec![replica_set::Change::Remove(
                drop_replica.shard_id,
                drop_replica.peer_id,
            )]);

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::UpdateCollection(update_operation),
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::CreateShardingKey(crete_sharding_key) => {
            // Validate that:
            // - key does not exist yet
            //
            // If placement suggested:
            // - Correct amount of peers provided
            // - Peers exist

            let state = collection.state().await;

            let shards_numer = state.config.params.shard_number.get() as usize;

            let shard_keys_mapping = state.shards_key_mapping;
            if shard_keys_mapping.contains_key(&crete_sharding_key.shard_key) {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Sharding key {} already exists for collection {}",
                        crete_sharding_key.shard_key, collection_name
                    ),
                });
            }

            let placement = if let Some(placement) = crete_sharding_key.placement {
                if placement.len() != shards_numer {
                    return Err(StorageError::BadRequest {
                        description: format!(
                            "Placement for sharding key {} should contain {} peers, but {} provided",
                            crete_sharding_key.shard_key,
                            shards_numer,
                            placement.len()
                        ),
                    });
                }
                for peer_id in placement.iter().copied() {
                    validate_peer_exists(peer_id)?;
                }
                placement
            } else {
                // Suggest random placement
                let all_peer_ids = get_all_peer_ids();

                let mut rng = rand::thread_rng();

                let placement: Vec<_> = if all_peer_ids.len() < shards_numer {
                    // random select `shards_numer` from all_peer_ids with repetition
                    (0..shards_numer)
                        .filter_map(|_| all_peer_ids.choose(&mut rng))
                        .copied()
                        .collect()
                } else {
                    // random select `shards_numer` from all_peer_ids without repetition
                    all_peer_ids
                        .choose_multiple(&mut rng, shards_numer)
                        .copied()
                        .collect()
                };
                placement
            };

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::CreateShardKey(CreateShardKey {
                        collection_name,
                        shard_key: crete_sharding_key.shard_key,
                        placement,
                    }),
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::DropShardingKey(drop_sharding_key) => {
            // Validate that:
            // - key does exist

            let state = collection.state().await;

            let shard_keys_mapping = state.shards_key_mapping;
            if !shard_keys_mapping.contains_key(&drop_sharding_key.shard_key) {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Sharding key {} does not exists for collection {}",
                        drop_sharding_key.shard_key, collection_name
                    ),
                });
            }

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::DropShardKey(DropShardKey {
                        collection_name,
                        shard_key: drop_sharding_key.shard_key,
                    }),
                    wait_timeout,
                )
                .await
        }
    }
}
