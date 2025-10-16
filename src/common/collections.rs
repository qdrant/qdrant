use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::CollectionExists;
use api::rest::models::{CollectionDescription, CollectionsResponse};
use collection::config::ShardingMethod;
use collection::operations::cluster_ops::{
    AbortTransferOperation, ClusterOperations, DropReplicaOperation, MoveShardOperation,
    ReplicatePoints, ReplicatePointsOperation, ReplicateShardOperation, ReshardingDirection,
    RestartTransfer, RestartTransferOperation, StartResharding,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::types::{
    AliasDescription, CollectionClusterInfo, CollectionInfo, CollectionsAliasesResponse,
};
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::replica_set;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId, ShardsPlacement};
use collection::shards::transfer::{
    ShardTransfer, ShardTransferKey, ShardTransferMethod, ShardTransferRestart,
};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::seq::IteratorRandom;
use segment::types::Filter;
use storage::content_manager::collection_meta_ops::ShardTransferOperations::{Abort, Start};
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateShardKey, DropShardKey, ReshardingOperation,
    SetShardReplicaState, ShardTransferOperations, UpdateCollectionOperation,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequirements};
use uuid::Uuid;

pub async fn do_collection_exists(
    toc: &TableOfContent,
    access: Access,
    name: &str,
) -> Result<CollectionExists, StorageError> {
    let collection_pass = access.check_collection_access(name, AccessRequirements::new())?;

    // if this returns Ok, it means the collection exists.
    // if not, we check that the error is NotFound
    let Err(error) = toc.get_collection(&collection_pass).await else {
        return Ok(CollectionExists { exists: true });
    };
    match error {
        StorageError::NotFound { .. } => Ok(CollectionExists { exists: false }),
        e => Err(e),
    }
}

pub async fn do_get_collection(
    toc: &TableOfContent,
    access: Access,
    name: &str,
    shard_selection: Option<ShardId>,
) -> Result<CollectionInfo, StorageError> {
    let collection_pass =
        access.check_collection_access(name, AccessRequirements::new().whole())?;

    let collection = toc.get_collection(&collection_pass).await?;

    let shard_selection = match shard_selection {
        None => ShardSelectorInternal::All,
        Some(shard_id) => ShardSelectorInternal::ShardId(shard_id),
    };

    Ok(collection.info(&shard_selection).await?)
}

pub async fn do_list_collections(
    toc: &TableOfContent,
    access: Access,
) -> Result<CollectionsResponse, StorageError> {
    let collections = toc
        .all_collections(&access)
        .await
        .into_iter()
        .map(|pass| CollectionDescription {
            name: pass.name().to_string(),
        })
        .collect_vec();

    Ok(CollectionsResponse { collections })
}

/// Construct shards-replicas layout for the shard from the given scope of peers
/// Example:
///   Shards: 3
///   Replicas: 2
///   Peers: [A, B, C]
///
/// Placement:
/// [
///         [A, B]
///         [B, C]
///         [A, C]
/// ]
fn generate_even_placement(
    mut pool: Vec<PeerId>,
    shard_number: usize,
    replication_factor: usize,
) -> ShardsPlacement {
    let mut exact_placement = Vec::new();
    let mut rng = rand::rng();
    pool.shuffle(&mut rng);
    let mut loop_iter = pool.iter().cycle();

    // pool: [1,2,3,4]
    // shuf_pool: [2,3,4,1]
    //
    // loop_iter:       [2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1,...]
    // shard_placement: [2, 3, 4][1, 2, 3][4, 1, 2][3, 4, 1][2, 3, 4]

    let max_replication_factor = std::cmp::min(replication_factor, pool.len());
    for _shard in 0..shard_number {
        let mut shard_placement = Vec::new();
        for _replica in 0..max_replication_factor {
            shard_placement.push(*loop_iter.next().unwrap());
        }
        exact_placement.push(shard_placement);
    }
    exact_placement
}

pub async fn do_list_collection_aliases(
    toc: &TableOfContent,
    access: Access,
    collection_name: &str,
) -> Result<CollectionsAliasesResponse, StorageError> {
    let collection_pass =
        access.check_collection_access(collection_name, AccessRequirements::new())?;
    let aliases: Vec<AliasDescription> = toc
        .collection_aliases(&collection_pass, &access)
        .await?
        .into_iter()
        .map(|alias| AliasDescription {
            alias_name: alias,
            collection_name: collection_name.to_string(),
        })
        .collect();
    Ok(CollectionsAliasesResponse { aliases })
}

pub async fn do_list_aliases(
    toc: &TableOfContent,
    access: Access,
) -> Result<CollectionsAliasesResponse, StorageError> {
    let aliases = toc.list_aliases(&access).await?;
    Ok(CollectionsAliasesResponse { aliases })
}

pub async fn do_list_snapshots(
    toc: &TableOfContent,
    access: Access,
    collection_name: &str,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let collection_pass = access
        .check_collection_access(collection_name, AccessRequirements::new().whole().extras())?;
    Ok(toc
        .get_collection(&collection_pass)
        .await?
        .list_snapshots()
        .await?)
}

pub async fn do_create_snapshot(
    toc: Arc<TableOfContent>,
    access: Access,
    collection_name: &str,
) -> Result<SnapshotDescription, StorageError> {
    let collection_pass = access
        .check_collection_access(
            collection_name,
            AccessRequirements::new().write().whole().extras(),
        )?
        .into_static();

    let result = tokio::spawn(async move { toc.create_snapshot(&collection_pass).await }).await??;

    Ok(result)
}

pub async fn do_get_collection_cluster(
    toc: &TableOfContent,
    access: Access,
    name: &str,
) -> Result<CollectionClusterInfo, StorageError> {
    let collection_pass =
        access.check_collection_access(name, AccessRequirements::new().whole().extras())?;
    let collection = toc.get_collection(&collection_pass).await?;
    Ok(collection.cluster_info(toc.this_peer_id).await?)
}

pub async fn do_update_collection_cluster(
    dispatcher: &Dispatcher,
    collection_name: String,
    operation: ClusterOperations,
    access: Access,
    wait_timeout: Option<Duration>,
) -> Result<bool, StorageError> {
    let collection_pass = access.check_collection_access(
        &collection_name,
        AccessRequirements::new().write().manage().whole().extras(),
    )?;

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

    // All checks should've been done at this point.
    let pass = new_unchecked_verification_pass();

    let collection = dispatcher
        .toc(&access, &pass)
        .get_collection(&collection_pass)
        .await?;

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
                            to_shard_id: move_shard.to_shard_id,
                            to: move_shard.to_peer_id,
                            from: move_shard.from_peer_id,
                            sync: false,
                            method: move_shard.method,
                            filter: None,
                        }),
                    ),
                    access,
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
                            to_shard_id: replicate_shard.to_shard_id,
                            to: replicate_shard.to_peer_id,
                            from: replicate_shard.from_peer_id,
                            sync: true,
                            method: replicate_shard.method,
                            filter: None,
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::ReplicatePoints(ReplicatePointsOperation { replicate_points }) => {
            let ReplicatePoints {
                filter,
                from_shard_key,
                to_shard_key,
            } = replicate_points;

            let from_shard_ids = collection.get_shard_ids(&from_shard_key).await?;

            // Temporary, before we support multi-source transfers
            if from_shard_ids.len() != 1 {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Only replicating from shard keys with exactly one shard is supported. Shard key {from_shard_key} has {} shards",
                        from_shard_ids.len()
                    ),
                });
            }

            // validate shard key exists
            let from_replicas = collection.get_replicas(&from_shard_key).await?;
            let to_replicas = collection.get_replicas(&to_shard_key).await?;

            debug_assert!(!from_replicas.is_empty());

            if to_replicas.len() != 1 {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Only replicating to shard keys with exactly one replica is supported. Shard key {to_shard_key} has {} replicas",
                        to_replicas.len()
                    ),
                });
            }

            // Don't support filters for now
            if filter.is_some() {
                return Err(StorageError::BadRequest {
                    description: "Filtering for replicating points is not supported yet"
                        .to_string(),
                });
            }

            let (from_shard_id, from_peer_id) = from_replicas[0];
            let (to_shard_id, to_peer_id) = to_replicas[0];

            // validate source & target peers exist
            validate_peer_exists(to_peer_id)?;
            validate_peer_exists(from_peer_id)?;

            // submit operation to consensus
            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        Start(ShardTransfer {
                            shard_id: from_shard_id,
                            to_shard_id: Some(to_shard_id),
                            from: from_peer_id,
                            to: to_peer_id,
                            sync: true,
                            method: Some(ShardTransferMethod::StreamRecords),
                            // No filter supported yet but we need to pass some value to change behavior of shard transfer validation
                            filter: Some(Filter::new()),
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::AbortTransfer(AbortTransferOperation { abort_transfer }) => {
            let transfer = ShardTransferKey {
                shard_id: abort_transfer.shard_id,
                to_shard_id: abort_transfer.to_shard_id,
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
                    access,
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
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::CreateShardingKey(create_sharding_key_op) => {
            let create_sharding_key = create_sharding_key_op.create_sharding_key;

            // Validate that:
            // - proper sharding method is used
            // - key does not exist yet
            //
            // If placement suggested:
            // - Peers exist

            let state = collection.state().await;

            match state.config.params.sharding_method.unwrap_or_default() {
                ShardingMethod::Auto => {
                    return Err(StorageError::bad_request(
                        "Shard Key cannot be created with Auto sharding method",
                    ));
                }
                ShardingMethod::Custom => {}
            }

            let shard_number = create_sharding_key
                .shards_number
                .unwrap_or(state.config.params.shard_number)
                .get() as usize;
            let replication_factor = create_sharding_key
                .replication_factor
                .unwrap_or(state.config.params.replication_factor)
                .get() as usize;

            if let Some(initial_state) = create_sharding_key.initial_state {
                match initial_state {
                    replica_set::ReplicaState::Active | replica_set::ReplicaState::Partial => {}
                    _ => {
                        return Err(StorageError::bad_request(format!(
                            "Initial state cannot be {initial_state:?}, only Active or Partial are allowed",
                        )));
                    }
                }
            }

            let shard_keys_mapping = state.shards_key_mapping;
            if shard_keys_mapping.contains_key(&create_sharding_key.shard_key) {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Sharding key {} already exists for collection {}",
                        create_sharding_key.shard_key, collection_name
                    ),
                });
            }

            let peers_pool: Vec<_> = if let Some(placement) = create_sharding_key.placement {
                if placement.is_empty() {
                    return Err(StorageError::BadRequest {
                        description: format!(
                            "Sharding key {} placement cannot be empty. If you want to use random placement, do not specify placement",
                            create_sharding_key.shard_key
                        ),
                    });
                }

                for peer_id in placement.iter().copied() {
                    validate_peer_exists(peer_id)?;
                }
                placement
            } else {
                get_all_peer_ids()
            };

            let exact_placement =
                generate_even_placement(peers_pool, shard_number, replication_factor);

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::CreateShardKey(CreateShardKey {
                        collection_name,
                        shard_key: create_sharding_key.shard_key,
                        placement: exact_placement,
                        initial_state: create_sharding_key.initial_state,
                    }),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::DropShardingKey(drop_sharding_key_op) => {
            let drop_sharding_key = drop_sharding_key_op.drop_sharding_key;
            // Validate that:
            // - proper sharding method is used
            // - key does exist

            let state = collection.state().await;

            match state.config.params.sharding_method.unwrap_or_default() {
                ShardingMethod::Auto => {
                    return Err(StorageError::bad_request(
                        "Shard Key cannot be created with Auto sharding method",
                    ));
                }
                ShardingMethod::Custom => {}
            }

            let shard_keys_mapping = state.shards_key_mapping;
            if !shard_keys_mapping.contains_key(&drop_sharding_key.shard_key) {
                return Err(StorageError::BadRequest {
                    description: format!(
                        "Sharding key {} does not exist for collection {collection_name}",
                        drop_sharding_key.shard_key,
                    ),
                });
            }

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::DropShardKey(DropShardKey {
                        collection_name,
                        shard_key: drop_sharding_key.shard_key,
                    }),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::RestartTransfer(RestartTransferOperation { restart_transfer }) => {
            // TODO(reshading): Deduplicate resharding operations handling?

            let RestartTransfer {
                shard_id,
                to_shard_id,
                from_peer_id,
                to_peer_id,
                method,
            } = restart_transfer;

            let transfer_key = ShardTransferKey {
                shard_id,
                to_shard_id,
                to: to_peer_id,
                from: from_peer_id,
            };

            if !collection.check_transfer_exists(&transfer_key).await {
                return Err(StorageError::NotFound {
                    description: format!(
                        "Shard transfer {} -> {} for collection {}:{} does not exist",
                        transfer_key.from, transfer_key.to, collection_name, transfer_key.shard_id
                    ),
                });
            }

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::TransferShard(
                        collection_name,
                        ShardTransferOperations::Restart(ShardTransferRestart {
                            shard_id,
                            to_shard_id,
                            to: to_peer_id,
                            from: from_peer_id,
                            method,
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::StartResharding(op) => {
            let StartResharding {
                uuid,
                direction,
                peer_id,
                shard_key,
            } = op.start_resharding;

            if !dispatcher.is_resharding_enabled() {
                return Err(StorageError::bad_request(
                    "resharding is only supported in Qdrant Cloud",
                ));
            }

            // Assign random UUID if not specified by user before processing operation on all peers
            let uuid = uuid.unwrap_or_else(Uuid::new_v4);

            let collection_state = collection.state().await;

            if let Some(shard_key) = &shard_key
                && !collection_state.shards_key_mapping.contains_key(shard_key)
            {
                return Err(StorageError::bad_request(format!(
                    "sharding key {shard_key} does not exist for collection {collection_name}",
                )));
            }

            let shard_id = match (direction, shard_key.as_ref()) {
                // When scaling up, just pick the next shard ID
                (ReshardingDirection::Up, _) => {
                    collection_state
                        .shards
                        .keys()
                        .copied()
                        .max()
                        .expect("collection must contain shards")
                        + 1
                }
                // When scaling down without shard keys, pick the last shard ID
                (ReshardingDirection::Down, None) => collection_state
                    .shards
                    .keys()
                    .copied()
                    .max()
                    .expect("collection must contain shards"),
                // When scaling down with shard keys, pick the last shard ID of that key
                (ReshardingDirection::Down, Some(shard_key)) => collection_state
                    .shards_key_mapping
                    .get(shard_key)
                    .expect("specified shard key must exist")
                    .iter()
                    .copied()
                    .max()
                    .expect("collection must contain shards"),
            };

            let peer_id = match (peer_id, direction) {
                // Select user specified peer, but make sure it exists
                (Some(peer_id), _) => {
                    validate_peer_exists(peer_id)?;
                    peer_id
                }

                // When scaling up, select peer with least number of shards for this collection
                (None, ReshardingDirection::Up) => {
                    let mut shards_on_peers = collection_state
                        .shards
                        .values()
                        .flat_map(|shard_info| shard_info.replicas.keys())
                        .fold(HashMap::new(), |mut counts, peer_id| {
                            *counts.entry(*peer_id).or_insert(0) += 1;
                            counts
                        });
                    for peer_id in get_all_peer_ids() {
                        // Add registered peers not holding any shard yet
                        shards_on_peers.entry(peer_id).or_insert(0);
                    }
                    shards_on_peers
                        .into_iter()
                        .min_by_key(|(_, count)| *count)
                        .map(|(peer_id, _)| peer_id)
                        .expect("expected at least one peer")
                }

                // When scaling down, select random peer that contains the shard we're dropping
                // Other peers work, but are less efficient due to remote operations
                (None, ReshardingDirection::Down) => collection_state
                    .shards
                    .get(&shard_id)
                    .expect("select shard ID must always exist in collection state")
                    .replicas
                    .keys()
                    .choose(&mut rand::rng())
                    .copied()
                    .unwrap(),
            };

            if let Some(resharding) = &collection_state.resharding {
                return Err(StorageError::bad_request(format!(
                    "resharding {resharding:?} is already in progress \
                     for collection {collection_name}"
                )));
            }

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::Resharding(
                        collection_name.clone(),
                        ReshardingOperation::Start(ReshardKey {
                            uuid,
                            direction,
                            peer_id,
                            shard_id,
                            shard_key,
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::AbortResharding(_) => {
            // TODO(reshading): Deduplicate resharding operations handling?

            let Some(state) = collection.resharding_state().await else {
                return Err(StorageError::bad_request(format!(
                    "resharding is not in progress for collection {collection_name}"
                )));
            };

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::Resharding(
                        collection_name.clone(),
                        ReshardingOperation::Abort(ReshardKey {
                            uuid: state.uuid,
                            direction: state.direction,
                            peer_id: state.peer_id,
                            shard_id: state.shard_id,
                            shard_key: state.shard_key.clone(),
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
        ClusterOperations::FinishResharding(_) => {
            // TODO(resharding): Deduplicate resharding operations handling?

            let Some(state) = collection.resharding_state().await else {
                return Err(StorageError::bad_request(format!(
                    "resharding is not in progress for collection {collection_name}"
                )));
            };

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::Resharding(
                        collection_name.clone(),
                        ReshardingOperation::Finish(state.key()),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }

        ClusterOperations::FinishMigratingPoints(op) => {
            // TODO(resharding): Deduplicate resharding operations handling?

            let Some(state) = collection.resharding_state().await else {
                return Err(StorageError::bad_request(format!(
                    "resharding is not in progress for collection {collection_name}"
                )));
            };

            let op = op.finish_migrating_points;

            let shard_id = match (op.shard_id, state.direction) {
                (Some(shard_id), _) => shard_id,
                (None, ReshardingDirection::Up) => state.shard_id,
                (None, ReshardingDirection::Down) => {
                    return Err(StorageError::bad_request(
                        "shard ID must be specified when resharding down",
                    ));
                }
            };

            let peer_id = match (op.peer_id, state.direction) {
                (Some(peer_id), _) => peer_id,
                (None, ReshardingDirection::Up) => state.peer_id,
                (None, ReshardingDirection::Down) => {
                    return Err(StorageError::bad_request(
                        "peer ID must be specified when resharding down",
                    ));
                }
            };

            let from_state = match state.direction {
                ReshardingDirection::Up => replica_set::ReplicaState::Resharding,
                ReshardingDirection::Down => replica_set::ReplicaState::ReshardingScaleDown,
            };

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::SetShardReplicaState(SetShardReplicaState {
                        collection_name: collection_name.clone(),
                        shard_id,
                        peer_id,
                        state: replica_set::ReplicaState::Active,
                        from_state: Some(from_state),
                    }),
                    access,
                    wait_timeout,
                )
                .await
        }

        ClusterOperations::CommitReadHashRing(_) => {
            // TODO(reshading): Deduplicate resharding operations handling?

            let Some(state) = collection.resharding_state().await else {
                return Err(StorageError::bad_request(format!(
                    "resharding is not in progress for collection {collection_name}"
                )));
            };

            // TODO(resharding): Add precondition checks?

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::Resharding(
                        collection_name.clone(),
                        ReshardingOperation::CommitRead(ReshardKey {
                            uuid: state.uuid,
                            direction: state.direction,
                            peer_id: state.peer_id,
                            shard_id: state.shard_id,
                            shard_key: state.shard_key.clone(),
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }

        ClusterOperations::CommitWriteHashRing(_) => {
            // TODO(reshading): Deduplicate resharding operations handling?

            let Some(state) = collection.resharding_state().await else {
                return Err(StorageError::bad_request(format!(
                    "resharding is not in progress for collection {collection_name}"
                )));
            };

            // TODO(resharding): Add precondition checks?

            dispatcher
                .submit_collection_meta_op(
                    CollectionMetaOperations::Resharding(
                        collection_name.clone(),
                        ReshardingOperation::CommitWrite(ReshardKey {
                            uuid: state.uuid,
                            direction: state.direction,
                            peer_id: state.peer_id,
                            shard_id: state.shard_id,
                            shard_key: state.shard_key.clone(),
                        }),
                    ),
                    access,
                    wait_timeout,
                )
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_generate_even_placement() {
        let pool = vec![1, 2, 3];
        let placement = generate_even_placement(pool, 3, 2);

        assert_eq!(placement.len(), 3);
        for shard_placement in placement {
            assert_eq!(shard_placement.len(), 2);
            assert_ne!(shard_placement[0], shard_placement[1]);
        }

        let pool = vec![1, 2, 3];
        let placement = generate_even_placement(pool, 3, 3);

        assert_eq!(placement.len(), 3);
        for shard_placement in placement {
            assert_eq!(shard_placement.len(), 3);
            let set: HashSet<_> = shard_placement.into_iter().collect();
            assert_eq!(set.len(), 3);
        }

        let pool = vec![1, 2, 3, 4, 5, 6];
        let placement = generate_even_placement(pool, 3, 2);

        assert_eq!(placement.len(), 3);
        let flat_placement: Vec<_> = placement.into_iter().flatten().collect();
        let set: HashSet<_> = flat_placement.into_iter().collect();
        assert_eq!(set.len(), 6);

        let pool = vec![1, 2, 3, 4, 5];
        let placement = generate_even_placement(pool, 3, 10);

        assert_eq!(placement.len(), 3);
        for shard_placement in placement {
            assert_eq!(shard_placement.len(), 5);
        }
    }
}
