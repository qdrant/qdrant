use std::sync::Arc;

use collection::config::ShardingMethod;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::PeerId;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation, CreateShardKey,
    SetShardReplicaState,
};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::shard_distribution::ShardDistributionProposal;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::{Access, AccessRequrements};

/// Processes the existing collections, which were created outside the consensus:
/// - during the migration from single to cluster
/// - during restoring from a backup
pub async fn handle_existing_collections(
    toc_arc: Arc<TableOfContent>,
    consensus_state: ConsensusStateRef,
    dispatcher_arc: Arc<Dispatcher>,
    this_peer_id: PeerId,
    collections: Vec<String>,
) {
    let full_access = Access::full("Migration from single to cluster");
    let multipass = full_access
        .check_global_access(AccessRequrements::new().manage())
        .expect("Full access should have manage rights");

    consensus_state.is_leader_established.await_ready();
    for collection_name in collections {
        let collection_obj = match toc_arc
            .get_collection_by_pass(&multipass.issue_pass(&collection_name))
            .await
        {
            Ok(collection_obj) => collection_obj,
            Err(_) => break,
        };

        let collection_state = collection_obj.state().await;
        let shards_number = collection_state.config.params.shard_number.get();
        let sharding_method = collection_state.config.params.sharding_method;

        let mut collection_create_operation = CreateCollectionOperation::new(
            collection_name.to_string(),
            CreateCollection {
                vectors: collection_state.config.params.vectors,
                sparse_vectors: collection_state.config.params.sparse_vectors,
                shard_number: Some(shards_number),
                sharding_method,
                replication_factor: Some(collection_state.config.params.replication_factor.get()),
                write_consistency_factor: Some(
                    collection_state
                        .config
                        .params
                        .write_consistency_factor
                        .get(),
                ),
                on_disk_payload: Some(collection_state.config.params.on_disk_payload),
                hnsw_config: Some(collection_state.config.hnsw_config.into()),
                wal_config: Some(collection_state.config.wal_config.into()),
                optimizers_config: Some(collection_state.config.optimizer_config.into()),
                init_from: None,
                quantization_config: collection_state.config.quantization_config,
            },
        );

        let mut consensus_operations = Vec::new();

        match sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                collection_create_operation.set_distribution(ShardDistributionProposal {
                    distribution: collection_state
                        .shards
                        .iter()
                        .filter_map(|(shard_id, shard_info)| {
                            if shard_info.replicas.contains_key(&this_peer_id) {
                                Some((*shard_id, vec![this_peer_id]))
                            } else {
                                None
                            }
                        })
                        .collect(),
                });

                consensus_operations.push(CollectionMetaOperations::CreateCollection(
                    collection_create_operation,
                ));
            }
            ShardingMethod::Custom => {
                // We should create additional consensus operations here to set the shard distribution
                collection_create_operation.set_distribution(ShardDistributionProposal::empty());
                consensus_operations.push(CollectionMetaOperations::CreateCollection(
                    collection_create_operation,
                ));

                for (shard_key, shards) in &collection_state.shards_key_mapping {
                    let mut placement = Vec::new();

                    for shard_id in shards {
                        let shard_info = collection_state.shards.get(shard_id).unwrap();
                        placement.push(shard_info.replicas.keys().copied().collect());
                    }

                    consensus_operations.push(CollectionMetaOperations::CreateShardKey(
                        CreateShardKey {
                            collection_name: collection_name.to_string(),
                            shard_key: shard_key.clone(),
                            placement,
                        },
                    ))
                }
            }
        }

        for operation in consensus_operations {
            let _res = dispatcher_arc
                .submit_collection_meta_op(operation, full_access.clone(), None)
                .await;
        }

        for (shard_id, shard_info) in collection_state.shards {
            if shard_info.replicas.contains_key(&this_peer_id) {
                let _res = dispatcher_arc
                    .submit_collection_meta_op(
                        CollectionMetaOperations::SetShardReplicaState(SetShardReplicaState {
                            collection_name: collection_name.to_string(),
                            shard_id,
                            peer_id: this_peer_id,
                            state: ReplicaState::Active,
                            from_state: None,
                        }),
                        full_access.clone(),
                        None,
                    )
                    .await;
            }
        }
    }
}
