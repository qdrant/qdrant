use std::sync::Arc;

use collection::collection_state::State;
use collection::config::{CollectionConfigInternal, ShardingMethod};
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
use storage::rbac::{Access, AccessRequirements};

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
        .check_global_access(AccessRequirements::new().manage())
        .expect("Full access should have manage rights");

    consensus_state.is_leader_established.await_ready();
    for collection_name in collections {
        let Ok(collection_obj) = toc_arc
            .get_collection(&multipass.issue_pass(&collection_name))
            .await
        else {
            break;
        };

        let State {
            config,
            shards,
            resharding: _, // resharding can't exist outside of consensus
            transfers: _,  // transfers can't exist outside of consensus
            shards_key_mapping,
            payload_index_schema: _, // payload index schema doesn't require special handling in this case
        } = collection_obj.state().await;

        let CollectionConfigInternal {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config,
            uuid,
            metadata,
        } = config;

        let shards_number = params.shard_number.get();
        let sharding_method = params.sharding_method;

        let mut collection_create_operation = CreateCollectionOperation::new(
            collection_name.to_string(),
            CreateCollection {
                vectors: params.vectors,
                sparse_vectors: params.sparse_vectors,
                shard_number: Some(shards_number),
                sharding_method,
                replication_factor: Some(params.replication_factor.get()),
                write_consistency_factor: Some(params.write_consistency_factor.get()),
                on_disk_payload: Some(params.on_disk_payload),
                hnsw_config: Some(hnsw_config.into()),
                wal_config: Some(wal_config.into()),
                optimizers_config: Some(optimizer_config.into()),
                quantization_config,
                strict_mode_config,
                uuid,
                metadata,
            },
        )
        .expect("Failed to create collection operation");

        let mut consensus_operations = Vec::new();

        match sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                collection_create_operation.set_distribution(ShardDistributionProposal {
                    distribution: shards
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

                for (shard_key, shard_ids) in shards_key_mapping.iter() {
                    let mut placement = Vec::new();

                    for shard_id in shard_ids {
                        let shard_info = shards.get(shard_id).unwrap();
                        placement.push(shard_info.replicas.keys().copied().collect());
                    }

                    consensus_operations.push(CollectionMetaOperations::CreateShardKey(
                        CreateShardKey {
                            collection_name: collection_name.to_string(),
                            shard_key: shard_key.clone(),
                            placement,
                            initial_state: None, // Initial state can't be set during migration
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

        for (shard_id, shard_info) in shards {
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
