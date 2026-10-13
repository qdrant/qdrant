use std::collections::HashSet;
use std::num::NonZeroU32;
use std::time::Duration;

use collection::config::ShardingMethod;
use collection::operations::config_diff::HnswConfigDiff;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard_holder::ShardHolder;
use collection::shards::shard_trait::WaitUntil;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use common::ambient::{AmbientContext, AmbientFutureExt};
use segment::data_types::vector_name_config::{DenseVectorConfig, VectorNameConfig};
use segment::json_path::JsonPath;
use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType, ShardKey};
use shard::operations::{CreateVectorName, DeleteVectorName, VectorNameOperations};
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use storage::content_manager::CollectionContainer;
use storage::content_manager::collection_meta_ops::{CollectionMetaOperations, CreateShardKey};
use storage::content_manager::consensus_state_machine::{Action, CollectionConfigDiff};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tokio::runtime::Handle;

use super::alias_tests::{
    FULL_ACCESS, create_collection, create_custom_collection, new_dispatcher,
};

#[test]
fn set_payload_index_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let field_name: JsonPath = "city".parse().unwrap();
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let action = Action::SetPayloadIndex {
        collection: "test".into(),
        field_name: field_name.clone(),
        field_schema: field_schema.clone(),
    };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let segments = local_segments(&handle, &dispatcher);

    toc.apply_action(action.clone()).unwrap();
    wait_for_local_segments(&handle, &segments, |holder| {
        holder.iter().all(|(_, segment)| {
            segment.get().read().get_indexed_fields().get(&field_name) == Some(&field_schema)
        })
    });

    // Remove the local index while keeping the persisted collection schema,
    // so replay has to finish the shard work even when metadata already matches.
    let operation = CollectionUpdateOperations::FieldIndexOperation(
        FieldIndexOperations::DeleteIndex(field_name.clone()),
    );
    update_local_schema(&handle, &dispatcher, operation);
    assert!(segments.read().iter().all(|(_, segment)| {
        !segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&field_name)
    }));

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();
        wait_for_local_segments(&handle, &segments, |holder| {
            holder.iter().all(|(_, segment)| {
                segment.get().read().get_indexed_fields().get(&field_name) == Some(&field_schema)
            })
        });
    }

    let state = toc.collection_state("test").unwrap();
    assert_eq!(
        state
            .payload_index_schema
            .schema
            .get(&"city".parse().unwrap()),
        Some(&field_schema),
    );
}

#[test]
fn drop_payload_index_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let field_name: JsonPath = "city".parse().unwrap();
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let segments = local_segments(&handle, &dispatcher);

    toc.apply_action(Action::SetPayloadIndex {
        collection: "test".into(),
        field_name: field_name.clone(),
        field_schema: field_schema.clone(),
    })
    .unwrap();
    wait_for_local_segments(&handle, &segments, |holder| {
        holder.iter().all(|(_, segment)| {
            segment.get().read().get_indexed_fields().get(&field_name) == Some(&field_schema)
        })
    });

    let action = Action::DropPayloadIndex {
        collection: "test".into(),
        field_name: "city".parse().unwrap(),
    };
    toc.apply_action(action.clone()).unwrap();
    wait_for_local_segments(&handle, &segments, |holder| {
        holder.iter().all(|(_, segment)| {
            !segment
                .get()
                .read()
                .get_indexed_fields()
                .contains_key(&field_name)
        })
    });

    // Restore only the local index to model metadata removal before shard cleanup
    let index = CreateIndex {
        field_name: field_name.clone(),
        field_schema: Some(field_schema.clone()),
    };
    let operation =
        CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(index));
    update_local_schema(&handle, &dispatcher, operation);
    assert!(segments.read().iter().all(|(_, segment)| {
        segment.get().read().get_indexed_fields().get(&field_name) == Some(&field_schema)
    }));

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();
        wait_for_local_segments(&handle, &segments, |holder| {
            holder.iter().all(|(_, segment)| {
                !segment
                    .get()
                    .read()
                    .get_indexed_fields()
                    .contains_key(&field_name)
            })
        });
    }

    let state = toc.collection_state("test").unwrap();
    assert!(
        !state
            .payload_index_schema
            .schema
            .contains_key(&"city".parse().unwrap())
    );
}

#[test]
fn add_named_vector_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let config = VectorNameConfig::dense(DenseVectorConfig {
        size: 4,
        distance: Distance::Dot,
        multivector_config: None,
        datatype: None,
    });
    let action = Action::AddNamedVector {
        collection: "test".into(),
        vector_name: "text".into(),
        config: Box::new(config),
    };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let segments = local_segments(&handle, &dispatcher);

    toc.apply_action(action.clone()).unwrap();
    wait_for_local_segments(&handle, &segments, |holder| {
        holder.iter().all(|(_, segment)| {
            let segment = segment.get();
            let segment = segment.read();
            segment
                .config()
                .vector_data
                .get("text")
                .is_some_and(|config| config.size == 4 && config.distance == Distance::Dot)
        })
    });

    // Remove only the local vector to model a persisted config with unfinished shard work
    let delete = DeleteVectorName {
        vector_name: "text".into(),
    };
    let operation = CollectionUpdateOperations::VectorNameOperation(
        VectorNameOperations::DeleteVectorName(delete),
    );
    update_local_schema(&handle, &dispatcher, operation);
    assert!(segments.read().iter().all(|(_, segment)| {
        !segment
            .get()
            .read()
            .config()
            .vector_data
            .contains_key("text")
    }));

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();
        wait_for_local_segments(&handle, &segments, |holder| {
            holder.iter().all(|(_, segment)| {
                let segment = segment.get();
                let segment = segment.read();
                segment
                    .config()
                    .vector_data
                    .get("text")
                    .is_some_and(|config| config.size == 4 && config.distance == Distance::Dot)
            })
        });
    }

    let state = toc.collection_state("test").unwrap();
    let params = state.config.params.vectors.get_params("text").unwrap();
    assert_eq!(params.size.get(), 4);
    assert_eq!(params.distance, Distance::Dot);
}

#[test]
fn drop_named_vector_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let segments = local_segments(&handle, &dispatcher);
    let config = VectorNameConfig::dense(DenseVectorConfig {
        size: 4,
        distance: Distance::Cosine,
        multivector_config: None,
        datatype: None,
    });

    toc.apply_action(Action::AddNamedVector {
        collection: "test".into(),
        vector_name: "text".into(),
        config: Box::new(config.clone()),
    })
    .unwrap();
    wait_for_local_segments(&handle, &segments, |holder| {
        holder.iter().all(|(_, segment)| {
            segment
                .get()
                .read()
                .config()
                .vector_data
                .contains_key("text")
        })
    });

    let action = Action::DropNamedVector {
        collection: "test".into(),
        vector_name: "text".into(),
    };
    toc.apply_action(action.clone()).unwrap();
    assert!(segments.read().iter().all(|(_, segment)| {
        !segment
            .get()
            .read()
            .config()
            .vector_data
            .contains_key("text")
    }));

    // Restore only the local vector to model config removal before shard cleanup
    let create = CreateVectorName {
        vector_name: "text".into(),
        config,
    };
    let operation = CollectionUpdateOperations::VectorNameOperation(
        VectorNameOperations::CreateVectorName(create),
    );
    update_local_schema(&handle, &dispatcher, operation);
    assert!(segments.read().iter().all(|(_, segment)| {
        segment
            .get()
            .read()
            .config()
            .vector_data
            .contains_key("text")
    }));

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();
        assert!(segments.read().iter().all(|(_, segment)| {
            !segment
                .get()
                .read()
                .config()
                .vector_data
                .contains_key("text")
        }));
    }

    let state = toc.collection_state("test").unwrap();
    assert!(state.config.params.vectors.get_params("text").is_none());
}

#[test]
fn update_collection_config_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let old_m = toc.collection_state("test").unwrap().config.hnsw_config.m;
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let replica_set = handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        let holder = collection.shards_holder();
        holder.read().await.get_shard(0).unwrap().clone()
    });
    let initial_configs = handle
        .block_on(replica_set.local_optimizer_configs_for_test())
        .unwrap();
    assert!(!initial_configs.is_empty());
    assert!(
        initial_configs
            .iter()
            .all(|config| { config.dense_vectors[""].hnsw_config.m == old_m })
    );
    let action = Action::UpdateCollectionConfig {
        collection: "test".into(),
        diff: Box::new(CollectionConfigDiff::Hnsw(HnswConfigDiff {
            m: Some(old_m + 1),
            ..Default::default()
        })),
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        handle.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let configs = replica_set
                        .local_optimizer_configs_for_test()
                        .await
                        .unwrap();
                    assert_eq!(configs.len(), initial_configs.len());
                    if configs
                        .iter()
                        .all(|config| config.dense_vectors[""].hnsw_config.m == old_m + 1)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("local optimizers did not receive the updated HNSW config");
        });
    }

    let state = toc.collection_state("test").unwrap();
    assert_eq!(state.config.hnsw_config.m, old_m + 1);
}

#[test]
fn set_shard_number_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let action = Action::SetShardNumber {
        collection: "test".into(),
        shard_number: NonZeroU32::new(2).unwrap(),
    };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    let state = toc.collection_state("test").unwrap();
    assert_eq!(state.config.params.shard_number.get(), 2);
}

#[test]
fn set_replica_state_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let action = Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 0,
        state: ReplicaState::Active,
    };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    let state = toc.collection_state("test").unwrap();
    assert_eq!(state.shards[&0].replicas[&0], ReplicaState::Active);
}

#[test]
fn drop_shard_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let action = Action::DropShard {
        collection: "test".into(),
        shard_id: 0,
    };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    assert!(
        toc.collection_state("test")
            .unwrap()
            .shards
            .contains_key(&0)
    );
    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    assert!(
        !toc.collection_state("test")
            .unwrap()
            .shards
            .contains_key(&0)
    );
}

#[test]
fn remove_shard_from_key_mapping_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_custom_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.get_channel_service()
        .id_to_address
        .write()
        .insert(0, "http://localhost:6335".parse().unwrap());

    let shard_key = ShardKey::from("tenant");
    CollectionContainer::perform_collection_meta_op(
        toc.as_ref(),
        CollectionMetaOperations::CreateShardKey(CreateShardKey {
            collection_name: "test".into(),
            shard_key: shard_key.clone(),
            placement: vec![vec![0]],
            initial_state: Some(ReplicaState::Active),
        }),
    )
    .unwrap();

    let shard_id = 1;
    let action = Action::RemoveShardFromKeyMapping {
        collection: "test".into(),
        shard_id,
        shard_key,
    };
    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    let state = toc.collection_state("test").unwrap();
    assert!(state.shards_key_mapping[&"tenant".into()].is_empty());
    assert!(state.shards.contains_key(&shard_id));
}

#[test]
fn missing_collection_is_service_error() {
    let (_storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    let error = toc
        .apply_action(Action::DropPayloadIndex {
            collection: "missing".into(),
            field_name: "city".parse().unwrap(),
        })
        .unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
}

#[test]
fn register_transfer_action_replay() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let initial_state = toc.collection_state("test").unwrap();
    let transfer = ordinary_transfer();
    let mut resharding_transfer = transfer.clone();
    resharding_transfer.to_shard_id = Some(1);
    resharding_transfer.sync = true;
    resharding_transfer.method = Some(ShardTransferMethod::ReshardingStreamRecords);
    resharding_transfer.filter = Some(segment::types::Filter::default());
    let transfers = HashSet::from([transfer, resharding_transfer]);

    for transfer in &transfers {
        let action = Action::RegisterTransfer {
            collection: "test".into(),
            transfer: transfer.clone(),
        };
        toc.apply_action(action.clone()).unwrap();
        toc.apply_action(action).unwrap();
    }

    let state = toc.collection_state("test").unwrap();
    assert_eq!(state.transfers, transfers);
    assert_eq!(state.shards, initial_state.shards);

    let holder = ShardHolder::new(
        &storage_dir.path().join("collections/test"),
        ShardingMethod::Auto,
    )
    .unwrap();
    let persisted = holder.get_transfers(|_| true).into_iter().collect();
    assert_eq!(transfers, persisted);
}

#[test]
fn set_transfer_method_action_replay() {
    for sync in [false, true] {
        let (storage_dir, handle, dispatcher) = new_dispatcher();
        create_collection(&handle, &dispatcher, "test");

        let pass = new_unchecked_verification_pass();
        let toc = dispatcher.toc(&FULL_ACCESS, &pass);
        let mut transfer = ordinary_transfer();
        transfer.sync = sync;
        let mut unrelated = ordinary_transfer();
        unrelated.to = 2;
        for transfer in [&transfer, &unrelated] {
            toc.apply_action(Action::RegisterTransfer {
                collection: "test".into(),
                transfer: transfer.clone(),
            })
            .unwrap();
        }

        let action = Action::SetTransferMethod {
            collection: "test".into(),
            key: transfer.key(),
            method: ShardTransferMethod::Snapshot,
        };
        toc.apply_action(action.clone()).unwrap();
        toc.apply_action(action).unwrap();

        transfer.method = Some(ShardTransferMethod::Snapshot);
        let expected = HashSet::from([transfer, unrelated]);
        let state = toc.collection_state("test").unwrap();
        assert_eq!(state.transfers, expected);

        let holder = ShardHolder::new(
            &storage_dir.path().join("collections/test"),
            ShardingMethod::Auto,
        )
        .unwrap();
        let persisted = holder.get_transfers(|_| true).into_iter().collect();
        assert_eq!(expected, persisted);
    }
}

#[test]
fn set_transfer_method_action_missing_transfer() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let transfer = ordinary_transfer();
    let action = Action::SetTransferMethod {
        collection: "test".into(),
        key: transfer.key(),
        method: ShardTransferMethod::Snapshot,
    };

    let error = toc.apply_action(action).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(toc.collection_state("test").unwrap().transfers.is_empty());
}

fn ordinary_transfer() -> ShardTransfer {
    ShardTransfer {
        shard_id: 0,
        to_shard_id: None,
        from: 0,
        to: 1,
        sync: false,
        method: Some(ShardTransferMethod::WalDelta),
        filter: None,
    }
}

fn local_segments(handle: &Handle, dispatcher: &Dispatcher) -> LockedSegmentHolder {
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();

    handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        let holder = collection.shards_holder();
        let replica_set = holder.read().await.get_shard(0).unwrap().clone();
        let segments = replica_set.local_segments_for_test().await.unwrap();
        assert!(!segments.read().is_empty());
        segments
    })
}

fn wait_for_local_segments(
    handle: &Handle,
    segments: &LockedSegmentHolder,
    predicate: impl Fn(&SegmentHolder) -> bool,
) {
    handle.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !predicate(&segments.read()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("local segment schema did not converge");
    });
}

fn update_local_schema(
    handle: &Handle,
    dispatcher: &Dispatcher,
    operation: CollectionUpdateOperations,
) {
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));

    handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        collection
            .update_all_local(operation, WaitUntil::from(true), true)
            .measured(context)
            .await
            .unwrap();
    });
}
