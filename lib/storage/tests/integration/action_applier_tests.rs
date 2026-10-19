use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use collection::collection_state::{self, ShardInfo};
use collection::config::{CollectionConfigInternal, ShardingMethod};
use collection::hash_ring::HashRingRouter;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::config_diff::HnswConfigDiff;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::types::UpdateStatus;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use collection::shards::channel_service::ChannelService;
use collection::shards::remote_shard::RemoteShard;
use collection::shards::replica_set::replica_set_state::{ReplicaSetState, ReplicaState};
use collection::shards::resharding::{ReshardKey, ReshardState, ReshardingStage};
use collection::shards::shard_holder::ShardHolder;
use collection::shards::shard_trait::WaitUntil;
use collection::shards::transfer::transfer_tasks_pool::TransferTaskProgress;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::types::DeferredBehavior;
use parking_lot::Mutex;
use segment::data_types::vector_name_config::{DenseVectorConfig, VectorNameConfig};
use segment::json_path::JsonPath;
use segment::types::{Distance, ExtendedPointId, PayloadFieldSchema, PayloadSchemaType, ShardKey};
use shard::count::CountRequestInternal;
use shard::operations::{CreateVectorName, DeleteVectorName, VectorNameOperations};
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use storage::content_manager::CollectionContainer;
use storage::content_manager::collection_meta_ops::{CollectionMetaOperations, CreateShardKey};
use storage::content_manager::consensus::operation_sender::OperationSender;
use storage::content_manager::consensus::persistent::Persistent;
use storage::content_manager::consensus_manager::{ConsensusManager, ConsensusStateRef};
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::consensus_state_machine::{
    Action, CollectionConfigDiff, LocalShardInitMode, TransferOutcome,
};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::dispatcher::TocDispatcher;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tokio::runtime::Handle;

use super::alias_tests::{
    FULL_ACCESS, create_collection, create_custom_collection, new_dispatcher,
    new_dispatcher_with_proposals, open_dispatcher,
};

#[cfg(feature = "staging")]
#[test]
fn test_slow_down_action_replay() {
    use storage::content_manager::collection_meta_ops::TestSlowDown;

    let (_storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let delay = Duration::from_millis(20);

    for peer_id in [None, Some(toc.this_peer_id)] {
        let operation = TestSlowDown {
            peer_id,
            duration_ms: delay.as_millis() as u64,
        };
        let action = Action::TestSlowDown(operation);

        for _ in 0..2 {
            let start = std::time::Instant::now();
            toc.apply_action(action.clone()).unwrap();

            assert!(start.elapsed() >= delay);
        }
    }
}

#[test]
fn create_collection_action_replay() {
    let (storage_dir, handle, dispatcher, proposals) = new_dispatcher_with_proposals();
    create_collection(&handle, &dispatcher, "other");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let other_state = toc.collection_state("other").unwrap();
    let state = create_collection_action_state(&other_state);
    let action = Action::CreateCollection {
        collection: "test".into(),
        state: Box::new(state.clone()),
    };
    proposals.try_iter().for_each(drop);

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        assert_eq!(toc.collection_state("test").unwrap(), state);
        assert_eq!(toc.collection_state("other").unwrap(), other_state);
        let persisted =
            CollectionConfigInternal::load(&storage_dir.path().join("collections/test")).unwrap();
        assert_eq!(persisted, state.config);
        let proposal = proposals.try_recv().unwrap();
        assert_eq!(
            proposal,
            ConsensusOperations::initialize_replica("test".into(), 0, 0)
        );
        assert!(proposals.try_iter().next().is_none());
    }

    // Creation must not rebuild a shard that has already received writes
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 0,
        state: ReplicaState::Active,
    })
    .unwrap();
    let segments = local_segments(&handle, &dispatcher);
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::Single(vec![1.0; 10]),
            payload: None,
        }]),
    ));
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));
    handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        let holder = collection.shards_holder();
        let holder = holder.read().await;
        holder
            .get_shard(0)
            .unwrap()
            .update_local(operation.into(), WaitUntil::Visible, None, false)
            .measured(context)
            .await
            .unwrap();
    });

    toc.apply_action(action.clone()).unwrap();

    assert_eq!(
        segments
            .read()
            .iter()
            .map(|(_, segment)| segment.get().read().available_point_count())
            .sum::<usize>(),
        1
    );
    drop(segments);
    stop_collections(&handle, &dispatcher);
    drop(dispatcher);
    drop(handle);
    let (_storage_dir, handle, dispatcher, proposals) = open_dispatcher(storage_dir);
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    toc.apply_action(action).unwrap();

    let segments = local_segments(&handle, &dispatcher);
    assert_eq!(
        segments
            .read()
            .iter()
            .map(|(_, segment)| segment.get().read().available_point_count())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        proposals.try_recv().unwrap(),
        ConsensusOperations::initialize_replica("test".into(), 0, 0)
    );
}

#[test]
fn create_collection_action_incomplete_directory_retry() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "other");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let state = create_collection_action_state(&toc.collection_state("other").unwrap());
    let path = storage_dir.path().join("collections/test");
    fs_err::create_dir_all(path.join("0")).unwrap();
    fs_err::write(path.join("0/unfinished"), "partial collection build").unwrap();
    let blocker = storage_dir.path().join(".deleted");
    fs_err::write(&blocker, "block removal of incomplete collection").unwrap();
    let action = Action::CreateCollection {
        collection: "test".into(),
        state: Box::new(state.clone()),
    };

    let error = toc.apply_action(action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(toc.collection_state("test").is_none());
    fs_err::remove_file(blocker).unwrap();

    toc.apply_action(action).unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), state);
    assert!(!path.join("0/unfinished").exists());
}

#[test]
fn create_collection_action_different_uuid() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let initial_state = toc.collection_state("test").unwrap();
    let mut state = initial_state.clone();
    state.config.uuid = Some(uuid::Uuid::from_u128(42));

    let error = toc
        .apply_action(Action::CreateCollection {
            collection: "test".into(),
            state: Box::new(state),
        })
        .unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
}

fn create_collection_action_state(template: &collection_state::State) -> collection_state::State {
    let mut state = template.clone();
    state.config.uuid = Some(uuid::Uuid::from_u128(42));
    let metadata = serde_json::from_value(serde_json::json!({"owner": "action"})).unwrap();
    state.config.metadata = Some(metadata);
    state.config.params.shard_number = NonZeroU32::new(2).unwrap();
    state.config.hnsw_config.m = 7;
    state.config.optimizer_config.default_segment_number = 1;
    state.shards = [
        (
            0,
            ShardInfo {
                replicas: [
                    (0, ReplicaState::Initializing),
                    (1, ReplicaState::Initializing),
                ]
                .into(),
            },
        ),
        (
            1,
            ShardInfo {
                replicas: [(2, ReplicaState::Initializing)].into(),
            },
        ),
    ]
    .into();
    state
}

#[test]
fn create_and_register_shards_action_custom_replay() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_resharding_collection(&handle, &dispatcher, true, ReshardingDirection::Up);

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let field_name: JsonPath = "city".parse().unwrap();
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    toc.apply_action(Action::SetPayloadIndex {
        collection: "test".into(),
        field_name: field_name.clone(),
        field_schema: field_schema.clone(),
    })
    .unwrap();
    let before = toc.collection_state("test").unwrap();
    let shards = vec![
        (4, vec![0, 1], ReplicaState::Initializing),
        (5, vec![0], ReplicaState::Dead),
        (6, vec![2], ReplicaState::Initializing),
    ];
    let action = Action::CreateAndRegisterShards {
        collection: "test".into(),
        shard_key: Some("new".into()),
        shards: shards.clone(),
    };
    let mut expected = before;
    for (id, peers, state) in &shards {
        let info = ShardInfo {
            replicas: peers.iter().map(|&peer| (peer, *state)).collect(),
        };
        expected.shards.insert(*id, info);
    }
    expected
        .shards_key_mapping
        .entry("new".into())
        .or_default()
        .extend([4, 5, 6]);

    toc.apply_action(action.clone()).unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), expected);
    assert_shard_indexes(&handle, &dispatcher, &[4, 5], &field_name, &field_schema);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let replicas: Vec<_> = handle.block_on(async {
        holder
            .read()
            .await
            .get_shards()
            .map(|(id, shard)| (id, shard.clone()))
            .collect()
    });

    toc.apply_action(action).unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), expected);
    handle.block_on(async {
        let holder = holder.read().await;
        for (id, replica) in &replicas {
            assert!(Arc::ptr_eq(replica, holder.get_shard(*id).unwrap()));
        }
    });
    let persisted = ShardHolder::new(
        &storage_dir.path().join("collections/test"),
        ShardingMethod::Custom,
    )
    .unwrap();
    assert_eq!(
        persisted.get_shard_key_to_ids_mapping(),
        expected.shards_key_mapping
    );
}

#[test]
fn create_and_register_shards_action_build_failure_reload() {
    for cut in 1..=3 {
        let (storage_dir, handle, dispatcher) = new_dispatcher();
        create_custom_collection(&handle, &dispatcher, "test");

        let pass = new_unchecked_verification_pass();
        let toc = dispatcher.toc(&FULL_ACCESS, &pass);
        let field_name: JsonPath = "city".parse().unwrap();
        let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
        toc.apply_action(Action::SetPayloadIndex {
            collection: "test".into(),
            field_name: field_name.clone(),
            field_schema: field_schema.clone(),
        })
        .unwrap();
        let before = toc.collection_state("test").unwrap();
        let path = storage_dir.path().join("collections/test");
        let mapping_path = path.join("shard_key_mapping.json");
        let backup = path.join("mapping.backup");
        let blocker = if cut < 3 {
            let blocker = path.join((cut + 1).to_string());
            fs_err::write(&blocker, "block the next shard build").unwrap();
            blocker
        } else {
            fs_err::rename(&mapping_path, &backup).unwrap();
            fs_err::create_dir(&mapping_path).unwrap();
            mapping_path.clone()
        };
        let action = Action::CreateAndRegisterShards {
            collection: "test".into(),
            shard_key: Some("tenant".into()),
            shards: (1..=3)
                .map(|id| (id, vec![0], ReplicaState::Initializing))
                .collect(),
        };

        let error = toc.apply_action(action.clone()).unwrap_err();

        assert!(matches!(error, StorageError::ServiceError { .. }));
        assert_eq!(toc.collection_state("test").unwrap(), before);
        for id in 1..=cut {
            assert!(path.join(format!("{id}/shard_config.json")).is_file());
        }
        if cut < 3 {
            fs_err::remove_file(blocker).unwrap();
        } else {
            fs_err::remove_dir(blocker).unwrap();
            fs_err::rename(backup, mapping_path).unwrap();
        }
        stop_collections(&handle, &dispatcher);
        drop(dispatcher);
        drop(handle);
        let (_storage_dir, handle, dispatcher, _proposals) = open_dispatcher(storage_dir);
        let toc = dispatcher.toc(&FULL_ACCESS, &pass);
        assert_eq!(toc.collection_state("test").unwrap(), before);

        toc.apply_action(action.clone()).unwrap();
        toc.apply_action(action).unwrap();

        let state = toc.collection_state("test").unwrap();
        assert_eq!(state.shards.len(), 3);
        assert_eq!(state.shards_key_mapping[&"tenant".into()], [1, 2, 3].into());
        assert_shard_indexes(&handle, &dispatcher, &[1, 2, 3], &field_name, &field_schema);
        stop_collections(&handle, &dispatcher);
    }
}

#[test]
fn create_and_register_shards_action_auto_resharding_reload() {
    create_and_register_shards_action_resharding_reload(false);
}

#[test]
fn create_and_register_shards_action_custom_resharding_reload() {
    create_and_register_shards_action_resharding_reload(true);
}

fn create_and_register_shards_action_resharding_reload(custom: bool) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    let mut resharding =
        create_resharding_collection(&handle, &dispatcher, custom, ReshardingDirection::Up);
    resharding.shard_id = if custom { 4 } else { 2 };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let field_name: JsonPath = "city".parse().unwrap();
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    toc.apply_action(Action::SetPayloadIndex {
        collection: "test".into(),
        field_name: field_name.clone(),
        field_schema: field_schema.clone(),
    })
    .unwrap();
    let before = toc.collection_state("test").unwrap();
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        // Reproduce a crash after the last build but before registration. Neither the
        // custom mapping nor the auto shard count makes this directory loadable yet.
        let replica = collection
            .create_replica_set(
                resharding.shard_id,
                resharding.shard_key.clone(),
                &[0],
                Some(ReplicaState::Resharding),
            )
            .await
            .unwrap();
        replica.stop_gracefully().await;
    });
    stop_collections(&handle, &dispatcher);
    drop(dispatcher);
    drop(handle);
    let (storage_dir, handle, dispatcher, _proposals) = open_dispatcher(storage_dir);
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    assert_eq!(toc.collection_state("test").unwrap(), before);
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let old_id = if custom { 1 } else { 0 };
    let old_ring = handle.block_on(async {
        holder
            .read()
            .await
            .hash_ring_router(old_id)
            .unwrap()
            .clone()
    });
    let action = Action::CreateAndRegisterShards {
        collection: "test".into(),
        shard_key: resharding.shard_key.clone(),
        shards: vec![(resharding.shard_id, vec![0], ReplicaState::Resharding)],
    };

    toc.apply_action(action.clone()).unwrap();

    assert_shard_indexes(
        &handle,
        &dispatcher,
        &[resharding.shard_id],
        &field_name,
        &field_schema,
    );
    assert_eq!(
        handle.block_on(async {
            holder
                .read()
                .await
                .hash_ring_router(old_id)
                .unwrap()
                .clone()
        }),
        old_ring
    );
    let path = storage_dir
        .path()
        .join("collections/test/resharding_state.json");
    fs_err::write(&path, "null").unwrap();
    let backup = path.with_extension("backup");
    fs_err::rename(&path, &backup).unwrap();
    fs_err::create_dir(&path).unwrap();
    let state_action = Action::SetReshardingState {
        collection: "test".into(),
        state: Some(resharding.clone()),
    };

    let error = toc.apply_action(state_action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(
        handle.block_on(async {
            holder
                .read()
                .await
                .hash_ring_router(old_id)
                .unwrap()
                .clone()
        }),
        old_ring
    );
    fs_err::remove_dir(&path).unwrap();
    fs_err::rename(&backup, &path).unwrap();
    toc.apply_action(action).unwrap();
    toc.apply_action(state_action).unwrap();
    if !custom {
        toc.apply_action(Action::SetShardNumber {
            collection: "test".into(),
            shard_number: NonZeroU32::new(3).unwrap(),
        })
        .unwrap();
    }
    let expected = toc.collection_state("test").unwrap();
    drop(holder);
    drop(collection);
    stop_collections(&handle, &dispatcher);
    drop(dispatcher);
    drop(handle);
    let (_storage_dir, handle, dispatcher, _proposals) = open_dispatcher(storage_dir);
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    assert_eq!(toc.collection_state("test").unwrap(), expected);
    assert_shard_indexes(
        &handle,
        &dispatcher,
        &[resharding.shard_id],
        &field_name,
        &field_schema,
    );
    stop_collections(&handle, &dispatcher);
}

fn assert_shard_indexes(
    handle: &Handle,
    dispatcher: &Dispatcher,
    ids: &[u32],
    field: &JsonPath,
    schema: &PayloadFieldSchema,
) {
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    handle.block_on(async {
        let collection = toc.get_collection(&collection_pass).await.unwrap();
        let holder = collection.shards_holder();
        let holder = holder.read().await;
        for &id in ids {
            let replica = holder.get_shard(id).unwrap();
            let segments = replica.local_segments_for_test().await.unwrap();
            let segments = segments.read();
            assert!(!segments.is_empty());
            for (_, segment) in segments.iter() {
                assert_eq!(
                    segment.get().read().get_indexed_fields().get(field),
                    Some(schema),
                    "missing index on shard {id}"
                );
            }
        }
    });
}

#[test]
fn drop_collection_action_replay() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");
    create_collection(&handle, &dispatcher, "other");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::UpdateAliases {
        set: [("alias".into(), "test".into())].into(),
        remove: Default::default(),
    })
    .unwrap();
    let other_state = toc.collection_state("other").unwrap();
    let action = Action::DropCollection {
        collection: "test".into(),
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        assert!(toc.collection_state("test").is_none());
        assert!(!storage_dir.path().join("collections/test").exists());
        assert!(toc.alias_mapping().get("alias").is_none());
        assert_eq!(toc.collection_state("other").unwrap(), other_state);
    }
}

#[test]
fn drop_collection_action_does_not_resolve_aliases() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::UpdateAliases {
        set: [("alias".into(), "test".into())].into(),
        remove: Default::default(),
    })
    .unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    let aliases = toc.alias_mapping();

    toc.apply_action(Action::DropCollection {
        collection: "alias".into(),
    })
    .unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    assert_eq!(toc.alias_mapping(), aliases);
}

#[test]
fn drop_collection_action_delete_failure_retry() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let deleted_path = storage_dir.path().join(".deleted");
    fs_err::write(&deleted_path, "block collection deletion").unwrap();
    let action = Action::DropCollection {
        collection: "test".into(),
    };

    let error = toc.apply_action(action.clone()).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(toc.collection_state("test").is_none());
    assert!(storage_dir.path().join("collections/test").exists());
    fs_err::remove_file(deleted_path).unwrap();

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        assert!(toc.collection_state("test").is_none());
        assert!(!storage_dir.path().join("collections/test").exists());
    }
}

#[cfg(feature = "staging")]
#[test]
fn test_slow_down_action_other_peer() {
    use storage::content_manager::collection_meta_ops::TestSlowDown;

    let (_storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass).clone();
    let operation = TestSlowDown {
        peer_id: Some(toc.this_peer_id + 1),
        // An incorrectly targeted sleep cannot finish within the receive timeout
        duration_ms: u64::MAX,
    };
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let action = Action::TestSlowDown(operation);
        for _ in 0..2 {
            toc.apply_action(action.clone()).unwrap();
        }
        sender.send(()).unwrap();
    });

    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    task.join().unwrap();
}

#[test]
#[cfg(feature = "staging")]
fn test_transient_error_action_replay() {
    use storage::content_manager::collection_meta_ops::TestTransientError;

    let (_storage_dir, _handle, dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);

    // The endpoints make the draw deterministic while still exercising the applier
    for peer_id in [None, Some(toc.this_peer_id), Some(toc.this_peer_id + 1)] {
        for probability in [0, 100] {
            let operation = TestTransientError {
                peer_id,
                failure_probability_percent: probability,
            };
            let action = Action::TestTransientError(operation);
            let should_fail = probability == 100 && peer_id.is_none_or(|id| id == toc.this_peer_id);

            for _ in 0..2 {
                let result = toc.apply_action(action.clone());

                if should_fail {
                    let error = result.unwrap_err();
                    assert!(matches!(error, StorageError::ServiceError { .. }));
                    assert!(
                        error
                            .to_string()
                            .contains("simulated transient consensus error")
                    );
                } else {
                    result.unwrap();
                }
            }
        }
    }
}

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
fn update_collection_config_action_preserves_healthy_and_restarts_failed_optimizers() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let replica_set = handle.block_on(async {
        collection
            .shards_holder()
            .read()
            .await
            .get_shard(0)
            .unwrap()
            .clone()
    });
    let initial = handle
        .block_on(replica_set.local_optimizers_for_test())
        .unwrap();
    let m = toc.collection_state("test").unwrap().config.hnsw_config.m;
    let unchanged = Action::UpdateCollectionConfig {
        collection: "test".into(),
        diff: Box::new(CollectionConfigDiff::Hnsw(HnswConfigDiff {
            m: Some(m),
            ..Default::default()
        })),
    };

    toc.apply_action(unchanged.clone()).unwrap();
    handle.block_on(async { tokio::time::sleep(Duration::from_millis(100)).await });

    let current = handle
        .block_on(replica_set.local_optimizers_for_test())
        .unwrap();
    assert!(
        Arc::ptr_eq(&initial, &current),
        "unchanged healthy config must retain optimizers"
    );
    let segments = handle
        .block_on(replica_set.local_segments_for_test())
        .unwrap();
    segments
        .write()
        .report_optimizer_error("injected optimizer error");

    toc.apply_action(unchanged).unwrap();

    handle.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = replica_set.local_optimizers_for_test().await.unwrap();
                if !Arc::ptr_eq(&initial, &current) && !collection.has_optimizer_errors().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("unchanged config must restart failed optimizers");
    });
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
fn remove_shard_key_action_replay() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_custom_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.get_channel_service()
        .id_to_address
        .write()
        .insert(0, "http://localhost:6335".parse().unwrap());

    for (key, placement) in [("tenant", vec![vec![0], vec![0]]), ("other", vec![vec![0]])] {
        CollectionContainer::perform_collection_meta_op(
            toc.as_ref(),
            CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: "test".into(),
                shard_key: key.into(),
                placement,
                initial_state: Some(ReplicaState::Active),
            }),
        )
        .unwrap();
    }

    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let before = toc.collection_state("test").unwrap();
    let mut expected_mapping = before.shards_key_mapping.clone();
    let removed_ids = expected_mapping.remove(&ShardKey::from("tenant")).unwrap();
    let action = Action::RemoveShardKey {
        collection: "test".into(),
        shard_key: "tenant".into(),
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        let state = toc.collection_state("test").unwrap();
        assert_eq!(state.shards_key_mapping, expected_mapping);
        assert_eq!(state.shards, before.shards);
        handle.block_on(async {
            let holder = holder.read().await;
            assert!(
                removed_ids
                    .iter()
                    .all(|id| !holder.get_shard_id_to_key_mapping().contains_key(id))
            );
            for id in &removed_ids {
                assert!(holder.hash_ring_router(*id).is_none());
            }
            let other_id = *expected_mapping[&ShardKey::from("other")]
                .iter()
                .next()
                .unwrap();
            assert!(holder.hash_ring_router(other_id).is_some());
        });

        for id in &removed_ids {
            let error = handle
                .block_on(collection.cleanup_local_shard(*id, true, Some(Duration::from_secs(5))))
                .unwrap_err();
            assert!(
                error.to_string().contains("failed to get shard hash ring"),
                "{error}"
            );
        }

        let persisted = ShardHolder::new(
            &storage_dir.path().join("collections/test"),
            ShardingMethod::Custom,
        )
        .unwrap();
        assert_eq!(persisted.get_shard_key_to_ids_mapping(), expected_mapping);
        for id in &removed_ids {
            assert!(
                storage_dir
                    .path()
                    .join(format!("collections/test/{id}/replica_state.json"))
                    .is_file()
            );
        }
    }

    for id in removed_ids {
        toc.apply_action(Action::DropShard {
            collection: "test".into(),
            shard_id: id,
        })
        .unwrap();
        assert!(
            !storage_dir
                .path()
                .join(format!("collections/test/{id}"))
                .exists()
        );
    }
    assert_eq!(toc.collection_state("test").unwrap().shards.len(), 1);
}

#[test]
fn start_resharding_custom_scale_up_save_failure_retry() {
    start_resharding_scale_up_save_failure_retry(true);
}

#[test]
fn start_resharding_auto_scale_up_save_failure_retry() {
    start_resharding_scale_up_save_failure_retry(false);
}

fn start_resharding_scale_up_save_failure_retry(custom: bool) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    let mut state =
        create_resharding_collection(&handle, &dispatcher, custom, ReshardingDirection::Up);
    state.shard_id = if custom { 4 } else { 2 };
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let new_shard = handle
        .block_on(collection.create_replica_set(
            state.shard_id,
            state.shard_key.clone(),
            &[0],
            Some(ReplicaState::Resharding),
        ))
        .unwrap();
    let path = storage_dir
        .path()
        .join("collections/test/resharding_state.json");
    fs_err::write(&path, "null").unwrap();
    let backup = path.with_extension("backup");
    fs_err::rename(&path, &backup).unwrap();
    fs_err::create_dir(&path).unwrap();

    handle.block_on(async {
        let mut holder = holder.write().await;
        let old_id = if custom { 1 } else { 0 };
        let old_ring = holder.hash_ring_router(old_id).unwrap().clone();

        assert!(
            holder
                .start_resharding_unchecked(state.key(), Some(new_shard))
                .await
                .is_err()
        );

        assert!(holder.contains_shard(state.shard_id));
        assert_eq!(holder.resharding_state(), None);
        assert_eq!(holder.hash_ring_router(old_id).unwrap(), &old_ring);

        fs_err::remove_dir(&path).unwrap();
        fs_err::rename(&backup, &path).unwrap();
        let mut expected_ring = old_ring;
        expected_ring.start_resharding(state.shard_id, ReshardingDirection::Up);
        for _ in 0..2 {
            holder.check_start_resharding(&state.key()).unwrap();
            holder
                .start_resharding_unchecked(state.key(), None)
                .await
                .unwrap();

            assert_eq!(holder.resharding_state(), Some(state.clone()));
            assert_eq!(holder.hash_ring_router(old_id).unwrap(), &expected_ring);
        }

        // A failed replay of Start must also preserve an existing dual ring and stage
        holder.commit_read_hashring(&state.key()).unwrap();
        let before = holder.resharding_state();
        fs_err::rename(&path, &backup).unwrap();
        fs_err::create_dir(&path).unwrap();

        assert!(
            holder
                .start_resharding_unchecked(state.key(), None)
                .await
                .is_err()
        );
        assert_eq!(holder.resharding_state(), before);
        assert_eq!(holder.hash_ring_router(old_id).unwrap(), &expected_ring);

        fs_err::remove_dir(&path).unwrap();
        fs_err::rename(&backup, &path).unwrap();
        holder
            .start_resharding_unchecked(state.key(), None)
            .await
            .unwrap();
        assert_eq!(holder.resharding_state(), Some(state.clone()));
        assert_eq!(holder.hash_ring_router(old_id).unwrap(), &expected_ring);
    });

    let sharding = if custom {
        ShardingMethod::Custom
    } else {
        ShardingMethod::Auto
    };
    let persisted =
        ShardHolder::new(&storage_dir.path().join("collections/test"), sharding).unwrap();
    assert_eq!(persisted.resharding_state(), Some(state));
}

#[test]
fn set_resharding_state_action_custom_scale_up_replay() {
    set_resharding_state_action_replay(true, ReshardingDirection::Up);
}

#[test]
fn set_resharding_state_action_custom_scale_down_replay() {
    set_resharding_state_action_replay(true, ReshardingDirection::Down);
}

#[test]
fn set_resharding_state_action_auto_scale_up_replay() {
    set_resharding_state_action_replay(false, ReshardingDirection::Up);
}

#[test]
fn set_resharding_state_action_auto_scale_down_replay() {
    set_resharding_state_action_replay(false, ReshardingDirection::Down);
}

fn set_resharding_state_action_replay(custom: bool, direction: ReshardingDirection) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    let state = create_resharding_collection(&handle, &dispatcher, custom, direction);
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let mut expected = toc.collection_state("test").unwrap();
    let mut expected_ring = handle.block_on(async {
        holder
            .read()
            .await
            .hash_ring_router(state.shard_id)
            .unwrap()
            .clone()
    });
    expected_ring.start_resharding(state.shard_id, direction);
    let sharding = if custom {
        ShardingMethod::Custom
    } else {
        ShardingMethod::Auto
    };

    // The action carries a complete state, including stages not emitted by Start
    for stage in [
        ReshardingStage::MigratingPoints,
        ReshardingStage::ReadHashRingCommitted,
        ReshardingStage::WriteHashRingCommitted,
    ] {
        let mut state = state.clone();
        state.stage = stage;
        expected.resharding = Some(state.clone());
        let mut ring = expected_ring.clone();
        if stage == ReshardingStage::WriteHashRingCommitted {
            ring.commit_resharding();
        }
        let action = Action::SetReshardingState {
            collection: "test".into(),
            state: Some(state),
        };

        for _ in 0..2 {
            toc.apply_action(action.clone()).unwrap();

            assert_eq!(toc.collection_state("test").unwrap(), expected);
            handle.block_on(async {
                let holder = holder.read().await;
                assert_eq!(
                    holder.hash_ring_router(if custom { 1 } else { 0 }).unwrap(),
                    &ring
                );
                if custom {
                    assert!(!holder.hash_ring_router(3).unwrap().is_resharding());
                }
            });
            let persisted =
                ShardHolder::new(&storage_dir.path().join("collections/test"), sharding).unwrap();
            assert_eq!(persisted.resharding_state(), expected.resharding);
        }
    }

    // Clearing state must preserve the ring prepared by finish or abort
    let ring = handle.block_on(async {
        holder
            .read()
            .await
            .hash_ring_router(if custom { 1 } else { 0 })
            .unwrap()
            .clone()
    });
    expected.resharding = None;
    for _ in 0..2 {
        toc.apply_action(Action::SetReshardingState {
            collection: "test".into(),
            state: None,
        })
        .unwrap();

        assert_eq!(toc.collection_state("test").unwrap(), expected);
        handle.block_on(async {
            assert_eq!(
                holder
                    .read()
                    .await
                    .hash_ring_router(if custom { 1 } else { 0 })
                    .unwrap(),
                &ring
            );
        });
        let persisted =
            ShardHolder::new(&storage_dir.path().join("collections/test"), sharding).unwrap();
        assert_eq!(persisted.resharding_state(), None);
    }
}

#[test]
fn set_resharding_stage_action_custom_scale_up_replay() {
    set_resharding_stage_action_replay(true, ReshardingDirection::Up);
}

#[test]
fn set_resharding_stage_action_custom_scale_down_replay() {
    set_resharding_stage_action_replay(true, ReshardingDirection::Down);
}

#[test]
fn set_resharding_stage_action_auto_scale_up_replay() {
    set_resharding_stage_action_replay(false, ReshardingDirection::Up);
}

#[test]
fn set_resharding_stage_action_auto_scale_down_replay() {
    set_resharding_stage_action_replay(false, ReshardingDirection::Down);
}

fn set_resharding_stage_action_replay(custom: bool, direction: ReshardingDirection) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    let state = create_resharding_collection(&handle, &dispatcher, custom, direction);
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::SetReshardingState {
        collection: "test".into(),
        state: Some(state),
    })
    .unwrap();
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let mut expected = toc.collection_state("test").unwrap();
    let mut expected_ring = handle.block_on(async {
        holder
            .read()
            .await
            .hash_ring_router(if custom { 1 } else { 0 })
            .unwrap()
            .clone()
    });
    let sharding = if custom {
        ShardingMethod::Custom
    } else {
        ShardingMethod::Auto
    };

    for stage in [
        ReshardingStage::ReadHashRingCommitted,
        ReshardingStage::WriteHashRingCommitted,
    ] {
        expected.resharding.as_mut().unwrap().stage = stage;
        if stage == ReshardingStage::WriteHashRingCommitted {
            expected_ring.commit_resharding();
        }
        let action = Action::SetReshardingStage {
            collection: "test".into(),
            stage,
        };

        for _ in 0..2 {
            toc.apply_action(action.clone()).unwrap();

            assert_eq!(toc.collection_state("test").unwrap(), expected);
            handle.block_on(async {
                let holder = holder.read().await;
                assert_eq!(
                    holder.hash_ring_router(if custom { 1 } else { 0 }).unwrap(),
                    &expected_ring
                );
                if custom {
                    assert!(!holder.hash_ring_router(3).unwrap().is_resharding());
                }
            });
            let persisted =
                ShardHolder::new(&storage_dir.path().join("collections/test"), sharding).unwrap();
            assert_eq!(persisted.resharding_state(), expected.resharding);
        }
    }
}

#[test]
fn set_resharding_stage_action_missing_state() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let before = toc.collection_state("test").unwrap();

    let error = toc
        .apply_action(Action::SetReshardingStage {
            collection: "test".into(),
            stage: ReshardingStage::ReadHashRingCommitted,
        })
        .unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(toc.collection_state("test").unwrap(), before);
}

fn create_resharding_collection(
    handle: &Handle,
    dispatcher: &Dispatcher,
    custom: bool,
    direction: ReshardingDirection,
) -> ReshardState {
    if custom {
        create_custom_collection(handle, dispatcher, "test");
    } else {
        create_collection(handle, dispatcher, "test");
    }
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    if custom {
        toc.get_channel_service()
            .id_to_address
            .write()
            .insert(0, "http://localhost:6335".parse().unwrap());
        for (key, placement) in [("tenant", vec![vec![0], vec![0]]), ("other", vec![vec![0]])] {
            CollectionContainer::perform_collection_meta_op(
                toc.as_ref(),
                CollectionMetaOperations::CreateShardKey(CreateShardKey {
                    collection_name: "test".into(),
                    shard_key: key.into(),
                    placement,
                    initial_state: Some(ReplicaState::Active),
                }),
            )
            .unwrap();
        }
    } else {
        let collection_pass = FULL_ACCESS
            .check_collection_access("test", AccessRequirements::new(), "test")
            .unwrap();
        let collection = handle
            .block_on(toc.get_collection(&collection_pass))
            .unwrap();
        handle.block_on(async {
            let shard = collection
                .create_replica_set(1, None, &[0], Some(ReplicaState::Active))
                .await
                .unwrap();
            collection
                .shards_holder()
                .write()
                .await
                .add_shard(1, shard, None)
                .await
                .unwrap();
        });
        toc.apply_action(Action::SetShardNumber {
            collection: "test".into(),
            shard_number: NonZeroU32::new(2).unwrap(),
        })
        .unwrap();
    }

    ReshardState::new(
        uuid::Uuid::new_v4(),
        direction,
        0,
        if custom { 2 } else { 1 },
        custom.then(|| "tenant".into()),
    )
}

#[test]
fn delete_migrated_points_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_custom_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    for peer_id in [0, 1] {
        toc.get_channel_service()
            .id_to_address
            .write()
            .insert(peer_id, "http://localhost:6335".parse().unwrap());
    }

    for (key, placement) in [
        ("tenant", vec![vec![0], vec![0], vec![1]]),
        ("other", vec![vec![0], vec![0]]),
    ] {
        CollectionContainer::perform_collection_meta_op(
            toc.as_ref(),
            CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: "test".into(),
                shard_key: key.into(),
                placement,
                initial_state: Some(ReplicaState::Active),
            }),
        )
        .unwrap();
    }

    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));
    let point_ids: Vec<ExtendedPointId> = (0..64u64).map(Into::into).collect();
    let points = point_ids
        .iter()
        .map(|&id| PointStructPersisted {
            id,
            vector: VectorStructPersisted::Single(vec![1.0; 10]),
            payload: None,
        })
        .collect();
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    handle
        .block_on(
            collection
                .update_all_local(operation, WaitUntil::Visible, true)
                .measured(context.clone()),
        )
        .unwrap();

    // Cleanup must force deletion even after a surviving replica has become dead
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 1,
        peer_id: 0,
        state: ReplicaState::Dead,
    })
    .unwrap();
    let key = ReshardKey {
        uuid: uuid::Uuid::new_v4(),
        direction: ReshardingDirection::Down,
        peer_id: 0,
        shard_id: 2,
        shard_key: Some("tenant".into()),
    };
    let old_ring = handle.block_on(async {
        let mut holder = holder.write().await;
        let old_ring = holder.hash_ring_router(1).unwrap().clone();
        holder
            .start_resharding_unchecked(key.clone(), None)
            .await
            .unwrap();
        assert!(!holder.get_shard(3).unwrap().has_local_shard().await);
        old_ring
    });
    let expected_ids: Vec<_> = point_ids
        .iter()
        .copied()
        .filter(|id| old_ring.is_in_shard(id, 1))
        .collect();
    assert!(!expected_ids.is_empty());
    assert!(expected_ids.len() < point_ids.len());
    let before = toc.collection_state("test").unwrap();
    let action = Action::DeleteMigratedPoints {
        collection: "test".into(),
        key,
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        handle.block_on(async {
            let holder = holder.read().await;
            // Shard 2 is the scale-down target; shards 4 and 5 belong to another key.
            // They retain every point, while shard 1 keeps only its old-ring points.
            for (id, expected) in [
                (1, &expected_ids),
                (2, &point_ids),
                (4, &point_ids),
                (5, &point_ids),
            ] {
                let replica_set = holder.get_shard(id).unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let records = replica_set
                            .local_scroll_by_id(
                                None,
                                65,
                                &false.into(),
                                &false.into(),
                                None,
                                None,
                                None,
                                DeferredBehavior::WithDeferred,
                            )
                            .measured(context.clone())
                            .await
                            .unwrap();
                        let ids: Vec<_> = records.into_iter().map(|record| record.id).collect();
                        if &ids == expected {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("local points did not converge after cleanup");
            }
        });
        assert_eq!(toc.collection_state("test").unwrap(), before);
    }
}

#[test]
fn revert_hash_ring_action_scale_up_replay() {
    revert_hash_ring_action_replay(ReshardingDirection::Up);
}

#[test]
fn revert_hash_ring_action_scale_down_replay() {
    revert_hash_ring_action_replay(ReshardingDirection::Down);
}

fn revert_hash_ring_action_replay(direction: ReshardingDirection) {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_custom_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.get_channel_service()
        .id_to_address
        .write()
        .insert(0, "http://localhost:6335".parse().unwrap());

    for (key, placement) in [("tenant", vec![vec![0], vec![0]]), ("other", vec![vec![0]])] {
        CollectionContainer::perform_collection_meta_op(
            toc.as_ref(),
            CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: "test".into(),
                shard_key: key.into(),
                placement,
                initial_state: Some(ReplicaState::Active),
            }),
        )
        .unwrap();
    }

    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let key = ReshardKey {
        uuid: uuid::Uuid::new_v4(),
        direction,
        peer_id: 0,
        shard_id: 2,
        shard_key: Some("tenant".into()),
    };
    let other_ring = handle.block_on(async {
        let mut holder = holder.write().await;
        holder
            .start_resharding_unchecked(key.clone(), None)
            .await
            .unwrap();
        assert!(holder.hash_ring_router(1).unwrap().is_resharding());
        holder.hash_ring_router(3).unwrap().clone()
    });
    let before = toc.collection_state("test").unwrap();
    let action = Action::RevertHashRing {
        collection: "test".into(),
        key,
    };
    let mut expected_ring = HashRingRouter::single();
    expected_ring.add(1);
    if direction == ReshardingDirection::Down {
        expected_ring.add(2);
    }

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        handle.block_on(async {
            let holder = holder.read().await;
            assert_eq!(holder.hash_ring_router(1).unwrap(), &expected_ring);
            assert_eq!(holder.hash_ring_router(3).unwrap(), &other_ring);
        });
        assert_eq!(toc.collection_state("test").unwrap(), before);
    }
}

#[test]
fn revert_hash_ring_action_missing_ring() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let before = toc.collection_state("test").unwrap();
    let key = ReshardKey {
        uuid: uuid::Uuid::new_v4(),
        direction: ReshardingDirection::Up,
        peer_id: 0,
        shard_id: 1,
        shard_key: Some("missing".into()),
    };
    let action = Action::RevertHashRing {
        collection: "test".into(),
        key,
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        assert_eq!(toc.collection_state("test").unwrap(), before);
    }
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

#[test]
fn unregister_transfer_action_finish_replay() {
    unregister_transfer_action_replay(TransferOutcome::Finish);
}

#[test]
fn unregister_transfer_action_abort_replay() {
    unregister_transfer_action_replay(TransferOutcome::Abort);
}

fn unregister_transfer_action_replay(outcome: TransferOutcome) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let transfer = ordinary_transfer();
    let mut unrelated = ordinary_transfer();
    unrelated.to = 2;
    for transfer in [&transfer, &unrelated] {
        toc.apply_action(Action::RegisterTransfer {
            collection: "test".into(),
            transfer: transfer.clone(),
        })
        .unwrap();
    }
    let initial_shards = toc.collection_state("test").unwrap().shards;
    let expected = HashSet::from([unrelated]);
    let action = Action::UnregisterTransfer {
        collection: "test".into(),
        key: transfer.key(),
        outcome,
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        let state = toc.collection_state("test").unwrap();
        assert_eq!(state.transfers, expected);
        assert_eq!(state.shards, initial_shards);

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
fn init_local_shard_action_ensure_exists_replay() {
    init_local_shard_action_replay(LocalShardInitMode::EnsureExists);
}

#[test]
fn init_local_shard_action_reset_to_empty_replay() {
    init_local_shard_action_replay(LocalShardInitMode::ResetToEmpty);
}

fn init_local_shard_action_replay(mode: LocalShardInitMode) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let replica_set = handle.block_on(async { holder.read().await.get_shard(0).unwrap().clone() });
    let field_name: JsonPath = "city".parse().unwrap();
    let field_schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    toc.apply_action(Action::SetPayloadIndex {
        collection: "test".into(),
        field_name: field_name.clone(),
        field_schema: field_schema.clone(),
    })
    .unwrap();
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 1,
        state: ReplicaState::Dead,
    })
    .unwrap();
    handle.block_on(replica_set.remove_local()).unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    let action = Action::InitLocalShard {
        collection: "test".into(),
        shard_id: 0,
        mode,
    };

    toc.apply_action(action.clone()).unwrap();

    assert!(handle.block_on(replica_set.has_local_shard()));
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 0,
        state: ReplicaState::Active,
    })
    .unwrap();
    let expected_state = toc.collection_state("test").unwrap();
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));

    if mode == LocalShardInitMode::EnsureExists {
        // A transfer may already have copied indexes before consensus replays Start
        toc.apply_action(Action::SetPayloadIndex {
            collection: "test".into(),
            field_name: field_name.clone(),
            field_schema: field_schema.clone(),
        })
        .unwrap();
        let segments = local_segments(&handle, &dispatcher);
        wait_for_local_segments(&handle, &segments, |holder| {
            holder.iter().all(|(_, segment)| {
                segment.get().read().get_indexed_fields().get(&field_name) == Some(&field_schema)
            })
        });
    }

    for _ in 0..2 {
        let point = PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::Single(vec![1.0; 10]),
            payload: None,
        };
        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        handle
            .block_on(
                collection
                    .update_from_client_simple(operation, true, None, WriteOrdering::default())
                    .measured(context.clone()),
            )
            .unwrap();

        toc.apply_action(action.clone()).unwrap();

        let request = Arc::new(CountRequestInternal {
            filter: None,
            exact: true,
        });
        let count = handle
            .block_on(
                replica_set
                    .count_local(request, None, DeferredBehavior::VisibleOnly)
                    .measured(context.clone()),
            )
            .unwrap()
            .unwrap();
        let expected_count = usize::from(mode == LocalShardInitMode::EnsureExists);
        assert_eq!(count.count, expected_count);
        assert_eq!(toc.collection_state("test").unwrap(), expected_state);
        let state_file = fs_err::File::open(
            storage_dir
                .path()
                .join("collections/test/0/replica_state.json"),
        )
        .unwrap();
        let persisted: ReplicaSetState = serde_json::from_reader(state_file).unwrap();
        assert_eq!(persisted.peers(), &expected_state.shards[&0].replicas);
        let segments = local_segments(&handle, &dispatcher);
        let expected_schema = match mode {
            LocalShardInitMode::EnsureExists => Some(&field_schema),
            LocalShardInitMode::ResetToEmpty => None,
        };
        wait_for_local_segments(&handle, &segments, |holder| {
            holder.iter().all(|(_, segment)| {
                segment.get().read().get_indexed_fields().get(&field_name) == expected_schema
            })
        });
    }
}

#[test]
fn init_local_shard_action_missing_shard() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let initial_state = toc.collection_state("test").unwrap();

    for mode in [
        LocalShardInitMode::EnsureExists,
        LocalShardInitMode::ResetToEmpty,
    ] {
        let action = Action::InitLocalShard {
            collection: "test".into(),
            shard_id: 99,
            mode,
        };
        let error = toc.apply_action(action).unwrap_err();

        assert!(matches!(error, StorageError::ServiceError { .. }));
        assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    }
}

#[test]
fn spawn_transfer_driver_action_replay() {
    let (storage_dir, handle, dispatcher, proposals) = new_dispatcher_with_proposals();
    create_collection(&handle, &dispatcher, "test");
    let _consensus = setup_transfer_consensus(&dispatcher, storage_dir.path());

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let transfer = ordinary_transfer();
    let mut unrelated = transfer.clone();
    unrelated.to = 2;
    for transfer in [&transfer, &unrelated] {
        toc.apply_action(Action::RegisterTransfer {
            collection: "test".into(),
            transfer: transfer.clone(),
        })
        .unwrap();
    }
    let initial_state = toc.collection_state("test").unwrap();
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    proposals.try_iter().for_each(drop);

    toc.apply_action(Action::SpawnTransferDriver {
        collection: "test".into(),
        transfer: unrelated.clone(),
    })
    .unwrap();
    let action = Action::SpawnTransferDriver {
        collection: "test".into(),
        transfer: transfer.clone(),
    };

    // Drivers wait for a leader, so replay must cancel and await a running task
    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();
    }

    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    for transfer in [&transfer, &unrelated] {
        let result = handle.block_on(collection.stop_shard_transfer(&transfer.key()));
        assert!(result.is_some(), "driver was not spawned for {transfer:?}");
        assert!(
            handle
                .block_on(collection.stop_shard_transfer(&transfer.key()))
                .is_none()
        );
    }
    assert!(proposals.try_iter().next().is_none());

    // A restarted consensus thread can replay after all node-local tasks were lost
    toc.apply_action(action).unwrap();
    assert!(
        handle
            .block_on(collection.stop_shard_transfer(&transfer.key()))
            .is_some()
    );
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
}

#[test]
fn spawn_transfer_driver_action_reports_failure() {
    let (storage_dir, handle, dispatcher, proposals) = new_dispatcher_with_proposals();
    create_collection(&handle, &dispatcher, "test");
    let consensus = setup_transfer_consensus(&dispatcher, storage_dir.path());
    consensus.is_leader_established.make_ready();

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let transfer = ordinary_transfer();
    toc.apply_action(Action::RegisterTransfer {
        collection: "test".into(),
        transfer: transfer.clone(),
    })
    .unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    proposals.try_iter().for_each(drop);

    // The destination has no address, so every driver attempt fails without networking
    toc.apply_action(Action::SpawnTransferDriver {
        collection: "test".into(),
        transfer: transfer.clone(),
    })
    .unwrap();

    let proposal = proposals.recv_timeout(Duration::from_secs(10)).unwrap();
    let expected =
        ConsensusOperations::abort_transfer("test".into(), transfer.clone(), "transmission failed");
    assert_eq!(proposal, expected);
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    toc.apply_action(Action::StopTransferDriver {
        collection: "test".into(),
        key: transfer.key(),
    })
    .unwrap();
}

#[test]
fn spawn_transfer_driver_action_missing_dispatcher() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let action = Action::SpawnTransferDriver {
        collection: "test".into(),
        transfer: ordinary_transfer(),
    };
    let error = toc.apply_action(action).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(error.to_string().contains("without a consensus dispatcher"));
}

#[test]
fn spawn_transfer_driver_action_missing_method() {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");
    let _consensus = setup_transfer_consensus(&dispatcher, storage_dir.path());

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let mut transfer = ordinary_transfer();
    transfer.method = None;
    let action = Action::SpawnTransferDriver {
        collection: "test".into(),
        transfer,
    };
    let error = toc.apply_action(action).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert!(error.to_string().contains("without a transfer method"));
}

fn setup_transfer_consensus(
    dispatcher: &Dispatcher,
    storage_path: &std::path::Path,
) -> ConsensusStateRef {
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let mut persistent = Persistent::load_or_init(storage_path, true, false, None).unwrap();
    persistent.this_peer_id = toc.this_peer_id;
    let (sender, _receiver) = std::sync::mpsc::channel();
    let consensus = ConsensusManager::new(
        persistent,
        toc.clone(),
        OperationSender::new(sender),
        storage_path,
    )
    .unwrap();
    let consensus = ConsensusStateRef::from(consensus);
    toc.with_toc_dispatcher(TocDispatcher::new(Arc::downgrade(toc), consensus.clone()));

    consensus
}

#[test]
fn stop_transfer_driver_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let transfer = ordinary_transfer();
    toc.apply_action(Action::RegisterTransfer {
        collection: "test".into(),
        transfer: transfer.clone(),
    })
    .unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    let action = Action::StopTransferDriver {
        collection: "test".into(),
        key: transfer.key(),
    };

    // A restarted node has the transfer record but no driver task
    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
}

#[test]
fn invalidate_clean_local_shards_action_replay() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let result = handle
        .block_on(collection.cleanup_local_shard(0, true, Some(Duration::from_secs(5))))
        .unwrap();
    assert_eq!(result.status, UpdateStatus::Completed);
    assert!(collection.clean_local_shards_statuses().contains_key(&0));
    let initial_state = toc.collection_state("test").unwrap();
    let action = Action::InvalidateCleanLocalShards {
        collection: "test".into(),
        shard_ids: vec![0, 99],
    };

    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    assert!(collection.clean_local_shards_statuses().is_empty());
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
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

#[test]
fn remove_replica_action_local_replay() {
    remove_replica_action_replay(0);
}

#[test]
fn remove_replica_action_remote_replay() {
    remove_replica_action_replay(1);
}

fn remove_replica_action_replay(peer_id: u64) {
    let (storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    for peer_id in [1, 2] {
        toc.apply_action(Action::SetReplicaState {
            collection: "test".into(),
            shard_id: 0,
            peer_id,
            state: ReplicaState::Active,
        })
        .unwrap();
    }
    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let replica_set = handle.block_on(async { holder.read().await.get_shard(0).unwrap().clone() });
    let mut expected = toc.collection_state("test").unwrap();
    expected
        .shards
        .get_mut(&0)
        .unwrap()
        .replicas
        .remove(&peer_id);
    let action = Action::RemoveReplica {
        collection: "test".into(),
        shard_id: 0,
        peer_id,
    };

    for _ in 0..2 {
        toc.apply_action(action.clone()).unwrap();

        assert_eq!(toc.collection_state("test").unwrap(), expected);
        assert_eq!(handle.block_on(replica_set.has_local_shard()), peer_id != 0);

        let state_file = fs_err::File::open(
            storage_dir
                .path()
                .join("collections/test/0/replica_state.json"),
        )
        .unwrap();
        let persisted: ReplicaSetState = serde_json::from_reader(state_file).unwrap();
        assert_eq!(persisted.peers(), &expected.shards[&0].replicas);
        assert_eq!(persisted.is_local, peer_id != 0);
    }
}

#[test]
fn remove_replica_action_missing_shard() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let action = Action::RemoveReplica {
        collection: "test".into(),
        shard_id: 99,
        peer_id: 0,
    };

    let error = toc.apply_action(action).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
}

#[test]
fn revert_proxy_shard_action_forward_replay() {
    proxy_action_replay(
        Action::RevertProxyShard {
            collection: "test".into(),
            shard_id: 0,
        },
        false,
    );
}

#[test]
fn revert_proxy_shard_action_queue_replay() {
    proxy_action_replay(
        Action::RevertProxyShard {
            collection: "test".into(),
            shard_id: 0,
        },
        true,
    );
}

fn proxy_action_replay(action: Action, queue: bool) {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::SetReplicaState {
        collection: "test".into(),
        shard_id: 0,
        peer_id: 0,
        state: ReplicaState::Active,
    })
    .unwrap();

    let collection_pass = FULL_ACCESS
        .check_collection_access("test", AccessRequirements::new(), "test")
        .unwrap();
    let collection = handle
        .block_on(toc.get_collection(&collection_pass))
        .unwrap();
    let holder = collection.shards_holder();
    let replica_set = handle.block_on(async { holder.read().await.get_shard(0).unwrap().clone() });
    let remote = RemoteShard::new(
        0,
        "test".into(),
        1,
        ChannelService::new(6333, false, None, None),
    );
    let context = AmbientContext::request("test", toc.get_collection_hw_metrics("test".into()));
    let point = PointStructPersisted {
        id: 1.into(),
        vector: VectorStructPersisted::Single(vec![1.0; 10]),
        payload: None,
    };
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));

    handle.block_on(async {
        let upsert = collection
            .update_from_client_simple(operation, true, None, WriteOrdering::default())
            .measured(context.clone());

        if queue {
            let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
            replica_set
                .queue_proxify_local(remote, None, progress)
                .await
                .unwrap();
            upsert.await.unwrap();
        } else {
            upsert.await.unwrap();
            replica_set.proxify_local(remote, None, None).await.unwrap();
        }

        assert!(replica_set.is_proxy().await);
        assert_eq!(replica_set.is_queue_proxy().await, queue);
    });
    let initial_state = toc.collection_state("test").unwrap();
    let sends_queued_updates = queue && matches!(action, Action::UnproxifyShard { .. });

    // Peer 1 has no address. Reverting a queue proxy discards pending remote updates;
    // unproxifying attempts to send them and fails. Both keep the local points.
    for attempt in 0..2 {
        let result = toc.apply_action(action.clone());
        if attempt == 0 && sends_queued_updates {
            let error = result.unwrap_err();
            assert!(matches!(error, StorageError::ServiceError { .. }));
        } else {
            result.unwrap();
        }

        handle.block_on(async {
            assert!(!replica_set.is_proxy().await);
            assert!(replica_set.is_local().await);

            let request = Arc::new(CountRequestInternal {
                filter: None,
                exact: true,
            });
            let count = replica_set
                .count_local(request, None, DeferredBehavior::VisibleOnly)
                .measured(context.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(count.count, 1);
        });
        assert_eq!(toc.collection_state("test").unwrap(), initial_state);
    }
}

#[test]
fn revert_proxy_shard_action_missing_shard() {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    toc.apply_action(Action::DropShard {
        collection: "test".into(),
        shard_id: 0,
    })
    .unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    let action = Action::RevertProxyShard {
        collection: "test".into(),
        shard_id: 0,
    };

    toc.apply_action(action.clone()).unwrap();
    toc.apply_action(action).unwrap();

    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
}

#[test]
fn unproxify_shard_action_forward_replay() {
    proxy_action_replay(
        Action::UnproxifyShard {
            collection: "test".into(),
            shard_id: 0,
        },
        false,
    );
}

#[test]
fn unproxify_shard_action_queue_send_failure() {
    proxy_action_replay(
        Action::UnproxifyShard {
            collection: "test".into(),
            shard_id: 0,
        },
        true,
    );
}

#[test]
fn unproxify_shard_action_missing_shard() {
    unproxify_shard_action_missing_local(true);
}

#[test]
fn unproxify_shard_action_without_local_replica() {
    unproxify_shard_action_missing_local(false);
}

fn unproxify_shard_action_missing_local(drop_shard: bool) {
    let (_storage_dir, handle, dispatcher) = new_dispatcher();
    create_collection(&handle, &dispatcher, "test");

    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    let setup = if drop_shard {
        Action::DropShard {
            collection: "test".into(),
            shard_id: 0,
        }
    } else {
        Action::RemoveReplica {
            collection: "test".into(),
            shard_id: 0,
            peer_id: 0,
        }
    };
    toc.apply_action(setup).unwrap();
    let initial_state = toc.collection_state("test").unwrap();
    let action = Action::UnproxifyShard {
        collection: "test".into(),
        shard_id: 0,
    };

    let error = toc.apply_action(action).unwrap_err();

    assert!(matches!(error, StorageError::ServiceError { .. }));
    assert_eq!(toc.collection_state("test").unwrap(), initial_state);
}

fn stop_collections(handle: &Handle, dispatcher: &Dispatcher) {
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&FULL_ACCESS, &pass);
    for name in toc.collection_names() {
        let collection_pass = FULL_ACCESS
            .check_collection_access(&name, AccessRequirements::new(), "test")
            .unwrap();
        handle.block_on(async {
            toc.get_collection(&collection_pass)
                .await
                .unwrap()
                .stop_gracefully()
                .await;
        });
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
