use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use api::rest::SearchRequestInternal;
use collection::collection::Collection;
use collection::config::{
    COLLECTION_CONFIG_FILE, CollectionConfigInternal, CollectionParams, ShardingMethod, WalConfig,
};
use collection::operations::CollectionUpdateOperations;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::shared_storage_config::SharedStorageConfig;
use collection::operations::types::{NodeType, VectorsConfig};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::shard_config::ShardConfig;
use collection::shards::shard_holder::ShardHolder;
use collection::shards::transfer::ShardTransfer;
use collection::shards::{shard_initializing_flag_path, shard_path};
use common::ambient::{self, AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use segment::types::{Distance, WithPayloadInterface, WithVector};
use shard::snapshots::snapshot_data::SnapshotData;
use tempfile::Builder;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::common::{
    REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer, dummy_on_replica_failure,
    dummy_request_shard_transfer, simple_collection_fixture,
};

async fn _test_snapshot_and_recover_collection(node_type: NodeType) {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
        created_at: None,
    };

    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let recover_dir = Builder::new()
        .prefix("test_collection_rec")
        .tempdir()
        .unwrap();
    let collection_name = "test".to_string();
    let collection_name_rec = "test_rec".to_string();

    let storage_config: SharedStorageConfig = SharedStorageConfig {
        node_type,
        ..Default::default()
    };

    let this_peer_id = 0;
    let shard_distribution = CollectionShardDistribution::all_local(
        Some(config.params.shard_number.into()),
        this_peer_id,
    );

    let collection = Collection::new(
        collection_name,
        this_peer_id,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        Arc::new(storage_config),
        shard_distribution,
        None,
        ChannelService::new(REST_PORT, false, None, None),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();

    let local_shards = collection.get_local_shards().await;
    for shard_id in local_shards {
        collection
            .set_shard_replica_state(shard_id, 0, ReplicaState::Active, None)
            .await
            .unwrap();
    }

    // Upload 1000 random vectors to the collection
    let mut points = Vec::new();
    for i in 0..100 {
        points.push(PointStructPersisted {
            id: i.into(),
            vector: VectorStructPersisted::Single(vec![i as f32, 0.0, 0.0, 0.0]),
            payload: Some(serde_json::from_str(r#"{"number": "John Doe"}"#).unwrap()),
        });
    }
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));
    let ctx = AmbientContext::new();
    collection
        .update_from_client_simple(insert_points, true, None, WriteOrdering::default())
        .measured(ctx)
        .await
        .unwrap();

    // Take a snapshot
    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let snapshot_description = collection
        .create_snapshot(snapshots_temp_dir.path(), 0)
        .await
        .unwrap();

    let snapshot_data =
        SnapshotData::new_packed_persistent(snapshots_path.path().join(snapshot_description.name));

    if let Err(err) = Collection::restore_snapshot(snapshot_data, recover_dir.path(), 0, false) {
        panic!("Failed to restore snapshot: {err}")
    }

    let recovered_collection = Collection::load(
        collection_name_rec,
        this_peer_id,
        recover_dir.path(),
        snapshots_path.path(),
        Default::default(),
        ChannelService::new(REST_PORT, false, None, None),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    let query_vector = vec![1.0, 0.0, 0.0, 0.0];

    let full_search_request = SearchRequestInternal {
        vector: query_vector.clone().into(),
        filter: None,
        limit: 100,
        offset: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(WithVector::Bool(true)),
        params: None,
        score_threshold: None,
    };

    let ctx = AmbientContext::new();
    let reference_result = collection
        .search(
            full_search_request.clone().into(),
            None,
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .measured(ctx)
        .await
        .unwrap();

    let ctx = AmbientContext::new();
    let recovered_result = recovered_collection
        .search(
            full_search_request.into(),
            None,
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .measured(ctx)
        .await
        .unwrap();

    assert_eq!(reference_result.len(), recovered_result.len());

    for (reference, recovered) in reference_result.iter().zip(recovered_result.iter()) {
        assert_eq!(reference.id, recovered.id);
        assert_eq!(reference.payload, recovered.payload);
        assert_eq!(reference.vector, recovered.vector);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_and_recover_collection_normal() {
    let _scope = ambient::async_test_guard();
    _test_snapshot_and_recover_collection(NodeType::Normal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_and_recover_collection_listener() {
    let _scope = ambient::async_test_guard();
    _test_snapshot_and_recover_collection(NodeType::Listener).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_during_apply_state_shard_count_increase() {
    let collection_dir = tempfile::tempdir().unwrap();
    let collection = simple_collection_fixture(collection_dir.path(), 1).await;
    let snapshot_temp_dir = tempfile::tempdir().unwrap();
    let recover_dir = tempfile::tempdir().unwrap();

    let mut state = collection.state().await;
    state.config.params.shard_number = NonZeroU32::new(2).unwrap();
    state.shards.insert(1, state.shards[&0].clone());
    state.transfers.insert(ShardTransfer {
        shard_id: 0,
        to_shard_id: None,
        from: 0,
        to: 1,
        sync: true,
        method: None,
        filter: None,
    });

    // A sender that first learns of a transfer from a Raft snapshot aborts it before
    // reconciling shards. Finish a collection snapshot at that point without relying on timing.
    let mut snapshot = None;
    collection
        .apply_state(state, 0, |_| {
            snapshot = Some(tokio::task::block_in_place(|| {
                Handle::current().block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        collection.create_snapshot(snapshot_temp_dir.path(), 0),
                    )
                    .await
                    .expect("snapshot creation must progress while shard reconciliation is paused")
                    .unwrap()
                })
            }));
        })
        .await
        .unwrap();

    let snapshot = snapshot.expect("sender must abort the transfer while applying Raft state");
    let snapshot_data =
        SnapshotData::new_packed_persistent(collection.snapshots_path().join(snapshot.name));
    Collection::restore_snapshot(snapshot_data, recover_dir.path(), 0, false).unwrap();

    let archived_config = CollectionConfigInternal::load(recover_dir.path()).unwrap();
    assert_eq!(archived_config.params.shard_number.get(), 1);
    assert!(collection.contains_shard(1).await);
    assert_eq!(collection.config().await.params.shard_number.get(), 2);
    let persisted_config = CollectionConfigInternal::load(collection_dir.path()).unwrap();
    assert_eq!(persisted_config.params.shard_number.get(), 2);
}

#[tokio::test]
async fn apply_state_shard_count_increase_retries_failed_config_save() {
    let dir = tempfile::tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 1).await;
    let holder = collection.shards_holder();
    let mut state = collection.state().await;
    state.config.params.shard_number = NonZeroU32::new(2).unwrap();
    let mut shard = state.shards[&0].clone();
    shard.replicas.insert(0, ReplicaState::Resharding);
    state.shards.insert(1, shard);
    let resharding = ReshardState::new(Uuid::new_v4(), ReshardingDirection::Up, 0, 1, None);
    state.resharding = Some(resharding.clone());

    // The config file cannot be atomically replaced while a directory occupies its path.
    let config_path = dir.path().join(COLLECTION_CONFIG_FILE);
    let backup = dir.path().join("config-backup");
    fs_err::rename(&config_path, &backup).unwrap();
    fs_err::create_dir(&config_path).unwrap();

    assert!(
        collection
            .apply_state(state.clone(), 0, |_| {})
            .await
            .is_err()
    );
    let registered = holder.read().await.get_shard(1).unwrap().clone();
    assert_eq!(collection.config().await.params.shard_number.get(), 1);
    let persisted_holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
    assert!(persisted_holder.resharding_state().is_none());
    assert!(collection.resharding_state().await.is_none());

    fs_err::remove_dir(&config_path).unwrap();
    fs_err::rename(&backup, &config_path).unwrap();
    collection.apply_state(state, 0, |_| {}).await.unwrap();

    assert!(Arc::ptr_eq(
        &registered,
        holder.read().await.get_shard(1).unwrap()
    ));
    assert_eq!(collection.config().await.params.shard_number.get(), 2);
    let persisted = CollectionConfigInternal::load(dir.path()).unwrap();
    assert_eq!(persisted.params.shard_number.get(), 2);
    let persisted_holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
    assert_eq!(
        persisted_holder.resharding_state(),
        Some(resharding.clone())
    );
    assert_eq!(collection.resharding_state().await, Some(resharding));
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_state_shard_count_decrease_retries_failed_removal() {
    let dir = tempfile::tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 2).await;
    let mut state = collection.state().await;
    state.resharding = Some(ReshardState::new(
        Uuid::new_v4(),
        ReshardingDirection::Up,
        0,
        1,
        None,
    ));
    collection
        .apply_state(state.clone(), 0, |_| {})
        .await
        .unwrap();

    state.config.params.shard_number = NonZeroU32::MIN;
    state.shards.remove(&1).unwrap();
    state.resharding = None;

    // A directory at the flag's path blocks cleanup after the shard config is removed.
    let flag_path = shard_initializing_flag_path(dir.path(), 1);
    fs_err::create_dir(&flag_path).unwrap();
    assert!(
        collection
            .apply_state(state.clone(), 0, |_| {})
            .await
            .is_err()
    );

    assert!(collection.contains_shard(1).await);
    assert!(
        ShardConfig::load(&shard_path(dir.path(), 1))
            .unwrap()
            .is_none()
    );
    assert_eq!(collection.config().await.params.shard_number.get(), 1);
    let persisted = CollectionConfigInternal::load(dir.path()).unwrap();
    assert_eq!(persisted.params.shard_number.get(), 1);
    let persisted_holder = ShardHolder::new(dir.path(), ShardingMethod::Auto).unwrap();
    assert!(persisted_holder.resharding_state().is_none());
    assert!(collection.resharding_state().await.is_none());

    fs_err::remove_dir(&flag_path).unwrap();
    collection.apply_state(state, 0, |_| {}).await.unwrap();

    assert!(!collection.contains_shard(1).await);
    assert_eq!(collection.config().await.params.shard_number.get(), 1);
    let persisted = CollectionConfigInternal::load(dir.path()).unwrap();
    assert_eq!(persisted.params.shard_number.get(), 1);
}
