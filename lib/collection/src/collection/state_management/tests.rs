use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;

use ahash::AHashMap;
use common::ambient;
use common::budget::ResourceBudget;
use segment::data_types::modifier::Modifier;
use segment::types::{Distance, Memory, VectorStorageDatatype};
use tempfile::TempDir;

use crate::collection::Collection;
use crate::config::CollectionConfigInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{Datatype, SparseIndexParams, SparseVectorParams, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::tests::fixtures::create_collection_config;

const PEER_ID: PeerId = 1;
const SHARD_ID: ShardId = 0;

async fn collection_fixture(path: &Path, mut config: CollectionConfigInternal) -> Collection {
    // Only schema operations should change the segments in these tests.
    config.optimizer_config.max_optimization_threads = Some(0);
    config.optimizer_config.flush_interval_sec = 30;

    let collection = Collection::new(
        "snapshot-vector-schema".to_owned(),
        PEER_ID,
        path,
        &path.join("snapshots"),
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution {
            shards: AHashMap::from_iter([(SHARD_ID, HashSet::from([PEER_ID]))]),
        },
        None,
        ChannelService::default(),
        Arc::new(|_, _, _| {}),
        Arc::new(|_| {}),
        Arc::new(|_, _| {}),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();
    collection
        .set_shard_replica_state(SHARD_ID, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();
    collection
}

async fn local_replica(collection: &Collection) -> Arc<ShardReplicaSet> {
    let holder = collection.shards_holder().read_owned().await;
    Arc::clone(holder.get_shard(SHARD_ID).unwrap())
}

#[tokio::test]
async fn snapshot_vector_schema_keeps_the_only_vector_config_during_reconciliation() {
    let _scope = ambient::async_test_guard();
    let dir = TempDir::new().unwrap();
    let mut initial = create_collection_config();
    initial.params.vectors = VectorsConfig::Multi(BTreeMap::from([(
        "v1".to_owned(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    )]));
    let collection = collection_fixture(dir.path(), initial).await;
    let mut target = collection.config().await;
    target.params.vectors = VectorsConfig::Multi(BTreeMap::from([(
        "v1".to_owned(),
        VectorParamsBuilder::new(8, Distance::Cosine).build(),
    )]));
    collection.apply_config(target.clone()).await.unwrap();

    // Hold the local shard lock so that the first delete operation cannot finish.
    // Poll through the synchronous part of reconciliation before reading the config.
    let replica = local_replica(&collection).await;
    let local_guard = replica.lock_local_for_test().await;
    let mut reconcile = pin!(tokio::task::unconstrained(
        collection.apply_vector_name_schema(&target),
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let config_during_reconciliation = collection.config().await;
    let persisted_during_reconciliation = CollectionConfigInternal::load(dir.path()).unwrap();
    drop(local_guard);

    reconcile.await.unwrap();
    collection.stop_gracefully().await;

    assert_eq!(config_during_reconciliation, target);
    assert_eq!(persisted_during_reconciliation, target);
}

#[tokio::test]
async fn snapshot_vector_schema_applies_dense_and_sparse_transitions_to_segments() {
    let _scope = ambient::async_test_guard();
    let dir = TempDir::new().unwrap();
    let dense = VectorParamsBuilder::new(4, Distance::Dot).build();
    let sparse = SparseVectorParams {
        index: None,
        modifier: None,
    };
    let mut initial = create_collection_config();
    initial.params.vectors = VectorsConfig::Multi(BTreeMap::from([
        ("dense_changed".to_owned(), dense.clone()),
        ("dense_deleted".to_owned(), dense.clone()),
        ("to_sparse".to_owned(), dense),
    ]));
    initial.params.sparse_vectors = Some(BTreeMap::from([
        ("sparse_changed".to_owned(), sparse.clone()),
        ("sparse_deleted".to_owned(), sparse.clone()),
        ("to_dense".to_owned(), sparse.clone()),
    ]));
    let collection = collection_fixture(dir.path(), initial).await;

    let mut state = collection.state().await;
    state.config.params.vectors = VectorsConfig::Multi(BTreeMap::from([
        (
            "dense_changed".to_owned(),
            VectorParamsBuilder::new(8, Distance::Cosine)
                .with_memory(Memory::Cold)
                .with_datatype(Datatype::Float16)
                .build(),
        ),
        (
            "dense_added".to_owned(),
            VectorParamsBuilder::new(6, Distance::Dot).build(),
        ),
        (
            "to_dense".to_owned(),
            VectorParamsBuilder::new(10, Distance::Dot).build(),
        ),
    ]));
    state.config.params.sparse_vectors = Some(BTreeMap::from([
        (
            "sparse_changed".to_owned(),
            SparseVectorParams {
                index: Some(SparseIndexParams {
                    full_scan_threshold: Some(42),
                    datatype: Some(Datatype::Float16),
                    ..Default::default()
                }),
                modifier: Some(Modifier::Idf),
            },
        ),
        ("sparse_added".to_owned(), sparse.clone()),
        ("to_sparse".to_owned(), sparse),
    ]));
    let target = state.config.clone();

    collection
        .apply_state(state, PEER_ID, |_| {})
        .await
        .unwrap();

    assert_eq!(collection.config().await, target);
    assert_eq!(CollectionConfigInternal::load(dir.path()).unwrap(), target);

    let replica = local_replica(&collection).await;
    let local_guard = replica.lock_local_for_test().await;
    let local = local_guard.as_ref().unwrap().local_shard().unwrap();
    let segments = local.segments();
    {
        let segments = segments.read();
        assert!(!segments.is_empty());
        for (_, segment) in segments.iter() {
            let segment = segment.get().read();
            let config = segment.config();
            assert_eq!(config.vector_data.len(), 3);
            let dense = &config.vector_data["dense_changed"];
            assert_eq!(dense.size, 8);
            assert_eq!(dense.distance, Distance::Cosine);
            assert_eq!(dense.datatype, Some(VectorStorageDatatype::Float16));
            assert_eq!(config.vector_data["dense_added"].size, 6);
            assert_eq!(config.vector_data["to_dense"].size, 10);

            assert_eq!(config.sparse_vector_data.len(), 3);
            let sparse = &config.sparse_vector_data["sparse_changed"];
            assert_eq!(sparse.modifier, Some(Modifier::Idf));
            assert_eq!(sparse.index.datatype, Some(VectorStorageDatatype::Float16));
            assert!(config.sparse_vector_data.contains_key("sparse_added"));
            assert!(config.sparse_vector_data.contains_key("to_sparse"));
        }
    }
    drop(local_guard);
    collection.stop_gracefully().await;
}

#[tokio::test]
async fn snapshot_vector_schema_retry_repairs_divergent_shard_after_config_persisted() {
    let _scope = ambient::async_test_guard();
    let dir = TempDir::new().unwrap();
    let mut initial = create_collection_config();
    initial.params.shard_number = std::num::NonZeroU32::new(2).unwrap();
    initial.params.vectors = VectorsConfig::Multi(BTreeMap::from([
        (
            "v1".to_owned(),
            VectorParamsBuilder::new(4, Distance::Dot).build(),
        ),
        (
            "obsolete".to_owned(),
            VectorParamsBuilder::new(4, Distance::Dot).build(),
        ),
    ]));
    initial.optimizer_config.max_optimization_threads = Some(0);
    initial.optimizer_config.flush_interval_sec = 30;

    let collection = Collection::new(
        "snapshot-vector-schema-retry".to_owned(),
        PEER_ID,
        dir.path(),
        &dir.path().join("snapshots"),
        &initial,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution {
            shards: AHashMap::from_iter([
                (0, HashSet::from([PEER_ID])),
                (1, HashSet::from([PEER_ID])),
            ]),
        },
        None,
        ChannelService::default(),
        Arc::new(|_, _, _| {}),
        Arc::new(|_| {}),
        Arc::new(|_, _| {}),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();
    for shard_id in [0, 1] {
        collection
            .set_shard_replica_state(shard_id, PEER_ID, ReplicaState::Active, None)
            .await
            .unwrap();
    }

    let mut state = collection.state().await;
    state.config.params.vectors = VectorsConfig::Multi(BTreeMap::from([
        (
            "v1".to_owned(),
            VectorParamsBuilder::new(8, Distance::Cosine).build(),
        ),
        (
            "v2".to_owned(),
            VectorParamsBuilder::new(6, Distance::Dot).build(),
        ),
    ]));
    let target = state.config.clone();

    // Simulate a previous attempt where `apply_config` persisted the target schema and
    // shard 0 already deleted `obsolete`, while shard 1 failed before applying any updates.
    collection.apply_config(target.clone()).await.unwrap();
    let shard0 = {
        let holder = collection.shards_holder().read_owned().await;
        Arc::clone(holder.get_shard(0).unwrap())
    };
    shard0
        .update_local(
            crate::operations::OperationWithClockTag::from(
                shard::operations::CollectionUpdateOperations::VectorNameOperation(
                    shard::operations::VectorNameOperations::DeleteVectorName(
                        shard::operations::DeleteVectorName {
                            vector_name: "obsolete".to_owned(),
                        },
                    ),
                ),
            ),
            crate::shards::shard_trait::WaitUntil::Segment,
            None,
            true,
        )
        .await
        .unwrap();

    // Retrying `apply_state` with the same snapshot sees `collection_config == target`,
    // yet must still reconcile both shards' divergent segments.
    collection
        .apply_state(state.clone(), PEER_ID, |_| {})
        .await
        .unwrap();

    // A subsequent retry with converged segments must succeed cleanly.
    collection
        .apply_state(state, PEER_ID, |_| {})
        .await
        .unwrap();

    for shard_id in [0, 1] {
        let replica = {
            let holder = collection.shards_holder().read_owned().await;
            Arc::clone(holder.get_shard(shard_id).unwrap())
        };
        let segments = replica.local_segments().await.unwrap();
        let segments = segments.read();
        assert!(!segments.is_empty());
        for (_, segment) in segments.iter() {
            let segment = segment.get().read();
            let config = segment.config();
            assert_eq!(config.vector_data.len(), 2);
            assert!(!config.vector_data.contains_key("obsolete"));
            assert_eq!(config.vector_data["v1"].size, 8);
            assert_eq!(config.vector_data["v1"].distance, Distance::Cosine);
            assert_eq!(config.vector_data["v2"].size, 6);
            assert_eq!(config.vector_data["v2"].distance, Distance::Dot);
        }
    }

    collection.stop_gracefully().await;
}
