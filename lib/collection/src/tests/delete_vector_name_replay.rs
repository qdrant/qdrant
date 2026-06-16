//! Reproducer for the WAL replay bug introduced by `delete_named_vector`.
//!
//! Hypothesis: `Collection::delete_named_vector` removes the vector from the persisted
//! `CollectionParams` but does NOT reconcile the WAL. Historical Upsert entries that
//! still reference the deleted vector name fail at WAL replay on reload, dropping
//! their target points entirely.
//!
//! Procedure:
//!   1. Build a collection with two named dense vectors `a` and `b`.
//!   2. Upsert a single point that populates both `a` and `b`.
//!   3. Call `delete_named_vector("b")`.
//!   4. Close the collection and reopen it.
//!   5. Assert the point survives reload.
//!
//! If the test fails, the bug is confirmed.

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use ahash::AHashMap;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::vector_name_config::{DenseVectorConfig, VectorNameConfig};
use segment::types::{Distance, WithPayloadInterface, WithVector};
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorPersisted,
    VectorStructPersisted, WriteOrdering,
};
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{PointRequestInternal, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::shards::shard::{PeerId, ShardId};

const PEER_ID: PeerId = 1;
const COLLECTION_NAME: &str = "test";

fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(|_, _, _| {})
}

/// Synchronously flush every local shard's segments so subsequent `stop_gracefully` +
/// reload doesn't have to replay the original Upsert from the WAL.
async fn force_flush_all_local(collection: &Collection) {
    let shards_holder = collection.shards_holder();
    let holder = shards_holder.read().await;
    for replica_set in holder.all_shards() {
        replica_set.force_flush_local_for_test().await;
    }
}

fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(|_| {})
}
fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_, _| {})
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(target_os = "windows", ignore = "too slow on Windows CI")]
async fn delete_named_vector_then_reload_loses_points() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new().prefix("dvn_repro_coll").tempdir().unwrap();
    let snapshots_dir = Builder::new().prefix("dvn_repro_snap").tempdir().unwrap();

    // Two dense named vectors, single shard, single replica.
    let mut dense_vectors = BTreeMap::new();
    dense_vectors.insert(
        "a".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    dense_vectors.insert(
        "b".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(dense_vectors),
        sparse_vectors: None,
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        // Disable the optimizer — this bug must be observable without any optimizer
        // activity. If the test fails with the optimizer disabled, it's a WAL replay
        // bug, not a proxy-segment race.
        max_optimization_threads: Some(0),
        prevent_unoptimized: None,
    };

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let shards: AHashMap<ShardId, HashSet<PeerId>> =
        AHashMap::from_iter([(0, HashSet::from([PEER_ID]))]);

    let collection = Collection::new(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
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

    collection
        .set_shard_replica_state(0, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    // Upsert a single point with both vectors.
    let point_id = 42u64.into();
    let mut named = std::collections::HashMap::new();
    named.insert(
        "a".to_string(),
        VectorPersisted::Dense(vec![0.1, 0.2, 0.3, 0.4]),
    );
    named.insert(
        "b".to_string(),
        VectorPersisted::Dense(vec![0.5, 0.6, 0.7, 0.8]),
    );
    let point = PointStructPersisted {
        id: point_id,
        vector: VectorStructPersisted::Named(named),
        payload: None,
    };
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));
    collection
        .update_from_client_simple(
            upsert,
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .expect("initial upsert failed");

    // Confirm the point is there with both vectors before we touch the schema.
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: vec![point_id],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("pre-delete retrieve failed");
    assert_eq!(
        records.len(),
        1,
        "point should exist before delete_named_vector",
    );

    // Drop the `b` vector schema-wide.
    collection
        .delete_named_vector("b".to_string())
        .await
        .expect("delete_named_vector(b) failed");

    // Confirm the point still exists with just `a` while we're live.
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: vec![point_id],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("post-delete live retrieve failed");
    assert_eq!(
        records.len(),
        1,
        "point should survive live delete_named_vector",
    );

    // Close and reopen.
    collection.stop_gracefully().await;
    drop(collection);

    let collection = Collection::load(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        Arc::new(SharedStorageConfig::default()),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    // The point must still exist after reload. If the WAL replay bug is real, the
    // historical Upsert that included `b` was rejected (because `b` is no longer in
    // the persisted CollectionParams), and the point is dropped.
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: vec![point_id],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("post-reload retrieve failed");
    assert_eq!(
        records.len(),
        1,
        "point should survive WAL replay after delete_named_vector — \
         if this fails, the WAL replay drops historical Upserts whose named-vector \
         map references the (since-deleted) vector name",
    );
}

/// Paired with the test above: same workload, but explicit `force_flush_all_local()` between
/// the Upsert and the `delete_named_vector` so the original Upsert is baked into segment
/// storage before close. WAL replay on reload then doesn't have to apply the Upsert against
/// the post-deletion config, and the point survives.
///
/// If the FIRST test fails (point lost) and THIS test passes (point preserved), the bug is
/// specifically: "WAL replay rejects historical Upserts whose named-vector map references a
/// since-deleted vector name". With the flush, the Upsert is already in segment storage and
/// the replay window starts past it.
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(target_os = "windows", ignore = "too slow on Windows CI")]
async fn delete_named_vector_after_flush_survives_reload() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new().prefix("dvn_flushed_coll").tempdir().unwrap();
    let snapshots_dir = Builder::new().prefix("dvn_flushed_snap").tempdir().unwrap();

    let mut dense_vectors = BTreeMap::new();
    dense_vectors.insert(
        "a".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    dense_vectors.insert(
        "b".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(dense_vectors),
        sparse_vectors: None,
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };
    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: Some(0),
        prevent_unoptimized: None,
    };
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };
    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };
    let shards: AHashMap<ShardId, HashSet<PeerId>> =
        AHashMap::from_iter([(0, HashSet::from([PEER_ID]))]);

    let collection = Collection::new(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
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

    collection
        .set_shard_replica_state(0, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    let point_id = 42u64.into();
    let mut named = std::collections::HashMap::new();
    named.insert(
        "a".to_string(),
        VectorPersisted::Dense(vec![0.1, 0.2, 0.3, 0.4]),
    );
    named.insert(
        "b".to_string(),
        VectorPersisted::Dense(vec![0.5, 0.6, 0.7, 0.8]),
    );
    let point = PointStructPersisted {
        id: point_id,
        vector: VectorStructPersisted::Named(named),
        payload: None,
    };
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));
    collection
        .update_from_client_simple(
            upsert,
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .expect("upsert failed");

    // The critical difference vs. the first test: flush the segment so the Upsert is
    // durably applied before we drop the vector schema.
    force_flush_all_local(&collection).await;

    collection
        .delete_named_vector("b".to_string())
        .await
        .expect("delete_named_vector(b) failed");

    collection.stop_gracefully().await;
    drop(collection);

    let collection = Collection::load(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        Arc::new(SharedStorageConfig::default()),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: vec![point_id],
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("post-reload retrieve failed");
    assert_eq!(
        records.len(),
        1,
        "with a flush between Upsert and delete_named_vector, the point should \
         survive reload — the original Upsert was applied to segment storage before \
         the vector was dropped, so WAL replay doesn't have to redo it",
    );
}

/// Multi-cycle counterpart to `delete_named_vector_after_flush_survives_reload`: 10
/// rounds of `CreateVectorName → upsert → flush → DeleteVectorName`, each with its own
/// fresh dynamically-added vector name. After all cycles close + reopen and assert
/// every point is still retrievable.
///
/// This passes — establishing that the bug is not "any Create+Delete cycle corrupts
/// segment state", but specifically "an unflushed Upsert with a since-deleted vector
/// name fails WAL replay". Flushing each cycle's upsert before its DeleteVectorName
/// closes the bug window, even across many cycles.
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(target_os = "windows", ignore = "too slow on Windows CI")]
async fn repeated_create_then_delete_vector_name_with_flush_survives_reload() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new().prefix("dvn_cycle_coll").tempdir().unwrap();
    let snapshots_dir = Builder::new().prefix("dvn_cycle_snap").tempdir().unwrap();

    // Start with just `a` — `b` will be created dynamically.
    let mut dense_vectors = BTreeMap::new();
    dense_vectors.insert(
        "a".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(dense_vectors),
        sparse_vectors: None,
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };
    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: Some(0),
        prevent_unoptimized: None,
    };
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };
    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };
    let shards: AHashMap<ShardId, HashSet<PeerId>> =
        AHashMap::from_iter([(0, HashSet::from([PEER_ID]))]);

    let collection = Collection::new(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
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

    collection
        .set_shard_replica_state(0, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    const NUM_CYCLES: u64 = 10;
    let mut point_ids = Vec::with_capacity(NUM_CYCLES as usize);

    for i in 0..NUM_CYCLES {
        let name = format!("v_{i}");
        // 1. Dynamically create the vector.
        let cfg = VectorNameConfig::dense(DenseVectorConfig {
            size: 4,
            distance: Distance::Dot,
            multivector_config: None,
            datatype: None,
        });
        collection
            .create_named_vector(name.clone(), cfg, HwMeasurementAcc::new())
            .await
            .unwrap_or_else(|e| panic!("create_named_vector({name:?}) failed: {e:?}"));

        // 2. Upsert a unique point that includes the new vector.
        let point_id = (100 + i).into();
        point_ids.push(point_id);
        let mut named = std::collections::HashMap::new();
        named.insert(
            "a".to_string(),
            VectorPersisted::Dense(vec![0.1, 0.2, 0.3, 0.4]),
        );
        named.insert(
            name.clone(),
            VectorPersisted::Dense(vec![0.5, 0.6, 0.7, i as f32 / 10.0]),
        );
        let point = PointStructPersisted {
            id: point_id,
            vector: VectorStructPersisted::Named(named),
            payload: None,
        };
        let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point]),
        ));
        collection
            .update_from_client_simple(
                upsert,
                true,
                None,
                WriteOrdering::default(),
                HwMeasurementAcc::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("upsert cycle {i} failed: {e:?}"));

        // 3. Flush so the upsert is baked into segment storage.
        force_flush_all_local(&collection).await;

        // 4. Drop the dynamically-added vector.
        collection
            .delete_named_vector(name.clone())
            .await
            .unwrap_or_else(|e| panic!("delete_named_vector({name:?}) failed: {e:?}"));
    }

    // All N points should still be there live.
    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: point_ids.clone(),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("post-cycles live retrieve failed");
    assert_eq!(
        records.len(),
        NUM_CYCLES as usize,
        "all {NUM_CYCLES} points should survive the live cycles",
    );

    // Close and reopen.
    collection.stop_gracefully().await;
    drop(collection);

    let collection = Collection::load(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        Arc::new(SharedStorageConfig::default()),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await;

    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: point_ids.clone(),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Bool(true),
            },
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("post-reload retrieve failed");
    let returned_ids: Vec<_> = records.iter().map(|r| r.id).collect();
    let missing: Vec<_> = point_ids
        .iter()
        .filter(|id| !returned_ids.contains(id))
        .copied()
        .collect();
    assert_eq!(
        records.len(),
        NUM_CYCLES as usize,
        "all {NUM_CYCLES} points should survive reload after their respective \
         CreateVectorName + Upsert + Flush + DeleteVectorName cycles. \
         Missing: {missing:?}",
    );
}
