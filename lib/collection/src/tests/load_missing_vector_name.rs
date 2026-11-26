//! Loading a shard whose segments miss a vector that the collection config declares
//! (e.g. a crash between the config update and the shard update of `CreateVectorName`).
//! The load path reconciles the segment by creating the vector and prefilling it with
//! deleted entries, which bumps hw counters on a blocking-pool thread.

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use ahash::AHashMap;
use common::ambient::{self, AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use segment::types::{Distance, WithPayloadInterface, WithVector};
use tempfile::Builder;

use crate::collection::Collection;
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
use crate::shards::shard::{PeerId, ShardId};

const PEER_ID: PeerId = 1;
const COLLECTION_NAME: &str = "test";

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(target_os = "windows", ignore = "too slow on Windows CI")]
async fn load_creates_vector_missing_from_segment() {
    let _scope = ambient::async_test_guard();
    let collection_dir = Builder::new().prefix("lmv_coll").tempdir().unwrap();
    let snapshots_dir = Builder::new().prefix("lmv_snap").tempdir().unwrap();

    let mut dense_vectors = BTreeMap::new();
    dense_vectors.insert(
        "a".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorsConfig::Multi(dense_vectors),
            sparse_vectors: None,
            shard_number: NonZeroU32::new(1).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        },
        optimizer_config: OptimizersConfig {
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
        },
        wal_config: WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        },
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
        created_at: None,
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
        .set_shard_replica_state(0, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    let point_id = 42u64.into();
    let upsert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
            id: point_id,
            vector: VectorStructPersisted::Named(std::collections::HashMap::from([(
                "a".to_string(),
                VectorPersisted::Dense(vec![0.1, 0.2, 0.3, 0.4]),
            )])),
            payload: None,
        }]),
    ));
    collection
        .update_from_client_simple(upsert, true, None, WriteOrdering::default())
        .measured(AmbientContext::new())
        .await
        .unwrap();

    // Bake the point into segment storage, so the segment has entries to prefill.
    {
        let shards_holder = collection.shards_holder();
        let holder = shards_holder.read().await;
        for replica_set in holder.all_shards() {
            replica_set.force_flush_local_for_test().await;
        }
    }

    collection.stop_gracefully().await;
    drop(collection);

    // Simulate a crash after the config update of `CreateVectorName` but before the
    // shard update: the config declares `b`, the segments don't have it.
    let mut persisted = CollectionConfigInternal::load(collection_dir.path()).unwrap();
    persisted.params.vectors.insert(
        "b".to_string(),
        VectorParamsBuilder::new(4, Distance::Dot).build(),
    );
    persisted.save(collection_dir.path()).unwrap();

    let collection = Collection::load(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        Arc::new(SharedStorageConfig::default()),
        ChannelService::default(),
        Arc::new(|_, _, _| {}),
        Arc::new(|_| {}),
        Arc::new(|_, _| {}),
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
            None,
            &ShardSelectorInternal::All,
            None,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();
    assert_eq!(records.len(), 1, "point should survive the reload");
}
