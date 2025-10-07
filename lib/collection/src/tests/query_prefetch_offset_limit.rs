use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use ahash::AHashMap;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use rand::{Rng, rng};
use segment::data_types::vectors::NamedQuery;
use segment::types::{Distance, ExtendedPointId, WithPayloadInterface, WithVector};
use shard::query::query_enum::QueryEnum;
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::VectorsConfig;
use crate::operations::universal_query::shard_query::{
    ScoringQuery, ShardPrefetch, ShardQueryRequest,
};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState, ReplicaState};
use crate::shards::shard::{PeerId, ShardId};

const DIM: u64 = 4;
const PEER_ID: u64 = 1;
const SHARD_COUNT: u32 = 1;
const POINT_COUNT: usize = 1_000;

/// Create a collection used for limit+offset tests
async fn fixture() -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM, Distance::Dot).build()),
        shard_number: NonZeroU32::new(SHARD_COUNT).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: OptimizersConfig::fixture(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

    let collection_name = "test".to_string();
    let shards: AHashMap<ShardId, HashSet<PeerId>> = (0..SHARD_COUNT)
        .map(|i| (i, HashSet::from([PEER_ID])))
        .collect();

    let storage_config: SharedStorageConfig = SharedStorageConfig::default();
    let storage_config = Arc::new(storage_config);

    let collection = Collection::new(
        collection_name.clone(),
        PEER_ID,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        storage_config.clone(),
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

    // Activate all shards
    for shard_id in 0..SHARD_COUNT {
        collection
            .set_shard_replica_state(shard_id, PEER_ID, ReplicaState::Active, None)
            .await
            .expect("failed to activate shard");
    }

    // Upsert points
    let points = (0..POINT_COUNT)
        .map(|i| PointStructPersisted {
            id: ExtendedPointId::from(i as u64),
            vector: VectorStructPersisted::Single(
                (0..DIM).map(|_| rng().random_range(0.0..1.0)).collect(),
            ),
            payload: None,
        })
        .collect();
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));

    collection
        .update_from_client(
            operation,
            true,
            WriteOrdering::Weak,
            None,
            HwMeasurementAcc::disposable(),
        )
        .await
        .expect("failed to insert points");

    collection
}

/// Test that limit and offset works properly with prefetches.
///
/// Bug: <https://github.com/qdrant/qdrant/pull/6412>
#[tokio::test(flavor = "multi_thread")]
async fn test_limit_offset_with_prefetch() {
    let collection = fixture().await;

    let do_query = async |offset, limit| {
        collection
            .query(
                ShardQueryRequest {
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedQuery::default_dense(vec![0.1, 0.2, 0.3, 0.4]),
                    ))),
                    prefetches: vec![ShardPrefetch {
                        prefetches: vec![],
                        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedQuery::default_dense(vec![0.1, 0.2, 0.3, 0.4]),
                        ))),
                        limit: 100,
                        params: None,
                        filter: None,
                        score_threshold: None,
                    }],
                    filter: None,
                    params: None,
                    offset,
                    limit,
                    with_payload: WithPayloadInterface::Bool(false),
                    with_vector: WithVector::Bool(false),
                    score_threshold: None,
                },
                None,
                ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::disposable(),
            )
            .await
            .expect("failed to query")
    };

    // With an offset of 5 and a limit of 15, we should still get 15 results
    // This was 10 before <https://github.com/qdrant/qdrant/pull/6412>
    let points = do_query(5, 15).await;
    assert_eq!(points.len(), 15, "expected 15 points, got {}", points.len());

    let points = do_query(10, 10).await;
    assert_eq!(points.len(), 10, "expected 10 points, got {}", points.len());

    // Prefetch limited to 100, with offset of 95 we have just 5 results left
    // This was zero before <https://github.com/qdrant/qdrant/pull/6412>
    let points = do_query(95, 10).await;
    assert_eq!(points.len(), 5, "expected 5 points, got {}", points.len());

    // Use a nested prefetch limiting to 50 results
    let do_query = async |offset, limit| {
        collection
            .query(
                ShardQueryRequest {
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedQuery::default_dense(vec![0.1, 0.2, 0.3, 0.4]),
                    ))),
                    prefetches: vec![ShardPrefetch {
                        prefetches: vec![ShardPrefetch {
                            prefetches: vec![],
                            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                                NamedQuery::default_dense(vec![0.1, 0.2, 0.3, 0.4]),
                            ))),
                            limit: 50,
                            params: None,
                            filter: None,
                            score_threshold: None,
                        }],
                        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedQuery::default_dense(vec![0.1, 0.2, 0.3, 0.4]),
                        ))),
                        limit: 100,
                        params: None,
                        filter: None,
                        score_threshold: None,
                    }],
                    filter: None,
                    params: None,
                    offset,
                    limit,
                    with_payload: WithPayloadInterface::Bool(false),
                    with_vector: WithVector::Bool(false),
                    score_threshold: None,
                },
                None,
                ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::disposable(),
            )
            .await
            .expect("failed to query")
    };

    // With an offset of 5 and a limit of 15, we should still get 15 results
    // This was 10 before <https://github.com/qdrant/qdrant/pull/6412>
    let points = do_query(5, 15).await;
    assert_eq!(points.len(), 15, "expected 15 points, got {}", points.len());

    let points = do_query(10, 10).await;
    assert_eq!(points.len(), 10, "expected 10 points, got {}", points.len());

    // Nested prefetch limited to 50, with offset of 45 we have just 5 results left
    // This was zero before <https://github.com/qdrant/qdrant/pull/6412>
    let points = do_query(45, 10).await;
    assert_eq!(points.len(), 5, "expected 5 points, got {}", points.len());
}

fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_transfer, _reason| {})
}
