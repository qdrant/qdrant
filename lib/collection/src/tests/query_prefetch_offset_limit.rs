use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use ahash::AHashMap;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use rand::{RngExt, rng};
use segment::data_types::vectors::NamedQuery;
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Distance, ExtendedPointId, FieldCondition, Filter, Match, MinShould, Payload,
    ScoredPoint, ValueVariants, WithPayloadInterface, WithVector,
};
use shard::query::query_enum::QueryEnum;
use serde_json::Value;
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
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_trait::WaitUntil;

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
            WaitUntil::from(true),
            None,
            WriteOrdering::Weak,
            None,
            HwMeasurementAcc::new(),
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
                None,
                ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::new(),
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
                None,
                ShardSelectorInternal::All,
                None,
                HwMeasurementAcc::new(),
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

/// Collection of points carrying `a`, `b` and `c` payload fields, laid out so
/// that the intersection of conditions is observable:
/// - `a = i % 2` (half the points have a = 1)
/// - `b = 2` for i in [500, 600), `b = 9` otherwise
/// - `c = 3` for i in [600, 650), `c = 9` otherwise
async fn fixture_with_payload() -> Collection {
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

    let collection = Collection::new(
        collection_name.clone(),
        PEER_ID,
        collection_dir.path(),
        snapshots_path.path(),
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

    for shard_id in 0..SHARD_COUNT {
        collection
            .set_shard_replica_state(shard_id, PEER_ID, ReplicaState::Active, None)
            .await
            .expect("failed to activate shard");
    }

    let points = (0..POINT_COUNT)
        .map(|i| {
            let b = if (500..600).contains(&i) { 2 } else { 9 };
            let c = if (600..650).contains(&i) { 3 } else { 9 };
            PointStructPersisted {
                id: ExtendedPointId::from(i as u64),
                vector: VectorStructPersisted::Single(
                    (0..DIM).map(|_| rng().random_range(0.0..1.0)).collect(),
                ),
                payload: Some(Payload(serde_json::Map::from_iter([
                    ("a".to_string(), Value::from(i % 2)),
                    ("b".to_string(), Value::from(b)),
                    ("c".to_string(), Value::from(c)),
                ]))),
            }
        })
        .collect();
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ));

    collection
        .update_from_client(
            operation,
            WaitUntil::from(true),
            None,
            WriteOrdering::Weak,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("failed to insert points");

    collection
}

/// `a == 1` as a field condition.
fn a_is_one() -> Condition {
    Condition::Field(FieldCondition::new_match(
        JsonPath::new("a"),
        Match::from(ValueVariants::Integer(1)),
    ))
}

/// `b == 2` as a field condition.
fn b_is_two() -> Condition {
    Condition::Field(FieldCondition::new_match(
        JsonPath::new("b"),
        Match::from(ValueVariants::Integer(2)),
    ))
}

/// `c == 3` as a field condition.
fn c_is_three() -> Condition {
    Condition::Field(FieldCondition::new_match(
        JsonPath::new("c"),
        Match::from(ValueVariants::Integer(3)),
    ))
}

async fn query_with_filters(
    collection: &Collection,
    root_filter: Filter,
    prefetch_filter: Filter,
) -> Vec<ScoredPoint> {
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
                    limit: 1000,
                    params: None,
                    filter: Some(prefetch_filter),
                    score_threshold: None,
                }],
                filter: Some(root_filter),
                params: None,
                offset: 0,
                limit: 1000,
                with_payload: WithPayloadInterface::Bool(true),
                with_vector: WithVector::Bool(false),
                score_threshold: None,
            },
            None,
            None,
            ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("failed to query")
}

fn assert_only_a1_b2(points: &[ScoredPoint]) {
    for point in points {
        let payload = point.payload.as_ref().expect("payload requested");
        assert_eq!(
            payload.0.get("a"),
            Some(&Value::from(1)),
            "root filter violated by point {}",
            point.id,
        );
        assert_eq!(
            payload.0.get("b"),
            Some(&Value::from(2)),
            "prefetch filter violated by point {}",
            point.id,
        );
    }
}

/// The root filter must be ANDed with each prefetch filter. `merge_owned`
/// unions `should` lists and `min_should` condition sets, which would OR the
/// two filters instead; the propagated filter has to be nested, not merged
/// flat.
///
/// Bug: root `{should: [a=1]}` + prefetch `{should: [b=2]}` returned points
/// matching either condition, and root `min_should: 2 of [a=1, c=3]` +
/// prefetch `min_should: 1 of [b=2]` let points skip `b=2` entirely.
#[tokio::test(flavor = "multi_thread")]
async fn test_root_filter_ands_with_prefetch_should() {
    let collection = fixture_with_payload().await;

    // should + should: only odd i in [500, 600) have a=1 AND b=2 (50 points)
    let points = query_with_filters(
        &collection,
        Filter::new_should(a_is_one()),
        Filter::new_should(b_is_two()),
    )
    .await;
    assert_eq!(points.len(), 50, "expected exactly the a=1 AND b=2 points");
    assert_only_a1_b2(&points);

    // min_should + min_should: no point satisfies (a=1 AND c=3) AND b=2,
    // since b=2 and c=3 cover disjoint id ranges
    let points = query_with_filters(
        &collection,
        Filter::new_min_should(MinShould {
            conditions: vec![a_is_one(), c_is_three()],
            min_count: 2,
        }),
        Filter::new_min_should(MinShould {
            conditions: vec![b_is_two()],
            min_count: 1,
        }),
    )
    .await;
    assert!(
        points.is_empty(),
        "expected no points, got {}",
        points.len()
    );
}
