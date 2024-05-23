use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use api::rest::VectorStruct;
use common::cpu::CpuBudget;
use segment::types::{Distance, ExtendedPointId, Payload, PayloadFieldSchema, PayloadSchemaType};
use serde_json::{Map, Value};
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::operations::point_ops::{PointInsertOperationsInternal, PointOperations, PointStruct};
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    OrderByInterface, PointRequestInternal, ScrollRequestInternal, VectorsConfig,
};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerState, ReplicaState};
use crate::shards::shard::{PeerId, ShardId};

const DIM: u64 = 4;
const PEER_ID: u64 = 1;
const SHARD_COUNT: u32 = 4;
const DUPLICATE_POINT_ID: ExtendedPointId = ExtendedPointId::NumId(100);

/// Create the collection used for deduplication tests.
async fn fixture() -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM, Distance::Dot).build()),
        shard_number: NonZeroU32::new(SHARD_COUNT).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let config = CollectionConfig {
        params: collection_params,
        optimizer_config: OptimizersConfig::fixture(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    };

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

    let collection_name = "test".to_string();
    let shards: HashMap<ShardId, HashSet<PeerId>> = (0..SHARD_COUNT)
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
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        CpuBudget::default(),
    )
    .await
    .unwrap();

    // Create payload index to allow order by
    collection
        .create_payload_index(
            "num".parse().unwrap(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
        )
        .await
        .expect("failed to create payload index");

    // Insert two points into all shards directly, a point matching the shard ID, and point 100
    // We insert into all shards directly to prevent spreading points by the hashring
    // We insert the same point into multiple shards on purpose
    for (shard_id, shard) in collection.shards_holder().write().await.get_shards() {
        let op = OperationWithClockTag::from(CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperationsInternal::PointsList(vec![
                PointStruct {
                    id: (*shard_id as u64).into(),
                    vector: VectorStruct::Multi(HashMap::new()),
                    payload: Some(Payload(Map::from_iter([(
                        "num".to_string(),
                        Value::from(-(*shard_id as i32)),
                    )]))),
                },
                PointStruct {
                    id: DUPLICATE_POINT_ID,
                    vector: VectorStruct::Multi(HashMap::new()),
                    payload: Some(Payload(Map::from_iter([(
                        "num".to_string(),
                        Value::from(100 - *shard_id as i32),
                    )]))),
                },
            ])),
        ));
        shard
            .update_local(op, true)
            .await
            .expect("failed to insert points");
    }

    // Activate all shards
    for shard_id in 0..SHARD_COUNT {
        collection
            .set_shard_replica_state(shard_id as ShardId, PEER_ID, ReplicaState::Active, None)
            .await
            .expect("failed to active shard");
    }

    collection
}

#[tokio::test(flavor = "multi_thread")]
async fn test_scroll_dedup() {
    let collection = fixture().await;

    // Scroll all points without ordering
    let result = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: None,
                with_payload: Some(false.into()),
                with_vector: false.into(),
                order_by: None,
            },
            None,
            &ShardSelectorInternal::All,
        )
        .await
        .expect("failed to search");
    assert!(!result.points.is_empty(), "expected some points");

    let mut seen = HashSet::new();
    for point_id in result.points.iter().map(|point| point.id) {
        assert!(
            seen.insert(point_id),
            "got point id {point_id} more than once, they should be deduplicated",
        );
    }

    // Scroll all points with ordering
    let result = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: None,
                with_payload: Some(false.into()),
                with_vector: false.into(),
                order_by: Some(OrderByInterface::Key("num".parse().unwrap())),
            },
            None,
            &ShardSelectorInternal::All,
        )
        .await
        .expect("failed to search");
    assert!(!result.points.is_empty(), "expected some points");

    let mut seen = HashSet::new();
    for point_id in result.points.iter().map(|point| point.id) {
        assert!(
            seen.insert(point_id),
            "got point id {point_id} more than once, they should be deduplicated",
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_retrieve_dedup() {
    let collection = fixture().await;

    let records = collection
        .retrieve(
            PointRequestInternal {
                ids: (0..SHARD_COUNT as u64)
                    .map(ExtendedPointId::from)
                    .chain([DUPLICATE_POINT_ID])
                    .collect(),
                with_payload: Some(false.into()),
                with_vector: false.into(),
            },
            None,
            &ShardSelectorInternal::All,
        )
        .await
        .expect("failed to search");
    assert!(!records.is_empty(), "expected some records");

    let mut seen = HashSet::new();
    for point_id in records.iter().map(|record| record.id) {
        assert!(
            seen.insert(point_id),
            "got point id {point_id} more than once, they should be deduplicated",
        );
    }
}

pub fn dummy_on_replica_failure() -> ChangePeerState {
    Arc::new(move |_peer_id, _shard_id| {})
}

pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

pub fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_transfer, _reason| {})
}
