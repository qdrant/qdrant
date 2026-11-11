//! Collection reads must not hold the `shards_holder` read lock while awaiting shard requests.
//! Otherwise a slow read blocks collection state changes, such as `apply_state`, that need the
//! write lock.
//!
//! Bug: <https://github.com/qdrant/qdrant/issues/11455>

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use rstest::rstest;
use segment::data_types::facets::FacetParams;
use segment::data_types::vectors::NamedQuery;
use segment::types::{
    Distance, ExtendedPointId, Payload, PayloadFieldSchema, PayloadSchemaType,
    WithPayloadInterface, WithVector,
};
use serde_json::{Map, Value};
use shard::count::CountRequestInternal;
use shard::query::query_enum::QueryEnum;
use tempfile::Builder;

use crate::collection::Collection;
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{
    CollectionResult, CoreSearchRequest, PointRequestInternal, ScrollRequestInternal, VectorsConfig,
};
use crate::operations::universal_query::shard_query::{ScoringQuery, ShardQueryRequest};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_trait::WaitUntil;
use crate::tests::points_dedup::{
    dummy_abort_shard_transfer, dummy_on_replica_failure, dummy_request_shard_transfer,
};

const DIM: u64 = 4;
const PEER_ID: PeerId = 1;
const SHARD_ID: ShardId = 0;
const POINT_COUNT: u64 = 10;
const FIELD: &str = "num";

/// Time for a spawned read to reach the blocked local shard
const READ_SETTLE_TIME: Duration = Duration::from_millis(100);

/// Upper bound for taking the shards holder write lock while a read is in flight
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum ReadOp {
    Count,
    Scroll,
    Retrieve,
    Search,
    Query,
    Facet,
    Info,
    MemoryReport,
}

async fn fixture(path: &Path) -> Collection {
    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM, Distance::Dot).build()),
            shard_number: NonZeroU32::new(1).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        },
        optimizer_config: OptimizersConfig::fixture(),
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

    let collection_path = path.join("collection");
    let snapshots_path = path.join("snapshots");
    fs_err::create_dir_all(&collection_path).unwrap();
    fs_err::create_dir_all(&snapshots_path).unwrap();

    let shards: AHashMap<ShardId, HashSet<PeerId>> =
        AHashMap::from_iter([(SHARD_ID, HashSet::from([PEER_ID]))]);

    let collection = Collection::new(
        "test".to_string(),
        PEER_ID,
        &collection_path,
        &snapshots_path,
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
        .set_shard_replica_state(SHARD_ID, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    // Facet requires a payload index
    collection
        .create_payload_index_with_wait(
            FIELD.parse().unwrap(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
            true,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();

    let points = (0..POINT_COUNT)
        .map(|id| PointStructPersisted {
            id: ExtendedPointId::from(id),
            vector: VectorStructPersisted::Single(vec![1.0; DIM as usize]),
            payload: Some(Payload(Map::from_iter([(
                FIELD.to_string(),
                Value::from(id),
            )]))),
        })
        .collect();
    collection
        .update_from_client(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(points),
            )),
            WaitUntil::Visible,
            None,
            WriteOrdering::Weak,
            None,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();

    collection
}

async fn read(collection: &Collection, op: ReadOp) -> CollectionResult<()> {
    // Select by shard ID to read local only, which waits for a locked local shard. A cluster read
    // would fail over and return early instead.
    let shard_selection = ShardSelectorInternal::ShardId(SHARD_ID);
    match op {
        ReadOp::Count => {
            let request = CountRequestInternal {
                filter: None,
                exact: true,
            };
            collection
                .count(request, None, None, &shard_selection, None)
                .await?;
        }
        ReadOp::Scroll => {
            let request = ScrollRequestInternal {
                offset: None,
                limit: Some(POINT_COUNT as usize),
                filter: None,
                with_payload: Some(false.into()),
                with_vector: false.into(),
                order_by: None,
            };
            collection
                .scroll_by(request, None, None, &shard_selection, None)
                .await?;
        }
        ReadOp::Retrieve => {
            let request = PointRequestInternal {
                ids: vec![ExtendedPointId::from(0)],
                with_payload: None,
                with_vector: WithVector::Bool(false),
            };
            collection
                .retrieve(request, None, None, &shard_selection, None)
                .await?;
        }
        ReadOp::Search => {
            let request = CoreSearchRequest {
                query: QueryEnum::Nearest(NamedQuery::default_dense(vec![1.0; DIM as usize])),
                filter: None,
                params: None,
                limit: 10,
                offset: 0,
                with_payload: None,
                with_vector: None,
                score_threshold: None,
            };
            collection
                .search(request, None, None, &shard_selection, None)
                .await?;
        }
        ReadOp::Query => {
            let request = ShardQueryRequest {
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedQuery::default_dense(vec![1.0; DIM as usize]),
                ))),
                prefetches: vec![],
                filter: None,
                params: None,
                offset: 0,
                limit: 10,
                with_payload: WithPayloadInterface::Bool(false),
                with_vector: WithVector::Bool(false),
                score_threshold: None,
            };
            collection
                .query(request, None, None, shard_selection, None)
                .await?;
        }
        ReadOp::Facet => {
            let request = FacetParams {
                key: FIELD.parse().unwrap(),
                limit: 10,
                filter: None,
                exact: true,
            };
            collection
                .facet(request, shard_selection, None, None, None)
                .await?;
        }
        ReadOp::Info => {
            collection.info(&shard_selection).await?;
        }
        ReadOp::MemoryReport => {
            collection.memory_report().await?;
        }
    }
    Ok(())
}

/// A read blocked inside a shard must not block the shards holder write lock.
#[rstest]
#[case::count(ReadOp::Count)]
#[case::scroll(ReadOp::Scroll)]
#[case::retrieve(ReadOp::Retrieve)]
#[case::search(ReadOp::Search)]
#[case::query(ReadOp::Query)]
#[case::facet(ReadOp::Facet)]
#[case::info(ReadOp::Info)]
#[case::memory_report(ReadOp::MemoryReport)]
#[tokio::test(flavor = "multi_thread")]
async fn test_read_releases_shards_holder_lock(#[case] op: ReadOp) {
    let dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let collection = Arc::new(fixture(dir.path()).await);

    let replica_set = Arc::clone(
        collection
            .shards_holder
            .read()
            .await
            .get_shard(SHARD_ID)
            .unwrap(),
    );

    // Park the read inside the shard by blocking the local shard
    let local_guard = replica_set.lock_local_for_test().await;
    let read_task = tokio::spawn({
        let collection = Arc::clone(&collection);
        async move { read(&collection, op).await }.measured(AmbientContext::new())
    });
    tokio::time::sleep(READ_SETTLE_TIME).await;
    assert!(
        !read_task.is_finished(),
        "{op:?} must be blocked on the local shard",
    );

    // The shards holder write lock must be available while the read is in flight
    let shards_holder_guard = tokio::time::timeout(LOCK_TIMEOUT, collection.shards_holder.write())
        .await
        .unwrap_or_else(|_| panic!("{op:?} holds the shards holder lock while awaiting the shard"));
    drop(shards_holder_guard);
    assert!(!read_task.is_finished());

    // Unblock the shard, the read must complete
    drop(local_guard);
    read_task.await.unwrap().unwrap();
}
