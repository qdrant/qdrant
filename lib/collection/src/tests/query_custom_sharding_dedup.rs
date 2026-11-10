use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use segment::data_types::vectors::NamedQuery;
use segment::types::{Distance, ExtendedPointId, ShardKey, WithPayloadInterface, WithVector};
use shard::query::query_enum::QueryEnum;
use tempfile::Builder;

use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, ShardingMethod, WalConfig};
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
const PEER_ID: PeerId = 1;

/// Collection with two custom shard keys, each holding point id 1.
async fn fixture() -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM, Distance::Dot).build()),
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        sharding_method: Some(ShardingMethod::Custom),
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
        created_at: None,
    };

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

    let shards: AHashMap<ShardId, HashSet<PeerId>> =
        [(0, HashSet::from([PEER_ID]))].into_iter().collect();

    let storage_config: SharedStorageConfig = SharedStorageConfig::default();
    let storage_config = Arc::new(storage_config);

    let channel_service = ChannelService::default();
    channel_service
        .id_to_address
        .write()
        .insert(PEER_ID, "http://localhost:6333".parse().unwrap());

    let collection = Collection::new(
        "test".to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_path.path(),
        &config,
        storage_config.clone(),
        CollectionShardDistribution { shards },
        None,
        channel_service,
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
        .expect("failed to activate shard");

    for key in ["x", "y"] {
        collection
            .create_shard_key(
                ShardKey::from(key),
                vec![vec![PEER_ID]],
                ReplicaState::Active,
            )
            .await
            .unwrap_or_else(|_| panic!("failed to create shard key {key}"));

        let points = vec![PointStructPersisted {
            id: ExtendedPointId::from(1u64),
            vector: VectorStructPersisted::Single(vec![1.0, 0.0, 0.0, 0.0]),
            payload: None,
        }];
        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(points),
        ));
        collection
            .update_from_client(
                operation,
                WaitUntil::from(true),
                None,
                WriteOrdering::Weak,
                Some(ShardKey::from(key)),
            )
            .measured(AmbientContext::new())
            .await
            .unwrap_or_else(|_| panic!("failed to insert point under key {key}"));
    }

    collection
}

/// Cross-shard merge must not collapse same-id points living under different
/// custom shard keys (scroll prefetch gives every candidate score 1.0, so an
/// id-only dedup drops all but one twin before collection-level MMR runs).
///
/// Bug: <https://github.com/qdrant/qdrant/issues/11475>
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_keeps_same_id_different_shard_keys() {
    let collection = fixture().await;

    let results = collection
        .query(
            ShardQueryRequest {
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedQuery::default_dense(vec![1.0, 0.0, 0.0, 0.0]),
                ))),
                prefetches: vec![ShardPrefetch {
                    prefetches: vec![],
                    query: None,
                    limit: 10,
                    params: None,
                    filter: None,
                    score_threshold: None,
                }],
                filter: None,
                params: None,
                offset: 0,
                limit: 10,
                with_payload: WithPayloadInterface::Bool(false),
                with_vector: WithVector::Bool(false),
                score_threshold: None,
            },
            None,
            None,
            ShardSelectorInternal::All,
            None,
        )
        .measured(AmbientContext::new())
        .await
        .expect("query failed");

    let pairs: HashSet<(ExtendedPointId, Option<ShardKey>)> = results
        .iter()
        .map(|point| (point.id, point.shard_key.clone()))
        .collect();
    assert!(
        pairs.contains(&(ExtendedPointId::from(1u64), Some(ShardKey::from("x")))),
        "point 1 under key x missing: {pairs:?}",
    );
    assert!(
        pairs.contains(&(ExtendedPointId::from(1u64), Some(ShardKey::from("y")))),
        "point 1 under key y missing: {pairs:?}",
    );
}

fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(move |_transfer, _reason| {})
}
