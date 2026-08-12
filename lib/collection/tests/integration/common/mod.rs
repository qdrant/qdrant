use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use collection::collection::{Collection, RequestShardTransfer};
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::types::CollectionResult;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::CollectionId;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::{
    ShardTransfer, ShardTransferConsensus, ShardTransferKey, ShardTransferMethod,
};
use common::budget::ResourceBudget;
use segment::types::Distance;

/// Test collections for this upper bound of shards.
/// Testing with more shards is problematic due to `number of open files problem`
/// See https://github.com/qdrant/qdrant/issues/379
pub const N_SHARDS: u32 = 3;

pub const REST_PORT: u16 = 6333;

pub const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    default_segment_number: 2,
    max_segment_size: None,
    #[expect(deprecated)]
    memmap_threshold: None,
    indexing_threshold: Some(50_000),
    flush_interval_sec: 30,
    max_optimization_threads: Some(2),
    prevent_unoptimized: None,
};

#[cfg(test)]
pub async fn simple_collection_fixture(collection_path: &Path, shard_number: u32) -> Collection {
    collection_fixture(
        collection_path,
        shard_number,
        TEST_OPTIMIZERS_CONFIG.clone(),
    )
    .await
}

/// Like [`simple_collection_fixture`], but with a custom optimizers config.
#[cfg(test)]
pub async fn collection_fixture(
    collection_path: &Path,
    shard_number: u32,
    optimizer_config: OptimizersConfig,
) -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
        shard_number: NonZeroU32::new(shard_number).expect("Shard number can not be zero"),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let snapshot_path = collection_path.join("snapshots");

    // Default to a collection with all the shards local
    new_local_collection(
        "test".to_string(),
        collection_path,
        &snapshot_path,
        &collection_config,
    )
    .await
    .unwrap()
}

/// `Collection::start_resharding` never touches its `ShardTransferConsensus`
/// argument (the resharding driver is disabled), so every method can be a stub.
pub struct NoopReshardingConsensus;

#[async_trait]
impl ShardTransferConsensus for NoopReshardingConsensus {
    fn this_peer_id(&self) -> PeerId {
        0
    }

    fn peers(&self) -> Vec<PeerId> {
        vec![0]
    }

    fn consensus_commit_term(&self) -> (u64, u64) {
        (0, 0)
    }

    fn recovered_switch_to_partial(
        &self,
        _transfer_config: &ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn start_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn restart_shard_transfer(
        &self,
        _transfer_config: ShardTransfer,
        _collection_id: CollectionId,
        _default_method: ShardTransferMethod,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn abort_shard_transfer(
        &self,
        _transfer: ShardTransferKey,
        _collection_id: CollectionId,
        _reason: &str,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn set_shard_replica_set_state(
        &self,
        _peer_id: Option<PeerId>,
        _collection_id: CollectionId,
        _shard_id: ShardId,
        _state: ReplicaState,
        _from_state: Option<ReplicaState>,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn commit_read_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }

    async fn commit_write_hashring(
        &self,
        _collection_id: CollectionId,
        _reshard_key: ReshardKey,
    ) -> CollectionResult<()> {
        unimplemented!("not exercised by start_resharding")
    }
}

pub fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

pub fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_transfer, _reason| {})
}

/// Default to a collection with all the shards local
#[cfg(test)]
pub async fn new_local_collection(
    id: CollectionId,
    path: &Path,
    snapshots_path: &Path,
    config: &CollectionConfigInternal,
) -> CollectionResult<Collection> {
    let collection = Collection::new(
        id,
        0,
        path,
        snapshots_path,
        config,
        Default::default(),
        CollectionShardDistribution::all_local(Some(config.params.shard_number.into()), 0),
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
    .await;

    let collection = collection?;

    let local_shards = collection.get_local_shards().await;
    for shard_id in local_shards {
        collection
            .set_shard_replica_state(shard_id, 0, ReplicaState::Active, None)
            .await?;
    }
    Ok(collection)
}

/// Default to a collection with all the shards local
#[cfg(test)]
pub async fn load_local_collection(
    id: CollectionId,
    path: &Path,
    snapshots_path: &Path,
) -> Collection {
    Collection::load(
        id,
        0,
        path,
        snapshots_path,
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
    .await
}
