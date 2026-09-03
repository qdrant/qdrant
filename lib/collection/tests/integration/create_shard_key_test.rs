//! `Collection::create_shard_key` treats `shard_key_mapping.json` as the operation's single
//! commit point: it is written once, after every shard of the key exists on disk. These tests
//! cover the states a consensus re-apply can land in — nothing committed, everything committed —
//! plus the duplicate-key case that must never be mistaken for a replay.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, ShardingMethod, WalConfig};
use collection::operations::types::CollectionError;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::{PeerId, ShardId, ShardsPlacement};
use collection::shards::shard_holder::SHARD_KEY_MAPPING_FILE;
use common::budget::ResourceBudget;
use segment::types::{Distance, ShardKey};
use tempfile::Builder;
use tonic::transport::Uri;

use crate::common::{
    REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer, dummy_on_replica_failure,
    dummy_request_shard_transfer,
};

const COLLECTION_ID: &str = "test_create_shard_key";
const PEER_ID: PeerId = 0;

fn key(name: &str) -> ShardKey {
    ShardKey::Keyword(name.into())
}

/// A placement of `shards` single-replica shards, all on the local peer
fn placement(shards: usize) -> ShardsPlacement {
    vec![vec![PEER_ID]; shards]
}

fn custom_sharding_config() -> CollectionConfigInternal {
    let params = CollectionParams {
        vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
        shard_number: NonZeroU32::new(1).unwrap(),
        sharding_method: Some(ShardingMethod::Custom),
        ..CollectionParams::empty()
    };

    CollectionConfigInternal {
        params,
        optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
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
    }
}

/// `create_shard_key` rejects placements referencing peers it does not know about, so the local
/// peer has to be resolvable through the channel service.
fn channel_service() -> ChannelService {
    let channel_service = ChannelService::new(REST_PORT, false, None, None);
    channel_service
        .id_to_address
        .write()
        .insert(PEER_ID, Uri::from_static("http://127.0.0.1:6333"));
    channel_service
}

/// Custom sharding starts out with no shards at all, so the distribution is empty
async fn new_collection(path: &Path) -> Collection {
    Collection::new(
        COLLECTION_ID.to_string(),
        PEER_ID,
        path,
        &path.join("snapshots"),
        &custom_sharding_config(),
        Arc::default(),
        CollectionShardDistribution::all_local(Some(0), PEER_ID),
        None,
        channel_service(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap()
}

async fn load_collection(path: &Path) -> Collection {
    Collection::load(
        COLLECTION_ID.to_string(),
        PEER_ID,
        path,
        &path.join("snapshots"),
        Arc::default(),
        channel_service(),
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

/// Shard ids the key mapping associates with `shard_key`
async fn mapped_shard_ids(collection: &Collection, shard_key: &ShardKey) -> Vec<ShardId> {
    let mut ids = collection.get_shard_ids(shard_key).await.unwrap();
    ids.sort_unstable();
    ids
}

/// Shard ids actually registered in the shard holder
async fn registered_shard_ids(collection: &Collection) -> Vec<ShardId> {
    let mut ids: Vec<_> = collection.state().await.shards.keys().copied().collect();
    ids.sort_unstable();
    ids
}

fn assert_bad_request(err: CollectionError) {
    assert!(
        matches!(err, CollectionError::BadRequest { .. }),
        "expected a dismissable user error, got {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_key_allocates_contiguous_ids() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = new_collection(dir.path()).await;

    collection
        .create_shard_key(key("k"), placement(3), ReplicaState::Active)
        .await
        .unwrap();

    // Ids are allocated past the highest mapped shard ID, which is 0 for an empty mapping
    assert_eq!(mapped_shard_ids(&collection, &key("k")).await, [1, 2, 3]);
    assert_eq!(registered_shard_ids(&collection).await, [1, 2, 3]);

    collection.stop_gracefully().await;
}

/// A crash anywhere before the mapping write leaves the peer in its pre-operation state, so the
/// re-apply runs from scratch and lands on the same shard ids — on top of the directories the
/// crashed attempt left behind.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_key_replays_after_crash_before_commit() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();

    {
        let collection = new_collection(dir.path()).await;
        collection
            .create_shard_key(key("k"), placement(3), ReplicaState::Active)
            .await
            .unwrap();
        collection.stop_gracefully().await;
    }

    // Roll the mapping back to what it was before the operation, keeping the shard directories.
    // This is the on-disk state of a peer that died partway through the create loop.
    fs_err::write(dir.path().join(SHARD_KEY_MAPPING_FILE), "[]").unwrap();
    for shard_id in 1..=3 {
        assert!(dir.path().join(shard_id.to_string()).is_dir());
    }

    let collection = load_collection(dir.path()).await;

    // Nothing was committed, so nothing is visible: `load_shards` only loads shards the mapping
    // references
    assert!(collection.state().await.shards_key_mapping.is_empty());
    assert!(registered_shard_ids(&collection).await.is_empty());

    collection
        .create_shard_key(key("k"), placement(3), ReplicaState::Active)
        .await
        .unwrap();

    assert_eq!(mapped_shard_ids(&collection, &key("k")).await, [1, 2, 3]);
    assert_eq!(registered_shard_ids(&collection).await, [1, 2, 3]);

    collection.stop_gracefully().await;
}

/// Re-applying an entry that already committed is dismissed, and changes nothing
#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_key_replay_after_commit_is_dismissed() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = new_collection(dir.path()).await;

    collection
        .create_shard_key(key("k"), placement(2), ReplicaState::Active)
        .await
        .unwrap();

    let err = collection
        .create_shard_key(key("k"), placement(2), ReplicaState::Active)
        .await
        .unwrap_err();
    assert_bad_request(err);

    assert_eq!(mapped_shard_ids(&collection, &key("k")).await, [1, 2]);
    assert_eq!(registered_shard_ids(&collection).await, [1, 2]);

    collection.stop_gracefully().await;
}

/// A duplicate `CreateShardKey` for an existing key is not a replay, and must not be reconciled
/// against the ids that key already holds: those ids anchor an allocation that runs straight into
/// live shards belonging to other keys.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_key_duplicate_with_different_placement_is_rejected() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = new_collection(dir.path()).await;

    collection
        .create_shard_key(key("k"), placement(2), ReplicaState::Active)
        .await
        .unwrap();
    collection
        .create_shard_key(key("other"), placement(1), ReplicaState::Active)
        .await
        .unwrap();

    assert_eq!(mapped_shard_ids(&collection, &key("k")).await, [1, 2]);
    assert_eq!(mapped_shard_ids(&collection, &key("other")).await, [3]);

    // Anchoring on the ids of "k" and skipping the ones it already has would allocate shard 3 —
    // which "other" owns — and overwrite it
    let err = collection
        .create_shard_key(key("k"), placement(3), ReplicaState::Active)
        .await
        .unwrap_err();
    assert_bad_request(err);

    assert_eq!(mapped_shard_ids(&collection, &key("k")).await, [1, 2]);
    assert_eq!(mapped_shard_ids(&collection, &key("other")).await, [3]);
    assert_eq!(registered_shard_ids(&collection).await, [1, 2, 3]);

    collection.stop_gracefully().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_shard_key_rejects_empty_placement() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = new_collection(dir.path()).await;

    let err = collection
        .create_shard_key(key("k"), placement(0), ReplicaState::Active)
        .await
        .unwrap_err();
    assert_bad_request(err);

    assert!(collection.state().await.shards_key_mapping.is_empty());

    collection.stop_gracefully().await;
}
