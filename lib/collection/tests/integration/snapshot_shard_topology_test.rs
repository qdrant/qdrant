//! A collection snapshot must stay restorable if the shard topology changes while it is created.
//!
//! The snapshot archives the shards it lists under the shards holder lock, then waits for their
//! data without holding the lock. Creating a shard key or starting resharding in that window adds
//! a shard. The archived collection config and shard key mapping must not reference it, because
//! its data is not in the snapshot.
//!
//! Schema changes in that window are the opposite case: they are in the archived WAL, so the
//! archived collection config must include them.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, ShardingMethod, WalConfig};
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId};
use common::ambient::{self, AmbientContext, AmbientFutureExt as _};
use common::budget::ResourceBudget;
use futures::FutureExt as _;
use segment::data_types::vector_name_config::{DenseVectorConfig, VectorNameConfig};
use segment::types::{Distance, ShardKey};
use shard::snapshots::snapshot_data::SnapshotData;
use tempfile::Builder;
use tokio::task::JoinHandle;
use tonic::transport::Uri;
use uuid::Uuid;

use crate::common::{
    NoopReshardingConsensus, REST_PORT, TEST_OPTIMIZERS_CONFIG, dummy_abort_shard_transfer,
    dummy_on_replica_failure, dummy_request_shard_transfer, simple_collection_fixture,
};

const PEER_ID: PeerId = 0;

/// Interval for polling the shards holder lock
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Upper bound for every wait in these tests
const TIMEOUT: Duration = Duration::from_secs(30);

fn key(name: &str) -> ShardKey {
    ShardKey::Keyword(name.into())
}

/// Custom sharded collection with a single shard key `a`
async fn custom_sharded_collection(path: &Path) -> Collection {
    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorParamsBuilder::new(4, Distance::Dot).build().into(),
            shard_number: NonZeroU32::new(1).unwrap(),
            sharding_method: Some(ShardingMethod::Custom),
            ..CollectionParams::empty()
        },
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
        created_at: None,
    };

    // Creating a shard key validates that the placement peers are known
    let channel_service = ChannelService::new(REST_PORT, false, None, None);
    channel_service
        .id_to_address
        .write()
        .insert(PEER_ID, Uri::from_static("http://127.0.0.1:6333"));

    let collection = Collection::new(
        "test".to_string(),
        PEER_ID,
        path,
        &path.join("snapshots"),
        &config,
        Arc::default(),
        CollectionShardDistribution::all_local(Some(0), PEER_ID),
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
        .create_shard_key(key("a"), vec![vec![PEER_ID]], ReplicaState::Active)
        .await
        .unwrap();

    collection
}

/// Start a snapshot of `collection` and park it right after it released the shards holder lock
///
/// Returns the snapshot task and a sender that unparks it when dropped. At that point the
/// snapshot has listed its shards, but not yet archived any shard data or collection metadata.
async fn start_parked_snapshot(
    collection: &Arc<Collection>,
    snapshots_temp_dir: &Path,
) -> (JoinHandle<String>, std::sync::mpsc::Sender<()>) {
    let shards_holder = collection.shards_holder();
    let replica_set = {
        let shards_holder = shards_holder.read().await;
        let mut shards = shards_holder.all_shards();
        let replica_set = Arc::clone(shards.next().unwrap());
        assert!(shards.next().is_none(), "expected a single shard");
        replica_set
    };
    let segments = replica_set.local_segments_for_test().await.unwrap();

    // Park the shard snapshot future, it runs after the shards holder lock is released
    let (unpark, parked) = std::sync::mpsc::channel::<()>();
    let (segments_locked_tx, segments_locked_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let _segments_guard = segments.write();
        segments_locked_tx.send(()).unwrap();
        // Returns once the sender is dropped
        let _ = parked.recv();
    });
    segments_locked_rx.await.unwrap();

    // Hold the snapshot inside the shards holder lock, so we know when it got there
    let local_guard = replica_set.lock_local_for_test().await;

    let snapshot_task = tokio::spawn({
        let collection = Arc::clone(collection);
        let snapshots_temp_dir = snapshots_temp_dir.to_path_buf();
        async move {
            collection
                .create_snapshot(&snapshots_temp_dir, PEER_ID)
                .await
                .unwrap()
                .name
        }
        .measured(AmbientContext::new())
    });

    // Wait for the snapshot to take the shards holder lock
    tokio::time::timeout(TIMEOUT, async {
        while shards_holder.write().now_or_never().is_some() {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("snapshot must take the shards holder lock");

    // Let it list the shards, then wait for it to release the lock
    drop(local_guard);
    tokio::time::timeout(TIMEOUT, async {
        while shards_holder.write().now_or_never().is_none() {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("snapshot must release the shards holder lock");

    assert!(
        !snapshot_task.is_finished(),
        "snapshot must be parked on the shard data",
    );

    (snapshot_task, unpark)
}

/// Restore the snapshot and load it as a collection
async fn restore_snapshot(snapshot_path: &Path, path: &Path) -> Collection {
    let snapshot_data = SnapshotData::new_packed_persistent(snapshot_path);
    if let Err(err) = Collection::restore_snapshot(snapshot_data, path, PEER_ID, false) {
        panic!("Failed to restore snapshot: {err}");
    }

    Collection::load(
        "test_restored".to_string(),
        PEER_ID,
        path,
        &path.join("snapshots"),
        Arc::default(),
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

async fn shard_ids(collection: &Collection) -> Vec<ShardId> {
    let mut ids: Vec<_> = collection.state().await.shards.keys().copied().collect();
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_with_concurrent_shard_key_creation() {
    let _scope = ambient::async_test_guard();
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let collection = Arc::new(custom_sharded_collection(dir.path()).await);
    let shard_ids_a = collection.get_shard_ids(&key("a")).await.unwrap();

    let (snapshot_task, unpark) =
        start_parked_snapshot(&collection, snapshots_temp_dir.path()).await;

    // Add a shard key, and so a shard, while the snapshot is in progress
    tokio::time::timeout(
        TIMEOUT,
        collection.create_shard_key(key("b"), vec![vec![PEER_ID]], ReplicaState::Active),
    )
    .await
    .expect("creating a shard key must not wait for the snapshot")
    .unwrap();
    assert_eq!(shard_ids(&collection).await.len(), 2);

    drop(unpark);
    let snapshot_name = snapshot_task.await.unwrap();

    // The snapshot must only reference the shard of key `a`, the shard of key `b` is not in it
    let restore_dir = Builder::new().prefix("restored").tempdir().unwrap();
    let restored = restore_snapshot(
        &dir.path().join("snapshots").join(snapshot_name),
        restore_dir.path(),
    )
    .await;
    assert_eq!(
        restored.get_shard_ids(&key("a")).await.unwrap(),
        shard_ids_a
    );
    assert!(restored.get_shard_ids(&key("b")).await.is_err());
    assert_eq!(shard_ids(&restored).await, shard_ids_a);

    restored.stop_gracefully().await;
    collection.stop_gracefully().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_with_concurrent_resharding_up() {
    let _scope = ambient::async_test_guard();
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let collection = Arc::new(simple_collection_fixture(dir.path(), 1).await);

    let (snapshot_task, unpark) =
        start_parked_snapshot(&collection, snapshots_temp_dir.path()).await;

    // Add shard 1 and bump the shard number while the snapshot is in progress
    let resharding_key = ReshardKey {
        uuid: Uuid::new_v4(),
        direction: ReshardingDirection::Up,
        peer_id: PEER_ID,
        shard_id: 1,
        shard_key: None,
    };
    tokio::time::timeout(
        TIMEOUT,
        collection.start_resharding(
            resharding_key,
            Box::new(NoopReshardingConsensus),
            std::future::ready(()),
            std::future::ready(()),
        ),
    )
    .await
    .expect("starting resharding must not wait for the snapshot")
    .unwrap();
    assert_eq!(collection.config().await.params.shard_number.get(), 2);
    assert_eq!(shard_ids(&collection).await, [0, 1]);

    drop(unpark);
    let snapshot_name = snapshot_task.await.unwrap();

    // The snapshot must only reference shard 0, shard 1 is not in it
    let restore_dir = Builder::new().prefix("restored").tempdir().unwrap();
    let restored = restore_snapshot(
        &dir.path().join("snapshots").join(snapshot_name),
        restore_dir.path(),
    )
    .await;
    assert_eq!(restored.config().await.params.shard_number.get(), 1);
    assert_eq!(shard_ids(&restored).await, [0]);

    restored.stop_gracefully().await;
    collection.stop_gracefully().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_with_concurrent_vector_creation() {
    let _scope = ambient::async_test_guard();
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let snapshots_temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
    let collection = Arc::new(simple_collection_fixture(dir.path(), 1).await);

    let (snapshot_task, unpark) =
        start_parked_snapshot(&collection, snapshots_temp_dir.path()).await;

    // Create a named vector while the snapshot is in progress
    let vector_config = VectorNameConfig::dense(DenseVectorConfig {
        size: 4,
        distance: Distance::Dot,
        multivector_config: None,
        datatype: None,
    });
    tokio::time::timeout(
        TIMEOUT,
        collection
            .create_named_vector("extra".into(), vector_config)
            .measured(AmbientContext::new()),
    )
    .await
    .expect("creating a named vector must not wait for the snapshot")
    .unwrap();

    drop(unpark);
    let snapshot_name = snapshot_task.await.unwrap();

    // The shard data and WAL in the snapshot have the vector, so the config must declare it
    let restore_dir = Builder::new().prefix("restored").tempdir().unwrap();
    let restored = restore_snapshot(
        &dir.path().join("snapshots").join(snapshot_name),
        restore_dir.path(),
    )
    .await;
    assert!(
        restored
            .config()
            .await
            .params
            .vectors
            .get_params("extra")
            .is_some(),
    );

    restored.stop_gracefully().await;
    collection.stop_gracefully().await;
}
