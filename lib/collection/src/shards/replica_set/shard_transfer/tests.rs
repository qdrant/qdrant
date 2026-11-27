use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use rstest::rstest;
use segment::types::Distance;
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use super::*;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{CountRequestInternal, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_trait::WaitUntil;
use crate::wal_delta::LockedWal;

const TEST_COLLECTION_ID: &str = "test_collection";
const TEST_SHARD_ID: ShardId = 1;
const TEST_PEER_ID: PeerId = 1;
const TEST_REMOTE_PEER_ID: PeerId = 2;

/// How long to let queue proxification run before cancelling it. It must be blocked on the WAL
/// lock by then.
const CANCEL_AFTER: Duration = Duration::from_millis(100);

/// Cancelling queue proxification while it waits for the WAL lock must keep the local shard.
///
/// Before the fix, the local shard was taken out of the replica set before waiting for the WAL
/// lock, so cancelling at that point dropped it and left the replica set without a local shard.
#[rstest]
#[case::latest_version(false)]
#[case::from_version(true)]
#[tokio::test]
async fn test_cancel_queue_proxify_local_keeps_local_shard(#[case] with_version: bool) {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let replica_set = new_shard_replica_set(&collection_dir).await;

    upsert_point(&replica_set, 1).await;

    let from_version = if with_version {
        let version = replica_set.wal_version().await.unwrap();
        assert!(version.is_some(), "WAL must have an operation");
        version
    } else {
        None
    };

    // Hold the WAL lock, so queue proxification has to wait for it
    let wal = local_wal(&replica_set).await;
    let wal_lock = wal.lock().await;

    // Cancel queue proxification while it waits for the WAL lock
    let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
    let result = tokio::time::timeout(
        CANCEL_AFTER,
        replica_set.queue_proxify_local(remote_shard(), from_version, progress.clone()),
    )
    .await;
    assert!(result.is_err(), "queue proxify must wait for the WAL lock");

    drop(wal_lock);

    assert!(
        matches!(*replica_set.local.read().await, Some(Shard::Local(_))),
        "local shard must still be in the replica set after cancelling queue proxification",
    );

    // Local shard keeps working
    upsert_point(&replica_set, 2).await;
    assert_eq!(count_points(&replica_set).await, 2);

    // And can be queue proxified again
    replica_set
        .queue_proxify_local(remote_shard(), from_version, progress)
        .await
        .unwrap();
    assert!(replica_set.is_queue_proxy().await);

    replica_set.revert_queue_proxy_local().await;
    assert!(matches!(
        *replica_set.local.read().await,
        Some(Shard::Local(_)),
    ));
    assert_eq!(count_points(&replica_set).await, 2);

    replica_set.stop_gracefully().await;
}

/// Queue proxification waits for a held WAL lock, and completes once it is released.
#[rstest]
#[case::latest_version(false)]
#[case::from_version(true)]
#[tokio::test]
async fn test_queue_proxify_local_waits_for_wal_lock(#[case] with_version: bool) {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let replica_set = Arc::new(new_shard_replica_set(&collection_dir).await);

    upsert_point(&replica_set, 1).await;

    let from_version = if with_version {
        replica_set.wal_version().await.unwrap()
    } else {
        None
    };

    // Hold the WAL lock, so queue proxification has to wait for it
    let wal = local_wal(&replica_set).await;
    let wal_lock = wal.lock().await;

    let queue_proxify = tokio::spawn({
        let replica_set = Arc::clone(&replica_set);
        let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
        async move {
            replica_set
                .queue_proxify_local(remote_shard(), from_version, progress)
                .await
        }
    });

    tokio::time::sleep(CANCEL_AFTER).await;
    assert!(
        !queue_proxify.is_finished(),
        "queue proxify must wait for the WAL lock",
    );

    drop(wal_lock);

    queue_proxify.await.unwrap().unwrap();
    assert!(replica_set.is_queue_proxy().await);

    replica_set.revert_queue_proxy_local().await;
    assert!(matches!(
        *replica_set.local.read().await,
        Some(Shard::Local(_)),
    ));
    assert_eq!(count_points(&replica_set).await, 1);

    replica_set.stop_gracefully().await;
}

/// Queue proxification from a version outside of WAL bounds fails, and keeps the local shard.
#[tokio::test]
async fn test_queue_proxify_local_out_of_wal_bounds_keeps_local_shard() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let replica_set = new_shard_replica_set(&collection_dir).await;

    upsert_point(&replica_set, 1).await;

    let version = replica_set.wal_version().await.unwrap().unwrap();

    let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));
    let result = replica_set
        .queue_proxify_local(remote_shard(), Some(version + 2), progress)
        .await;
    assert!(result.is_err(), "version past WAL bounds must be rejected");

    assert!(matches!(
        *replica_set.local.read().await,
        Some(Shard::Local(_)),
    ));
    assert_eq!(count_points(&replica_set).await, 1);

    replica_set.stop_gracefully().await;
}

async fn local_wal(replica_set: &ShardReplicaSet) -> LockedWal {
    match replica_set.local.read().await.deref() {
        Some(Shard::Local(local)) => Arc::clone(&local.wal.wal),
        _ => panic!("replica set must have a plain local shard"),
    }
}

fn remote_shard() -> RemoteShard {
    RemoteShard::new(
        TEST_SHARD_ID,
        TEST_COLLECTION_ID.to_string(),
        TEST_REMOTE_PEER_ID,
        ChannelService::default(),
    )
}

async fn upsert_point(replica_set: &ShardReplicaSet, id: u64) {
    let operation = OperationWithClockTag::from(CollectionUpdateOperations::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperationsInternal::PointsList(vec![
            PointStructPersisted {
                id: id.into(),
                vector: VectorStructPersisted::Single(vec![0.1, 0.2, 0.3, 0.4]),
                payload: None,
            },
        ])),
    ));

    replica_set
        .update_local(operation, WaitUntil::Visible, None, false)
        .measured(AmbientContext::new())
        .await
        .expect("failed to upsert point")
        .expect("local shard must be present");
}

async fn count_points(replica_set: &ShardReplicaSet) -> usize {
    replica_set
        .count_local(
            Arc::new(CountRequestInternal {
                filter: None,
                exact: true,
            }),
            None,
            DeferredBehavior::VisibleOnly,
        )
        .measured(AmbientContext::new())
        .await
        .expect("failed to count points")
        .expect("local shard must be present")
        .count
}

async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
    let update_runtime = Handle::current();
    let search_runtime = AdaptiveSearchHandle::current_for_tests();

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        shard_number: NonZeroU32::new(1).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        ..CollectionParams::empty()
    };

    let optimizers_config = OptimizersConfig::fixture();
    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: optimizers_config.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: None,
        strict_mode_config: None,
        uuid: None,
        metadata: None,
        created_at: None,
    };

    let payload_index_schema_file = collection_dir.path().join("payload-schema.json");
    let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());
    let shared_config = Arc::new(RwLock::new(config));

    ShardReplicaSet::build(
        TEST_SHARD_ID,
        None,
        TEST_COLLECTION_ID.to_string(),
        TEST_PEER_ID,
        true,
        HashSet::new(),
        dummy_on_replica_failure(),
        dummy_abort_shard_transfer(),
        collection_dir.path(),
        shared_config,
        optimizers_config.clone(),
        Arc::new(SharedStorageConfig::default()),
        payload_index_schema,
        ChannelService::default(),
        update_runtime,
        search_runtime,
        ResourceBudget::default(),
        Some(ReplicaState::Active),
    )
    .await
    .unwrap()
}

fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_shard_transfer, _reason| {})
}
