use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use rstest::rstest;
use segment::types::Distance;
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;
use tokio::sync::{RwLock, mpsc, oneshot};

use super::*;
use crate::collection::Collection;
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
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::shards::shard::ShardId;
use crate::shards::shard_trait::WaitUntil;
use crate::tests::fixtures::create_collection_config;
use crate::update_handler::UpdateSignal;

const TEST_COLLECTION_ID: &str = "test_collection";
const TEST_TARGET_SHARD_ID: ShardId = 1;
const TEST_SOURCE_SHARD_ID: ShardId = 2;
const TEST_PEER_ID: PeerId = 1;

/// How long the abandoned recovery stalls before restoring, once it is clear the
/// retry is not going to finish because it is queued behind it.
const STALLED_RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[rstest]
#[case::file(false)]
#[case::stream(true)]
#[tokio::test]
async fn test_snapshot_setup_releases_holder_while_update_queue_is_full(#[case] streaming: bool) {
    let collection_dir = tempfile::tempdir().unwrap();
    let snapshots_dir = tempfile::tempdir().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let config = create_collection_config();
    let distribution = CollectionShardDistribution::all_local(Some(1), TEST_PEER_ID);
    let collection = Collection::new(
        TEST_COLLECTION_ID.to_string(),
        TEST_PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Arc::new(Default::default()),
        distribution,
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
    let shard = collection
        .shards_holder()
        .read()
        .await
        .get_shard(0)
        .cloned()
        .unwrap();
    let local_read = shard.local.read().await;
    let Some(Shard::Local(local)) = local_read.as_ref() else {
        panic!("fixture must have a local shard");
    };

    // Replace the sender with a full queue whose receiver we control. Snapshot setup
    // must wait here until we make room for its plunger.
    let (sender, mut receiver) = mpsc::channel(1);
    sender.try_send(UpdateSignal::Nop).unwrap();
    let original_sender = local.update_sender.swap(Arc::new(sender));
    let mut snapshot = Box::pin(async {
        if streaming {
            use actix_web::Responder as _;

            let stream = collection
                .stream_shard_snapshot(0, None, temp_dir.path())
                .await?;
            let request = actix_web::test::TestRequest::default().to_http_request();
            let response = stream.respond_to(&request);
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            assert!(!body.is_empty());
        } else {
            let description = collection.create_shard_snapshot(0, temp_dir.path()).await?;
            let snapshot_path = snapshots_dir.path().join("shards/0").join(description.name);
            assert!(snapshot_path.is_file());
        }
        CollectionResult::Ok(())
    });

    assert!(futures::poll!(snapshot.as_mut()).is_pending());

    // Probe the holder directly: a strict-mode update also needs the local-shard
    // write lock, which waits for snapshot setup's local-shard read lock.
    let holder = collection.shards_holder();
    let writer = tokio::time::timeout(Duration::from_secs(10), holder.write())
        .await
        .expect("holder writer must complete while snapshot setup waits for queue capacity");
    drop(writer);
    tokio::time::timeout(Duration::from_secs(10), collection.state())
        .await
        .expect("collection reader must complete while snapshot setup waits for queue capacity");

    // Restore the worker's sender before completing setup. The pending send must
    // still use our full queue, proving setup reached that send before the writer ran.
    local.update_sender.store(original_sender);
    assert!(matches!(receiver.try_recv(), Ok(UpdateSignal::Nop)));
    let signal = tokio::select! {
        result = &mut snapshot => panic!("snapshot finished before sending its plunger: {result:?}"),
        signal = tokio::time::timeout(Duration::from_secs(10), receiver.recv()) => {
            signal.unwrap().unwrap()
        }
    };
    let UpdateSignal::Plunger(notify) = signal else {
        panic!("snapshot setup must send a plunger");
    };

    notify.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), snapshot)
        .await
        .unwrap()
        .unwrap();

    drop(local_read);
    collection.stop_gracefully().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_snapshot_recovery_before_initializing_flag_does_not_mark_dirty() {
    let target_collection_dir = Builder::new()
        .prefix("snapshot-recovery-cancel-target")
        .tempdir()
        .unwrap();
    let source_collection_dir = Builder::new()
        .prefix("snapshot-recovery-cancel-source")
        .tempdir()
        .unwrap();

    let target_replica_set =
        new_shard_replica_set(&target_collection_dir, TEST_TARGET_SHARD_ID).await;
    let source_replica_set =
        new_shard_replica_set(&source_collection_dir, TEST_SOURCE_SHARD_ID).await;

    let source_shard_path = source_collection_dir
        .path()
        .join(TEST_SOURCE_SHARD_ID.to_string());
    assert!(
        LocalShard::check_data(&source_shard_path),
        "test fixture must create a valid source local shard"
    );

    let shard_flag =
        shard_initializing_flag_path(target_collection_dir.path(), TEST_TARGET_SHARD_ID);
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    install_restore_local_replica_before_flag_hook(shard_flag.clone(), reached_tx, release_rx);

    let cancel = cancel::CancellationToken::new();
    let restore = target_replica_set.restore_local_replica_from(
        &source_shard_path,
        RecoveryType::Full,
        target_collection_dir.path(),
        cancel.clone(),
    );
    tokio::pin!(restore);

    tokio::select! {
        _ = reached_rx => {}
        result = &mut restore => {
            panic!("snapshot recovery completed before the cancellation window: {result:?}");
        }
    }

    cancel.cancel();
    let _ = release_tx.send(());

    let result = restore.await;
    assert!(
        matches!(result, Err(CollectionError::Cancelled { .. })),
        "recovery should observe cancellation before creating the initializing flag, got {result:?}"
    );
    assert!(
        !shard_flag.exists(),
        "cancellation before destructive restore starts must not leave a false dirty marker"
    );
    assert!(
        !target_replica_set.is_dummy().await,
        "cancellation before destructive restore starts must keep the old local shard installed"
    );

    let local = target_replica_set.local.read().await;
    assert!(
        matches!(local.as_ref(), Some(Shard::Local(_))),
        "the old local shard should remain installed"
    );
    drop(local);

    target_replica_set.stop_gracefully().await;
    source_replica_set.stop_gracefully().await;
}

/// An abandoned snapshot recovery must not roll back the one that replaced it.
///
/// Unserialized, two recoveries of one shard resolve as *last to finish wins*, and the
/// abandoned one wins: it is abandoned because it stalled, so it finishes after the
/// retry it was replaced by, and restores on top of it.
///
/// ```text
/// stalled: clear -> download ..........................-> restore -> rolls back retry
/// retry:      clear -> download -> restore -> Ok to caller -> caller writes
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn test_abandoned_snapshot_recovery_does_not_roll_back_the_retry() {
    let target_collection_dir = Builder::new()
        .prefix("concurrent-recovery-target")
        .tempdir()
        .unwrap();

    let target_replica_set =
        Arc::new(new_shard_replica_set(&target_collection_dir, TEST_TARGET_SHARD_ID).await);

    // `clear_local_for_snapshot_recovery` refuses to run on a source-of-truth replica.
    // The receiving replica of a snapshot transfer sits in `PartialSnapshot`.
    target_replica_set
        .set_replica_state(TEST_PEER_ID, ReplicaState::PartialSnapshot)
        .await
        .unwrap();

    // One downloaded copy of the shard snapshot per recovery: `restore_local_replica_from`
    // moves the data out of the directory it is given, so they cannot share one.
    let (_stalled_snapshot_dir, stalled_snapshot) = new_shard_snapshot().await;
    let (_retry_snapshot_dir, retry_snapshot) = new_shard_snapshot().await;

    let (stalled_cleared_tx, stalled_cleared_rx) = oneshot::channel();
    let (retry_done_tx, retry_done_rx) = oneshot::channel();

    // The recovery that will be abandoned. It clears the shard, then stalls in its
    // download for as long as the retry needs.
    let stalled_recovery = {
        let replica_set = Arc::clone(&target_replica_set);
        let collection_path = target_collection_dir.path().to_path_buf();

        tokio::spawn(async move {
            let _recovery_lock = replica_set.take_snapshot_recovery_lock().await;

            replica_set
                .clear_local_for_snapshot_recovery(&collection_path)
                .await
                .unwrap();

            let _ = stalled_cleared_tx.send(());

            // Stall until the retry is done - or, once recoveries are serialized, until
            // it is clear the retry cannot proceed because it is waiting on this lock.
            let _ = tokio::time::timeout(STALLED_RECOVERY_TIMEOUT, retry_done_rx).await;

            replica_set
                .restore_local_replica_from(
                    &stalled_snapshot,
                    RecoveryType::Full,
                    &collection_path,
                    cancel::CancellationToken::new(),
                )
                .await
                .unwrap()
        })
    };

    // The retry lands while the stalled recovery is in flight.
    stalled_cleared_rx.await.unwrap();

    let retry_recovery = {
        let replica_set = Arc::clone(&target_replica_set);
        let collection_path = target_collection_dir.path().to_path_buf();

        tokio::spawn(async move {
            let _recovery_lock = replica_set.take_snapshot_recovery_lock().await;

            replica_set
                .clear_local_for_snapshot_recovery(&collection_path)
                .await
                .unwrap();

            let restored = replica_set
                .restore_local_replica_from(
                    &retry_snapshot,
                    RecoveryType::Full,
                    &collection_path,
                    cancel::CancellationToken::new(),
                )
                .await
                .unwrap();

            // The retry reported success, so its caller acts on it: the sender switches
            // the transfer to `Partial` and flushes its queue proxy into the shard.
            replica_set
                .set_replica_state(TEST_PEER_ID, ReplicaState::Partial)
                .await
                .unwrap();
            upsert_point(&replica_set, 42).await;

            let _ = retry_done_tx.send(());

            restored
        })
    };

    assert!(
        retry_recovery.await.unwrap(),
        "the retry should have restored the shard"
    );
    assert!(
        stalled_recovery.await.unwrap(),
        "the stalled recovery should have restored the shard"
    );

    assert_eq!(
        count_points(&target_replica_set).await,
        1,
        "the abandoned recovery rolled back the retry that replaced it: the write \
         the caller made after the retry reported success was discarded",
    );

    target_replica_set.stop_gracefully().await;
}

/// Build a valid unpacked shard snapshot to recover from.
///
/// Returns the temp dir - which the caller must keep alive - and the replica path
/// inside it, in the shape `restore_local_replica_from` expects.
async fn new_shard_snapshot() -> (TempDir, std::path::PathBuf) {
    let collection_dir = Builder::new()
        .prefix("concurrent-recovery-snapshot")
        .tempdir()
        .unwrap();

    let replica_set = new_shard_replica_set(&collection_dir, TEST_SOURCE_SHARD_ID).await;
    replica_set.force_flush_local_for_test().await;
    replica_set.stop_gracefully().await;

    let replica_path = collection_dir.path().join(TEST_SOURCE_SHARD_ID.to_string());
    assert!(
        LocalShard::check_data(&replica_path),
        "test fixture must produce a valid shard snapshot"
    );

    (collection_dir, replica_path)
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
        .update_local(
            operation,
            WaitUntil::Visible,
            None,
            HwMeasurementAcc::new(),
            false,
        )
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
            HwMeasurementAcc::new(),
            DeferredBehavior::VisibleOnly,
        )
        .await
        .expect("failed to count points")
        .expect("local shard must be present")
        .count
}

fn install_restore_local_replica_before_flag_hook(
    shard_flag: std::path::PathBuf,
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
) {
    let mut hook = RESTORE_LOCAL_REPLICA_BEFORE_FLAG_HOOK.lock().unwrap();
    assert!(
        hook.is_none(),
        "restore-local-replica test hook is already installed"
    );
    *hook = Some((shard_flag, reached, release));
}

async fn new_shard_replica_set(collection_dir: &TempDir, shard_id: ShardId) -> ShardReplicaSet {
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
        shard_id,
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
