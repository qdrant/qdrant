//! Regression test for WAL-less shard snapshot clock consistency.
//!
//! A shard-transfer (WAL-less) snapshot archives the shard's clock maps but not its WAL. The
//! archived clocks must not be ahead of the snapshotted segment data: otherwise a shard recovered
//! from such a snapshot - e.g. an aborted snapshot transfer, where the queue proxy never replays
//! the remaining operations - ends up with a recovery point ahead of its data, and a later
//! WAL-delta recovery silently skips the missing operations.
//!
//! The fix captures the clocks *before* the snapshot's plunger (so the snapshotted segments are
//! guaranteed to include every operation reflected in them) instead of reading the persisted clock
//! files, which track the WAL write position and run ahead of the applied segment state.
//!
//! Requires the `staging` feature for the `QDRANT__STAGING__SNAPSHOT_SHARD_SEGMENTS_DELAY` hook.
#![cfg(feature = "staging")]

use std::collections::HashMap;
use std::io::Read;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use collection::collection::Collection;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use collection::operations::shared_storage_config::SharedStorageConfig;
use collection::operations::types::{NodeType, VectorsConfig};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::channel_service::ChannelService;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::Distance;
use tempfile::Builder;
use tokio::time::sleep;

use crate::common::{
    REST_PORT, dummy_abort_shard_transfer, dummy_on_replica_failure, dummy_request_shard_transfer,
};

const PEER_ID: u64 = 0;
const SHARD_ID: u32 = 0;
const INITIAL_POINTS: u64 = 100;

/// Minimal view of the archived `newest_clocks.json` (a serialized `ClockMap`); we only need the
/// tick of each clock. Extra fields (`token`, `snapshot`) are ignored.
#[derive(serde::Deserialize)]
struct ArchivedClockMap {
    clocks: Vec<ArchivedClock>,
}

#[derive(serde::Deserialize)]
struct ArchivedClock {
    peer_id: u64,
    clock_id: u32,
    current_tick: u64,
}

async fn upsert_point(collection: &Collection, id: u64, wait: bool) {
    let point = PointStructPersisted {
        id: id.into(),
        vector: VectorStructPersisted::Single(vec![id as f32, 0.0, 0.0, 0.0]),
        payload: None,
    };
    let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![point]),
    ));
    collection
        .update_from_client_simple(
            operation,
            wait,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
}

/// Create a WAL-less shard snapshot while updates run concurrently, then assert the clocks archived
/// into the snapshot are not ahead of the snapshotted segment data.
#[tokio::test(flavor = "multi_thread")]
async fn test_wal_less_snapshot_clocks_not_ahead_of_data() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Short flush interval so background updates persist the advanced clocks to disk during the
    // snapshot - that is what made the (pre-fix) on-disk clocks run ahead of the segment data.
    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 2,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 1,
        max_optimization_threads: Some(2),
        prevent_unoptimized: None,
    };

    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
            shard_number: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        },
        optimizer_config,
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
    };

    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
    let snapshots_dir = Builder::new().prefix("test_snapshots").tempdir().unwrap();
    let snapshot_temp_dir = Builder::new().prefix("snapshot_temp").tempdir().unwrap();

    let collection = Collection::new(
        "test".to_string(),
        PEER_ID,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Arc::new(SharedStorageConfig {
            node_type: NodeType::Normal,
            ..Default::default()
        }),
        CollectionShardDistribution::all_local(Some(1), PEER_ID),
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
    .await
    .unwrap();

    collection
        .set_shard_replica_state(SHARD_ID, PEER_ID, ReplicaState::Active, None)
        .await
        .unwrap();

    // Insert the initial, fully-applied data set. This is exactly what the snapshot's segments will
    // contain (background updates below are submitted after the snapshot's plunger).
    for id in 0..INITIAL_POINTS {
        upsert_point(&collection, id, true).await;
    }

    let collection = Arc::new(collection);

    // Continuously upsert *new* points in the background while the snapshot is being created. These
    // updates are submitted after the snapshot's plunger, so they are not part of the snapshotted
    // segments, but they advance - and, thanks to the short flush interval, persist - the clocks.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let collection = collection.clone();
        let stop = stop.clone();
        async move {
            // Let the snapshot capture the segments first, so these updates form the "gap".
            sleep(Duration::from_millis(1500)).await;
            let mut id = INITIAL_POINTS;
            while !stop.load(Ordering::Relaxed) {
                upsert_point(&collection, id, false).await;
                id += 1;
                sleep(Duration::from_millis(20)).await;
            }
        }
    });

    // Recovery point of the data the snapshot will contain, captured before any background update.
    let reference: HashMap<(u64, u32), u64> = collection
        .shard_recovery_point(SHARD_ID)
        .await
        .unwrap()
        .iter_as_clock_tags()
        .map(|tag| ((tag.peer_id, tag.clock_id), tag.clock_tick))
        .collect();
    assert!(
        !reference.is_empty(),
        "the initial data set should have advanced the shard clocks",
    );

    // Delay between snapshotting the segments and archiving the clocks, so the background updates
    // push the persisted clocks ahead of the snapshotted segment data.
    unsafe {
        std::env::set_var("QDRANT__STAGING__SNAPSHOT_SHARD_SEGMENTS_DELAY", "5");
    }

    let snapshot = collection
        .create_shard_snapshot(SHARD_ID, snapshot_temp_dir.path())
        .await
        .unwrap();

    unsafe {
        std::env::remove_var("QDRANT__STAGING__SNAPSHOT_SHARD_SEGMENTS_DELAY");
    }
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();

    // Read the clocks archived into the snapshot tar.
    let snapshot_path = snapshots_dir
        .path()
        .join(format!("shards/{SHARD_ID}"))
        .join(&snapshot.name);
    let archived = read_archived_newest_clocks(&snapshot_path);
    assert!(
        !archived.clocks.is_empty(),
        "the snapshot should archive the shard clocks",
    );

    // The archived clocks must not exceed the reference recovery point (the clocks of the data the
    // snapshot actually contains). If they do, a shard recovered from this WAL-less snapshot would
    // have a recovery point ahead of its data and lose operations on a later WAL-delta recovery.
    for clock in &archived.clocks {
        let reference_tick = reference.get(&(clock.peer_id, clock.clock_id)).copied();
        assert!(
            reference_tick.is_some_and(|tick| clock.current_tick <= tick),
            "archived clock (peer={}, clock_id={}) tick {} is ahead of the snapshotted data \
             (reference tick {reference_tick:?}); WAL-less snapshot clocks must not exceed the \
             snapshotted segment data",
            clock.peer_id,
            clock.clock_id,
            clock.current_tick,
        );
    }
}

/// Extract and parse `newest_clocks.json` from a shard snapshot tar.
fn read_archived_newest_clocks(snapshot_path: &Path) -> ArchivedClockMap {
    let file = fs_err::File::open(snapshot_path)
        .unwrap_or_else(|err| panic!("failed to open snapshot {}: {err}", snapshot_path.display()));
    let mut archive = tar::Archive::new(file);

    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().into_owned();
        if path == Path::new("newest_clocks.json") {
            let mut contents = String::new();
            entry.read_to_string(&mut contents).unwrap();
            return serde_json::from_str(&contents).unwrap_or_else(|err| {
                panic!("failed to parse archived newest_clocks.json: {err}")
            });
        }
    }

    panic!(
        "snapshot {} did not contain newest_clocks.json",
        snapshot_path.display(),
    );
}
