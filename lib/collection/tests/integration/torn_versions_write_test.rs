//! A torn write of a segment's point-versions file must not cost a point whose data is durable
//! and whose operation the WAL no longer holds.

use std::path::{Path, PathBuf};

use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::shards::local_shard::LocalShard;
use collection::shards::shard_path;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use fs_err as fs;
use tempfile::Builder;

use crate::common::{load_local_collection, simple_collection_fixture};

const POINT_COUNT: u64 = 8;
const VERSIONS_FILE: &str = "mutable_id_tracker.versions";

#[tokio::test(flavor = "multi_thread")]
async fn test_torn_versions_write_keeps_point() {
    let collection_dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection_path = collection_dir.path();
    let snapshots_path = collection_path.join("snapshots");

    let collection = simple_collection_fixture(collection_path, 1).await;

    let ids = (0..POINT_COUNT).map(u64::into).collect();
    let vectors = (0..POINT_COUNT)
        .map(|i| vec![i as f32, 0.0, 1.0, 1.0])
        .collect();
    let insert_points = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsBatch(BatchPersisted {
            ids,
            vectors: BatchVectorStructPersisted::Single(vectors),
            payloads: None,
        }),
    ));
    collection
        .update_from_client_simple(
            insert_points,
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap();
    collection.stop_gracefully().await;

    // Reloading replays the WAL and ends in a forced sync flush, so every point is durable on disk
    // without depending on the flush worker's timer.
    let collection =
        load_local_collection("test".to_string(), collection_path, &snapshots_path).await;
    collection.stop_gracefully().await;

    let shard_path = shard_path(collection_path, 0);

    // The flush worker acknowledges flushed operations and drops them from the WAL. Do it here so
    // the state under test is the steady one, reached whenever a flush completes.
    let wal_path = LocalShard::wal_path(&shard_path);
    for entry in fs::read_dir(&wal_path).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    // Point versions are flushed last, as a dense array of one u64 per slot. Cutting its tail is
    // what a kill part-way through the flush order leaves behind: a slot mapped, but versionless.
    let versions_path = largest_versions_file(&LocalShard::segments_path(&shard_path));
    let versions_len = fs::metadata(&versions_path).unwrap().len();
    let slot = size_of::<u64>() as u64;
    assert!(versions_len >= 2 * slot, "expected a segment with points");
    fs::OpenOptions::new()
        .write(true)
        .open(&versions_path)
        .unwrap()
        .set_len(versions_len - slot)
        .unwrap();

    let collection =
        load_local_collection("test".to_string(), collection_path, &snapshots_path).await;
    let points_count = collection
        .info(&ShardSelectorInternal::All)
        .await
        .unwrap()
        .points_count;
    assert_eq!(
        points_count,
        Some(POINT_COUNT as usize),
        "repair dropped a point that the WAL can no longer restore",
    );
    collection.stop_gracefully().await;
}

fn largest_versions_file(segments_path: &Path) -> PathBuf {
    fs::read_dir(segments_path)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path().join(VERSIONS_FILE);
            let len = path.is_file().then(|| fs::metadata(&path).unwrap().len())?;
            Some((len, path))
        })
        .max_by_key(|(len, _)| *len)
        .expect("no mutable ID tracker in any segment")
        .1
}
