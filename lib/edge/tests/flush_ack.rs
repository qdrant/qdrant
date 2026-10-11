//! Regression coverage for #11068: `EdgeShard::flush` must acknowledge the
//! persisted segment version so the WAL truncates its closed segments.
//! Before the fix, the WAL directory only ever grew, and every closed
//! segment stayed on disk forever.

use std::collections::HashMap;
use std::fs;
use std::num::NonZero;
use std::path::Path;

use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams, WalOptions};
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::ExtendedPointId;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const VECTOR_NAME: &str = "edge-flush-ack-test-vector";

fn tiny_wal_config() -> EdgeConfig {
    // A 1 KiB segment capacity forces the WAL to close a segment after
    // every upsert, which makes the closed-segment count grow fast enough
    // to detect within a handful of operations.
    EdgeConfig::builder()
        .vector(
            VECTOR_NAME,
            EdgeVectorParams {
                size: 1,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )
        .wal_options(WalOptions {
            segment_capacity: 1024,
            segment_queue_len: 0,
            retain_closed: NonZero::new(1).unwrap(),
        })
        .build()
}

fn point(id: ExtendedPointId) -> PointStructPersisted {
    PointStructPersisted {
        id,
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![0.0]),
        )]))),
        payload: None,
    }
}

fn count_closed_segments(wal_dir: &Path) -> usize {
    let entries = match fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("closed-"))
        })
        .count()
}

#[test]
fn flush_acknowledges_persisted_version() {
    let dir = tempfile::Builder::new()
        .prefix("edge-flush-ack")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), tiny_wal_config()).unwrap();

    // Write 50 points with a flush after every one. Each flush must
    // acknowledge the persisted version, keeping the closed-segment
    // count at `retain_closed` (= 1) rather than climbing to 50.
    for i in 0..50 {
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(
                ExtendedPointId::NumId(i as u64),
            )]))))
            .unwrap();
        shard.flush().unwrap();
    }

    let closed_after = count_closed_segments(&dir.path().join("wal"));
    assert_eq!(
        closed_after, 1,
        "flush() must keep only retain_closed (= 1) closed segments, found {closed_after}"
    );

    shard.flush().unwrap();
    let closed_after_double_flush = count_closed_segments(&dir.path().join("wal"));
    assert_eq!(
        closed_after_double_flush, 1,
        "a second flush() with no new writes must not grow the closed-segment count"
    );
}

#[test]
fn reload_after_flush_preserves_upserts() {
    // The reporter's secondary observation in #11068: after the WAL is
    // truncated, the on-disk segments carry the latest operation numbers.
    // `Wal::ack` keeps the newest closed segment, which `Wal::open` reads on
    // reload to recover the next operation number. An upsert of an existing
    // point with a new payload must take effect after reload.
    let dir = tempfile::Builder::new()
        .prefix("edge-flush-ack-reload")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), tiny_wal_config()).unwrap();
    shard
        .update(PointOperation(UpsertPoints(PointsList(vec![point(
            ExtendedPointId::NumId(1),
        )]))))
        .unwrap();
    shard.flush().unwrap();

    drop(shard);

    // Re-open and upsert a new payload on the same point id. The point
    // count must remain 1 (no duplicate insert) and the new payload must
    // be visible.
    let shard = EdgeShard::load(dir.path(), None).unwrap();
    shard
        .update(PointOperation(UpsertPoints(PointsList(vec![point(
            ExtendedPointId::NumId(1),
        )]))))
        .unwrap();
    shard.flush().unwrap();

    assert_eq!(shard.info().unwrap().points_count, 1);
}
