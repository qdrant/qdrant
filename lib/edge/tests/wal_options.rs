use std::collections::HashMap;
use std::num::NonZero;

use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams, WalOptions};
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::ExtendedPointId;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const VECTOR_NAME: &str = "edge-wal-options-test-vector";

fn test_config() -> EdgeConfig {
    EdgeConfig {
        on_disk_payload: false,
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: 1,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )]),
        sparse_vectors: HashMap::new(),
        hnsw_config: Default::default(),
        quantization_config: None,
        optimizers: Default::default(),
    }
}

fn small_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 4 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}

fn point(id: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![id as f32]),
        )]))),
        payload: None,
    }
}

#[test]
fn load_with_wal_options_accepts_custom_capacity() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-custom")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
    drop(shard);

    let shard = EdgeShard::load_with_wal_options(dir.path(), None, small_wal_options()).unwrap();
    drop(shard);
}

#[test]
fn load_still_works_with_default_wal_options() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-default")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
    drop(shard);

    let shard = EdgeShard::load(dir.path(), None).unwrap();
    drop(shard);
}

/// Mismatching WAL options on reload: create with default 32 MiB, reload
/// with custom 4 MiB after upserting a point. Verifies the WAL segments
/// on disk can be opened with a smaller segment_capacity and the point
/// is still readable. This is the central concern raised by upstream
/// review (qdrant/qdrant#9067).
#[test]
fn reload_with_smaller_wal_capacity_after_upsert() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-shrink")
        .tempdir()
        .unwrap();

    // Phase 1: create with default 32 MiB WAL, upsert one point.
    {
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(42)]))))
            .unwrap();
        assert_eq!(shard.info().points_count, 1);
        // drop -> flushes WAL + segments.
    }

    // Phase 2: reload with custom small 4 MiB WAL.
    let shard = EdgeShard::load_with_wal_options(dir.path(), None, small_wal_options()).unwrap();
    assert_eq!(
        shard.info().points_count,
        1,
        "point must survive reload with mismatched WAL options"
    );

    // Phase 3: write another point under the smaller WAL.
    shard
        .update(PointOperation(UpsertPoints(PointsList(vec![point(43)]))))
        .unwrap();
    assert_eq!(shard.info().points_count, 2);
}

/// Symmetric case: create with custom small 4 MiB, reload with default
/// 32 MiB. Verifies WAL segments aren't truncated or rejected when the
/// new segment_capacity is larger than what they were created with.
#[test]
fn reload_with_larger_wal_capacity_after_upsert() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-grow")
        .tempdir()
        .unwrap();

    // Phase 1: create via new() then immediately reload with custom
    // small WAL options (new() itself can't take WalOptions in this
    // PR — only load() can — but the flow exercises the same surface
    // the embedded use case relies on).
    {
        let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
        drop(shard);
        let shard =
            EdgeShard::load_with_wal_options(dir.path(), None, small_wal_options()).unwrap();
        shard
            .update(PointOperation(UpsertPoints(PointsList(vec![point(100)]))))
            .unwrap();
        assert_eq!(shard.info().points_count, 1);
    }

    // Phase 2: reload with default 32 MiB WAL.
    let shard = EdgeShard::load(dir.path(), None).unwrap();
    assert_eq!(
        shard.info().points_count,
        1,
        "point must survive reload with default WAL options"
    );

    // Phase 3: write another point under the default WAL.
    shard
        .update(PointOperation(UpsertPoints(PointsList(vec![point(101)]))))
        .unwrap();
    assert_eq!(shard.info().points_count, 2);
}
