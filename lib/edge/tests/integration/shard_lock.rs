// `EdgeVectorParams` still has the deprecated `on_disk` field, which a struct literal must set.
#![allow(deprecated)]

use std::collections::HashMap;

use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams};
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::ExtendedPointId;
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const VECTOR_NAME: &str = "edge-shard-lock-test-vector";

fn config(wal_enabled: bool) -> EdgeConfig {
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
                memory: None,
                hnsw_config: None,
            },
        )
        .wal_enabled(wal_enabled)
        .build()
}

fn upsert(shard: &EdgeShard, id: u64) {
    let point = PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![id as f32]),
        )]))),
        payload: None,
    };
    shard
        .update(PointOperation(UpsertPoints(PointsList(vec![point]))))
        .unwrap();
}

/// A WAL-less shard takes no WAL lock, so without a directory lock a second load opens a second
/// engine over the same segment files.
#[test]
fn second_load_of_wal_less_shard_is_refused() {
    let dir = tempfile::Builder::new()
        .prefix("edge-lock-wal-less")
        .tempdir()
        .unwrap();
    let first = EdgeShard::new(dir.path(), config(false)).unwrap();
    upsert(&first, 1);

    let Err(err) = EdgeShard::load(dir.path(), None) else {
        panic!("a second load of a live WAL-less shard must fail");
    };
    assert!(edge::is_shard_locked(&err), "{err}");

    drop(first);
    let reopened = EdgeShard::load(dir.path(), None).unwrap();
    assert_eq!(reopened.info().unwrap().points_count, 1);
}

/// A second load of a held shard must fail before it reads the shard. The loader deletes segment
/// directories without a completeness marker, which the first handle's optimizer creates while
/// it builds a segment.
#[test]
fn second_load_of_held_shard_does_not_touch_segments() {
    let dir = tempfile::Builder::new()
        .prefix("edge-lock-held")
        .tempdir()
        .unwrap();
    let first = EdgeShard::new(dir.path(), config(true)).unwrap();
    upsert(&first, 1);
    let in_flight = dir.path().join(SEGMENTS_PATH).join("segment-being-built");
    fs_err::create_dir(&in_flight).unwrap();

    let Err(err) = EdgeShard::load(dir.path(), None) else {
        panic!("a second load of a held shard must fail");
    };
    assert!(edge::is_shard_locked(&err), "{err}");
    assert!(
        in_flight.exists(),
        "the refused load deleted a segment directory"
    );
    drop(first);
}

/// The lock is released with the shard, and `new` respects it too.
#[test]
fn lock_follows_the_shard_lifetime() {
    let dir = tempfile::Builder::new()
        .prefix("edge-lock-lifetime")
        .tempdir()
        .unwrap();
    let first = EdgeShard::new(dir.path(), config(true)).unwrap();
    let Err(err) = EdgeShard::new(dir.path(), config(true)) else {
        panic!("new on a held path must fail");
    };
    assert!(edge::is_shard_locked(&err), "{err}");
    drop(first);
    drop(EdgeShard::load(dir.path(), None).unwrap());
}
