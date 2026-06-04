/// Integration tests for `qdrant-edge-ffi`.
///
/// These tests exercise the full DB invariant path through the FFI surface:
/// persistence, payload round-trips, concurrent safety, and missing-ID contracts.
/// Each test creates its own isolated tempdir shard so they can run in parallel
/// without interference.
use std::collections::HashMap;
use std::sync::Arc;

use qdrant_edge_ffi::config::{Distance, EdgeConfig, VectorDataConfig};
use qdrant_edge_ffi::error::EdgeError;
use qdrant_edge_ffi::query::CountRequest;
use qdrant_edge_ffi::types::{PointId, Vector, Point, WithPayload, WithVector};
use qdrant_edge_ffi::update::UpdateOperation;
use qdrant_edge_ffi::EdgeShard;

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Minimal 4-D Dot-distance single vector-field config.
/// The field is named "vec"; points use `Vector::Named` to match.
fn make_config() -> EdgeConfig {
    EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    }
}

/// Build a `Vector::Named` with a 4-D dense vector under key "vec".
fn named_vec(values: [f32; 4]) -> Vector {
    use qdrant_edge_ffi::types::NamedVector;
    Vector::Named {
        map: HashMap::from([(
            "vec".to_string(),
            NamedVector::Dense {
                values: values.to_vec(),
            },
        )]),
    }
}

/// Upsert three points (IDs 1, 2, 3) with simple payloads into `shard`.
fn upsert_three(shard: &EdgeShard) {
    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"title":"point one"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.4, 0.3, 0.2, 0.1]),
            payload: Some(r#"{"title":"point two"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"title":"point three"}"#.to_string()),
        },
    ];
    let op = UpdateOperation::upsert_points(points).expect("upsert_points failed");
    shard.update(op).expect("shard.update failed");
}

// ── Test 1: persistence_survives_reload ───────────────────────────────────────

/// THE core DB invariant: data written and flushed in one session must be
/// fully visible after loading the same path in a new session.
#[test]
fn persistence_survives_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // Session 1: create shard, upsert 3 points, flush, then unload.
    {
        let shard: Arc<EdgeShard> = EdgeShard::load(path.clone(), Some(make_config()))
            .expect("load (session 1) failed");
        upsert_three(&shard);
        shard.flush().expect("flush failed");
        shard.unload();
        // Arc drops here; shard is fully closed.
    }

    // Session 2: reload from the SAME path with config: None.
    {
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), None).expect("load (session 2) failed");

        // points_count must be 3.
        let info = shard.info().expect("info failed");
        assert_eq!(
            info.points_count, 3,
            "expected 3 points after reload, got {}",
            info.points_count
        );

        // count() must agree.
        let n = shard
            .count(CountRequest { filter: None, exact: true })
            .expect("count failed");
        assert_eq!(n, 3, "count() returned {} after reload", n);

        // retrieve() must return all 3 IDs.
        let ids = vec![
            PointId::NumId { value: 1 },
            PointId::NumId { value: 2 },
            PointId::NumId { value: 3 },
        ];
        let records = shard
            .retrieve(
                ids,
                Some(WithPayload::Bool { enable: false }),
                Some(WithVector::Bool { enable: false }),
            )
            .expect("retrieve failed");
        assert_eq!(
            records.len(),
            3,
            "retrieve returned {} records after reload",
            records.len()
        );
    }
}

// ── Test 2: crash_recovery_after_dirty_drop ───────────────────────────────────

/// Simulate a "dirty" shutdown by dropping the Arc without calling
/// flush/unload explicitly, then reloading the same path.
///
/// The Drop implementation on `edge::EdgeShard` performs a final flush, so
/// normal-Arc-drop already synchronises data. This test verifies that the
/// WAL/segment load path produces the correct point count on reload.
///
/// NOTE: True kill-9 crash recovery (where Drop never runs) requires a
/// subprocess-level test; that is out of scope for this crate's unit/integration
/// harness. The WAL replays all pending ops on the next load — this test
/// exercises the load path but not the kill-9 path.
#[test]
fn crash_recovery_after_dirty_drop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // "Dirty" session: upsert then drop without explicit flush/unload.
    {
        let shard: Arc<EdgeShard> = EdgeShard::load(path.clone(), Some(make_config()))
            .expect("load failed");
        upsert_three(&shard);
        // Drop here — no flush() or unload() call.
    }

    // Reload and assert the 3 points are present.
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path.clone(), None).expect("reload after dirty drop failed");

    let info = shard.info().expect("info failed");
    assert_eq!(
        info.points_count, 3,
        "expected 3 points after dirty-drop reload, got {}",
        info.points_count
    );
}

// ── Test 3: payload_round_trips_through_ffi ───────────────────────────────────

/// A nested JSON payload written through `upsert_points` must be retrievable
/// with structural equality (not byte-identical string equality — key order
/// is not guaranteed by JSON serialisation).
#[test]
fn payload_round_trips_through_ffi() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    let original_json = r#"{"a":1,"nested":{"b":true},"arr":[1,2,3]}"#;

    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let op = UpdateOperation::upsert_points(vec![Point {
        id: PointId::NumId { value: 42 },
        vector: named_vec([0.1, 0.2, 0.3, 0.4]),
        payload: Some(original_json.to_string()),
    }])
    .expect("upsert_points failed");
    shard.update(op).expect("update failed");

    let records = shard
        .retrieve(
            vec![PointId::NumId { value: 42 }],
            Some(WithPayload::Bool { enable: true }),
            Some(WithVector::Bool { enable: false }),
        )
        .expect("retrieve failed");

    assert_eq!(records.len(), 1, "expected exactly 1 record");
    let payload_str = records[0]
        .payload
        .as_deref()
        .expect("payload was None but expected Some");

    // Compare as serde_json::Value so key ordering differences don't matter.
    let expected: serde_json::Value =
        serde_json::from_str(original_json).expect("failed to parse original JSON");
    let actual: serde_json::Value =
        serde_json::from_str(payload_str).expect("failed to parse retrieved JSON");

    assert_eq!(
        expected, actual,
        "payload round-trip mismatch: expected {expected}, got {actual}"
    );
}

// ── Test 4: concurrent_reads_and_unload ──────────────────────────────────────

/// Multiple threads reading concurrently while the main thread calls `unload`
/// must never panic or crash. Each operation must return either Ok or
/// `EdgeError::ShardClosed` — no other error is acceptable.
#[test]
fn concurrent_reads_and_unload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let thread_count = 4;
    let iterations = 50;

    let handles: Vec<_> = (0..thread_count)
        .map(|_| {
            let shard_clone = Arc::clone(&shard);
            std::thread::spawn(move || {
                for _ in 0..iterations {
                    let result = shard_clone.count(CountRequest { filter: None, exact: false });
                    match result {
                        Ok(_) => {}
                        Err(EdgeError::ShardClosed) => {}
                        Err(other) => {
                            panic!(
                                "unexpected error during concurrent count: {:?}",
                                other
                            );
                        }
                    }
                }
            })
        })
        .collect();

    // Unload from main thread while reader threads may still be running.
    shard.unload();

    for handle in handles {
        handle.join().expect("reader thread panicked");
    }
}

// ── Test 5: retrieve_missing_ids_omitted ─────────────────────────────────────

/// The documented contract: IDs that do not exist in the shard are silently
/// omitted from retrieve() results — no error is raised.
#[test]
fn retrieve_missing_ids_omitted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    // Upsert only IDs 1 and 2.
    let op = UpdateOperation::upsert_points(vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: None,
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.4, 0.3, 0.2, 0.1]),
            payload: None,
        },
    ])
    .expect("upsert_points failed");
    shard.update(op).expect("update failed");

    // Retrieve IDs 1 and 999 — only 1 exists.
    let records = shard
        .retrieve(
            vec![PointId::NumId { value: 1 }, PointId::NumId { value: 999 }],
            Some(WithPayload::Bool { enable: false }),
            Some(WithVector::Bool { enable: false }),
        )
        .expect("retrieve should succeed even with missing IDs");

    assert_eq!(
        records.len(),
        1,
        "expected exactly 1 record (999 should be silently omitted), got {}",
        records.len()
    );

    // Confirm the returned record is ID 1, not some phantom.
    match &records[0].id {
        PointId::NumId { value } => assert_eq!(*value, 1, "returned ID should be 1"),
        other => panic!("unexpected PointId variant: {:?}", other),
    }
}

// ── Test 6: snapshot_unpack_then_query ────────────────────────────────────────

// TODO: The v1 FFI surface exposes `unpack_snapshot(snapshot_path, target_path)`
// for consuming a pre-made snapshot archive, but there is NO method on `EdgeShard`
// to CREATE a snapshot (no `create_snapshot` / `snapshot` constructor).  Without
// the ability to produce a snapshot from within the test, this test cannot be
// implemented in a self-contained way.  When snapshot creation is added to the
// FFI surface (e.g. `EdgeShard::create_snapshot(path) -> Result<()>`), this
// test should:
//   1. Create a shard and upsert points.
//   2. Call `shard.create_snapshot(snapshot_path)`.
//   3. Call `unpack_snapshot(snapshot_path, target_path)`.
//   4. `EdgeShard::load(target_path, None)` and assert `info().points_count`.
