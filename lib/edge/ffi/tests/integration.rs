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
use qdrant_edge_ffi::types::{NamedVector, Point, PointId, Vector, WithPayload, WithVector};
use qdrant_edge_ffi::update::UpdateOperation;
use qdrant_edge_ffi::{
    CountRequest, EdgeShard, Query, RetrieveRequest, ScrollRequest, SearchRequest,
};

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
                hnsw_config: None,
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
    let op = UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed");
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
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load (session 1) failed");
        upsert_three(&shard);
        shard.flush().expect("flush failed");
        shard.unload().expect("unload failed");
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
            .count(CountRequest {
                filter: None,
                exact: true,
            })
            .expect("count failed");
        assert_eq!(n, 3, "count() returned {n} after reload");

        // retrieve() must return all 3 IDs.
        let ids = vec![
            PointId::NumId { value: 1 },
            PointId::NumId { value: 2 },
            PointId::NumId { value: 3 },
        ];
        let records = shard
            .retrieve(RetrieveRequest {
                point_ids: ids,
                with_payload: Some(WithPayload::Bool { enable: false }),
                with_vector: Some(WithVector::Bool { enable: false }),
            })
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
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load failed");
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

    let op = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 42 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(original_json.to_string()),
        }],
        None,
        None,
    )
    .expect("upsert_points failed");
    shard.update(op).expect("update failed");

    let records = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 42 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: Some(WithVector::Bool { enable: false }),
        })
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
                    let result = shard_clone.count(CountRequest {
                        filter: None,
                        exact: false,
                    });
                    match result {
                        Ok(_) => {}
                        Err(EdgeError::ShardClosed) => {}
                        Err(other) => {
                            panic!("unexpected error during concurrent count: {other:?}");
                        }
                    }
                }
            })
        })
        .collect();

    // Unload from main thread while reader threads may still be running.
    shard.unload().expect("unload failed");

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
    let op = UpdateOperation::upsert_points(
        vec![
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
        ],
        None,
        None,
    )
    .expect("upsert_points failed");
    shard.update(op).expect("update failed");

    // Retrieve IDs 1 and 999 — only 1 exists.
    let records = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }, PointId::NumId { value: 999 }],
            with_payload: Some(WithPayload::Bool { enable: false }),
            with_vector: Some(WithVector::Bool { enable: false }),
        })
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
        other @ PointId::Uuid { .. } => panic!("unexpected PointId variant: {other:?}"),
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

// ── Test 7: scalar_quantization_accepted_at_load ──────────────────────────────

/// The FFI exposes all four quantization strategies for parity with the Python
/// Edge SDK, so `load` must ACCEPT a Scalar-quantized config (not reject it).
/// Edge core's `for_appendable_segment` filter may drop Scalar on the appendable
/// segment — that is shared engine behavior across all Edge SDKs, not something
/// this SDK rejects.
#[test]
fn scalar_quantization_accepted_at_load() {
    use qdrant_edge_ffi::config::{
        Memory, QuantizationConfig, ScalarQuantizationParams, ScalarType,
    };

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: Some(QuantizationConfig::Scalar {
                    config: ScalarQuantizationParams {
                        r#type: ScalarType::Int8,
                        quantile: Some(0.99),
                        memory: Some(Memory::Pinned),
                    },
                }),
                multivector_config: None,
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };

    let shard = EdgeShard::load(path, Some(config)).expect("Scalar quantization must be accepted");

    // config() is an "as-requested" read-back: it reports Scalar (what the host
    // asked for), even though the engine drops it on the appendable segment.
    let read_back = shard.config().expect("config() failed");
    let vd = read_back.vector_data.get("vec").expect("vec field present");
    assert!(
        matches!(
            vd.quantization_config,
            Some(QuantizationConfig::Scalar { .. })
        ),
        "config() should report the requested Scalar quantization, got {:?}",
        vd.quantization_config
    );
    // No HNSW was requested → it must read back as None (not the engine default).
    assert!(
        vd.hnsw_config.is_none(),
        "a field with no HNSW config must read back as None, got {:?}",
        vd.hnsw_config
    );
}

// ── Test 8: oversized_limit_rejected_not_allocated ────────────────────────────

/// C7 regression: a host-supplied `limit` flows into eager engine allocations
/// (e.g. `HashSet::with_capacity(limit)` on the random-scroll path). An
/// unbounded value like `u64::MAX` would request a multi-terabyte allocation and
/// *abort* the process — an abort that `panic = "unwind"` cannot catch. The
/// `bounded_limit` guard on the FFI boundary must reject it as a catchable
/// `InvalidArgument` BEFORE it reaches the engine.
///
/// Reaching the assertion at all is the primary proof (no abort); the
/// `InvalidArgument` check confirms it fails cleanly rather than some other way.
#[test]
fn oversized_limit_rejected_not_allocated() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let request = ScrollRequest {
        offset: None,
        limit: Some(u64::MAX),
        filter: None,
        with_payload: None,
        with_vector: None,
        order_by: None,
    };

    // `.err().expect()` rather than `.expect_err()`: see note in
    // `unsupported_quantization_rejected_at_load` — the Ok type involves a
    // non-`Debug` UniFFI object, so `expect_err` does not compile.
    #[allow(clippy::err_expect)]
    let err = shard
        .scroll(request)
        .err()
        .expect("scroll with u64::MAX limit should be rejected, not allocated");

    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for oversized limit, got {err:?}"
    );

    // The shard must still be usable after a rejected request — the guard fails
    // the single call, it does not poison the shard.
    let ok = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: None,
            with_vector: None,
            order_by: None,
        })
        .expect("scroll with a sane limit should succeed after a rejected one");
    assert_eq!(ok.records.len(), 3, "all three points should scroll back");
}

// ── Test 9: delete_then_reload_reduces_count ──────────────────────────────────

/// Core DB invariant: a delete must persist. Upsert three, delete one, flush,
/// reload from disk, and confirm the deleted point is gone (count drops to 2 and
/// retrieving its ID returns nothing). Until now only additive writes were
/// covered; this exercises the delete path end-to-end through the FFI.
#[test]
fn delete_then_reload_reduces_count() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    {
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load failed");
        upsert_three(&shard);

        let op = UpdateOperation::delete_points(vec![PointId::NumId { value: 2 }])
            .expect("delete_points failed");
        shard.update(op).expect("delete update failed");
        shard.flush().expect("flush failed");
    } // drop closes the shard

    let reloaded: Arc<EdgeShard> = EdgeShard::load(path, None).expect("reload after delete failed");

    let count = reloaded
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(count, 2, "deleted point must not survive reload");

    let got = reloaded
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 2 }],
            with_payload: None,
            with_vector: None,
        })
        .expect("retrieve failed");
    assert!(got.is_empty(), "deleted ID 2 must not be retrievable");
}

// ── Test 10: search_returns_ranked_results ────────────────────────────────────

/// The search path through the FFI was never exercised by an integration test.
/// Upsert three points and rank them under Dot distance against a known query.
///
/// Dot is the *raw* (unnormalized) dot product, so magnitude matters. The
/// hand-computed scores for query `[0.1,0.2,0.3,0.4]` are: ID 1
/// `[0.1,0.2,0.3,0.4]` → 0.30, ID 2 `[0.4,0.3,0.2,0.1]` → 0.20, ID 3
/// `[0.5,0.5,0.5,0.5]` → 0.50 (largest magnitude wins under Dot). So the
/// expected ranking is `[3, 1, 2]`, NOT "same-direction-first". This asserts the
/// real ranking, the descending order, and ID 3's exact score.
#[test]
fn search_returns_ranked_results() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let results = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: vec![0.1, 0.2, 0.3, 0.4],
                },
                using: Some("vec".to_string()),
            },
            limit: 3,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .expect("search failed");

    assert_eq!(results.len(), 3, "expected all three points ranked");

    let ranked_ids: Vec<u64> = results
        .iter()
        .map(|r| match &r.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID PointId: {value:?}"),
        })
        .collect();
    assert_eq!(
        ranked_ids,
        vec![3, 1, 2],
        "Dot-distance ranking should be [3, 1, 2] by raw dot product"
    );

    // Dot distance: higher score = nearer, so results must be non-increasing.
    assert!(
        results[0].score >= results[1].score && results[1].score >= results[2].score,
        "scores must be in descending order, got {:?}",
        results.iter().map(|r| r.score).collect::<Vec<_>>()
    );
    // Top hit (ID 3) has the largest dot product, 0.50.
    assert!(
        (results[0].score - 0.50).abs() < 1e-5,
        "top hit's Dot score should be ~0.50, got {}",
        results[0].score
    );
}

// ── Test 11: set_payload_visible_after_retrieve ───────────────────────────────

/// `set_payload` is the primary mutation beyond `upsert`, and was untested.
/// Upsert a point, set a new payload key on it, and confirm the key is visible
/// on retrieve.
#[test]
fn set_payload_visible_after_retrieve() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let op = UpdateOperation::set_payload(
        vec![PointId::NumId { value: 1 }],
        r#"{"tag":"hot"}"#.to_string(),
        None,
    )
    .expect("set_payload failed");
    shard.update(op).expect("set_payload update failed");

    let got = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");

    assert_eq!(got.len(), 1, "expected the one point back");
    let payload = got[0]
        .payload
        .as_deref()
        .expect("payload should be present after set_payload");
    let value: serde_json::Value =
        serde_json::from_str(payload).expect("payload should be valid JSON");
    assert_eq!(
        value.get("tag").and_then(|v| v.as_str()),
        Some("hot"),
        "set_payload key must be visible on retrieve, got {payload}"
    );
    // The original upsert payload must survive the merge (set_payload merges).
    assert_eq!(
        value.get("title").and_then(|v| v.as_str()),
        Some("point one"),
        "set_payload must merge, not replace; original key lost: {payload}"
    );
}

// ── Test 12: product_quantization_accepted_at_load ────────────────────────────

/// Parity with the Python Edge SDK: Product quantization must be ACCEPTED at
/// `load` (the FFI exposes all four strategies; the engine decides what it can
/// apply on its appendable segments).
#[test]
fn product_quantization_accepted_at_load() {
    use qdrant_edge_ffi::config::{
        CompressionRatio, Memory, ProductQuantizationParams, QuantizationConfig,
    };

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: Some(QuantizationConfig::Product {
                    config: ProductQuantizationParams {
                        compression: CompressionRatio::X16,
                        memory: Some(Memory::Pinned),
                    },
                }),
                multivector_config: None,
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };

    let shard = EdgeShard::load(path, Some(config));
    assert!(
        shard.is_ok(),
        "Product quantization must be accepted at load (parity with Python SDK), got {:?}",
        shard.err()
    );
}

// ── Test 12b: turbo_quantization_accepted_at_load ─────────────────────────────

/// Turbo (TurboQuant) is exposed for parity with the Python Edge SDK and IS
/// applied on appendable segments (`supports_appendable()` = Binary | Turbo).
/// `load` must accept it.
#[test]
fn turbo_quantization_accepted_at_load() {
    use qdrant_edge_ffi::config::{
        Memory, QuantizationConfig, TurboQuantBitSize, TurboQuantizationParams,
    };

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: Some(QuantizationConfig::Turbo {
                    config: TurboQuantizationParams {
                        memory: Some(Memory::Pinned),
                        bits: Some(TurboQuantBitSize::Bits4),
                    },
                }),
                multivector_config: None,
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };

    let shard = EdgeShard::load(path, Some(config));
    assert!(
        shard.is_ok(),
        "Turbo quantization must be accepted at load, got {:?}",
        shard.err()
    );
}

// ── Test 13: binary_quantization_accepted_at_load ─────────────────────────────

/// C-Quant positive case: Binary quantization IS supported on appendable
/// segments, so `validate` must NOT reject it. Guards against over-rejection
/// (someone accidentally narrowing the allow-arm).
#[test]
fn binary_quantization_accepted_at_load() {
    use qdrant_edge_ffi::config::{BinaryQuantizationParams, Memory, QuantizationConfig};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: Some(QuantizationConfig::Binary {
                    config: BinaryQuantizationParams {
                        memory: Some(Memory::Pinned),
                        encoding: None,
                        query_encoding: None,
                    },
                }),
                multivector_config: None,
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };

    let shard = EdgeShard::load(path, Some(config)).expect("Binary quantization must be accepted");

    // config() must honestly reflect the requested quantization (it reads the
    // rich edge::EdgeConfig, not the lossy plain projection that drops it).
    let read_back = shard.config().expect("config() failed");
    let quant = read_back
        .vector_data
        .get("vec")
        .and_then(|vd| vd.quantization_config.as_ref())
        .expect("Binary quantization should round-trip through config()");
    assert!(
        matches!(quant, QuantizationConfig::Binary { .. }),
        "config() should report Binary quantization, got {quant:?}"
    );
}

// ── Test 14: zero_vector_size_rejected_at_load ────────────────────────────────

/// A `size` of 0 (or above 65536) would flow into the engine and crash
/// uncatchably instead of surfacing a clean error. `EdgeConfig::validate` must
/// reject out-of-range dimensionality as `InvalidArgument` at `load`.
#[test]
fn zero_vector_size_rejected_at_load() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let mut config = make_config();
    config
        .vector_data
        .get_mut("vec")
        .expect("vec field exists")
        .size = 0;

    #[allow(clippy::err_expect)]
    let err = EdgeShard::load(path, Some(config))
        .err()
        .expect("load with size=0 should be rejected, not crash the engine");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for size=0, got {err:?}"
    );
}

// ── Test 15: oversized_search_and_query_limits_rejected ───────────────────────

/// C7 regression coverage for the request types beyond `scroll`: a huge `limit`
/// on `search` and `query` must also be rejected at the FFI boundary (each goes
/// through `bounded_limit`). Guards against a future refactor that forgets the
/// cap on one of these paths.
#[test]
fn oversized_search_and_query_limits_rejected() {
    use qdrant_edge_ffi::{QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    #[allow(clippy::err_expect)]
    let search_err = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: vec![0.1, 0.2, 0.3, 0.4],
                },
                using: Some("vec".to_string()),
            },
            limit: u64::MAX,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .err()
        .expect("search with u64::MAX limit should be rejected");
    assert!(
        matches!(search_err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for oversized search limit, got {search_err:?}"
    );

    #[allow(clippy::err_expect)]
    let query_err = shard
        .query(QueryRequest {
            prefetches: Vec::new(),
            query: Some(ScoringQuery::Vector {
                query: Query::Nearest {
                    vector: NamedVector::Dense {
                        values: vec![0.1, 0.2, 0.3, 0.4],
                    },
                    using: Some("vec".to_string()),
                },
            }),
            limit: u64::MAX,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .err()
        .expect("query with u64::MAX limit should be rejected");
    assert!(
        matches!(query_err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for oversized query limit, got {query_err:?}"
    );
}

// ── Test 16: hnsw_config_optimize_and_search ──────────────────────────────────

/// Loads a shard with an explicit HNSW config, upserts, runs `optimize()` (which
/// builds the HNSW index from the freshly-written plain segments), and confirms
/// search still returns correct results. Proves the HNSW config reaches the
/// optimizer (it would be a no-op if `index` were hardcoded to Plain) and that
/// `optimize()` works end-to-end.
#[test]
fn hnsw_config_optimize_and_search() {
    use qdrant_edge_ffi::config::{HnswIndexConfig, Memory};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                hnsw_config: Some(HnswIndexConfig {
                    m: 16,
                    ef_construct: 100,
                    full_scan_threshold: 10_000,
                    max_indexing_threads: 1,
                    memory: Some(Memory::Cached),
                    payload_m: None,
                }),
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };

    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(config)).expect("load with HNSW config failed");
    upsert_three(&shard);

    // optimize() builds the HNSW index; returns whether anything was optimized.
    // We don't assert the bool (a tiny shard may already be optimal), only that
    // it does not error and the shard stays searchable.
    shard.optimize().expect("optimize failed");

    let results = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: vec![0.1, 0.2, 0.3, 0.4],
                },
                using: Some("vec".to_string()),
            },
            limit: 3,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .expect("search after optimize failed");
    assert_eq!(
        results.len(),
        3,
        "search after optimize should return all points"
    );

    // The HNSW config round-trips through config() (it reads the rich
    // edge::EdgeConfig, not the lossy plain projection).
    let read_back = shard.config().expect("config() failed");
    let vd = read_back
        .vector_data
        .get("vec")
        .expect("config() should report the vec field");
    let hnsw = vd
        .hnsw_config
        .as_ref()
        .expect("HNSW config should round-trip through config()");
    assert_eq!(hnsw.m, 16, "round-tripped HNSW m should match what was set");
    assert_eq!(
        hnsw.ef_construct, 100,
        "round-tripped HNSW ef_construct should match what was set"
    );
}

// ── Test 17: oversized_hnsw_params_rejected_not_allocated ──────────────────────

/// HNSW params drive eager per-vector allocations / thread spawning at
/// `optimize()`; an unbounded `m` would request a multi-terabyte allocation and
/// *abort* the process (uncatchable under panic=unwind), and a huge
/// `max_indexing_threads` is a thread bomb. `validate()` must reject these at
/// `load` with a catchable `InvalidArgument` BEFORE they reach the engine —
/// same discipline as the `bounded_limit` / size guards.
///
/// Reaching the assertions at all is the proof (no abort).
#[test]
fn oversized_hnsw_params_rejected_not_allocated() {
    use qdrant_edge_ffi::config::{HnswIndexConfig, Memory};

    // Build an EdgeConfig with a given HNSW config on the single "vec" field.
    let make = |hnsw: HnswIndexConfig| EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                hnsw_config: Some(hnsw),
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };
    let sane = HnswIndexConfig {
        m: 16,
        ef_construct: 100,
        full_scan_threshold: 10_000,
        max_indexing_threads: 1,
        memory: Some(Memory::Cached),
        payload_m: None,
    };

    // Each oversized field must be rejected as InvalidArgument, not allocated.
    for (label, hnsw) in [
        (
            "huge m",
            HnswIndexConfig {
                m: u64::MAX,
                ..sane.clone()
            },
        ),
        (
            "huge payload_m",
            HnswIndexConfig {
                payload_m: Some(u64::MAX),
                ..sane.clone()
            },
        ),
        (
            "ef_construct too small",
            HnswIndexConfig {
                ef_construct: 0,
                ..sane.clone()
            },
        ),
        (
            "huge ef_construct",
            HnswIndexConfig {
                ef_construct: u64::MAX,
                ..sane.clone()
            },
        ),
        (
            "thread bomb",
            HnswIndexConfig {
                max_indexing_threads: u64::MAX,
                ..sane.clone()
            },
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir failed");
        let path = dir.path().to_string_lossy().into_owned();
        #[allow(clippy::err_expect)]
        let err = EdgeShard::load(path, Some(make(hnsw)))
            .err()
            .unwrap_or_else(|| panic!("{label}: load should be rejected, not allocated"));
        assert!(
            matches!(err, EdgeError::InvalidArgument { .. }),
            "{label}: expected InvalidArgument, got {err:?}"
        );
    }

    // A sane HNSW config must still load fine (no over-rejection).
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    assert!(
        EdgeShard::load(path, Some(make(sane))).is_ok(),
        "a sane HNSW config must still be accepted"
    );
}

// ── Test 18: scroll_pagination_via_next_offset ────────────────────────────────

/// Scroll the shard page-by-page using the `next_offset` cursor and confirm:
/// every page (except possibly the last) returns `next_offset`, feeding it back
/// yields the following page, the final page returns `next_offset == None`, and
/// the union across pages is all points with no duplicates. This pins the
/// pagination contract, which the single-page scroll tests don't exercise.
#[test]
fn scroll_pagination_via_next_offset() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    // Upsert 5 points (IDs 1..=5) so a page size of 2 yields 3 pages (2+2+1).
    let points: Vec<Point> = (1..=5u64)
        .map(|i| Point {
            id: PointId::NumId { value: i },
            vector: named_vec([i as f32, 0.0, 0.0, 0.0]),
            payload: None,
        })
        .collect();
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");

    let page_size = 2u64;
    let mut offset: Option<PointId> = None;
    let mut seen: Vec<u64> = Vec::new();
    let mut pages = 0;

    loop {
        let resp = shard
            .scroll(ScrollRequest {
                offset: offset.clone(),
                limit: Some(page_size),
                filter: None,
                with_payload: None,
                with_vector: None,
                order_by: None,
            })
            .expect("scroll failed");
        pages += 1;
        assert!(pages <= 10, "pagination did not terminate (loop guard)");

        assert!(
            resp.records.len() as u64 <= page_size,
            "a page must not exceed the requested limit"
        );
        for r in &resp.records {
            match &r.id {
                PointId::NumId { value } => seen.push(*value),
                PointId::Uuid { value } => panic!("unexpected UUID PointId: {value:?}"),
            }
        }

        match resp.next_offset {
            Some(next) => offset = Some(next),
            None => break, // last page
        }
    }

    assert_eq!(
        pages, 3,
        "5 points at page size 2 should span exactly 3 pages"
    );
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![1, 2, 3, 4, 5],
        "pagination should visit every point exactly once, got {seen:?}"
    );
}

// ── Additional shared helpers ─────────────────────────────────────────────────

/// A single-field config over "vec" (size 4) with a chosen distance metric.
fn config_with_distance(distance: Distance) -> EdgeConfig {
    EdgeConfig {
        vector_data: HashMap::from([(
            "vec".to_string(),
            VectorDataConfig {
                size: 4,
                distance,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    }
}

/// A two-field config over "vec" and "vec2" (both size 4, Dot) — used to prove
/// that deleting one named vector leaves the other intact.
fn two_field_config() -> EdgeConfig {
    let field = || VectorDataConfig {
        size: 4,
        distance: Distance::Dot,
        quantization_config: None,
        multivector_config: None,
        datatype: None,
        hnsw_config: None,
    };
    EdgeConfig {
        vector_data: HashMap::from([("vec".to_string(), field()), ("vec2".to_string(), field())]),
        sparse_vector_data: HashMap::new(),
    }
}

/// Build a `Vector::Named` populating both "vec" and "vec2".
fn two_field_vector(a: [f32; 4], b: [f32; 4]) -> Vector {
    use qdrant_edge_ffi::types::NamedVector;
    Vector::Named {
        map: HashMap::from([
            ("vec".to_string(), NamedVector::Dense { values: a.to_vec() }),
            (
                "vec2".to_string(),
                NamedVector::Dense { values: b.to_vec() },
            ),
        ]),
    }
}

/// A filter that matches points whose payload `category` equals `value`.
fn category_filter(value: &str) -> qdrant_edge_ffi::filter::Filter {
    use qdrant_edge_ffi::filter::{Condition, FieldCondition, Filter, Match, ValueVariants};
    Filter {
        must: Some(vec![Condition::Field {
            condition: FieldCondition {
                key: "category".to_string(),
                r#match: Some(Match::Value {
                    value: ValueVariants::String {
                        value: value.to_string(),
                    },
                }),
                range: None,
                datetime_range: None,
                geo_bounding_box: None,
                geo_radius: None,
                geo_polygon: None,
                values_count: None,
            },
        }]),
        should: None,
        must_not: None,
        min_should: None,
    }
}

/// Build a filter nested `depth` levels deep via `Condition::Filter`. The
/// innermost (deepest) filter is an empty match-all filter, so a legal-depth
/// tree matches every point.
fn nested_filter(depth: u32) -> qdrant_edge_ffi::filter::Filter {
    use qdrant_edge_ffi::filter::{Condition, Filter};
    let mut f = Filter {
        must: None,
        should: None,
        must_not: None,
        min_should: None,
    };
    for _ in 0..depth {
        f = Filter {
            must: Some(vec![Condition::Filter { filter: f }]),
            should: None,
            must_not: None,
            min_should: None,
        };
    }
    f
}

/// Build a `Prefetch` nested `depth` levels deep via `Prefetch.prefetches`.
fn nested_prefetch(depth: u32) -> qdrant_edge_ffi::Prefetch {
    use qdrant_edge_ffi::Prefetch;
    let mut p = Prefetch {
        limit: 1,
        query: None,
        prefetches: vec![],
        filter: None,
        score_threshold: None,
        params: None,
    };
    for _ in 0..depth {
        p = Prefetch {
            limit: 1,
            query: None,
            prefetches: vec![p],
            filter: None,
            score_threshold: None,
            params: None,
        };
    }
    p
}

/// Assert that `upsert_points` rejects `vector` eagerly (at the constructor,
/// before any shard is involved) with `InvalidArgument`.
fn assert_vector_rejected(vector: Vector) {
    #[allow(clippy::err_expect)]
    let err = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 1 },
            vector,
            payload: None,
        }],
        None,
        None,
    )
    .err()
    .expect("invalid vector must be rejected by the upsert_points constructor");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );
}

/// Parse a named dense vector out of a retrieved/searched `vector` JSON string.
/// Named vectors serialize externally-tagged as `{"<field>":{"Dense":[..]}}`.
fn dense_named_vector(vjson: &str, field: &str) -> Vec<f32> {
    let parsed: serde_json::Value = serde_json::from_str(vjson).expect("vector JSON must parse");
    let arr = parsed
        .get(field)
        .and_then(|f| f.get("Dense"))
        .cloned()
        .unwrap_or_else(|| panic!("expected a Dense vector under {field:?} in {vjson}"));
    serde_json::from_value(arr).expect("dense vector array")
}

// ── Test 19: multi_vector_invalid_matrices_rejected (MUST A) ──────────────────

/// The new multi-vector guard rejects, at the `upsert_points` constructor,
/// an empty outer matrix, ragged rows, and a zero-dimension row — via both the
/// single-field `Vector::MultiDense` and the `NamedVector::MultiDense` paths.
/// These are converted eagerly (before any shard exists), so no config is
/// needed. Guards against the release-mode panic / silent reshape that
/// `new_unchecked` would otherwise cause.
#[test]
fn multi_vector_invalid_matrices_rejected() {
    use qdrant_edge_ffi::types::NamedVector;

    // Single-field multi-vector path.
    assert_vector_rejected(Vector::MultiDense { vectors: vec![] }); // empty outer
    assert_vector_rejected(Vector::MultiDense {
        vectors: vec![vec![1.0, 2.0], vec![3.0]], // ragged
    });
    assert_vector_rejected(Vector::MultiDense {
        vectors: vec![vec![]], // zero-dim row
    });

    // Named multi-vector path (NamedVector::MultiDense inside Vector::Named).
    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "vec".to_string(),
            NamedVector::MultiDense { vectors: vec![] },
        )]),
    });
    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "vec".to_string(),
            NamedVector::MultiDense {
                vectors: vec![vec![1.0, 2.0], vec![3.0]],
            },
        )]),
    });
}

// ── Test 20: non_finite_vector_components_rejected (MUST B) ───────────────────

/// Any NaN / ±∞ component is rejected at the `upsert_points` constructor across
/// every vector shape: single dense, named dense, multi-vector row, and sparse
/// `values`. Without this guard a poisoned component would be stored and later
/// serialized back to the host as JSON `null`.
#[test]
fn non_finite_vector_components_rejected() {
    use qdrant_edge_ffi::types::{NamedVector, SparseVector};

    // Single dense — NaN and Infinity.
    assert_vector_rejected(Vector::Single {
        values: vec![f32::NAN, 0.0, 0.0, 0.0],
    });
    assert_vector_rejected(Vector::Single {
        values: vec![f32::INFINITY, 0.0, 0.0, 0.0],
    });
    // Named dense.
    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "vec".to_string(),
            NamedVector::Dense {
                values: vec![1.0, f32::NAN, 0.0, 0.0],
            },
        )]),
    });
    // Multi-vector row.
    assert_vector_rejected(Vector::MultiDense {
        vectors: vec![vec![1.0, f32::INFINITY]],
    });
    // Sparse values.
    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "sp".to_string(),
            NamedVector::Sparse {
                vector: SparseVector {
                    indices: vec![0, 1],
                    values: vec![1.0, f32::NEG_INFINITY],
                },
            },
        )]),
    });
}

// ── Test 21: finite_vectors_accepted_by_upsert_constructor (MUST B guard) ─────

/// Over-rejection guard: valid finite vectors (single, multi, named dense) must
/// still be accepted by the `upsert_points` constructor.
#[test]
fn finite_vectors_accepted_by_upsert_constructor() {
    assert!(
        UpdateOperation::upsert_points(
            vec![Point {
                id: PointId::NumId { value: 1 },
                vector: Vector::Single {
                    values: vec![0.1, 0.2, 0.3, 0.4],
                },
                payload: None,
            }],
            None,
            None
        )
        .is_ok(),
        "a finite single vector must be accepted"
    );
    assert!(
        UpdateOperation::upsert_points(
            vec![Point {
                id: PointId::NumId { value: 2 },
                vector: Vector::MultiDense {
                    vectors: vec![vec![1.0, 2.0], vec![3.0, 4.0]],
                },
                payload: None,
            }],
            None,
            None
        )
        .is_ok(),
        "a finite, uniform multi-vector must be accepted"
    );
    assert!(
        UpdateOperation::upsert_points(
            vec![Point {
                id: PointId::NumId { value: 3 },
                vector: named_vec([0.1, 0.2, 0.3, 0.4]),
                payload: None,
            }],
            None,
            None
        )
        .is_ok(),
        "a finite named dense vector must be accepted"
    );
}

// ── Test 22: deeply_nested_filter_rejected_shallow_accepted (MUST C) ──────────

/// `Condition::Filter` recursion is depth-capped at the FFI boundary
/// (MAX_QUERY_NESTING_DEPTH = 64) so a host-supplied deep tree cannot overflow
/// the stack. A depth-3 nested (all-matching) filter is accepted and counts
/// every point; a depth-65 tree is rejected as `InvalidArgument`.
#[test]
fn deeply_nested_filter_rejected_shallow_accepted() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    // Shallow (depth 3): accepted, and the all-matching filter counts all points.
    let ok = shard
        .count(CountRequest {
            filter: Some(nested_filter(3)),
            exact: true,
        })
        .expect("a depth-3 nested filter must be accepted");
    assert_eq!(
        ok, 3,
        "an all-matching depth-3 nested filter should count all 3 points"
    );

    // Deep (depth 65): rejected before it can recurse into the engine.
    #[allow(clippy::err_expect)]
    let err = shard
        .count(CountRequest {
            filter: Some(nested_filter(65)),
            exact: true,
        })
        .err()
        .expect("a depth-65 nested filter must be rejected");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for an over-deep filter, got {err:?}"
    );
}

// ── Test 23: deeply_nested_prefetch_rejected (MUST C, prefetch path) ──────────

/// The same depth cap applies to `Prefetch.prefetches` recursion via `query()`.
/// A 65-deep prefetch tree is rejected as `InvalidArgument` at conversion,
/// before reaching the engine.
#[test]
fn deeply_nested_prefetch_rejected() {
    use qdrant_edge_ffi::QueryRequest;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    #[allow(clippy::err_expect)]
    let err = shard
        .query(QueryRequest {
            prefetches: vec![nested_prefetch(65)],
            query: None,
            limit: 3,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .err()
        .expect("a depth-65 prefetch tree must be rejected");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument for an over-deep prefetch, got {err:?}"
    );
}

// ── Test 24: filter_restricts_count_scroll_and_search (MUST D, CRITICAL) ──────

/// The CRITICAL review finding: a `Filter` must actually restrict results, not
/// merely be accepted. With three points carrying distinct `category` payloads,
/// a `category == "a"` filter must return ONLY the two matching points through
/// `count`, `scroll`, and `search` alike (unindexed payload, filtered by scan).
#[test]
fn filter_restricts_count_scroll_and_search() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    // ids 1,2 → category "a"; id 3 → category "b".
    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"category":"b"}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");

    // count: exactly the two "a" points.
    let n = shard
        .count(CountRequest {
            filter: Some(category_filter("a")),
            exact: true,
        })
        .expect("count failed");
    assert_eq!(
        n, 2,
        "filter category==a must count exactly the two matching points"
    );

    // scroll: returns only ids {1, 2}.
    let resp = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: Some(category_filter("a")),
            with_payload: None,
            with_vector: None,
            order_by: None,
        })
        .expect("scroll failed");
    let mut scrolled: Vec<u64> = resp
        .records
        .iter()
        .map(|r| match &r.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID: {value:?}"),
        })
        .collect();
    scrolled.sort_unstable();
    assert_eq!(
        scrolled,
        vec![1, 2],
        "scroll with filter must return only matching ids"
    );

    // search: returns only the two matching points, even querying toward id 3.
    let hits = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: vec![0.5, 0.5, 0.5, 0.5],
                },
                using: Some("vec".to_string()),
            },
            limit: 10,
            offset: None,
            filter: Some(category_filter("a")),
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .expect("search failed");
    assert_eq!(
        hits.len(),
        2,
        "search with filter must return only the two category==a points"
    );
    for h in &hits {
        match &h.id {
            PointId::NumId { value } => assert!(
                *value == 1 || *value == 2,
                "search returned a non-matching id: {value}"
            ),
            PointId::Uuid { value } => panic!("unexpected UUID: {value:?}"),
        }
    }
}

// ── Test 25: flush_under_concurrent_upserts (SHOULD E) ────────────────────────

/// Backs the blocking-flush change: a writer thread upserts in a loop while the
/// main thread hammers `flush()`. Neither must panic, and after joining, the
/// point count must equal the number of successful upserts.
#[test]
fn flush_under_concurrent_upserts() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let n = 100u64;
    let done = Arc::new(AtomicBool::new(false));

    let writer = {
        let shard = Arc::clone(&shard);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut ok = 0u64;
            for i in 0..n {
                let op = UpdateOperation::upsert_points(
                    vec![Point {
                        id: PointId::NumId { value: i },
                        vector: named_vec([i as f32, 0.0, 0.0, 0.0]),
                        payload: None,
                    }],
                    None,
                    None,
                )
                .expect("upsert_points failed");
                if shard.update(op).is_ok() {
                    ok += 1;
                }
            }
            done.store(true, Ordering::SeqCst);
            ok
        })
    };

    // Flush concurrently for the whole duration the writer runs.
    while !done.load(Ordering::SeqCst) {
        let _ = shard.flush();
    }

    let ok = writer.join().expect("writer thread panicked");
    shard.flush().expect("final flush failed");

    let count = shard
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(
        ok, n,
        "all upserts should have succeeded (shard stayed open)"
    );
    assert_eq!(
        count, ok,
        "point count must equal the number of successful upserts"
    );
}

// ── Test 26: vector_content_round_trips (SHOULD F) ────────────────────────────

/// Vector *content* (not just presence) must round-trip: upsert a point, then
/// read the exact floats back via both `retrieve` and `search` with vectors
/// enabled. f32 → JSON → f32 is exact, so equality is asserted directly.
#[test]
fn vector_content_round_trips_through_retrieve_and_search() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let input = [0.1f32, 0.2, 0.3, 0.4];
    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 7 },
                    vector: named_vec(input),
                    payload: None,
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");

    // retrieve with vectors.
    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 7 }],
            with_payload: None,
            with_vector: Some(WithVector::Bool { enable: true }),
        })
        .expect("retrieve failed");
    assert_eq!(recs.len(), 1, "expected the one point back");
    let vjson = recs[0].vector.as_deref().expect("vector should be present");
    assert_eq!(
        dense_named_vector(vjson, "vec"),
        input.to_vec(),
        "retrieved vector must round-trip exactly, got {vjson}"
    );

    // search with vectors.
    let hits = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: input.to_vec(),
                },
                using: Some("vec".to_string()),
            },
            limit: 1,
            offset: None,
            filter: None,
            params: None,
            with_vector: Some(WithVector::Bool { enable: true }),
            with_payload: None,
            score_threshold: None,
        })
        .expect("search failed");
    assert_eq!(hits.len(), 1, "expected the one hit");
    let vjson = hits[0]
        .vector
        .as_deref()
        .expect("vector should be present on hit");
    assert_eq!(
        dense_named_vector(vjson, "vec"),
        input.to_vec(),
        "searched vector must round-trip exactly, got {vjson}"
    );
}

// ── Test 27: cosine_distance_ranks_by_direction (SHOULD G) ────────────────────

/// A `Cosine` shard loads and ranks by angular similarity (magnitude ignored):
/// query [1,0,0,0] against [1,0,0,0], [1,1,0,0], [0,1,0,0] ranks [1, 3, 2] with
/// descending scores and a top score of ~1.0.
#[test]
fn cosine_distance_ranks_by_direction() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(config_with_distance(Distance::Cosine))).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([1.0, 0.0, 0.0, 0.0]),
            payload: None,
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.0, 1.0, 0.0, 0.0]),
            payload: None,
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([1.0, 1.0, 0.0, 0.0]),
            payload: None,
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");

    let hits = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: NamedVector::Dense {
                    values: vec![1.0, 0.0, 0.0, 0.0],
                },
                using: Some("vec".to_string()),
            },
            limit: 3,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .expect("search failed");

    let ids: Vec<u64> = hits
        .iter()
        .map(|h| match &h.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID: {value:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 3, 2],
        "cosine ranking should order by direction: exact, 45°, orthogonal"
    );
    assert!(
        hits[0].score >= hits[1].score && hits[1].score >= hits[2].score,
        "cosine scores must be descending, got {:?}",
        hits.iter().map(|h| h.score).collect::<Vec<_>>()
    );
    assert!(
        (hits[0].score - 1.0).abs() < 1e-5,
        "cosine top score (exact match) should be ~1.0, got {}",
        hits[0].score
    );
}

// ── Test 28: euclid_and_manhattan_rank_nearest_first (SHOULD G) ───────────────

/// `Euclid` and `Manhattan` shards load and rank nearest-first: for query
/// [1,0,0,0] the ranking of [1,0,0,0], [0,1,0,0], [1,1,0,0] is [1, 3, 2].
#[test]
fn euclid_and_manhattan_rank_nearest_first() {
    for distance in [Distance::Euclid, Distance::Manhattan] {
        let dir = tempfile::tempdir().expect("tempdir failed");
        let path = dir.path().to_string_lossy().into_owned();
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path, Some(config_with_distance(distance))).expect("load failed");

        let points = vec![
            Point {
                id: PointId::NumId { value: 1 },
                vector: named_vec([1.0, 0.0, 0.0, 0.0]),
                payload: None,
            },
            Point {
                id: PointId::NumId { value: 2 },
                vector: named_vec([0.0, 1.0, 0.0, 0.0]),
                payload: None,
            },
            Point {
                id: PointId::NumId { value: 3 },
                vector: named_vec([1.0, 1.0, 0.0, 0.0]),
                payload: None,
            },
        ];
        shard
            .update(
                UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"),
            )
            .expect("update failed");

        let hits = shard
            .search(SearchRequest {
                query: Query::Nearest {
                    vector: NamedVector::Dense {
                        values: vec![1.0, 0.0, 0.0, 0.0],
                    },
                    using: Some("vec".to_string()),
                },
                limit: 3,
                offset: None,
                filter: None,
                params: None,
                with_vector: None,
                with_payload: None,
                score_threshold: None,
            })
            .expect("search failed");

        let ids: Vec<u64> = hits
            .iter()
            .map(|h| match &h.id {
                PointId::NumId { value } => *value,
                PointId::Uuid { value } => panic!("unexpected UUID: {value:?}"),
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 3, 2],
            "{distance:?}: nearest-first ranking should be [1, 3, 2]"
        );
    }
}

// ── Test 29: delete_points_by_filter_persists (SHOULD H) ──────────────────────

/// `delete_points_by_filter` must remove matching points durably: delete every
/// `category == "a"` point, flush, reload, and confirm only the "b" point
/// survives.
#[test]
fn delete_points_by_filter_persists() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();

    {
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load failed");
        let points = vec![
            Point {
                id: PointId::NumId { value: 1 },
                vector: named_vec([0.1, 0.2, 0.3, 0.4]),
                payload: Some(r#"{"category":"a"}"#.to_string()),
            },
            Point {
                id: PointId::NumId { value: 2 },
                vector: named_vec([0.2, 0.1, 0.4, 0.3]),
                payload: Some(r#"{"category":"a"}"#.to_string()),
            },
            Point {
                id: PointId::NumId { value: 3 },
                vector: named_vec([0.5, 0.5, 0.5, 0.5]),
                payload: Some(r#"{"category":"b"}"#.to_string()),
            },
        ];
        shard
            .update(
                UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"),
            )
            .expect("update failed");
        shard
            .update(
                UpdateOperation::delete_points_by_filter(category_filter("a"))
                    .expect("delete_points_by_filter failed"),
            )
            .expect("delete update failed");
        shard.flush().expect("flush failed");
    }

    let reloaded: Arc<EdgeShard> = EdgeShard::load(path, None).expect("reload failed");
    let count = reloaded
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(
        count, 1,
        "only the non-matching point should survive delete-by-filter"
    );
    let survivor = reloaded
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 3 }],
            with_payload: None,
            with_vector: None,
        })
        .expect("retrieve failed");
    assert_eq!(
        survivor.len(),
        1,
        "the category==b point (id 3) must remain"
    );
    let removed = reloaded
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }, PointId::NumId { value: 2 }],
            with_payload: None,
            with_vector: None,
        })
        .expect("retrieve failed");
    assert!(
        removed.is_empty(),
        "category==a points must be gone after reload"
    );
}

// ── Test 30: update_vectors_replaces_stored_vector (SHOULD H) ─────────────────

/// `update_vectors` must replace the stored vector of an existing point; the
/// new content must be visible on retrieve after a flush.
#[test]
fn update_vectors_replaces_stored_vector() {
    use qdrant_edge_ffi::types::PointVectors;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: named_vec([0.1, 0.2, 0.3, 0.4]),
                    payload: None,
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");

    let replacement = [9.0f32, 8.0, 7.0, 6.0];
    shard
        .update(
            UpdateOperation::update_vectors(
                vec![PointVectors {
                    id: PointId::NumId { value: 1 },
                    vector: named_vec(replacement),
                }],
                None,
            )
            .expect("update_vectors failed"),
        )
        .expect("update_vectors update failed");
    shard.flush().expect("flush failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: None,
            with_vector: Some(WithVector::Bool { enable: true }),
        })
        .expect("retrieve failed");
    let vjson = recs[0].vector.as_deref().expect("vector should be present");
    assert_eq!(
        dense_named_vector(vjson, "vec"),
        replacement.to_vec(),
        "update_vectors must replace the stored vector, got {vjson}"
    );
}

// ── Test 31: delete_vectors_removes_named_field_only (SHOULD H) ───────────────

/// `delete_vectors` must remove only the named vector field, leaving the point
/// and its other vector field intact.
#[test]
fn delete_vectors_removes_named_field_only() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(two_field_config())).expect("load failed");

    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: two_field_vector([0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]),
                    payload: None,
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");

    shard
        .update(
            UpdateOperation::delete_vectors(
                vec![PointId::NumId { value: 1 }],
                vec!["vec2".to_string()],
            )
            .expect("delete_vectors failed"),
        )
        .expect("delete_vectors update failed");
    shard.flush().expect("flush failed");

    let count = shard
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(
        count, 1,
        "the point must remain after deleting one of its vectors"
    );

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: None,
            with_vector: Some(WithVector::Bool { enable: true }),
        })
        .expect("retrieve failed");
    let vjson = recs[0].vector.as_deref().expect("vector should be present");
    let parsed: serde_json::Value = serde_json::from_str(vjson).expect("vector JSON must parse");
    assert!(
        parsed.get("vec").is_some(),
        "the kept vector field must still be present: {vjson}"
    );
    assert!(
        parsed.get("vec2").is_none(),
        "the deleted vector field must be gone: {vjson}"
    );
}

// ── Test 32: delete_payload_removes_only_named_keys (SHOULD H) ────────────────

/// `delete_payload` must remove only the listed keys and leave the rest.
#[test]
fn delete_payload_removes_only_named_keys() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: named_vec([0.1, 0.2, 0.3, 0.4]),
                    payload: Some(r#"{"a":1,"b":2}"#.to_string()),
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");

    shard
        .update(
            UpdateOperation::delete_payload(
                vec![PointId::NumId { value: 1 }],
                vec!["a".to_string()],
            )
            .expect("delete_payload failed"),
        )
        .expect("delete_payload update failed");
    shard.flush().expect("flush failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");
    let pjson = recs[0]
        .payload
        .as_deref()
        .expect("payload should be present");
    let parsed: serde_json::Value = serde_json::from_str(pjson).expect("payload JSON must parse");
    assert!(
        parsed.get("a").is_none(),
        "deleted key `a` must be gone: {pjson}"
    );
    assert_eq!(
        parsed.get("b").and_then(|v| v.as_i64()),
        Some(2),
        "untouched key `b` must remain: {pjson}"
    );
}

// ── Test 33: clear_payload_removes_all_keys (SHOULD H) ────────────────────────

/// `clear_payload` must remove the entire payload while keeping the point.
#[test]
fn clear_payload_removes_all_keys() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: named_vec([0.1, 0.2, 0.3, 0.4]),
                    payload: Some(r#"{"a":1,"b":2}"#.to_string()),
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");

    shard
        .update(
            UpdateOperation::clear_payload(vec![PointId::NumId { value: 1 }])
                .expect("clear_payload failed"),
        )
        .expect("clear_payload update failed");
    shard.flush().expect("flush failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");
    assert_eq!(
        recs.len(),
        1,
        "the point itself must remain after clear_payload"
    );
    // A cleared payload is either omitted (None) or an empty object.
    if let Some(pjson) = recs[0].payload.as_deref() {
        let parsed: serde_json::Value =
            serde_json::from_str(pjson).expect("payload JSON must parse");
        assert!(
            parsed.as_object().map(|o| o.is_empty()).unwrap_or(false),
            "cleared payload must have no keys, got {pjson}"
        );
    }
    let count = shard
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(count, 1, "clear_payload must not remove the point");
}

// ── Test 34: multivector_round_trips (NICE I) ─────────────────────────────────

/// A multi-vector field is exposable via `multivector_config`: upsert a
/// ColBERT-style matrix and read it back. Named multi-vectors serialize
/// externally-tagged as `{"mv":{"MultiDense":{"flattened_vectors":[..],"dim":N}}}`.
#[test]
fn multivector_round_trips() {
    use qdrant_edge_ffi::config::{MultiVectorComparator, MultiVectorConfig};
    use qdrant_edge_ffi::types::NamedVector;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let config = EdgeConfig {
        vector_data: HashMap::from([(
            "mv".to_string(),
            VectorDataConfig {
                size: 2,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: Some(MultiVectorConfig {
                    comparator: MultiVectorComparator::MaxSim,
                }),
                datatype: None,
                hnsw_config: None,
            },
        )]),
        sparse_vector_data: HashMap::new(),
    };
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(config)).expect("load with multivector config failed");

    let rows = vec![vec![1.0f32, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]];
    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: Vector::Named {
                        map: HashMap::from([(
                            "mv".to_string(),
                            NamedVector::MultiDense {
                                vectors: rows.clone(),
                            },
                        )]),
                    },
                    payload: None,
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");
    shard.flush().expect("flush failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: None,
            with_vector: Some(WithVector::Bool { enable: true }),
        })
        .expect("retrieve failed");
    let vjson = recs[0].vector.as_deref().expect("vector should be present");
    let parsed: serde_json::Value = serde_json::from_str(vjson).expect("vector JSON must parse");
    let multi = parsed
        .get("mv")
        .and_then(|f| f.get("MultiDense"))
        .unwrap_or_else(|| panic!("expected MultiDense under `mv` in {vjson}"));
    let flattened: Vec<f32> = serde_json::from_value(
        multi
            .get("flattened_vectors")
            .expect("flattened_vectors")
            .clone(),
    )
    .expect("flattened vectors");
    let dim = multi.get("dim").and_then(|d| d.as_u64()).expect("dim");
    assert_eq!(dim, 2, "each multi-vector row is 2-dimensional");
    assert_eq!(
        flattened,
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "multi-vector content must round-trip exactly, got {vjson}"
    );
}

// ── Test 35: rrf_fusion_over_prefetches (NICE K) ──────────────────────────────

/// A multi-stage `query()` with two vector prefetches fused by `Fusion::Rrf`
/// must run end-to-end and produce a fused ranked set containing every point
/// exactly once, in descending score order.
#[test]
fn rrf_fusion_over_prefetches_returns_fused_set() {
    use qdrant_edge_ffi::{Fusion, Prefetch, QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let branch = |vector: Vec<f32>| Prefetch {
        limit: 3,
        query: Some(ScoringQuery::Vector {
            query: Query::Nearest {
                vector: NamedVector::Dense { values: vector },
                using: Some("vec".to_string()),
            },
        }),
        prefetches: vec![],
        filter: None,
        score_threshold: None,
        params: None,
    };

    let hits = shard
        .query(QueryRequest {
            prefetches: vec![
                branch(vec![0.1, 0.2, 0.3, 0.4]),
                branch(vec![0.4, 0.3, 0.2, 0.1]),
            ],
            query: Some(ScoringQuery::Fusion {
                fusion: Fusion::Rrf {
                    k: 60,
                    weights: None,
                },
            }),
            limit: 3,
            offset: None,
            filter: None,
            params: None,
            with_vector: None,
            with_payload: None,
            score_threshold: None,
        })
        .expect("RRF fusion query failed");

    assert_eq!(
        hits.len(),
        3,
        "RRF over two prefetches should fuse to all three points"
    );
    let mut ids: Vec<u64> = hits
        .iter()
        .map(|h| match &h.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID: {value:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "fused result set should contain each point exactly once"
    );
    assert!(
        hits[0].score >= hits[1].score && hits[1].score >= hits[2].score,
        "fused scores must be in descending order, got {:?}",
        hits.iter().map(|h| h.score).collect::<Vec<_>>()
    );
}

// ── Test 36: sparse_vector_round_trips (NICE I) ───────────────────────────────

/// A sparse vector field is exposable via `sparse_vector_data`: upsert a sparse
/// vector and read it back. Named sparse vectors serialize externally-tagged as
/// `{"sp":{"Sparse":{"indices":[..],"values":[..]}}}`.
#[test]
fn sparse_vector_round_trips() {
    use qdrant_edge_ffi::config::SparseVectorDataConfig;
    use qdrant_edge_ffi::types::{NamedVector, SparseVector};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let config = EdgeConfig {
        vector_data: HashMap::new(),
        sparse_vector_data: HashMap::from([(
            "sp".to_string(),
            SparseVectorDataConfig {
                full_scan_threshold: None,
                datatype: None,
                modifier: None,
            },
        )]),
    };
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(config)).expect("load with sparse config failed");

    let indices = vec![1u32, 5, 9];
    let values = vec![0.5f32, 1.5, 2.5];
    shard
        .update(
            UpdateOperation::upsert_points(
                vec![Point {
                    id: PointId::NumId { value: 1 },
                    vector: Vector::Named {
                        map: HashMap::from([(
                            "sp".to_string(),
                            NamedVector::Sparse {
                                vector: SparseVector {
                                    indices: indices.clone(),
                                    values: values.clone(),
                                },
                            },
                        )]),
                    },
                    payload: None,
                }],
                None,
                None,
            )
            .expect("upsert_points failed"),
        )
        .expect("update failed");
    shard.flush().expect("flush failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: None,
            with_vector: Some(WithVector::Bool { enable: true }),
        })
        .expect("retrieve failed");
    let vjson = recs[0].vector.as_deref().expect("vector should be present");
    let parsed: serde_json::Value = serde_json::from_str(vjson).expect("vector JSON must parse");
    let sparse = parsed
        .get("sp")
        .and_then(|f| f.get("Sparse"))
        .unwrap_or_else(|| panic!("expected Sparse under `sp` in {vjson}"));
    let got_indices: Vec<u32> =
        serde_json::from_value(sparse.get("indices").expect("indices").clone())
            .expect("sparse indices");
    let got_values: Vec<f32> =
        serde_json::from_value(sparse.get("values").expect("values").clone())
            .expect("sparse values");
    assert_eq!(
        got_indices, indices,
        "sparse indices must round-trip, got {vjson}"
    );
    assert_eq!(
        got_values, values,
        "sparse values must round-trip, got {vjson}"
    );
}

// ── New-surface tests: full update-operation and scoring-query coverage ───────

/// `create_field_index` makes `facet` usable on an FFI-only shard — previously
/// impossible, since faceting requires a payload index and the FFI had no way
/// to build one.
#[test]
fn create_field_index_enables_facet() {
    use qdrant_edge_ffi::{FacetRequest, PayloadSchemaType};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.4, 0.3, 0.2, 0.1]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"category":"b"}"#.to_string()),
        },
    ];
    let op = UpdateOperation::upsert_points(points, None, None).expect("upsert failed");
    shard.update(op).expect("update failed");

    let op =
        UpdateOperation::create_field_index("category".to_string(), PayloadSchemaType::Keyword)
            .expect("create_field_index failed");
    shard.update(op).expect("index creation failed");

    let facets = shard
        .facet(FacetRequest {
            key: "category".to_string(),
            limit: 10,
            exact: true,
            filter: None,
        })
        .expect("facet over the freshly indexed field must work");
    let mut counts: Vec<(String, u64)> = facets
        .hits
        .into_iter()
        .map(|h| (h.value, h.count))
        .collect();
    counts.sort();
    assert_eq!(counts, vec![("a".to_string(), 2), ("b".to_string(), 1)]);

    // And drop it again — deleting the index must succeed.
    let op = UpdateOperation::delete_field_index("category".to_string())
        .expect("delete_field_index failed");
    shard.update(op).expect("index deletion failed");
}

/// `update_mode: InsertOnly` must not overwrite an existing point.
#[test]
fn conditional_upsert_insert_only_preserves_existing() {
    use qdrant_edge_ffi::UpdateMode;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let op = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.9, 0.9, 0.9, 0.9]),
            payload: Some(r#"{"title":"overwritten"}"#.to_string()),
        }],
        None,
        Some(UpdateMode::InsertOnly),
    )
    .expect("conditional upsert failed");
    shard.update(op).expect("update failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");
    let payload = recs[0].payload.as_deref().expect("payload expected");
    assert!(
        payload.contains("point one"),
        "InsertOnly must not overwrite the existing point, got {payload}"
    );
}

/// `overwrite_payload` replaces the payload wholesale; `set_payload` merges.
#[test]
fn overwrite_payload_replaces_wholesale() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let op = UpdateOperation::overwrite_payload(
        vec![PointId::NumId { value: 1 }],
        r#"{"only":"this"}"#.to_string(),
    )
    .expect("overwrite_payload failed");
    shard.update(op).expect("update failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");
    let payload = recs[0].payload.as_deref().expect("payload expected");
    assert!(
        payload.contains("only"),
        "new key must be present: {payload}"
    );
    assert!(
        !payload.contains("title"),
        "overwrite must drop the old keys, got {payload}"
    );
}

/// Filter-addressed payload ops apply to matching points only.
#[test]
fn clear_payload_by_filter_applies_to_matches() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    // Match "point one" by full-text-less means: HasId is simplest here.
    let filter = qdrant_edge_ffi::filter::Filter {
        must: Some(vec![qdrant_edge_ffi::filter::Condition::HasId {
            ids: vec![PointId::NumId { value: 1 }],
        }]),
        should: None,
        must_not: None,
        min_should: None,
    };
    let op = UpdateOperation::clear_payload_by_filter(filter).expect("clear failed");
    shard.update(op).expect("update failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }, PointId::NumId { value: 2 }],
            with_payload: Some(WithPayload::Bool { enable: true }),
            with_vector: None,
        })
        .expect("retrieve failed");
    let p1 = recs
        .iter()
        .find(|r| matches!(r.id, PointId::NumId { value: 1 }))
        .unwrap();
    let p2 = recs
        .iter()
        .find(|r| matches!(r.id, PointId::NumId { value: 2 }))
        .unwrap();
    assert_eq!(
        p1.payload.as_deref(),
        Some("{}"),
        "payload of id 1 must be cleared"
    );
    assert!(
        p2.payload.as_deref().unwrap_or("").contains("point two"),
        "payload of id 2 must be untouched"
    );
}

/// Recommend / Discover / Context / MMR queries run end-to-end.
#[test]
fn advanced_vector_queries_return_results() {
    use qdrant_edge_ffi::{ContextPair, QueryRequest, RecommendStrategy, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let dense = |values: [f32; 4]| NamedVector::Dense {
        values: values.to_vec(),
    };
    let using = Some("vec".to_string());

    let queries = vec![
        Query::Recommend {
            positives: vec![dense([0.1, 0.2, 0.3, 0.4])],
            negatives: vec![dense([0.5, 0.5, 0.5, 0.5])],
            strategy: Some(RecommendStrategy::BestScore),
            using: using.clone(),
        },
        Query::Recommend {
            positives: vec![dense([0.1, 0.2, 0.3, 0.4])],
            negatives: vec![],
            strategy: Some(RecommendStrategy::SumScores),
            using: using.clone(),
        },
        Query::Discover {
            target: dense([0.1, 0.2, 0.3, 0.4]),
            context: vec![ContextPair {
                positive: dense([0.1, 0.2, 0.3, 0.4]),
                negative: dense([0.5, 0.5, 0.5, 0.5]),
            }],
            using: using.clone(),
        },
        Query::Context {
            context: vec![ContextPair {
                positive: dense([0.1, 0.2, 0.3, 0.4]),
                negative: dense([0.5, 0.5, 0.5, 0.5]),
            }],
            using: using.clone(),
        },
    ];
    for query in queries {
        let hits = shard
            .query(QueryRequest {
                limit: 3,
                offset: None,
                query: Some(ScoringQuery::Vector { query }),
                prefetches: vec![],
                with_vector: None,
                with_payload: None,
                filter: None,
                score_threshold: None,
                params: None,
            })
            .expect("advanced query failed");
        assert!(!hits.is_empty(), "advanced query returned no hits");
    }

    // MMR is a ScoringQuery of its own.
    let hits = shard
        .query(QueryRequest {
            limit: 3,
            offset: None,
            query: Some(ScoringQuery::Mmr {
                vector: dense([0.1, 0.2, 0.3, 0.4]),
                using,
                lambda: 0.5,
                candidates_limit: 10,
            }),
            prefetches: vec![],
            with_vector: None,
            with_payload: None,
            filter: None,
            score_threshold: None,
            params: None,
        })
        .expect("mmr query failed");
    assert!(!hits.is_empty(), "mmr query returned no hits");

    // A recommend query without positives must be rejected at the boundary.
    let err = shard
        .query(QueryRequest {
            limit: 3,
            offset: None,
            query: Some(ScoringQuery::Vector {
                query: Query::Recommend {
                    positives: vec![],
                    negatives: vec![],
                    strategy: None,
                    using: Some("vec".to_string()),
                },
            }),
            prefetches: vec![],
            with_vector: None,
            with_payload: None,
            filter: None,
            score_threshold: None,
            params: None,
        })
        .expect_err("recommend without positives must fail");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument, got {err:?}"
    );
}

/// Formula re-scoring over a prefetch: `$score + popularity`.
#[test]
fn formula_rescoring_boosts_by_payload() {
    use qdrant_edge_ffi::{Expression, Prefetch, QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.1, 0.1, 0.1]),
            payload: Some(r#"{"popularity":0.0}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            // Slightly less similar, but hugely popular — the formula must
            // push it to the top.
            vector: named_vec([0.09, 0.09, 0.09, 0.09]),
            payload: Some(r#"{"popularity":100.0}"#.to_string()),
        },
    ];
    let op = UpdateOperation::upsert_points(points, None, None).expect("upsert failed");
    shard.update(op).expect("update failed");

    let expression = Expression::sum(vec![
        Expression::variable("$score".to_string()),
        Expression::variable("popularity".to_string()),
    ])
    .expect("expression build failed");

    let hits = shard
        .query(QueryRequest {
            limit: 2,
            offset: None,
            query: Some(ScoringQuery::Formula {
                expression,
                defaults: HashMap::from([("popularity".to_string(), "0.0".to_string())]),
            }),
            prefetches: vec![Prefetch {
                limit: 10,
                query: Some(ScoringQuery::Vector {
                    query: Query::Nearest {
                        vector: NamedVector::Dense {
                            values: vec![0.1, 0.1, 0.1, 0.1],
                        },
                        using: Some("vec".to_string()),
                    },
                }),
                prefetches: vec![],
                filter: None,
                score_threshold: None,
                params: None,
            }],
            with_vector: None,
            with_payload: None,
            filter: None,
            score_threshold: None,
            params: None,
        })
        .expect("formula query failed");
    assert!(
        matches!(hits[0].id, PointId::NumId { value: 2 }),
        "the popular point must rank first after formula boost"
    );
}

/// Grouped query: one group per category, best hit each.
#[test]
fn query_groups_returns_one_group_per_key() {
    use qdrant_edge_ffi::{GroupRequest, QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.4, 0.3, 0.2, 0.1]),
            payload: Some(r#"{"category":"a"}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"category":"b"}"#.to_string()),
        },
    ];
    let op = UpdateOperation::upsert_points(points, None, None).expect("upsert failed");
    shard.update(op).expect("update failed");

    let groups = shard
        .query_groups(GroupRequest {
            query: QueryRequest {
                limit: 10,
                offset: None,
                query: Some(ScoringQuery::Vector {
                    query: Query::Nearest {
                        vector: NamedVector::Dense {
                            values: vec![0.1, 0.2, 0.3, 0.4],
                        },
                        using: Some("vec".to_string()),
                    },
                }),
                prefetches: vec![],
                with_vector: None,
                with_payload: None,
                filter: None,
                score_threshold: None,
                params: None,
            },
            group_by: "category".to_string(),
            groups: 10,
            group_size: 1,
        })
        .expect("query_groups failed");
    assert_eq!(groups.len(), 2, "expected one group per category");
    for group in &groups {
        assert_eq!(group.hits.len(), 1, "group_size=1 must cap hits per group");
    }
}

/// Vector-name ops: add a dense field, write to it, then drop it.
#[test]
fn create_and_delete_named_vector_field() {
    use qdrant_edge_ffi::config::Distance;
    use qdrant_edge_ffi::types::NamedVector;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let op =
        UpdateOperation::create_dense_vector("extra".to_string(), 4, Distance::Dot, None, None)
            .expect("create_dense_vector failed");
    shard.update(op).expect("adding a vector field failed");

    // The new field is writable.
    let op = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 9 },
            vector: Vector::Named {
                map: HashMap::from([
                    (
                        "vec".to_string(),
                        NamedVector::Dense {
                            values: vec![0.1, 0.1, 0.1, 0.1],
                        },
                    ),
                    (
                        "extra".to_string(),
                        NamedVector::Dense {
                            values: vec![0.2, 0.2, 0.2, 0.2],
                        },
                    ),
                ]),
            },
            payload: None,
        }],
        None,
        None,
    )
    .expect("upsert failed");
    shard.update(op).expect("writing the new field failed");

    // Size bound is enforced at the boundary.
    // `.err().expect()` rather than `.expect_err()`: the Ok type involves a
    // non-`Debug` UniFFI object, so `expect_err` does not compile.
    #[allow(clippy::err_expect)]
    let err = UpdateOperation::create_dense_vector("bad".to_string(), 0, Distance::Dot, None, None)
        .err()
        .expect("size 0 must be rejected");
    assert!(matches!(err, EdgeError::InvalidArgument { .. }));

    // And the field can be dropped again.
    let op = UpdateOperation::delete_vector_name("extra".to_string());
    shard.update(op).expect("deleting the vector field failed");
}

/// Re-creating an existing vector name with **different** params must be
/// rejected loudly instead of silently no-op'ing storage while overwriting the
/// advertised config (a config/storage desync). An **identical** re-create is
/// an idempotent no-op.
#[test]
fn create_vector_name_conflict_is_rejected() {
    use qdrant_edge_ffi::config::Distance;
    use qdrant_edge_ffi::types::NamedVector;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let create = |size: u64, distance: Distance| {
        UpdateOperation::create_dense_vector("extra".to_string(), size, distance, None, None)
            .expect("create_dense_vector op build failed")
    };

    shard
        .update(create(4, Distance::Dot))
        .expect("initial create failed");
    // Identical re-create: idempotent, not an error.
    shard
        .update(create(4, Distance::Dot))
        .expect("identical re-create must be idempotent");

    // Conflicting re-create: rejected, so config() cannot drift from storage.
    let err = shard
        .update(create(8, Distance::Cosine))
        .expect_err("a conflicting re-create must be rejected");
    assert!(
        format!("{err:?}").contains("already exists"),
        "expected an 'already exists' rejection, got {err:?}"
    );

    // Storage still holds the original 4-dim field: a 4-dim write succeeds.
    let op = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 9 },
            vector: Vector::Named {
                map: HashMap::from([
                    (
                        "vec".to_string(),
                        NamedVector::Dense {
                            values: vec![0.1, 0.1, 0.1, 0.1],
                        },
                    ),
                    (
                        "extra".to_string(),
                        NamedVector::Dense {
                            values: vec![0.2, 0.2, 0.2, 0.2],
                        },
                    ),
                ]),
            },
            payload: None,
        }],
        None,
        None,
    )
    .expect("upsert build failed");
    shard
        .update(op)
        .expect("a 4-dim write into the original 'extra' field must still work");
}

/// An identical re-create of a *construction-defined* vector (declared in the
/// initial `EdgeConfig`, and so stored with `on_disk: Some(false)` while the op
/// leaves it `None`) must be an idempotent no-op, not a false conflict — the
/// create op only defines identity fields, so storage/tuning differences must
/// not be compared.
#[test]
fn create_vector_name_identical_recreate_of_config_vector_is_idempotent() {
    use qdrant_edge_ffi::config::Distance;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    // `make_config` declares "vec" as size 4 / Dot.
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    // Identical identity (size 4 / Dot) → idempotent Ok, despite the stored
    // vector carrying on_disk: Some(false) that the op leaves None.
    let op = UpdateOperation::create_dense_vector("vec".to_string(), 4, Distance::Dot, None, None)
        .expect("create_dense_vector op build failed");
    shard
        .update(op)
        .expect("identical re-create of a config-defined vector must be idempotent");

    // A genuinely conflicting re-create of the same config vector is still rejected.
    let bad =
        UpdateOperation::create_dense_vector("vec".to_string(), 8, Distance::Cosine, None, None)
            .expect("create_dense_vector op build failed");
    let err = shard
        .update(bad)
        .expect_err("a conflicting re-create of a config-defined vector must be rejected");
    assert!(
        format!("{err:?}").contains("already exists"),
        "expected an 'already exists' rejection, got {err:?}"
    );
}

/// A non-finite `StartFrom::Float` must be rejected at the boundary as a clean
/// `InvalidArgument`, never reaching the engine (where NaN panics on a
/// float-indexed field and silently truncates the scan on an integer one).
#[test]
fn order_by_start_from_non_finite_is_rejected() {
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType, StartFrom};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);
    shard
        .update(
            UpdateOperation::create_field_index("rank".to_string(), PayloadSchemaType::Integer)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let result = shard.scroll(ScrollRequest {
        offset: None,
        limit: Some(10),
        filter: None,
        with_payload: Some(WithPayload::Bool { enable: false }),
        with_vector: None,
        order_by: Some(OrderBy {
            key: "rank".to_string(),
            direction: Some(Direction::Asc),
            start_from: Some(StartFrom::Float { value: f64::NAN }),
        }),
    });
    assert!(
        matches!(result, Err(EdgeError::InvalidArgument { .. })),
        "NaN start_from must be a clean InvalidArgument, got {result:?}"
    );
}

/// `decay` and `div` must reject non-finite tuning params at construction (like
/// `constant`): a NaN evades the engine's comparison-based range checks and
/// produces a NaN score — a debug-build panic across the FFI boundary, or a
/// silent all-zero rescore in release.
#[test]
fn formula_decay_and_div_reject_non_finite_params() {
    use qdrant_edge_ffi::{DecayKind, Expression};

    let x = Expression::variable("$score".to_string());

    let midpoint_err = Expression::decay(DecayKind::Lin, x.clone(), None, Some(f32::NAN), None)
        .expect_err("NaN decay midpoint must be rejected");
    assert!(matches!(midpoint_err, EdgeError::InvalidArgument { .. }));

    let scale_err = Expression::decay(DecayKind::Lin, x.clone(), None, None, Some(f32::NAN))
        .expect_err("NaN decay scale must be rejected");
    assert!(matches!(scale_err, EdgeError::InvalidArgument { .. }));

    let div_err = Expression::div(x.clone(), x.clone(), Some(f32::NAN))
        .expect_err("NaN div by_zero_default must be rejected");
    assert!(matches!(div_err, EdgeError::InvalidArgument { .. }));
}

/// The lifecycle additions: `path`, `snapshot_manifest`, and the config
/// setters are callable and validated.
#[test]
fn lifecycle_additions_work() {
    use qdrant_edge_ffi::OptimizersConfig;
    use qdrant_edge_ffi::config::{HnswIndexConfig, Memory};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path_string = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> =
        EdgeShard::load(path_string.clone(), Some(make_config())).expect("load failed");

    assert_eq!(shard.path().expect("path failed"), path_string);

    let manifest = shard.snapshot_manifest().expect("snapshot_manifest failed");
    serde_json::from_str::<serde_json::Value>(&manifest)
        .expect("snapshot manifest must be valid JSON");

    shard
        .set_hnsw_config(HnswIndexConfig {
            m: 16,
            ef_construct: 100,
            full_scan_threshold: 10_000,
            max_indexing_threads: 1,
            memory: Some(Memory::Cached),
            payload_m: None,
        })
        .expect("set_hnsw_config failed");

    // Out-of-range HNSW params are rejected, not applied.
    let err = shard
        .set_hnsw_config(HnswIndexConfig {
            m: u64::MAX,
            ef_construct: 100,
            full_scan_threshold: 10_000,
            max_indexing_threads: 1,
            memory: None,
            payload_m: None,
        })
        .expect_err("oversized m must be rejected");
    assert!(matches!(err, EdgeError::InvalidArgument { .. }));

    shard
        .set_vector_hnsw_config(
            "vec".to_string(),
            HnswIndexConfig {
                m: 8,
                ef_construct: 64,
                full_scan_threshold: 10_000,
                max_indexing_threads: 1,
                memory: None,
                payload_m: None,
            },
        )
        .expect("set_vector_hnsw_config failed");

    shard
        .set_optimizers_config(OptimizersConfig {
            deleted_threshold: Some(0.5),
            vacuum_min_vector_number: Some(100),
            default_segment_number: Some(1),
            max_segment_size_kb: None,
            indexing_threshold_kb: Some(1000),
            prevent_unoptimized: None,
        })
        .expect("set_optimizers_config failed");

    // `create` on an occupied path must fail; on a fresh path it must work.
    // `.err().expect()`: see note above — the Ok type is a non-`Debug`
    // UniFFI object.
    #[allow(clippy::err_expect)]
    let err = EdgeShard::create(path_string, make_config())
        .err()
        .expect("create over an existing shard must fail");
    assert!(matches!(err, EdgeError::OperationError { .. }));

    let dir2 = tempfile::tempdir().expect("tempdir failed");
    let fresh = EdgeShard::create(dir2.path().to_string_lossy().into_owned(), make_config())
        .expect("create on a fresh path failed");
    upsert_three(&fresh);
}

/// Slice conditions partition the shard: the two halves of `total: 2` are
/// disjoint and together cover every point.
#[test]
fn slice_condition_partitions_points() {
    use qdrant_edge_ffi::CountRequest;
    use qdrant_edge_ffi::filter::{Condition, Filter};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let slice_count = |total: u32, index: u32| {
        shard
            .count(CountRequest {
                filter: Some(Filter {
                    must: Some(vec![Condition::Slice { total, index }]),
                    should: None,
                    must_not: None,
                    min_should: None,
                }),
                exact: true,
            })
            .expect("slice count failed")
    };
    assert_eq!(
        slice_count(1, 0),
        3,
        "the single slice must cover everything"
    );
    assert_eq!(
        slice_count(2, 0) + slice_count(2, 1),
        3,
        "the two halves must partition the shard"
    );
}

/// `WithPayload::Exclude` drops the listed keys but keeps the rest.
#[test]
fn with_payload_exclude_drops_keys() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let op = UpdateOperation::upsert_points(
        vec![Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"keep":"yes","secret":"no"}"#.to_string()),
        }],
        None,
        None,
    )
    .expect("upsert failed");
    shard.update(op).expect("update failed");

    let recs = shard
        .retrieve(RetrieveRequest {
            point_ids: vec![PointId::NumId { value: 1 }],
            with_payload: Some(WithPayload::Exclude {
                fields: vec!["secret".to_string()],
            }),
            with_vector: None,
        })
        .expect("retrieve failed");
    let payload = recs[0].payload.as_deref().expect("payload expected");
    assert!(
        payload.contains("keep"),
        "non-excluded key must remain: {payload}"
    );
    assert!(
        !payload.contains("secret"),
        "excluded key must be dropped: {payload}"
    );
}

// ── Formula node-count cap ──────────────────────────────────────────────────

/// The formula-expression tree is capped at 10_000 total nodes
/// (`MAX_FORMULA_NODES`), independently of the depth cap (64). A host can
/// reuse one `Arc<Expression>` handle as multiple children — `sum([e, e])`
/// repeated in a loop — which grows node count as `2^depth` while depth stays
/// tiny: after 13 doublings depth is only 13 (well under the 64-deep cap) but
/// node count is `2^14 - 1 = 16383`, already past the 10_000 cap. Hitting the
/// rejection within 14 iterations (not needing anywhere near 64) proves the
/// size guard fires on its own, not merely as a side effect of the depth
/// guard.
#[test]
fn formula_oversized_node_count_rejected() {
    use qdrant_edge_ffi::Expression;

    let mut e = Expression::variable("$score".to_string());
    // 2^14 = 16384 > 10_000, so composing `sum([e, e])` at most 14 times must
    // trip the node-count cap; the depth cap (64) never comes into play.
    for _ in 0..14u32 {
        match Expression::sum(vec![e.clone(), e.clone()]) {
            Ok(next) => e = next,
            Err(err) => {
                assert!(
                    matches!(err, EdgeError::InvalidArgument { .. }),
                    "expected InvalidArgument when the formula node-count cap fires, got {err:?}"
                );
                return;
            }
        }
    }
    panic!(
        "expected the formula node-count cap (10_000) to reject the tree within 14 \
         doublings of a reused handle (2^14 - 1 = 16383 nodes), but it never fired"
    );
}

/// Over-rejection guard: a small, realistic formula (`$score + popularity`,
/// the same shape `formula_rescoring_boosts_by_payload` builds) must still
/// construct successfully — the node-count cap must not reject ordinary use.
#[test]
fn formula_normal_expression_accepted() {
    use qdrant_edge_ffi::Expression;

    let result = Expression::sum(vec![
        Expression::variable("$score".to_string()),
        Expression::variable("popularity".to_string()),
    ]);
    assert!(
        result.is_ok(),
        "a small realistic formula must not be rejected by the node-count cap, got {:?}",
        result.err()
    );
}

/// `Expression::decay` has its own node-count guard (it does not go through
/// the shared `Expression::node` helper, since it takes an `x` and an
/// optional `target` rather than a homogeneous `children` slice), currently
/// untested elsewhere. Build one accepted `x` handle near the cap (size
/// 8_191, via 12 doublings of a reused handle) and pass it as both `x` and
/// `target`: the combined size (1 + 8_191 + 8_191 = 16_383) exceeds
/// `MAX_FORMULA_NODES` (10_000), so `decay` must reject it as
/// `InvalidArgument` — proving its own guard fires before the eager
/// `x.inner.clone()` / `target.inner.clone()`.
#[test]
fn formula_decay_oversized_node_count_rejected() {
    use qdrant_edge_ffi::{DecayKind, Expression};

    let mut big_x = Expression::variable("$score".to_string());
    for _ in 0..12u32 {
        big_x = Expression::sum(vec![big_x.clone(), big_x.clone()]).expect(
            "building the oversized-decay fixture (ending at size 8_191) must itself stay \
             within the node cap",
        );
    }

    let err = Expression::decay(
        DecayKind::Lin,
        big_x.clone(),
        Some(big_x.clone()),
        None,
        None,
    )
    .expect_err(
        "decay's own node-count guard must reject x+target combined size (16_383) \
             exceeding the 10_000 node cap",
    );
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument when decay's node-count guard fires, got {err:?}"
    );
}

/// Over-rejection guard for `decay`, mirroring `formula_normal_expression_accepted`:
/// a small, realistic decay expression (`decay(Gauss, $score)`, no `target`) must
/// still construct successfully.
#[test]
fn formula_decay_accepted() {
    use qdrant_edge_ffi::{DecayKind, Expression};

    let result = Expression::decay(
        DecayKind::Gauss,
        Expression::variable("$score".to_string()),
        None,
        None,
        None,
    );
    assert!(
        result.is_ok(),
        "a small decay expression must not be rejected by the node-count cap, got {:?}",
        result.err()
    );
}

/// Regression test for the clone-ordering fix in `Expression::node`: `build`
/// (the eager `ExpressionInternal` clone) must only run after both the depth
/// and node-count checks pass, so a host cannot force an unbounded clone by
/// reusing one accepted handle as many variadic children. Build one accepted
/// handle `e` near the cap (size 4_095, via 11 doublings), then reuse it as
/// all four children of a single `sum` — the combined size
/// (1 + 4×4_095 = 16_381) exceeds the 10_000 cap and must be rejected, proving
/// the guard covers the variadic-reuse path, not just repeated binary
/// doubling (which `formula_oversized_node_count_rejected` already covers).
#[test]
fn formula_wide_fanout_rejected() {
    use qdrant_edge_ffi::Expression;

    let mut e = Expression::variable("$score".to_string());
    for _ in 0..11u32 {
        e = Expression::sum(vec![e.clone(), e.clone()]).expect(
            "building the near-cap fixture (ending at size 4_095) must itself stay within the \
             node cap",
        );
    }

    let err = Expression::sum(vec![e.clone(), e.clone(), e.clone(), e.clone()]).expect_err(
        "reusing one accepted handle as 4 children (~16_381 nodes) must be rejected by the \
             node-count cap",
    );
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument when the wide-fanout node-count cap fires, got {err:?}"
    );
}

// ── Sparse vector validation ────────────────────────────────────────────────

/// `indices.len() != values.len()` in a host-supplied sparse vector must be
/// rejected eagerly by the `upsert_points` constructor as `InvalidArgument`
/// — previously this reached an out-of-bounds index at insert/scoring time.
#[test]
fn sparse_length_mismatch_rejected() {
    use qdrant_edge_ffi::types::{NamedVector, SparseVector};

    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "sp".to_string(),
            NamedVector::Sparse {
                vector: SparseVector {
                    indices: vec![1, 2, 3],
                    values: vec![0.5],
                },
            },
        )]),
    });
}

/// The complementary length mismatch — `values` longer than `indices` — must
/// also be rejected eagerly by the `upsert_points` constructor as
/// `InvalidArgument`. `sparse_length_mismatch_rejected` above only covers
/// `indices` being the longer side.
#[test]
fn sparse_values_longer_rejected() {
    use qdrant_edge_ffi::types::{NamedVector, SparseVector};

    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "sp".to_string(),
            NamedVector::Sparse {
                vector: SparseVector {
                    indices: vec![1],
                    values: vec![0.5, 0.6],
                },
            },
        )]),
    });
}

/// Duplicate indices in a host-supplied sparse vector must be rejected
/// eagerly by the `upsert_points` constructor as `InvalidArgument` —
/// previously they silently double-counted in scoring.
#[test]
fn sparse_duplicate_indices_rejected() {
    use qdrant_edge_ffi::types::{NamedVector, SparseVector};

    assert_vector_rejected(Vector::Named {
        map: HashMap::from([(
            "sp".to_string(),
            NamedVector::Sparse {
                vector: SparseVector {
                    indices: vec![7, 7],
                    values: vec![1.0, 1.0],
                },
            },
        )]),
    });
}

// NOTE: "a valid sparse vector still upserts and round-trips" (the
// over-rejection guard for this fix) is already covered end-to-end by
// `sparse_vector_round_trips` above, which upserts
// `SparseVector { indices: [1, 5, 9], values: [0.5, 1.5, 2.5] }` into a real
// sparse-configured shard and reads it back byte-for-byte. No separate test
// is added here to avoid duplicating that coverage.

// ── order_value surfaced on order-by results ────────────────────────────────

/// `ScrollRequest.order_by` must populate `Record.order_value` on every
/// returned record, in the requested sort order — even when `with_payload`
/// is `false`, proving the sort key is recoverable without fetching the
/// payload (the whole point of surfacing it). Points are upserted in an order
/// that deliberately does not match their `rank`, so a correct result proves
/// sorting is by payload value, not by ID or insertion order.
#[test]
fn order_by_scroll_populates_order_value() {
    use qdrant_edge_ffi::types::OrderValue;
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"rank":3}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"rank":1}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"rank":2}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");

    shard
        .update(
            UpdateOperation::create_field_index("rank".to_string(), PayloadSchemaType::Integer)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let resp = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: Some(WithPayload::Bool { enable: false }),
            with_vector: None,
            order_by: Some(OrderBy {
                key: "rank".to_string(),
                direction: Some(Direction::Asc),
                start_from: None,
            }),
        })
        .expect("order-by scroll failed");

    assert_eq!(resp.records.len(), 3, "expected all three points back");

    let order_values: Vec<i64> = resp
        .records
        .iter()
        .map(|r| match r.order_value {
            Some(OrderValue::Int { value }) => value,
            other => panic!("expected Some(OrderValue::Int {{ .. }}), got {other:?}"),
        })
        .collect();
    assert_eq!(
        order_values,
        vec![1, 2, 3],
        "order_value must be populated and ascending by `rank`, recoverable without payload"
    );

    // Cross-check against the point ids: rank 1→id 2, rank 2→id 3, rank 3→id 1.
    let ids: Vec<u64> = resp
        .records
        .iter()
        .map(|r| match &r.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID PointId: {value:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![2, 3, 1],
        "scroll order must follow ascending `rank`, not id or insertion order"
    );

    // The request set `with_payload: false`; a regression that ignores the
    // flag on the order-by path must be caught here.
    for record in &resp.records {
        assert!(
            record.payload.is_none(),
            "payload must be omitted when with_payload is false"
        );
    }
}

/// Resuming an order-by scroll via `StartFrom` (built from the previous
/// page's last `order_value`) must continue from that point (inclusive), not
/// restart or skip.
#[test]
fn order_by_scroll_resumes_via_start_from() {
    use qdrant_edge_ffi::types::OrderValue;
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType, StartFrom};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"rank":1}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"rank":2}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"rank":3}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");
    shard
        .update(
            UpdateOperation::create_field_index("rank".to_string(), PayloadSchemaType::Integer)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let order_by = |start_from: Option<StartFrom>| OrderBy {
        key: "rank".to_string(),
        direction: Some(Direction::Asc),
        start_from,
    };

    let first_page = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: Some(WithPayload::Bool { enable: false }),
            with_vector: None,
            order_by: Some(order_by(None)),
        })
        .expect("first order-by scroll failed");
    let last = first_page
        .records
        .last()
        .expect("expected at least one record");
    let last_value = match last.order_value {
        Some(OrderValue::Int { value }) => value,
        other => panic!("expected Some(OrderValue::Int {{ .. }}), got {other:?}"),
    };
    assert_eq!(
        last_value, 3,
        "the last row of an ascending scroll must carry the largest rank"
    );

    let resumed = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: Some(WithPayload::Bool { enable: false }),
            with_vector: None,
            order_by: Some(order_by(Some(StartFrom::Integer { value: last_value }))),
        })
        .expect("resumed order-by scroll failed");
    assert_eq!(
        resumed.records.len(),
        1,
        "resuming from the last row's order_value (inclusive) must return only that row"
    );
    match &resumed.records[0].id {
        PointId::NumId { value } => {
            assert_eq!(*value, 3, "the resumed row must be the rank==3 point")
        }
        PointId::Uuid { value } => panic!("unexpected UUID PointId: {value:?}"),
    }
}

/// The query/search side of the `order_value` fix: `EdgeShard::query` with
/// `ScoringQuery::OrderBy` (no prefetches — a top-level order-by query is a
/// supported standalone case, equivalent to a plain ordered scroll) must
/// populate `ScoredPoint.order_value` on every hit, in the requested sort
/// order. Mirrors `order_by_scroll_populates_order_value` above, but through
/// `query()` instead of `scroll()` — previously this path had zero coverage.
#[test]
fn query_order_by_populates_order_value() {
    use qdrant_edge_ffi::types::OrderValue;
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType, QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"rank":3}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"rank":1}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"rank":2}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");

    shard
        .update(
            UpdateOperation::create_field_index("rank".to_string(), PayloadSchemaType::Integer)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let hits = shard
        .query(QueryRequest {
            limit: 10,
            offset: None,
            query: Some(ScoringQuery::OrderBy {
                order_by: OrderBy {
                    key: "rank".to_string(),
                    direction: Some(Direction::Asc),
                    start_from: None,
                },
            }),
            prefetches: vec![],
            with_vector: None,
            with_payload: Some(WithPayload::Bool { enable: false }),
            filter: None,
            score_threshold: None,
            params: None,
        })
        .expect("order-by query failed");

    assert_eq!(hits.len(), 3, "expected all three points back");

    let order_values: Vec<i64> = hits
        .iter()
        .map(|h| match h.order_value {
            Some(OrderValue::Int { value }) => value,
            other => panic!("expected Some(OrderValue::Int {{ .. }}), got {other:?}"),
        })
        .collect();
    assert_eq!(
        order_values,
        vec![1, 2, 3],
        "order_value must be populated and ascending by `rank` on the query() path too"
    );

    // Cross-check against the point ids: rank 1→id 2, rank 2→id 3, rank 3→id 1.
    let ids: Vec<u64> = hits
        .iter()
        .map(|h| match &h.id {
            PointId::NumId { value } => *value,
            PointId::Uuid { value } => panic!("unexpected UUID PointId: {value:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![2, 3, 1],
        "query order must follow ascending `rank`, not id or insertion order"
    );

    for hit in &hits {
        assert!(
            hit.payload.is_none(),
            "payload must be omitted when with_payload is false"
        );
    }
}

/// Strengthens `order_by_scroll_resumes_via_start_from`: pages through 5
/// points at `limit: Some(2)` — deliberately smaller than the point count —
/// resuming via `StartFrom` built from each page's last `order_value`, and
/// proves the pages together reconstruct the full, gap-free rank sequence
/// with exactly the one documented inclusive-overlap row at each page
/// boundary. A single resumed hop (as in `..._resumes_via_start_from`) could
/// pass by accident; requiring more than two pages here proves real
/// multi-page continuation, not a single-page degenerate case.
#[test]
fn order_by_scroll_multipage_continuation() {
    use qdrant_edge_ffi::types::OrderValue;
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType, StartFrom};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    // Ranks deliberately run opposite to id order, so a correct result proves
    // sorting is by payload value, not by id or insertion order.
    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"rank":5}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"rank":4}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"rank":3}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 4 },
            vector: named_vec([0.3, 0.3, 0.3, 0.3]),
            payload: Some(r#"{"rank":2}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 5 },
            vector: named_vec([0.6, 0.1, 0.1, 0.1]),
            payload: Some(r#"{"rank":1}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");
    shard
        .update(
            UpdateOperation::create_field_index("rank".to_string(), PayloadSchemaType::Integer)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let order_by = |start_from: Option<StartFrom>| OrderBy {
        key: "rank".to_string(),
        direction: Some(Direction::Asc),
        start_from,
    };

    // Page through with limit=2, resuming via StartFrom built from the
    // previous page's last order_value, until a short page signals the end.
    let mut pages: Vec<Vec<i64>> = Vec::new();
    let mut start_from = None;
    loop {
        let page = shard
            .scroll(ScrollRequest {
                offset: None,
                limit: Some(2),
                filter: None,
                with_payload: Some(WithPayload::Bool { enable: false }),
                with_vector: None,
                order_by: Some(order_by(start_from.clone())),
            })
            .expect("order-by scroll page failed");
        assert!(
            !page.records.is_empty(),
            "a resumed page must never come back empty while ranks remain"
        );
        let ranks: Vec<i64> = page
            .records
            .iter()
            .map(|r| match r.order_value {
                Some(OrderValue::Int { value }) => value,
                other => panic!("expected Some(OrderValue::Int {{ .. }}), got {other:?}"),
            })
            .collect();
        let last_value = *ranks.last().expect("checked non-empty above");
        let is_last_page = page.records.len() < 2;
        pages.push(ranks);
        if is_last_page {
            break;
        }
        start_from = Some(StartFrom::Integer { value: last_value });
    }

    assert!(
        pages.len() > 2,
        "5 points at limit=2 must take more than 2 pages — proves real multi-page \
         continuation, not a single resumed hop (got {} pages: {pages:?})",
        pages.len()
    );

    // Each page after the first must start with exactly the previous page's
    // last row (the inclusive StartFrom boundary); flatten and drop that one
    // duplicate per boundary to recover the full rank sequence.
    let mut flattened: Vec<i64> = Vec::new();
    for (i, page) in pages.iter().enumerate() {
        if i == 0 {
            flattened.extend(page.iter().copied());
        } else {
            let prev_last = *pages[i - 1].last().expect("pages are never empty");
            assert_eq!(
                page[0], prev_last,
                "page {i} must start with the previous page's last order_value (inclusive resume)"
            );
            flattened.extend(page.iter().skip(1).copied());
        }
    }
    assert_eq!(
        flattened,
        vec![1, 2, 3, 4, 5],
        "pages must together cover all 5 ranks in ascending order, with no gaps and no extra \
         rows beyond the single documented boundary overlap per page"
    );
}

/// Exercises `OrderValue::Float`: a float-valued payload field indexed as
/// `PayloadSchemaType::Float` and ordered on must surface
/// `Record.order_value` as `Some(OrderValue::Float { .. })`, complementing
/// the integer coverage above.
#[test]
fn order_by_scroll_float_order_value() {
    use qdrant_edge_ffi::types::OrderValue;
    use qdrant_edge_ffi::{Direction, OrderBy, PayloadSchemaType};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    let points = vec![
        Point {
            id: PointId::NumId { value: 1 },
            vector: named_vec([0.1, 0.2, 0.3, 0.4]),
            payload: Some(r#"{"score":3.5}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 2 },
            vector: named_vec([0.2, 0.1, 0.4, 0.3]),
            payload: Some(r#"{"score":1.5}"#.to_string()),
        },
        Point {
            id: PointId::NumId { value: 3 },
            vector: named_vec([0.5, 0.5, 0.5, 0.5]),
            payload: Some(r#"{"score":2.5}"#.to_string()),
        },
    ];
    shard
        .update(UpdateOperation::upsert_points(points, None, None).expect("upsert_points failed"))
        .expect("update failed");
    shard
        .update(
            UpdateOperation::create_field_index("score".to_string(), PayloadSchemaType::Float)
                .expect("create_field_index failed"),
        )
        .expect("index creation failed");

    let resp = shard
        .scroll(ScrollRequest {
            offset: None,
            limit: Some(10),
            filter: None,
            with_payload: Some(WithPayload::Bool { enable: false }),
            with_vector: None,
            order_by: Some(OrderBy {
                key: "score".to_string(),
                direction: Some(Direction::Asc),
                start_from: None,
            }),
        })
        .expect("order-by scroll failed");

    assert_eq!(resp.records.len(), 3, "expected all three points back");
    let values: Vec<f64> = resp
        .records
        .iter()
        .map(|r| match r.order_value {
            Some(OrderValue::Float { value }) => value,
            other => panic!("expected Some(OrderValue::Float {{ .. }}), got {other:?}"),
        })
        .collect();
    assert_eq!(
        values,
        vec![1.5, 2.5, 3.5],
        "float order_value must be populated and ascending by `score`"
    );
}

// ── Snapshot negative tests ─────────────────────────────────────────────────

/// A missing snapshot path must be rejected cleanly (no panic) as
/// `OperationError`, and the shard must go on serving its original data —
/// not half-recovered. `update_from_snapshot` unpacks and parses the archive
/// before touching the shard's live data, so a failure here never reaches the
/// shard at all.
#[test]
fn update_from_snapshot_bad_path_errors() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    #[allow(clippy::err_expect)]
    let err = shard
        .update_from_snapshot("/definitely/missing/nope.snapshot".to_string(), None)
        .err()
        .expect("a missing snapshot path must be rejected, not panic");
    assert!(
        matches!(err, EdgeError::OperationError { .. }),
        "expected OperationError for a missing snapshot path, got {err:?}"
    );

    let info = shard.info().expect("info failed");
    assert_eq!(
        info.points_count, 3,
        "shard must survive a failed snapshot restore with its original data intact"
    );
    let count = shard
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(count, 3, "count() must still see the original 3 points");
}

/// A file that is not a valid snapshot archive at all (random bytes) must be
/// rejected cleanly as `OperationError`, with the shard's original data
/// intact afterwards.
#[test]
fn update_from_snapshot_corrupt_archive_errors() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let bogus_dir = tempfile::tempdir().expect("tempdir failed");
    let bogus_path = bogus_dir.path().join("corrupt.snapshot");
    #[allow(clippy::disallowed_methods)]
    std::fs::write(
        &bogus_path,
        b"not a tar archive, just garbage bytes\x00\x01\x02\xff",
    )
    .expect("failed to write corrupt archive file");

    #[allow(clippy::err_expect)]
    let err = shard
        .update_from_snapshot(bogus_path.to_string_lossy().into_owned(), None)
        .err()
        .expect("a corrupt snapshot archive must be rejected, not panic");
    assert!(
        matches!(err, EdgeError::OperationError { .. }),
        "expected OperationError for a corrupt snapshot archive, got {err:?}"
    );

    let count = shard
        .count(CountRequest {
            filter: None,
            exact: true,
        })
        .expect("count failed");
    assert_eq!(
        count, 3,
        "shard must survive a corrupt snapshot restore with its original data intact"
    );
}

/// `update_from_snapshot` unpacks and parses the snapshot archive *before*
/// checking whether the shard is still loaded — so that a doomed restore
/// against a missing/corrupt archive never touches a live shard's data (see
/// `update_from_snapshot_bad_path_errors`). One consequence: called on an
/// *unloaded* shard with an invalid path, it still surfaces `OperationError`
/// from the failed unpack, not `ShardClosed` — the closed-check is only
/// reached once the archive has legitimately unpacked, and this SDK has no
/// `create_snapshot` API to construct a valid one from a test (see the
/// `snapshot_unpack_then_query` TODO above). This test pins the real,
/// observed behavior: calling it after `unload()` still fails cleanly, no
/// panic, in either the open or the closed case.
#[test]
fn update_from_snapshot_after_unload_returns_error_not_panic() {
    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);
    shard.unload().expect("unload failed");

    #[allow(clippy::err_expect)]
    let err = shard
        .update_from_snapshot("/does/not/matter.snapshot".to_string(), None)
        .err()
        .expect("update_from_snapshot on a closed shard must fail, not panic");
    assert!(
        matches!(err, EdgeError::OperationError { .. }),
        "expected OperationError (the path is rejected before the closed-check is reached), got {err:?}"
    );
}

/// A missing snapshot path passed to the free `unpack_snapshot` function must
/// be rejected cleanly as `OperationError`, not panic.
#[test]
fn unpack_snapshot_bad_path_errors() {
    use qdrant_edge_ffi::unpack_snapshot;

    let target_dir = tempfile::tempdir().expect("tempdir failed");
    let target = target_dir.path().to_string_lossy().into_owned();

    #[allow(clippy::err_expect)]
    let err = unpack_snapshot("/missing.snapshot".to_string(), target)
        .err()
        .expect("unpacking a missing snapshot path must fail, not panic");
    assert!(
        matches!(err, EdgeError::OperationError { .. }),
        "expected OperationError for a missing snapshot path, got {err:?}"
    );
}

/// Pins the exact `MAX_FORMULA_NODES` (10_000) threshold: a `sum` over N leaf
/// handles has size `1 + N`, so 9_999 leaves is exactly at the cap (accepted)
/// and 10_000 leaves is one over (rejected).
#[test]
fn formula_node_count_exact_boundary() {
    use qdrant_edge_ffi::Expression;

    let v = Expression::variable("$score".to_string());

    // Exactly 10_000 nodes (1 sum node + 9_999 leaves) — accepted.
    assert!(
        Expression::sum(vec![v.clone(); 9_999]).is_ok(),
        "a formula with exactly MAX_FORMULA_NODES (10_000) nodes must be accepted"
    );

    // 10_001 nodes (1 sum node + 10_000 leaves) — rejected.
    let err = Expression::sum(vec![v.clone(); 10_000])
        .expect_err("a formula with 10_001 nodes (cap + 1) must be rejected");
    assert!(
        matches!(err, EdgeError::InvalidArgument { .. }),
        "expected InvalidArgument at the node-count cap + 1, got {err:?}"
    );
}

/// Distance matrix over sampled points. Gated behind the `matrix` feature,
/// which is on by default (the mobile bindgen excludes it via
/// `--no-default-features`); this test therefore runs by default.
#[cfg(feature = "matrix")]
#[test]
fn search_matrix_relates_samples() {
    use qdrant_edge_ffi::SearchMatrixRequest;

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    let response = shard
        .search_matrix(SearchMatrixRequest {
            sample_size: 3,
            limit_per_sample: 2,
            filter: None,
            using: Some("vec".to_string()),
        })
        .expect("search_matrix failed");
    assert_eq!(response.sample_ids.len(), 3, "all three points sampled");
    assert_eq!(
        response.nearests.len(),
        response.sample_ids.len(),
        "one neighbour row per sample"
    );
    for row in &response.nearests {
        assert!(
            row.len() <= 2 && !row.is_empty(),
            "each sample must have 1..=2 neighbours within the sampled set"
        );
    }
}

// ── Payload schema in info() ──────────────────────────────────────────────────

/// `info()` must report every payload index: a bare-type index shows its
/// `data_type` with no params, and the indexed point count.
#[test]
fn info_reports_payload_schema_for_bare_index() {
    use qdrant_edge_ffi::PayloadSchemaType;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    upsert_three(&shard);
    shard
        .update(
            UpdateOperation::create_field_index("title".to_string(), PayloadSchemaType::Keyword)
                .expect("create_field_index failed"),
        )
        .expect("update failed");

    let info = shard.info().expect("info failed");
    let index = info
        .payload_schema
        .get("title")
        .expect("'title' index missing from payload_schema");
    assert!(matches!(index.data_type, PayloadSchemaType::Keyword));
    assert!(
        index.params.is_none(),
        "bare-type index must report no params, got {:?}",
        index.params
    );
    assert_eq!(index.points, 3, "all three points carry 'title'");
}

/// Parameters passed to `create_field_index_with_params` must be echoed back
/// by `info()`, and must survive flush → unload → reload (they are part of
/// the persisted index configuration, not session state).
#[test]
fn field_index_params_echo_in_info_and_survive_reload() {
    use qdrant_edge_ffi::config::Memory;
    use qdrant_edge_ffi::{KeywordIndexParams, PayloadIndexParams, PayloadSchemaType};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    let assert_echoed = |shard: &EdgeShard| {
        let info = shard.info().expect("info failed");
        let index = info
            .payload_schema
            .get("title")
            .expect("'title' index missing from payload_schema");
        assert!(matches!(index.data_type, PayloadSchemaType::Keyword));
        let Some(PayloadIndexParams::Keyword { config }) = &index.params else {
            panic!("expected echoed keyword params, got {:?}", index.params);
        };
        assert_eq!(config.is_tenant, Some(true));
        assert!(matches!(config.memory, Some(Memory::Cold)));
        assert_eq!(config.enable_hnsw, Some(false));
        assert_eq!(config.prefix, Some(true));
        assert_eq!(index.points, 3);
    };

    {
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load failed");
        upsert_three(&shard);
        shard
            .update(
                UpdateOperation::create_field_index_with_params(
                    "title".to_string(),
                    PayloadIndexParams::Keyword {
                        config: KeywordIndexParams {
                            is_tenant: Some(true),
                            memory: Some(Memory::Cold),
                            enable_hnsw: Some(false),
                            prefix: Some(true),
                        },
                    },
                )
                .expect("create_field_index_with_params failed"),
            )
            .expect("update failed");

        assert_echoed(&shard);

        shard.flush().expect("flush failed");
        shard.unload().expect("unload failed");
    }

    let shard: Arc<EdgeShard> = EdgeShard::load(path, None).expect("reload failed");
    assert_echoed(&shard);
}

/// A params-created full-text index must be a working index (not just stored
/// config): the tokenizer/stopwords options apply to matching.
#[test]
fn text_index_with_params_filters_with_stopwords() {
    use qdrant_edge_ffi::filter::{Condition, FieldCondition, Match};
    use qdrant_edge_ffi::{PayloadIndexParams, Stopwords, TextIndexParams};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");

    upsert_three(&shard);
    shard
        .update(
            UpdateOperation::create_field_index_with_params(
                "title".to_string(),
                PayloadIndexParams::Text {
                    config: TextIndexParams {
                        tokenizer: None,
                        min_token_len: None,
                        max_token_len: None,
                        lowercase: None,
                        ascii_folding: None,
                        phrase_matching: None,
                        stopwords: Some(Stopwords::Set {
                            languages: None,
                            custom: Some(vec!["point".to_string()]),
                        }),
                        memory: None,
                        stemmer: None,
                        enable_hnsw: None,
                    },
                },
            )
            .expect("create_field_index_with_params failed"),
        )
        .expect("update failed");

    let count_matching = |text: &str| {
        let filter = qdrant_edge_ffi::filter::Filter {
            must: Some(vec![Condition::Field {
                condition: FieldCondition {
                    key: "title".to_string(),
                    r#match: Some(Match::Text {
                        text: text.to_string(),
                    }),
                    range: None,
                    datetime_range: None,
                    geo_bounding_box: None,
                    geo_radius: None,
                    geo_polygon: None,
                    values_count: None,
                },
            }]),
            should: None,
            must_not: None,
            min_should: None,
        };
        shard
            .count(CountRequest {
                filter: Some(filter),
                exact: true,
            })
            .expect("count failed")
    };

    // "two" is a real token: exactly one point ("point two") matches.
    assert_eq!(count_matching("two"), 1);
    // "point" is in the custom stopword set: it was never indexed, so no
    // point matches even though every title contains it.
    assert_eq!(count_matching("point"), 0);
}
