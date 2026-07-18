/// Integration tests for `qdrant-edge-ffi`.
///
/// These tests exercise the full DB invariant path through the FFI surface:
/// persistence, payload round-trips, concurrent safety, and missing-ID contracts.
/// Each test creates its own isolated tempdir shard so they can run in parallel
/// without interference.
use std::collections::HashMap;
use std::sync::Arc;

use qdrant_edge_ffi::EdgeShard;
use qdrant_edge_ffi::config::{Distance, EdgeConfig, VectorDataConfig};
use qdrant_edge_ffi::error::EdgeError;
use qdrant_edge_ffi::query::{CountRequest, Query, ScrollRequest, SearchRequest};
use qdrant_edge_ffi::types::{Point, PointId, Vector, WithPayload, WithVector};
use qdrant_edge_ffi::update::UpdateOperation;

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
        let shard: Arc<EdgeShard> =
            EdgeShard::load(path.clone(), Some(make_config())).expect("load (session 1) failed");
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
            .count(CountRequest {
                filter: None,
                exact: true,
            })
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
                    let result = shard_clone.count(CountRequest {
                        filter: None,
                        exact: false,
                    });
                    match result {
                        Ok(_) => {}
                        Err(EdgeError::ShardClosed) => {}
                        Err(other) => {
                            panic!("unexpected error during concurrent count: {:?}", other);
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

// ── Test 7: scalar_quantization_accepted_at_load ──────────────────────────────

/// The FFI exposes all four quantization strategies for parity with the Python
/// Edge SDK, so `load` must ACCEPT a Scalar-quantized config (not reject it).
/// Edge core's `for_appendable_segment` filter may drop Scalar on the appendable
/// segment — that is shared engine behavior across all Edge SDKs, not something
/// this SDK rejects.
#[test]
fn scalar_quantization_accepted_at_load() {
    use qdrant_edge_ffi::config::{QuantizationConfig, ScalarQuantizationParams, ScalarType};

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
                        always_ram: Some(true),
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
        .retrieve(vec![PointId::NumId { value: 2 }], None, None)
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
                vector: vec![0.1, 0.2, 0.3, 0.4],
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
    )
    .expect("set_payload failed");
    shard.update(op).expect("set_payload update failed");

    let got = shard
        .retrieve(
            vec![PointId::NumId { value: 1 }],
            Some(WithPayload::Bool { enable: true }),
            None,
        )
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
        CompressionRatio, ProductQuantizationParams, QuantizationConfig,
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
                        always_ram: Some(true),
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
    use qdrant_edge_ffi::config::{QuantizationConfig, TurboQuantBitSize, TurboQuantizationParams};

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
                        always_ram: Some(true),
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
    use qdrant_edge_ffi::config::{BinaryQuantizationParams, QuantizationConfig};

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
                        always_ram: Some(true),
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
    use qdrant_edge_ffi::query::{QueryRequest, ScoringQuery};

    let dir = tempfile::tempdir().expect("tempdir failed");
    let path = dir.path().to_string_lossy().into_owned();
    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(make_config())).expect("load failed");
    upsert_three(&shard);

    #[allow(clippy::err_expect)]
    let search_err = shard
        .search(SearchRequest {
            query: Query::Nearest {
                vector: vec![0.1, 0.2, 0.3, 0.4],
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
                    vector: vec![0.1, 0.2, 0.3, 0.4],
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
    use qdrant_edge_ffi::config::HnswIndexConfig;

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
                    on_disk: Some(false),
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
                vector: vec![0.1, 0.2, 0.3, 0.4],
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
    use qdrant_edge_ffi::config::HnswIndexConfig;

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
        on_disk: Some(false),
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
        .update(UpdateOperation::upsert_points(points).expect("upsert_points failed"))
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
