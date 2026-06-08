use std::collections::HashMap;
use std::sync::Arc;

use qdrant_edge_ffi::config::{Distance, EdgeConfig, VectorDataConfig};
use qdrant_edge_ffi::error::EdgeError;
use qdrant_edge_ffi::filter::{Condition, FieldCondition, Filter, GeoLineString, GeoPoint, GeoPolygon, Match};
use qdrant_edge_ffi::query::CountRequest;
use qdrant_edge_ffi::types::{PointId, WithPayload};
use qdrant_edge_ffi::EdgeShard;
use segment::types::{
    Condition as SegmentCondition, Filter as SegmentFilter, PointIdType,
    WithPayloadInterface,
};

// ── PointId / UUID ────────────────────────────────────────────────────────────

#[test]
fn bad_uuid_point_id_returns_error_not_panic() {
    let bad = PointId::Uuid { value: "not-a-uuid".to_string() };
    let r: Result<PointIdType, _> = bad.try_into();
    assert!(r.is_err());
}

#[test]
fn good_uuid_point_id_converts() {
    let g = PointId::Uuid { value: "550e8400-e29b-41d4-a716-446655440000".to_string() };
    let r: Result<PointIdType, _> = g.try_into();
    assert!(r.is_ok());
}

#[test]
fn num_point_id_converts() {
    let n = PointId::NumId { value: 42 };
    assert!(matches!(n.try_into(), Ok(PointIdType::NumId(42))));
}

// ── GeoPoint ──────────────────────────────────────────────────────────────────

#[test]
fn out_of_range_geo_point_returns_error() {
    use qdrant_edge_ffi::filter::GeoPoint;
    use segment::types::GeoPoint as SegmentGeoPoint;

    let bad = GeoPoint { lon: 10.0, lat: 999.0 };
    let r: Result<SegmentGeoPoint, _> = bad.try_into();
    assert!(r.is_err());
}

#[test]
fn valid_geo_point_converts() {
    use qdrant_edge_ffi::filter::GeoPoint;
    use segment::types::GeoPoint as SegmentGeoPoint;

    let good = GeoPoint { lon: 13.4, lat: 52.5 };
    let r: Result<SegmentGeoPoint, _> = good.try_into();
    assert!(r.is_ok());
}

// ── JSON-path key (via Condition::Field) ─────────────────────────────────────

#[test]
fn bad_payload_key_in_field_condition_returns_error() {
    // A key with a space is not a valid JSON-path.
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "has space".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

#[test]
fn valid_payload_key_in_field_condition_converts() {
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "meta.author".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_ok());
}

// ── Cascade / composition tests ───────────────────────────────────────────────

/// HasId with a bad UUID propagates an error through the collect path.
#[test]
fn has_id_bad_uuid_returns_error() {
    let cond = Condition::HasId {
        ids: vec![PointId::Uuid { value: "nope".to_string() }],
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// IsEmpty with an invalid JSON-path key returns an error.
#[test]
fn is_empty_bad_key_returns_error() {
    let cond = Condition::IsEmpty { key: "bad key".to_string() };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// IsNull with an invalid JSON-path key returns an error.
#[test]
fn is_null_bad_key_returns_error() {
    let cond = Condition::IsNull { key: "bad key".to_string() };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// A Filter whose `must` list contains a bad-key condition short-circuits
/// through the transpose and surfaces the error.
#[test]
fn filter_must_with_bad_key_returns_error() {
    let bad_cond = Condition::IsEmpty { key: "bad key".to_string() };
    let filter = Filter {
        must: Some(vec![bad_cond]),
        should: None,
        must_not: None,
    };
    let r: Result<SegmentFilter, _> = filter.try_into();
    assert!(r.is_err());
}

// ── Match::Any / Match::Except fallibility (C1) ───────────────────────────────

fn field_with_match(m: Match) -> Condition {
    Condition::Field {
        condition: FieldCondition {
            key: "k".to_string(),
            r#match: Some(m),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
        },
    }
}

#[test]
fn match_any_both_none_returns_error() {
    let cond = field_with_match(Match::Any { strings: None, integers: None });
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

#[test]
fn match_any_both_set_returns_error() {
    let cond = field_with_match(Match::Any {
        strings: Some(vec!["a".to_string()]),
        integers: Some(vec![1]),
    });
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

#[test]
fn match_any_strings_only_ok() {
    let cond = field_with_match(Match::Any {
        strings: Some(vec!["hello".to_string()]),
        integers: None,
    });
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_ok());
}

#[test]
fn match_except_neither_returns_error() {
    let cond = field_with_match(Match::Except { strings: None, integers: None });
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

// ── WithPayload::Fields bad key (I2) ─────────────────────────────────────────

#[test]
fn with_payload_bad_field_returns_error() {
    let wp = WithPayload::Fields {
        fields: vec!["valid_key".to_string(), "bad key with spaces".to_string()],
    };
    let r: Result<WithPayloadInterface, _> = wp.try_into();
    assert!(r.is_err());
}

#[test]
fn with_payload_good_fields_ok() {
    let wp = WithPayload::Fields {
        fields: vec!["meta.author".to_string(), "title".to_string()],
    };
    let r: Result<WithPayloadInterface, _> = wp.try_into();
    assert!(r.is_ok());
}

// ── C5: branchable EdgeError variants ────────────────────────────────────────

/// A bad UUID at the FFI boundary must produce `EdgeError::InvalidArgument`,
/// not the generic `OperationError`.
#[test]
fn bad_uuid_is_invalid_argument() {
    let bad = PointId::Uuid { value: "nope".to_string() };
    let r: Result<PointIdType, _> = bad.try_into();
    assert!(r.is_err());
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

/// After unloading a shard, every operation must return `EdgeError::ShardClosed`.
#[test]
fn closed_shard_returns_shard_closed() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // Build a minimal one-field config (size 4, Dot distance).
    let config = EdgeConfig {
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
    };

    let shard: Arc<EdgeShard> = EdgeShard::load(path, Some(config))
        .expect("EdgeShard::load failed");

    // Eagerly release all file handles.
    shard.unload();

    // Any operation on the unloaded shard must yield ShardClosed.
    let result = shard.count(CountRequest { filter: None, exact: false });
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), EdgeError::ShardClosed),
        "expected ShardClosed after unload"
    );
}

// ── GeoPolygon filter ─────────────────────────────────────────────────────────

/// A valid GeoPolygon FieldCondition (exterior with 4 points forming a closed
/// ring) must convert successfully to the segment type.
#[test]
fn geo_polygon_field_condition_converts() {
    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint { lon: 0.0, lat: 0.0 },
                GeoPoint { lon: 0.0, lat: 1.0 },
                GeoPoint { lon: 1.0, lat: 1.0 },
                GeoPoint { lon: 0.0, lat: 0.0 },
            ],
        },
        interiors: None,
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_ok(), "expected Ok, got: {:?}", r.unwrap_err());
}

/// A polygon whose exterior contains an out-of-range coordinate must propagate
/// the error from GeoPoint validation.
#[test]
fn geo_polygon_bad_coordinate_returns_error() {
    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint { lon: 0.0, lat: 999.0 }, // lat out of range
                GeoPoint { lon: 0.0, lat: 1.0 },
                GeoPoint { lon: 1.0, lat: 1.0 },
                GeoPoint { lon: 0.0, lat: 999.0 },
            ],
        },
        interiors: None,
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err(), "expected Err for out-of-range coordinate");
}

// ── Turbo4 datatype round-trip ────────────────────────────────────────────────

/// VectorStorageDatatype::Turbo4 must round-trip through the segment type
/// without loss.
#[test]
fn turbo4_datatype_round_trips() {
    use qdrant_edge_ffi::config::VectorStorageDatatype;
    use segment::types::VectorStorageDatatype as SegmentVectorStorageDatatype;

    let ffi_turbo = VectorStorageDatatype::Turbo4;
    let seg: SegmentVectorStorageDatatype = ffi_turbo.into();
    assert!(
        matches!(seg, SegmentVectorStorageDatatype::Turbo4),
        "expected SegmentVectorStorageDatatype::Turbo4, got {:?}",
        seg
    );

    let back: VectorStorageDatatype = seg.into();
    assert!(
        matches!(back, VectorStorageDatatype::Turbo4),
        "expected VectorStorageDatatype::Turbo4 back, got {:?}",
        back
    );
}
