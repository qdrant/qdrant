use qdrant_edge_ffi::filter::{Condition, FieldCondition, Filter};
use qdrant_edge_ffi::types::PointId;
use segment::types::{Condition as SegmentCondition, Filter as SegmentFilter, PointIdType};

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
