use std::collections::HashMap;
use std::sync::Arc;

use qdrant_edge_ffi::config::{Distance, EdgeConfig, VectorDataConfig};
use qdrant_edge_ffi::error::EdgeError;
use qdrant_edge_ffi::filter::{
    Condition, FieldCondition, Filter, GeoLineString, GeoPoint, GeoPolygon, GeoRadius, Match,
};
use qdrant_edge_ffi::types::{PointId, WithPayload};
use qdrant_edge_ffi::{CountRequest, EdgeShard};
use segment::types::{
    Condition as SegmentCondition, Filter as SegmentFilter, PointIdType, WithPayloadInterface,
};

// ── PointId / UUID ────────────────────────────────────────────────────────────

#[test]
fn bad_uuid_point_id_returns_error_not_panic() {
    let bad = PointId::Uuid {
        value: "not-a-uuid".to_string(),
    };
    let r: Result<PointIdType, _> = bad.try_into();
    assert!(r.is_err());
}

#[test]
fn good_uuid_point_id_converts() {
    let g = PointId::Uuid {
        value: "550e8400-e29b-41d4-a716-446655440000".to_string(),
    };
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

    let bad = GeoPoint {
        lon: 10.0,
        lat: 999.0,
    };
    let r: Result<SegmentGeoPoint, _> = bad.try_into();
    assert!(r.is_err());
}

#[test]
fn valid_geo_point_converts() {
    use qdrant_edge_ffi::filter::GeoPoint;
    use segment::types::GeoPoint as SegmentGeoPoint;

    let good = GeoPoint {
        lon: 13.4,
        lat: 52.5,
    };
    let r: Result<SegmentGeoPoint, _> = good.try_into();
    assert!(r.is_ok());
}

// ── JSON-path key (via Condition::Field) ─────────────────────────────────────

#[test]
fn bad_payload_key_in_field_condition_returns_error() {
    // A key with a space is not a valid JSON-path. A valid predicate is set so
    // the failure is unambiguously the key parse, not the no-predicate check.
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "has space".to_string(),
            r#match: Some(Match::Text {
                text: "x".to_string(),
            }),
            range: None,
            datetime_range: None,
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
    // A valid key plus a real predicate (a no-predicate condition is rejected,
    // see `field_condition_no_predicate_returns_error`).
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "meta.author".to_string(),
            r#match: Some(Match::Text {
                text: "ann".to_string(),
            }),
            range: None,
            datetime_range: None,
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
        ids: vec![PointId::Uuid {
            value: "nope".to_string(),
        }],
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// IsEmpty with an invalid JSON-path key returns an error.
#[test]
fn is_empty_bad_key_returns_error() {
    let cond = Condition::IsEmpty {
        key: "bad key".to_string(),
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// IsNull with an invalid JSON-path key returns an error.
#[test]
fn is_null_bad_key_returns_error() {
    let cond = Condition::IsNull {
        key: "bad key".to_string(),
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err());
}

/// A Filter whose `must` list contains a bad-key condition short-circuits
/// through the transpose and surfaces the error.
#[test]
fn filter_must_with_bad_key_returns_error() {
    let bad_cond = Condition::IsEmpty {
        key: "bad key".to_string(),
    };
    let filter = Filter {
        must: Some(vec![bad_cond]),
        should: None,
        must_not: None,
        min_should: None,
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
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
        },
    }
}

#[test]
fn match_any_both_none_returns_error() {
    let cond = field_with_match(Match::Any {
        strings: None,
        integers: None,
    });
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
    let cond = field_with_match(Match::Except {
        strings: None,
        integers: None,
    });
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
    let bad = PointId::Uuid {
        value: "nope".to_string(),
    };
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

    let shard: Arc<EdgeShard> =
        EdgeShard::load(path, Some(config)).expect("EdgeShard::load failed");

    // Eagerly release all file handles.
    shard.unload().expect("unload failed");

    // Any operation on the unloaded shard must yield ShardClosed.
    let result = shard.count(CountRequest {
        filter: None,
        exact: false,
    });
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
            datetime_range: None,
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
                GeoPoint {
                    lon: 0.0,
                    lat: 999.0,
                }, // lat out of range
                GeoPoint { lon: 0.0, lat: 1.0 },
                GeoPoint { lon: 1.0, lat: 1.0 },
                GeoPoint {
                    lon: 0.0,
                    lat: 999.0,
                },
            ],
        },
        interiors: None,
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err(), "expected Err for out-of-range coordinate");
}

/// A polygon whose exterior ring has fewer than 4 points is rejected at the FFI
/// boundary (mirrors the engine's `validate_line_string`), rather than reaching
/// the geo index where it can panic on indexed payloads.
#[test]
fn geo_polygon_too_few_points_returns_error() {
    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint { lon: 0.0, lat: 0.0 },
                GeoPoint { lon: 0.0, lat: 1.0 },
                GeoPoint { lon: 0.0, lat: 0.0 },
            ], // only 3 points
        },
        interiors: None,
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err(), "expected Err for ring with fewer than 4 points");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

/// A polygon whose exterior ring does not close (first != last) is rejected.
#[test]
fn geo_polygon_unclosed_ring_returns_error() {
    let polygon = GeoPolygon {
        exterior: GeoLineString {
            points: vec![
                GeoPoint { lon: 0.0, lat: 0.0 },
                GeoPoint { lon: 0.0, lat: 1.0 },
                GeoPoint { lon: 1.0, lat: 1.0 },
                GeoPoint { lon: 1.0, lat: 0.0 }, // != first → not closed
            ],
        },
        interiors: None,
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err(), "expected Err for unclosed ring");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

/// A malformed *interior* ring is rejected too — the same validating
/// conversion runs for exterior and interior rings.
#[test]
fn geo_polygon_bad_interior_ring_returns_error() {
    let good_exterior = GeoLineString {
        points: vec![
            GeoPoint { lon: 0.0, lat: 0.0 },
            GeoPoint {
                lon: 0.0,
                lat: 10.0,
            },
            GeoPoint {
                lon: 10.0,
                lat: 10.0,
            },
            GeoPoint { lon: 0.0, lat: 0.0 },
        ],
    };
    let bad_interior = GeoLineString {
        points: vec![
            GeoPoint { lon: 1.0, lat: 1.0 },
            GeoPoint { lon: 2.0, lat: 2.0 },
        ], // only 2 points
    };
    let polygon = GeoPolygon {
        exterior: good_exterior,
        interiors: Some(vec![bad_interior]),
    };
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: Some(polygon),
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(r.is_err(), "expected Err for malformed interior ring");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

// ── GeoRadius bounds ──────────────────────────────────────────────────────────

fn field_with_geo_radius(radius: f64) -> Condition {
    Condition::Field {
        condition: FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: Some(GeoRadius {
                center: GeoPoint {
                    lon: 13.4,
                    lat: 52.5,
                },
                radius,
            }),
            geo_polygon: None,
            values_count: None,
        },
    }
}

#[test]
fn geo_radius_negative_returns_error() {
    let r: Result<SegmentCondition, _> = field_with_geo_radius(-1.0).try_into();
    assert!(r.is_err(), "expected Err for negative radius");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

#[test]
fn geo_radius_nan_returns_error() {
    let r: Result<SegmentCondition, _> = field_with_geo_radius(f64::NAN).try_into();
    assert!(r.is_err(), "expected Err for NaN radius");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

#[test]
fn geo_radius_infinite_returns_error() {
    let r: Result<SegmentCondition, _> = field_with_geo_radius(f64::INFINITY).try_into();
    assert!(r.is_err(), "expected Err for infinite radius");
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

#[test]
fn geo_radius_valid_converts() {
    let r: Result<SegmentCondition, _> = field_with_geo_radius(1000.0).try_into();
    assert!(
        r.is_ok(),
        "expected Ok for a valid radius, got: {:?}",
        r.err()
    );
}

/// A zero radius is a degenerate (empty) circle but NOT invalid — the engine
/// accepts it (`check_point` uses `distance < radius`, so it matches nothing).
/// Pin this so the bound stays "non-negative", not "strictly positive".
#[test]
fn geo_radius_zero_converts() {
    let r: Result<SegmentCondition, _> = field_with_geo_radius(0.0).try_into();
    assert!(
        r.is_ok(),
        "expected Ok for a zero radius, got: {:?}",
        r.err()
    );
}

// ── FieldCondition: at-least-one predicate ────────────────────────────────────

/// A FieldCondition with no predicate set is a silent no-op (matches every
/// point); the FFI rejects it, mirroring the engine's `validate_field_condition`
/// ("at least one", NOT "exactly one").
#[test]
fn field_condition_no_predicate_returns_error() {
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "meta.author".to_string(),
            r#match: None,
            range: None,
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(
        r.is_err(),
        "expected Err for a predicate-less field condition"
    );
    assert!(matches!(r.unwrap_err(), EdgeError::InvalidArgument { .. }));
}

/// Setting MORE than one predicate is allowed (they AND together) — this is the
/// contract the engine/gRPC/REST/Python SDK use, so the FFI must not reject it.
#[test]
fn field_condition_multiple_predicates_converts() {
    use qdrant_edge_ffi::filter::{RangeFloat, ValuesCount};
    let cond = Condition::Field {
        condition: FieldCondition {
            key: "tags".to_string(),
            r#match: Some(Match::Text {
                text: "x".to_string(),
            }),
            range: Some(RangeFloat {
                gte: Some(1.0),
                gt: None,
                lte: None,
                lt: None,
            }),
            datetime_range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: Some(ValuesCount {
                gte: Some(1),
                gt: None,
                lte: None,
                lt: None,
            }),
        },
    };
    let r: Result<SegmentCondition, _> = cond.try_into();
    assert!(
        r.is_ok(),
        "multiple predicates must be allowed (AND-combined), got: {:?}",
        r.err()
    );
}

// ── Turbo4 datatype round-trip ────────────────────────────────────────────────

/// VectorStorageDatatype::Turbo4 must round-trip through the segment type
/// without loss.
#[test]
fn turbo4_datatype_round_trips() {
    use qdrant_edge_ffi::config::VectorStorageDatatype;
    use segment::types::VectorStorageDatatype as SegmentVectorStorageDatatype;

    let ffi_turbo = VectorStorageDatatype::Turbo4;
    let seg = SegmentVectorStorageDatatype::from(ffi_turbo);
    assert!(
        matches!(seg, SegmentVectorStorageDatatype::Turbo4),
        "expected SegmentVectorStorageDatatype::Turbo4, got {:?}",
        seg
    );

    let back = VectorStorageDatatype::from(seg);
    assert!(
        matches!(back, VectorStorageDatatype::Turbo4),
        "expected VectorStorageDatatype::Turbo4 back, got {:?}",
        back
    );
}

// ── New filter surface: slice / nested / text matches / datetime / min_should ─

#[test]
fn slice_condition_converts_and_validates() {
    let ok = Condition::Slice { total: 4, index: 3 };
    let r: Result<SegmentCondition, _> = ok.try_into();
    assert!(r.is_ok());

    let zero_total = Condition::Slice { total: 0, index: 0 };
    let r: Result<SegmentCondition, _> = zero_total.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));

    let index_out_of_range = Condition::Slice { total: 2, index: 2 };
    let r: Result<SegmentCondition, _> = index_out_of_range.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));
}

#[test]
fn nested_condition_converts() {
    let nested = Condition::Nested {
        key: "diet".to_string(),
        filter: Filter {
            must: Some(vec![Condition::IsEmpty {
                key: "gaps".to_string(),
            }]),
            should: None,
            must_not: None,
            min_should: None,
        },
    };
    let r: Result<SegmentCondition, _> = nested.try_into();
    assert!(matches!(r, Ok(SegmentCondition::Nested(_))));

    let bad_key = Condition::Nested {
        key: "diet[".to_string(),
        filter: Filter {
            must: None,
            should: None,
            must_not: None,
            min_should: None,
        },
    };
    let r: Result<SegmentCondition, _> = bad_key.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));
}

#[test]
fn text_match_variants_convert() {
    use segment::types::Match as SegmentMatch;

    let cases = vec![
        Match::TextAny {
            text_any: "a b".to_string(),
        },
        Match::Phrase {
            phrase: "a b".to_string(),
        },
        Match::Prefix {
            prefix: "ab".to_string(),
        },
    ];
    let converted: Vec<SegmentMatch> = cases
        .into_iter()
        .map(|m| m.try_into().expect("text match must convert"))
        .collect();
    assert!(matches!(converted[0], SegmentMatch::TextAny(_)));
    assert!(matches!(converted[1], SegmentMatch::Phrase(_)));
    assert!(matches!(converted[2], SegmentMatch::Prefix(_)));
}

#[test]
fn datetime_range_converts_and_rejects_garbage() {
    use qdrant_edge_ffi::filter::RangeDatetime;
    use segment::types::FieldCondition as SegmentFieldCondition;

    let base = |datetime_range: Option<RangeDatetime>| FieldCondition {
        key: "ts".to_string(),
        r#match: None,
        range: None,
        datetime_range,
        geo_bounding_box: None,
        geo_radius: None,
        geo_polygon: None,
        values_count: None,
    };

    let ok = base(Some(RangeDatetime {
        gte: Some("2024-01-01T00:00:00Z".to_string()),
        gt: None,
        lte: Some("2025-01-01T00:00:00Z".to_string()),
        lt: None,
    }));
    let r: Result<SegmentFieldCondition, _> = ok.try_into();
    assert!(r.is_ok(), "valid RFC 3339 bounds must convert: {r:?}");

    let bad = base(Some(RangeDatetime {
        gte: Some("not-a-date".to_string()),
        gt: None,
        lte: None,
        lt: None,
    }));
    let r: Result<SegmentFieldCondition, _> = bad.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));

    // Both range kinds at once is contradictory: the engine holds only one.
    let mut both = base(Some(RangeDatetime {
        gte: Some("2024-01-01T00:00:00Z".to_string()),
        gt: None,
        lte: None,
        lt: None,
    }));
    both.range = Some(qdrant_edge_ffi::filter::RangeFloat {
        gte: Some(1.0),
        gt: None,
        lte: None,
        lt: None,
    });
    let r: Result<SegmentFieldCondition, _> = both.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));
}

#[test]
fn min_should_converts() {
    use qdrant_edge_ffi::filter::MinShould;

    let filter = Filter {
        must: None,
        should: None,
        must_not: None,
        min_should: Some(MinShould {
            conditions: vec![
                Condition::IsEmpty {
                    key: "a".to_string(),
                },
                Condition::IsNull {
                    key: "b".to_string(),
                },
            ],
            min_count: 1,
        }),
    };
    let r: Result<SegmentFilter, _> = filter.try_into();
    let converted = r.expect("min_should filter must convert");
    let min_should = converted.min_should.expect("min_should must be present");
    assert_eq!(min_should.min_count, 1);
    assert_eq!(min_should.conditions.len(), 2);
}

#[test]
fn with_payload_exclude_converts_to_selector() {
    use segment::types::PayloadSelector;

    let w = WithPayload::Exclude {
        fields: vec!["secret".to_string()],
    };
    let r: Result<WithPayloadInterface, _> = w.try_into();
    assert!(matches!(
        r,
        Ok(WithPayloadInterface::Selector(PayloadSelector::Exclude(_)))
    ));

    let bad = WithPayload::Exclude {
        fields: vec!["secret[".to_string()],
    };
    let r: Result<WithPayloadInterface, _> = bad.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));
}

#[test]
fn order_by_start_from_converts() {
    use qdrant_edge_ffi::{OrderBy, StartFrom};
    use segment::data_types::order_by::OrderBy as SegmentOrderBy;

    let ok = OrderBy {
        key: "ts".to_string(),
        direction: None,
        start_from: Some(StartFrom::Datetime {
            value: "2024-01-01T00:00:00Z".to_string(),
        }),
    };
    let r: Result<SegmentOrderBy, _> = ok.try_into();
    assert!(r.is_ok(), "datetime start_from must convert: {r:?}");

    let bad = OrderBy {
        key: "ts".to_string(),
        direction: None,
        start_from: Some(StartFrom::Datetime {
            value: "yesterday-ish".to_string(),
        }),
    };
    let r: Result<SegmentOrderBy, _> = bad.try_into();
    assert!(matches!(r, Err(EdgeError::InvalidArgument { .. })));
}
