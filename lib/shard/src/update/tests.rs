use std::num::NonZeroUsize;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::RwLock;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, only_default_vector};
use segment::entry::ReadSegmentEntry as _;
use segment::entry::entry_point::SegmentEntry as _;
use segment::payload_json;
use segment::types::{
    Condition, FieldCondition, Filter, Match, MatchValue, PayloadKeyType, PointIdType,
    ValueVariants,
};
use tempfile::Builder;

use crate::fixtures::{
    build_segment_1, build_segment_2, empty_segment, empty_segment_with_deferred,
};
use crate::operations::point_ops::PointStructRawPersisted;
use crate::segment_holder::{FlushMode, SegmentHolder};
use crate::update::{
    clear_payload_by_filter, create_field_index, delete_payload_by_filter, delete_points_by_filter,
    delete_vectors_by_filter, overwrite_payload_by_filter, set_payload, set_payload_by_filter,
    sync_points_raw, upsert_points_raw,
};

#[test]
fn test_delete_by_filter_version_bump() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let hw_counter = HardwareCounterCell::new();

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);

    const DELETE_OP_NUM: u64 = 16;

    assert!(
        holder
            .iter()
            .all(|i| i.1.get().read().version() < DELETE_OP_NUM)
    );

    let old_version = holder
        .flush_all(FlushMode::Sync, false)
        .expect("Failed to flush test segment holder");

    let segments = Arc::new(RwLock::new(holder));

    // A filter that matches no points.
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        "color".parse().unwrap(),
        Match::Value(MatchValue {
            value: ValueVariants::String("white".to_string()),
        }),
    )));

    let deleted_count =
        delete_points_by_filter(&segments.read(), DELETE_OP_NUM, &filter, &hw_counter).unwrap();
    assert_eq!(deleted_count, 0);

    let new_version = segments
        .read()
        .flush_all(FlushMode::Sync, false)
        .expect("Failed to flush test segment holder");

    // Flushing again inrceases by 1 and is now equal to `DELETE_OP_NUM` as we want to acknowledge the empty
    // delete operation in WAL.
    assert_eq!(old_version + 1, new_version);
    assert_eq!(new_version, DELETE_OP_NUM);
}

fn retrieve_raw_record(
    holder: &SegmentHolder,
    segment_id: crate::segment_holder::SegmentId,
    point_id: u64,
) -> Option<segment::data_types::segment_record::SegmentRecordRaw> {
    let hw_counter = HardwareCounterCell::new();
    let is_stopped = std::sync::atomic::AtomicBool::new(false);
    let segment = holder.get(segment_id).unwrap().get();
    let segment = segment.read();
    segment
        .retrieve_raw(
            &[point_id.into()],
            &segment::types::WithPayload::from(true),
            &segment::types::WithVector::Bool(true),
            &hw_counter,
            &is_stopped,
            common::types::DeferredBehavior::WithDeferred,
        )
        .unwrap()
        .remove(&point_id.into())
}

#[test]
fn test_upsert_points_raw_moves_point_from_non_appendable() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut non_appendable = build_segment_1(dir.path()); // points 1-5
    non_appendable.appendable_flag = false;
    let appendable = empty_segment(dir.path());

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let new_vector: Vec<f32> = vec![9.0, 8.0, 7.0, 6.0];
    let new_bytes: Vec<u8> = new_vector.iter().flat_map(|v| v.to_le_bytes()).collect();
    let payload: segment::types::Payload = payload_json! {"city": "Berlin"};

    let points = [
        PointStructRawPersisted {
            id: 1.into(),
            vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())].into(),
            payload: Some(payload.clone()),
        },
        PointStructRawPersisted {
            id: 100.into(),
            vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())].into(),
            payload: None,
        },
    ];

    let updated = upsert_points_raw(&holder, 100, points.iter(), &hw_counter).unwrap();
    assert_eq!(updated, 1);

    {
        let non_app = holder.get(sid_non_app).unwrap().get();
        let non_app = non_app.read();
        assert!(!non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred));
    }

    for point_id in [1, 100] {
        let record = retrieve_raw_record(&holder, sid_app, point_id)
            .unwrap_or_else(|| panic!("point {point_id} must be in the appendable segment"));
        let vectors = record.vectors.expect("vectors were requested");
        assert_eq!(
            vectors.to_vec(),
            vec![(DEFAULT_VECTOR_NAME.to_owned(), new_bytes.clone())],
            "raw bytes of point {point_id} must round-trip exactly",
        );
    }

    let record = retrieve_raw_record(&holder, sid_app, 1).unwrap();
    assert_eq!(record.payload, Some(payload));
    let record = retrieve_raw_record(&holder, sid_app, 100).unwrap();
    assert_eq!(record.payload.filter(|p| !p.is_empty()), None);
}

#[test]
fn test_sync_points_raw() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let segment = build_segment_1(dir.path()); // points 1-5
    let mut holder = SegmentHolder::default();
    let sid = holder.add_new(segment);

    let point_2 = PointStructRawPersisted::from(retrieve_raw_record(&holder, sid, 2).unwrap());
    let point_2_version_before = holder
        .get(sid)
        .unwrap()
        .get()
        .read()
        .point_version(2.into());

    let mut point_3 = PointStructRawPersisted::from(retrieve_raw_record(&holder, sid, 3).unwrap());
    let changed_bytes: Vec<u8> = [9.0f32, 8.0, 7.0, 6.0]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    point_3.vectors = vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes.clone())].into();

    let point_100 = PointStructRawPersisted {
        id: 100.into(),
        vectors: vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes.clone())].into(),
        payload: None,
    };

    let (deleted, new, updated) = sync_points_raw(
        &holder,
        100,
        None,
        None,
        &[point_2, point_3, point_100],
        &hw_counter,
    )
    .unwrap();

    assert_eq!(deleted, 3, "points 1, 4 and 5 are not in the sync set");
    assert_eq!(new, 1, "point 100 is new");
    assert_eq!(updated, 1, "only point 3 has different bytes");

    {
        let segment = holder.get(sid).unwrap().get();
        let segment = segment.read();
        for point_id in [1, 4, 5] {
            assert!(
                !segment.has_point(
                    point_id.into(),
                    common::types::DeferredBehavior::WithDeferred
                ),
                "point {point_id} must be deleted",
            );
        }
        assert_eq!(segment.point_version(2.into()), point_2_version_before);
        assert_eq!(segment.point_version(3.into()), Some(100));
    }

    let record = retrieve_raw_record(&holder, sid, 3).unwrap();
    assert_eq!(
        record.vectors.unwrap().to_vec(),
        vec![(DEFAULT_VECTOR_NAME.to_owned(), changed_bytes)],
    );
}

/// Helper: creates a non-appendable segment with a single point at the given version and city payload.
fn build_non_appendable_with_city(
    path: &std::path::Path,
    point_id: u64,
    version: u64,
    city: &str,
) -> segment::segment::Segment {
    let hw_counter = HardwareCounterCell::new();
    let mut seg = empty_segment(path);
    seg.upsert_point(
        version,
        point_id.into(),
        only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
        &hw_counter,
    )
    .unwrap();
    let payload: segment::types::Payload = payload_json! {"city": city.to_owned()};
    seg.set_payload(version, point_id.into(), &payload, &None, &hw_counter)
        .unwrap();
    seg.appendable_flag = false;
    seg
}

/// Helper: creates an appendable segment with deferred threshold 0 (all points deferred),
/// containing a single point with the given city payload.
fn build_deferred_with_city(
    path: &std::path::Path,
    point_id: u64,
    version: u64,
    city: &str,
) -> segment::segment::Segment {
    let hw_counter = HardwareCounterCell::new();
    // threshold 0 => every point is deferred
    let mut seg = empty_segment_with_deferred(path, 0);
    seg.upsert_point(
        version,
        point_id.into(),
        only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
        &hw_counter,
    )
    .unwrap();
    let payload: segment::types::Payload = payload_json! {"city": city.to_owned()};
    seg.set_payload(version, point_id.into(), &payload, &None, &hw_counter)
        .unwrap();
    assert!(
        seg.point_is_deferred(point_id.into()),
        "Point {point_id} should be deferred"
    );
    seg
}

fn city_filter(city: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        "city".parse().unwrap(),
        Match::Value(MatchValue {
            value: ValueVariants::String(city.to_string()),
        }),
    )))
}

/// Delete by filter with deferred points corner case:
///   - Non-appendable segment: point 1 at version 1, city=Berlin
///   - Appendable+deferred segment: point 1 at version 2, city=Amsterdam
///   - Delete by filter on city=Amsterdam
///
/// The deferred point (newest) matches the filter, so both copies must be deleted.
#[test]
fn test_delete_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let deleted = delete_points_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

    // The deferred version matches the filter => both copies deleted.
    assert!(deleted > 0, "Should have deleted at least one copy");

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        !app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy should be deleted (matches filter)"
    );
    assert!(
        !non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy should also be deleted (deferred version matched filter)"
    );
}

/// Delete by filter with deferred points corner case:
///   - Non-appendable segment: point 1 at version 1, city=Berlin
///   - Appendable+deferred segment: point 1 at version 2, city=Amsterdam
///   - Delete by filter on city=Berlin
///
/// The old copy matches the filter, but the newest version is deferred and does NOT
/// match city=Berlin. The delete must be skipped for all copies so that after
/// optimization deduplication we're left with the Amsterdam version.
#[test]
fn test_delete_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let _deleted = delete_points_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    // The deferred version (Amsterdam) does NOT match the filter (Berlin),
    // so both copies must be kept. Once the optimizer kicks in and deduplicates,
    // only the Amsterdam version will remain.
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept (does not match filter, is newest)"
    );
    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept (deferred version is newer and does not match filter)"
    );
}

// --- set_payload_by_filter deferred tests ---

/// Set payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Amsterdam (matches deferred copy)
///
/// The deferred point (newest) matches the filter, so the operation should be applied.
#[test]
fn test_set_payload_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let payload: segment::types::Payload = payload_json! {"color": "red"};
    let updated =
        set_payload_by_filter(&holder, 10, &payload, &filter, &None, &hw_counter).unwrap();

    assert!(updated > 0, "Should have updated at least one point");
}

/// Set payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Berlin (matches old copy only)
///
/// The deferred version does NOT match, so the operation must be skipped.
#[test]
fn test_set_payload_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let payload: segment::types::Payload = payload_json! {"color": "red"};
    let updated =
        set_payload_by_filter(&holder, 10, &payload, &filter, &None, &hw_counter).unwrap();

    assert_eq!(
        updated, 0,
        "Operation should be skipped (deferred version does not match filter)"
    );

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept"
    );
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept"
    );
}

// --- delete_payload_by_filter deferred tests ---

/// Delete payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Amsterdam (matches deferred copy)
///
/// The deferred point (newest) matches, so the payload key should be deleted.
#[test]
fn test_delete_payload_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let keys: Vec<PayloadKeyType> = vec!["city".parse().unwrap()];
    let updated = delete_payload_by_filter(&holder, 10, &filter, &keys, &hw_counter).unwrap();

    assert!(updated > 0, "Should have updated at least one point");
}

/// Delete payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Berlin (matches old copy only)
///
/// The deferred version does NOT match, so the operation must be skipped.
#[test]
fn test_delete_payload_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let keys: Vec<PayloadKeyType> = vec!["city".parse().unwrap()];
    let updated = delete_payload_by_filter(&holder, 10, &filter, &keys, &hw_counter).unwrap();

    assert_eq!(
        updated, 0,
        "Operation should be skipped (deferred version does not match filter)"
    );

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept"
    );
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept"
    );
}

// --- clear_payload_by_filter deferred tests ---

/// Clear payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Amsterdam (matches deferred copy)
///
/// The deferred point (newest) matches, so the payload should be cleared.
#[test]
fn test_clear_payload_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let updated = clear_payload_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

    assert!(updated > 0, "Should have updated at least one point");
}

/// Clear payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Berlin (matches old copy only)
///
/// The deferred version does NOT match, so the operation must be skipped.
#[test]
fn test_clear_payload_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let updated = clear_payload_by_filter(&holder, 10, &filter, &hw_counter).unwrap();

    assert_eq!(
        updated, 0,
        "Operation should be skipped (deferred version does not match filter)"
    );

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept"
    );
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept"
    );
}

// --- overwrite_payload_by_filter deferred tests ---

/// Overwrite payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Amsterdam (matches deferred copy)
///
/// The deferred point (newest) matches, so the payload should be overwritten.
#[test]
fn test_overwrite_payload_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let payload: segment::types::Payload = payload_json! {"color": "red"};
    let updated = overwrite_payload_by_filter(&holder, 10, &payload, &filter, &hw_counter).unwrap();

    assert!(updated > 0, "Should have updated at least one point");
}

/// Overwrite payload by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Berlin (matches old copy only)
///
/// The deferred version does NOT match, so the operation must be skipped.
#[test]
fn test_overwrite_payload_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let payload: segment::types::Payload = payload_json! {"color": "red"};
    let updated = overwrite_payload_by_filter(&holder, 10, &payload, &filter, &hw_counter).unwrap();

    assert_eq!(
        updated, 0,
        "Operation should be skipped (deferred version does not match filter)"
    );

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept"
    );
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept"
    );
}

// --- delete_vectors_by_filter deferred tests ---

/// Delete vectors by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Amsterdam (matches deferred copy)
///
/// The deferred point (newest) matches, so the vector should be deleted.
#[test]
fn test_delete_vectors_by_filter_deferred_filter_matches_deferred() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);

    let filter = city_filter("Amsterdam");
    let vector_names = vec![DEFAULT_VECTOR_NAME.into()];
    let deleted =
        delete_vectors_by_filter(&holder, 10, &filter, &vector_names, &hw_counter).unwrap();

    assert!(deleted > 0, "Should have deleted at least one vector");
}

/// Delete vectors by filter with deferred points:
///   - Non-appendable: point 1 v1, city=Berlin
///   - Deferred: point 1 v2, city=Amsterdam
///   - Filter: city=Berlin (matches old copy only)
///
/// The deferred version does NOT match, so the operation must be skipped.
#[test]
fn test_delete_vectors_by_filter_deferred_filter_matches_old_copy() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let non_appendable = build_non_appendable_with_city(dir.path(), 1, 1, "Berlin");
    let appendable = build_deferred_with_city(dir.path(), 1, 2, "Amsterdam");

    let mut holder = SegmentHolder::default();
    let sid_non_app = holder.add_new(non_appendable);
    let sid_app = holder.add_new(appendable);

    let filter = city_filter("Berlin");
    let vector_names = vec![DEFAULT_VECTOR_NAME.into()];
    let deleted =
        delete_vectors_by_filter(&holder, 10, &filter, &vector_names, &hw_counter).unwrap();

    assert_eq!(
        deleted, 0,
        "Operation should be skipped (deferred version does not match filter)"
    );

    let non_app = holder.get(sid_non_app).unwrap().get();
    let non_app = non_app.read();
    let app = holder.get(sid_app).unwrap().get();
    let app = app.read();

    assert!(
        non_app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Old copy must be kept"
    );
    assert!(
        app.has_point(1.into(), common::types::DeferredBehavior::WithDeferred),
        "Deferred copy must be kept"
    );
}

/// Upsert of a point that lives in a non-appendable segment takes the
/// CoW-move arm of `apply_points_with_conditional_move`. The moved record
/// must match in-place upsert semantics (`upsert_with_payload`): named
/// vectors and payload absent from the incoming point are dropped, not
/// carried over from the old record.
#[test]
fn test_upsert_cow_move_replaces_whole_point() {
    use std::collections::HashMap;

    use common::types::DeferredBehavior;
    use segment::data_types::named_vectors::NamedVectors;
    use segment::entry::entry_point::SegmentEntry;
    use segment::segment_constructor::simple_segment_constructor::{
        VECTOR1_NAME, VECTOR2_NAME, build_segment_with_two_named_vecs,
    };
    use segment::types::{Distance, PointIdType};

    use crate::operations::point_ops::{
        PointStructPersisted, VectorPersisted, VectorStructPersisted,
    };
    use crate::update::upsert_points;

    const DIM: usize = 4;
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let point_id: PointIdType = 7.into();

    // Old record: both named vectors plus a payload.
    let seed = |segment: &mut segment::segment::Segment| {
        segment
            .upsert_point(
                100,
                point_id,
                NamedVectors::from_pairs([
                    (VECTOR1_NAME.to_owned(), vec![0.1, 0.2, 0.3, 0.4]),
                    (VECTOR2_NAME.to_owned(), vec![0.5, 0.6, 0.7, 0.8]),
                ]),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(
                100,
                point_id,
                &payload_json! {"city": "Berlin"},
                &None,
                &hw_counter,
            )
            .unwrap();
    };

    // Incoming upsert: only `vector1`, no payload.
    let incoming = PointStructPersisted {
        id: point_id,
        vector: VectorStructPersisted::Named(HashMap::from([(
            VECTOR1_NAME.to_owned(),
            VectorPersisted::Dense(vec![1.0, 1.0, 1.0, 1.0]),
        )])),
        payload: None,
    };

    let check = |segment: &dyn SegmentEntry, path: &str| {
        assert!(segment.has_point(point_id, DeferredBehavior::WithDeferred));
        assert!(
            segment
                .vector(VECTOR1_NAME, point_id, &hw_counter)
                .unwrap()
                .is_some(),
            "{path}: upserted vector must be present",
        );
        assert!(
            segment
                .vector(VECTOR2_NAME, point_id, &hw_counter)
                .unwrap()
                .is_none(),
            "{path}: named vector absent from the upsert must be dropped",
        );
        assert!(
            segment.payload(point_id, &hw_counter).unwrap().is_empty(),
            "{path}: payload absent from the upsert must be cleared",
        );
    };

    // Oracle: in-place upsert into an appendable segment.
    let mut in_place =
        build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
    seed(&mut in_place);
    let mut holder = SegmentHolder::default();
    let sid = holder.add_new(in_place);
    upsert_points(&holder, 101, [&incoming], &hw_counter).unwrap();
    let segment = holder.get(sid).unwrap().get();
    check(&*segment.read(), "in-place");

    // CoW move: the point's segment is non-appendable, so the upsert must
    // move it into the appendable destination with the same semantics.
    let mut source =
        build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
    seed(&mut source);
    source.appendable_flag = false;
    let destination =
        build_segment_with_two_named_vecs(dir.path(), DIM, DIM, Distance::Dot).unwrap();
    let mut holder = SegmentHolder::default();
    holder.add_new(source);
    let sid = holder.add_new(destination);
    upsert_points(&holder, 101, [&incoming], &hw_counter).unwrap();
    let segment = holder.get(sid).unwrap().get();
    check(&*segment.read(), "CoW move");
}

/// Failure-direction regression test for the payload index durability fix: an
/// index built while a payload change is still pending must not let the durable
/// index config outrun the state the build observed. `create_field_index` flushes
/// the segment (serialized with the flush pipeline) before building, so the
/// pending change becomes durable together with the index: after a crash the
/// reloaded segment sees the cleared payload, the re-applied `CreateFieldIndex`
/// (WAL replay) stays a truthful no-op (`AlreadyBuilt`), and filtered reads agree
/// with full scans.
///
/// Without the pre-build flush, the reloaded segment resurrects the cleared
/// payload row while the durable index has no posting for it, and the row stays
/// invisible to filtered reads forever.
#[test]
fn create_field_index_pins_pending_payload_state() {
    use std::sync::atomic::AtomicBool;

    use common::types::DeferredBehavior;
    use segment::entry::{NonAppendableSegmentEntry as _, StorageSegmentEntry as _};
    use segment::segment_constructor::load_segment;
    use segment::types::{PayloadFieldSchema, PayloadSchemaType};
    use uuid::Uuid;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let key: PayloadKeyType = "city".parse().unwrap();
    let schema = PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword);
    let is_stopped = AtomicBool::new(false);

    let mut seg = empty_segment(dir.path());
    seg.upsert_point(
        1,
        0.into(),
        only_default_vector(&[1.0, 0.0, 0.0, 0.0]),
        &hw_counter,
    )
    .unwrap();
    let payload: segment::types::Payload = payload_json! {"city": "Berlin"};
    seg.set_payload(2, 0.into(), &payload, &None, &hw_counter)
        .unwrap();
    seg.flush(true).unwrap();

    // Pending payload clear: stays in memory until the next flush cycle.
    seg.clear_payload(3, 0.into(), &hw_counter).unwrap();
    let segment_path = seg.segment_path.clone();

    let mut holder = SegmentHolder::default();
    holder.add_new(seg);

    // The build observes the cleared row and must pin it durably before the
    // index config becomes durable.
    create_field_index(&holder, 4, &key, Some(&schema), &hw_counter).unwrap();

    // Simulated crash: dropped without any flush after the op.
    drop(holder);

    let mut segment = load_segment(&segment_path, Uuid::nil(), None, &AtomicBool::new(false))
        .expect("segment must load after simulated crash");

    // The pre-build flush persisted the pending clear together with the index.
    let reloaded = segment.payload(0.into(), &hw_counter).unwrap();
    assert!(
        !reloaded.0.contains_key("city"),
        "the payload state observed by the index build must be durable",
    );

    // WAL replay re-applies the CreateFieldIndex op; the config is truthful, so
    // `AlreadyBuilt` is a correct no-op.
    segment
        .create_field_index(4, &key, Some(&schema), &hw_counter)
        .unwrap();

    let hits = segment
        .read_filtered(
            None,
            None,
            Some(&city_filter("Berlin")),
            &is_stopped,
            &hw_counter,
            DeferredBehavior::VisibleOnly,
        )
        .unwrap();
    assert!(
        hits.is_empty(),
        "filtered reads must agree with payload storage: the row was cleared",
    );
}

/// Fixtures use dim-4 f32 vectors: 16 bytes per point.
const TEST_POINT_SIZE_BYTES: usize = 16;

#[test]
fn test_capacity_error_when_all_appendable_at_cap() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let mut non_appendable = build_segment_1(dir.path()); // points 1-5
    non_appendable.appendable_flag = false;

    // Fill the only appendable segment up to the cap of 2 points.
    let mut appendable = empty_segment(dir.path());
    for point_id in [100u64, 101] {
        appendable
            .upsert_point(
                10,
                point_id.into(),
                only_default_vector(&[1.0, 0.0, 1.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
    }

    let mut holder = SegmentHolder::default();
    holder.add_new(non_appendable);
    holder.add_new(appendable);
    holder.set_max_segment_size_bytes(NonZeroUsize::new(2 * TEST_POINT_SIZE_BYTES));

    // Setting payload on the points of the non-appendable segment needs to CoW-move them,
    // but no appendable segment is below the cap: the operation must fail with the
    // recoverable capacity error instead of growing the full destination further.
    let points: Vec<PointIdType> = (1..=5u64).map(PointIdType::from).collect();
    let err = set_payload(
        &holder,
        100,
        &payload_json! {"town": "Amsterdam"},
        &points,
        &None,
        &hw_counter,
    )
    .expect_err("no appendable segment below the cap can accept the moved points");
    assert!(
        err.is_out_of_appendable_capacity(),
        "expected OutOfAppendableCapacity, got: {err}",
    );

    // The insert path reports the same error instead of writing past the cap.
    let err = holder
        .smallest_appendable_segment()
        .expect_err("even the smallest appendable segment is at the cap");
    assert!(
        err.is_out_of_appendable_capacity(),
        "expected OutOfAppendableCapacity, got: {err}",
    );
}
