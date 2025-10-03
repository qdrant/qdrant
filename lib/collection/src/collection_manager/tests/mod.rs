use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::data_types::vectors::{VectorStructInternal, only_default_vector};
use segment::entry::entry_point::SegmentEntry;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::types::{ExtendedPointId, PayloadContainer, PointIdType, WithPayload, WithVector};
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::retrieve_blocking;
use shard::update::{delete_points, set_payload, upsert_points};
use tempfile::Builder;

use super::holders::proxy_segment;
use crate::collection_manager::fixtures::{build_segment_1, build_segment_2, empty_segment};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentHolder, SegmentId,
};
use crate::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

mod test_search_aggregation;

fn wrap_proxy(segments: LockedSegmentHolder, sid: SegmentId, path: &Path) -> SegmentId {
    let mut write_segments = segments.write();

    let temp_segment: LockedSegment = empty_segment(path).into();

    let optimizing_segment = write_segments.get(sid).unwrap().clone();

    let proxy = ProxySegment::new(
        optimizing_segment,
        temp_segment,
        proxy_segment::LockedRmSet::default(),
        proxy_segment::LockedIndexChanges::default(),
    );

    let (new_id, _replaced_segments) = write_segments.swap_new(proxy, &[sid]);
    new_id
}

#[test]
fn test_update_proxy_segments() {
    let is_stopped = AtomicBool::new(false);
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);

    let segments = Arc::new(RwLock::new(holder));

    let _proxy_id = wrap_proxy(segments.clone(), sid1, dir.path());

    let vectors = vec![
        only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
        only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
    ];

    let hw_counter = HardwareCounterCell::new();

    for i in 1..10 {
        let points = vec![
            PointStructPersisted {
                id: (100 * i + 1).into(),
                vector: VectorStructPersisted::from(VectorStructInternal::from(vectors[0].clone())),
                payload: None,
            },
            PointStructPersisted {
                id: (100 * i + 2).into(),
                vector: VectorStructPersisted::from(VectorStructInternal::from(vectors[1].clone())),
                payload: None,
            },
        ];
        upsert_points(&segments.read(), 1000 + i, &points, &hw_counter).unwrap();
    }

    let all_ids = segments
        .read()
        .iter()
        .flat_map(|(_id, segment)| {
            segment
                .get()
                .read()
                .read_filtered(None, Some(100), None, &is_stopped, &hw_counter)
        })
        .sorted()
        .collect_vec();

    for i in 1..10 {
        let idx = 100 * i + 1;
        assert!(all_ids.contains(&idx.into()), "Not found {idx}")
    }
}

#[test]
fn test_move_points_to_copy_on_write() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);

    let segments = Arc::new(RwLock::new(holder));

    let proxy_id = wrap_proxy(segments.clone(), sid1, dir.path());

    let hw_counter = HardwareCounterCell::new();

    let points = vec![
        PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::from(vec![0.0, 0.0, 0.0, 0.0]),
            payload: None,
        },
        PointStructPersisted {
            id: 2.into(),
            vector: VectorStructPersisted::from(vec![0.0, 0.0, 0.0, 0.0]),
            payload: None,
        },
    ];

    upsert_points(&segments.read(), 1001, &points, &hw_counter).unwrap();

    let points = vec![
        PointStructPersisted {
            id: 2.into(),
            vector: VectorStructPersisted::from(vec![0.0, 0.0, 0.0, 0.0]),
            payload: None,
        },
        PointStructPersisted {
            id: 3.into(),
            vector: VectorStructPersisted::from(vec![0.0, 0.0, 0.0, 0.0]),
            payload: None,
        },
    ];

    upsert_points(&segments.read(), 1002, &points, &hw_counter).unwrap();

    let segments_write = segments.write();

    let locked_proxy = match segments_write.get(proxy_id).unwrap() {
        LockedSegment::Original(_) => panic!("wrong type"),
        LockedSegment::Proxy(locked_proxy) => locked_proxy,
    };

    let read_proxy = locked_proxy.read();

    let copy_on_write_segment = match read_proxy.write_segment.clone() {
        LockedSegment::Original(locked_segment) => locked_segment,
        LockedSegment::Proxy(_) => panic!("wrong type"),
    };

    let copy_on_write_segment_read = copy_on_write_segment.read();

    let copy_on_write_points = copy_on_write_segment_read.iter_points().collect_vec();

    let id_mapper = copy_on_write_segment_read.id_tracker.clone();

    eprintln!("copy_on_write_points = {copy_on_write_points:#?}");

    for idx in copy_on_write_points {
        let internal = id_mapper.borrow().internal_id(idx).unwrap();
        eprintln!("{idx} -> {internal}");
    }

    let id_tracker = copy_on_write_segment_read.id_tracker.clone();
    let internal_ids = id_tracker.borrow().iter_ids().collect_vec();

    eprintln!("internal_ids = {internal_ids:#?}");

    for idx in internal_ids {
        let external = id_mapper.borrow().external_id(idx).unwrap();
        eprintln!("{idx} -> {external}");
    }
}

#[test]
fn test_upsert_points_in_smallest_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = build_segment_1(dir.path());
    let mut segment2 = build_segment_2(dir.path());
    let segment3 = empty_segment(dir.path());

    let hw_counter = HardwareCounterCell::new();

    // Fill segment 1 and 2 to the capacity
    for point_id in 0..100 {
        segment1
            .upsert_point(
                20,
                point_id.into(),
                only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
        segment2
            .upsert_point(
                20,
                (100 + point_id).into(),
                only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
                &hw_counter,
            )
            .unwrap();
    }

    let mut holder = SegmentHolder::default();

    let _sid1 = holder.add_new(segment1);
    let _sid2 = holder.add_new(segment2);
    let sid3 = holder.add_new(segment3);

    let segments = Arc::new(RwLock::new(holder));

    let points: Vec<_> = (1000..1010)
        .map(|id| PointStructPersisted {
            id: id.into(),
            vector: VectorStructPersisted::from(VectorStructInternal::from(vec![
                0.0, 0.0, 0.0, 0.0,
            ])),
            payload: None,
        })
        .collect();
    upsert_points(&segments.read(), 1000, &points, &hw_counter).unwrap();

    // Segment 1 and 2 are over capacity, we expect to have the new points in segment 3
    {
        let segment3 = segments.read();
        let segment3_read = segment3.get(sid3).unwrap().get().read();
        for point_id in 1000..1010 {
            assert!(segment3_read.has_point(point_id.into()));
        }
        for point_id in 0..10 {
            assert!(!segment3_read.has_point(point_id.into()));
        }
    }
}

/// Test that a delete operation deletes all point versions.
///
/// See: <https://github.com/qdrant/qdrant/pull/5956>
#[test]
fn test_delete_all_point_versions() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();

    let point_id = ExtendedPointId::from(123);
    let old_vector = vec![0.0, 1.0, 2.0, 3.0];
    let new_vector = vec![3.0, 2.0, 1.0, 0.0];

    let mut segment1 = empty_segment(dir.path());
    let mut segment2 = empty_segment(dir.path());

    // Insert point 123 in both segments, having version 100 and 101
    segment1
        .upsert_point(
            100,
            point_id,
            segment::data_types::vectors::only_default_vector(&old_vector),
            &hw_counter,
        )
        .unwrap();
    segment2
        .upsert_point(
            101,
            point_id,
            segment::data_types::vectors::only_default_vector(&new_vector),
            &hw_counter,
        )
        .unwrap();

    // Set up locked segment holder
    let mut holder = SegmentHolder::default();
    let sid1 = holder.add_new(segment1);
    let sid2 = holder.add_new(segment2);
    let segments = Arc::new(RwLock::new(holder));

    // We should be able to retrieve point 123
    let retrieved = retrieve_blocking(
        segments.clone(),
        &[point_id],
        &WithPayload::from(false),
        &WithVector::from(true),
        &AtomicBool::new(false),
        HwMeasurementAcc::new(),
    )
    .unwrap();
    assert_eq!(
        retrieved,
        AHashMap::from([(
            point_id,
            RecordInternal {
                id: point_id,
                vector: Some(VectorStructInternal::Single(new_vector)),
                payload: None,
                shard_key: None,
                order_value: None,
            }
        )])
    );

    {
        // Assert that point 123 is in both segments
        let holder = segments.read();
        assert!(holder.get(sid1).unwrap().get().read().has_point(point_id));
        assert!(holder.get(sid2).unwrap().get().read().has_point(point_id));

        // Delete point 123
        delete_points(&holder, 102, &[123.into()], &hw_counter).unwrap();

        // Assert that point 123 is deleted from both segments
        // Note: before the bug fix the point was only deleted from segment 2
        assert!(!holder.get(sid1).unwrap().get().read().has_point(point_id));
        assert!(!holder.get(sid2).unwrap().get().read().has_point(point_id));
    }

    // Drop the last segment, only keep the first
    // Pretend the segment was picked up by the optimizer, and was totally optimized away
    let removed_segments = segments.write().remove(&[sid2]);
    assert_eq!(removed_segments.len(), 1);

    // We must not be able to retrieve point 123
    // Note: before the bug fix we could retrieve the point again from segment 1
    let retrieved = retrieve_blocking(
        segments.clone(),
        &[point_id],
        &WithPayload::from(false),
        &WithVector::from(false),
        &AtomicBool::new(false),
        HwMeasurementAcc::new(),
    )
    .unwrap();
    assert!(retrieved.is_empty());
}

#[test]
fn test_proxy_shared_updates() {
    // Testing that multiple proxies that share point with the same id but different versions
    // are able to successfully apply and resolve update operation.

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = empty_segment(dir.path());
    let mut segment2 = empty_segment(dir.path());

    let old_vec = vec![1.0, 0.0, 0.0, 1.0];
    let new_vec = vec![1.0, 0.0, 1.0, 0.0];

    let old_payload = payload_json! {"size": vec!["small"]};
    let new_payload = payload_json! {"size": vec!["big"]};

    let write_segment = LockedSegment::new(empty_segment(dir.path()));

    let idx1 = PointIdType::from(1);
    let idx2 = PointIdType::from(2);

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(10, idx1, only_default_vector(&old_vec), &hw_counter)
        .unwrap();
    segment1
        .set_payload(10, idx1, &old_payload, &None, &hw_counter)
        .unwrap();
    segment1
        .upsert_point(20, idx2, only_default_vector(&new_vec), &hw_counter)
        .unwrap();
    segment1
        .set_payload(20, idx2, &new_payload, &None, &hw_counter)
        .unwrap();

    segment2
        .upsert_point(20, idx1, only_default_vector(&new_vec), &hw_counter)
        .unwrap();
    segment2
        .set_payload(20, idx1, &new_payload, &None, &hw_counter)
        .unwrap();
    segment2
        .upsert_point(10, idx2, only_default_vector(&old_vec), &hw_counter)
        .unwrap();
    segment2
        .set_payload(10, idx2, &old_payload, &None, &hw_counter)
        .unwrap();

    let deleted_points = proxy_segment::LockedRmSet::default();
    let changed_indexes = proxy_segment::LockedIndexChanges::default();

    let locked_segment_1 = LockedSegment::new(segment1);
    let locked_segment_2 = LockedSegment::new(segment2);

    let proxy_segment_1 = ProxySegment::new(
        locked_segment_1,
        write_segment.clone(),
        Arc::clone(&deleted_points),
        Arc::clone(&changed_indexes),
    );

    let proxy_segment_2 = ProxySegment::new(
        locked_segment_2,
        write_segment.clone(),
        deleted_points,
        changed_indexes,
    );

    let mut holder = SegmentHolder::default();

    let proxy_1_id = holder.add_new(proxy_segment_1);
    let proxy_2_id = holder.add_new(proxy_segment_2);

    let payload = payload_json! {"color": vec!["yellow"]};

    let ids = vec![idx1, idx2];

    set_payload(&holder, 30, &payload, &ids, &None, &hw_counter).unwrap();

    // Points should still be accessible in both proxies through write segment
    for &point_id in &ids {
        assert!(
            holder
                .get(proxy_1_id)
                .unwrap()
                .get()
                .read()
                .has_point(point_id),
        );
        assert!(
            holder
                .get(proxy_2_id)
                .unwrap()
                .get()
                .read()
                .has_point(point_id),
        );
    }

    let locked_holder = Arc::new(RwLock::new(holder));

    let is_stopped = AtomicBool::new(false);

    let with_payload = WithPayload::from(true);
    let with_vector = WithVector::from(true);

    let result = retrieve_blocking(
        locked_holder.clone(),
        &ids,
        &with_payload,
        &with_vector,
        &is_stopped,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    assert_eq!(
        result.keys().copied().collect::<HashSet<_>>(),
        HashSet::from_iter(ids),
        "must retrieve all point IDs",
    );

    let expected_payload = payload_json! {"size": vec!["big"], "color": vec!["yellow"]};

    for (point_id, record) in result {
        let payload = record
            .payload
            .unwrap_or_else(|| panic!("No payload for point_id = {point_id}"));

        assert_eq!(payload, expected_payload);
    }
}

#[test]
fn test_proxy_shared_updates_same_version() {
    // Testing that multiple proxies that share point with the same id but the same versions
    // are able to successfully apply and resolve update operation.
    //
    // It is undefined which instance of the point is picked if they have the exact same version.
    // What we can check is that at least one instance of the point is selected, and that we don't
    // accidentally lose points.
    // This is especially important with merging <https://github.com/qdrant/qdrant/pull/5962>.

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut segment1 = empty_segment(dir.path());
    let mut segment2 = empty_segment(dir.path());

    let old_vec = vec![1.0, 0.0, 0.0, 1.0];
    let new_vec = vec![1.0, 0.0, 1.0, 0.0];

    let old_payload = payload_json! {"size": "small"};
    let new_payload = payload_json! {"size": "big"};

    let write_segment = LockedSegment::new(empty_segment(dir.path()));

    let idx1 = PointIdType::from(1);
    let idx2 = PointIdType::from(2);

    let hw_counter = HardwareCounterCell::new();

    segment1
        .upsert_point(10, idx1, only_default_vector(&old_vec), &hw_counter)
        .unwrap();
    segment1
        .set_payload(10, idx1, &old_payload, &None, &hw_counter)
        .unwrap();
    segment1
        .upsert_point(10, idx2, only_default_vector(&new_vec), &hw_counter)
        .unwrap();
    segment1
        .set_payload(10, idx2, &new_payload, &None, &hw_counter)
        .unwrap();

    segment2
        .upsert_point(10, idx1, only_default_vector(&new_vec), &hw_counter)
        .unwrap();
    segment2
        .set_payload(10, idx1, &new_payload, &None, &hw_counter)
        .unwrap();
    segment2
        .upsert_point(10, idx2, only_default_vector(&old_vec), &hw_counter)
        .unwrap();
    segment2
        .set_payload(10, idx2, &old_payload, &None, &hw_counter)
        .unwrap();

    let deleted_points = proxy_segment::LockedRmSet::default();
    let changed_indexes = proxy_segment::LockedIndexChanges::default();

    let locked_segment_1 = LockedSegment::new(segment1);
    let locked_segment_2 = LockedSegment::new(segment2);

    let proxy_segment_1 = ProxySegment::new(
        locked_segment_1,
        write_segment.clone(),
        Arc::clone(&deleted_points),
        Arc::clone(&changed_indexes),
    );

    let proxy_segment_2 = ProxySegment::new(
        locked_segment_2,
        write_segment.clone(),
        deleted_points,
        changed_indexes,
    );

    let mut holder = SegmentHolder::default();

    let proxy_1_id = holder.add_new(proxy_segment_1);
    let proxy_2_id = holder.add_new(proxy_segment_2);

    let payload = payload_json! {"color": "yellow"};

    let ids = vec![idx1, idx2];

    set_payload(&holder, 20, &payload, &ids, &None, &hw_counter).unwrap();

    // Points should still be accessible in both proxies through write segment
    for &point_id in &ids {
        assert!(
            holder
                .get(proxy_1_id)
                .unwrap()
                .get()
                .read()
                .has_point(point_id),
        );
        assert!(
            holder
                .get(proxy_2_id)
                .unwrap()
                .get()
                .read()
                .has_point(point_id),
        );
    }

    let locked_holder = Arc::new(RwLock::new(holder));

    let is_stopped = AtomicBool::new(false);

    let with_payload = WithPayload::from(true);
    let with_vector = WithVector::from(true);

    let result = retrieve_blocking(
        locked_holder.clone(),
        &ids,
        &with_payload,
        &with_vector,
        &is_stopped,
        HwMeasurementAcc::new(),
    )
    .unwrap();

    assert_eq!(
        result.keys().copied().collect::<HashSet<_>>(),
        HashSet::from_iter(ids),
        "must retrieve all point IDs",
    );

    for (point_id, record) in result {
        let payload = record
            .payload
            .unwrap_or_else(|| panic!("No payload for point_id = {point_id}"));

        dbg!(&payload);

        let color = payload.get_value(&JsonPath::new("color"));
        assert_eq!(color.len(), 1);
        let color = color[0];
        assert_eq!(color.as_str(), Some("yellow"));

        let size = payload.get_value(&JsonPath::new("size"));
        assert_eq!(size.len(), 1);
        let size = size[0];
        assert!(["small", "big"].contains(&size.as_str().unwrap()));
    }
}
