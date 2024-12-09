use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use segment::data_types::vectors::{only_default_vector, VectorStructInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::types::{Payload, PointIdType, WithPayload, WithVector};
use serde_json::json;
use tempfile::Builder;

use super::holders::proxy_segment;
use crate::collection_manager::fixtures::{build_segment_1, build_segment_2, empty_segment};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentHolder, SegmentId,
};
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::collection_manager::segments_updater::{set_payload, upsert_points};
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
        upsert_points(&segments.read(), 1000 + i, &points).unwrap();
    }

    let all_ids = segments
        .read()
        .iter()
        .flat_map(|(_id, segment)| {
            segment
                .get()
                .read()
                .read_filtered(None, Some(100), None, &is_stopped)
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

    upsert_points(&segments.read(), 1001, &points).unwrap();

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

    upsert_points(&segments.read(), 1002, &points).unwrap();

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

    // Fill segment 1 and 2 to the capacity
    for point_id in 0..100 {
        segment1
            .upsert_point(
                20,
                point_id.into(),
                only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
            )
            .unwrap();
        segment2
            .upsert_point(
                20,
                (100 + point_id).into(),
                only_default_vector(&[0.0, 0.0, 0.0, 0.0]),
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
    upsert_points(&segments.read(), 1000, &points).unwrap();

    // Segment 1 and 2 are over capacity, we expect to have the new points in segment 3
    {
        let segment3 = segments.read().get(sid3).unwrap().get();
        let segment3_read = segment3.read();
        for point_id in 1000..1010 {
            assert!(segment3_read.has_point(point_id.into()));
        }
        for point_id in 0..10 {
            assert!(!segment3_read.has_point(point_id.into()));
        }
    }
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

    let old_payload: Payload = json!({ "size": vec!["small"] }).into();
    let new_payload: Payload = json!({ "size": vec!["big"] }).into();
    // let newest_vec = vec![1.0, 1.0, 0.0, 0.0];

    let write_segment = LockedSegment::new(empty_segment(dir.path()));

    let idx1 = PointIdType::from(1);
    let idx2 = PointIdType::from(2);

    segment1
        .upsert_point(10, idx1, only_default_vector(&old_vec))
        .unwrap();
    segment1.set_payload(10, idx1, &old_payload, &None).unwrap();
    segment1
        .upsert_point(20, idx2, only_default_vector(&new_vec))
        .unwrap();
    segment1.set_payload(20, idx2, &new_payload, &None).unwrap();

    segment2
        .upsert_point(20, idx1, only_default_vector(&new_vec))
        .unwrap();
    segment2.set_payload(20, idx1, &new_payload, &None).unwrap();
    segment2
        .upsert_point(10, idx2, only_default_vector(&old_vec))
        .unwrap();
    segment2.set_payload(10, idx2, &old_payload, &None).unwrap();

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
        write_segment,
        deleted_points,
        changed_indexes,
    );

    let mut holder = SegmentHolder::default();

    holder.add_new(proxy_segment_1);
    holder.add_new(proxy_segment_2);

    let payload: Payload = json!({ "color": vec!["yellow"] }).into();

    let ids = vec![idx1, idx2];

    set_payload(&holder, 30, &payload, &ids, &None).unwrap();

    let locked_holder = Arc::new(RwLock::new(holder));

    let is_stopped = AtomicBool::new(false);

    let with_payload = WithPayload::from(true);
    let with_vector = WithVector::from(true);

    let result = SegmentsSearcher::retrieve_blocking(
        locked_holder.clone(),
        &ids,
        &with_payload,
        &with_vector,
        &is_stopped,
    )
    .unwrap();

    let expected_payload: Payload = json!({ "size": vec!["big"], "color": vec!["yellow"] }).into();

    for (point_id, record) in result {
        if let Some(payload) = record.payload {
            assert_eq!(payload, expected_payload);
        } else {
            panic!("No payload for point_id = {point_id}");
        }
    }
}
