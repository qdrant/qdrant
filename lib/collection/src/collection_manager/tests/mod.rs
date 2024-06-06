mod test_search_aggregation;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use segment::data_types::vectors::{only_default_vector, VectorStruct};
use segment::entry::entry_point::SegmentEntry;
use segment::types::{PayloadFieldSchema, PayloadKeyType, PointIdType};
use tempfile::Builder;

use crate::collection_manager::fixtures::{build_segment_1, build_segment_2, empty_segment};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentHolder, SegmentId,
};
use crate::collection_manager::segments_updater::upsert_points;
use crate::operations::point_ops::PointStruct;

fn wrap_proxy(segments: LockedSegmentHolder, sid: SegmentId, path: &Path) -> SegmentId {
    let mut write_segments = segments.write();

    let temp_segment: LockedSegment = empty_segment(path).into();

    let optimizing_segment = write_segments.get(sid).unwrap().clone();

    let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
    let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
    let proxy_created_indexes = Arc::new(RwLock::new(
        HashMap::<PayloadKeyType, PayloadFieldSchema>::new(),
    ));

    let proxy = ProxySegment::new(
        optimizing_segment,
        temp_segment,
        proxy_deleted_points,
        proxy_created_indexes,
        proxy_deleted_indexes,
    );

    let (new_id, _replaced_segments) = write_segments.swap(proxy, &[sid]);
    new_id
}

#[test]
fn test_update_proxy_segments() {
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
            PointStruct {
                id: (100 * i + 1).into(),
                vector: VectorStruct::from(vectors[0].clone()).into(),
                payload: None,
            },
            PointStruct {
                id: (100 * i + 2).into(),
                vector: VectorStruct::from(vectors[1].clone()).into(),
                payload: None,
            },
        ];
        upsert_points(&segments.read(), 1000 + i, &points).unwrap();
    }

    let all_ids = segments
        .read()
        .iter()
        .flat_map(|(_id, segment)| segment.get().read().read_filtered(None, Some(100), None))
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
        PointStruct {
            id: 1.into(),
            vector: VectorStruct::from(vec![0.0, 0.0, 0.0, 0.0]).into(),
            payload: None,
        },
        PointStruct {
            id: 2.into(),
            vector: VectorStruct::from(vec![0.0, 0.0, 0.0, 0.0]).into(),
            payload: None,
        },
    ];

    upsert_points(&segments.read(), 1001, &points).unwrap();

    let points = vec![
        PointStruct {
            id: 2.into(),
            vector: VectorStruct::from(vec![0.0, 0.0, 0.0, 0.0]).into(),
            payload: None,
        },
        PointStruct {
            id: 3.into(),
            vector: VectorStruct::from(vec![0.0, 0.0, 0.0, 0.0]).into(),
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
