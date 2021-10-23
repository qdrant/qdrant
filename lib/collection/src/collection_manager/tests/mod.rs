use crate::collection_manager::fixtures::{build_segment_1, build_segment_2, empty_segment};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentHolder, SegmentId,
};
use crate::collection_manager::segments_updater::upsert_points;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::entry::entry_point::SegmentEntry;
use segment::types::{PayloadKeyType, PointIdType};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tempdir::TempDir;

fn wrap_proxy(segments: LockedSegmentHolder, sid: SegmentId, path: &Path) -> SegmentId {
    let mut write_segments = segments.write();

    let temp_segment: LockedSegment = empty_segment(path).into();

    let optimizing_segment = write_segments.get(sid).unwrap();

    let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
    let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
    let proxy_created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

    let proxy = ProxySegment::new(
        optimizing_segment.clone(),
        temp_segment.clone(),
        proxy_deleted_points.clone(),
        proxy_deleted_indexes.clone(),
        proxy_created_indexes.clone(),
    );

    let proxy_id = write_segments.swap(proxy, &[sid], false).unwrap();

    proxy_id
}

#[test]
fn test_update_proxy_segments() {
    let dir = TempDir::new("segment_dir").unwrap();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    let segments = Arc::new(RwLock::new(holder));

    let _proxy_id = wrap_proxy(segments.clone(), sid1, dir.path());

    let vectors = vec![vec![0.0, 0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0, 0.0]];

    for i in 1..10 {
        let ids = vec![100 * i + 1, 100 * i + 2];
        upsert_points(&segments, 1000 + i, &ids, &vectors, &None).unwrap();
    }

    let all_ids = segments
        .read()
        .iter()
        .map(|(_id, segment)| segment.get().read().read_filtered(0, 100, None))
        .flatten()
        .sorted()
        .collect_vec();

    for i in 1..10 {
        let idx = 100 * i + 1;
        assert!(all_ids.contains(&idx), "Not found {}", idx)
    }
}

#[test]
fn test_move_points_to_copy_on_write() {
    let dir = TempDir::new("segment_dir").unwrap();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    let segments = Arc::new(RwLock::new(holder));

    let proxy_id = wrap_proxy(segments.clone(), sid1, dir.path());

    let vectors = vec![vec![0.0, 0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0, 0.0]];
    upsert_points(&segments, 1001, &vec![1, 2], &vectors, &None).unwrap();

    let vectors = vec![vec![0.0, 0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0, 0.0]];
    upsert_points(&segments, 1002, &vec![2, 3], &vectors, &None).unwrap();

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

    let vector_storage = copy_on_write_segment_read.vector_storage.clone();
    let id_mapper = copy_on_write_segment_read.id_tracker.clone();

    eprintln!("copy_on_write_points = {:#?}", copy_on_write_points);

    for idx in copy_on_write_points {
        let internal = id_mapper.borrow().internal_id(idx).unwrap();
        eprintln!("{} -> {}", idx, internal);
    }

    let internal_ids = vector_storage.borrow().iter_ids().collect_vec();

    eprintln!("internal_ids = {:#?}", internal_ids);

    for idx in internal_ids {
        let external = id_mapper.borrow().external_id(idx).unwrap();
        eprintln!("{} -> {}", idx, external);
    }
}
