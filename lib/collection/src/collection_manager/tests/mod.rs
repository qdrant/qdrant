use crate::collection_manager::fixtures::{build_segment_1, build_segment_2, empty_segment};
use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder};
use crate::collection_manager::segments_updater::upsert_points;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::types::{PayloadKeyType, PointIdType};
use std::collections::HashSet;
use std::sync::Arc;
use tempdir::TempDir;

#[test]
fn test_update_proxy_segments() {
    let dir = TempDir::new("segment_dir").unwrap();

    let temp_segment: LockedSegment = empty_segment(dir.path()).into();

    let segment1 = build_segment_1(dir.path());
    let segment2 = build_segment_2(dir.path());

    let mut holder = SegmentHolder::default();

    let sid1 = holder.add(segment1);
    let _sid2 = holder.add(segment2);

    let segments = Arc::new(RwLock::new(holder));

    let _proxy_id = {
        let mut write_segments = segments.write();

        let optimizing_segment = write_segments.get(sid1).unwrap();

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

        let proxy_id = write_segments.swap(proxy, &[sid1], false).unwrap();

        proxy_id
    };

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
