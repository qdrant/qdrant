use segment::types::{PointIdType, PayloadKeyType};
use crate::collection::CollectionResult;
use crate::segment_manager::holders::segment_holder::{SegmentId, LockedSegment, LockedSegmentHolder};
use std::sync::Arc;
use segment::segment::Segment;
use std::collections::HashSet;
use crate::segment_manager::holders::proxy_segment::ProxySegment;
use segment::entry::entry_point::SegmentEntry;
use parking_lot::RwLock;
use itertools::Itertools;

pub trait SegmentOptimizer {
    /// Checks if segment optimization is required
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId>;

    /// Build temp segment
    fn temp_segment(&self) -> CollectionResult<LockedSegment>;

    /// Build optimized segment
    fn optimized_segment(&self, optimizing_segments: &Vec<LockedSegment>) -> CollectionResult<Segment>;


    /// Performs optimization of collections's segments, including:
    ///     - Segment rebuilding
    ///     - Segment joining
    fn optimize(&self, segments: LockedSegmentHolder, ids: Vec<SegmentId>) -> CollectionResult<bool> {
        let tmp_segment = self.temp_segment()?;

        let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
        let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let proxy_created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

        let optimizing_segments: Vec<_> = {
            let read_segments = segments.read();
            ids.iter().cloned()
                .map(|id| read_segments.get(id))
                .filter_map(|x| x.and_then(|x| Some(x.clone()) ))
                .collect()
        };

        let mut optimized_segment = self.optimized_segment(&optimizing_segments)?;

        let proxies: Vec<_> = optimizing_segments.iter()
            .map(|sg| ProxySegment::new(
                sg.clone(),
                tmp_segment.clone(),
                proxy_deleted_points.clone(),
                proxy_deleted_indexes.clone(),
                proxy_created_indexes.clone()
            )).collect();


        let proxy_ids: Vec<_> = {
            let mut write_segments = segments.write();
            proxies.into_iter()
                .zip(ids.iter().cloned())
                .map(|(proxy, idx)| write_segments.swap(proxy, &vec![idx], false).unwrap())
                .collect()
        };

        // ---- SLOW PART -----
        for segment in optimizing_segments {
            match segment {
                LockedSegment::Original(segment_arc) => {
                    let segment_guard = segment_arc.read();
                    optimized_segment.update_from(&segment_guard)?;
                },
                LockedSegment::Proxy(_) => panic!("Attempt to optimize segment which is already currently under optimization. Should never happen"),
            }
        }

        // Delete points in 2 steps
        // First step - delete all points with read lock
        // Second step - delete all the rest points with full write lock
        let deleted_points_snapshot: HashSet<PointIdType> = proxy_deleted_points.read().iter().cloned().collect();
        let deleted_indexes = proxy_deleted_indexes.read().iter().cloned().collect_vec();
        let create_indexes = proxy_created_indexes.read().iter().cloned().collect_vec();

        for point_id in deleted_points_snapshot.iter().cloned() {
            optimized_segment.delete_point(
                optimized_segment.version,
                point_id,
            ).unwrap();
        }
        optimized_segment.finish_building()?;

        for delete_field_name in deleted_indexes.iter() {
            optimized_segment.delete_field_index(optimized_segment.version, delete_field_name)?;
        }

        for create_field_name in create_indexes.iter() {
            optimized_segment.create_field_index(optimized_segment.version, create_field_name)?;
        }
        // ---- SLOW PART ENDS HERE -----

        { // This block locks all operations with collection. It should be fast
            let mut write_segments = segments.write();
            let deleted_points = proxy_deleted_points.read();
            let points_diff = deleted_points_snapshot.difference(&deleted_points);
            for point_id in points_diff.into_iter() {
                optimized_segment.delete_point(
                    optimized_segment.version,
                    *point_id,
                ).unwrap();
            }

            for deleted_field_name in proxy_deleted_indexes.read().iter() {
                optimized_segment.delete_field_index(optimized_segment.version, deleted_field_name)?;
            }

            for created_field_name in proxy_created_indexes.read().iter() {
                optimized_segment.create_field_index(optimized_segment.version, created_field_name)?;
            }

            write_segments.swap(optimized_segment, &proxy_ids, true)?;
            if tmp_segment.get().read().vectors_count() > 0 { // Do not add temporary segment if no points changed
                write_segments.add_locked(tmp_segment);
            } else {
                tmp_segment.drop_data()?;
            }
        }
        Ok(true)
    }
}