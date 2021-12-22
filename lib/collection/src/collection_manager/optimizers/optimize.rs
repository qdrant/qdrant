use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::operations::types::CollectionResult;
use crate::update_handler::Optimizer;
use itertools::Itertools;
use parking_lot::RwLock;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::types::{PayloadKeyType, PointIdType};
use std::collections::HashSet;
use std::sync::Arc;

/// Performs optimization of collections's segments, including:
///     - Segment rebuilding
///     - Segment joining
pub async fn optimize(
    optimizer: Arc<Optimizer>,
    segments: LockedSegmentHolder,
    ids: Vec<SegmentId>,
) -> CollectionResult<bool> {
    let tmp_segment = optimizer.temp_segment()?;

    let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
    let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
    let proxy_created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

    // Exclusive lock for the segments operations
    let mut write_segments = segments.write().await;

    let optimizing_segments: Vec<_> = ids
        .iter()
        .cloned()
        .map(|id| write_segments.get(id))
        .filter_map(|x| x.cloned())
        .collect();

    let proxies = optimizing_segments.iter().map(|sg| {
        ProxySegment::new(
            sg.clone(),
            tmp_segment.clone(),
            proxy_deleted_points.clone(),
            proxy_deleted_indexes.clone(),
            proxy_created_indexes.clone(),
        )
    });

    let proxy_ids: Vec<_> = proxies
        .zip(ids.iter().cloned())
        .map(|(proxy, idx)| write_segments.swap(proxy, &[idx], false).unwrap())
        .collect();

    // Release segments lock
    drop(write_segments);

    // Delete points in 2 steps
    // First step - delete all points with read lock
    // Second step - delete all the rest points with full write lock
    let mut processed_deleted_points_snapshot: HashSet<PointIdType> = Default::default();

    // ---- SLOW PART -----

    let locked_optimized_segment: LockedSegment = {
        let mut segment_builder = optimizer.optimized_segment_builder(&optimizing_segments)?;

        for segment in optimizing_segments {
            match segment {
                LockedSegment::Original(segment_arc) => {
                    let segment_guard = segment_arc.read();
                    segment_builder.update_from(&segment_guard)?;
                }
                LockedSegment::Proxy(_) => panic!("Attempt to optimize segment which is already currently under optimization. Should never happen"),
            }
        }

        for field in proxy_deleted_indexes.read().iter() {
            segment_builder.indexed_fields.remove(field);
        }
        for field in proxy_created_indexes.read().iter().cloned() {
            segment_builder.indexed_fields.insert(field);
        }

        let mut optimized_segment: Segment = segment_builder.try_into()?;

        let deleted_points_snapshot: HashSet<PointIdType> =
            proxy_deleted_points.read().iter().cloned().collect();

        for &point_id in &deleted_points_snapshot {
            optimized_segment
                .delete_point(optimized_segment.version(), point_id)
                .unwrap();
            processed_deleted_points_snapshot.insert(point_id);
        }

        let deleted_indexes = proxy_deleted_indexes.read().iter().cloned().collect_vec();
        let create_indexes = proxy_created_indexes.read().iter().cloned().collect_vec();

        for delete_field_name in &deleted_indexes {
            optimized_segment.delete_field_index(optimized_segment.version(), delete_field_name)?;
        }

        for create_field_name in &create_indexes {
            optimized_segment.create_field_index(optimized_segment.version(), create_field_name)?;
        }
        optimized_segment.into()
    };

    // ---- SLOW PART ENDS HERE -----

    {
        // This block locks all operations with collection. It should be fast
        let mut write_segments = segments.write().await;

        {
            // Operations with locked optimized segment. Required for proper async handling:
            // Raw segment could not be created within `.await` constructions.
            // ref: https://blog.rust-lang.org/inside-rust/2019/10/11/AsyncAwait-Not-Send-Error-Improvements.html
            let optimized_segment_lock = locked_optimized_segment.get();
            let mut optimized_segment_write = optimized_segment_lock.write();
            let deleted_points = proxy_deleted_points.read();
            let points_diff = processed_deleted_points_snapshot.difference(&deleted_points);
            for &point_id in points_diff {
                let op_num = optimized_segment_write.version();
                optimized_segment_write
                    .delete_point(op_num, point_id)
                    .unwrap();
            }

            for deleted_field_name in proxy_deleted_indexes.read().iter() {
                let op_num = optimized_segment_write.version();
                optimized_segment_write.delete_field_index(op_num, deleted_field_name)?;
            }

            for created_field_name in proxy_created_indexes.read().iter() {
                let op_num = optimized_segment_write.version();
                optimized_segment_write.create_field_index(op_num, created_field_name)?;
            }
        }

        write_segments.swap(locked_optimized_segment, &proxy_ids, true)?;

        let has_appendable_segments = write_segments.random_appendable_segment().is_some();

        // Append a temp segment to a collection if it is not empty or there is no other appendable segment
        if tmp_segment.get().read().vectors_count() > 0 || !has_appendable_segments {
            write_segments.add_locked(tmp_segment);
        } else {
            tmp_segment.drop_data()?;
        }
    }
    Ok(true)
}
