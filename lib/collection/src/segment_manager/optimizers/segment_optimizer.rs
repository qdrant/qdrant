use segment::types::{PointIdType, PayloadKeyType, SegmentConfig, Indexes, StorageType, PayloadIndexType};
use crate::collection::CollectionResult;
use crate::segment_manager::holders::segment_holder::{SegmentId, LockedSegment, LockedSegmentHolder};
use std::sync::Arc;
use segment::segment::Segment;
use std::collections::HashSet;
use crate::segment_manager::holders::proxy_segment::ProxySegment;
use segment::entry::entry_point::SegmentEntry;
use parking_lot::RwLock;
use itertools::Itertools;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use std::convert::TryInto;
use std::path::Path;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;


#[derive(Debug, Clone)]
pub struct OptimizerThresholds {
    pub memmap_threshold: usize,
    pub indexing_threshold: usize,
    pub payload_indexing_threshold: usize,
}

pub trait SegmentOptimizer {
    /// Get path of the whole collection
    fn collection_path(&self) -> &Path;

    /// Get temp path, where optimized segments could be temporary stored
    fn temp_path(&self) -> &Path;

    /// Get basic segment config
    fn base_segment_config(&self) -> SegmentConfig;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Checks if segment optimization is required
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId>;

    /// Build temp segment
    fn temp_segment(&self) -> CollectionResult<LockedSegment> {
        let config = self.base_segment_config();
        Ok(LockedSegment::new(build_simple_segment(
            self.collection_path(),
            config.vector_size,
            config.distance,
        )?))
    }

    /// Build optimized segment
    fn optimized_segment_builder(&self, optimizing_segments: &Vec<LockedSegment>) -> CollectionResult<SegmentBuilder> {
        let total_vectors: usize = optimizing_segments.iter()
            .map(|s| s.get().read().vectors_count()).sum();

        let have_indexed_fields = optimizing_segments.iter()
            .any(|s| !s.get().read().get_indexed_fields().is_empty());

        let mut optimized_config = self.base_segment_config();

        let thresholds = self.threshold_config();

        if total_vectors < thresholds.memmap_threshold {
            optimized_config.storage_type = StorageType::InMemory;
        } else {
            optimized_config.storage_type = StorageType::Mmap;
        }

        if total_vectors < thresholds.indexing_threshold {
            optimized_config.index = Indexes::Plain {};
        } else {
            optimized_config.index = match optimized_config.index {
                Indexes::Plain { } => Indexes::default_hnsw(),
                _ => optimized_config.index
            }
        }

        // Create structure index only if there is something to index
        if total_vectors < thresholds.payload_indexing_threshold || !have_indexed_fields {
            optimized_config.payload_index = Some(PayloadIndexType::Plain)
        } else {
            optimized_config.payload_index = Some(PayloadIndexType::Struct);
        }

        Ok(SegmentBuilder::new(
            self.collection_path(),
            self.temp_path(),
            &optimized_config
        )?)
    }


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
                .filter_map(|x| x.and_then(|x| Some(x.clone())))
                .collect()
        };

        let mut segment_builder = self.optimized_segment_builder(&optimizing_segments)?;

        let proxies: Vec<_> = optimizing_segments.iter()
            .map(|sg| ProxySegment::new(
                sg.clone(),
                tmp_segment.clone(),
                proxy_deleted_points.clone(),
                proxy_deleted_indexes.clone(),
                proxy_created_indexes.clone(),
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
                    segment_builder.update_from(&segment_guard)?;
                }
                LockedSegment::Proxy(_) => panic!("Attempt to optimize segment which is already currently under optimization. Should never happen"),
            }
        }

        for field in proxy_deleted_indexes.read().iter() { segment_builder.indexed_fields.remove(field); }
        for field in proxy_created_indexes.read().iter().cloned() { segment_builder.indexed_fields.insert(field); }

        let mut optimized_segment: Segment = segment_builder.try_into()?;

        // Delete points in 2 steps
        // First step - delete all points with read lock
        // Second step - delete all the rest points with full write lock
        let deleted_points_snapshot: HashSet<PointIdType> = proxy_deleted_points.read().iter().cloned().collect();

        for point_id in deleted_points_snapshot.iter().cloned() {
            optimized_segment.delete_point(
                optimized_segment.version,
                point_id,
            ).unwrap();
        }

        let deleted_indexes = proxy_deleted_indexes.read().iter().cloned().collect_vec();
        let create_indexes = proxy_created_indexes.read().iter().cloned().collect_vec();

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
}