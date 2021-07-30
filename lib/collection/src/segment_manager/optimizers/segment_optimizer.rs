use crate::config::CollectionParams;
use crate::operations::types::CollectionResult;
use crate::segment_manager::holders::proxy_segment::ProxySegment;
use crate::segment_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use itertools::Itertools;
use parking_lot::RwLock;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    HnswConfig, Indexes, PayloadIndexType, PayloadKeyType, PointIdType, SegmentConfig, StorageType,
};
use std::collections::HashSet;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

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
    fn collection_params(&self) -> CollectionParams;

    /// Get HNSW config
    fn hnsw_config(&self) -> HnswConfig;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Checks if segment optimization is required
    fn check_condition(&self, segments: LockedSegmentHolder) -> Vec<SegmentId>;

    /// Build temp segment
    fn temp_segment(&self) -> CollectionResult<LockedSegment> {
        let collection_params = self.collection_params();
        let config = SegmentConfig {
            vector_size: collection_params.vector_size,
            distance: collection_params.distance,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
        };
        Ok(LockedSegment::new(build_simple_segment(
            self.collection_path(),
            config.vector_size,
            config.distance,
        )?))
    }

    /// Build optimized segment
    fn optimized_segment_builder(
        &self,
        optimizing_segments: &[LockedSegment],
    ) -> CollectionResult<SegmentBuilder> {
        let total_vectors: usize = optimizing_segments
            .iter()
            .map(|s| s.get().read().vectors_count())
            .sum();

        let have_indexed_fields = optimizing_segments
            .iter()
            .any(|s| !s.get().read().get_indexed_fields().is_empty());

        let thresholds = self.threshold_config();
        let collection_params = self.collection_params();

        let is_indexed = total_vectors >= thresholds.indexing_threshold;

        // Create structure index only if there is something to index
        let is_payload_indexed =
            total_vectors >= thresholds.payload_indexing_threshold && have_indexed_fields;

        let is_on_disk = total_vectors >= thresholds.memmap_threshold;

        let optimized_config = SegmentConfig {
            vector_size: collection_params.vector_size,
            distance: collection_params.distance,
            index: if is_indexed {
                Indexes::Hnsw(self.hnsw_config())
            } else {
                Indexes::Plain {}
            },
            payload_index: Some(if is_payload_indexed {
                PayloadIndexType::Struct
            } else {
                PayloadIndexType::Plain
            }),
            storage_type: if is_on_disk {
                StorageType::Mmap
            } else {
                StorageType::InMemory
            },
        };

        Ok(SegmentBuilder::new(
            self.collection_path(),
            self.temp_path(),
            &optimized_config,
        )?)
    }

    /// Performs optimization of collections's segments, including:
    ///     - Segment rebuilding
    ///     - Segment joining
    fn optimize(
        &self,
        segments: LockedSegmentHolder,
        ids: Vec<SegmentId>,
    ) -> CollectionResult<bool> {
        let tmp_segment = self.temp_segment()?;

        let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
        let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let proxy_created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

        let optimizing_segments: Vec<_> = {
            let read_segments = segments.read();
            ids.iter()
                .cloned()
                .map(|id| read_segments.get(id))
                .filter_map(|x| x.cloned())
                .collect()
        };

        let mut segment_builder = self.optimized_segment_builder(&optimizing_segments)?;

        let proxies = optimizing_segments.iter().map(|sg| {
            ProxySegment::new(
                sg.clone(),
                tmp_segment.clone(),
                proxy_deleted_points.clone(),
                proxy_deleted_indexes.clone(),
                proxy_created_indexes.clone(),
            )
        });

        let proxy_ids: Vec<_> = {
            let mut write_segments = segments.write();
            proxies
                .zip(ids.iter().cloned())
                .map(|(proxy, idx)| write_segments.swap(proxy, &[idx], false).unwrap())
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

        for field in proxy_deleted_indexes.read().iter() {
            segment_builder.indexed_fields.remove(field);
        }
        for field in proxy_created_indexes.read().iter().cloned() {
            segment_builder.indexed_fields.insert(field);
        }

        let mut optimized_segment: Segment = segment_builder.try_into()?;

        // Delete points in 2 steps
        // First step - delete all points with read lock
        // Second step - delete all the rest points with full write lock
        let deleted_points_snapshot: HashSet<PointIdType> =
            proxy_deleted_points.read().iter().cloned().collect();

        for point_id in deleted_points_snapshot.iter().cloned() {
            optimized_segment
                .delete_point(optimized_segment.version, point_id)
                .unwrap();
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

        {
            // This block locks all operations with collection. It should be fast
            let mut write_segments = segments.write();
            let deleted_points = proxy_deleted_points.read();
            let points_diff = deleted_points_snapshot.difference(&deleted_points);
            for point_id in points_diff.into_iter() {
                optimized_segment
                    .delete_point(optimized_segment.version, *point_id)
                    .unwrap();
            }

            for deleted_field_name in proxy_deleted_indexes.read().iter() {
                optimized_segment
                    .delete_field_index(optimized_segment.version, deleted_field_name)?;
            }

            for created_field_name in proxy_created_indexes.read().iter() {
                optimized_segment
                    .create_field_index(optimized_segment.version, created_field_name)?;
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
