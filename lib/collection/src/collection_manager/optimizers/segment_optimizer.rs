use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::config::CollectionParams;
use crate::operations::types::{CollectionError, CollectionResult};
use itertools::Itertools;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use segment::entry::entry_point::SegmentEntry;
use segment::segment::Segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    HnswConfig, Indexes, PayloadIndexType, PayloadKeyType, PointIdType, SegmentConfig, StorageType,
};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OptimizerThresholds {
    pub memmap_threshold: usize,
    pub indexing_threshold: usize,
    pub payload_indexing_threshold: usize,
}

/// SegmentOptimizer - trait implementing common functionality of the optimizers
///
/// It provides functions which allow to re-build specified segments into a new, better one.
/// Process allows read and write (with some tricks) access to the optimized segments.
///
/// Process of the optimization is same for all optimizers.
/// The selection of the candidates for optimization and the configuration
/// of resulting segment are up to concrete implementations.
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
    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId>;

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

    /// Restores original segments from proxies
    ///
    /// # Arguments
    ///
    /// * `segments` - segment holder
    /// * `proxy_ids` - ids of poxy-wrapped segment to restore
    ///
    /// # Result
    ///
    /// Original segments are pushed into `segments`, proxies removed.
    /// Returns IDs on restored segments
    ///
    fn unwrap_proxy(
        &self,
        segments: &LockedSegmentHolder,
        proxy_ids: &[SegmentId],
    ) -> CollectionResult<Vec<SegmentId>> {
        let mut segments_lock = segments.write();
        let mut restored_segment_ids = vec![];
        for &proxy_id in proxy_ids {
            if let Some(proxy_segment_ref) = segments_lock.get(proxy_id) {
                let locked_proxy_segment = proxy_segment_ref.clone();
                match locked_proxy_segment {
                    LockedSegment::Original(_) => {
                        /* Already unwrapped. It should not actually be here */
                        log::warn!("Attempt to unwrap raw segment! Should not happen.")
                    }
                    LockedSegment::Proxy(proxy_segment) => {
                        let wrapped_segment = proxy_segment.read().wrapped_segment.clone();
                        restored_segment_ids.push(segments_lock.swap(
                            wrapped_segment,
                            &[proxy_id],
                            false,
                        )?);
                    }
                }
            }
        }
        Ok(restored_segment_ids)
    }

    /// Checks if optimization cancellation is requested.
    fn check_cancellation(&self, stopped: &AtomicBool) -> CollectionResult<()> {
        if stopped.load(Ordering::Relaxed) {
            return Err(CollectionError::Cancelled {
                description: "optimization cancelled by service".to_string(),
            });
        }
        Ok(())
    }

    /// Unwraps proxy, adds temp segment into collection and returns a `Cancelled` error.
    ///
    /// # Arguments
    ///
    /// * `segments` - all registered segments of the collection
    /// * `proxy_ids` - currently used proxies
    /// * `temp_segment` - currently used temporary segment
    ///
    /// # Result
    ///
    /// Rolls back back optimization state.
    /// All processed changes will still be there, but the collection should be returned
    /// into state before optimization.
    ///
    fn handle_cancellation(
        &self,
        segments: &LockedSegmentHolder,
        proxy_ids: &[SegmentId],
        temp_segment: &LockedSegment,
    ) -> CollectionResult<()> {
        self.unwrap_proxy(segments, proxy_ids)?;
        if temp_segment.get().read().vectors_count() > 0 {
            let mut write_segments = segments.write();
            write_segments.add_locked(temp_segment.clone());
        }
        Ok(())
    }

    /// Function to wrap slow part of optimization. Performs proxy rollback in case of cancellation.
    /// Warn: this function might be _VERY_ CPU intensive,
    /// so it is necessary to avoid any locks inside this part of the code
    ///
    /// # Arguments
    ///
    /// * `optimizing_segments` - Segments to optimize
    /// * `proxy_deleted_points` - Holds a set of points, deleted while optimization was running
    /// * `proxy_deleted_indexes` - Holds a set of Indexes, deleted while optimization was running
    /// * `proxy_created_indexes` - Holds a set of Indexes, created while optimization was running
    /// * `stopped` - flag to check if optimization was cancelled by external thread
    ///
    /// # Result
    ///
    /// Constructs optimized segment
    fn build_new_segment(
        &self,
        optimizing_segments: &[LockedSegment],
        proxy_deleted_points: Arc<RwLock<HashSet<PointIdType>>>,
        proxy_deleted_indexes: Arc<RwLock<HashSet<PayloadKeyType>>>,
        proxy_created_indexes: Arc<RwLock<HashSet<PayloadKeyType>>>,
        stopped: &AtomicBool,
    ) -> CollectionResult<Segment> {
        let mut segment_builder = self.optimized_segment_builder(optimizing_segments)?;

        self.check_cancellation(stopped)?;

        for segment in optimizing_segments {
            match segment {
                LockedSegment::Original(segment_arc) => {
                    let segment_guard = segment_arc.read();
                    segment_builder.update_from(&segment_guard, stopped)?;
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

        let mut optimized_segment: Segment = segment_builder.build(stopped)?;

        // Delete points in 2 steps
        // First step - delete all points with read lock
        // Second step - delete all the rest points with full write lock
        //
        // Use collection copy to prevent long time lock of `proxy_deleted_points`
        let deleted_points_snapshot: Vec<PointIdType> =
            proxy_deleted_points.read().iter().cloned().collect();

        for &point_id in &deleted_points_snapshot {
            optimized_segment
                .delete_point(optimized_segment.version(), point_id)
                .unwrap();
        }

        let deleted_indexes = proxy_deleted_indexes.read().iter().cloned().collect_vec();
        let create_indexes = proxy_created_indexes.read().iter().cloned().collect_vec();

        for delete_field_name in &deleted_indexes {
            optimized_segment.delete_field_index(optimized_segment.version(), delete_field_name)?;
            self.check_cancellation(stopped)?;
        }

        for create_field_name in &create_indexes {
            optimized_segment.create_field_index(optimized_segment.version(), create_field_name)?;
            self.check_cancellation(stopped)?;
        }

        Ok(optimized_segment)
    }

    /// Performs optimization of collections's segments, including:
    ///     - Segment rebuilding
    ///     - Segment joining
    ///
    /// # Arguments
    ///
    /// * `segments` - segments holder
    /// * `ids` - list of segment ids to perform optimization on. All segments will be merged into single one
    /// * `stopped` - flag for early stopping of the optimization.
    ///               If appears to be `true` - optimization process should be cancelled, all segments unwrapped
    ///
    /// # Result
    ///
    /// New optimized segment should be added into `segments`.
    /// If there were any record changes during the optimization - an additional plain segment will be created.
    ///
    fn optimize(
        &self,
        segments: LockedSegmentHolder,
        ids: Vec<SegmentId>,
        stopped: &AtomicBool,
    ) -> CollectionResult<bool> {
        // On the one hand - we want to check consistently if all provided segments are
        // available for optimization (not already under one) and we want to do it before creating a temp segment
        // which is an expensive operation. So we can't not unlock `segments` after the check and before the insert.
        //
        // On the other hand - we do not want to hold write lock during the segment creation.
        // Solution in the middle - is a upgradable lock. It ensures consistency after the check and allows to perform read operation.
        let segment_lock = segments.upgradable_read();

        let optimizing_segments: Vec<_> = ids
            .iter()
            .cloned()
            .map(|id| segment_lock.get(id))
            .filter_map(|x| x.cloned())
            .collect();

        // Check if all segments are not under other optimization or some ids are missing
        let all_segments_ok = optimizing_segments.len() == ids.len()
            && optimizing_segments
                .iter()
                .all(|s| matches!(s, LockedSegment::Original(_)));

        if !all_segments_ok {
            // Cancel the optimization
            return Ok(false);
        }

        let tmp_segment = self.temp_segment()?;

        let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
        let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let proxy_created_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));

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
            // Exclusive lock for the segments operations
            let mut write_segments = RwLockUpgradableReadGuard::upgrade(segment_lock);

            proxies
                .zip(ids.iter().cloned())
                .map(|(proxy, idx)| write_segments.swap(proxy, &[idx], false).unwrap())
                .collect()
        };

        // ---- SLOW PART -----
        let mut optimized_segment = match self.build_new_segment(
            &optimizing_segments,
            proxy_deleted_points.clone(),
            proxy_deleted_indexes.clone(),
            proxy_created_indexes.clone(),
            stopped,
        ) {
            Ok(segment) => segment,
            Err(error) => {
                if matches!(error, CollectionError::Cancelled { .. }) {
                    self.handle_cancellation(&segments, &proxy_ids, &tmp_segment)?;
                }
                return Err(error);
            }
        };

        // Avoid unnecessary point removing in the critical section:
        // - save already removed points while avoiding long read locks
        // - exclude already removed points from post-optimization removing
        let already_remove_points = {
            let mut all_removed_points: HashSet<_> =
                proxy_deleted_points.read().iter().cloned().collect();
            for existing_point in optimized_segment.iter_points() {
                all_removed_points.remove(&existing_point);
            }
            all_removed_points
        };

        // ---- SLOW PART ENDS HERE -----

        {
            // This block locks all operations with collection. It should be fast
            let mut write_segments = segments.write();
            let deleted_points = proxy_deleted_points.read();
            let points_diff = already_remove_points.difference(&deleted_points);
            for &point_id in points_diff {
                optimized_segment
                    .delete_point(optimized_segment.version(), point_id)
                    .unwrap();
            }

            for deleted_field_name in proxy_deleted_indexes.read().iter() {
                optimized_segment
                    .delete_field_index(optimized_segment.version(), deleted_field_name)?;
            }

            for created_field_name in proxy_created_indexes.read().iter() {
                optimized_segment
                    .create_field_index(optimized_segment.version(), created_field_name)?;
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
