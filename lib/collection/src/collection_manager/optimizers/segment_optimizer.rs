use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use segment::common::operation_error::check_process_stopped;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::common::version::StorageVersion;
use segment::entry::entry_point::SegmentEntry;
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    HnswConfig, Indexes, PayloadFieldSchema, PayloadKeyType, PayloadStorageType, PointIdType,
    QuantizationConfig, SegmentConfig, VectorStorageType, VECTOR_ELEMENT_SIZE,
};

use crate::collection_manager::holders::proxy_segment::ProxySegment;
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;
use crate::operations::types::{CollectionError, CollectionResult};

const BYTES_IN_KB: usize = 1024;

#[derive(Debug, Clone)]
pub struct OptimizerThresholds {
    pub max_segment_size: usize,
    pub memmap_threshold: usize,
    pub indexing_threshold: usize,
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
    /// Get name describing this optimizer
    fn name(&self) -> &str;

    /// Get path of the whole collection
    fn collection_path(&self) -> &Path;

    /// Get temp path, where optimized segments could be temporary stored
    fn temp_path(&self) -> &Path;

    /// Get basic segment config
    fn collection_params(&self) -> CollectionParams;

    /// Get HNSW config
    fn hnsw_config(&self) -> &HnswConfig;

    /// Get quantization config
    fn quantization_config(&self) -> Option<QuantizationConfig>;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Checks if segment optimization is required
    fn check_condition(
        &self,
        segments: LockedSegmentHolder,
        excluded_ids: &HashSet<SegmentId>,
    ) -> Vec<SegmentId>;

    fn get_telemetry_data(&self) -> OperationDurationStatistics;

    fn get_telemetry_counter(&self) -> Arc<Mutex<OperationDurationsAggregator>>;

    /// Build temp segment
    fn temp_segment(&self, save_version: bool) -> CollectionResult<LockedSegment> {
        let collection_params = self.collection_params();
        let config = SegmentConfig {
            vector_data: collection_params.into_base_vector_data()?,
            sparse_vector_data: collection_params.into_sparse_vector_data()?,
            payload_storage_type: if collection_params.on_disk_payload {
                PayloadStorageType::OnDisk
            } else {
                PayloadStorageType::InMemory
            },
        };
        Ok(LockedSegment::new(build_segment(
            self.collection_path(),
            &config,
            save_version,
        )?))
    }

    /// Build optimized segment
    fn optimized_segment_builder(
        &self,
        optimizing_segments: &[LockedSegment],
    ) -> CollectionResult<SegmentBuilder> {
        // Example:
        //
        // S1: {
        //     text_vectors: 10000,
        //     image_vectors: 100
        // }
        // S2: {
        //     text_vectors: 200,
        //     image_vectors: 10000
        // }

        // Example: bytes_count_by_vector_name = {
        //     text_vectors: 10200 * dim * VECTOR_ELEMENT_SIZE
        //     image_vectors: 10100 * dim * VECTOR_ELEMENT_SIZE
        // }
        let mut bytes_count_by_vector_name = HashMap::new();

        for segment in optimizing_segments {
            let segment = match segment {
                LockedSegment::Original(segment) => segment,
                LockedSegment::Proxy(_) => {
                    return Err(CollectionError::service_error(
                        "Proxy segment is not expected here".to_string(),
                    ))
                }
            };
            let locked_segment = segment.read();

            for (vector_name, dim) in locked_segment.vector_dims() {
                let available_vectors =
                    locked_segment.available_vector_count(&vector_name).unwrap();
                let vector_size = dim * VECTOR_ELEMENT_SIZE * available_vectors;
                let size = bytes_count_by_vector_name.entry(vector_name).or_insert(0);
                *size += vector_size;
            }
        }

        // Example: maximal_vector_store_size_bytes = 10200 * dim * VECTOR_ELEMENT_SIZE
        let maximal_vector_store_size_bytes = bytes_count_by_vector_name
            .values()
            .max()
            .copied()
            .unwrap_or(0);

        let thresholds = self.threshold_config();
        let collection_params = self.collection_params();

        let is_indexed = maximal_vector_store_size_bytes
            >= thresholds.indexing_threshold.saturating_mul(BYTES_IN_KB);

        let is_on_disk = maximal_vector_store_size_bytes
            >= thresholds.memmap_threshold.saturating_mul(BYTES_IN_KB);

        let mut vector_data = collection_params.into_base_vector_data()?;
        let mut sparse_vector_data = collection_params.into_sparse_vector_data()?;

        // If indexing, change to HNSW index and quantization
        if is_indexed {
            let collection_hnsw = self.hnsw_config();
            let collection_quantization = self.quantization_config();
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                // Assign HNSW index
                let param_hnsw = collection_params
                    .vectors
                    .get_params(vector_name)
                    .and_then(|params| params.hnsw_config);
                let vector_hnsw = param_hnsw
                    .and_then(|c| c.update(collection_hnsw).ok())
                    .unwrap_or_else(|| collection_hnsw.clone());
                config.index = Indexes::Hnsw(vector_hnsw);

                // Assign quantization config
                let param_quantization = collection_params
                    .vectors
                    .get_params(vector_name)
                    .and_then(|params| params.quantization_config.as_ref());
                let vector_quantization = param_quantization
                    .or(collection_quantization.as_ref())
                    .cloned();
                config.quantization_config = vector_quantization;
            });
        }

        // If storing on disk, set storage type
        if is_on_disk {
            vector_data.values_mut().for_each(|config| {
                config.storage_type = VectorStorageType::Mmap;
            });

            sparse_vector_data
                .iter_mut()
                .for_each(|(vector_name, config)| {
                    // Assign sparse index on disk
                    if let Some(sparse_config) = &collection_params.sparse_vectors {
                        if let Some(params) = sparse_config.get(vector_name) {
                            if let Some(index) = params.index.as_ref() {
                                config.index = Some(*index);
                            }
                        }
                    }
                });
        }

        let optimized_config = SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: if collection_params.on_disk_payload {
                PayloadStorageType::OnDisk
            } else {
                PayloadStorageType::InMemory
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
    ) -> Vec<SegmentId> {
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
                        let (restored_id, _proxies) =
                            segments_lock.swap(wrapped_segment, &[proxy_id]);
                        restored_segment_ids.push(restored_id);
                    }
                }
            }
        }
        restored_segment_ids
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
    /// Rolls back optimization state.
    /// All processed changes will still be there, but the collection should be returned into state
    /// before optimization.
    fn handle_cancellation(
        &self,
        segments: &LockedSegmentHolder,
        proxy_ids: &[SegmentId],
        temp_segment: &LockedSegment,
    ) {
        self.unwrap_proxy(segments, proxy_ids);
        if temp_segment.get().read().available_point_count() > 0 {
            let mut write_segments = segments.write();
            write_segments.add_locked(temp_segment.clone());
        }
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
        proxy_created_indexes: Arc<RwLock<HashMap<PayloadKeyType, PayloadFieldSchema>>>,
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
        for (field, schema_type) in proxy_created_indexes.read().iter() {
            segment_builder
                .indexed_fields
                .insert(field.to_owned(), schema_type.to_owned());
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
        let create_indexes = proxy_created_indexes.read().clone();

        for delete_field_name in &deleted_indexes {
            optimized_segment.delete_field_index(optimized_segment.version(), delete_field_name)?;
            self.check_cancellation(stopped)?;
        }

        for (create_field_name, schema) in create_indexes {
            optimized_segment.create_field_index(
                optimized_segment.version(),
                &create_field_name,
                Some(&schema),
            )?;
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
        check_process_stopped(stopped)?;

        let mut timer = ScopeDurationMeasurer::new(&self.get_telemetry_counter());
        timer.set_success(false);

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

        check_process_stopped(stopped)?;

        let tmp_segment = self.temp_segment(false)?;

        let proxy_deleted_points = Arc::new(RwLock::new(HashSet::<PointIdType>::new()));
        let proxy_deleted_indexes = Arc::new(RwLock::new(HashSet::<PayloadKeyType>::new()));
        let proxy_created_indexes = Arc::new(RwLock::new(HashMap::<
            PayloadKeyType,
            PayloadFieldSchema,
        >::new()));

        let mut proxies = Vec::new();
        for sg in optimizing_segments.iter() {
            let mut proxy = ProxySegment::new(
                sg.clone(),
                tmp_segment.clone(),
                proxy_deleted_points.clone(),
                proxy_created_indexes.clone(),
                proxy_deleted_indexes.clone(),
            );
            // Wrapped segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            proxy.replicate_field_indexes(0)?;
            proxies.push(proxy);
        }

        // Save segment version once all payload indices have been converted
        // If this ends up not being saved due to a crash, the segment will not be used
        match &tmp_segment {
            LockedSegment::Original(segment) => {
                let segment_path = &segment.read().current_path;
                SegmentVersion::save(segment_path)?;
            }
            LockedSegment::Proxy(_) => unreachable!(),
        }

        let proxy_ids: Vec<_> = {
            // Exclusive lock for the segments operations.
            let mut write_segments = RwLockUpgradableReadGuard::upgrade(segment_lock);
            let mut proxy_ids = Vec::new();
            for (mut proxy, idx) in proxies.into_iter().zip(ids.iter().cloned()) {
                // replicate_field_indexes for the second time,
                // because optimized segments could have been changed.
                // The probability is small, though,
                // so we can afford this operation under the full collection write lock
                let op_num = 0;
                proxy.replicate_field_indexes(op_num)?; // Slow only in case the index is change in the gap between two calls
                proxy_ids.push(write_segments.swap(proxy, &[idx]).0);
            }
            proxy_ids
        };

        check_process_stopped(stopped).map_err(|error| {
            self.handle_cancellation(&segments, &proxy_ids, &tmp_segment);
            error
        })?;

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
                    self.handle_cancellation(&segments, &proxy_ids, &tmp_segment);
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

        check_process_stopped(stopped).map_err(|error| {
            self.handle_cancellation(&segments, &proxy_ids, &tmp_segment);
            error
        })?;

        {
            // This block locks all operations with collection. It should be fast
            let mut write_segments_guard = segments.write();
            let deleted_points = proxy_deleted_points.read();
            let points_diff = deleted_points.difference(&already_remove_points);
            for &point_id in points_diff {
                optimized_segment
                    .delete_point(optimized_segment.version(), point_id)
                    .unwrap();
            }

            for deleted_field_name in proxy_deleted_indexes.read().iter() {
                optimized_segment
                    .delete_field_index(optimized_segment.version(), deleted_field_name)?;
            }

            for (created_field_name, schema_type) in proxy_created_indexes.read().iter() {
                optimized_segment.create_field_index(
                    optimized_segment.version(),
                    created_field_name,
                    Some(schema_type),
                )?;
            }

            optimized_segment.prefault_mmap_pages();

            let (_, proxies) = write_segments_guard.swap(optimized_segment, &proxy_ids);

            let has_appendable_segments =
                write_segments_guard.random_appendable_segment().is_some();

            // Release reference counter of the optimized segments
            drop(optimizing_segments);

            // Append a temp segment to a collection if it is not empty or there is no other appendable segment
            if tmp_segment.get().read().available_point_count() > 0 || !has_appendable_segments {
                write_segments_guard.add_locked(tmp_segment);

                // unlock collection for search and updates
                drop(write_segments_guard);
                // After the collection is unlocked - we can remove data as slow as we want.

                // Only remove data after we ensure the consistency of the collection.
                // If remove fails - we will still have operational collection with reported error.
                for proxy in proxies {
                    proxy.drop_data()?;
                }
            } else {
                // unlock collection for search and updates
                drop(write_segments_guard);
                // After the collection is unlocked - we can remove data as slow as we want.

                // Proxy contains pointer to the `tmp_segment`, so they should be removed first
                for proxy in proxies {
                    proxy.drop_data()?;
                }
                tmp_segment.drop_data()?;
            }
        }
        timer.set_success(true);
        Ok(true)
    }
}
