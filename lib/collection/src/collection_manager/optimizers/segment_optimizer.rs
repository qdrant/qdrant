use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::disk::dir_size;
use io::storage_version::StorageVersion;
use itertools::Itertools;
use parking_lot::{Mutex, RwLockUpgradableReadGuard};
use segment::common::operation_error::{check_process_stopped, OperationResult};
use segment::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::entry::entry_point::SegmentEntry;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{HnswConfig, Indexes, QuantizationConfig, SegmentConfig, VectorStorageType};

use crate::collection_manager::holders::proxy_segment::{self, ProxyIndexChange, ProxySegment};
use crate::collection_manager::holders::segment_holder::{
    LockedSegment, LockedSegmentHolder, SegmentId,
};
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;
use crate::operations::types::{CollectionError, CollectionResult};

const BYTES_IN_KB: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct OptimizerThresholds {
    pub max_segment_size_kb: usize,
    pub memmap_threshold_kb: usize,
    pub indexing_threshold_kb: usize,
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

    /// Get the path of the segments directory
    fn segments_path(&self) -> &Path;

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

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator>;

    /// Build temp segment
    fn temp_segment(&self, save_version: bool) -> CollectionResult<LockedSegment> {
        let collection_params = self.collection_params();
        let config = SegmentConfig {
            vector_data: collection_params.to_base_vector_data()?,
            sparse_vector_data: collection_params.to_sparse_vector_data()?,
            payload_storage_type: collection_params.payload_storage_type(),
        };
        Ok(LockedSegment::new(build_segment(
            self.segments_path(),
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

        // Counting up how much space do the segments being optimized actually take on the fs.
        // If there was at least one error while reading the size, this will be `None`.
        let mut space_occupied = Some(0u64);

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

            for vector_name in locked_segment.vector_names() {
                let vector_size = locked_segment.available_vectors_size_in_bytes(&vector_name)?;
                let size = bytes_count_by_vector_name.entry(vector_name).or_insert(0);
                *size += vector_size;
            }

            space_occupied =
                space_occupied.and_then(|acc| match dir_size(locked_segment.data_path()) {
                    Ok(size) => Some(size + acc),
                    Err(err) => {
                        log::debug!(
                            "Could not estimate size of segment `{}`: {}",
                            locked_segment.data_path().display(),
                            err
                        );
                        None
                    }
                });
        }

        let space_needed = space_occupied.map(|x| 2 * x);

        // Ensure temp_path exists

        if !self.temp_path().exists() {
            std::fs::create_dir_all(self.temp_path()).map_err(|err| {
                CollectionError::service_error(format!(
                    "Could not create temp directory `{}`: {}",
                    self.temp_path().display(),
                    err
                ))
            })?;
        }

        let space_available = match fs4::available_space(self.temp_path()) {
            Ok(available) => Some(available),
            Err(err) => {
                log::debug!(
                    "Could not estimate available storage space in `{}`: {}",
                    self.temp_path().display(),
                    err
                );
                None
            }
        };

        match (space_available, space_needed) {
            (Some(space_available), Some(space_needed)) => {
                if space_available < space_needed {
                    return Err(CollectionError::service_error(
                        "Not enough space available for optimization".to_string(),
                    ));
                }
            }
            _ => {
                log::warn!(
                    "Could not estimate available storage space in `{}`; will try optimizing anyway",
                    self.name()
                );
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

        let threshold_is_indexed = maximal_vector_store_size_bytes
            >= thresholds.indexing_threshold_kb.saturating_mul(BYTES_IN_KB);

        let threshold_is_on_disk = maximal_vector_store_size_bytes
            >= thresholds.memmap_threshold_kb.saturating_mul(BYTES_IN_KB);

        let mut vector_data = collection_params.to_base_vector_data()?;
        let mut sparse_vector_data = collection_params.to_sparse_vector_data()?;

        // If indexing, change to HNSW index and quantization
        if threshold_is_indexed {
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

        // If storing on disk, set storage type in current segment (not in collection config)
        if threshold_is_on_disk {
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                // Check whether on_disk is explicitly configured, if not, set it to true
                let config_on_disk = collection_params
                    .vectors
                    .get_params(vector_name)
                    .and_then(|config| config.on_disk);

                match config_on_disk {
                    Some(true) => config.storage_type = VectorStorageType::Mmap, // Both agree, but prefer mmap storage type
                    Some(false) => {} // on_disk=false wins, do nothing
                    None => config.storage_type = VectorStorageType::Mmap, // Mmap threshold wins
                }

                // If we explicitly configure on_disk, but the segment storage type uses something
                // that doesn't match, warn about it
                if let Some(config_on_disk) = config_on_disk {
                    if config_on_disk != config.storage_type.is_on_disk() {
                        log::warn!("Collection config for vector {vector_name} has on_disk={config_on_disk:?} configured, but storage type for segment doesn't match it");
                    }
                }
            });
        }

        sparse_vector_data
            .iter_mut()
            .for_each(|(vector_name, config)| {
                // Assign sparse index on disk
                if let Some(sparse_config) = &collection_params.sparse_vectors {
                    if let Some(params) = sparse_config.get(vector_name) {
                        let config_on_disk = params
                            .index
                            .and_then(|index_params| index_params.on_disk)
                            .unwrap_or(threshold_is_on_disk);

                        // If mmap OR index is exceeded
                        let is_big = threshold_is_on_disk || threshold_is_indexed;

                        let index_type = match (is_big, config_on_disk) {
                            (true, true) => SparseIndexType::Mmap, // Big and configured on disk
                            (true, false) => SparseIndexType::ImmutableRam, // Big and not on disk nor reached threshold
                            (false, _) => SparseIndexType::MutableRam,      // Small
                        };

                        config.index.index_type = index_type;
                    }
                }
            });

        let optimized_config = SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: collection_params.payload_storage_type(),
        };

        Ok(SegmentBuilder::new(
            self.segments_path(),
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
                            segments_lock.swap_new(wrapped_segment, &[proxy_id]);
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
        temp_segment: LockedSegment,
    ) -> OperationResult<()> {
        self.unwrap_proxy(segments, proxy_ids);
        if !temp_segment.get().read().is_empty() {
            let mut write_segments = segments.write();
            write_segments.add_new_locked(temp_segment);
        } else {
            // Temp segment is already removed from proxy, so nobody could write to it in between
            temp_segment.drop_data()?;
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
    /// * `proxy_changed_indexes` - Holds a set of indexes changes, created or deleted while optimization was running
    /// * `stopped` - flag to check if optimization was cancelled by external thread
    ///
    /// # Result
    ///
    /// Constructs optimized segment
    fn build_new_segment(
        &self,
        optimizing_segments: &[LockedSegment],
        proxy_deleted_points: proxy_segment::LockedRmSet,
        proxy_changed_indexes: proxy_segment::LockedIndexChanges,
        permit: CpuPermit,
        stopped: &AtomicBool,
    ) -> CollectionResult<Segment> {
        let mut segment_builder = self.optimized_segment_builder(optimizing_segments)?;

        self.check_cancellation(stopped)?;

        let segments: Vec<_> = optimizing_segments
            .iter()
            .map(|i| match i {
                LockedSegment::Original(o) => o.clone(),
                LockedSegment::Proxy(_) => {
                    panic!("Trying to optimize a segment that is already being optimized!")
                }
            })
            .collect();

        let mut defragmentation_keys = HashSet::new();
        for segment in &segments {
            let payload_index = &segment.read().payload_index;
            let payload_index = payload_index.borrow();

            let keys = payload_index
                .config()
                .indexed_fields
                .iter()
                .filter_map(|(key, schema)| schema.is_tenant().then_some(key))
                .cloned();
            defragmentation_keys.extend(keys);
        }

        if !defragmentation_keys.is_empty() {
            segment_builder.set_defragment_keys(defragmentation_keys.into_iter().collect());
        }

        {
            let segment_guards = segments.iter().map(|segment| segment.read()).collect_vec();
            segment_builder.update(
                &segment_guards.iter().map(Deref::deref).collect_vec(),
                stopped,
            )?;
        }

        // Apply index changes to segment builder
        // Indexes are only used for defragmentation in segment builder, so versions are ignored
        for (field_name, change) in proxy_changed_indexes.read().iter_unordered() {
            match change {
                ProxyIndexChange::Create(schema, _) => {
                    segment_builder.add_indexed_field(field_name.to_owned(), schema.to_owned());
                }
                ProxyIndexChange::Delete(_) => {
                    segment_builder.remove_indexed_field(field_name);
                }
            }
        }

        let mut optimized_segment: Segment = segment_builder.build(permit, stopped)?;

        // Apply index changes before point deletions
        // Point deletions bump the segment version, can cause index changes to be ignored
        let old_optimized_segment_version = optimized_segment.version();
        for (field_name, change) in proxy_changed_indexes.read().iter_ordered() {
            debug_assert!(
                change.version() >= old_optimized_segment_version,
                "proxied index change should have newer version than segment",
            );
            match change {
                ProxyIndexChange::Create(schema, version) => {
                    optimized_segment.create_field_index(*version, field_name, Some(schema))?;
                }
                ProxyIndexChange::Delete(version) => {
                    optimized_segment.delete_field_index(*version, field_name)?;
                }
            }
            self.check_cancellation(stopped)?;
        }

        // Delete points
        let deleted_points_snapshot = proxy_deleted_points
            .read()
            .iter()
            .map(|(point_id, versions)| (*point_id, *versions))
            .collect::<Vec<_>>();
        for (point_id, versions) in deleted_points_snapshot {
            optimized_segment
                .delete_point(versions.operation_version, point_id)
                .unwrap();
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
    /// Returns id of the created optimized segment. If no optimization was done - returns None
    ///
    fn optimize(
        &self,
        segments: LockedSegmentHolder,
        ids: Vec<SegmentId>,
        permit: CpuPermit,
        stopped: &AtomicBool,
    ) -> CollectionResult<usize> {
        check_process_stopped(stopped)?;

        let mut timer = ScopeDurationMeasurer::new(self.get_telemetry_counter());
        timer.set_success(false);

        // On the one hand - we want to check consistently if all provided segments are
        // available for optimization (not already under one) and we want to do it before creating a temp segment
        // which is an expensive operation. So we can't not unlock `segments` after the check and before the insert.
        //
        // On the other hand - we do not want to hold write lock during the segment creation.
        // Solution in the middle - is a upgradable lock. It ensures consistency after the check and allows to perform read operation.
        let segments_lock = segments.upgradable_read();

        let optimizing_segments: Vec<_> = ids
            .iter()
            .cloned()
            .map(|id| segments_lock.get(id))
            .filter_map(|x| x.cloned())
            .collect();

        // Check if all segments are not under other optimization or some ids are missing
        let all_segments_ok = optimizing_segments.len() == ids.len()
            && optimizing_segments
                .iter()
                .all(|s| matches!(s, LockedSegment::Original(_)));

        if !all_segments_ok {
            // Cancel the optimization
            return Ok(0);
        }

        check_process_stopped(stopped)?;

        let tmp_segment = self.temp_segment(false)?;
        let proxy_deleted_points = proxy_segment::LockedRmSet::default();
        let proxy_index_changes = proxy_segment::LockedIndexChanges::default();

        let mut proxies = Vec::new();
        for sg in optimizing_segments.iter() {
            let mut proxy = ProxySegment::new(
                sg.clone(),
                tmp_segment.clone(),
                Arc::clone(&proxy_deleted_points),
                Arc::clone(&proxy_index_changes),
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
            let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
            let mut proxy_ids = Vec::new();
            for (mut proxy, idx) in proxies.into_iter().zip(ids.iter().cloned()) {
                // replicate_field_indexes for the second time,
                // because optimized segments could have been changed.
                // The probability is small, though,
                // so we can afford this operation under the full collection write lock
                let op_num = 0;
                proxy.replicate_field_indexes(op_num)?; // Slow only in case the index is change in the gap between two calls
                proxy_ids.push(write_segments.swap_new(proxy, &[idx]).0);
            }
            proxy_ids
        };

        if let Err(e) = check_process_stopped(stopped) {
            self.handle_cancellation(&segments, &proxy_ids, tmp_segment)?;
            return Err(CollectionError::from(e));
        }

        // ---- SLOW PART -----

        let mut optimized_segment = match self.build_new_segment(
            &optimizing_segments,
            Arc::clone(&proxy_deleted_points),
            Arc::clone(&proxy_index_changes),
            permit,
            stopped,
        ) {
            Ok(segment) => segment,
            Err(error) => {
                if matches!(error, CollectionError::Cancelled { .. }) {
                    self.handle_cancellation(&segments, &proxy_ids, tmp_segment)?;
                    return Err(error);
                }
                return Err(error);
            }
        };

        // Avoid unnecessary point removing in the critical section:
        // - save already removed points while avoiding long read locks
        // - exclude already removed points from post-optimization removing
        let already_remove_points = {
            let mut all_removed_points: HashSet<_> =
                proxy_deleted_points.read().keys().copied().collect();
            for existing_point in optimized_segment.iter_points() {
                all_removed_points.remove(&existing_point);
            }
            all_removed_points
        };

        // ---- SLOW PART ENDS HERE -----

        if let Err(e) = check_process_stopped(stopped) {
            self.handle_cancellation(&segments, &proxy_ids, tmp_segment)?;
            return Err(CollectionError::from(e));
        }

        {
            // This block locks all operations with collection. It should be fast
            let mut write_segments_guard = segments.write();
            let old_optimized_segment_version = optimized_segment.version();

            // Apply index changes before point deletions
            // Point deletions bump the segment version, can cause index changes to be ignored
            for (field_name, change) in proxy_index_changes.read().iter_ordered() {
                debug_assert!(
                    change.version() >= old_optimized_segment_version,
                    "proxied index change should have newer version than segment",
                );
                match change {
                    ProxyIndexChange::Create(schema, version) => {
                        optimized_segment.create_field_index(*version, field_name, Some(schema))?;
                    }
                    ProxyIndexChange::Delete(version) => {
                        optimized_segment.delete_field_index(*version, field_name)?;
                    }
                }
                self.check_cancellation(stopped)?;
            }

            let deleted_points = proxy_deleted_points.read();
            let points_diff = deleted_points
                .iter()
                .filter(|&(point_id, _version)| !already_remove_points.contains(point_id));
            for (&point_id, &versions) in points_diff {
                // Delete points here with their operation version, that'll bump the optimized
                // segment version and will ensure we flush the new changes
                debug_assert!(
                    versions.operation_version >= old_optimized_segment_version,
                    "proxied point deletes should have newer version than segment",
                );
                optimized_segment
                    .delete_point(versions.operation_version, point_id)
                    .unwrap();
            }

            optimized_segment.prefault_mmap_pages();

            let point_count = optimized_segment.available_point_count();

            let (_, proxies) = write_segments_guard.swap_new(optimized_segment, &proxy_ids);
            debug_assert_eq!(
                proxies.len(),
                proxy_ids.len(),
                "swapped different number of proxies on unwrap, missing or incorrect segment IDs?",
            );

            let has_appendable_segments = write_segments_guard.has_appendable_segment();

            // Release reference counter of the optimized segments
            drop(optimizing_segments);

            // Append a temp segment to collection if it is not empty or there is no other appendable segment
            if !has_appendable_segments || !tmp_segment.get().read().is_empty() {
                write_segments_guard.add_new_locked(tmp_segment);

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

            timer.set_success(true);

            Ok(point_count)
        }
    }
}
