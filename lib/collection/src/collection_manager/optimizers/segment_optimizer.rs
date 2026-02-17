use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::budget::{ResourceBudget, ResourcePermit};
use common::bytes::bytes_to_human;
use common::counter::hardware_counter::HardwareCounterCell;
use common::disk::dir_disk_size;
use common::progress_tracker::ProgressTracker;
use common::storage_version::StorageVersion;
use fs_err as fs;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use segment::common::operation_error::check_process_stopped;
use segment::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::entry::entry_point::NonAppendableSegmentEntry;
use segment::index::sparse_index::sparse_index_config::SparseIndexType;
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    HnswConfig, HnswGlobalConfig, Indexes, QuantizationConfig, SegmentConfig, VectorStorageType,
};
use shard::proxy_segment::{DeletedPoints, ProxyIndexChanges};
use shard::segment_holder::locked::LockedSegmentHolder;
use uuid::Uuid;

use crate::collection_manager::holders::proxy_segment::{ProxyIndexChange, ProxySegment};
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder, SegmentId};
use crate::config::CollectionParams;
use crate::operations::config_diff::DiffConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::update_handler::Optimizer;

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
    fn name(&self) -> &'static str;

    /// Get the path of the segments directory
    fn segments_path(&self) -> &Path;

    /// Get temp path, where optimized segments could be temporary stored
    fn temp_path(&self) -> &Path;

    /// Get basic segment config
    fn collection_params(&self) -> CollectionParams;

    /// Get HNSW config
    fn hnsw_config(&self) -> &HnswConfig;

    /// Get HNSW global config
    fn hnsw_global_config(&self) -> &HnswGlobalConfig;

    /// Get quantization config
    fn quantization_config(&self) -> Option<QuantizationConfig>;

    /// Get thresholds configuration for the current optimizer
    fn threshold_config(&self) -> &OptimizerThresholds;

    /// Find segments that require optimization and write them into `planner`.
    fn plan_optimizations(&self, planner: &mut OptimizationPlanner);

    /// Wrapper around [`SegmentOptimizer::plan_optimizations`].
    /// Simplified interface and extra checks.
    #[cfg(test)]
    fn plan_optimizations_for_test(&self, segments: &LockedSegmentHolder) -> Vec<Vec<SegmentId>> {
        let segments = segments.read();

        let mut planner = OptimizationPlanner::new(0, segments.iter_original());
        self.plan_optimizations(&mut planner);
        let result = planner.into_scheduled_for_test();

        // Verify consistency: re-planning with remaining segments should match tail
        let mut remaining: BTreeMap<_, _> = segments.iter_original().collect();
        for (i, batch) in result.iter().enumerate() {
            for &id in batch {
                remaining.remove(&id);
            }
            let mut planner =
                OptimizationPlanner::new(i + 1, remaining.iter().map(|(&id, &seg)| (id, seg)));
            self.plan_optimizations(&mut planner);
            let actual = planner.into_scheduled_for_test();
            let expected = &result[i + 1..];
            if self.name() == "merge"
                && actual.is_empty()
                && expected.len() == 1
                && expected[0].len() == 2
            {
                // Special case for MergeOptimizer:
                // `[A B] [C D]` is allowed, but `[C D]` is not. See its doc.
                continue;
            }
            assert_eq!(actual, expected);
        }

        result
    }

    fn get_telemetry_counter(&self) -> &Mutex<OperationDurationsAggregator>;

    /// Build temp segment
    fn temp_segment(&self, save_version: bool) -> CollectionResult<LockedSegment> {
        let collection_params = self.collection_params();
        let quantization_config = self.quantization_config();
        let config = SegmentConfig {
            vector_data: collection_params.to_base_vector_data(quantization_config.as_ref())?,
            sparse_vector_data: collection_params.to_sparse_vector_data()?,
            payload_storage_type: collection_params.payload_storage_type(),
        };
        Ok(LockedSegment::new(build_segment(
            self.segments_path(),
            &config,
            save_version,
        )?))
    }

    /// Returns error if segment size is larger than available disk space
    fn check_segments_size(&self, optimizing_segments: &[LockedSegment]) -> CollectionResult<()> {
        // Counting up how much space do the segments being optimized actually take on the fs.
        // If there was at least one error while reading the size, this will be `None`.
        let mut space_occupied = Some(0u64);

        for segment in optimizing_segments {
            let segment = match segment {
                LockedSegment::Original(segment) => segment,
                LockedSegment::Proxy(_) => {
                    return Err(CollectionError::service_error(
                        "Proxy segment is not expected here".to_string(),
                    ));
                }
            };

            let locked_segment = segment.read();

            space_occupied =
                space_occupied.and_then(|acc| match dir_disk_size(locked_segment.data_path()) {
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
            fs::create_dir_all(self.temp_path()).map_err(|err| {
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
                if space_needed > 0 {
                    log::debug!(
                        "Available space: {}, needed for optimization: {}",
                        bytes_to_human(space_available as usize),
                        bytes_to_human(space_needed as usize),
                    );
                }
                if space_available < space_needed {
                    return Err(CollectionError::service_error(format!(
                        "Not enough space available for optimization, needed: {}, available: {}",
                        bytes_to_human(space_needed as usize),
                        bytes_to_human(space_available as usize),
                    )));
                }
            }
            _ => {
                log::warn!(
                    "Could not estimate available storage space in `{}`; will try optimizing anyway",
                    self.name(),
                );
            }
        }

        Ok(())
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
                    ));
                }
            };
            let locked_segment = segment.read();

            for vector_name in locked_segment.vector_names() {
                let vector_size = locked_segment.available_vectors_size_in_bytes(&vector_name)?;
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

        let threshold_is_indexed = maximal_vector_store_size_bytes
            >= thresholds.indexing_threshold_kb.saturating_mul(BYTES_IN_KB);

        let threshold_is_on_disk = maximal_vector_store_size_bytes
            >= thresholds.memmap_threshold_kb.saturating_mul(BYTES_IN_KB);

        let collection_quantization = self.quantization_config();
        let mut vector_data =
            collection_params.to_base_vector_data(collection_quantization.as_ref())?;
        let mut sparse_vector_data = collection_params.to_sparse_vector_data()?;

        // If indexing, change to HNSW index and quantization
        if threshold_is_indexed {
            let collection_hnsw = self.hnsw_config();
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                // Assign HNSW index
                let param_hnsw = collection_params
                    .vectors
                    .get_params(vector_name)
                    .and_then(|params| params.hnsw_config);
                let vector_hnsw = collection_hnsw.update_opt(param_hnsw.as_ref());
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

        // We want to use single-file mmap in the following cases:
        // - It is explicitly configured by `mmap_threshold` -> threshold_is_on_disk=true
        // - The segment is indexed and configured on disk -> threshold_is_indexed=true && config_on_disk=Some(true)
        if threshold_is_on_disk || threshold_is_indexed {
            vector_data.iter_mut().for_each(|(vector_name, config)| {
                // Check whether on_disk is explicitly configured, if not, set it to true
                let config_on_disk = collection_params
                    .vectors
                    .get_params(vector_name)
                    .and_then(|config| config.on_disk);

                match config_on_disk {
                    Some(true) => config.storage_type = VectorStorageType::Mmap, // Both agree, but prefer mmap storage type
                    Some(false) => {
                        if common::flags::feature_flags().single_file_mmap_vector_storage  {
                            config.storage_type = VectorStorageType::InRamMmap;
                        }
                    } // on_disk=false wins, do nothing
                    None => if threshold_is_on_disk {
                        config.storage_type = VectorStorageType::Mmap
                    } else if common::flags::feature_flags().single_file_mmap_vector_storage  {
                        config.storage_type = VectorStorageType::InRamMmap;
                    }, // Mmap threshold wins
                }

                // If we explicitly configure on_disk, but the segment storage type uses something
                // that doesn't match, warn about it
                if let Some(config_on_disk) = config_on_disk
                    && config_on_disk != config.storage_type.is_on_disk() {
                        log::warn!("Collection config for vector {vector_name} has on_disk={config_on_disk:?} configured, but storage type for segment doesn't match it");
                    }
            });
        }

        sparse_vector_data
            .iter_mut()
            .for_each(|(vector_name, config)| {
                // Assign sparse index on disk
                if let Some(sparse_config) = &collection_params.sparse_vectors
                    && let Some(params) = sparse_config.get(vector_name)
                {
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
            });

        let optimized_config = SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: collection_params.payload_storage_type(),
        };

        Ok(SegmentBuilder::new(
            self.temp_path(),
            &optimized_config,
            self.hnsw_global_config(),
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
    fn unwrap_proxy(
        &self,
        segments: &LockedSegmentHolder,
        proxy_ids: &[SegmentId],
    ) -> CollectionResult<()> {
        let mut segments_lock = segments.write();
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
                        segments_lock.replace(proxy_id, wrapped_segment)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Function to wrap slow part of optimization. Performs proxy rollback in case of cancellation.
    /// Warn: this function might be _VERY_ CPU intensive,
    /// so it is necessary to avoid any locks inside this part of the code
    ///
    /// Returns the newly constructed optimized segment.
    #[allow(clippy::too_many_arguments)]
    fn build_new_segment(
        &self,
        input_segments: &[LockedSegment], // Segments to optimize/merge into one
        output_segment_uuid: Uuid,        // The UUID of the resulting optimized segment
        proxies: &[LockedSegment],
        permit: ResourcePermit, // IO resources for copying data
        resource_budget: ResourceBudget,
        stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        progress: ProgressTracker,
    ) -> CollectionResult<Segment> {
        let mut segment_builder = self.optimized_segment_builder(input_segments)?;

        check_process_stopped(stopped)?;

        let progress_copy_data = progress.subtask("copy_data");
        let progress_populate_storages = progress.subtask("populate_vector_storages");
        let progress_wait_permit = progress.subtask("wait_cpu_permit");

        let segments: Vec<_> = input_segments
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
                .indices
                .iter()
                .filter(|(_, schema)| schema.schema.is_tenant())
                .map(|(key, _)| key.clone());
            defragmentation_keys.extend(keys);
        }

        if !defragmentation_keys.is_empty() {
            segment_builder.set_defragment_keys(defragmentation_keys.into_iter().collect());
        }

        {
            progress_copy_data.start();
            let segment_guards = segments.iter().map(|segment| segment.read()).collect_vec();
            segment_builder.update(
                &segment_guards.iter().map(Deref::deref).collect_vec(),
                stopped,
            )?;
            drop(progress_copy_data);
        }

        let proxy_index_changes = self.proxy_index_changes(proxies);

        // Apply index changes to segment builder
        // Indexes are only used for defragmentation in segment builder, so versions are ignored
        for (field_name, change) in proxy_index_changes.iter_unordered() {
            match change {
                ProxyIndexChange::Create(schema, _) => {
                    segment_builder.add_indexed_field(field_name.to_owned(), schema.to_owned());
                }
                ProxyIndexChange::Delete(_) => {
                    segment_builder.remove_indexed_field(field_name);
                }
                ProxyIndexChange::DeleteIfIncompatible(_, schema) => {
                    segment_builder.remove_index_field_if_incompatible(field_name, schema);
                }
            }
        }

        // Before switching from IO to CPU, make sure that vectors cache is heated up,
        // so indexing process won't need to wait for IO.
        progress_populate_storages.start();
        segment_builder.populate_vector_storages()?;
        drop(progress_populate_storages);

        // 000 - acquired
        // +++ - blocked on waiting
        //
        // Case: 1 indexation job at a time, long indexing
        //
        //  IO limit = 1
        // CPU limit = 2                         Next optimization
        //                                       │            loop
        //                                       │
        //                                       ▼
        //  IO 0  00000000000000                  000000000
        // CPU 1              00000000000000000
        //     2              00000000000000000
        //
        //
        //  IO 0  ++++++++++++++00000000000000000
        // CPU 1                       ++++++++0000000000
        //     2                       ++++++++0000000000
        //
        //
        //  Case: 1 indexing job at a time, short indexation
        //
        //
        //   IO limit = 1
        //  CPU limit = 2
        //
        //
        //   IO 0  000000000000   ++++++++0000000000
        //  CPU 1            00000
        //      2            00000
        //
        //   IO 0  ++++++++++++00000000000   +++++++
        //  CPU 1                       00000
        //      2                       00000
        // At this stage workload shifts from IO to CPU, so we can release IO permit

        // Use same number of threads for indexing as for IO.
        // This ensures that IO is equally distributed between optimization jobs.
        progress_wait_permit.start();
        let desired_cpus = permit.num_io as usize;
        let indexing_permit = resource_budget
            .replace_with(permit, desired_cpus, 0, stopped)
            .map_err(|_| {
                CollectionError::cancelled("optimization cancelled while waiting for budget")
            })?;
        drop(progress_wait_permit);

        let mut rng = rand::rng();
        let mut optimized_segment = segment_builder.build(
            self.segments_path(),
            output_segment_uuid,
            indexing_permit,
            stopped,
            &mut rng,
            hw_counter,
            progress,
        )?;

        // Delete points
        let deleted_points_snapshot = self.proxy_deleted_points(proxies);
        let proxy_index_changes = self.proxy_index_changes(proxies);

        // Apply index changes before point deletions
        // Point deletions bump the segment version, can cause index changes to be ignored
        let old_optimized_segment_version = optimized_segment.version();
        for (field_name, change) in proxy_index_changes.iter_ordered() {
            debug_assert!(
                change.version() >= old_optimized_segment_version,
                "proxied index change should have newer version than segment",
            );
            match change {
                ProxyIndexChange::Create(schema, version) => {
                    optimized_segment.create_field_index(
                        *version,
                        field_name,
                        Some(schema),
                        hw_counter,
                    )?;
                }
                ProxyIndexChange::Delete(version) => {
                    optimized_segment.delete_field_index(*version, field_name)?;
                }
                ProxyIndexChange::DeleteIfIncompatible(version, schema) => {
                    optimized_segment
                        .delete_field_index_if_incompatible(*version, field_name, schema)?;
                }
            }
            check_process_stopped(stopped)?;
        }

        for (point_id, versions) in deleted_points_snapshot {
            optimized_segment
                .delete_point(versions.operation_version, point_id, hw_counter)
                .unwrap();
        }

        Ok(optimized_segment)
    }

    /// Test wrapper for [`SegmentOptimizer::optimize`].
    #[cfg(test)]
    fn optimize_for_test(&self, segments: LockedSegmentHolder, ids: Vec<SegmentId>) -> usize {
        let permit_cpu_count = segment::index::hnsw_index::num_rayon_threads(0);
        let budget = ResourceBudget::new(permit_cpu_count, permit_cpu_count);
        self.optimize(
            segments,
            ids,
            Uuid::new_v4(),
            budget.try_acquire(0, permit_cpu_count).unwrap(),
            budget,
            &AtomicBool::new(false),
            ProgressTracker::new_for_test(),
            Box::new(|| ()),
        )
        .unwrap()
    }

    /// Performs optimization of collections's segments.
    ///
    /// It will merge multiple segments into a single new segment.
    ///
    /// # Result
    ///
    /// New optimized segment should be added into `segments`.
    /// If there were any record changes during the optimization - an additional plain segment will be created.
    ///
    /// Returns id of the created optimized segment. If no optimization was done - returns None
    #[expect(clippy::too_many_arguments)]
    fn optimize(
        &self,
        segment_holder: LockedSegmentHolder,
        input_segment_ids: Vec<SegmentId>, // Segment ids to optimize/merge into one
        output_segment_uuid: Uuid,         // The UUID of the resulting optimized segment
        permit: ResourcePermit,
        resource_budget: ResourceBudget,
        stopped: &AtomicBool,
        progress: ProgressTracker,
        on_successful_start: Box<dyn FnOnce()>,
    ) -> CollectionResult<usize> {
        check_process_stopped(stopped)?;

        let mut timer = ScopeDurationMeasurer::new(self.get_telemetry_counter());
        timer.set_success(false);

        // On the one hand - we want to check consistently if all provided segments are
        // available for optimization (not already under one) and we want to do it before creating a temp segment
        // which is an expensive operation. So we can't unlock `segments` after the check and before the insert.
        //
        // On the other hand - we do not want to hold write lock during the segment creation.
        // Solution in the middle - is an upgradable lock. It ensures consistency after the check and allows to perform read operation.
        let segment_holder_read = segment_holder.upgradable_read();

        // Find appendable segments other than optimized ones
        //
        // If there are such segments - we can avoid creating a temp segment
        // If there are none, we need to create a new empty segment to allow writes during optimization
        let appendable_segments_ids = segment_holder_read.appendable_segments_ids();
        let has_appendable_segments_except_optimized = appendable_segments_ids
            .iter()
            .any(|id| !input_segment_ids.contains(id));
        let need_extra_cow_segment = !has_appendable_segments_except_optimized;

        let input_segments: Vec<_> = input_segment_ids
            .iter()
            .cloned()
            .map(|id| segment_holder_read.get(id))
            .filter_map(|x| x.cloned())
            .collect();

        // Check if all segments are not under other optimization or some ids are missing
        let all_segments_ok = input_segments.len() == input_segment_ids.len()
            && input_segments
                .iter()
                .all(|s| matches!(s, LockedSegment::Original(_)));

        if !all_segments_ok {
            // Cancel the optimization
            return Ok(0);
        }

        // Check that we have enough disk space for optimization
        self.check_segments_size(&input_segments)?;

        check_process_stopped(stopped)?;

        on_successful_start();

        let hw_counter = HardwareCounterCell::disposable(); // Internal operation, no measurement needed!

        let extra_cow_segment_opt = need_extra_cow_segment
            .then(|| self.temp_segment(false))
            .transpose()?;

        let mut proxies = Vec::new();
        for sg in input_segments.iter() {
            let proxy = ProxySegment::new(sg.clone());
            // Wrapped segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            if let Some(extra_cow_segment) = &extra_cow_segment_opt {
                proxy.replicate_field_indexes(0, &hw_counter, extra_cow_segment)?;
            }

            proxies.push(proxy);
        }

        // Save segment version once all payload indices have been converted
        // If this ends up not being saved due to a crash, the segment will not be used
        match &extra_cow_segment_opt {
            Some(LockedSegment::Original(segment)) => {
                let segment_path = &segment.read().segment_path;
                SegmentVersion::save(segment_path)?;
            }
            Some(LockedSegment::Proxy(_)) => unreachable!(),
            None => {}
        }

        let mut locked_proxies: Vec<LockedSegment> = Vec::with_capacity(proxies.len());

        let (proxy_ids, cow_segment_id_opt, counter_handler): (Vec<_>, _, _) = {
            // Exclusive lock for the segments operations.
            let mut segment_holder_write = RwLockUpgradableReadGuard::upgrade(segment_holder_read);
            let mut proxy_ids = Vec::new();
            for (proxy, idx) in proxies.into_iter().zip(input_segment_ids.iter().cloned()) {
                // During optimization, we expect that logical point data in the wrapped segment is
                // not changed at all. But this would be possible if we wrap another proxy segment,
                // because it can share state through it's write segment. To prevent this we assert
                // here that we only wrap non-proxy segments.
                // Also helps to ensure the delete propagation behavior in
                // `optimize_segment_propagate_changes` remains  sound.
                // See: <https://github.com/qdrant/qdrant/pull/7208>
                debug_assert!(
                    matches!(proxy.wrapped_segment, LockedSegment::Original(_)),
                    "during optimization, wrapped segment in a proxy segment must not be another proxy segment",
                );

                // replicate_field_indexes for the second time,
                // because optimized segments could have been changed.
                // The probability is small, though,
                // so we can afford this operation under the full collection write lock
                if let Some(extra_cow_segment) = &extra_cow_segment_opt {
                    proxy.replicate_field_indexes(0, &hw_counter, extra_cow_segment)?; // Slow only in case the index is change in the gap between two calls
                }

                let locked_proxy = LockedSegment::from(proxy);
                segment_holder_write.replace(idx, locked_proxy.clone())?;

                proxy_ids.push(idx);
                locked_proxies.push(locked_proxy);
            }

            let cow_segment_id_opt = extra_cow_segment_opt
                .map(|extra_cow_segment| segment_holder_write.add_new_locked(extra_cow_segment));

            // Increase optimization counter right after replacing segments with proxies.
            // We'll decrease it after inserting the optimized segment.
            let counter_handler = segment_holder_write.running_optimizations.inc();

            (proxy_ids, cow_segment_id_opt, counter_handler)
        };

        // SLOW PART: create single optimized segment and propagate all new changes to it
        let result = self.optimize_segment_propagate_changes(
            input_segments,
            output_segment_uuid,
            &locked_proxies,
            permit,
            resource_budget,
            stopped,
            &hw_counter,
            progress,
        );

        let (optimized_segment, deleted_points) = match result {
            Ok(segment) => segment,
            Err(err) => {
                // Properly cancel optimization on all error kinds
                // Unwrap proxies and add temp segment to holder
                self.unwrap_proxy(&segment_holder, &proxy_ids)?;
                return Err(err);
            }
        };

        // Fast part: blocks updates, propagates rest of the changes, swaps optimized segment
        let points_count = match self.finish_optimization(
            &segment_holder,
            locked_proxies,
            optimized_segment,
            &deleted_points,
            &proxy_ids,
            cow_segment_id_opt,
            stopped,
            &hw_counter,
        ) {
            Ok(points_count) => points_count,
            Err(err) => {
                // Properly cancel optimization on all error kinds
                // Unwrap proxies and add temp segment to holder
                self.unwrap_proxy(&segment_holder, &proxy_ids)?;
                return Err(err);
            }
        };

        drop(counter_handler);

        timer.set_success(true);

        Ok(points_count)
    }

    /// Accumulates approximate set of points deleted in a given set of proxies
    ///
    /// This list is not synchronized (if not externally enforced),
    /// but guarantees that it contains at least all points deleted in the proxies
    /// before the call to this function.
    fn proxy_deleted_points(&self, proxies: &[LockedSegment]) -> DeletedPoints {
        let mut deleted_points = DeletedPoints::new();
        for proxy_segment in proxies {
            match proxy_segment {
                LockedSegment::Original(_) => {
                    log::error!("Reading raw segment, while proxy expected");
                    debug_assert!(false, "Reading raw segment, while proxy expected");
                }
                LockedSegment::Proxy(proxy) => {
                    let proxy_read = proxy.read();
                    for (point_id, versions) in proxy_read.get_deleted_points() {
                        let entry = deleted_points.entry(*point_id).or_insert(*versions);
                        entry.operation_version =
                            entry.operation_version.max(versions.operation_version);
                        entry.local_version = entry.local_version.max(versions.local_version);
                    }
                }
            }
        }
        deleted_points
    }

    /// Accumulates index changes made in a given set of proxies
    ///
    /// This list is not synchronized (if not externally enforced),
    /// but guarantees that it contains at least all index changes made in the proxies
    /// before the call to this function.
    fn proxy_index_changes(&self, proxies: &[LockedSegment]) -> ProxyIndexChanges {
        let mut index_changes = ProxyIndexChanges::default();
        for proxy_segment in proxies {
            match proxy_segment {
                LockedSegment::Original(_) => {
                    log::error!("Reading raw segment, while proxy expected");
                    debug_assert!(false, "Reading raw segment, while proxy expected");
                }
                LockedSegment::Proxy(proxy) => {
                    let proxy_read = proxy.read();
                    index_changes.merge(proxy_read.get_index_changes())
                }
            }
        }
        index_changes
    }

    /// Create a single optimized segment from the given segments.
    ///
    /// All point deletes or payload index changes made during optimization are propagated to the
    /// optimized segment at the very end.
    ///
    /// This internally takes a write lock on the segments holder to block new updates when
    /// finalizing optimization. It is returned so that the optimized segment can be inserted and
    /// proxy segments can be dissolved before releasing the lock.
    ///
    /// # Warning
    ///
    /// This function is slow and must only be used on an optimization worker.
    #[allow(clippy::too_many_arguments)]
    fn optimize_segment_propagate_changes(
        &self,
        optimizing_segments: Vec<LockedSegment>,
        output_segment_uuid: Uuid,
        proxies: &[LockedSegment],
        permit: ResourcePermit, // IO resources for copying data
        resource_budget: ResourceBudget,
        stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        progress: ProgressTracker,
    ) -> CollectionResult<(Segment, DeletedPoints)> {
        check_process_stopped(stopped)?;

        // ---- SLOW PART -----

        let optimized_segment = self.build_new_segment(
            &optimizing_segments,
            output_segment_uuid,
            proxies,
            permit,
            resource_budget,
            stopped,
            hw_counter,
            progress,
        )?;

        // Avoid unnecessary point removing in the critical section:
        // - save already removed points while avoiding long read locks
        // - exclude already removed points from post-optimization removing
        let already_remove_points = {
            let mut all_removed_points = self.proxy_deleted_points(proxies);
            for existing_point in optimized_segment.iter_points() {
                all_removed_points.remove(&existing_point);
            }
            all_removed_points
        };

        // ---- SLOW PART ENDS HERE -----

        check_process_stopped(stopped)?;

        Ok((optimized_segment, already_remove_points))
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_optimization(
        &self,
        segment_holder: &LockedSegmentHolder,
        locked_proxies: Vec<LockedSegment>,
        mut optimized_segment: Segment,
        already_remove_points: &DeletedPoints,
        proxy_ids: &[SegmentId],
        cow_segment_id_opt: Option<SegmentId>,
        stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> CollectionResult<usize> {
        // This block locks all write operations with collection. It should be fast.
        let upgradable_segment_holder = segment_holder.upgradable_read();

        // This mutex prevents update operations, which could create inconsistency during transition.
        let update_guard = segment_holder.acquire_updates_lock();

        let proxy_index_changes = self.proxy_index_changes(&locked_proxies);

        // Apply index changes before point deletions
        // Point deletions bump the segment version, can cause index changes to be ignored
        for (field_name, change) in proxy_index_changes.iter_ordered() {
            // Warn: change version might be lower than the segment version,
            // because we might already applied the change earlier in optimization.
            // Applied optimizations are not removed from `proxy_index_changes`.
            match change {
                ProxyIndexChange::Create(schema, version) => {
                    optimized_segment.create_field_index(
                        *version,
                        field_name,
                        Some(schema),
                        hw_counter,
                    )?;
                }
                ProxyIndexChange::Delete(version) => {
                    optimized_segment.delete_field_index(*version, field_name)?;
                }
                ProxyIndexChange::DeleteIfIncompatible(version, schema) => {
                    optimized_segment
                        .delete_field_index_if_incompatible(*version, field_name, schema)?;
                }
            }

            check_process_stopped(stopped)?;
        }

        let deleted_points = self.proxy_deleted_points(&locked_proxies);

        let points_diff = deleted_points
            .iter()
            .filter(|&(point_id, _version)| !already_remove_points.contains_key(point_id));

        for (&point_id, &versions) in points_diff {
            // In this specific case we're sure logical point data in the wrapped segment is not
            // changed at all. We ensure this with an assertion at time of proxying, which makes
            // sure we only wrap original segments. Because we're sure logical data doesn't change,
            // we also know pending deletes are always newer. Here we assert that's actually the
            // case.
            debug_assert!(
                versions.operation_version
                    >= optimized_segment.point_version(point_id).unwrap_or(0),
                "proxied point deletes should have newer version than point in segment",
            );
            optimized_segment
                .delete_point(versions.operation_version, point_id, hw_counter)
                .unwrap();
        }

        // Replace proxy segments with new optimized segment
        let point_count = optimized_segment.available_point_count();

        let mut writable_segment_holder =
            RwLockUpgradableReadGuard::upgrade(upgradable_segment_holder);

        let (_, proxies) = writable_segment_holder.swap_new(optimized_segment, proxy_ids);
        debug_assert_eq!(
            proxies.len(),
            proxy_ids.len(),
            "swapped different number of proxies on unwrap, missing or incorrect segment IDs?",
        );

        if let Some(cow_segment_id) = cow_segment_id_opt {
            // Temp segment might be taken into another parallel optimization
            // so it is not necessary exist by this time
            writable_segment_holder.remove_segment_if_not_needed(cow_segment_id)?;
        }

        drop(writable_segment_holder);
        // Allow updates again
        drop(update_guard);

        // Drop all pointers to proxies, so we can de-arc them
        drop(locked_proxies);

        // Only remove data after we ensure the consistency of the collection.
        // If remove fails - we will still have operational collection with reported error.
        for proxy in proxies {
            proxy.drop_data()?;
        }

        Ok(point_count)
    }
}

pub struct OptimizationPlanner<'a> {
    /// Segments that could be scheduled for optimization.
    remaining: BTreeMap<SegmentId, &'a Arc<RwLock<Segment>>>,

    /// The resulting optimization plan.
    ///
    /// Each entry contains
    /// - a batch of segments to be optimized/merged into a new segment,
    /// - an optional optimizer so you can call [`SegmentOptimizer::optimize`]
    ///   on it later.
    scheduled: Vec<(Option<Arc<Optimizer>>, Vec<SegmentId>)>,

    /// Amount of currently running optimizations. We'll assume that each of
    /// them eventually produces one new segment.
    running: usize,

    /// This goes into [`Self::scheduled`].
    /// Should be set before calling [`Self::plan`].
    optimizer: Option<Arc<Optimizer>>,
}

impl<'a> OptimizationPlanner<'a> {
    pub fn new<I>(running: usize, segments: I) -> Self
    where
        I: IntoIterator<Item = (SegmentId, &'a Arc<RwLock<Segment>>)>,
    {
        Self {
            remaining: segments.into_iter().collect(),
            scheduled: Vec::new(),
            running,
            optimizer: None,
        }
    }

    pub fn remaining(&self) -> &BTreeMap<SegmentId, &'a Arc<RwLock<Segment>>> {
        &self.remaining
    }

    /// Returns [`Self::scheduled`], but without `Option<Arc<Optimizer>>` part.
    #[cfg(test)]
    pub fn into_scheduled_for_test(self) -> Vec<Vec<SegmentId>> {
        self.scheduled
            .into_iter()
            .map(|(_, segments)| segments)
            .collect_vec()
    }

    /// The expected resulting number of segments after the optimization plan is
    /// executed, and all currently running optimizations are finished.
    pub fn expected_segments_number(&self) -> usize {
        self.remaining.len() + self.scheduled.len() + self.running
    }

    /// Schedule this batch of segments to be optimized/merged into new segment.
    pub fn plan(&mut self, segments: Vec<SegmentId>) {
        debug_assert!(!segments.is_empty());
        for segment_id in &segments {
            let removed = self.remaining.remove(segment_id).is_some();
            debug_assert!(removed);
        }
        self.scheduled.push((self.optimizer.clone(), segments));
    }
}

/// Plans optimizations for the given segments and optimizers.
///
/// Returns a list of scheduled optimizations, each containing the
/// corresponding optimizer and a batch of segment IDs to be optimized.
pub fn plan_optimizations(
    segments: &SegmentHolder,
    optimizers: &[Arc<Optimizer>],
) -> Vec<(Arc<Optimizer>, Vec<SegmentId>)> {
    let mut planner = OptimizationPlanner::new(
        segments.running_optimizations.count(),
        segments.iter_original(),
    );
    for optimizer in optimizers {
        planner.optimizer = Some(Arc::clone(optimizer));
        optimizer.plan_optimizations(&mut planner);
    }
    planner
        .scheduled
        .into_iter()
        .inspect(|(optimizer, _segments)| debug_assert!(optimizer.is_some()))
        .filter_map(|(optimizer, segments)| Some((optimizer?, segments)))
        .collect()
}
