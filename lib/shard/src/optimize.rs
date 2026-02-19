//! Optimization execution module.
//!
//! Core optimization execution logic that is agnostic to collection-level policies.
//! The collection layer provides the strategy via `OptimizationStrategy`.

use std::collections::HashSet;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::budget::{ResourceBudget, ResourcePermit};
use common::bytes::bytes_to_human;
use common::counter::hardware_counter::HardwareCounterCell;
use common::disk::dir_disk_size;
use common::progress_tracker::ProgressTracker;
use common::storage_version::StorageVersion;
use fs_err as fs;
use itertools::Itertools;
use parking_lot::{Mutex, RwLockUpgradableReadGuard};
use segment::common::operation_error::{OperationResult, check_process_stopped};
use segment::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::entry::NonAppendableSegmentEntry;
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::segment_builder::SegmentBuilder;
use uuid::Uuid;

use crate::locked_segment::LockedSegment;
use crate::proxy_segment::{DeletedPoints, ProxyIndexChange, ProxyIndexChanges, ProxySegment};
use crate::segment_holder::SegmentId;
use crate::segment_holder::locked::LockedSegmentHolder;

/// Result of optimization execution
#[derive(Debug)]
pub struct OptimizationResult {
    /// Number of points in the optimized segment
    pub points_count: usize,
}

/// Configuration for optimization execution paths
#[derive(Debug, Clone)]
pub struct OptimizationPaths {
    /// Path where final segments are stored
    pub segments_path: PathBuf,
    /// Path for temporary segments during optimization
    pub temp_path: PathBuf,
}

/// Strategy interface for creating segment builders and temporary segments used by optimization.
///
/// This abstracts policy decisions (what config to use) from the execution logic.
pub trait OptimizationStrategy: Send {
    /// Create a segment builder for the given input segments.
    fn create_segment_builder(
        &self,
        input_segments: &[LockedSegment],
    ) -> OperationResult<SegmentBuilder>;

    /// Create a temporary COW segment for writes during optimization.
    fn create_temp_segment(&self) -> OperationResult<LockedSegment>;
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
pub fn unwrap_proxy(
    segments: &LockedSegmentHolder,
    proxy_ids: &[SegmentId],
) -> OperationResult<()> {
    let mut segments_lock = segments.write();
    for &proxy_id in proxy_ids {
        if let Some(proxy_segment_ref) = segments_lock.get(proxy_id) {
            let locked_proxy_segment = proxy_segment_ref.clone();
            match locked_proxy_segment {
                LockedSegment::Original(_) => {
                    // Already unwrapped. It should not actually be here
                    log::warn!("Attempt to unwrap raw segment! Should not happen.");
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

/// Accumulates approximate set of points deleted in a given set of proxies
///
/// This list is not synchronized (if not externally enforced),
/// but guarantees that it contains at least all points deleted in the proxies
/// before the call to this function.
pub fn proxy_deleted_points(proxies: &[LockedSegment]) -> DeletedPoints {
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
pub fn proxy_index_changes(proxies: &[LockedSegment]) -> ProxyIndexChanges {
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

/// Function to wrap slow part of optimization. Performs proxy rollback in case of cancellation.
/// Warn: this function might be _VERY_ CPU intensive,
/// so it is necessary to avoid any locks inside this part of the code
///
/// Returns the newly constructed optimized segment.
#[allow(clippy::too_many_arguments)]
fn build_new_segment<F: ?Sized + OptimizationStrategy>(
    factory: &F,
    input_segments: &[LockedSegment], // Segments to optimize/merge into one
    output_segment_uuid: Uuid,        // The UUID of the resulting optimized segment
    proxies: &[LockedSegment],
    permit: ResourcePermit, // IO resources for copying data
    resource_budget: ResourceBudget,
    stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
    progress: ProgressTracker,
    segments_path: &Path,
) -> OperationResult<Segment> {
    let mut segment_builder = factory.create_segment_builder(input_segments)?;

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

    let index_changes = proxy_index_changes(proxies);

    // Apply index changes to segment builder
    // Indexes are only used for defragmentation in segment builder, so versions are ignored
    for (field_name, change) in index_changes.iter_unordered() {
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
            segment::common::operation_error::OperationError::cancelled(
                "optimization cancelled while waiting for budget",
            )
        })?;
    drop(progress_wait_permit);

    let mut rng = rand::rng();
    let mut optimized_segment = segment_builder.build(
        segments_path,
        output_segment_uuid,
        indexing_permit,
        stopped,
        &mut rng,
        hw_counter,
        progress,
    )?;

    // Delete points
    let deleted_points_snapshot = proxy_deleted_points(proxies);
    let index_changes = proxy_index_changes(proxies);

    // Apply index changes before point deletions
    // Point deletions bump the segment version, can cause index changes to be ignored
    let old_optimized_segment_version = optimized_segment.version();
    for (field_name, change) in index_changes.iter_ordered() {
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
fn optimize_segment_propagate_changes<F: ?Sized + OptimizationStrategy>(
    factory: &F,
    optimizing_segments: Vec<LockedSegment>,
    output_segment_uuid: Uuid,
    proxies: &[LockedSegment],
    permit: ResourcePermit, // IO resources for copying data
    resource_budget: ResourceBudget,
    stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
    progress: ProgressTracker,
    segments_path: &Path,
) -> OperationResult<(Segment, DeletedPoints)> {
    check_process_stopped(stopped)?;

    // ---- SLOW PART -----

    let optimized_segment = build_new_segment(
        factory,
        &optimizing_segments,
        output_segment_uuid,
        proxies,
        permit,
        resource_budget,
        stopped,
        hw_counter,
        progress,
        segments_path,
    )?;

    // Avoid unnecessary point removing in the critical section:
    // - save already removed points while avoiding long read locks
    // - exclude already removed points from post-optimization removing
    let already_remove_points = {
        let mut all_removed_points = proxy_deleted_points(proxies);
        for existing_point in optimized_segment.iter_points() {
            all_removed_points.remove(&existing_point);
        }
        all_removed_points
    };

    // ---- SLOW PART ENDS HERE -----

    check_process_stopped(stopped)?;

    Ok((optimized_segment, already_remove_points))
}

/// Finish optimization: propagate remaining changes and swap segments
#[allow(clippy::too_many_arguments)]
fn finish_optimization(
    segment_holder: &LockedSegmentHolder,
    locked_proxies: Vec<LockedSegment>,
    mut optimized_segment: Segment,
    already_remove_points: &DeletedPoints,
    proxy_ids: &[SegmentId],
    cow_segment_id_opt: Option<SegmentId>,
    stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<usize> {
    // This block locks all write operations with collection. It should be fast.

    let upgradable_segment_holder = segment_holder.upgradable_read();

    // This mutex prevents update operations, which could create inconsistency during transition.
    let update_guard = segment_holder.acquire_updates_lock();

    let index_changes = proxy_index_changes(&locked_proxies);

    // Apply index changes before point deletions
    // Point deletions bump the segment version, can cause index changes to be ignored
    for (field_name, change) in index_changes.iter_ordered() {
        match change {
            // Warn: change version might be lower than the segment version,
            // because we might already applied the change earlier in optimization.
            // Applied optimizations are not removed from `proxy_index_changes`.
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

    let deleted_points = proxy_deleted_points(&locked_proxies);
    let points_diff = deleted_points
        .iter()
        .filter(|&(point_id, _)| !already_remove_points.contains_key(point_id));

    for (&point_id, &versions) in points_diff {
        // In this specific case we're sure logical point data in the wrapped segment is not
        // changed at all. We ensure this with an assertion at time of proxying, which makes
        // sure we only wrap original segments. Because we're sure logical data doesn't change,
        // we also know pending deletes are always newer. Here we assert that's actually the
        // case.
        debug_assert!(
            versions.operation_version >= optimized_segment.point_version(point_id).unwrap_or(0),
            "proxied point deletes should have newer version than point in segment",
        );
        optimized_segment
            .delete_point(versions.operation_version, point_id, hw_counter)
            .unwrap();
    }

    // Replace proxy segments with new optimized segment
    let point_count = optimized_segment.available_point_count();
    let mut writable_segment_holder = RwLockUpgradableReadGuard::upgrade(upgradable_segment_holder);

    let (_, proxies) = writable_segment_holder.swap_new(optimized_segment, proxy_ids);
    debug_assert_eq!(
        proxies.len(),
        proxy_ids.len(),
        "swapped different number of proxies"
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

/// Returns error if segment size is larger than available disk space
fn check_segments_size(
    optimizer_name: &str,
    optimizing_segments: &[LockedSegment],
    temp_path: &Path,
) -> OperationResult<()> {
    // Counting up how much space do the segments being optimized actually take on the fs.
    // If there was at least one error while reading the size, this will be `None`.
    let mut space_occupied = Some(0u64);

    for segment in optimizing_segments {
        match segment {
            LockedSegment::Original(segment) => {
                let locked_segment = segment.read();
                space_occupied = space_occupied.and_then(|acc| {
                    match dir_disk_size(locked_segment.data_path()) {
                        Ok(size) => Some(size + acc),
                        Err(err) => {
                            log::debug!(
                                "Could not estimate size of segment `{}`: {}",
                                locked_segment.data_path().display(),
                                err
                            );
                            None
                        }
                    }
                });
            }
            LockedSegment::Proxy(_) => {
                return Err(
                    segment::common::operation_error::OperationError::service_error(
                        "Proxy segment is not expected here",
                    ),
                );
            }
        }
    }

    let space_needed = space_occupied.map(|x| 2 * x);

    // Ensure temp_path exists
    if !temp_path.exists() {
        fs::create_dir_all(temp_path).map_err(|err| {
            segment::common::operation_error::OperationError::service_error(format!(
                "Could not create temp directory `{}`: {}",
                temp_path.display(),
                err
            ))
        })?;
    }

    let space_available = match fs4::available_space(temp_path) {
        Ok(available) => Some(available),
        Err(err) => {
            log::debug!(
                "Could not estimate available storage space in `{}`: {}",
                temp_path.display(),
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
                return Err(
                    segment::common::operation_error::OperationError::service_error(format!(
                        "Not enough space available for optimization, needed: {}, available: {}",
                        bytes_to_human(space_needed as usize),
                        bytes_to_human(space_available as usize),
                    )),
                );
            }
        }
        _ => {
            log::warn!(
                "Could not estimate available storage space in `{optimizer_name}`; will try optimizing anyway",
            );
        }
    }

    Ok(())
}

/// Performs optimization of segments (merge / reindex / vacuum, etc.)
#[allow(clippy::too_many_arguments)]
pub fn execute_optimization<F: ?Sized + OptimizationStrategy>(
    optimizer_name: &'static str,
    segment_holder: LockedSegmentHolder,
    input_segment_ids: Vec<SegmentId>,
    output_segment_uuid: Uuid,
    paths: &OptimizationPaths,
    permit: ResourcePermit,
    resource_budget: ResourceBudget,
    stopped: &AtomicBool,
    progress: ProgressTracker,
    telemetry_counter: &Mutex<OperationDurationsAggregator>,
    factory: &F,
    on_successful_start: Box<dyn FnOnce()>,
) -> OperationResult<OptimizationResult> {
    check_process_stopped(stopped)?;

    let mut timer = ScopeDurationMeasurer::new(telemetry_counter);
    timer.set_success(false);

    let segment_holder_read = segment_holder.upgradable_read();

    // Determine if we need a separate COW segment for writes
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

    // Check that all segments exist and are not already under optimization
    let all_segments_ok = input_segments.len() == input_segment_ids.len()
        && input_segments
            .iter()
            .all(|s| matches!(s, LockedSegment::Original(_)));
    if !all_segments_ok {
        return Ok(OptimizationResult { points_count: 0 });
    }

    check_segments_size(optimizer_name, &input_segments, &paths.temp_path)?;

    check_process_stopped(stopped)?;

    on_successful_start();

    let hw_counter = HardwareCounterCell::disposable();

    let extra_cow_segment_opt = need_extra_cow_segment
        .then(|| factory.create_temp_segment())
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
                "during optimization, wrapped segment must not be another proxy segment"
            );

            // replicate_field_indexes for the second time,
            // because optimized segments could have been changed.
            // The probability is small, though,
            // so we can afford this operation under the full collection write lock
            if let Some(extra_cow_segment) = &extra_cow_segment_opt {
                proxy.replicate_field_indexes(0, &hw_counter, extra_cow_segment)?;
            }

            let locked_proxy = LockedSegment::from(proxy);
            segment_holder_write.replace(idx, locked_proxy.clone())?; // Slow only in case the index is change in the gap between two calls

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
    let build_result = optimize_segment_propagate_changes(
        factory,
        input_segments,
        output_segment_uuid,
        &locked_proxies,
        permit,
        resource_budget,
        stopped,
        &hw_counter,
        progress,
        &paths.segments_path,
    );

    let (optimized_segment, already_remove_points) = match build_result {
        Ok(result) => result,
        Err(err) => {
            // Properly cancel optimization on all error kinds
            // Unwrap proxies and add temp segment to holder
            unwrap_proxy(&segment_holder, &proxy_ids)?;
            return Err(err);
        }
    };

    // Fast part: blocks updates, propagates rest of the changes, swaps optimized segment
    let points_count = match finish_optimization(
        &segment_holder,
        locked_proxies,
        optimized_segment,
        &already_remove_points,
        &proxy_ids,
        cow_segment_id_opt,
        stopped,
        &hw_counter,
    ) {
        Ok(points_count) => points_count,
        Err(err) => {
            // Properly cancel optimization on all error kinds
            // Unwrap proxies and add temp segment to holder
            unwrap_proxy(&segment_holder, &proxy_ids)?;
            return Err(err);
        }
    };

    drop(counter_handler);

    timer.set_success(true);

    Ok(OptimizationResult { points_count })
}
