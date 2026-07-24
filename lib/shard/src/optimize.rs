//! Optimization execution module.
//!
//! Core optimization execution logic that is agnostic to collection-level policies.
//! The collection layer provides the strategy via `OptimizationStrategy`.

use std::collections::HashSet;
use std::debug_assert_matches;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::budget::{ResourceBudget, ResourcePermit};
use common::bytes::bytes_to_human;
use common::counter::hardware_counter::HardwareCounterCell;
use common::disk::dir_disk_size;
use common::fs::safe_delete_with_suffix;
use common::progress_tracker::ProgressTracker;
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use fs_err as fs;
use itertools::Itertools;
use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::{Mutex, RwLockUpgradableReadGuard};
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::common::operation_time_statistics::{
    OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::entry::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry as _, StorageSegmentEntry as _,
};
use segment::segment::{Segment, SegmentVersion};
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{PointIdType, VectorNameBuf};
use uuid::Uuid;

use crate::locked_segment::LockedSegment;
use crate::proxy_segment::{
    DeletedPoints, IntendedVector, ProxyIndexChange, ProxyIndexChanges, ProxyVectorNameChanges,
    UnsyncedProxySegment,
};
use crate::segment_holder::SegmentId;
use crate::segment_holder::locked::LockedSegmentHolder;
use crate::segment_manifest::NewSegmentToken;

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
    ///
    /// Returns the segment together with the [`NewSegmentToken`] obliging the caller to register it
    /// in the manifest once it is published into the holder.
    fn create_temp_segment(&self) -> OperationResult<(LockedSegment, NewSegmentToken)>;

    /// Vector names currently present in the live collection schema, if a live source is wired in.
    ///
    /// `None` means the live schema is unknown, in which case the merge must treat a source vector
    /// name absent from the target conservatively (cancel). When present, it lets the builder tell a
    /// genuinely deleted vector (safe to prune) from the CreateVectorName-vs-optimizer race (must
    /// cancel). See [`SegmentBuilder::set_live_vector_names`].
    fn live_vector_names(&self) -> Option<HashSet<VectorNameBuf>>;
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

/// Remove a partially-built optimized segment left on disk when optimization is cancelled.
///
/// `SegmentBuilder::build` already renamed the new segment into `segments_path`, and dropping the
/// in-memory `Segment` only releases resources without deleting the directory (deletion is the
/// explicit `drop_data`). A graceful cancellation always happens before the segment is swapped
/// into the holder, so it is never live at this point and is safe to delete. Best-effort: failures
/// are logged, not propagated, so they can't mask the original cancellation error.
///
/// Note: this in-process cleanup is not crash safe. If the process dies between `build` and this
/// call, the orphaned segment directory is left on disk and must be reclaimed on restart instead.
/// See https://github.com/qdrant/qdrant/pull/9217#pullrequestreview-4381966021
fn cleanup_cancelled_optimized_segment(segments_path: &Path, output_segment_uuid: Uuid) {
    let orphan_path = segments_path.join(output_segment_uuid.to_string());
    if !orphan_path.exists() {
        return;
    }
    if let Err(err) = safe_delete_with_suffix(&orphan_path) {
        log::warn!(
            "Failed to remove cancelled optimized segment at {}: {err}",
            orphan_path.display(),
        );
    }
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

/// Accumulates vector name changes made in a given set of proxies
///
/// This list is not synchronized (if not externally enforced),
/// but guarantees that it contains at least all vector name changes made in the proxies
/// before the call to this function.
pub fn proxy_vector_name_changes(proxies: &[LockedSegment]) -> ProxyVectorNameChanges {
    let mut changes = ProxyVectorNameChanges::default();
    for proxy_segment in proxies {
        match proxy_segment {
            LockedSegment::Original(_) => {
                log::error!("Reading raw segment, while proxy expected");
                debug_assert!(false, "Reading raw segment, while proxy expected");
            }
            LockedSegment::Proxy(proxy) => {
                let proxy_read = proxy.read();
                changes.merge(proxy_read.get_vector_name_changes())
            }
        }
    }
    changes
}

/// Function to wrap slow part of optimization. Performs proxy rollback in case of cancellation.
/// Warn: this function might be _VERY_ CPU intensive,
/// so it is necessary to avoid any locks inside this part of the code
///
/// `temp_path` and `space_estimate` are used by [`recheck_free_space`] to
/// re-validate available disk between the long phases (`update`,
/// `populate_vector_storages`, `build`) so that we abort with a clean
/// "No space left on device" error rather than crashing inside the
/// segment builder if the disk fills up mid-optimization. The watchdog
/// enforces remaining headroom (precheck_available - space_needed), so
/// it does NOT trip on the optimizer's own expected writes.
///
/// Returns the newly constructed optimized segment.
#[allow(clippy::too_many_arguments)]
fn build_new_segment<F: ?Sized + OptimizationStrategy>(
    factory: &F,
    optimizer_name: &str,
    input_segments: &[LockedSegment], // Segments to optimize/merge into one
    output_segment_uuid: Uuid,        // The UUID of the resulting optimized segment
    deferred_internal_id: Option<PointOffsetType>,
    proxies: &[LockedSegment],
    permit: ResourcePermit, // IO resources for copying data
    resource_budget: ResourceBudget,
    stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
    progress: ProgressTracker,
    segments_path: &Path,
    temp_path: &Path,
    space_estimate: OptimizationSpaceEstimate,
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

    // Wire in the live collection schema so the merge can distinguish a deleted vector (prune it)
    // from the CreateVectorName race (cancel). Read here, after the proxy install froze the source
    // segments: the schema is persisted before a vector-name op reaches the segments, so any name a
    // frozen source carries is guaranteed visible in this read, and no concurrent create can be
    // missed (which would otherwise cause a wrong prune).
    if let Some(live_vector_names) = factory.live_vector_names() {
        segment_builder.set_live_vector_names(live_vector_names);
    }

    {
        progress_copy_data.start();
        let segment_guards = segments.iter().map(|segment| segment.read()).collect_vec();
        segment_builder.update(
            &segment_guards.iter().map(Deref::deref).collect_vec(),
            stopped,
            hw_counter,
        )?;
        drop(progress_copy_data);
    }

    // Mid-flight watchdog: copying segment data may have consumed a lot of
    // disk, and other tenants may have done the same in parallel. Re-check
    // before we kick off the (longer) HNSW indexing phase. We compare
    // against expected headroom, not the original estimate, because the
    // optimizer itself is expected to consume the estimate by design.
    recheck_free_space(optimizer_name, temp_path, space_estimate)?;

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

    // Mid-flight watchdog: vector storage population may have written a
    // significant amount of data. Re-check before the HNSW build phase,
    // which is both the longest phase and the one that produces the link
    // tables that historically blow up disk usage past the pre-flight
    // estimate (see review of <https://github.com/qdrant/qdrant/pull/4578>).
    recheck_free_space(optimizer_name, temp_path, space_estimate)?;

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
        deferred_internal_id,
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
    optimizer_name: &str,
    optimizing_segments: Vec<LockedSegment>,
    output_segment_uuid: Uuid,
    deferred_internal_id: Option<PointOffsetType>,
    proxies: &[LockedSegment],
    permit: ResourcePermit, // IO resources for copying data
    resource_budget: ResourceBudget,
    stopped: &AtomicBool,
    hw_counter: &HardwareCounterCell,
    progress: ProgressTracker,
    segments_path: &Path,
    temp_path: &Path,
    space_estimate: OptimizationSpaceEstimate,
) -> OperationResult<(Segment, DeletedPoints)> {
    check_process_stopped(stopped)?;

    // ---- SLOW PART -----

    let optimized_segment = build_new_segment(
        factory,
        optimizer_name,
        &optimizing_segments,
        output_segment_uuid,
        deferred_internal_id,
        proxies,
        permit,
        resource_budget,
        stopped,
        hw_counter,
        progress,
        segments_path,
        temp_path,
        space_estimate,
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

    // Apply vector name changes before index and point changes
    // New named vectors must exist before indexes or points reference them
    let old_optimized_segment_version = optimized_segment.version();
    let vector_name_changes = proxy_vector_name_changes(&locked_proxies);
    for (vector_name, intent) in vector_name_changes.iter_ordered() {
        debug_assert!(
            intent.version() >= old_optimized_segment_version,
            "proxied vector name change should have newer version than segment",
        );
        match intent {
            IntendedVector::Absent { version } => {
                optimized_segment.delete_vector_name(*version, vector_name)?;
            }
            IntendedVector::Present {
                config,
                version,
                supersedes_wrapped,
            } => {
                if *supersedes_wrapped {
                    // The optimised segment was built from the wrapped data,
                    // so it currently carries the *old* schema for this name.
                    // `create_vector_name_impl` is idempotent and would
                    // silently keep that old storage; clear it first so the
                    // new schema actually takes effect.
                    optimized_segment.delete_vector_name(*version, vector_name)?;
                }
                optimized_segment.create_vector_name(*version, vector_name, config)?;
            }
        }
        check_process_stopped(stopped)?;
    }

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
        optimized_segment
            .delete_point(versions.operation_version, point_id, hw_counter)
            .unwrap();
    }

    // Replace proxy segments with new optimized segment
    let point_count = optimized_segment.available_point_count();
    let optimized_segment_version = optimized_segment.version();
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

    let read_segment_holder = RwLockWriteGuard::downgrade(writable_segment_holder);
    // Can read, but can't yet write updates.

    let mut deferred_points_set = AHashSet::new();
    for proxy in &proxies {
        deferred_points_set.extend(proxy.get().read().deferred_point_ids());
    }
    let deferred_points: Vec<PointIdType> = deferred_points_set.into_iter().collect();

    if !deferred_points.is_empty() {
        const CHUNK_SIZE: usize = 100;

        // Deferred points in proxy segment may become visible for optimized segment (in most cases).
        // It's time to deduplicate them and remove older versions from optimized segment,
        // where they were visible while deferred status.
        // There are a situations, when deferred point is still deferred after optimization,
        // so `deduplicate_points` also cover this case and delete only older versions of the point,
        // which are still visible for optimized segment.
        deferred_points
            .chunks(CHUNK_SIZE)
            .try_for_each(|chunk| read_segment_holder.deduplicate_points(chunk, hw_counter))?;
    }

    // It is important to update manifest before we retire proxy data,
    // as we don't want to have a situation, where new segment is not yet registered, but
    // old segment data is already dropped.
    read_segment_holder.sync_segment_manifest(None)?;

    // Don't destroy the replaced segments' data yet. Points were copy-on-write moved out of them
    // (and out of the optimized segment's in-memory state, which the next optimization bakes into
    // its build output) while their new copies may still sit unflushed in appendable segments. WAL
    // replay can only re-derive those moves from the on-disk pre-images, so the files must survive
    // until a flush proves this optimization durable. Register the destruction as a post-flush
    // action: it runs once the durable waterline covers `optimized_segment_version`, and until then
    // the WAL acknowledge stays capped at each source's persisted version (the same pin the proxy
    // imposed while the optimization ran), so every operation the files contradict, deletions in
    // particular, is replayed and re-applied on a restart. See
    // `SegmentHolder::register_post_flush_action`.
    //
    // This cap has to be tracked at the holder level because the proxy can no longer
    // impose it. While a proxy was a member of the holder it pinned the WAL acknowledge for
    // free: its `persistent_version` reported the wrapped source's durable point while its
    // `version` climbed with every propagated change, so `flush_all` saw unsaved work and
    // capped the ack there. The `swap_new` above evicted the proxies, so that contribution is
    // gone from the holder's flush accounting even though the source files it protected are
    // still on disk. `register_segment_drop` re-expresses the same pin independently of segment
    // membership, snapshotting each proxy's final `persistent_version` as `ack_pin`.
    for proxy in proxies {
        let ack_pin = proxy.get().read().persistent_version();
        read_segment_holder.register_segment_drop(optimized_segment_version, ack_pin, proxy);
    }

    drop(read_segment_holder);
    // Allow updates again
    drop(update_guard);

    // Drop all pointers to proxies, so we can de-arc them
    drop(locked_proxies);

    Ok(point_count)
}

/// Minimum free disk space we want to keep available at any point during the
/// slow part of an optimization. If the available space drops below this
/// threshold mid-flight, we abort the optimization with a canonical
/// `"No space left on device:"` error so we never let the segment builder
/// crash on a raw `ENOSPC`.
///
/// This buffer is intentionally small: the accurate size estimation is done
/// up-front in [`check_segments_size`]. This watchdog only catches the case
/// where an external process (or a parallel optimization) consumed disk
/// space between the pre-flight check and now.
const OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES: u64 = 8 * 1024 * 1024;

/// Outcome of [`check_segments_size`] used by the mid-flight watchdog.
///
/// We pass both numbers around (rather than just the single `space_needed`
/// estimate) so that [`recheck_free_space`] can enforce **remaining
/// headroom** rather than the full initial estimate. Without
/// `space_available_at_precheck` we can't tell the difference between
/// "the optimizer wrote the bytes we expected it to write" (healthy)
/// and "an external process consumed disk on top of our writes"
/// (the only thing the watchdog should care about).
#[derive(Copy, Clone, Debug)]
struct OptimizationSpaceEstimate {
    /// Estimated bytes the optimization will consume on disk
    /// (`2 * total_input_segment_size`). `None` if the estimate failed.
    space_needed: Option<u64>,
    /// Available bytes at `temp_path` observed during the pre-flight check.
    /// `None` if `fs4::available_space` failed.
    space_available_at_precheck: Option<u64>,
}

/// Returns error if segment size is larger than available disk space.
///
/// On success, returns the estimate plus the observed pre-flight free
/// space. The mid-flight watchdog ([`recheck_free_space`]) consumes both
/// to enforce the **headroom** the optimizer expects to preserve, rather
/// than the full pre-flight estimate (which the optimizer itself is
/// allowed to consume by design).
fn check_segments_size(
    optimizer_name: &str,
    optimizing_segments: &[LockedSegment],
    temp_path: &Path,
) -> OperationResult<OptimizationSpaceEstimate> {
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
                // Lead with `No space left on device:` so this error is
                // surfaced consistently with the WAL/insertion path
                // (`DiskUsageWatcher`) and matches the assertion in
                // `tests/e2e_tests/test_low_disk.py`.
                return Err(
                    segment::common::operation_error::OperationError::service_error(format!(
                        "No space left on device: optimization '{optimizer_name}' aborted, \
                         needed: {}, available: {} in '{}'",
                        bytes_to_human(space_needed as usize),
                        bytes_to_human(space_available as usize),
                        temp_path.display(),
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

    Ok(OptimizationSpaceEstimate {
        space_needed,
        space_available_at_precheck: space_available,
    })
}

/// Computes the **headroom** the optimizer expects to preserve on the
/// `temp_path` filesystem during its slow phases.
///
/// `headroom = max(precheck_available - space_needed, safety_floor)`
///
/// In other words: at any mid-flight checkpoint, the disk should still
/// have at least as much free space as we _hadn't_ planned to consume,
/// minus the safety floor. If it has less, *some external party*
/// consumed disk on top of our writes (parallel optimization, snapshot,
/// WAL growth, neighbouring service, ...) and the run is no longer safe
/// to continue.
///
/// When the pre-flight failed to compute either value, we fall back to
/// the safety floor only. That keeps the watchdog non-trivial even on
/// hosts where `statvfs` is flaky, while never tightening the contract
/// the pre-flight already accepted.
fn expected_headroom_bytes(estimate: OptimizationSpaceEstimate) -> u64 {
    estimate
        .space_available_at_precheck
        .unwrap_or(0)
        .saturating_sub(estimate.space_needed.unwrap_or(0))
        .max(OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES)
}

/// Mid-flight watchdog. Re-checks that there is still enough **headroom**
/// at `temp_path` to keep the optimization running.
///
/// Pre-flight [`check_segments_size`] only runs once at the start of an
/// optimization. The slow part (vector storage population + HNSW indexing)
/// can take many minutes, during which:
///
/// * other parallel optimizations on the same shard may consume disk,
/// * external processes (snapshots, WAL growth, neighboring services) may
///   consume disk,
/// * the optimization itself may exceed the conservative `2 * x`
///   pre-flight estimate when the input is unusually compressible or when
///   HNSW link tables turn out larger than the raw vector data.
///
/// In all those cases we want to fail with a clean
/// `"No space left on device:"` error rather than let the segment builder
/// panic when a `write(2)` returns `ENOSPC`. This is what xhjkl flagged in
/// the review of <https://github.com/qdrant/qdrant/pull/4578> as the main
/// known weakness of the up-front-only check.
///
/// Errors from `fs4::available_space` are intentionally treated as "skip"
/// (returns `Ok(())`) so a transient `statvfs` failure can't itself abort
/// an otherwise healthy optimization.
///
/// We compare against **headroom**, not the original `space_needed`,
/// because the optimizer itself consumes that budget by design — using
/// `space_needed` directly would trip the watchdog on every healthy run
/// past the first phase (caught by CodeRabbit on the original review of
/// this PR).
fn recheck_free_space(
    optimizer_name: &str,
    temp_path: &Path,
    estimate: OptimizationSpaceEstimate,
) -> OperationResult<()> {
    recheck_free_space_with(optimizer_name, temp_path, estimate, |p| {
        fs4::available_space(p).ok()
    })
}

/// Test seam for [`recheck_free_space`] that lets unit tests inject the
/// `available_space` lookup. Production callers go through
/// [`recheck_free_space`].
fn recheck_free_space_with<F>(
    optimizer_name: &str,
    temp_path: &Path,
    estimate: OptimizationSpaceEstimate,
    available_space_fn: F,
) -> OperationResult<()>
where
    F: FnOnce(&Path) -> Option<u64>,
{
    let space_available = match available_space_fn(temp_path) {
        Some(available) => available,
        None => {
            log::debug!(
                "Could not re-check available storage space in `{}`",
                temp_path.display(),
            );
            return Ok(());
        }
    };

    let required_headroom = expected_headroom_bytes(estimate);

    if space_available < required_headroom {
        return Err(
            segment::common::operation_error::OperationError::service_error(format!(
                "No space left on device: optimization '{optimizer_name}' aborted mid-flight, \
                 only {} free in '{}' (need at least {} headroom)",
                bytes_to_human(space_available as usize),
                temp_path.display(),
                bytes_to_human(required_headroom as usize),
            )),
        );
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
    deferred_internal_id: Option<PointOffsetType>,
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

    let space_estimate = check_segments_size(optimizer_name, &input_segments, &paths.temp_path)?;

    check_process_stopped(stopped)?;

    on_successful_start();

    let hw_counter = HardwareCounterCell::disposable();

    // Building the cow segment yields a `NewSegmentToken`; we register it below, once it is added to
    // the holder, and before the slow build can route writes into it.
    let (extra_cow_segment_opt, extra_cow_token_opt) = if need_extra_cow_segment {
        let (segment, token) = factory.create_temp_segment()?;
        (Some(segment), Some(token))
    } else {
        (None, None)
    };

    let mut proxies = Vec::new();
    for sg in input_segments.iter() {
        let proxy = UnsyncedProxySegment::new(sg.clone());
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
            // Register the freshly added cow segment in the manifest before we save the version,
            // it guarantees that no writes will happen into unregistered segment.
            segment_holder_read.sync_segment_manifest(extra_cow_token_opt)?;
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
            debug_assert_matches!(
                proxy.wrapped_segment(),
                LockedSegment::Original(_),
                "during optimization, wrapped segment must not be another proxy segment",
            );

            // Now that this write lock froze the wrapped segment, finalize the proxy: this syncs
            // `deleted_mask` from the (now immutable) segment. An upsert/delete could have raced
            // onto the still-appendable wrapped segment between `UnsyncedProxySegment::new` and
            // this lock; syncing here makes the mask cover the segment's full final point range —
            // otherwise a
            // point inserted in that window sits past the mask and scored search treats it as
            // deleted. The type-state guarantees this happens exactly once and cannot be skipped.
            let proxy = proxy.finalize();

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
        optimizer_name,
        input_segments,
        output_segment_uuid,
        deferred_internal_id,
        &locked_proxies,
        permit,
        resource_budget,
        stopped,
        &hw_counter,
        progress,
        &paths.segments_path,
        &paths.temp_path,
        space_estimate,
    );

    let (optimized_segment, already_remove_points) = match build_result {
        Ok(result) => result,
        Err(err) => {
            // Properly cancel optimization on all error kinds
            // Unwrap proxies and add temp segment to holder
            unwrap_proxy(&segment_holder, &proxy_ids)?;
            // A graceful cancellation always happens before the optimized segment is swapped into
            // the holder, so the segment `build` already moved into `segments_path` is now an
            // orphan that `Drop` won't remove. Delete it explicitly. Non-cancellation errors may
            // occur after the swap, where the segment is live, so they are left untouched.
            if matches!(err, OperationError::Cancelled { .. }) {
                cleanup_cancelled_optimized_segment(&paths.segments_path, output_segment_uuid);
            }
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
            // A graceful cancellation always happens before the optimized segment is swapped into
            // the holder, so the segment `build` already moved into `segments_path` is now an
            // orphan that `Drop` won't remove. Delete it explicitly. Non-cancellation errors may
            // occur after the swap, where the segment is live, so they are left untouched.
            if matches!(err, OperationError::Cancelled { .. }) {
                cleanup_cancelled_optimized_segment(&paths.segments_path, output_segment_uuid);
            }
            return Err(err);
        }
    };

    drop(counter_handler);

    timer.set_success(true);

    Ok(OptimizationResult { points_count })
}

#[cfg(test)]
mod disk_watchdog_tests {
    //! Tests for the optimizer's mid-flight disk-space watchdog
    //! ([`recheck_free_space`]). These tests do not exercise an actual
    //! optimization; they pin the watchdog's threshold semantics
    //! (headroom, not full estimate) and the canonical
    //! `"No space left on device:"` error format.
    //!
    //! Regression target: <https://github.com/qdrant/qdrant/issues/4297>.
    //! In particular, [`watchdog_does_not_trip_on_optimizers_own_writes`]
    //! pins the bug fix called out by CodeRabbit on the original review
    //! of this PR — the optimizer is *expected* to consume `space_needed`
    //! bytes by design, so the watchdog must enforce **headroom** rather
    //! than the original full estimate.

    use std::cell::Cell;
    use std::path::Path;

    use super::{
        OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES, OptimizationSpaceEstimate, expected_headroom_bytes,
        recheck_free_space_with,
    };

    fn estimate(space_needed: u64, precheck_available: u64) -> OptimizationSpaceEstimate {
        OptimizationSpaceEstimate {
            space_needed: Some(space_needed),
            space_available_at_precheck: Some(precheck_available),
        }
    }

    fn unknown_estimate() -> OptimizationSpaceEstimate {
        OptimizationSpaceEstimate {
            space_needed: None,
            space_available_at_precheck: None,
        }
    }

    fn run(
        temp_path: &Path,
        est: OptimizationSpaceEstimate,
        available: Option<u64>,
    ) -> Result<(), String> {
        recheck_free_space_with("test-optimizer", temp_path, est, |_| available)
            .map_err(|e| e.to_string())
    }

    #[test]
    fn headroom_is_precheck_minus_needed_clamped_to_safety_floor() {
        // Healthy: 100 MiB available, plan to consume 60 MiB → 40 MiB
        // expected to remain free at all times. 40 MiB > 8 MiB floor, so
        // headroom is the slack value.
        let est = estimate(60 << 20, 100 << 20);
        assert_eq!(expected_headroom_bytes(est), 40 << 20);

        // Tight: 10 MiB available, plan to consume 9 MiB → only 1 MiB
        // slack, but the safety floor of 8 MiB takes over. The watchdog
        // never tightens below the floor (the up-front check already
        // accepted this run, we don't second-guess it mid-flight).
        let est = estimate(9 << 20, 10 << 20);
        assert_eq!(expected_headroom_bytes(est), OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES);

        // Unknown estimate: floor only.
        assert_eq!(
            expected_headroom_bytes(unknown_estimate()),
            OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES,
        );
    }

    #[test]
    fn watchdog_does_not_trip_on_optimizers_own_writes() {
        // This is the regression test for the CodeRabbit-flagged bug.
        //
        // Pre-flight: 100 MiB available, plan to consume 60 MiB → 40 MiB
        //             headroom.
        // Mid-flight: optimizer has written ~50 MiB of its own data, so
        //             only 50 MiB free. That's less than the original
        //             `space_needed` (60 MiB) but *more* than the
        //             headroom (40 MiB). The watchdog must accept.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let est = estimate(60 << 20, 100 << 20);
        run(tmp.path(), est, Some(50 << 20)).expect(
            "watchdog must not trip when free space drops to a value the \
             optimizer was expected to consume by design",
        );
    }

    #[test]
    fn watchdog_trips_when_external_consumer_eats_into_headroom() {
        // Pre-flight: 100 MiB available, plan to consume 60 MiB → 40 MiB
        //             headroom.
        // Mid-flight: someone else (parallel optimizer, snapshot, WAL,
        //             external process) consumed enough that only 30 MiB
        //             is free, below the 40 MiB headroom. The watchdog
        //             must abort with the canonical OOD message.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let est = estimate(60 << 20, 100 << 20);
        let err = run(tmp.path(), est, Some(30 << 20)).unwrap_err();

        assert!(
            err.contains("No space left on device:"),
            "must lead with canonical OOD prefix to match \
             tests/e2e_tests/test_low_disk.py; got: {err}",
        );
        assert!(
            err.contains("test-optimizer"),
            "must name the optimizer for log triage; got: {err}",
        );
        assert!(
            err.contains(&tmp.path().display().to_string()),
            "must include the temp path for log triage; got: {err}",
        );
    }

    #[test]
    fn watchdog_trips_below_safety_floor_even_without_estimate() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        // No estimate at all; only the 8 MiB floor applies.
        let err = run(tmp.path(), unknown_estimate(), Some(1024)).unwrap_err();
        assert!(err.contains("No space left on device:"), "got: {err}");
    }

    #[test]
    fn watchdog_skips_on_statvfs_failure() {
        // `available_space_fn` returning `None` simulates a transient
        // statvfs/GetDiskFreeSpaceEx failure. The watchdog must NOT abort
        // an otherwise healthy optimization on its own observability
        // hiccup.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let est = estimate(60 << 20, 100 << 20);
        run(tmp.path(), est, None).expect(
            "watchdog must skip (return Ok) when available_space lookup \
             itself fails; we never want to abort an otherwise healthy \
             optimization on a transient observability hiccup",
        );
    }

    #[test]
    fn watchdog_evaluates_available_space_lookup_exactly_once() {
        // Make sure the production pathway never calls `statvfs` more
        // than once per checkpoint (it's a syscall on every platform).
        let tmp = tempfile::tempdir().expect("tmpdir");
        let calls = Cell::new(0u32);
        let est = estimate(60 << 20, 100 << 20);
        let _ = recheck_free_space_with("test-optimizer", tmp.path(), est, |_| {
            calls.set(calls.get() + 1);
            Some(50 << 20)
        });
        assert_eq!(calls.get(), 1, "available_space_fn must be called exactly once");
    }

    #[test]
    fn safety_buffer_constant_stays_in_sane_range() {
        assert!(
            OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES >= 1024 * 1024,
            "watchdog buffer must be at least 1 MB or it's pointless",
        );
        assert!(
            OPTIMIZER_DISK_WATCHDOG_BUFFER_BYTES <= 64 * 1024 * 1024,
            "watchdog buffer must stay small or it'll false-positive on \
             tight test environments",
        );
    }
}
