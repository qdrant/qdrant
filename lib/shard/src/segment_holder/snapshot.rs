use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use io::storage_version::StorageVersion;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SnapshotManifest;
use segment::entry::SegmentEntry;
use segment::segment::SegmentVersion;
use segment::types::{SegmentConfig, SnapshotFormat};

use crate::locked_segment::LockedSegment;
use crate::payload_index_schema::PayloadIndexSchema;
use crate::proxy_segment::{LockedIndexChanges, LockedRmSet, ProxySegment};
use crate::segment_holder::{LockedSegmentHolder, SegmentHolder, SegmentId};

impl SegmentHolder {
    pub fn snapshot_manifest(&self) -> OperationResult<SnapshotManifest> {
        let mut manifest = SnapshotManifest::default();

        for (_, segment) in self.iter() {
            segment
                .get()
                .read()
                .collect_snapshot_manifest(&mut manifest)?;
        }

        Ok(manifest)
    }

    /// Take a snapshot of all segments into `snapshot_dir_path`
    ///
    /// It is recommended to provide collection parameters. This function internally creates a
    /// temporary segment, which will source the configuration from it.
    ///
    /// Shortcuts at the first failing segment snapshot.
    #[expect(clippy::too_many_arguments)]
    pub fn snapshot_all_segments(
        segments: LockedSegmentHolder,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        temp_dir: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SnapshotManifest>,
    ) -> OperationResult<()> {
        // Snapshotting may take long-running read locks on segments blocking incoming writes, do
        // this through proxied segments to allow writes to continue.

        let mut snapshotted_segments = HashSet::<String>::new();

        Self::proxy_all_segments_and_apply(
            segments,
            segments_path,
            segment_config,
            payload_index_schema,
            |segment| {
                let read_segment = segment.read();
                read_segment.take_snapshot(
                    temp_dir,
                    tar,
                    format,
                    manifest,
                    &mut snapshotted_segments,
                )?;
                Ok(())
            },
        )
    }

    /// Temporarily proxify all segments and apply function `f` to it.
    ///
    /// Intended to smoothly accept writes while performing long-running read operations on each
    /// segment, such as during snapshotting. It should prevent blocking reads on segments for any
    /// significant amount of time.
    ///
    /// This calls function `f` on all segments, but each segment is temporarily proxified while
    /// the function is called.
    ///
    /// All segments are proxified at the same time on start. That ensures each wrapped (proxied)
    /// segment is kept at the same point in time. Each segment is unproxied one by one, right
    /// after function `f` has been applied. That helps keeping proxies as shortlived as possible.
    ///
    /// A read lock is kept during the whole process to prevent external actors from messing with
    /// the segment holder while segments are in proxified state. That means no other actors can
    /// take a write lock while this operation is running.
    ///
    /// As part of this process, a new segment is created. All proxies direct their writes to this
    /// segment. The segment is added to the collection if it has any operations, otherwise it is
    /// deleted when all segments are unproxied again.
    ///
    /// It is recommended to provide collection parameters. The segment configuration will be
    /// sourced from it.
    pub fn proxy_all_segments_and_apply<F>(
        segments: LockedSegmentHolder,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
        mut operation: F,
    ) -> OperationResult<()>
    where
        F: FnMut(&RwLock<dyn SegmentEntry>) -> OperationResult<()>,
    {
        let segments_lock = segments.upgradable_read();

        // Proxy all segments
        log::trace!("Proxying all shard segments to apply function");
        let (mut proxies, tmp_segment, mut segments_lock) = Self::proxy_all_segments(
            segments_lock,
            segments_path,
            segment_config,
            payload_index_schema,
        )?;

        // Apply provided function
        log::trace!("Applying function on all proxied shard segments");
        let mut result = Ok(());
        let mut unproxied_segment_ids = Vec::with_capacity(proxies.len());

        for (segment_id, proxy_segment) in &proxies {
            // Get segment to snapshot
            let op_result = match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => {
                    let guard = proxy_segment.read();
                    let segment = guard.wrapped_segment.get();
                    // Call provided function on wrapped segment while holding guard to parent segment
                    operation(segment)
                }
                // All segments to snapshot should be proxy, warn if this is not the case
                LockedSegment::Original(segment) => {
                    debug_assert!(
                        false,
                        "Reached non-proxy segment while applying function to proxies, this should not happen, ignoring",
                    );
                    // Call provided function on segment
                    operation(segment.as_ref())
                }
            };

            if let Err(err) = op_result {
                result = Err(OperationError::service_error(format!(
                    "Applying function to a proxied shard segment {segment_id} failed: {err}"
                )));
                break;
            }

            // Try to unproxy/release this segment since we don't use it anymore
            // Unproxying now lets us release the segment earlier, prevent unnecessary writes to the temporary segment.
            // Make sure to keep at least one proxy segment to maintain access to the points in the shared write segment.
            // The last proxy and the shared write segment will be promoted into the segment_holder atomically
            // by `Self::unproxy_all_segments` afterwards to maintain the read consistency.
            let remaining = proxies.len() - unproxied_segment_ids.len();
            if remaining > 1 {
                match Self::try_unproxy_segment(segments_lock, *segment_id, proxy_segment.clone()) {
                    Ok(lock) => {
                        segments_lock = lock;
                        unproxied_segment_ids.push(*segment_id);
                    }
                    Err(lock) => segments_lock = lock,
                }
            }
        }
        proxies.retain(|(id, _)| !unproxied_segment_ids.contains(id));

        // Unproxy all segments
        // Always do this to prevent leaving proxy segments behind
        log::trace!("Unproxying all shard segments after function is applied");
        Self::unproxy_all_segments(segments_lock, proxies, tmp_segment)?;

        result
    }

    /// Proxy all shard segments for [`Self::proxy_all_segments_and_apply`].
    #[allow(clippy::type_complexity)]
    fn proxy_all_segments<'a>(
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> OperationResult<(
        Vec<(SegmentId, LockedSegment)>,
        LockedSegment,
        RwLockUpgradableReadGuard<'a, SegmentHolder>,
    )> {
        // This counter will be used to measure operations on temp segment,
        // which is part of internal process and can be ignored
        let hw_counter = HardwareCounterCell::disposable();

        // Create temporary appendable segment to direct all proxy writes into
        let tmp_segment = segments_lock.build_tmp_segment(
            segments_path,
            segment_config,
            payload_index_schema,
            false,
        )?;

        // List all segments we want to snapshot
        let segment_ids = segments_lock.segment_ids();

        // Create proxy for all segments
        let mut new_proxies = Vec::with_capacity(segment_ids.len());
        for segment_id in segment_ids {
            let segment = segments_lock.get(segment_id).unwrap();
            let mut proxy = ProxySegment::new(
                segment.clone(),
                tmp_segment.clone(),
                // In this case, each proxy has their own set of deleted points
                // We cannot share deletes because they're propagated to different wrapped
                // segments, and we unproxy at different times
                LockedRmSet::default(),
                LockedIndexChanges::default(),
            );

            // Write segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            proxy.replicate_field_indexes(0, &hw_counter)?;
            new_proxies.push((segment_id, proxy));
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

        // Replace all segments with proxies
        // We cannot fail past this point to prevent only having some segments proxified
        let mut proxies = Vec::with_capacity(new_proxies.len());
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for (segment_id, mut proxy) in new_proxies {
            // Replicate field indexes the second time, because optimized segments could have
            // been changed. The probability is small, though, so we can afford this operation
            // under the full collection write lock
            let op_num = proxy.version();
            if let Err(err) = proxy.replicate_field_indexes(op_num, &hw_counter) {
                log::error!("Failed to replicate proxy segment field indexes, ignoring: {err}");
            }

            // We must keep existing segment IDs because ongoing optimizations might depend on the mapping being the same
            write_segments.replace(segment_id, proxy)?;
            let locked_proxy_segment = write_segments
                .get(segment_id)
                .cloned()
                .expect("failed to get segment from segment holder we just swapped in");
            proxies.push((segment_id, locked_proxy_segment));
        }
        let segments_lock = RwLockWriteGuard::downgrade_to_upgradable(write_segments);

        Ok((proxies, tmp_segment, segments_lock))
    }

    /// Try to unproxy a single shard segment for [`Self::proxy_all_segments_and_apply`].
    ///
    /// # Warning
    ///
    /// If unproxying fails an error is returned with the lock and the proxy is left behind in the
    /// shard holder.
    fn try_unproxy_segment(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        segment_id: SegmentId,
        proxy_segment: LockedSegment,
    ) -> Result<RwLockUpgradableReadGuard<SegmentHolder>, RwLockUpgradableReadGuard<SegmentHolder>>
    {
        // We must propagate all changes in the proxy into their wrapped segments, as we'll put the
        // wrapped segment back into the segment holder. This can be an expensive step if we
        // collected a lot of changes in the proxy, so we do this in two batches to prevent
        // unnecessary locking. First we propagate all changes with a read lock on the shard
        // holder, to prevent blocking other readers. Second we propagate any new changes again
        // with a write lock on the segment holder, blocking other operations. This second batch
        // should be very fast, as we already propagated all changes in the first, which is why we
        // can hold a write lock. Once done, we can swap out the proxy for the wrapped shard.

        let proxy_segment = match proxy_segment {
            LockedSegment::Proxy(proxy_segment) => proxy_segment,
            LockedSegment::Original(_) => {
                log::warn!(
                    "Unproxying segment {segment_id} that is not proxified, that is unexpected, skipping",
                );
                return Err(segments_lock);
            }
        };

        // Batch 1: propagate changes to wrapped segment with segment holder read lock
        {
            let _update_guard = segments_lock.update_lock.lock();
            if let Err(err) = proxy_segment.read().propagate_to_wrapped() {
                log::error!(
                    "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
                );
            }
        }

        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);

        // Batch 2: propagate changes to wrapped segment with segment holder write lock
        // Propagate proxied changes to wrapped segment, take it out and swap with proxy
        // Important: put the wrapped segment back with its original segment ID
        let wrapped_segment = {
            let proxy_segment = proxy_segment.read();
            if let Err(err) = proxy_segment.propagate_to_wrapped() {
                log::error!(
                    "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
                );
            }
            proxy_segment.wrapped_segment.clone()
        };
        write_segments.replace(segment_id, wrapped_segment).unwrap();

        // Downgrade write lock to read and give it back
        Ok(RwLockWriteGuard::downgrade_to_upgradable(write_segments))
    }

    /// Unproxy all shard segments for [`Self::proxy_all_segments_and_apply`].
    fn unproxy_all_segments(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        proxies: Vec<(SegmentId, LockedSegment)>,
        tmp_segment: LockedSegment,
    ) -> OperationResult<()> {
        // We must propagate all changes in the proxy into their wrapped segments, as we'll put the
        // wrapped segment back into the segment holder. This can be an expensive step if we
        // collected a lot of changes in the proxy, so we do this in two batches to prevent
        // unnecessary locking. First we propagate all changes with a read lock on the shard
        // holder, to prevent blocking other readers. Second we propagate any new changes again
        // with a write lock on the segment holder, blocking other operations. This second batch
        // should be very fast, as we already propagated all changes in the first, which is why we
        // can hold a write lock. Once done, we can swap out the proxy for the wrapped shard.

        // Batch 1: propagate changes to wrapped segment with segment holder read lock
        proxies
            .iter()
            .filter_map(|(segment_id, proxy_segment)| match proxy_segment {
                LockedSegment::Proxy(proxy_segment) => Some((segment_id, proxy_segment)),
                LockedSegment::Original(_) => None,
            }).for_each(|(proxy_id, proxy_segment)| {
            let _update_guard = segments_lock.update_lock.lock();
            if let Err(err) = proxy_segment.read().propagate_to_wrapped() {
                log::error!("Propagating proxy segment {proxy_id} changes to wrapped segment failed, ignoring: {err}");
            }
        });

        // Batch 2: propagate changes to wrapped segment with segment holder write lock
        // Swap out each proxy with wrapped segment once changes are propagated
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for (segment_id, proxy_segment) in proxies {
            match proxy_segment {
                // Propagate proxied changes to wrapped segment, take it out and swap with proxy
                // Important: put the wrapped segment back with its original segment ID
                LockedSegment::Proxy(proxy_segment) => {
                    let wrapped_segment = {
                        let proxy_segment = proxy_segment.read();
                        if let Err(err) = proxy_segment.propagate_to_wrapped() {
                            log::error!(
                                "Propagating proxy segment {segment_id} changes to wrapped segment failed, ignoring: {err}",
                            );
                        }
                        proxy_segment.wrapped_segment.clone()
                    };
                    write_segments.replace(segment_id, wrapped_segment)?;
                }
                // If already unproxied, do nothing
                LockedSegment::Original(_) => {}
            }
        }

        // Finalize temporary segment we proxied writes to
        // Append a temp segment to collection if it is not empty or there is no other appendable segment
        if !write_segments.has_appendable_segment() || !tmp_segment.get().read().is_empty() {
            log::trace!(
                "Keeping temporary segment with {} points",
                tmp_segment.get().read().available_point_count(),
            );
            write_segments.add_new_locked(tmp_segment);
        } else {
            log::trace!("Dropping temporary segment with no changes");
            tmp_segment.drop_data()?;
        }

        Ok(())
    }
}
