use std::path::Path;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::save_on_disk::SaveOnDisk;
use io::storage_version::StorageVersion;
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use segment::common::operation_error::OperationResult;
use segment::data_types::manifest::SnapshotManifest;
use segment::entry::SegmentEntry;
use segment::segment::SegmentVersion;
use segment::types::SegmentConfig;

use crate::locked_segment::LockedSegment;
use crate::payload_index_schema::PayloadIndexSchema;
use crate::proxy_segment::ProxySegment;
use crate::segment_holder::{SegmentHolder, SegmentId};

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

    /// Proxy all shard segments for [`proxy_all_segments_and_apply`].
    #[allow(clippy::type_complexity)]
    pub fn proxy_all_segments<'a>(
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        segments_path: &Path,
        segment_config: Option<SegmentConfig>,
        payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    ) -> OperationResult<(
        Vec<(SegmentId, LockedSegment)>,
        SegmentId,
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
            let proxy = ProxySegment::new(segment.clone());

            // Write segment is fresh, so it has no operations
            // Operation with number 0 will be applied
            proxy.replicate_field_indexes(0, &hw_counter, &tmp_segment)?;
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
        for (segment_id, proxy) in new_proxies {
            // Replicate field indexes the second time, because optimized segments could have
            // been changed. The probability is small, though, so we can afford this operation
            // under the full collection write lock
            let op_num = proxy.version();
            if let Err(err) = proxy.replicate_field_indexes(op_num, &hw_counter, &tmp_segment) {
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

        // Make sure at least one appendable segment exists
        let temp_segment_id = write_segments.add_new_locked(tmp_segment);

        let segments_lock = RwLockWriteGuard::downgrade_to_upgradable(write_segments);

        Ok((proxies, temp_segment_id, segments_lock))
    }

    /// Try to unproxy a single shard segment for [`proxy_all_segments_and_apply`].
    ///
    /// # Warning
    ///
    /// If unproxying fails an error is returned with the lock and the proxy is left behind in the
    /// shard holder.
    pub fn try_unproxy_segment(
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
            if let Err(err) = proxy_segment.write().propagate_to_wrapped() {
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
            let mut proxy_segment = proxy_segment.write();
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

    /// Unproxy all shard segments for [`proxy_all_segments_and_apply`].
    pub fn unproxy_all_segments(
        segments_lock: RwLockUpgradableReadGuard<SegmentHolder>,
        proxies: Vec<(SegmentId, LockedSegment)>,
        tmp_segment_id: SegmentId,
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
            if let Err(err) = proxy_segment.write().propagate_to_wrapped() {
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
                        let mut proxy_segment = proxy_segment.write();
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

        debug_assert!(
            write_segments.get(tmp_segment_id).is_some(),
            "temp segment must exist",
        );

        // Remove temporary appendable segment, if we don't need it anymore
        write_segments.remove_segment_if_not_needed(tmp_segment_id)?;

        Ok(())
    }
}
