use fs_err as fs;
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use segment::common::operation_error::OperationError;

use crate::locked_segment::LockedSegment;
use crate::segment_holder::{PostFlushOutcome, SegmentHolder, SegmentId};

/// The segment holder lock handed back when unproxying fails, with the error that caused it.
pub type UnproxyError<'a> = (RwLockUpgradableReadGuard<'a, SegmentHolder>, OperationError);

impl SegmentHolder {
    /// Swap every proxy in `proxy_ids` back for the segment it wraps.
    ///
    /// Changes buffered in a proxy (deleted points, index and vector-name changes) are propagated
    /// into the wrapped segments first, while only the upgradable read lock is held. That can be
    /// an expensive step, so it is important that it does not block reads.
    ///
    /// Each unwrapped proxy's pending changes log file is deleted after the next segments flush
    /// cycle. It is left on disk now, but a post flush action is scheduled to remove it later.
    ///
    /// # Locking
    ///
    /// Takes no lock of its own: the caller owns the updates lock and the returned write lock, and
    /// decides when each of them is released.
    ///
    /// # Result
    ///
    /// The wrapped segments are put back into the holder under the same segment IDs, the proxies
    /// are dropped, and the write lock is returned.
    ///
    /// If propagating changes fails, nothing is unwrapped: every proxy stays installed and keeps
    /// serving its changes, and the upgradable read lock is handed back untouched with the error.
    /// Unwrapping a proxy whose changes did not reach the wrapped segment would lose them for
    /// good, so we must not touch the holder in that case.
    pub(crate) fn unproxy_segments<'a>(
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        proxy_ids: &[SegmentId],
    ) -> Result<RwLockWriteGuard<'a, SegmentHolder>, UnproxyError<'a>> {
        // Propagate proxied changes back into the wrapped segments to not lose these in-memory
        // changes, while we only hold the upgradable read lock
        let proxies: Vec<_> = proxy_ids
            .iter()
            .filter_map(|&proxy_id| match segments_lock.get(proxy_id) {
                Some(LockedSegment::Proxy(proxy_segment)) => {
                    Some((proxy_id, proxy_segment.clone()))
                }
                _ => None,
            })
            .collect();
        for (proxy_id, proxy_segment) in &proxies {
            if let Err(err) = proxy_segment.write().propagate_to_wrapped() {
                log::error!(
                    "Propagating proxy segment {proxy_id} changes to wrapped segment failed: {err}",
                );
                return Err((segments_lock, err));
            }
        }

        // Swap out each proxy with its wrapped segment once changes are propagated
        let mut write_segments = RwLockUpgradableReadGuard::upgrade(segments_lock);
        for &proxy_id in proxy_ids {
            let proxy_segment = match write_segments.get(proxy_id) {
                Some(LockedSegment::Proxy(proxy_segment)) => proxy_segment.clone(),
                // Already unwrapped. It should not actually be here
                Some(LockedSegment::Original(_)) => {
                    log::warn!("Attempt to unwrap raw segment {proxy_id}! Should not happen.");
                    continue;
                }
                None => continue,
            };

            // Points might have changed in between propagating above and taking the write lock, so
            // propagate once more. Failing to propagate loses in-memory state, but is recovered on
            // restart: the persisted pending changes log stays behind and is replayed then, and
            // everything past it is replayed from the WAL.
            let propagated = proxy_segment.write().propagate_to_wrapped();
            if let Err(err) = &propagated {
                log::error!(
                    "Propagating proxy segment {proxy_id} changes to wrapped segment failed, ignoring: {err}",
                );
            }

            let proxy_segment_read = proxy_segment.read();
            let wrapped_segment = proxy_segment_read.wrapped_segment.clone();
            let log_path = proxy_segment_read.pending_changes_log_path().to_path_buf();
            drop(proxy_segment_read);

            if let Err(err) = write_segments.replace(proxy_id, wrapped_segment.clone()) {
                return Err((
                    RwLockWriteGuard::downgrade_to_upgradable(write_segments),
                    err,
                ));
            }

            // Schedule proxy log file to delete after next flush cycle
            if propagated.is_ok() {
                let ready_at = wrapped_segment.get().read().version();
                write_segments.register_post_flush_action(ready_at, ready_at, move || {
                    match fs::remove_file(&log_path) {
                        Ok(()) => {}
                        // File may never have existed on disk at all if never flushed before unwrap
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => {
                            return Err(OperationError::service_error(format!(
                                "Failed to remove pending changes log {}: {err}",
                                log_path.display(),
                            )));
                        }
                    }
                    Ok(PostFlushOutcome::Done)
                });
            }
        }

        Ok(write_segments)
    }
}
