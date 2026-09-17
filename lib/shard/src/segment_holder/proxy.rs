use fs_err as fs;
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};
use segment::common::operation_error::OperationError;

use crate::locked_segment::LockedSegment;
use crate::segment_holder::locked::{LockedSegmentHolder, UpdatesGuard};
use crate::segment_holder::{PostFlushOutcome, SegmentHolder, SegmentId};

/// The segment holder lock handed back when unproxying fails, with the error that caused it.
pub type UnproxyError<'a> = (RwLockUpgradableReadGuard<'a, SegmentHolder>, OperationError);

impl SegmentHolder {
    /// Swap every proxy in `proxy_ids` back for the segment it wraps.
    ///
    /// Changes buffered in a proxy (deleted points, index and vector-name changes) must reach the
    /// wrapped segment before the proxy is dropped. Propagating them can be expensive when a proxy
    /// has been live for a while, so it is done in two phases:
    ///
    /// 1. The bulk of the changes is propagated while only the upgradable read lock is held and
    ///    the updates lock is *not*. Reads keep running and updates keep landing, so the expensive
    ///    part of unproxying blocks neither.
    /// 2. The updates lock is taken, freezing updates, and every proxy is propagated once more to
    ///    pick up whatever arrived during phase 1. That delta is bounded by how long phase 1 took,
    ///    so it is normally empty or tiny, and the exclusive phase stays cheap. Only then is the
    ///    write lock taken to swap the proxies out.
    ///
    /// Each unwrapped proxy's pending changes log file is deleted after the next segments flush
    /// cycle. It is left on disk now, but a post flush action is scheduled to remove it later.
    ///
    /// # Locking
    ///
    /// Acquires the updates lock between the two phases and hands it back to the caller together
    /// with the write lock, which decides when each of them is released. The caller must NOT hold
    /// the updates lock already: that would block updates during phase 1 and collapse the split
    /// into one exclusive pass (and, since the lock is not reentrant, deadlock).
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
        segments: &'a LockedSegmentHolder,
        segments_lock: RwLockUpgradableReadGuard<'a, SegmentHolder>,
        proxy_ids: &[SegmentId],
    ) -> Result<(RwLockWriteGuard<'a, SegmentHolder>, UpdatesGuard<'a>), UnproxyError<'a>> {
        // Phase 1: propagate the bulk of the buffered changes while holding only the upgradable
        // read lock. Updates are deliberately left running, so this may take as long as it needs.
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

        // Phase 2: freeze updates, then re-propagate whatever landed during phase 1 and swap the
        // proxies out. The updates lock is taken before upgrading to the write lock, matching the
        // [segment holder -> updates] order every non-update path uses; taking it while holding
        // only the upgradable read lock still lets in-flight updates take their read lock and
        // finish, so they drain instead of deadlocking against the upgrade below.
        let updates_guard = segments.acquire_updates_lock();
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

            // Points may have changed while phase 1 ran without the updates lock, so propagate
            // once more. Updates are frozen now, so this catches everything and is the last word.
            // Failing to propagate loses in-memory state, but is recovered on restart: the
            // persisted pending changes log stays behind and is replayed then, and everything
            // past it is replayed from the WAL.
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

        Ok((write_segments, updates_guard))
    }
}
