use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::UniversalRead;
use parking_lot::RwLock;
use segment::common::operation_error::OperationResult;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::lifecycle::is_transient_open_error;

impl<S: UniversalRead + 'static> ReadOnlyEdgeShard<S> {
    /// Refresh the follower to the leader's current on-disk state.
    ///
    /// Caller-driven (never self-triggered), mirroring `ReadOnlySegment::live_reload`: the host
    /// owns the cadence (a timer, an explicit call after a known leader flush, or an FS watch).
    /// Eventually consistent — a point becomes visible once the leader has flushed it to the
    /// segment files and a subsequent `refresh` has run.
    pub fn refresh(&self) -> OperationResult<()> {
        let hw_counter = HardwareCounterCell::disposable();
        self.refresh_with(&hw_counter)
    }

    /// [`refresh`](Self::refresh) with a caller-supplied hardware counter.
    pub fn refresh_with(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        // 1. Reload the config snapshot (best-effort: a transient read while the leader rewrites it
        //    just leaves the previous snapshot in place until the next refresh).
        match EdgeConfig::load(&self.path) {
            Some(Ok(config)) => *self.config.write() = Arc::new(config),
            Some(Err(err)) => log::warn!("failed to reload edge config during refresh: {err}"),
            None => {}
        }

        // 2. Snapshot the current on-disk segment set (backend-specific; see `SegmentEnumerator`).
        let on_disk = self.enumerator.list_segments()?;

        // 3. Under the holder lock, add newly-appeared segments and drop removed ones, then collect
        //    the survivors to live_reload *after* releasing the lock — so reads only block during
        //    the cheap add/drop, not during the reload.
        let survivors: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)> = {
            let mut holder = self.segments.write();

            // Segments present before this refresh that still exist on disk.
            let survivor_uuids: Vec<Uuid> = holder
                .uuids()
                .into_iter()
                .filter(|uuid| on_disk.contains_key(uuid))
                .collect();

            // Add before drop: when an optimization migrates points from old to new segments, both
            // must be momentarily visible so migrated points never disappear.
            for (uuid, segment_path) in &on_disk {
                if holder.contains(uuid) {
                    continue;
                }
                match ReadOnlySegment::<S>::open(&self.fs, segment_path, *uuid, None) {
                    Ok(segment) => {
                        let appendable = segment.segment_config.is_appendable();
                        holder.insert(*uuid, appendable, Arc::new(RwLock::new(segment)));
                    }
                    // Mid-write by the leader or removed underneath us — retried next refresh.
                    Err(err) if is_transient_open_error(&err) => {}
                    Err(err) => {
                        log::warn!("failed to open segment {uuid} during refresh: {err}");
                    }
                }
            }

            holder.remove_missing(&on_disk);

            survivor_uuids
                .into_iter()
                .filter_map(|uuid| holder.segment_arc(&uuid).map(|segment| (uuid, segment)))
                .collect()
        };

        // 4. Live-reload survivors to fold in the leader's flushed in-place appends and deletes.
        //    Newly-added segments are already current, so they are skipped.
        for (uuid, segment) in survivors {
            if let Err(err) = segment.write().live_reload(&self.fs, hw_counter) {
                // The segment retains the unapplied delta in its `pending_reload`; the next refresh
                // folds in fresh changes and replays the union, so nothing is lost.
                log::warn!("live_reload of segment {uuid} failed, will retry next refresh: {err}");
            }
        }

        Ok(())
    }
}
