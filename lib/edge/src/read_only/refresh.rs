use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::RwLock;
use segment::common::operation_error::OperationResult;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::load::load_segments_parallel;

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Refresh the follower to the leader's current on-disk state.
    ///
    /// Caller-driven (never self-triggered), mirroring `ReadOnlySegment::live_reload`: the host
    /// owns the cadence (a timer, an explicit call after a known leader flush, or an FS watch).
    /// Eventually consistent — a point becomes visible once the leader has flushed it to the
    /// segment files and a subsequent `refresh` has run.
    pub fn refresh(&self) -> OperationResult<()>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        let hw_counter = HardwareCounterCell::disposable();
        self.refresh_with(&hw_counter)
    }

    /// [`refresh`](Self::refresh) with a caller-supplied hardware counter.
    pub fn refresh_with(&self, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        // 1. Snapshot the current on-disk segment set (backend-specific; see `SegmentEnumerator`).
        let on_disk = self.enumerator.list_segments()?;

        // 2. Load newly-appeared segments in parallel (outside the holder lock), then under the lock
        //    add them and drop removed ones, and collect the survivors to live_reload *after*
        //    releasing the lock — so reads only block during the cheap add/drop, not during load
        //    or reload.
        let new_segments: Vec<(Uuid, PathBuf)> = {
            let holder = self.segments.read();
            on_disk
                .iter()
                .filter(|(uuid, _)| !holder.contains(uuid))
                .map(|(uuid, segment_path)| (*uuid, segment_path.clone()))
                .collect()
        };
        let loaded = load_segments_parallel::<S>(&self.search_pool, &self.fs, new_segments)?;

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
            for (uuid, segment) in loaded {
                let appendable = segment.segment_config.is_appendable();
                holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
            }

            holder.remove_missing(&on_disk);

            survivor_uuids
                .into_iter()
                .filter_map(|uuid| holder.segment_arc(&uuid).map(|segment| (uuid, segment)))
                .collect()
        };

        // 3. Re-derive the config from the current segments — a read-only follower has no
        //    edge_config.json, so the segments are the source of truth. Folded over all segments
        //    in UUID order, so the derivation is deterministic and a segment carrying no
        //    information about a parameter never masks one that does. No-op for an empty shard
        //    (the previous snapshot stays in place until segments appear).
        let derived = {
            let holder = self.segments.read();
            let mut uuids = holder.uuids();
            uuids.sort_unstable();
            uuids
                .into_iter()
                .filter_map(|uuid| holder.segment_arc(&uuid))
                .fold(None, |acc, segment| {
                    Some(EdgeConfig::fold_from_segment_config(
                        acc,
                        &segment.read().segment_config,
                    ))
                })
        };
        if let Some(derived) = derived {
            *self.config.write() = Arc::new(derived);
        }

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
