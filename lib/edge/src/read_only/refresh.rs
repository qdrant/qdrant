use std::path::PathBuf;
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::IsNotFound as _;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::load::load_segments_parallel;

/// How a single [`refresh_attempt`](ReadOnlyEdgeShard::refresh_attempt) ended.
enum RefreshOutcome {
    /// The attempt fully converged on its manifest snapshot.
    Complete,
    /// Segments vanished benignly mid-attempt (the leader removed them, confirmed
    /// against a re-read manifest); re-run against the fresh manifest to pick up
    /// their replacements.
    ManifestChanged,
}

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Refresh the follower to the leader's current on-disk state.
    ///
    /// Caller-driven (never self-triggered), mirroring `ReadOnlySegment::live_reload`: the host
    /// owns the cadence (a timer, an explicit call after a known leader flush, or an FS watch).
    /// Eventually consistent — a point becomes visible once the leader has flushed it to the
    /// segment files and a subsequent `refresh` has run.
    ///
    /// Leader-side segment churn is absorbed: a segment whose files vanish while its live-reload
    /// runs is checked against a re-read manifest, and if the leader indeed removed it, the segment
    /// is dropped and the refresh re-runs (bounded) to pick up its replacements. `Err` therefore
    /// means the shard genuinely needs attention: a component failed to reload, or a segment's
    /// essential files are missing while the manifest still lists it. Either way the shard stays
    /// consistent — every swap is atomic, a failed segment keeps serving its pre-refresh state,
    /// and the next refresh replays its unapplied delta (see `pending_reload`).
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
        // A benign mid-attempt segment removal re-runs the attempt against the fresh manifest;
        // bound the re-runs so a leader churning segments faster than the follower converges
        // cannot spin this loop forever.
        const MAX_ATTEMPTS: usize = 3;

        for _ in 0..MAX_ATTEMPTS {
            match self.refresh_attempt(hw_counter)? {
                RefreshOutcome::Complete => return Ok(()),
                RefreshOutcome::ManifestChanged => {}
            }
        }

        // Attempts exhausted without converging. The shard is still consistent — every completed
        // swap was atomic — it just may not reflect the newest segments yet; the next refresh
        // continues from here.
        log::warn!(
            "shard refresh did not converge after {MAX_ATTEMPTS} attempts \
             (leader keeps replacing segments); serving the state reached so far",
        );
        Ok(())
    }

    /// One refresh pass over a single manifest snapshot.
    ///
    /// Completes as much as possible before reporting problems: newly-appeared segments are
    /// swapped in and every survivor is live-reloaded (they are independent) even when one of
    /// them fails. Not-found failures are then resolved against a re-read manifest — a segment
    /// the leader removed mid-attempt is dropped and reported as [`RefreshOutcome::ManifestChanged`]
    /// so the caller re-runs against the fresh manifest; one whose files are missing while the
    /// manifest still lists it escalates. Any other reload failure escalates after the loop.
    fn refresh_attempt(&self, hw_counter: &HardwareCounterCell) -> OperationResult<RefreshOutcome>
    where
        S::Fs: Send + Sync + Clone + 'static,
    {
        // 1. Snapshot the current on-disk segment set (backend-specific; see `SegmentEnumerator`).
        let on_disk = self.enumerator.list_segments()?;

        // 2. Load newly-appeared segments in parallel (outside the holder lock), then under the lock
        //    add them and drop removed ones, and collect the survivors to live_reload *after*
        //    releasing the lock — so reads only block during the cheap add/drop, not during load
        //    or reload. The manifest is superset-biased, so unloadable segments are skipped by
        //    `load_segments_parallel` and simply retried on the next refresh.
        let new_segments: Vec<(Uuid, PathBuf)> = {
            let holder = self.segments.read();
            on_disk
                .iter()
                .filter(|(uuid, _)| !holder.contains(uuid))
                .map(|(uuid, segment_path)| (*uuid, segment_path.clone()))
                .collect()
        };
        let loaded = load_segments_parallel::<S>(
            &self.search_pool,
            &self.fs,
            new_segments,
            self.load_profile.as_ref(),
        );

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
        //    Newly-added segments are already current, so they are skipped. Survivors are
        //    independent, so a failure does not stop the others from reloading; a failed segment
        //    keeps serving its pre-refresh state and its unapplied delta is retained in
        //    `pending_reload`, so a later reload replays the union and nothing is lost.
        let mut not_found: Vec<(Uuid, OperationError)> = Vec::new();
        let mut first_hard_error: Option<OperationError> = None;
        for (uuid, segment) in survivors {
            match segment.write().live_reload(&self.fs, hw_counter) {
                Ok(()) => {}
                // An essential file is gone; whether that is benign (the leader removed the
                // segment while we reloaded it) is decided against a re-read manifest below.
                Err(err) if err.is_not_found() => not_found.push((uuid, err)),
                Err(err) => {
                    log::error!("live_reload of segment {uuid} failed: {err}");
                    first_hard_error.get_or_insert(err);
                }
            }
        }

        // 5. Resolve not-found failures against a re-read manifest: gone from the manifest means
        //    the leader removed the segment mid-attempt — drop it and re-run to pick up its
        //    replacements; still listed means its essential files are genuinely missing — escalate.
        let outcome = if not_found.is_empty() {
            RefreshOutcome::Complete
        } else {
            let fresh = self.enumerator.list_segments()?;
            let mut gone: Vec<Uuid> = Vec::new();
            let mut still_listed_error: Option<OperationError> = None;
            for (uuid, err) in not_found {
                if fresh.contains_key(&uuid) {
                    log::error!(
                        "segment {uuid} is listed in the manifest but its files are missing: {err}",
                    );
                    still_listed_error.get_or_insert(err);
                } else {
                    log::debug!("segment {uuid} was removed by the leader during refresh");
                    gone.push(uuid);
                }
            }

            // Drop the removed segments even when escalating below: they are confirmed gone, so
            // keeping them would only leave handles to vanished files.
            if !gone.is_empty() {
                let mut holder = self.segments.write();
                for uuid in &gone {
                    holder.remove(uuid);
                }
            }

            if let Some(err) = still_listed_error {
                return Err(err);
            }
            RefreshOutcome::ManifestChanged
        };

        if let Some(err) = first_hard_error {
            return Err(err);
        }
        Ok(outcome)
    }
}
