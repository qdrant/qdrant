//! The live_reload pass of a follower that resolves point moves, see
//! [`point_moves`](segment::id_tracker::point_moves).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use common::universal_io::{IsNotFound as _, UniversalReadFsAsync};
use parking_lot::RwLock;
use rayon::prelude::*;
use segment::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use segment::id_tracker::point_moves::MoveResolution;
use segment::index::UniversalReadExt;
use segment::segment::read_only::ReadOnlySegment;
use uuid::Uuid;

use crate::EdgeConfig;
use crate::read_only::ReadOnlyEdgeShard;
use crate::read_only::enumerate::SegmentListing;
use crate::read_only::live_reload::LiveReloadOutcome;
use crate::read_only::load::{LoadedSegments, load_segments_parallel, reload_segments_parallel};
use crate::read_only::moves::{
    Trackers, UnheldTargets, is_segment_gone, read_move_log_tails, resolve_moves, unknown_targets,
};

/// A held segment, and the deletes a pass applies to it.
type SegmentDeletes<S> = (Uuid, Arc<RwLock<ReadOnlySegment<S>>>, MoveResolution);

/// How long a pass waits for the reads of the previous epoch before it masks superseded copies.
/// Past it, masks and releases wait for the next pass, which is stale but never missing.
const READ_EPOCH_WAIT: Duration = Duration::from_secs(5);

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// One live_reload pass over a single manifest snapshot, resolving point moves: the
    /// counterpart of [`live_reload_attempt`](Self::live_reload_attempt) for a follower with the
    /// `resolve_point_moves` feature flag.
    ///
    ///
    /// Completes as much as possible before reporting problems: newly-appeared segments are
    /// swapped in and every held segment is live-reloaded (they are independent) even when one of
    /// them fails. Not-found failures are then resolved against a re-read manifest — a segment
    /// the leader removed mid-attempt is dropped and reported as [`LiveReloadOutcome::ManifestChanged`]
    /// so the caller re-runs against the fresh manifest; one whose files are missing while the
    /// manifest still lists it escalates. Any other reload failure escalates after the pass.
    ///
    /// All new data of a pass is installed before any of its deletes, so a delete never becomes
    /// visible before the copy that justifies it (see
    /// [`point_moves`](segment::id_tracker::point_moves)):
    ///
    /// 1. read the manifest, with the states of the segments a reader must not load;
    /// 2. load the newly listed segments, without installing them;
    /// 3. reload the held segments, each installing its new data; with point moves resolved, their
    ///    new tombstones wait;
    /// 4. resolve point moves across all of them;
    /// 5. install the new segments, and drop the superseded segments the drop rule allows;
    /// 6. start a new read epoch, and wait for the reads of the previous one to finish;
    /// 7. apply every held segment's deletes.
    pub(super) fn live_reload_attempt_resolving(
        &self,
        is_stopped: &AtomicBool,
    ) -> OperationResult<LiveReloadOutcome>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        check_process_stopped(is_stopped)?;

        // 1. Snapshot the current on-disk segment set (backend-specific; see `SegmentEnumerator`).
        let listing = self.enumerator.list_segments_with_states()?;
        let on_disk = &listing.usable;
        check_process_stopped(is_stopped)?;

        // 2. Load newly-appeared segments in parallel, outside the holder lock. They are installed
        //    in step 5 only, after the reloads and the move resolution, so that nothing is visible
        //    before the deletes it justifies are decided. The manifest is superset-biased, so
        //    unloadable segments are skipped by `load_segments_parallel` and simply retried on the
        //    next live_reload.
        let new_segments: Vec<(Uuid, PathBuf)> = {
            let holder = self.segments.read();
            on_disk
                .iter()
                .filter(|(uuid, _)| !holder.contains(uuid))
                .map(|(uuid, listing)| (*uuid, listing.path.clone()))
                .collect()
        };
        let LoadedSegments {
            segments: mut loaded,
            failed,
        } = load_segments_parallel::<S>(
            &self.load_pool,
            &self.fs,
            new_segments,
            self.load_profile.as_ref(),
            is_stopped,
        )?;
        // Whether the replacement of any superseded segment is in view
        let replacements_loaded = failed == 0;
        check_process_stopped(is_stopped)?;

        // 3. Live-reload every held segment to assimilate new appends and deletes from data: the
        //    survivors, and superseded segments the drop rule kept. Reloading the latter is what
        //    notices their files are gone.
        let held: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)> = {
            let holder = self.segments.read();
            holder
                .uuids()
                .into_iter()
                .filter_map(|uuid| Some((uuid, holder.segment_arc(&uuid)?)))
                .collect()
        };
        let results = reload_segments_parallel(&self.load_pool, held.clone(), is_stopped)?;

        let mut not_found: Vec<(Uuid, OperationError)> = Vec::new();
        let mut first_hard_error: Option<OperationError> = None;
        // Whether every appendable survivor, a possible move target, is fresh in this pass
        let mut appendables_reloaded = true;
        let mut superseded_gone: Vec<Uuid> = Vec::new();
        for (uuid, result) in results {
            let Err(err) = result else {
                continue;
            };
            let listed = on_disk.contains_key(&uuid);
            if listed && self.segments.read().is_appendable(&uuid) {
                appendables_reloaded = false;
            }
            match err {
                // A superseded segment the drop rule kept: its files are gone after the grace
                err if !listed && err.is_not_found() => superseded_gone.push(uuid),
                // An essential file is gone; whether that is benign (the leader removed the
                // segment while we reloaded it) is decided against a re-read manifest below.
                err if err.is_not_found() => not_found.push((uuid, err)),
                err => {
                    log::error!("live_reload of segment {uuid} failed: {err}");
                    first_hard_error.get_or_insert(err);
                }
            }
        }

        // Resolve not-found failures against a re-read manifest: gone from the manifest means the
        // leader removed the segment mid-attempt — drop it and re-run to pick up its replacements;
        // still listed means its essential files are genuinely missing — escalate after the pass.
        let mut outcome = LiveReloadOutcome::Complete;
        let mut still_listed_error: Option<OperationError> = None;
        let mut gone: Vec<Uuid> = superseded_gone;
        if !not_found.is_empty() {
            check_process_stopped(is_stopped)?;
            let fresh = self.enumerator.list_segments()?;
            for (uuid, err) in not_found {
                if fresh.contains_key(&uuid) {
                    log::error!(
                        "segment {uuid} is listed in the manifest but its files are missing: {err}",
                    );
                    still_listed_error.get_or_insert(err);
                } else {
                    log::debug!("segment {uuid} was removed by the leader during live_reload");
                    gone.push(uuid);
                    outcome = LiveReloadOutcome::ManifestChanged;
                }
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
        let held: Vec<(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)> = held
            .into_iter()
            .filter(|(uuid, _)| !gone.contains(uuid))
            .collect();
        check_process_stopped(is_stopped)?;

        // 4. Resolve point moves across every segment held or about to be installed.
        let mut resolutions =
            self.resolve_point_moves(&held, &mut loaded, &listing, replacements_loaded);
        check_process_stopped(is_stopped)?;

        // 5. Install the new segments, with their deletes applied first: no read sees them yet, so
        //    those deletes cannot hide a point from one. Drop a superseded segment only when its
        //    replacement is surely in view, every possible move target is fresh, and no copy on it
        //    may be the only visible one of its point. The swap and the config re-derivation that
        //    follows it are one indivisible step: a config lagging behind the segment set must never
        //    be observable.
        for (uuid, segment) in &mut loaded {
            if let Some(resolution) = resolutions.remove(uuid) {
                segment.apply_point_moves(&resolution)?;
            }
        }
        check_process_stopped(is_stopped)?;
        {
            let mut holder = self.segments.write();

            // Add before drop: when an optimization migrates points from old to new segments, both
            // must be momentarily visible so migrated points never disappear.
            for (uuid, segment) in loaded {
                let appendable = segment.segment_config.is_appendable();
                holder.insert(uuid, appendable, Arc::new(RwLock::new(segment)));
            }

            let no_resolution = MoveResolution::default();
            for (uuid, segment) in &held {
                if on_disk.contains_key(uuid) {
                    continue;
                }
                let resolution = resolutions.get(uuid).unwrap_or(&no_resolution);
                let holds_moves = segment
                    .read()
                    .id_tracker
                    .borrow()
                    .holds_point_moves_beyond(resolution);
                if replacements_loaded && appendables_reloaded && !holds_moves {
                    holder.remove(uuid);
                    resolutions.remove(uuid);
                } else {
                    log::debug!(
                        "keeping superseded segment {uuid}: its replacement or a moved point's new copy may not be in view yet",
                    );
                }
            }
        }

        // Re-derive the config from the current segments — a read-only follower has no
        // edge_config.json, so the segments are the source of truth. Folded over all segments
        // in UUID order, so the derivation is deterministic and a segment carrying no
        // information about a parameter never masks one that does. No-op for an empty shard
        // (the previous snapshot stays in place until segments appear).
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

        // 6. Masking a copy, or releasing a held delete, is only safe once no read is left that may
        //    have missed the new copy: one that visited its target before step 3 or 5. Plain
        //    deletes depend on no other segment. If the wait times out, masks and releases wait
        //    for the next pass, which is stale but never missing.
        let masks_pending = resolutions
            .values()
            .any(|resolution| !resolution.superseded.is_empty());
        let waited = !masks_pending
            || self
                .read_epochs
                .advance_and_wait(READ_EPOCH_WAIT, is_stopped);
        if !waited {
            log::debug!("reads of the previous epoch still running, masks wait for the next pass");
        }

        // 7. Apply the deletes of the held segments, each under its own lock, in parallel.
        let deletes: Vec<SegmentDeletes<S>> = {
            let holder = self.segments.read();
            resolutions
                .into_iter()
                .filter_map(|(uuid, resolution)| {
                    let resolution = if waited {
                        resolution
                    } else {
                        resolution.without_superseded()
                    };
                    let segment = holder.segment_arc(&uuid)?;
                    (!resolution.is_empty()).then_some((uuid, segment, resolution))
                })
                .collect()
        };
        let applied: Vec<(Uuid, OperationResult<()>)> = self.load_pool.install(|| {
            deletes
                .into_par_iter()
                .map(|(uuid, segment, resolution)| {
                    (uuid, segment.write().apply_point_moves(&resolution))
                })
                .collect()
        });
        for (uuid, result) in applied {
            if let Err(err) = result {
                log::error!("applying the deletes of segment {uuid} failed: {err}");
                first_hard_error.get_or_insert(err);
            }
        }

        if let Some(err) = still_listed_error {
            return Err(err);
        }
        if let Some(err) = first_hard_error {
            return Err(err);
        }
        check_process_stopped(is_stopped)?;
        Ok(outcome)
    }

    /// Resolve point moves across `held` and `loaded`: classify the tombstones that wait for a tail
    /// read of their move log, then decide every segment's deletes. Nothing is applied here.
    ///
    /// Segments whose id tracker does not resolve moves take no part, so without the
    /// `resolve_point_moves` flag this returns nothing.
    fn resolve_point_moves(
        &self,
        held: &[(Uuid, Arc<RwLock<ReadOnlySegment<S>>>)],
        loaded: &mut [(Uuid, ReadOnlySegment<S>)],
        listing: &SegmentListing,
        replacements_loaded: bool,
    ) -> HashMap<Uuid, MoveResolution>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let trackers: Trackers<S> = held
            .iter()
            .map(|(uuid, segment)| (*uuid, segment.read().id_tracker.clone()))
            .chain(
                loaded
                    .iter()
                    .map(|(uuid, segment)| (*uuid, segment.id_tracker.clone())),
            )
            .collect();
        if !trackers
            .iter()
            .any(|(_, tracker)| tracker.borrow().resolves_point_moves())
        {
            return HashMap::new();
        }

        // Tail reads of the move logs, for tombstones no record in the listed part names. The read
        // is IO, so no lock is held across it.
        let requests: Vec<(Uuid, PathBuf, u64)> = trackers
            .iter()
            .filter_map(|(uuid, tracker)| {
                let (path, start) = tracker.borrow().point_moves_tail_to_read()?;
                Some((*uuid, path, start))
            })
            .collect();
        if !requests.is_empty() {
            for (uuid, start, bytes) in read_move_log_tails(&self.fs, requests) {
                if let Some((_, segment)) = held.iter().find(|(held, _)| *held == uuid) {
                    segment.write().ingest_point_moves_tail(start, &bytes);
                } else if let Some((_, segment)) = loaded.iter_mut().find(|(new, _)| *new == uuid) {
                    segment.ingest_point_moves_tail(start, &bytes);
                }
            }
        }

        // Move targets neither held nor listed: probe whether their directory is gone
        let unknown = unknown_targets(&trackers, listing, &self.gone_segments.lock());
        for segment in unknown {
            match is_segment_gone(&self.fs, &self.path, segment) {
                Ok(true) => {
                    self.gone_segments.lock().insert(segment);
                }
                Ok(false) => {}
                Err(err) => {
                    log::warn!(
                        "probing move target segment {segment} failed, its moves wait: {err}"
                    );
                }
            }
        }

        let gone = self.gone_segments.lock();
        resolve_moves(
            &trackers,
            &UnheldTargets {
                listing,
                replacements_loaded,
                gone: &gone,
            },
        )
    }
}
