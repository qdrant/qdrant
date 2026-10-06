//! Point move resolution of a follower with the `resolve_point_moves` feature flag, see
//! [`point_moves`](segment::id_tracker::point_moves).
//!
//! It runs after each live_reload attempt, separately from it. The reload installs every segment's
//! new data and holds back the deletes it reads; resolution then decides, across all segments,
//! which of those deletes may land, and applies them. So all new data of a pass is visible before
//! any of its deletes, and a delete never becomes visible before the copy that justifies it.

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
use crate::read_only::load::reload_segments_parallel;
use crate::read_only::moves::{
    Trackers, UnheldTargets, is_segment_gone, read_move_log_tails, resolve_moves, unknown_targets,
};

/// A held segment.
type HeldSegment<S> = (Uuid, Arc<RwLock<ReadOnlySegment<S>>>);

/// A held segment, and the deletes resolution applies to it.
type SegmentDeletes<S> = (Uuid, Arc<RwLock<ReadOnlySegment<S>>>, MoveResolution);

/// How long resolution waits for the reads of the previous epoch before it masks superseded
/// copies. Past it, masks and releases wait for the next pass, which is stale but never missing.
const READ_EPOCH_WAIT: Duration = Duration::from_secs(5);

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Resolve point moves across every held segment, after a live_reload attempt installed its new
    /// data. `all_reloaded` tells whether that attempt reloaded every listed segment without error.
    ///
    /// 1. read the manifest, with the states of the segments a reader must not load;
    /// 2. reload the superseded segments the follower keeps, which live_reload skips: they may hold
    ///    deletes the writer wrote before their replacement was listed, and a reload notices when
    ///    their files are gone;
    /// 3. read the end of the move logs, for held deletes no record names yet;
    /// 4. decide every segment's deletes;
    /// 5. drop the superseded segments the drop rule allows;
    /// 6. start a new read epoch, and wait for the reads of the previous one to finish;
    /// 7. apply every segment's deletes.
    ///
    /// Completes as much as possible before reporting the first error.
    pub(super) fn resolve_point_moves(
        &self,
        all_reloaded: bool,
        is_stopped: &AtomicBool,
    ) -> OperationResult<()>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        check_process_stopped(is_stopped)?;
        let mut first_error: Option<OperationError> = None;

        // 1. The manifest, and whether every segment it lists is in view
        let listing = self.enumerator.list_segments_with_states()?;
        let all_listed_held = {
            let holder = self.segments.read();
            listing.usable.keys().all(|uuid| holder.contains(uuid))
        };
        check_process_stopped(is_stopped)?;

        // 2. Reload the superseded segments the follower keeps
        let superseded: Vec<HeldSegment<S>> = self
            .held_segments()
            .into_iter()
            .filter(|(uuid, _)| !listing.usable.contains_key(uuid))
            .collect();
        let results = reload_segments_parallel(
            &self.load_pool,
            &self.fs,
            superseded,
            self.load_profile.as_ref(),
            is_stopped,
        )?;
        let mut gone: Vec<Uuid> = Vec::new();
        for (uuid, result) in results {
            match result {
                Ok(()) => {}
                // Its files are gone after the leader's grace period
                Err(err) if err.is_not_found() => gone.push(uuid),
                Err(err) => {
                    log::error!("live_reload of superseded segment {uuid} failed: {err}");
                    first_error.get_or_insert(err);
                }
            }
        }
        if !gone.is_empty() {
            let mut holder = self.segments.write();
            for uuid in &gone {
                holder.remove(uuid);
            }
        }
        check_process_stopped(is_stopped)?;

        // 3. and 4. Decide every segment's deletes
        let held = self.held_segments();
        let mut resolutions = self.decide_point_moves(&held, &listing, all_listed_held);
        check_process_stopped(is_stopped)?;

        // 5. Drop a superseded segment only when its replacement is surely in view, every possible
        //    move target is fresh, and no copy on it may be the only visible one of its point
        let mut dropped = !gone.is_empty();
        {
            let mut holder = self.segments.write();
            let no_resolution = MoveResolution::default();
            for (uuid, segment) in &held {
                if listing.usable.contains_key(uuid) {
                    continue;
                }
                let resolution = resolutions.get(uuid).unwrap_or(&no_resolution);
                let holds_moves = segment
                    .read()
                    .id_tracker
                    .borrow()
                    .holds_point_moves_beyond(resolution);
                if all_listed_held && all_reloaded && !holds_moves {
                    holder.remove(uuid);
                    resolutions.remove(uuid);
                    dropped = true;
                } else {
                    log::debug!(
                        "keeping superseded segment {uuid}: its replacement or a moved point's new copy may not be in view yet",
                    );
                }
            }
        }
        if dropped {
            self.derive_config_after_drop();
        }

        // 6. Masking a copy, or releasing a held delete, is only safe once no read is left that may
        //    have missed the new copy: one that visited its target before the reload installed
        //    it. Plain deletes depend on no other segment. If the wait times out, masks and
        //    releases wait for the next pass, which is stale but never missing.
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

        // 7. Apply the deletes of every segment, each under its own lock, in parallel
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
                first_error.get_or_insert(err);
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Every segment the follower holds.
    fn held_segments(&self) -> Vec<HeldSegment<S>> {
        let holder = self.segments.read();
        holder
            .uuids()
            .into_iter()
            .filter_map(|uuid| Some((uuid, holder.segment_arc(&uuid)?)))
            .collect()
    }

    /// Decide what to delete in each of `held`: classify the deletes that wait for a read of their
    /// move log's end, then resolve every segment against the others. Nothing is applied here.
    ///
    /// Segments whose id tracker does not resolve moves take no part.
    fn decide_point_moves(
        &self,
        held: &[HeldSegment<S>],
        listing: &SegmentListing,
        all_listed_held: bool,
    ) -> HashMap<Uuid, MoveResolution>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let trackers: Trackers<S> = held
            .iter()
            .map(|(uuid, segment)| (*uuid, segment.read().id_tracker.clone()))
            .collect();
        if !trackers
            .iter()
            .any(|(_, tracker)| tracker.borrow().resolves_point_moves())
        {
            return HashMap::new();
        }

        // Reads of the move logs' ends, for deletes no record in the part read so far names. The
        // read is IO, so no lock is held across it.
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
                replacements_loaded: all_listed_held,
                gone: &gone,
            },
        )
    }

    /// Re-derive the config once superseded segments are dropped, the way live_reload derives it
    /// after installing new segments: folded over all segments in UUID order.
    fn derive_config_after_drop(&self) {
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
    }
}
