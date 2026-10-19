//! Point move resolution of a follower with the `resolve_point_moves` feature flag, see
//! [`point_moves`](segment::id_tracker::point_moves).
//!
//! It runs after each live_reload, separately from it. The reload installs every segment's new
//! data, applies the tombstones it reads, and keeps the move records; resolution then decides,
//! across all segments, which old copies a settled move supersedes, and masks them. So all new data
//! of a pass is visible before any copy it supersedes is masked, and an old copy never goes before
//! the copy that replaces it is visible.
//!
//! A copy that a moved point's new copy supersedes is only deleted by the next live_reload. A read
//! visits the segments one at a time, and several times (prefetch, rescore, payload retrieval), so
//! one that visited the move's target before the new copy was installed may still visit the
//! source. Deleting the old copy right away would let that read miss the point. One live_reload
//! later, only reads that outlast the time between two live_reloads can still miss it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

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
    Trackers, UnheldTargets, is_segment_gone, resolve_moves, unknown_targets,
};

/// A held segment.
type HeldSegment<S> = (Uuid, Arc<RwLock<ReadOnlySegment<S>>>);

/// A held segment, and the copies resolution masks in it.
type SegmentDeletes<S> = (Uuid, Arc<RwLock<ReadOnlySegment<S>>>, MoveResolution);

impl<S: UniversalReadExt + 'static> ReadOnlyEdgeShard<S> {
    /// Resolve point moves across every held segment, after a live_reload installed its new data.
    /// `all_reloaded` tells whether that live_reload reloaded every listed segment without error.
    ///
    /// 1. read the manifest, with the states of the segments a reader must not load;
    /// 2. reload the superseded segments the follower keeps, which live_reload skips: they may hold
    ///    tombstones and move records the writer wrote before their replacement was listed, and a
    ///    reload notices when their files are gone;
    /// 3. decide every segment's superseded copies;
    /// 4. drop the superseded segments the drop rule allows;
    /// 5. mask the superseded copies the previous live_reload decided too; keep the others for the
    ///    next one.
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

        // 3. Decide every segment's superseded copies
        let held = self.held_segments();
        let mut resolutions = self.decide_point_moves(&held, &listing, all_listed_held);
        check_process_stopped(is_stopped)?;

        // 4. Drop a superseded segment only when its replacement is surely in view, every possible
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

        // 5. A superseded copy is masked one live_reload later, see the module docs: now if the
        //    previous live_reload decided it too, otherwise it waits for the next one
        let mut previous = std::mem::take(&mut *self.superseded_to_delete.lock());
        let mut deferred = HashMap::new();
        let deletes: HashMap<Uuid, MoveResolution> = resolutions
            .into_iter()
            .map(|(uuid, MoveResolution { superseded })| {
                let previous = previous.remove(&uuid).unwrap_or_default();
                let (now, later): (Vec<_>, Vec<_>) = superseded
                    .into_iter()
                    .partition(|slot| previous.binary_search(slot).is_ok());
                if !later.is_empty() {
                    deferred.insert(uuid, later);
                }
                (uuid, MoveResolution { superseded: now })
            })
            .collect();
        *self.superseded_to_delete.lock() = deferred;
        if let Err(err) = self.apply_deletes(deletes) {
            first_error.get_or_insert(err);
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Delete the superseded copies the last live_reload kept for the next one, right away. Only
    /// for a follower no read can have started on yet.
    pub(super) fn delete_superseded_now(&self) -> OperationResult<()>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let deferred = std::mem::take(&mut *self.superseded_to_delete.lock());
        let deletes = deferred
            .into_iter()
            .map(|(uuid, superseded)| (uuid, MoveResolution { superseded }))
            .collect();
        self.apply_deletes(deletes)
    }

    /// Apply `deletes` to the held segments, each under its own lock, in parallel. Applies as much
    /// as possible before reporting the first error.
    fn apply_deletes(&self, deletes: HashMap<Uuid, MoveResolution>) -> OperationResult<()>
    where
        S::Fs: UniversalReadFsAsync + Send + Sync + Clone + 'static,
    {
        let deletes: Vec<SegmentDeletes<S>> = {
            let holder = self.segments.read();
            deletes
                .into_iter()
                .filter_map(|(uuid, resolution)| {
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
        let mut first_error = None;
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

    /// Decide what to delete in each of `held`, resolving every segment against the others. Nothing
    /// is applied here.
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
