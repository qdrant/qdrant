//! Point moves of the read-only appendable id tracker, see
//! [`point_moves`](crate::id_tracker::point_moves).
//!
//! A moved-out record names a slot, which this view may not have committed yet: the move log can
//! be read further than the mappings log. Such a slot is masked ahead, and dropped as soon as it
//! commits.

use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;

use super::{LiveReloadResult, ReadOnlyAppendableIdTracker};
use crate::id_tracker::DELETED_POINT_VERSION;
use crate::id_tracker::point_moves::{
    MoveEntry, MoveKind, MoveResolution, PointMoves, PointMovesMode, PointMovesView,
    moved_out_slots,
};
use crate::types::PointIdType;

/// The move log of an appendable segment, and what the view did with the records read from it.
#[derive(Debug)]
pub(super) struct AppendableMoves<S: UniversalRead> {
    pub(super) view: PointMovesView<S>,
    /// The records kept to resolve moves with, with [`PointMovesMode::Resolve`]. `None` with
    /// [`PointMovesMode::Apply`], where a moved-out record masks its slot once read.
    pub(super) state: Option<PointMoves>,
    /// Slots masked for a move that were not committed when they were masked. Each is dropped as
    /// soon as it commits.
    pub(super) premasked: RoaringBitmap,
}

impl<S: UniversalRead> AppendableMoves<S> {
    pub(super) fn new(view: PointMovesView<S>, mode: PointMovesMode) -> Self {
        Self {
            view,
            state: match mode {
                PointMovesMode::Apply => None,
                PointMovesMode::Resolve => Some(PointMoves::default()),
            },
            premasked: RoaringBitmap::new(),
        }
    }
}

impl<S: UniversalRead> ReadOnlyAppendableIdTracker<S> {
    /// Whether `internal_id` has settled in this view: the insert that claimed it has been read, its
    /// version is published and not the placeholder a later publish covers a lost slot with, the
    /// insert is not staged waiting for its components, and the view does not hide it as deferred.
    /// From then on the view shows the point on that slot, or a later state of it.
    pub fn is_settled(&self, internal_id: PointOffsetType) -> bool {
        self.max_claimed_internal_id
            .is_some_and(|max_claimed| internal_id <= max_claimed)
            && self
                .internal_to_version
                .get(internal_id as usize)
                .is_some_and(|&version| version != DELETED_POINT_VERSION)
            && self.staged_external_id(internal_id).is_none()
            && self
                .mappings
                .deferred_internal_id()
                .is_none_or(|cutoff| internal_id < cutoff)
    }

    /// The point staged on `internal_id`, if any: reported to the components, not published yet.
    fn staged_external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        // Empty but between a reload and its publish, or after a failed component reload
        self.staged_inserts
            .iter()
            .find(|&(_, &staged)| staged == internal_id)
            .map(|(&external_id, _)| external_id)
    }

    /// The records read from the move log, when this tracker resolves moves.
    pub fn point_moves(&self) -> Option<&PointMoves> {
        self.moves.state.as_ref()
    }

    pub fn point_moves_mut(&mut self) -> Option<&mut PointMoves> {
        self.moves.state.as_mut()
    }

    /// Whether `internal_id` is deleted in this view for good: committed without a live point, or
    /// masked before it committed, which drops it once it does.
    pub fn is_retired(&self, internal_id: PointOffsetType) -> bool {
        !self.may_hold_point(internal_id) || self.moves.premasked.contains(internal_id)
    }

    /// Whether `internal_id` holds a live point in this view, or may hold one later: it is pending,
    /// or not claimed yet. A committed slot without a live point is gone for good.
    fn may_hold_point(&self, internal_id: PointOffsetType) -> bool {
        if self.mappings.external_id(internal_id).is_some()
            || self.staged_external_id(internal_id).is_some()
        {
            return true;
        }
        let committed = (internal_id as usize) < self.internal_to_version.len();
        let claimed = self
            .max_claimed_internal_id
            .is_some_and(|max_claimed| internal_id <= max_claimed);
        !(claimed && committed)
    }

    /// Take in entries read from the move log. Applying moves, the slots of moved-out records are
    /// masked at once, collecting the committed ones that were live into `deleted`. Resolving them,
    /// the records are kept.
    pub(super) fn take_point_moves(
        &mut self,
        entries: Vec<MoveEntry>,
        deleted: &mut Vec<PointOffsetType>,
    ) {
        let entries = self.skip_retired_moved_out(entries);
        match self.moves.state.as_mut() {
            Some(state) => state.ingest(entries),
            None => {
                for internal_id in moved_out_slots(&entries) {
                    self.mask_slot(internal_id, deleted);
                }
            }
        }
    }

    /// `entries` without the moved-out pairs whose local slot is deleted for good in this view, see
    /// [`Self::is_retired`]: no record can decide anything about such a slot anymore.
    fn skip_retired_moved_out(&self, entries: Vec<MoveEntry>) -> Vec<MoveEntry> {
        entries
            .into_iter()
            .filter_map(|mut entry| {
                if entry.kind == MoveKind::MovedOut {
                    entry
                        .pairs
                        .retain(|&(internal_id, _)| !self.is_retired(internal_id));
                    if entry.pairs.is_empty() {
                        return None;
                    }
                }
                Some(entry)
            })
            .collect()
    }

    /// Take back the staged inserts on slots masked before they committed, once a reload has read
    /// the new part of the mappings log. The point goes with it: the copy it replaces is reported
    /// deleted.
    pub(super) fn unstage_masked_inserts(&mut self, deleted: &mut Vec<PointOffsetType>) {
        if self.moves.premasked.is_empty() {
            return;
        }
        let staged: Vec<(PointIdType, PointOffsetType)> = self
            .staged_inserts
            .iter()
            .map(|(&external_id, &internal_id)| (external_id, internal_id))
            .collect();
        for (external_id, internal_id) in staged {
            if self.moves.premasked.remove(internal_id) {
                self.staged_inserts.remove(&external_id);
                if let Some(previous) = self.mappings.set_link(external_id, internal_id)
                    && previous != internal_id
                {
                    deleted.push(previous);
                }
                self.mappings.drop(external_id);
            }
        }
    }

    /// Fold the moved-in records whose local slot has settled into the per-source index, and
    /// forget the moved-out records of the slots in `deleted`, which are gone in this view now.
    pub(super) fn update_point_moves(&mut self, deleted: &[PointOffsetType]) {
        let Some(state) = self.moves.state.as_ref() else {
            return;
        };
        let settled: RoaringBitmap = state
            .unsettled_slots()
            .filter(|&internal_id| self.is_settled(internal_id))
            .collect();
        if let Some(state) = self.moves.state.as_mut() {
            state.settle(&settled);
            state.forget(deleted.iter().copied());
        }
    }

    /// Whether a moved-out record names a copy this view may still show: its move may not have
    /// settled, so it may be the only visible copy of its point.
    pub fn holds_point_moves(&self) -> bool {
        self.point_moves().is_some_and(PointMoves::has_moved_out)
    }

    /// Whether a moved-out record names a copy this view may still show once `resolution` is
    /// applied.
    pub fn holds_point_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        self.point_moves().is_some_and(|state| {
            state
                .moved_out()
                .any(|(slot, _)| resolution.superseded.binary_search(&slot).is_err())
        })
    }

    /// The slots to delete now: those of `superseded` this view may still show. `superseded` are
    /// the slots of this segment whose copy a settled move replaced, as the shard found them: by
    /// this segment's moved-out records, or by other segments' moved-in records.
    pub fn resolve_point_moves(&self, superseded: Option<&RoaringBitmap>) -> MoveResolution {
        let mut resolution = MoveResolution::default();
        if let Some(superseded) = superseded {
            resolution.superseded.extend(
                superseded
                    .iter()
                    .filter(|&internal_id| !self.is_retired(internal_id)),
            );
        }
        resolution.normalize();
        resolution
    }

    /// Mask the slots `resolution` names. Returns the committed slots that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        let mut deleted = Vec::new();
        for &internal_id in &resolution.superseded {
            self.mask_slot(internal_id, &mut deleted);
        }
        if let Some(state) = self.moves.state.as_mut() {
            state.forget(resolution.superseded.iter().copied());
        }

        deleted.sort_unstable();
        deleted.dedup();
        LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        }
    }

    /// Mask `internal_id` for a move: drop the point it holds, or cancel the staged insert on it,
    /// collecting the slots reported deleted into `deleted`. A slot not committed yet is masked
    /// ahead, see [`AppendableMoves::premasked`].
    fn mask_slot(&mut self, internal_id: PointOffsetType, deleted: &mut Vec<PointOffsetType>) {
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            // The point leaves the segment, with every copy it has here
            deleted.extend(self.mappings.heads(&external_id));
            self.mappings.drop(external_id);
        } else if let Some(external_id) = self.staged_external_id(internal_id) {
            // Reported to the components already, so reported deleted
            self.staged_inserts.remove(&external_id);
            deleted.push(internal_id);
        } else if self.may_hold_point(internal_id) {
            self.moves.premasked.insert(internal_id);
        }
    }
}
