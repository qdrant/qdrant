//! Point moves of the read-only appendable id tracker, see
//! [`point_moves`](crate::id_tracker::point_moves).
//!
//! Tombstones here are `Delete` entries in the mappings log, which name an external id rather than
//! a slot. A held delete is therefore resolved at its position in the log to the slots it retires
//! there: the committed slot of the point and its pending ones, if any. Applying it later drops
//! those slots only while the point still holds them, so a later re-insert of the same id is not
//! affected.

use std::path::PathBuf;

use ahash::AHashSet;
use common::types::{DeferredBehavior, PointOffsetType};
use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use super::{LiveReloadResult, ReadOnlyAppendableIdTracker};
use crate::id_tracker::DELETED_POINT_VERSION;
use crate::id_tracker::point_moves::{
    Hold, MoveResolution, PointMoves, PointMovesView, SlotRef, skip_retired_moved_out,
};
use crate::types::PointIdType;

/// The move log of an appendable segment, the records read from it, and the deletes held back.
#[derive(Debug)]
pub(super) struct AppendableMoves<S: UniversalRead> {
    pub(super) view: PointMovesView<S>,
    pub(super) state: PointMoves,
    /// `Delete` entries read from the mappings log and not applied yet, in log order.
    pub(super) held: Vec<HeldDelete>,
    /// Slots a settled move supersedes that were not committed when the shard masked them. Each is
    /// dropped as soon as it commits.
    pub(super) premasked: AHashSet<PointOffsetType>,
}

/// A `Delete` of the mappings log, held back.
#[derive(Clone, Debug)]
pub(super) struct HeldDelete {
    pub(super) external_id: PointIdType,
    /// The point's committed slot at the delete's position in the log, if any.
    pub(super) committed: Option<PointOffsetType>,
    /// The point's staged and unversioned inserts at the delete's position in the log: the delete
    /// cancels them.
    pub(super) pending: SmallVec<[PointOffsetType; 1]>,
    pub(super) hold: Hold,
}

impl HeldDelete {
    /// The slots the delete retires.
    pub(super) fn slots(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.committed
            .into_iter()
            .chain(self.pending.iter().copied())
    }

    /// The slot the shard's resolution names the delete by.
    pub(super) fn key(&self) -> PointOffsetType {
        self.committed
            .or_else(|| self.pending.first().copied())
            .expect("a held delete retires a slot")
    }
}

impl<S: UniversalRead> AppendableMoves<S> {
    pub(super) fn new(view: PointMovesView<S>) -> Self {
        Self {
            view,
            state: PointMoves::default(),
            held: Vec::new(),
            premasked: AHashSet::new(),
        }
    }

    /// Classify every unclassified delete against the records read so far: a move if a record
    /// names one of its slots, a plain delete otherwise.
    fn classify(&mut self) {
        for held in &mut self.held {
            if held.hold == Hold::Unclassified {
                held.hold = if held.slots().any(|slot| self.state.names(slot)) {
                    Hold::Move
                } else {
                    Hold::Plain
                };
            }
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
        self.moves.as_ref().map(|moves| &moves.state)
    }

    pub fn point_moves_mut(&mut self) -> Option<&mut PointMoves> {
        self.moves.as_mut().map(|moves| &mut moves.state)
    }

    /// Whether `internal_id` is deleted in this view for good: committed without a live point, or
    /// masked before it committed, which drops it once it does.
    pub fn is_retired(&self, internal_id: PointOffsetType) -> bool {
        !self.may_hold_point(internal_id)
            || self
                .moves
                .as_ref()
                .is_some_and(|moves| moves.premasked.contains(&internal_id))
    }

    /// Where a tail read of the move log has to start, if some delete waits for one to be
    /// classified.
    pub fn point_moves_tail_to_read(&self) -> Option<(PathBuf, u64)> {
        let moves = self.moves.as_ref()?;
        moves
            .held
            .iter()
            .any(|held| held.hold == Hold::Unclassified)
            .then(|| (moves.view.path().to_path_buf(), moves.view.read_to()))
    }

    /// Take in a tail read of the move log from byte offset `start`, and classify the deletes that
    /// waited for it.
    pub fn ingest_point_moves_tail(&mut self, start: u64, bytes: &[u8]) {
        let Some(moves) = self.moves.as_mut() else {
            return;
        };
        if start != moves.view.read_to() {
            return;
        }
        let entries = moves.view.consume(start, bytes);
        let entries = skip_retired_moved_out(entries, |internal_id| self.is_retired(internal_id));
        let Some(moves) = self.moves.as_mut() else {
            return;
        };
        moves.state.ingest(entries);
        moves.classify();
        self.settle_point_moves();
    }

    /// Whether some delete is still held back for a move that did not settle, or for a tail read
    /// that did not happen.
    pub fn holds_point_moves(&self) -> bool {
        self.moves
            .as_ref()
            .is_some_and(|moves| moves.held.iter().any(|held| held.hold != Hold::Plain))
    }

    /// Whether some delete stays held back for a move once `resolution` is applied.
    pub fn holds_point_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        self.moves.as_ref().is_some_and(|moves| {
            moves.held.iter().any(|held| {
                held.hold != Hold::Plain
                    && resolution.superseded.binary_search(&held.key()).is_err()
            })
        })
    }

    /// Hold back a `Delete` of `external_id` read from the mappings log, resolved to the slots it
    /// retires at this position of the log. A delete of a point this view holds nowhere is a
    /// no-op, as when applied at once.
    pub(super) fn hold_delete(&mut self, external_id: PointIdType) {
        let committed = self
            .mappings
            .internal_id_with_behavior(&external_id, DeferredBehavior::WithDeferred);
        let pending: SmallVec<[PointOffsetType; 1]> = self
            .staged_inserts
            .get(&external_id)
            .copied()
            .into_iter()
            .chain(
                self.unversioned_inserts
                    .get(&external_id)
                    .into_iter()
                    .flatten()
                    .copied(),
            )
            .collect();
        let Some(moves) = self.moves.as_mut() else {
            return;
        };
        if committed.is_none() && pending.is_empty() {
            return;
        }
        let hold = if committed
            .into_iter()
            .chain(pending.iter().copied())
            .any(|slot| moves.state.names(slot))
        {
            Hold::Move
        } else {
            Hold::Unclassified
        };
        moves.held.push(HeldDelete {
            external_id,
            committed,
            pending,
            hold,
        });
    }

    /// The unversioned inserts the drain must not stage: those a held delete cancels whose slot is
    /// covered with the placeholder version. Such a slot was claimed by a writer that stopped
    /// before publishing it, and its data may be half-written. Applied in log order, the delete
    /// would have cancelled the insert before the placeholder made it look committed.
    pub(super) fn cancelled_placeholders(&self) -> AHashSet<(PointIdType, PointOffsetType)> {
        let Some(moves) = self.moves.as_ref() else {
            return AHashSet::new();
        };
        moves
            .held
            .iter()
            .flat_map(|held| {
                held.pending
                    .iter()
                    .map(move |&internal_id| (held.external_id, internal_id))
            })
            .filter(|&(_, internal_id)| {
                self.internal_to_version.get(internal_id as usize) == Some(&DELETED_POINT_VERSION)
            })
            .collect()
    }

    /// Fold the moved-in records whose local slot has settled into the per-source index.
    pub(super) fn settle_point_moves(&mut self) {
        let Some(mut moves) = self.moves.take() else {
            return;
        };
        moves
            .state
            .settle(|internal_id| self.is_settled(internal_id));
        self.moves = Some(moves);
    }

    /// The slots to delete now, per the shard's `settled` gate and `masked`, the slots of this
    /// segment other segments' settled moved-in records name.
    pub fn resolve_point_moves(
        &self,
        settled: &impl Fn(SlotRef) -> bool,
        masked: Option<&RoaringBitmap>,
    ) -> MoveResolution {
        let Some(moves) = self.moves.as_ref() else {
            return MoveResolution::default();
        };
        let mut resolution = MoveResolution::default();

        let mut held_slots = AHashSet::new();
        for held in &moves.held {
            held_slots.extend(held.slots());
            let key = held.key();
            match held.hold {
                Hold::Plain => resolution.plain.push(key),
                Hold::Move | Hold::Unclassified => {
                    if held
                        .slots()
                        .any(|slot| moves.state.is_superseded(slot, settled, masked))
                    {
                        resolution.superseded.push(key);
                    }
                }
            }
        }

        // A moved-out record without its delete in view: the point's new copy is visible, so the
        // old one goes before the delete arrives, or even if it never does
        for internal_id in moves.state.moved_out_slots() {
            if !held_slots.contains(&internal_id)
                && self.may_hold_point(internal_id)
                && moves.state.is_superseded(internal_id, settled, None)
            {
                resolution.superseded.push(internal_id);
            }
        }

        if let Some(masked) = masked {
            resolution
                .superseded
                .extend(masked.iter().filter(|&internal_id| {
                    !held_slots.contains(&internal_id) && self.may_hold_point(internal_id)
                }));
        }

        resolution.normalize();
        resolution
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

    /// Delete what `resolution` names: the held deletes it keys, and the other slots as masks.
    /// Returns the committed slots that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        let Some(mut moves) = self.moves.take() else {
            return LiveReloadResult::default();
        };

        let resolved: AHashSet<PointOffsetType> = resolution
            .plain
            .iter()
            .chain(&resolution.superseded)
            .copied()
            .collect();
        let (apply, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut moves.held)
            .into_iter()
            .partition(|held| resolved.contains(&held.key()));
        moves.held = keep;

        let mut deleted = Vec::new();
        let mut keys = AHashSet::new();
        for held in apply {
            keys.insert(held.key());
            for internal_id in held.slots() {
                self.retire_slot(held.external_id, internal_id, &mut deleted);
            }
        }

        // Masks: slots a settled move supersedes that no held delete retires
        for &internal_id in resolution
            .superseded
            .iter()
            .filter(|internal_id| !keys.contains(internal_id))
        {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            } else if let Some(external_id) = self.staged_external_id(internal_id) {
                // Reported to the components already, so reported deleted
                self.staged_inserts.remove(&external_id);
                deleted.push(internal_id);
            } else {
                moves.premasked.insert(internal_id);
            }
        }

        let slots: Vec<PointOffsetType> = resolved.into_iter().collect();
        moves.state.forget(slots);
        self.moves = Some(moves);

        deleted.sort_unstable();
        deleted.dedup();
        LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        }
    }

    /// Retire `internal_id` for a held delete of `external_id`: drop it while the point still holds
    /// it, or cancel it while it is still one of the point's staged or unversioned inserts.
    fn retire_slot(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
        deleted: &mut Vec<PointOffsetType>,
    ) {
        if self.mappings.external_id(internal_id) == Some(external_id) {
            self.mappings.drop(external_id);
            deleted.push(internal_id);
        } else if self.staged_inserts.get(&external_id) == Some(&internal_id) {
            // Reported to the components already, so reported deleted
            self.staged_inserts.remove(&external_id);
            deleted.push(internal_id);
        } else if let Some(pending) = self.unversioned_inserts.get_mut(&external_id) {
            pending.retain(|pending_id| *pending_id != internal_id);
            if pending.is_empty() {
                self.unversioned_inserts.remove(&external_id);
            }
        }
    }
}
