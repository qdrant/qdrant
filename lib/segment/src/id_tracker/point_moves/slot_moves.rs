//! Point moves of a tracker whose tombstones name slots: the immutable and disk-resident formats.

use std::path::Path;

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};
use roaring::RoaringBitmap;

use super::{
    MoveEntry, MoveResolution, PointMoves, PointMovesMode, PointMovesView, is_deleted,
    moved_out_slots, skip_deleted_moved_out,
};
use crate::common::operation_error::OperationResult;

/// The move log of a segment with slot tombstones (a deleted-points mask), and what the view
/// deleted for the moves read from it.
#[derive(Debug)]
pub struct SlotMoves<S: UniversalRead> {
    pub view: PointMovesView<S>,
    /// The records kept to resolve moves with, with [`PointMovesMode::Resolve`]. `None` with
    /// [`PointMovesMode::Apply`], where a moved-out record deletes its slot once read.
    pub state: Option<PointMoves>,
    /// Slots the view deleted for a move rather than for the mask: the mask as stored is the
    /// view's deleted state without them. A slot leaves once the mask marks it too.
    pub moved_away: RoaringBitmap,
}

impl<S: UniversalRead> SlotMoves<S> {
    /// Nothing read yet from the log of the segment at `segment_path`.
    pub fn new(segment_path: &Path, mode: PointMovesMode) -> Self {
        Self {
            view: PointMovesView::new(segment_path),
            state: match mode {
                PointMovesMode::Apply => None,
                PointMovesMode::Resolve => Some(PointMoves::default()),
            },
            moved_away: RoaringBitmap::new(),
        }
    }

    /// Read the entries appended to the log since the last read, see [`Self::ingest`].
    pub fn read_new(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
        deleted: &BitSlice,
    ) -> OperationResult<Vec<PointOffsetType>> {
        let entries = self.view.read_new(fs)?;
        Ok(self.ingest(entries, deleted))
    }

    /// Take in entries read from the log, without the records of slots deleted in `deleted`, the
    /// view's deleted state. Returns the slots to delete now: those of moved-out records when
    /// applying moves, none when resolving. Every slot of these formats has settled, since nothing
    /// is appended to them, so moved-in records settle at once.
    pub fn ingest(&mut self, entries: Vec<MoveEntry>, deleted: &BitSlice) -> Vec<PointOffsetType> {
        let entries = skip_deleted_moved_out(entries, deleted);
        match &mut self.state {
            Some(state) => {
                state.ingest(entries);
                state.settle_all();
                Vec::new()
            }
            None => moved_out_slots(&entries),
        }
    }

    /// Whether a moved-out record names a copy the view still shows: its move may not have
    /// settled, so it may be the only visible copy of its point.
    pub fn holds_moves(&self) -> bool {
        self.state.as_ref().is_some_and(PointMoves::has_moved_out)
    }

    /// Whether a moved-out record names a copy the view still shows once `resolution` is applied.
    pub fn holds_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        self.state.as_ref().is_some_and(|state| {
            state
                .moved_out()
                .any(|(slot, _)| resolution.superseded.binary_search(&slot).is_err())
        })
    }

    /// The slots to delete now: those of `superseded` still live in `deleted`, the view's deleted
    /// state. `superseded` are the slots of this segment whose copy a settled move replaced, as the
    /// shard found them: by this segment's moved-out records, or by other segments' moved-in
    /// records.
    pub fn resolve(
        &self,
        superseded: Option<&RoaringBitmap>,
        deleted: &BitSlice,
    ) -> MoveResolution {
        let mut resolution = MoveResolution::default();
        if let Some(superseded) = superseded {
            resolution
                .superseded
                .extend(superseded.iter().filter(|&slot| !is_deleted(deleted, slot)));
        }
        resolution.normalize();
        resolution
    }

    /// Note that the view deleted `slots`, live until now, for a move.
    pub fn moved_away(&mut self, slots: &[PointOffsetType]) {
        self.moved_away.extend(slots.iter().copied());
        self.forget(slots);
    }

    /// Take in a fresh read of the mask: a slot it marks is no longer deleted for a move only.
    pub fn mask_read(&mut self, mask: &BitSlice) {
        let marked: Vec<PointOffsetType> = self
            .moved_away
            .iter()
            .filter(|&slot| mask.get(slot as usize).is_some_and(|bit| *bit))
            .collect();
        for slot in marked {
            self.moved_away.remove(slot);
        }
    }

    /// The mask as stored, given the view's deleted state `deleted`.
    pub fn stored_mask(&self, deleted: &BitSlice) -> BitVec {
        let mut mask = deleted.to_bitvec();
        for slot in &self.moved_away {
            if let Some(mut bit) = mask.get_mut(slot as usize) {
                *bit = false;
            }
        }
        mask
    }

    /// Forget the records of `slots`, which are deleted in the view now.
    pub fn forget(&mut self, slots: &[PointOffsetType]) {
        if let Some(state) = &mut self.state {
            state.forget(slots.iter().copied());
        }
    }
}
