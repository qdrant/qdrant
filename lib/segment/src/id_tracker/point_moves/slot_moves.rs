//! Point moves of a tracker whose tombstones name slots: the immutable and disk-resident formats.

use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};
use roaring::RoaringBitmap;

use super::{
    Hold, MoveEntry, MoveResolution, PointMoves, PointMovesView, SlotRef, read_point_moves_tail,
    skip_retired_moved_out,
};
use crate::common::operation_error::OperationResult;

/// The move log of a segment with slot tombstones (a deleted-points mask), the records read from
/// it, and the tombstones held back.
#[derive(Debug)]
pub struct SlotMoves<S: UniversalRead> {
    pub view: PointMovesView<S>,
    pub state: PointMoves,
    /// Tombstones read from the mask and not applied yet.
    pub held: AHashMap<PointOffsetType, Hold>,
}

impl<S: UniversalRead> SlotMoves<S> {
    /// Read the whole log through a fresh handle from `raw_fs`. Called when opening a tracker, after
    /// its mask was read: the log read is newer than the mask read, so every tombstone of a move
    /// finds its record here, and the tombstones no record names are plain deletes.
    pub fn open(
        raw_fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<Self> {
        let mut view = PointMovesView::new(segment_path);
        let bytes = read_point_moves_tail(raw_fs, view.path(), 0)?;
        let entries = view.consume(0, &bytes);
        let mut moves = Self {
            view,
            state: PointMoves::default(),
            held: AHashMap::new(),
        };
        moves.ingest(entries);
        Ok(moves)
    }

    /// Take in entries read from the log. Every slot of these formats has settled, since nothing is
    /// appended to them, so moved-in records settle at once.
    pub fn ingest(&mut self, entries: impl IntoIterator<Item = MoveEntry>) {
        self.state.ingest(entries);
        self.state.settle(|_| true);
    }

    /// Hold back a tombstone read from the mask: as a move if a record names its slot, unclassified
    /// otherwise, since the log may have been read before the mask.
    pub fn hold(&mut self, slot: PointOffsetType) {
        let hold = if self.state.names(slot) {
            Hold::Move
        } else {
            Hold::Unclassified
        };
        self.held.insert(slot, hold);
    }

    /// Where a tail read of the log has to start, if some tombstone waits for one to be
    /// classified.
    pub fn tail_to_read(&self) -> Option<(PathBuf, u64)> {
        self.held
            .values()
            .any(|&hold| hold == Hold::Unclassified)
            .then(|| (self.view.path().to_path_buf(), self.view.read_to()))
    }

    /// Take in a tail read of the log from byte offset `start`, and classify the tombstones that
    /// waited for it: a move if a record names the slot now, a plain delete otherwise. Moved-out
    /// records of slots `retired` reports deleted for good are skipped.
    pub fn ingest_tail(
        &mut self,
        start: u64,
        bytes: &[u8],
        retired: impl Fn(PointOffsetType) -> bool,
    ) {
        if start != self.view.read_to() {
            return;
        }
        let entries = self.view.consume(start, bytes);
        self.ingest(skip_retired_moved_out(entries, retired));
        for (&slot, hold) in &mut self.held {
            if *hold == Hold::Unclassified {
                *hold = if self.state.names(slot) {
                    Hold::Move
                } else {
                    Hold::Plain
                };
            }
        }
    }

    /// Whether some tombstone is still held back for a move that did not settle, or for a tail read
    /// that did not happen.
    pub fn holds_moves(&self) -> bool {
        self.held.values().any(|&hold| hold != Hold::Plain)
    }

    /// Whether some tombstone stays held back for a move once `resolution` is applied.
    pub fn holds_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        self.held.iter().any(|(slot, &hold)| {
            hold != Hold::Plain && resolution.superseded.binary_search(slot).is_err()
        })
    }

    /// The slots to delete now, per the shard's `settled` gate and `masked`, the slots of this
    /// segment other segments' settled moved-in records name. `is_live` tells whether a slot is
    /// live in the view and not held.
    pub fn resolve(
        &self,
        settled: &impl Fn(SlotRef) -> bool,
        masked: Option<&RoaringBitmap>,
        is_live: impl Fn(PointOffsetType) -> bool,
    ) -> MoveResolution {
        let mut resolution = MoveResolution::default();

        for (&slot, &hold) in &self.held {
            match hold {
                Hold::Plain => resolution.plain.push(slot),
                Hold::Move | Hold::Unclassified => {
                    if self.state.is_superseded(slot, settled, masked) {
                        resolution.superseded.push(slot);
                    }
                }
            }
        }

        // A moved-out record without its tombstone in view: the point's new copy is visible, so the
        // old one goes before the tombstone arrives, or even if it never does
        for slot in self.state.moved_out_slots() {
            if !self.held.contains_key(&slot)
                && is_live(slot)
                && self.state.is_superseded(slot, settled, None)
            {
                resolution.superseded.push(slot);
            }
        }

        if let Some(masked) = masked {
            resolution.superseded.extend(
                masked
                    .iter()
                    .filter(|slot| !self.held.contains_key(slot) && is_live(*slot)),
            );
        }

        resolution.normalize();
        resolution
    }

    /// Forget the holds and records of `slots`, which are deleted in the view now.
    pub fn forget(&mut self, slots: &[PointOffsetType]) {
        for slot in slots {
            self.held.remove(slot);
        }
        self.state.forget(slots.iter().copied());
    }
}
