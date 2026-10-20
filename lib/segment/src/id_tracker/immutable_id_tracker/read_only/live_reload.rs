use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, PreloadFuture, UniversalRead, UniversalReadFs,
};
use roaring::RoaringBitmap;

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::point_moves::{MoveResolution, PointMoves};

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in, and
    /// the new part of the move log.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        fs.reschedule_open(&deleted_path(&self.path), Some(Self::open_options()), None);
        self.moves.view.live_preload(fs)
    }

    /// Re-read the on-disk `deleted` bitslice and apply points deleted since the last reload.
    ///
    /// The bitslice is a fixed-size bitmap whose bits the writer flips in
    /// place, which the held handle's `reopen()` — an append-only-growth
    /// contract — never picks up on caching backends. So a *fresh* handle is
    /// opened instead (a fresh open always mirrors the current remote bytes),
    /// diffed against the tracker's current state, and swapped in.
    ///
    /// The move log is read too: applying moves, the copies moved out are deleted as well.
    ///
    /// An immutable tracker only ever loses points (no inserts), so `inserted` is always empty.
    /// Result offsets are sorted ascending.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        let fresh = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(),
            Default::default(),
        )
        .ok_unchanged()?;
        let mask = fresh.as_ref().map(StoredBitSlice::read_all).transpose()?;
        // Read before anything applies: should it fail, the next reload reads the same mask again
        let entries = self.moves.view.read_new(fs)?;

        let mut deleted = Vec::new();
        if let Some(mask) = &mask {
            // `mappings` already reflects every previously reported deletion, so
            // it is the diff baseline: a set bit not yet dropped there is new.
            let newly_deleted: Vec<PointOffsetType> = mask
                .iter_ones()
                .map(|internal_id| internal_id as PointOffsetType)
                .filter(|&internal_id| !self.mappings.is_deleted_point(internal_id))
                .collect();
            for internal_id in newly_deleted {
                if let Some(external_id) = self.mappings.external_id(internal_id) {
                    self.mappings.drop(external_id);
                    deleted.push(internal_id);
                }
            }
            self.moves.mask_read(mask);
            self.moves.forget(&deleted);
        }
        drop(mask);
        if let Some(fresh) = fresh {
            self.deleted = fresh;
        }

        let moved = self.moves.ingest(entries, self.mappings.deleted());
        deleted.extend(self.move_away(&moved));
        deleted.sort_unstable();

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }

    /// Delete `internal_ids` for a move, returning the ones that were live.
    pub(super) fn move_away(&mut self, internal_ids: &[PointOffsetType]) -> Vec<PointOffsetType> {
        let mut deleted = Vec::with_capacity(internal_ids.len());
        for &internal_id in internal_ids {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            }
        }
        self.moves.moved_away(&deleted);
        deleted
    }

    /// The records read from the move log, when this tracker resolves moves.
    pub fn point_moves(&self) -> Option<&PointMoves> {
        self.moves.state.as_ref()
    }

    pub fn point_moves_mut(&mut self) -> Option<&mut PointMoves> {
        self.moves.state.as_mut()
    }

    /// Whether a moved-out record names a copy this view still shows, see
    /// [`SlotMoves::holds_moves`](crate::id_tracker::point_moves::SlotMoves::holds_moves).
    pub fn holds_point_moves(&self) -> bool {
        self.moves.holds_moves()
    }

    /// Whether a moved-out record names a copy this view still shows once `resolution` is applied.
    pub fn holds_point_moves_beyond(&self, resolution: &MoveResolution) -> bool {
        self.moves.holds_moves_beyond(resolution)
    }

    /// The slots to delete now, see
    /// [`SlotMoves::resolve`](crate::id_tracker::point_moves::SlotMoves::resolve).
    pub fn resolve_point_moves(&self, superseded: Option<&RoaringBitmap>) -> MoveResolution {
        self.moves.resolve(superseded, self.mappings.deleted())
    }

    /// Delete the slots of `resolution`, returning the ones that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        LiveReloadResult {
            inserted: Vec::new(),
            deleted: self.move_away(&resolution.superseded),
        }
    }

    /// The deleted mask as stored: the deleted state without the slots deleted for a move. What a
    /// writer resumes from.
    pub fn stored_deleted_mask(&self) -> BitVec {
        self.moves.stored_mask(self.mappings.deleted())
    }
}
