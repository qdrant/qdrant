//! Live-reload: pick up deletions written by the leader after open.

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, Populate, PreloadFuture, UniversalRead, UniversalReadFs,
};
use roaring::RoaringBitmap;

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::point_moves::{MoveEntry, MoveResolution, PointMoves};

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in, and
    /// the new part of the move log.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        );
        self.moves.view.live_preload(fs)
    }

    /// Re-read the on-disk deleted bitslice and report points deleted since the
    /// last reload. Mappings are immutable, so nothing is ever inserted.
    ///
    /// A *fresh* handle is opened rather than reusing the held one: the deleted
    /// file is mutated in place, which a `reopen()` (append-only-growth
    /// contract) never picks up on caching backends. A fresh open is guaranteed
    /// to mirror the current remote bytes; per-point lookups read the fresh
    /// state from then on too.
    ///
    /// `deleted_full` doubles as the diff baseline; when it was never
    /// materialized, every currently-deleted offset is reported (an idempotent
    /// replay downstream).
    ///
    /// The move log is read too: applying moves, the copies moved out are deleted as well.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        let fresh = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(Populate::No),
            Default::default(),
        )
        .ok_unchanged()?;
        let new: Option<BitVec> = match &fresh {
            Some(fresh) => Some(fresh.read_all()?.into_owned()),
            // Moves read below are taken in against the full set
            None => {
                self.deleted_full()?;
                None
            }
        };
        // Read before anything applies: should it fail, the next reload reads the same mask again
        let entries = self.moves.view.read_new(fs)?;

        let mut deleted = Vec::new();
        if let (Some(fresh), Some(mut new)) = (fresh, new) {
            self.deleted_file = fresh;
            self.moves.mask_read(&new);
            self.moves.mark_moved_away(&mut new);

            let baseline = self.deleted_full.take();
            deleted = match baseline {
                Some(old) => new
                    .iter_ones()
                    .filter(|&i| !old.get(i).is_some_and(|b| *b))
                    .map(|i| i as PointOffsetType)
                    .collect(),
                None => new.iter_ones().map(|i| i as PointOffsetType).collect(),
            };
            debug_assert!(deleted.is_sorted());
            self.moves.forget(&deleted);

            // `take` above emptied the cell, so this refreshes it to the new state
            // and serves both the next search view and the next reload baseline.
            let _ = self.deleted_full.set(new);
        }

        deleted.extend(self.take_point_moves(entries));
        deleted.sort_unstable();

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }

    /// Take in entries read from the move log, returning the slots deleted for a move: those of
    /// moved-out records, when applying moves. Needs the full deleted set materialized, unless
    /// `entries` is empty.
    pub(super) fn take_point_moves(&mut self, entries: Vec<MoveEntry>) -> Vec<PointOffsetType> {
        let points = self.reader.total_point_count() as usize;
        let Some(deleted_full) = self.deleted_full.get_mut() else {
            debug_assert!(entries.is_empty(), "point moves need the full deleted set");
            return Vec::new();
        };
        // The mask may be padded past the last point
        let points = points.min(deleted_full.len());
        let moved = self.moves.ingest(entries, &deleted_full[..points]);
        for &internal_id in &moved {
            deleted_full.set(internal_id as usize, true);
        }
        self.moves.moved_away(&moved);
        moved
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
    /// [`SlotMoves::resolve`](crate::id_tracker::point_moves::SlotMoves::resolve). Resolving
    /// moves, the full deleted set is always materialized.
    pub fn resolve_point_moves(&self, superseded: Option<&RoaringBitmap>) -> MoveResolution {
        match self.deleted_full.get() {
            Some(deleted_full) => {
                let points = deleted_full
                    .len()
                    .min(self.reader.total_point_count() as usize);
                self.moves.resolve(superseded, &deleted_full[..points])
            }
            None => MoveResolution::default(),
        }
    }

    /// Delete the slots of `resolution`, returning the ones that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        let Some(deleted_full) = self.deleted_full.get_mut() else {
            return LiveReloadResult::default();
        };
        let mut deleted = Vec::new();
        for &internal_id in &resolution.superseded {
            if let Some(mut bit) = deleted_full.get_mut(internal_id as usize)
                && !*bit
            {
                *bit = true;
                deleted.push(internal_id);
            }
        }
        self.moves.moved_away(&deleted);
        LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        }
    }
}
