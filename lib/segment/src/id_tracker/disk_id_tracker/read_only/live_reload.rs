//! Live-reload: pick up deletions written by the leader after open.

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OkUnchanged, Populate, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;
use roaring::RoaringBitmap;

use super::{DiskMoves, ReadOnlyDiskIdTracker};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::point_moves::{MoveResolution, SlotMoves, SlotRef, skip_retired_moved_out};

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        );
        match &self.moves {
            Some(moves) => moves.moves.view.live_preload(fs),
            None => Ok(Vec::new()),
        }
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
    /// When resolving point moves, newly tombstoned points are held back instead: the delta is
    /// empty, and the shard applies them later through [`Self::apply_point_moves`].
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        if let Some(moves) = &mut self.moves {
            let entries = moves.moves.view.read_new(fs)?;
            let effective = &moves.effective;
            let entries = skip_retired_moved_out(entries, |internal_id| {
                effective
                    .get(internal_id as usize)
                    .is_none_or(|deleted| *deleted)
            });
            moves.moves.ingest(entries);
        }

        let Some(fresh) = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(Populate::No),
            Default::default(),
        )
        .ok_unchanged()?
        else {
            return Ok(LiveReloadResult {
                inserted: Vec::new(),
                deleted: Vec::new(),
            });
        };

        let new: BitVec = fresh.read_all()?.into_owned();
        self.deleted_file = fresh;

        if let Some(moves) = &mut self.moves {
            let newly_deleted: Vec<PointOffsetType> = new
                .iter_ones()
                .filter(|&i| !moves.raw.get(i).is_some_and(|bit| *bit))
                .map(|i| i as PointOffsetType)
                .collect();
            if new.len() > moves.effective.len() {
                moves.effective.resize(new.len(), false);
            }
            for internal_id in newly_deleted {
                // A copy a settled move already masked needs no hold
                if !moves.effective[internal_id as usize] {
                    moves.moves.hold(internal_id);
                }
            }
            moves.raw = new;
            return Ok(LiveReloadResult::default());
        }

        let baseline = self.deleted_full.take();
        let deleted: Vec<PointOffsetType> = match baseline {
            Some(old) => new
                .iter_ones()
                .filter(|&i| !old.get(i).is_some_and(|b| *b))
                .map(|i| i as PointOffsetType)
                .collect(),
            None => new.iter_ones().map(|i| i as PointOffsetType).collect(),
        };
        debug_assert!(deleted.is_sorted());

        // `take` above emptied the cell, so this refreshes it to the new state
        // and serves both the next search view and the next reload baseline.
        let _ = self.deleted_full.set(new);

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }
}

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    pub fn point_moves(&self) -> Option<&SlotMoves<S>> {
        self.moves.as_deref().map(|moves| &moves.moves)
    }

    pub fn point_moves_mut(&mut self) -> Option<&mut SlotMoves<S>> {
        self.moves.as_deref_mut().map(|moves| &mut moves.moves)
    }

    /// Take in a tail read of the move log from byte offset `start`, see [`SlotMoves::ingest_tail`].
    pub fn ingest_point_moves_tail(&mut self, start: u64, bytes: &[u8]) {
        if let Some(moves) = self.moves.as_deref_mut() {
            let DiskMoves {
                moves: slot_moves,
                raw: _,
                effective,
            } = moves;
            slot_moves.ingest_tail(start, bytes, |internal_id| {
                effective
                    .get(internal_id as usize)
                    .is_none_or(|deleted| *deleted)
            });
        }
    }

    /// The slots to delete now, see [`SlotMoves::resolve`].
    pub fn resolve_point_moves(
        &self,
        settled: &impl Fn(SlotRef) -> bool,
        masked: Option<&RoaringBitmap>,
    ) -> MoveResolution {
        match &self.moves {
            Some(moves) => moves.moves.resolve(settled, masked, |internal_id| {
                moves
                    .effective
                    .get(internal_id as usize)
                    .is_some_and(|deleted| !*deleted)
            }),
            None => MoveResolution::default(),
        }
    }

    /// Delete the slots of `resolution`, returning the ones that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        let Some(moves) = &mut self.moves else {
            return LiveReloadResult::default();
        };
        let slots: Vec<PointOffsetType> = resolution
            .plain
            .iter()
            .chain(&resolution.superseded)
            .copied()
            .collect();
        let mut deleted = Vec::new();
        for &internal_id in &slots {
            if let Some(mut bit) = moves.effective.get_mut(internal_id as usize)
                && !*bit
            {
                *bit = true;
                deleted.push(internal_id);
            }
        }
        moves.moves.forget(&slots);
        deleted.sort_unstable();
        deleted.dedup();
        LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        }
    }
}
