use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OkUnchanged, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;
use roaring::RoaringBitmap;

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::id_tracker::point_moves::{MoveResolution, SlotMoves, SlotRef, skip_retired_moved_out};

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        fs.reschedule_open(&deleted_path(&self.path), Some(Self::open_options()), None);
        match &self.moves {
            Some(moves) => moves.view.live_preload(fs),
            None => Ok(Vec::new()),
        }
    }

    /// Re-read the on-disk `deleted` bitslice and apply points deleted since the last reload.
    ///
    /// The bitslice is a fixed-size bitmap whose bits the writer flips in
    /// place, which the held handle's `reopen()` — an append-only-growth
    /// contract — never picks up on caching backends. So a *fresh* handle is
    /// opened instead (a fresh open always mirrors the current remote bytes),
    /// diffed against the tracker's current state, and swapped in.
    ///
    /// An immutable tracker only ever loses points (no inserts), so `inserted` is always empty.
    /// Result offsets are sorted ascending.
    ///
    /// When resolving point moves, newly tombstoned points are held back instead: the delta is
    /// empty, and the shard applies them later through [`Self::apply_point_moves`].
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        if let Some(moves) = &mut self.moves {
            let entries = moves.view.read_new(fs)?;
            let mappings = &self.mappings;
            moves.ingest(skip_retired_moved_out(entries, |internal_id| {
                mappings.is_deleted_point(internal_id)
            }));
        }

        let Some(fresh) = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(),
            Default::default(),
        )
        .ok_unchanged()?
        else {
            return Ok(LiveReloadResult {
                inserted: Vec::new(),
                deleted: Vec::new(),
            });
        };

        if let Some(moves) = &mut self.moves {
            let newly_deleted: Vec<PointOffsetType> = fresh
                .read_all()?
                .iter_ones()
                .map(|internal_id| internal_id as PointOffsetType)
                .filter(|&internal_id| {
                    !self.mappings.is_deleted_point(internal_id)
                        && !moves.held.contains_key(&internal_id)
                })
                .collect();
            for internal_id in newly_deleted {
                moves.hold(internal_id);
            }
            self.deleted = fresh;
            return Ok(LiveReloadResult::default());
        }

        // `mappings` already reflects every previously reported deletion, so
        // it is the diff baseline: a set bit not yet dropped there is new.
        let newly_deleted: Vec<PointOffsetType> = {
            let deleted = fresh.read_all()?;
            deleted
                .iter_ones()
                .map(|internal_id| internal_id as PointOffsetType)
                .filter(|&internal_id| !self.mappings.is_deleted_point(internal_id))
                .collect()
        };
        self.deleted = fresh;

        let mut deleted = Vec::with_capacity(newly_deleted.len());
        for internal_id in newly_deleted {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            }
        }

        debug_assert!(deleted.is_sorted());

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }
}

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    pub fn point_moves(&self) -> Option<&SlotMoves<S>> {
        self.moves.as_deref()
    }

    pub fn point_moves_mut(&mut self) -> Option<&mut SlotMoves<S>> {
        self.moves.as_deref_mut()
    }

    /// Take in a tail read of the move log from byte offset `start`, see [`SlotMoves::ingest_tail`].
    pub fn ingest_point_moves_tail(&mut self, start: u64, bytes: &[u8]) {
        if let Some(moves) = &mut self.moves {
            let mappings = &self.mappings;
            moves.ingest_tail(start, bytes, |internal_id| {
                mappings.is_deleted_point(internal_id)
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
            Some(moves) => moves.resolve(settled, masked, |internal_id| {
                !self.mappings.is_deleted_point(internal_id)
            }),
            None => MoveResolution::default(),
        }
    }

    /// Delete the slots of `resolution`, returning the ones that were live.
    pub fn apply_point_moves(&mut self, resolution: &MoveResolution) -> LiveReloadResult {
        let mut deleted = Vec::new();
        let slots: Vec<PointOffsetType> = resolution
            .plain
            .iter()
            .chain(&resolution.superseded)
            .copied()
            .collect();
        for &internal_id in &slots {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            }
        }
        if let Some(moves) = &mut self.moves {
            moves.forget(&slots);
        }
        deleted.sort_unstable();
        deleted.dedup();
        LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        }
    }
}
