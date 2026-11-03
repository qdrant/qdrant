use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, PreloadFuture, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in, and the
    /// next read of the delete log when deletes are gated.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        let mut preloads = Vec::new();
        if let Some(gated) = &self.gated {
            preloads.extend(gated.live_preload(fs)?);
            if gated.is_started() {
                return Ok(preloads);
            }
        }
        fs.reschedule_open(&deleted_path(&self.path), Some(Self::open_options()), None);
        Ok(preloads)
    }

    /// Re-read the on-disk `deleted` bitslice and apply points deleted since the last reload.
    ///
    /// The bitslice is a fixed-size bitmap whose bits the writer flips in
    /// place, which the held handle's `reopen()` — an append-only-growth
    /// contract — never picks up on caching backends. So a *fresh* handle is
    /// opened instead (a fresh open always mirrors the current remote bytes),
    /// diffed against the tracker's current state, and swapped in.
    ///
    /// A tracker that gates deletes reads the delete log instead, once it holds any entry: deletes
    /// at or below the gate apply, newer ones are held until
    /// [`release_held_deletes`](Self::release_held_deletes).
    ///
    /// An immutable tracker only ever loses points (no inserts), so `inserted` is always empty.
    /// Result offsets are sorted ascending.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        if let Some(gated) = self.gated.as_mut()
            && let Some(slots) = gated.read_new(fs)?
        {
            return Ok(LiveReloadResult {
                inserted: Vec::new(),
                deleted: self.delete_slots(slots),
            });
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

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted: self.delete_slots(newly_deleted),
        })
    }

    /// Whether [`release_held_deletes`](Self::release_held_deletes) with this gate would release
    /// anything.
    pub fn releases_held_deletes(&self, gate: SeqNumberType) -> bool {
        self.gated
            .as_ref()
            .is_some_and(|gated| gated.releases_any(gate))
    }

    /// Raise the gate to `gate` and apply the held deletes it now covers. Returns the offsets they
    /// removed, sorted ascending.
    pub fn release_held_deletes(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        let Some(gated) = self.gated.as_mut() else {
            return Vec::new();
        };
        let slots = gated.release(gate);
        self.delete_slots(slots)
    }

    /// Drop the points at `slots`, ascending, returning the ones that were live.
    fn delete_slots(&mut self, slots: Vec<PointOffsetType>) -> Vec<PointOffsetType> {
        let mut deleted = Vec::with_capacity(slots.len());
        for internal_id in slots {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            }
        }
        debug_assert!(deleted.is_sorted());
        deleted
    }
}
