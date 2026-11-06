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
    /// next read of the delete log.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        let preloads = self.delete_log.live_preload(fs)?;
        fs.reschedule_open(&deleted_path(&self.path), Some(Self::open_options()), None);
        Ok(preloads)
    }

    /// Apply points deleted since the last reload, in the delete log or in the on-disk `deleted`
    /// bitslice.
    ///
    /// The bitslice is a fixed-size bitmap whose bits the writer flips in
    /// place, which the held handle's `reopen()` — an append-only-growth
    /// contract — never picks up on caching backends. So a *fresh* handle is
    /// opened instead (a fresh open always mirrors the current remote bytes),
    /// diffed against the tracker's current state, and swapped in.
    ///
    /// The log's deletes at or below the gate apply, newer ones are held until
    /// [`release_held_deletes`](Self::release_held_deletes).
    ///
    /// An immutable tracker only ever loses points (no inserts), so `inserted` is always empty.
    /// Result offsets are sorted ascending.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        // Every fallible read comes before any state changes, and the log, whose read advances
        // its cursor, comes last: a failed reload consumes nothing, and the next one reads the
        // same deletes again.
        let fresh = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(),
            Default::default(),
        )
        .ok_unchanged()?;

        // `mappings` already reflects every previously reported deletion, so
        // it is the diff baseline: a set bit not yet dropped there is new.
        let mut newly_deleted: Vec<PointOffsetType> = match &fresh {
            Some(fresh) => fresh
                .read_all()?
                .iter_ones()
                .map(|internal_id| internal_id as PointOffsetType)
                .filter(|&internal_id| !self.mappings.is_deleted_point(internal_id))
                .collect(),
            None => Vec::new(),
        };

        newly_deleted.extend(self.delete_log.read_new(fs)?);

        if let Some(fresh) = fresh {
            self.deleted = fresh;
        }

        newly_deleted.sort_unstable();
        newly_deleted.dedup();
        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted: self.delete_slots(newly_deleted),
        })
    }

    /// Whether [`release_held_deletes`](Self::release_held_deletes) with this gate would release
    /// anything.
    pub fn releases_held_deletes(&self, gate: SeqNumberType) -> bool {
        self.delete_log.releases_any(gate)
    }

    /// Raise the gate to `gate` and apply the held deletes it now covers. Returns the offsets they
    /// removed, sorted ascending.
    pub fn release_held_deletes(&mut self, gate: SeqNumberType) -> Vec<PointOffsetType> {
        let slots = self.delete_log.release(gate);
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
