//! Live-reload: pick up deletions written by the leader after open.

use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, Populate, PreloadFuture, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in, and the
    /// next read of the delete log.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        let preloads = self.delete_log.live_preload(fs)?;
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        );
        Ok(preloads)
    }

    /// Report points deleted since the last read, in the delete log or in the
    /// on-disk deleted bitslice. Mappings are immutable, so nothing is ever
    /// inserted.
    ///
    /// A *fresh* bitslice handle is opened: the deleted file is mutated in
    /// place, which a `reopen()` (append-only-growth contract) never picks up
    /// on caching backends. A fresh open is guaranteed to mirror the current
    /// remote bytes.
    ///
    /// The log's deletes at or below the gate apply, newer ones are held until
    /// [`release_held_deletes`](Self::release_held_deletes).
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        // Every fallible read comes before any state changes, and the log, whose read advances
        // its cursor, comes last: a failed reload consumes nothing, and the next one reads the
        // same deletes again.
        let mut slots: Vec<PointOffsetType> = match StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(Populate::No),
            Default::default(),
        )
        .ok_unchanged()?
        {
            Some(fresh) => fresh
                .read_all()?
                .iter_ones()
                .map(|slot| slot as PointOffsetType)
                .collect(),
            None => Vec::new(),
        };

        slots.extend(self.delete_log.read_new(fs)?);

        slots.sort_unstable();
        slots.dedup();
        let deleted = self.delete_slots(slots);
        debug_assert!(deleted.is_sorted());

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
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
}
