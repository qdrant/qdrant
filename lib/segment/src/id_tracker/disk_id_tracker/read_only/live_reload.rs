//! Live-reload: pick up deletions written by the leader after open.

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Re-read the on-disk deleted bitslice and report points deleted since the
    /// last reload. Mappings are immutable, so nothing is ever inserted.
    ///
    /// The deleted file is a fixed-size bitmap whose bits the writer flips in
    /// place, which the held handle's `reopen()` — an append-only-growth
    /// contract — never picks up on caching backends. So a *fresh* handle is
    /// opened instead (a fresh open always mirrors the current remote bytes)
    /// and swapped in; the per-point `get_bit` lookups read fresh state from
    /// then on too.
    ///
    /// The full deleted set (`deleted_full`) doubles as the diff baseline: if it
    /// was materialized (by a prior search/scroll/count/reload) we diff against
    /// it; otherwise this is the first baseline and every currently-deleted
    /// offset is reported (an idempotent replay downstream).
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        let fresh = StoredBitSlice::<S>::open(
            fs,
            deleted_path(&self.path),
            Self::open_options(),
            Default::default(),
        )?;
        let new: BitVec = fresh.read_all()?.into_owned();
        self.deleted_file = fresh;

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
