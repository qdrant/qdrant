//! Live-reload: pick up deletions written by the leader after open.

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, OkUnchanged, UniversalRead, UniversalReadFs};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in.
    pub fn live_preload(&self, fs: &impl CachedReadFs<File = S>) -> OperationResult<()> {
        // The reload reads the whole bitslice
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        )?;
        Ok(())
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
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
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
