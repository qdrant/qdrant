//! Live-reload: pick up deletions written by the leader after open.

use common::bitvec::BitVec;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkUnchanged, Populate, PreloadFuture, UniversalRead, UniversalReadFs,
};

use super::ReadOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
    /// Stage the fresh deleted-bitslice handle [`live_reload`](Self::live_reload) swaps in.
    pub fn live_preload(
        &self,
        fs: &impl CachedReadFs<File = S>,
    ) -> OperationResult<Vec<PreloadFuture>> {
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        );
        Ok(Vec::new())
    }

    /// Re-read the on-disk deleted bitslice and report points deleted since the
    /// last read. Mappings are immutable, so nothing is ever inserted.
    ///
    /// A *fresh* handle is opened: the deleted file is mutated in place, which a
    /// `reopen()` (append-only-growth contract) never picks up on caching
    /// backends. A fresh open is guaranteed to mirror the current remote bytes.
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
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
        let deleted: Vec<PointOffsetType> = new
            .iter_ones()
            .filter(|&i| !self.deleted.get(i).is_some_and(|b| *b))
            .map(|i| i as PointOffsetType)
            .collect();
        debug_assert!(deleted.is_sorted());
        self.deleted = new;

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }
}
