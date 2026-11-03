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
use crate::types::SeqNumberType;

impl<S: UniversalRead> ReadOnlyDiskIdTracker<S> {
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
        fs.reschedule_open(
            &deleted_path(&self.path),
            Some(Self::deleted_open_options()),
            None,
        );
        Ok(preloads)
    }

    /// Re-read the on-disk deleted bitslice and report points deleted since the
    /// last read. Mappings are immutable, so nothing is ever inserted.
    ///
    /// A *fresh* handle is opened: the deleted file is mutated in place, which a
    /// `reopen()` (append-only-growth contract) never picks up on caching
    /// backends. A fresh open is guaranteed to mirror the current remote bytes.
    ///
    /// A tracker that gates deletes reads the delete log instead, once it holds any entry: deletes
    /// at or below the gate apply, newer ones are held until
    /// [`release_held_deletes`](Self::release_held_deletes).
    pub fn live_reload(
        &mut self,
        fs: &impl UniversalReadFs<File = S>,
    ) -> OperationResult<LiveReloadResult> {
        if let Some(gated) = self.gated.as_mut()
            && let Some(slots) = gated.read_new(fs)?
        {
            return Ok(LiveReloadResult {
                inserted: Vec::new(),
                deleted: self.delete_logged(slots),
            });
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
        self.delete_logged(slots)
    }
}
