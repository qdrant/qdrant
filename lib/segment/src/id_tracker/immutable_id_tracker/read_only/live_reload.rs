use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalReadFs};

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_storage::deleted_path;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
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
