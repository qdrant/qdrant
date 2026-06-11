use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyImmutableIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::mutable_id_tracker::read_only::LiveReloadResult;

impl<S: UniversalRead> ReadOnlyImmutableIdTracker<S> {
    /// Re-read the on-disk `deleted` bitslice and apply points deleted since the last reload.
    ///
    /// An immutable tracker only ever loses points (no inserts), so `inserted` is always empty.
    /// Result offsets are sorted ascending.
    pub fn live_reload(&mut self) -> OperationResult<LiveReloadResult> {
        self.deleted.reopen()?;

        let newly_deleted: Vec<PointOffsetType> = {
            let deleted = self.deleted.read_all()?;
            deleted
                .iter_ones()
                .map(|internal_id| internal_id as PointOffsetType)
                .filter(|&internal_id| !self.mappings.is_deleted_point(internal_id))
                .collect()
        };

        let mut deleted = Vec::with_capacity(newly_deleted.len());
        for internal_id in newly_deleted {
            if let Some(external_id) = self.mappings.external_id(internal_id) {
                self.mappings.drop(external_id);
                deleted.push(internal_id);
            }
        }
        deleted.sort_unstable();

        Ok(LiveReloadResult {
            inserted: Vec::new(),
            deleted,
        })
    }
}
