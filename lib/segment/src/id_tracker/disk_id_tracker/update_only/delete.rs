//! The one mutation this type offers: marking internal offsets deleted.

use common::types::PointOffsetType;
use common::universal_io::{UniversalAppend, UniversalWriteFileOps as _};

use super::UpdateOnlyDiskIdTracker;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::immutable_id_tracker::deleted_path;

impl<S: UniversalAppend> UpdateOnlyDiskIdTracker<S> {
    /// Mark every offset in `internal_ids` deleted and persist the whole
    /// `deleted` file via `atomic_save`. Does not touch the `versions` file.
    /// Offsets already deleted are handled without error.
    pub fn delete_batch(&mut self, internal_ids: &[PointOffsetType]) -> OperationResult<()> {
        if internal_ids.is_empty() {
            return Ok(());
        }

        for &internal_id in internal_ids {
            self.deleted.set(internal_id as usize, true);
        }

        self.fs.atomic_save(
            &deleted_path(&self.path),
            bytemuck::cast_slice(self.deleted.as_raw_slice()),
        )?;

        Ok(())
    }
}
