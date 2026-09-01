use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalReadFs, UniversalWriteFileOps};
use serde_json::Value;

use super::lifecycle::classify_payload;
use super::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::OperationResult;

/// Writes what [`MutableNullIndex`] persists: the two masks saying, per point,
/// whether its field holds any value and whether any of them is null.
///
/// [`MutableNullIndex`]: super::MutableNullIndex
pub struct UpdateOnlyNullIndex {
    has_values: UpdateOnlyStoredFlags,
    is_null: UpdateOnlyStoredFlags,
}

impl UpdateOnlyNullIndex {
    /// Open the index at `dir` for writing, reading both masks into memory.
    pub fn open<Fs: UniversalReadFs + UniversalWriteFileOps>(
        fs: &Fs,
        dir: &Path,
    ) -> OperationResult<Self> {
        Ok(Self {
            has_values: UpdateOnlyStoredFlags::open(fs, &dir.join(HAS_VALUES_DIRNAME))?,
            is_null: UpdateOnlyStoredFlags::open(fs, &dir.join(IS_NULL_DIRNAME))?,
        })
    }

    /// Record `values`, the point's values for this index's field, at the slot
    /// the ID tracker claimed for it.
    ///
    /// Must be called for every point of the batch, including those whose field
    /// holds nothing: a point this index never saw is one it answers wrongly
    /// about.
    pub fn add_point(&mut self, slot: PointOffsetType, values: &[&Value]) -> OperationResult<()> {
        let (has_values, is_null) = classify_payload(values);

        self.has_values.set(slot, has_values);
        self.is_null.set(slot, is_null);

        Ok(())
    }

    /// Persist both masks.
    pub fn flush<Fs: UniversalWriteFileOps>(
        &mut self,
        fs: &Fs,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.has_values.flush(fs, hw_counter)?;
        self.is_null.flush(fs, hw_counter)
    }
}
