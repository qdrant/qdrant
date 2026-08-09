use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use serde_json::Value;

use super::lifecycle::classify_payload;
use super::{HAS_VALUES_DIRNAME, IS_NULL_DIRNAME};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::OperationResult;

/// Writes what [`MutableNullIndex`] persists: the two masks saying, per point,
/// whether its field holds any value and whether any of them is null.
///
/// Both go through [`classify_payload`], so this and the mutable index cannot
/// disagree about what a payload means.
///
/// [`MutableNullIndex`]: super::MutableNullIndex
pub struct UpdateOnlyNullIndex<S: UniversalAppend + 'static> {
    has_values: UpdateOnlyStoredFlags<S>,
    is_null: UpdateOnlyStoredFlags<S>,
}

impl<S: UniversalAppend + 'static> UpdateOnlyNullIndex<S> {
    /// Open the index at `dir` for writing, reading both masks into memory.
    pub fn open(fs: S::Fs, dir: &Path) -> OperationResult<Self> {
        Ok(Self {
            has_values: UpdateOnlyStoredFlags::open(fs.clone(), &dir.join(HAS_VALUES_DIRNAME))?,
            is_null: UpdateOnlyStoredFlags::open(fs, &dir.join(IS_NULL_DIRNAME))?,
        })
    }

    /// Record `values`, the point's values for this index's field, at the slot
    /// the ID tracker claimed for it.
    ///
    /// Called for every point of the batch, including those whose field holds
    /// nothing: "this point has no value here" is exactly what this index is
    /// asked, so a point it never saw is a point it answers wrongly about.
    pub fn add_point(&mut self, slot: PointOffsetType, values: &[&Value]) -> OperationResult<()> {
        let (has_values, is_null) = classify_payload(values);

        self.has_values.set(slot, has_values);
        self.is_null.set(slot, is_null);

        Ok(())
    }

    /// Persist both masks.
    pub fn flush(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        self.has_values.flush(hw_counter)?;
        self.is_null.flush(hw_counter)
    }
}
