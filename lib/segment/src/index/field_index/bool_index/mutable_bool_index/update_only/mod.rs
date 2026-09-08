use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalReadFs, UniversalWriteFileOps};
use serde_json::Value;

use super::super::MutableBoolIndex;
use super::{FALSES_DIRNAME, TRUES_DIRNAME};
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::ValueIndexer;

/// Writes what [`MutableBoolIndex`] persists: the two masks saying, per point,
/// whether its field holds a true and whether it holds a false. A point whose
/// field holds both is set in both.
pub struct UpdateOnlyBoolIndex {
    trues: UpdateOnlyStoredFlags,
    falses: UpdateOnlyStoredFlags,
}

impl UpdateOnlyBoolIndex {
    /// Open the index at `dir` for writing, reading both masks into memory.
    pub fn open<Fs: UniversalReadFs + UniversalWriteFileOps>(
        fs: &Fs,
        dir: &Path,
    ) -> OperationResult<Self> {
        Ok(Self {
            trues: UpdateOnlyStoredFlags::open(fs, &dir.join(TRUES_DIRNAME))?,
            falses: UpdateOnlyStoredFlags::open(fs, &dir.join(FALSES_DIRNAME))?,
        })
    }

    /// Record `values`, the point's values for this index's field, at the slot
    /// the ID tracker claimed for it. A point whose field holds no boolean is
    /// set in neither mask.
    pub fn add_point(&mut self, slot: PointOffsetType, values: &[&Value]) -> OperationResult<()> {
        let values = <MutableBoolIndex as ValueIndexer>::flatten_values(values);

        self.trues.set(slot, values.contains(&true));
        self.falses.set(slot, values.contains(&false));

        Ok(())
    }

    /// Persist both masks.
    pub fn flush<Fs: UniversalWriteFileOps>(
        &mut self,
        fs: &Fs,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.trues.flush(fs, hw_counter)?;
        self.falses.flush(fs, hw_counter)
    }
}
