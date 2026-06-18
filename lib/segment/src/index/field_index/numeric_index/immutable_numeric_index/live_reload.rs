use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::ImmutableNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::index::field_index::numeric_index::Encodable;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, S: UniversalRead> LiveReload
    for ImmutableNumericIndex<T, S>
where
    Vec<T>: Blob,
{
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        _fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for deleted_point in deleted_points {
            self.remove_point(*deleted_point);
        }

        Ok(())
    }
}
