use blobstore::Blob;
use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::{Encodable, ReadOnlyNumericIndex};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

impl<
    T: Encodable + Numericable + StoredValue + Send + Sync + Default + 'static,
    P,
    S: UniversalRead,
> LiveReload for ReadOnlyNumericIndex<T, P, S>
where
    Vec<T>: Blob,
{
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.inner
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}
