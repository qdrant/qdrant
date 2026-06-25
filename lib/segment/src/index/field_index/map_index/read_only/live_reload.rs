use common::counter::hardware_counter::HardwareCounterCell;
use common::persisted_hashmap::Key;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::MapIndexKey;
use super::ReadOnlyMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<N: MapIndexKey + Key + ?Sized, S: UniversalRead> LiveReload for ReadOnlyMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyMapIndex::Appendable(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            ReadOnlyMapIndex::OnDisk(index) => {
                index.live_reload(fs, deleted_points, new_points, hw_counter)
            }
        }
    }
}
