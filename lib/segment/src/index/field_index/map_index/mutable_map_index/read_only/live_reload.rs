use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;
use gridstore::error::GridstoreError;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::index::field_index::map_index::MapIndexKey;
use crate::index::field_index::map_index::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> LiveReload for ReadOnlyAppendableMapIndex<N, S>
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
        self.storage.live_reload(fs)?;

        let in_memory_index = &mut self.in_memory_index;

        for deleted_point in deleted_points {
            in_memory_index.remove_point(*deleted_point);
        }

        self.storage
            .view()
            .read_values::<Sequential, _, GridstoreError>(
                new_points.iter().map(|&id| ((), id)),
                |_, point_offset, maybe_values: Option<Vec<_>>| {
                    let values = maybe_values.unwrap_or_default();
                    in_memory_index.add_many_to_map(point_offset, values);
                    Ok(true)
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
