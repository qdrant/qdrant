use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
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
        deleted_points: &[PointOffsetType],
        new_points: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;

        let in_memory_storage = &mut self.inner;

        for deleted_point in deleted_points {
            in_memory_storage.remove_point(*deleted_point);
        }

        self.storage
            .view()
            .read_values::<Random, _, GridstoreError>(
                new_points.iter().copied().enumerate(),
                |_, point_offset, maybe_values: Option<Vec<_>>| {
                    in_memory_storage.remove_point(point_offset);

                    let values = maybe_values.unwrap_or_default();
                    in_memory_storage.add_many_to_map(point_offset, values);
                    Ok(())
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
