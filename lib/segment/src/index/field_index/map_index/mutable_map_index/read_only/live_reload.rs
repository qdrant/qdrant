use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;
use gridstore::error::GridstoreError;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::map_index::MapIndexKey;
use crate::index::field_index::map_index::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> ReadOnlyAppendableMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn live_reload(
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
            .for_each_in_batch::<Random, _, GridstoreError>(
                new_points,
                |idx, maybe_values: Option<Vec<_>>| {
                    let Some(values) = maybe_values else {
                        return Ok(());
                    };
                    let point_offset = new_points[idx];
                    for value in values {
                        in_memory_storage.ingest(point_offset, value);
                    }
                    Ok(())
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
