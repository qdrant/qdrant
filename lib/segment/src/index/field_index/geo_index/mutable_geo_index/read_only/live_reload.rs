use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyAppendableGeoMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::LiveReload;
use crate::types::RawGeoPoint;

impl<S: UniversalRead> LiveReload for ReadOnlyAppendableGeoMapIndex<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &[PointOffsetType],
        new_points: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;

        let in_memory_index = &mut self.in_memory_index;

        for deleted_point in deleted_points {
            in_memory_index.remove_point(*deleted_point)?;
        }

        self.storage
            .view()
            .for_each_in_batch::<Random, _, OperationError>(
                new_points,
                |idx, maybe_values: Option<Vec<RawGeoPoint>>| {
                    let Some(values) = maybe_values else {
                        return Ok(());
                    };
                    let point_offset = new_points[idx];
                    in_memory_index.ingest(point_offset, values)?;
                    Ok(())
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
