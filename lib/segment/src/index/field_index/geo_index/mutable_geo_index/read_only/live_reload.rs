use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyAppendableGeoIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::LiveReload;
use crate::types::{GeoPoint, RawGeoPoint};

impl<S: UniversalRead> LiveReload for ReadOnlyAppendableGeoIndex<S> {
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
            in_memory_index.remove_point(*deleted_point)?;
        }

        self.storage
            .view()
            .read_values::<Sequential, _, OperationError>(
                new_points.iter().map(|&id| ((), id)),
                |_, point_offset, maybe_values: Option<Vec<RawGeoPoint>>| {
                    let geo_points = maybe_values
                        .unwrap_or_default()
                        .into_iter()
                        .map(GeoPoint::from)
                        .collect::<Vec<_>>();
                    in_memory_index.add_many_geo_points(point_offset, geo_points, hw_counter)?;
                    Ok(())
                },
                hw_counter.payload_index_io_read_counter(),
            )?;

        Ok(())
    }
}
