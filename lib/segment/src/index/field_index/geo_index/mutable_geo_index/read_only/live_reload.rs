use common::ambient::hw::HwMetric;
use common::generic_consts::Sequential;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, PreloadFuture, UniversalRead, UniversalReadFs};

use super::ReadOnlyAppendableGeoIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::LiveReload;
use crate::types::{GeoPoint, RawGeoPoint};

impl<S: UniversalRead> LiveReload for ReadOnlyAppendableGeoIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<PreloadFuture>> {
        Ok(self.storage.live_preload(fs)?)
    }

    fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        for deleted_point in deleted_points {
            self.in_memory_index.remove_point(*deleted_point)?;
        }

        Ok(())
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        self.storage.live_reload(fs)?;
        self.apply_deletions(deleted_points)?;

        let in_memory_index = &mut self.in_memory_index;

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
                    // A retried reload replays offsets an earlier, failed attempt
                    // already indexed: drop them first so the replay is idempotent.
                    in_memory_index.remove_point(point_offset)?;
                    in_memory_index.add_many_geo_points(point_offset, geo_points)?;
                    Ok(true)
                },
                Some(HwMetric::PayloadIndexIoRead),
            )?;

        Ok(())
    }
}
