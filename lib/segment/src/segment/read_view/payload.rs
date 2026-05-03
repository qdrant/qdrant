use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::query_estimator::adjust_for_deferred_points;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Filter, Payload, PointIdType};

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Retrieve payload by internal ID.
    #[inline]
    pub fn payload_by_offset(
        &self,
        point_offset: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.payload_index.get_payload(point_offset, hw_counter)
    }

    /// Retrieve payload by external point ID.
    pub fn payload(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_by_offset(internal_id, hw_counter)
    }

    /// Estimate the number of available points matching the filter.
    pub fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        Ok(match filter {
            None => {
                let available = self.available_point_count_without_deferred();
                CardinalityEstimation {
                    primary_clauses: vec![],
                    min: available,
                    exp: available,
                    max: available,
                }
            }
            Some(filter) => {
                let cardinality = self
                    .payload_index
                    .estimate_cardinality(filter, hw_counter)?;

                let total_points = self.id_tracker.available_point_count();
                let available_points = self.available_point_count_without_deferred();
                adjust_for_deferred_points(cardinality, available_points, total_points)
            }
        })
    }
}
