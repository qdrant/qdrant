use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::DeferredBehavior;
use segment::common::operation_error::OperationResult;
use segment::types::{ExtendedPointId, WithPayload, WithPayloadInterface, WithVector};
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::retrieve_over;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn retrieve(
        &self,
        point_ids: &[ExtendedPointId],
        with_payload: Option<WithPayloadInterface>,
        with_vector: Option<WithVector>,
    ) -> OperationResult<Vec<RecordInternal>> {
        let with_payload =
            WithPayload::from(with_payload.unwrap_or(WithPayloadInterface::Bool(true)));
        let with_vector = with_vector.unwrap_or(WithVector::Bool(false));

        let mut points = retrieve_over(
            self.segment_arcs(),
            point_ids,
            &with_payload,
            &with_vector,
            &AtomicBool::new(false),
            HwMeasurementAcc::disposable_edge(),
            DeferredBehavior::VisibleOnly,
        )?;

        let points: Vec<_> = point_ids
            .iter()
            .filter_map(|id| points.remove(id))
            .collect();

        Ok(points)
    }
}
