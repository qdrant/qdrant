use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::OperationResult;
use segment::types::{ExtendedPointId, WithPayload, WithPayloadInterface, WithVector};
use shard::retrieve::record_internal::RecordInternal;
use shard::retrieve::retrieve_blocking::retrieve_blocking;

use crate::Shard;

impl Shard {
    pub fn retrieve(
        &self,
        ids: &[ExtendedPointId],
        with_payload: Option<WithPayloadInterface>,
        with_vector: Option<WithVector>,
    ) -> OperationResult<Vec<RecordInternal>> {
        let with_payload =
            WithPayload::from(with_payload.unwrap_or(WithPayloadInterface::Bool(true)));
        let with_vector = with_vector.unwrap_or(WithVector::Bool(false));

        let never_stopped = std::sync::atomic::AtomicBool::new(false);

        let hw_measurement = HwMeasurementAcc::disposable();

        let mut retrieve_result = retrieve_blocking(
            self.segments.clone(),
            ids,
            &with_payload,
            &with_vector,
            &never_stopped,
            hw_measurement,
        )?;

        let response: Vec<_> = ids
            .iter()
            .filter_map(|idx| retrieve_result.remove(idx))
            .collect();

        Ok(response)
    }
}
