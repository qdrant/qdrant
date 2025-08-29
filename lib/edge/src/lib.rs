use std::fmt;
use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use parking_lot::Mutex;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::query_context::QueryContext;
use segment::types::{DEFAULT_FULL_SCAN_THRESHOLD, ScoredPoint, WithPayload, WithVector};
use shard::operations::CollectionUpdateOperations;
use shard::search::CoreSearchRequest;
use shard::segment_holder::LockedSegmentHolder;
use shard::update::*;
use shard::wal::SerdeWal;

#[derive(Debug)]
pub struct Shard {
    path: PathBuf,
    wal: Mutex<SerdeWal<CollectionUpdateOperations>>,
    segments: LockedSegmentHolder,
}

impl Shard {
    pub fn update(&self, operation: CollectionUpdateOperations) -> OperationResult<()> {
        let mut wal = self.wal.lock();

        let operation_id = wal.write(&operation).map_err(service_error)?;
        let hw_counter = HardwareCounterCell::disposable();

        let result = match operation {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                process_point_operation(&self.segments, operation_id, point_operation, &hw_counter)
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                process_vector_operation(
                    &self.segments,
                    operation_id,
                    vector_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                process_payload_operation(
                    &self.segments,
                    operation_id,
                    payload_operation,
                    &hw_counter,
                )
            }
            CollectionUpdateOperations::FieldIndexOperation(index_operation) => {
                process_field_index_operation(
                    &self.segments,
                    operation_id,
                    &index_operation,
                    &hw_counter,
                )
            }
        };

        result.map(|_| ())
    }

    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        let segments: Vec<_> = self
            .segments
            .read()
            .non_appendable_then_appendable_segments()
            .collect();

        let CoreSearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold, // TODO
        } = search;

        let vector_name = query.get_vector_name().to_string();
        let query_vector = query.into();
        let with_payload = WithPayload::from(with_payload.unwrap_or_default());
        let with_vector = WithVector::from(with_vector.unwrap_or_default());

        let context =
            QueryContext::new(DEFAULT_FULL_SCAN_THRESHOLD, HwMeasurementAcc::disposable());

        let mut points = Vec::new();

        for segment in segments {
            let batched_points = segment.get().read().search_batch(
                &vector_name,
                &[&query_vector],
                &with_payload,
                &with_vector,
                filter.as_ref(),
                offset + limit,
                params.as_ref(),
                &context.get_segment_query_context(),
            )?;

            debug_assert_eq!(batched_points.len(), 1);

            let [mut segment_points] = batched_points.try_into().expect(""); // TODO!
            points.append(&mut segment_points);
        }

        // Sort points by ID (asc) and version (desc)
        //
        // E.g.:
        //   { id: 1, ver: 10 }, { id: 1, ver: 8 }, { id: 3, ver: 15 }, { id: 3, ver: 13 }...
        points.sort_unstable_by(|left, right| {
            left.id
                .cmp(&right.id)
                .then(left.version.cmp(&right.version).reverse())
        });

        // Deduplicate points with same ID, only retaining point with most recent (highest) version
        let mut prev_point_id = None;
        points.retain(|point| {
            let retain = prev_point_id != Some(point.id);
            prev_point_id = Some(point.id);
            retain
        });

        // Sort points by score (desc)
        //
        // E.g.:
        //   { id: 69, score: 666.0 }, { id: 42, score: 420.0 }, { id: 1337, score: 228.0 }...
        points.sort_unstable_by(|left, right| left.score.total_cmp(&right.score).reverse());

        // Remove first `offset` points
        let mut idx = 0;
        points.retain(|_| {
            let retain = idx > offset;
            idx += 1;
            retain
        });

        // Truncate up to `limit` points
        points.truncate(points.len().saturating_sub(offset));

        Ok(points)
    }
}

fn service_error(err: impl fmt::Display) -> OperationError {
    OperationError::service_error(err.to_string())
}
