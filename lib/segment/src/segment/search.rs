use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::ScoredPointOffset;

use super::Segment;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "testing")]
use crate::data_types::query_context::QueryContext;
use crate::data_types::segment_record::SegmentRecord;
#[cfg(feature = "testing")]
use crate::data_types::vectors::QueryVector;
use crate::data_types::vectors::VectorStructInternal;
use crate::entry::entry_point::NonAppendableSegmentEntry;
#[cfg(feature = "testing")]
use crate::types::VectorName;
#[cfg(feature = "testing")]
use crate::types::{Filter, SearchParams};
use crate::types::{ScoredPoint, WithPayload, WithVector};

impl Segment {
    /// Converts raw ScoredPointOffset search result into ScoredPoint result
    pub(super) fn process_search_result(
        &self,
        internal_result: Vec<ScoredPointOffset>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let id_tracker = self.id_tracker.borrow();
        let (point_ids, scored_offsets): (Vec<_>, Vec<_>) = internal_result
            .into_iter()
            .filter_map(|scored_point_offset| {
                let point_offset = scored_point_offset.idx;
                let point_id = id_tracker.external_id(point_offset);
                // This can happen if point was modified between retrieving and post-processing
                // But this function locks the segment, so it can't be modified during its execution
                debug_assert!(
                    point_id.is_some(),
                    "Point with internal ID {point_offset} not found in id tracker"
                );
                point_id.map(|id| (id, scored_point_offset))
            })
            .unzip();

        let mut segment_records = self.retrieve(
            &point_ids,
            with_payload,
            with_vector,
            hw_counter,
            is_stopped,
        )?;

        let mut results = Vec::with_capacity(point_ids.len());

        for (point_id, scored_offset) in point_ids.into_iter().zip(scored_offsets) {
            let ScoredPointOffset {
                idx: point_offset,
                score: point_score,
            } = scored_offset;

            let record = segment_records.remove(&point_id);

            // It is still possible, that for some reason scored points have duplicates
            // so we probably don't want to return error in release mode.
            // We also don't want to copy all data just to handle this unexpected case.
            let Some(record) = record else {
                debug_assert!(
                    false,
                    "Record for point ID {point_id} not found during search result processing"
                );
                continue;
            };

            let point_version = id_tracker.internal_version(point_offset).ok_or_else(|| {
                OperationError::service_error(format!(
                    "Corrupter id_tracker, no version for point {point_id}"
                ))
            })?;

            let SegmentRecord {
                id,
                vectors,
                payload,
            } = record;

            results.push(ScoredPoint {
                id,
                version: point_version,
                score: point_score,
                payload,
                vector: vectors.map(VectorStructInternal::from),
                shard_key: None,
                order_value: None,
            });
        }

        Ok(results)
    }

    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "testing")]
    pub fn search(
        &self,
        vector_name: &VectorName,
        vector: &QueryVector,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();

        let result = self.search_batch(
            vector_name,
            &[vector],
            with_payload,
            with_vector,
            filter,
            top,
            params,
            &segment_query_context,
        )?;

        Ok(result.into_iter().next().unwrap())
    }
}
