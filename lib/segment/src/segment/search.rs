use common::types::ScoredPointOffset;

use super::Segment;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::NamedVectors;
#[cfg(feature = "testing")]
use crate::data_types::query_context::QueryContext;
#[cfg(feature = "testing")]
use crate::data_types::vectors::QueryVector;
use crate::data_types::vectors::VectorStructInternal;
#[cfg(feature = "testing")]
use crate::entry::entry_point::SegmentEntry;
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
    ) -> OperationResult<Vec<ScoredPoint>> {
        let id_tracker = self.id_tracker.borrow();
        internal_result
            .into_iter()
            .filter_map(|scored_point_offset| {
                let point_offset = scored_point_offset.idx;
                let external_id = id_tracker.external_id(point_offset);
                match external_id {
                    Some(point_id) => Some((point_id, scored_point_offset)),
                    None => {
                        log::warn!(
                            "Point with internal ID {} not found in id tracker, skipping",
                            point_offset
                        );
                        None
                    }
                }
            })
            .map(|(point_id, scored_point_offset)| {
                let point_offset = scored_point_offset.idx;
                let point_version = id_tracker.internal_version(point_offset).ok_or_else(|| {
                    OperationError::service_error(format!(
                        "Corrupter id_tracker, no version for point {point_id}"
                    ))
                })?;
                let payload = if with_payload.enable {
                    let initial_payload = self.payload_by_offset(point_offset)?;
                    let processed_payload = if let Some(i) = &with_payload.payload_selector {
                        i.process(initial_payload)
                    } else {
                        initial_payload
                    };
                    Some(processed_payload)
                } else {
                    None
                };
                let vector = match with_vector {
                    WithVector::Bool(false) => None,
                    WithVector::Bool(true) => Some(self.all_vectors_by_offset(point_offset).into()),
                    WithVector::Selector(vectors) => {
                        let mut result = NamedVectors::default();
                        for vector_name in vectors {
                            if let Some(vector) =
                                self.vector_by_offset(vector_name, point_offset)?
                            {
                                result.insert(vector_name.clone(), vector);
                            }
                        }
                        Some(VectorStructInternal::from(result))
                    }
                };

                Ok(ScoredPoint {
                    id: point_id,
                    version: point_version,
                    score: scored_point_offset.score,
                    payload,
                    vector,
                    shard_key: None,
                    order_value: None,
                })
            })
            .collect()
    }

    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "testing")]
    pub fn search(
        &self,
        vector_name: &str,
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

        // This function is only for testing and no measurements are needed.
        segment_query_context
            .take_hardware_counter()
            .discard_results();

        Ok(result.into_iter().next().unwrap())
    }
}
