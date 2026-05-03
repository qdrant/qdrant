#[cfg(feature = "testing")]
use super::Segment;
#[cfg(feature = "testing")]
use crate::common::operation_error::OperationResult;
#[cfg(feature = "testing")]
use crate::data_types::query_context::QueryContext;
#[cfg(feature = "testing")]
use crate::data_types::vectors::QueryVector;
#[cfg(feature = "testing")]
use crate::entry::ReadSegmentEntry;
#[cfg(feature = "testing")]
use crate::types::{Filter, ScoredPoint, SearchParams, VectorName, WithPayload, WithVector};

#[cfg(feature = "testing")]
impl Segment {
    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[allow(clippy::too_many_arguments)]
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
