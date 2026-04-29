use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::SegmentQueryContext;
use crate::data_types::vectors::QueryVector;
use crate::id_tracker::IdTrackerRead;
use crate::types::{
    Filter, PointIdType, ScoredPoint, SearchParams, SeqNumberType, VectorName, WithPayload,
    WithVector,
};

pub struct SegmentReadView<'segment, TIdTracker: IdTrackerRead> {
    pub(crate) id_tracker: &'segment TIdTracker,
}

#[allow(unused, dead_code)]
impl<'s, TIdTracker: IdTrackerRead> SegmentReadView<'s, TIdTracker> {
    pub fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        self.id_tracker
            .internal_id(point_id)
            .and_then(|internal_id| self.id_tracker.internal_version(internal_id))
    }

    #[allow(clippy::too_many_arguments)]
    fn search_batch(
        &self,
        vector_name: &VectorName,
        query_vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        todo!()
    }
}
