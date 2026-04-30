mod segment_ops;

use common::counter::hardware_counter::HardwareCounterCell;

use crate::common::operation_error::OperationResult;
use crate::data_types::query_context::SegmentQueryContext;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::id_tracker::IdTrackerRead;
use crate::types::{
    Filter, PointIdType, ScoredPoint, SearchParams, SeqNumberType, VectorName, WithPayload,
    WithVector,
};

/// This structure serves as a generic representation of data
/// necessary for all read operations on a segment.
///
/// The motivation for this is to unify the read code between
/// regular `Segment` and `ReadOnlySegment`.
#[allow(unused, dead_code)]
pub struct SegmentReadView<'segment, TIdTracker: IdTrackerRead> {
    pub(crate) id_tracker: &'segment TIdTracker,
    pub(crate) segment_config: &'segment crate::types::SegmentConfig,
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

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        let internal_id = self.lookup_internal_id(point_id)?;
        todo!()
        // let vector_opt = self.vector_by_offset(vector_name, internal_id, hw_counter)?;
        // Ok(vector_opt)
    }
}
