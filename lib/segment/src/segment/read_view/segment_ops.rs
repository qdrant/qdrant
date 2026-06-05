use common::types::{DeferredBehavior, PointOffsetType};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::PointIdType;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Resolve an external id to its internal offset under the caller-chosen
    /// deferred semantics. The behavior is explicit so this low-level helper
    /// makes no snapshot-vs-latest policy assumption on the caller's behalf.
    pub(crate) fn lookup_internal_id(
        &self,
        point_id: PointIdType,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<PointOffsetType> {
        self.id_tracker
            .internal_id_with_behavior(point_id, deferred_behavior)
            .ok_or(OperationError::PointIdError {
                missed_point_id: point_id,
            })
    }
}
