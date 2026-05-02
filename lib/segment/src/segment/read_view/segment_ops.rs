use common::types::PointOffsetType;

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
    pub(crate) fn lookup_internal_id(
        &self,
        point_id: PointIdType,
    ) -> OperationResult<PointOffsetType> {
        self.id_tracker
            .internal_id(point_id)
            .ok_or(OperationError::PointIdError {
                missed_point_id: point_id,
            })
    }
}
