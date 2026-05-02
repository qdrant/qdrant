use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::segment::read_view::SegmentReadView;
use crate::types::PointIdType;

impl<'s, TIdTracker: IdTrackerRead> SegmentReadView<'s, TIdTracker> {
    pub(super) fn lookup_internal_id(
        &self,
        _point_id: PointIdType,
    ) -> OperationResult<PointOffsetType> {
        todo!()
    }
}
