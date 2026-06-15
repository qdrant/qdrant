use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::index::query_optimization::optimized_filter::ConditionChecker;
use crate::index::visited_pool::VisitedListHandle;

pub struct BuildConditionChecker<'a> {
    pub filter_list: &'a VisitedListHandle<'a>,
    pub current_point: PointOffsetType,
}

impl ConditionChecker for BuildConditionChecker<'_> {
    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        if point_id == self.current_point {
            return Ok(false); // Do not match current point while inserting it (second time)
        }
        Ok(self.filter_list.check(point_id))
    }
}
