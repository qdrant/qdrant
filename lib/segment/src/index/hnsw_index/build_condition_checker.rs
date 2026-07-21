use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::visited_pool::VisitedListHandle;

pub struct BuildConditionChecker<'a> {
    pub filter_list: &'a VisitedListHandle<'a>,
    pub current_point: PointOffsetType,
}

impl ConditionChecker for BuildConditionChecker<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        if point_id == self.current_point {
            return Ok(false); // Do not match current point while inserting it (second time)
        }
        Ok(self.filter_list.check(point_id))
    }

    fn check_batched<K>(&self, ids: &mut [K], select: Select, rest: Rest) -> OperationResult<usize>
    where
        K: CheckItem,
    {
        default_check_batched(ids, select, rest, |id| self.check(id))
    }
}
