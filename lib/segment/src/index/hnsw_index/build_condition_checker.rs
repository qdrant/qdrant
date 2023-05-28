use crate::entry::entry_point::OperationResult;
use crate::index::visited_pool::VisitedList;
use crate::payload_storage::FilterContext;
use crate::types::PointOffsetType;

pub struct BuildConditionChecker<'a> {
    pub filter_list: &'a VisitedList,
    pub current_point: PointOffsetType,
}

impl<'a> FilterContext for BuildConditionChecker<'a> {
    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        if point_id == self.current_point {
            return Ok(false); // Do not match current point while inserting it (second time)
        }
        Ok(self.filter_list.check(point_id))
    }
}
