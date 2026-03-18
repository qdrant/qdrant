use common::types::PointOffsetType;

use crate::index::visited_pool::VisitedListHandle;
use crate::payload_storage::FilterContext;

pub struct BuildConditionChecker<'a> {
    pub filter_list: &'a VisitedListHandle<'a>,
    pub current_point: PointOffsetType,
}

impl FilterContext for BuildConditionChecker<'_> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        if point_id == self.current_point {
            return false; // Do not match current point while inserting it (second time)
        }
        self.filter_list.check(point_id)
    }
}
