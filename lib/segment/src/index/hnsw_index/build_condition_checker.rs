use crate::index::visited_pool::VisitedList;
use crate::payload_storage::FilterContext;
use crate::types::PointOffsetType;

pub struct BuildConditionChecker {
    pub filter_list: VisitedList,
    pub current_point: PointOffsetType,
}

impl BuildConditionChecker {
    pub fn new(list_size: usize) -> Self {
        BuildConditionChecker {
            filter_list: VisitedList::new(list_size),
            current_point: PointOffsetType::default(),
        }
    }
}

impl FilterContext for BuildConditionChecker {
    fn check(&self, point_id: PointOffsetType) -> bool {
        if point_id == self.current_point {
            return false; // Do not match current point while inserting it (second time)
        }
        self.filter_list.check(point_id)
    }
}
