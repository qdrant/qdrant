use crate::payload_storage::payload_storage::ConditionChecker;
use crate::types::{Filter, PointOffsetType};
use crate::index::visited_pool::VisitedList;

pub struct BuildConditionChecker {
    pub filter_list: VisitedList,
    pub current_point: PointOffsetType
}

impl BuildConditionChecker {
    pub fn new(list_size: usize) -> Self {
        BuildConditionChecker {
            filter_list: VisitedList::new(list_size),
            current_point: PointOffsetType::default()
        }
    }
}


impl ConditionChecker for BuildConditionChecker {
    fn check(&self, point_id: PointOffsetType, _query: &Filter) -> bool {
        if point_id == self.current_point {
            return false // Do not match current point while inserting it (second time)
        }
        self.filter_list.check(point_id)
    }
}
