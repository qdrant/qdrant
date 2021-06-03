use crate::payload_storage::payload_storage::ConditionChecker;
use crate::types::{Filter, PointOffsetType};
use crate::index::visited_pool::VisitedList;

pub struct BuildConditionChecker {
    pub filter_list: VisitedList
}

impl BuildConditionChecker {
    pub fn new(list_size: usize) -> Self {
        BuildConditionChecker {
            filter_list: VisitedList::new(list_size)
        }
    }
}


impl ConditionChecker for BuildConditionChecker {
    fn check(&self, point_id: PointOffsetType, _query: &Filter) -> bool {
        self.filter_list.check(point_id)
    }
}
