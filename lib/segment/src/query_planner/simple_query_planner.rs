use crate::index::index::Index;
use crate::query_planner::query_planner::QueryPlanner;
use crate::types::{Filter, VectorElementType, PointOffsetType, ScoreType};
use std::rc::Rc;
use std::cell::RefCell;

pub struct SimpleQueryPlanner {
    index: Rc<RefCell<dyn Index>>
}

impl QueryPlanner for SimpleQueryPlanner {
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize
    ) -> Vec<(PointOffsetType, ScoreType)> {
        self.index.borrow().search(vector, filter, top)
    }
}

impl SimpleQueryPlanner {
    pub fn new(index: Rc<RefCell<dyn Index>>) -> Self {
        SimpleQueryPlanner {
            index
        }
    }
}