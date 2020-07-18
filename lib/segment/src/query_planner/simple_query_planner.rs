use crate::index::index::Index;
use crate::query_planner::query_planner::QueryPlanner;
use crate::types::{Filter, VectorElementType, PointOffsetType, ScoreType, SearchParams};
use std::rc::Rc;
use std::cell::RefCell;
use crate::vector_storage::vector_storage::ScoredPointOffset;

pub struct SimpleQueryPlanner {
    index: Rc<RefCell<dyn Index>>
}

impl QueryPlanner for SimpleQueryPlanner {
    fn search(&self,
              vector: &Vec<VectorElementType>,
              filter: Option<&Filter>,
              top: usize,
              params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        self.index.borrow().search(vector, filter, top, params)
    }
}

impl SimpleQueryPlanner {
    pub fn new(index: Rc<RefCell<dyn Index>>) -> Self {
        SimpleQueryPlanner {
            index
        }
    }
}