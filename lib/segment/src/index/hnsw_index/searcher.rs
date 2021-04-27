use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::vector_storage::vector_storage::ScoredPointOffset;
use std::collections::HashSet;
use crate::index::hnsw_index::visited_pool::VisitedPool;
use crate::types::{PointOffsetType, ScoreType};
use std::cell::RefCell;
use std::rc::Rc;

/// Structure that holds context of the search
pub struct Searcher {
    pub nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    pub candidates: Vec<PointOffsetType>,
    pub seen: Rc<RefCell<VisitedPool>>,
}


impl Searcher {
    pub fn new(entry_point: ScoredPointOffset, ef: usize, visited_pool: Rc<RefCell<VisitedPool>>) -> Self {

        {
            let mut vp = visited_pool.borrow_mut();
            vp.next_iteration();
            vp.update_visited(entry_point.idx);
        }

        let mut nearest = FixedLengthPriorityQueue::new(ef);
        nearest.push(entry_point);
        Searcher {
            nearest: FixedLengthPriorityQueue::new(ef),
            candidates: vec![entry_point.idx],
            seen: visited_pool.clone(),
        }
    }
}