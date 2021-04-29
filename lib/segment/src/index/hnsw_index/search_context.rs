use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::vector_storage::vector_storage::ScoredPointOffset;
use std::collections::HashSet;
use crate::index::hnsw_index::visited_pool::{VisitedList, VisitedPool};
use crate::types::{PointOffsetType, ScoreType};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc};
use parking_lot::RwLock;

/// Structure that holds context of the search
pub struct SearchContext {
    pub nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    pub candidates: Vec<PointOffsetType>,
}


impl SearchContext {
    pub fn new(entry_point: ScoredPointOffset, ef: usize) -> Self {
        let mut nearest = FixedLengthPriorityQueue::new(ef);
        nearest.push(entry_point);
        SearchContext {
            nearest: FixedLengthPriorityQueue::new(ef),
            candidates: vec![entry_point.idx]
        }
    }

    /// Updates search context with new scored point.
    /// If it is closer than existing - also add it to candidates for further search
    pub fn process_candidate(&mut self, score_point: ScoredPointOffset) {
        let was_added = match self.nearest.push(score_point.clone()) {
            None => true,
            Some(removed) => removed.idx != score_point.idx
        };
        if was_added {
            self.candidates.push(score_point.idx)
            // ToDo: update cache here
        }
    }
}