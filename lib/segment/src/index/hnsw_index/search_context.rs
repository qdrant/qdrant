use std::collections::BinaryHeap;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{ScoreType, ScoredPointOffset};
use num_traits::float::FloatCore;

/// Structure that holds context of the search
pub struct SearchContext {
    /// Overall nearest points found so far
    pub nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    /// Current candidates to process
    pub candidates: BinaryHeap<ScoredPointOffset>,
}

impl SearchContext {
    pub fn new(ef: usize) -> Self {
        SearchContext {
            nearest: FixedLengthPriorityQueue::new(ef),
            candidates: BinaryHeap::new(),
        }
    }

    pub fn lower_bound(&self) -> ScoreType {
        match self.nearest.top() {
            None => ScoreType::min_value(),
            Some(worst_of_the_best) => worst_of_the_best.score,
        }
    }

    /// Updates search context with new scored point.
    /// If it is closer than existing - also add it to candidates for further search
    pub fn process_candidate(&mut self, score_point: ScoredPointOffset) {
        let was_added = match self.nearest.push(score_point) {
            None => true,
            Some(removed) => removed.idx != score_point.idx,
        };
        if was_added {
            self.candidates.push(score_point);
        }
    }
}
