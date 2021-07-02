use crate::spaces::tools::FixedLengthPriorityQueue;
use crate::types::ScoreType;
use crate::vector_storage::ScoredPointOffset;
use num_traits::float::FloatCore;
use std::collections::BinaryHeap;

/// Structure that holds context of the search
pub struct SearchContext {
    pub nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    pub candidates: BinaryHeap<ScoredPointOffset>,
}

impl SearchContext {
    pub fn new(entry_point: ScoredPointOffset, ef: usize) -> Self {
        let mut nearest = FixedLengthPriorityQueue::new(ef);
        nearest.push(entry_point);
        SearchContext {
            nearest,
            candidates: BinaryHeap::from(vec![entry_point]),
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
            self.candidates.push(score_point)
        }
    }
}
