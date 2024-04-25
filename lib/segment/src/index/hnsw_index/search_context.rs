use std::collections::BinaryHeap;

use common::top_k::TopK;
use common::types::{ScoreType, ScoredPointOffset};

/// Structure that holds context of the search
pub struct SearchContext {
    /// Overall nearest points found so far
    pub nearest: TopK,
    /// Current candidates to process
    pub candidates: BinaryHeap<ScoredPointOffset>,
}

impl SearchContext {
    pub fn new(entry_point: ScoredPointOffset, ef: usize) -> Self {
        let mut nearest = TopK::new(ef);
        nearest.push(entry_point);
        SearchContext {
            nearest,
            candidates: BinaryHeap::from_iter([entry_point]),
        }
    }

    pub fn lower_bound(&self) -> ScoreType {
        self.nearest.threshold()
    }

    /// Updates search context with new scored point.
    /// If it is closer than existing - also add it to candidates for further search
    pub fn process_candidate(&mut self, score_point: ScoredPointOffset) {
        let was_added = match self.nearest.push(score_point) {
            None => true,
            Some(removed) => removed
                .iter()
                .any(|removed_point| removed_point.idx == score_point.idx),
        };
        if was_added {
            self.candidates.push(score_point);
        }
    }
}
