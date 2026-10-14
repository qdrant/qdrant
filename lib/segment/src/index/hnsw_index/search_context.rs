use std::collections::BinaryHeap;

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{ScoreType, ScoredPointOffset};
use num_traits::float::FloatCore;

use crate::index::visited_pool::VisitedListHandle;

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

    /// Create search context from `entries`, marking them as visited.
    pub fn with_entries(
        ef: usize,
        entries: &[ScoredPointOffset],
        visited_list: &mut VisitedListHandle,
    ) -> Self {
        let mut search_context = Self::new(ef);
        for &entry in entries {
            if !visited_list.check_and_update_visited(entry.idx) {
                search_context.process_candidate(entry);
            }
        }
        search_context
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
