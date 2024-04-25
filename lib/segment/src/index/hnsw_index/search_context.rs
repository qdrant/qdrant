use std::cmp::Reverse;
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

    /// Lazily use the candidate score to update the lower bound
    pub fn process_candidate(&mut self, score_point: ScoredPointOffset) {
        self.nearest.push(score_point);
    }

    /// Update candidates for next rounds with new candidates if they score higher than the latest lower bound
    pub fn update_candidates(&mut self, potential_candidates: &[ScoredPointOffset]) {
        if potential_candidates.is_empty() {
            return;
        }

        self.nearest.update_threshold();

        for potential_candidate in potential_candidates {
            if potential_candidate.score >= self.lower_bound() {
                self.candidates.push(*potential_candidate);
            }
        }
    }

    // Returns iterator over the nearest points found so far
    pub fn iter_nearest(&mut self) -> impl Iterator<Item = &ScoredPointOffset> {
        self.nearest.truncate();
        self.nearest.elements.iter().map(|Reverse(x)| x)
    }
}
