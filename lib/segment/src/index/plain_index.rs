use crate::vector_storage::vector_storage::{VectorMatcher, ScoredPointOffset, VectorCounter};
use crate::index::index::{Index, PayloadIndex};
use crate::types::{Filter, VectorElementType, Distance, SearchParams};
use crate::payload_storage::payload_storage::{ConditionChecker};

use std::sync::Arc;
use atomic_refcell::AtomicRefCell;


pub struct PlainPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_counter: Arc<AtomicRefCell<dyn VectorCounter>>,
}


impl PlainPayloadIndex {
    pub fn new(condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
               vector_counter: Arc<AtomicRefCell<dyn VectorCounter>>) -> Self {
        PlainPayloadIndex {
            condition_checker,
            vector_counter,
        }
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn estimate_cardinality(&self, query: &Filter) -> (usize, usize) {
        let mut matched_points = 0;
        let vector_count = self.vector_counter.borrow().vector_count();
        let condition_checker = self.condition_checker.borrow();
        for i in 0..vector_count {
            if condition_checker.check(i, query) {
                matched_points += 1;
            }
        }
        (matched_points, matched_points)
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        let mut matched_points = vec![];
        let vector_count = self.vector_counter.borrow().vector_count();
        let condition_checker = self.condition_checker.borrow();
        for i in 0..vector_count {
            if condition_checker.check(i, query) {
                matched_points.push(i);
            }
        }
        return matched_points;
    }
}


pub struct PlainIndex {
    vector_matcher:Arc<AtomicRefCell<dyn VectorMatcher>>,
    payload_index:Arc<AtomicRefCell<dyn PayloadIndex>>,
    distance: Distance,
}

impl PlainIndex {
    pub fn new(
        vector_matcher:Arc<AtomicRefCell<dyn VectorMatcher>>,
        payload_index:Arc<AtomicRefCell<dyn PayloadIndex>>,
        distance: Distance,
    ) -> PlainIndex {
        return PlainIndex {
            vector_matcher,
            payload_index,
            distance,
        };
    }
}


impl Index for PlainIndex {
    fn search(
        &self,
        vector: &Vec<VectorElementType>,
        filter: Option<&Filter>,
        top: usize,
        _params: Option<&SearchParams>
    ) -> Vec<ScoredPointOffset> {
        match filter {
            Some(filter) => {
                let filtered_ids = self.payload_index.borrow().query_points(filter);
                self.vector_matcher.borrow().score_points(vector, &filtered_ids, top, &self.distance)
            }
            None => self.vector_matcher.borrow().score_all(vector, top, &self.distance)
        }
    }
}