use crate::vector_storage::vector_storage::{ScoredPointOffset, VectorStorage};
use crate::index::index::{Index, PayloadIndex};
use crate::types::{Filter, VectorElementType, Distance, SearchParams};
use crate::payload_storage::payload_storage::{ConditionChecker};

use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::entry::entry_point::OperationResult;
use crate::index::field_index::Estimation;


pub struct PlainPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
}


impl PlainPayloadIndex {
    pub fn new(condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>,
               vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>) -> Self {
        PlainPayloadIndex {
            condition_checker,
            vector_storage,
        }
    }
}

impl PayloadIndex for PlainPayloadIndex {
    fn estimate_cardinality(&self, query: &Filter) -> Estimation {
        let mut matched_points = 0;
        let condition_checker = self.condition_checker.borrow();
        for i in self.vector_storage.borrow().iter_ids() {
            if condition_checker.check(i, query) {
                matched_points += 1;
            }
        }
        Estimation { min: matched_points, exp: matched_points, max: matched_points }
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        let mut matched_points = vec![];
        let condition_checker = self.condition_checker.borrow();
        for i in self.vector_storage.borrow().iter_ids() {
            if condition_checker.check(i, query) {
                matched_points.push(i);
            }
        }
        return matched_points;
    }
}


pub struct PlainIndex {
    vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
    payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
    distance: Distance,
}

impl PlainIndex {
    pub fn new(
        vector_storage: Arc<AtomicRefCell<dyn VectorStorage>>,
        payload_index: Arc<AtomicRefCell<dyn PayloadIndex>>,
        distance: Distance,
    ) -> PlainIndex {
        return PlainIndex {
            vector_storage,
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
        _params: Option<&SearchParams>,
    ) -> Vec<ScoredPointOffset> {
        match filter {
            Some(filter) => {
                let filtered_ids = self.payload_index.borrow().query_points(filter);
                self.vector_storage.borrow().score_points(vector, &filtered_ids, top, &self.distance)
            }
            None => self.vector_storage.borrow().score_all(vector, top, &self.distance)
        }
    }

    fn build_index(&mut self) -> OperationResult<()> {
        Ok(())
    }
}