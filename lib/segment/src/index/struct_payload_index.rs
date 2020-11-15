use crate::index::index::{PayloadIndex};
use crate::types::Filter;
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::payload_storage::payload_storage::ConditionChecker;
use crate::index::field_index::EstimationResult;


struct StructPayloadIndex {
    condition_checker: Arc<AtomicRefCell<dyn ConditionChecker>>
}

impl StructPayloadIndex {
    fn total_points(&self) -> usize {
        unimplemented!()
    }
}


impl PayloadIndex for StructPayloadIndex {
    fn estimate_cardinality(&self, query: &Filter) -> EstimationResult {
        unimplemented!()
    }

    fn query_points(&self, query: &Filter) -> Vec<usize> {
        unimplemented!()
    }
}

