use std::sync::Arc;

use bitvec::prelude::BitVec;
use common::types::PointOffsetType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HasIdConditionInternal {
    pub ids_mask: Arc<BitVec>,
}

impl HasIdConditionInternal {
    pub fn new(ids_mask: Arc<BitVec>) -> Self {
        Self { ids_mask }
    }

    pub fn number_of_ids(&self) -> usize {
        self.ids_mask.count_ones()
    }

    pub fn check_id(&self, id: PointOffsetType) -> bool {
        self.ids_mask.get(id as usize).map(|x| *x).unwrap_or(false)
    }
}
