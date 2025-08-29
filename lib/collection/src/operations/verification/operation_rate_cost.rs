use segment::types::Filter;

use crate::operations::types::QueryScrollRequestInternal;

pub fn filter_rate_cost(filter: &Filter) -> usize {
    filter.total_conditions_count()
}

/// Base cost for a read operation
pub const BASE_COST: usize = 1;

impl QueryScrollRequestInternal {
    pub fn scroll_rate_cost(&self) -> usize {
        let mut cost = BASE_COST;
        if let Some(filter) = &self.filter {
            cost += filter_rate_cost(filter);
        }
        cost
    }
}
