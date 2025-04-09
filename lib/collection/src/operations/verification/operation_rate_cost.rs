use segment::types::Filter;

use crate::operations::types::{CoreSearchRequest, QueryScrollRequestInternal};

pub fn filter_rate_cost(filter: &Filter) -> usize {
    filter.total_conditions_count()
}

/// Base cost for a read operation
pub const BASE_COST: usize = 1;

impl CoreSearchRequest {
    pub fn search_rate_cost(&self) -> usize {
        let mut cost = self.query.search_cost();
        if let Some(filter) = &self.filter {
            cost += filter_rate_cost(filter);
        }
        cost
    }
}

impl QueryScrollRequestInternal {
    pub fn scroll_rate_cost(&self) -> usize {
        let mut cost = BASE_COST;
        if let Some(filter) = &self.filter {
            cost += filter_rate_cost(filter);
        }
        cost
    }
}
