use segment::types::Filter;

use crate::operations::query_enum::QueryEnum;
use crate::operations::types::{CoreSearchRequest, QueryScrollRequestInternal};

pub fn filter_rate_cost(filter: &Filter) -> usize {
    filter.total_conditions_count()
}

/// Base cost for a read operation
pub const BASE_COST: usize = 1;

impl CoreSearchRequest {
    pub fn search_rate_cost(&self) -> usize {
        let mut cost = match &self.query {
            QueryEnum::Nearest(_nq) => {
                // TODO(strict-mode) should the cost be adjusted here?
                BASE_COST
            }
            QueryEnum::RecommendBestScore(rb) => {
                rb.query.positives.len() + rb.query.negatives.len()
            }
            QueryEnum::RecommendSumScores(rs) => {
                rs.query.positives.len() + rs.query.negatives.len()
            }
            QueryEnum::Discover(rd) => rd.query.pairs.len(),
            QueryEnum::Context(rc) => rc.query.pairs.len(),
        };
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
