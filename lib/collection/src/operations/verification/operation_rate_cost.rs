use crate::operations::query_enum::QueryEnum;
use crate::operations::types::{CoreSearchRequest, QueryScrollRequestInternal};

impl CoreSearchRequest {
    pub fn rate_cost(&self) -> usize {
        match &self.query {
            QueryEnum::Nearest(_nq) => {
                // TODO(strict-mode) should the cost be adjusted here?
                1
            }
            QueryEnum::RecommendBestScore(rb) => {
                rb.query.positives.len() + rb.query.negatives.len()
            }
            QueryEnum::RecommendSumScores(rs) => {
                rs.query.positives.len() + rs.query.negatives.len()
            }
            QueryEnum::Discover(rd) => rd.query.pairs.len(),
            QueryEnum::Context(rc) => rc.query.pairs.len(),
        }
    }
}

impl QueryScrollRequestInternal {
    pub fn rate_cost(&self) -> usize {
        // TODO(strict-mode) how much should a scroll cost?
        1
    }
}
