use super::QueryRequest;

/// Queries executed together as one planned batch.
///
/// Pass this request to [`EdgeShardRead::query_batch`](crate::EdgeShardRead::query_batch).
/// An empty batch returns an empty result list.
///
/// ```
/// use edge::{QueryBatchRequest, QueryRequest};
///
/// let request = QueryBatchRequest::new(vec![QueryRequest::new(10), QueryRequest::new(5)]);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct QueryBatchRequest {
    /// Queries to execute. Results are returned in the same order.
    pub queries: Vec<QueryRequest>,
}

impl QueryBatchRequest {
    pub fn new(queries: Vec<QueryRequest>) -> Self {
        Self { queries }
    }
}
