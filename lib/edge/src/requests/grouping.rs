use segment::json_path::JsonPath;

use crate::requests::query::QueryRequest;

/// Group the results of a base query by a payload field.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupRequest {
    /// Base scoring query. Its `limit`/`offset`/`with_payload`/`filter` are adjusted
    /// by the grouping driver; the query vector, params, prefetch and score_threshold
    /// are honoured.
    pub query: QueryRequest,
    /// Payload field to group by.
    pub group_by: JsonPath,
    /// Maximum number of groups to return.
    pub groups: usize,
    /// Maximum number of hits per group.
    pub group_size: usize,
}

impl GroupRequest {
    pub fn new(query: QueryRequest, group_by: JsonPath, groups: usize, group_size: usize) -> Self {
        Self {
            query,
            group_by,
            groups,
            group_size,
        }
    }
}
