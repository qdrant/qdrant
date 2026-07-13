use segment::types::Order;
pub use shard::query::*;

use crate::config::CollectionParams;
use crate::operations::types::CollectionResult;

/// Returns the expected order of results, depending on the type of query
pub fn query_result_order(
    query: Option<&ScoringQuery>,
    collection_params: &CollectionParams,
) -> CollectionResult<Option<Order>> {
    let order = shard::query::query_result_order(query, |vector_name| {
        collection_params.get_distance(vector_name)
    })?;
    Ok(order)
}
