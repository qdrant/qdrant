use segment::types::Order;
pub use shard::query::*;

use crate::config::CollectionParams;
use crate::operations::types::CollectionResult;

/// Returns the expected order of results, depending on the type of query
pub fn query_result_order(
    query: Option<&ScoringQuery>,
    collection_params: &CollectionParams,
) -> CollectionResult<Option<Order>> {
    let order = match query {
        Some(scoring_query) => match scoring_query {
            ScoringQuery::Vector(query_enum) => {
                if query_enum.is_distance_scored() {
                    Some(
                        collection_params
                            .get_distance(query_enum.get_vector_name())?
                            .distance_order(),
                    )
                } else {
                    Some(Order::LargeBetter)
                }
            }
            ScoringQuery::Fusion(fusion) => match fusion {
                FusionInternal::RrfK(_) | FusionInternal::Dbsf => Some(Order::LargeBetter),
            },
            // Score boosting formulas are always have descending order,
            // Euclidean scores can be negated within the formula
            ScoringQuery::Formula(_formula) => Some(Order::LargeBetter),
            ScoringQuery::OrderBy(order_by) => Some(Order::from(order_by.direction())),
            // Random sample does not require ordering
            ScoringQuery::Sample(SampleInternal::Random) => None,
            // MMR cannot be reordered
            ScoringQuery::Mmr(_) => None,
        },
        None => {
            // Order by ID
            Some(Order::SmallBetter)
        }
    };
    Ok(order)
}
