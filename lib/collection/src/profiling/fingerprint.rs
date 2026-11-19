//! Request shapes for workload fingerprints, built without serializing literal-heavy parts.
//!
//! Filters can carry large id sets: a distance matrix request runs N searches, each with a
//! `has_id` filter of all N sampled ids. Serializing those just to replace them by `"?"`
//! would cost O(N²) per shard operation. These shapes keep the structure (payload keys,
//! condition kinds, options) and replace id sets by a placeholder, so building one costs
//! O(number of conditions) instead of O(number of ids).

use segment::types::{Condition, Filter, MinShould, Nested, NestedCondition};
use serde_json::{Value, json};
use shard::query::{ShardPrefetch, ShardQueryRequest};

use crate::operations::generalizer::Generalizer;

const LITERAL: &str = "?";

fn to_value<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|err| Value::String(format!("<{err}>")))
}

fn conditions_shape(conditions: &[Condition]) -> Value {
    Value::Array(conditions.iter().map(condition_shape).collect())
}

/// The structure of `filter`: conditions with their payload keys, id sets as `"?"`.
pub fn filter_shape(filter: &Filter) -> Value {
    let Filter {
        should,
        min_should,
        must,
        must_not,
    } = filter;
    let mut shape = serde_json::Map::new();
    if let Some(should) = should {
        shape.insert("should".to_string(), conditions_shape(should));
    }
    if let Some(MinShould {
        conditions,
        min_count,
    }) = min_should
    {
        shape.insert(
            "min_should".to_string(),
            json!({ "conditions": conditions_shape(conditions), "min_count": min_count }),
        );
    }
    if let Some(must) = must {
        shape.insert("must".to_string(), conditions_shape(must));
    }
    if let Some(must_not) = must_not {
        shape.insert("must_not".to_string(), conditions_shape(must_not));
    }
    Value::Object(shape)
}

fn condition_shape(condition: &Condition) -> Value {
    match condition {
        // Small: a key and its match / range / geo values (normalized later)
        Condition::Field(field) => to_value(field),
        Condition::IsEmpty(is_empty) => to_value(is_empty),
        Condition::IsNull(is_null) => to_value(is_null),
        Condition::HasVector(has_vector) => to_value(has_vector),
        Condition::Slice(slice) => to_value(slice),
        // Id sets: any size, always literals
        Condition::HasId(_) => json!({ "has_id": LITERAL }),
        Condition::CustomIdChecker(_) => json!({ "custom_id_checker": LITERAL }),
        Condition::ResolvedPoints(_) => json!({ "resolved_points": LITERAL }),
        Condition::Nested(NestedCondition {
            nested: Nested { key, filter },
        }) => json!({ "nested": { "key": key, "filter": filter_shape(filter) } }),
        Condition::Filter(filter) => filter_shape(filter),
    }
}

fn prefetch_shape(prefetch: &ShardPrefetch) -> Value {
    let ShardPrefetch {
        prefetches,
        query,
        limit,
        params,
        filter,
        score_threshold,
    } = prefetch;
    json!({
        "prefetches": prefetches.iter().map(prefetch_shape).collect::<Vec<_>>(),
        // Vectors are replaced by their dimensions
        "query": query.as_ref().map(|query| to_value(&query.remove_details())),
        "limit": limit,
        "params": params,
        "filter": filter.as_ref().map(filter_shape),
        "score_threshold": score_threshold,
    })
}

/// The structure of a shard query: vectors and id sets are not serialized.
pub fn shard_query_shape(request: &ShardQueryRequest) -> Value {
    let ShardQueryRequest {
        prefetches,
        query,
        filter,
        score_threshold,
        limit,
        offset,
        params,
        with_vector,
        with_payload,
    } = request;
    json!({
        "prefetches": prefetches.iter().map(prefetch_shape).collect::<Vec<_>>(),
        "query": query.as_ref().map(|query| to_value(&query.remove_details())),
        "filter": filter.as_ref().map(filter_shape),
        "score_threshold": score_threshold,
        "limit": limit,
        "offset": offset,
        "params": params,
        "with_vector": with_vector,
        "with_payload": with_payload,
    })
}

#[cfg(test)]
mod tests {
    use ahash::AHashSet;
    use segment::types::{FieldCondition, HasIdCondition, Match, PointIdType, ValueVariants};

    use super::*;

    #[test]
    fn test_filter_shape_skips_id_sets() {
        let ids: AHashSet<PointIdType> = (0..100_000u64).map(PointIdType::from).collect();
        let filter = Filter::new_must(Condition::HasId(HasIdCondition::from(ids)));
        let shape = filter_shape(&filter);
        assert_eq!(shape, json!({ "must": [{ "has_id": "?" }] }));
        assert!(shape.to_string().len() < 64);
    }

    #[test]
    fn test_filter_shape_keeps_structure() {
        let city = Condition::Field(FieldCondition::new_match(
            "city".try_into().unwrap(),
            Match::new_value(ValueVariants::String("London".to_string())),
        ));
        let nested = Condition::new_nested(
            "diet".try_into().unwrap(),
            Filter::new_must(Condition::Field(FieldCondition::new_match(
                "likes".try_into().unwrap(),
                Match::new_value(ValueVariants::Bool(true)),
            ))),
        );
        let shape = filter_shape(&Filter {
            should: Some(vec![city]),
            min_should: None,
            must: Some(vec![nested]),
            must_not: None,
        });
        assert_eq!(shape["should"][0]["key"], "city");
        assert_eq!(shape["must"][0]["nested"]["key"], "diet");
        assert_eq!(
            shape["must"][0]["nested"]["filter"]["must"][0]["key"],
            "likes"
        );
    }
}
