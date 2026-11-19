use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use segment::data_types::facets::FacetParams;
use serde_json::Value;
use shard::count::CountRequestInternal;
use shard::operations::CollectionUpdateOperations;
use shard::query::query_enum::QueryEnum;
use shard::scroll::ScrollRequestInternal;
use shard::search::{CoreSearchRequest, CoreSearchRequestBatch};

use crate::operations::types::PointRequestInternal;
use crate::operations::universal_query::shard_query::ShardQueryRequest;
use crate::profiling::fingerprint::{filter_shape, shard_query_shape};
use crate::profiling::workload::normalize;

/// Log entries are retained in memory indefinitely, so a snapshot must not keep
/// unbounded request data alive. E.g. internally generated distance matrix
/// queries carry `has_id` filters with sample² point ids in total, which blows
/// up multi-fold when expanded into a JSON tree. Arrays longer than this are
/// cut down to this many elements plus an omission marker.
const MAX_LOGGED_ARRAY_LEN: usize = 64;

pub trait Loggable {
    fn to_log_value(&self) -> serde_json::Value;

    fn request_name(&self) -> &'static str;

    /// Hash of the query, which is going to be used for approximate deduplication and counting.
    fn request_hash(&self) -> u64;

    /// The complete request with literals stripped (see [`normalize`]), for workload
    /// fingerprints. Unlike [`Self::to_log_value`], arrays are not cut: two requests that
    /// differ only after the first [`MAX_LOGGED_ARRAY_LEN`] filter conditions must not share
    /// a fingerprint.
    fn to_fingerprint_value(&self) -> Value {
        let mut value = self.to_log_value();
        normalize(&mut value);
        value
    }
}

/// Serialize the whole request and strip its literals, for [`Loggable::to_fingerprint_value`].
fn normalized_value<T: serde::Serialize>(request: &T) -> Value {
    let mut value = serde_json::to_value(request).unwrap_or_default();
    normalize(&mut value);
    value
}

/// [`normalized_value`] of a batch, one item at a time: memory is bounded by the distinct
/// shapes, not by the size of the batch.
fn normalized_batch_value<T>(
    items: impl IntoIterator<Item = T>,
    value: impl Fn(T) -> Value,
) -> Value {
    let mut seen = ahash::AHashSet::new();
    let mut distinct: Vec<Value> = Vec::new();
    for item in items {
        let item = value(item);
        // `Value` is not `Hash`; its string is canonical enough for deduplication
        if seen.insert(item.to_string()) {
            distinct.push(item);
        }
    }
    Value::Array(distinct)
}

/// Serialize a request for the slow-requests log, bounding all arrays.
fn bounded_log_value<T: serde::Serialize>(request: &T) -> Value {
    let mut value = serde_json::to_value(request).unwrap_or_default();
    truncate_long_arrays(&mut value);
    value
}

fn omission_marker(omitted: usize) -> Value {
    Value::String(format!("... ({omitted} more items)"))
}

fn truncate_long_arrays(value: &mut Value) {
    match value {
        Value::Array(items) => {
            if items.len() > MAX_LOGGED_ARRAY_LEN {
                let omitted = items.len() - MAX_LOGGED_ARRAY_LEN;
                items.truncate(MAX_LOGGED_ARRAY_LEN);
                items.push(omission_marker(omitted));
            }
            for item in items.iter_mut() {
                truncate_long_arrays(item);
            }
        }
        Value::Object(map) => {
            for (_key, item) in map.iter_mut() {
                truncate_long_arrays(item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

impl Loggable for CollectionUpdateOperations {
    fn to_log_value(&self) -> Value {
        bounded_log_value(self)
    }

    fn request_name(&self) -> &'static str {
        "points-update"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for Vec<ShardQueryRequest> {
    fn to_log_value(&self) -> Value {
        // Serialize requests one by one, only up to the cap: serializing the
        // whole batch at once would materialize the full JSON tree (gigabytes
        // for internally generated matrix batches) before truncation could
        // trim it.
        let mut items: Vec<Value> = self
            .iter()
            .take(MAX_LOGGED_ARRAY_LEN)
            .map(bounded_log_value)
            .collect();
        if self.len() > MAX_LOGGED_ARRAY_LEN {
            items.push(omission_marker(self.len() - MAX_LOGGED_ARRAY_LEN));
        }
        Value::Array(items)
    }

    fn request_name(&self) -> &'static str {
        "query"
    }

    fn to_fingerprint_value(&self) -> Value {
        // Shapes, not the full requests: filters may hold large id sets (distance matrix)
        normalized_batch_value(self.iter(), |request| {
            let mut value = shard_query_shape(request);
            normalize(&mut value);
            value
        })
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for ScrollRequestInternal {
    fn to_log_value(&self) -> Value {
        bounded_log_value(self)
    }

    fn to_fingerprint_value(&self) -> Value {
        normalized_value(self)
    }

    fn request_name(&self) -> &'static str {
        "scroll"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl<T: Loggable> Loggable for Arc<T> {
    fn to_log_value(&self) -> Value {
        self.as_ref().to_log_value()
    }

    fn request_name(&self) -> &'static str {
        self.as_ref().request_name()
    }

    fn request_hash(&self) -> u64 {
        self.as_ref().request_hash()
    }

    fn to_fingerprint_value(&self) -> Value {
        self.as_ref().to_fingerprint_value()
    }
}

/// A search for logs and fingerprints: the query is reduced to its kind and vector name,
/// the vectors are not serialized.
fn core_search_value(search: &CoreSearchRequest) -> Value {
    let CoreSearchRequest {
        query,
        filter,
        params,
        limit,
        offset,
        with_payload,
        with_vector,
        score_threshold,
    } = search;
    let kind = match query {
        QueryEnum::Nearest(_) => "nearest",
        QueryEnum::RecommendBestScore(_) => "recommend_best_score",
        QueryEnum::RecommendSumScores(_) => "recommend_sum_scores",
        QueryEnum::Discover(_) => "discover",
        QueryEnum::Context(_) => "context",
        QueryEnum::FeedbackNaive(_) => "feedback_naive",
    };
    serde_json::json!({
        "query": { "kind": kind, "using": query.get_vector_name() },
        "filter": filter.as_ref().map(filter_shape),
        "params": params,
        "limit": limit,
        "offset": offset,
        "with_payload": with_payload,
        "with_vector": with_vector,
        "score_threshold": score_threshold,
    })
}

impl Loggable for CoreSearchRequestBatch {
    fn to_log_value(&self) -> Value {
        let mut items: Vec<Value> = self
            .searches
            .iter()
            .take(MAX_LOGGED_ARRAY_LEN)
            .map(core_search_value)
            .collect();
        items.iter_mut().for_each(truncate_long_arrays);
        if self.searches.len() > MAX_LOGGED_ARRAY_LEN {
            items.push(omission_marker(self.searches.len() - MAX_LOGGED_ARRAY_LEN));
        }
        Value::Array(items)
    }

    fn request_name(&self) -> &'static str {
        "search"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.to_log_value().to_string().hash(&mut hasher);
        hasher.finish()
    }

    fn to_fingerprint_value(&self) -> Value {
        normalized_batch_value(self.searches.iter(), |search| {
            let mut value = core_search_value(search);
            normalize(&mut value);
            value
        })
    }
}

impl Loggable for FacetParams {
    fn to_log_value(&self) -> Value {
        bounded_log_value(self)
    }

    fn to_fingerprint_value(&self) -> Value {
        normalized_value(self)
    }

    fn request_name(&self) -> &'static str {
        "facet"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for CountRequestInternal {
    fn to_log_value(&self) -> Value {
        bounded_log_value(self)
    }

    fn to_fingerprint_value(&self) -> Value {
        normalized_value(self)
    }

    fn request_name(&self) -> &'static str {
        "count"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Loggable for PointRequestInternal {
    fn to_log_value(&self) -> Value {
        bounded_log_value(self)
    }

    fn to_fingerprint_value(&self) -> Value {
        normalized_value(self)
    }

    fn request_name(&self) -> &'static str {
        "retrieve"
    }

    fn request_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.request_name().hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use segment::types::{Condition, Filter, HasIdCondition, WithPayloadInterface, WithVector};
    use serde_json::json;

    use super::*;

    #[test]
    fn test_truncate_long_arrays() {
        let mut value = json!({
            "short": [1, 2, 3],
            "long": (0..MAX_LOGGED_ARRAY_LEN + 100).collect::<Vec<_>>(),
            "nested": {
                "long": [(0..MAX_LOGGED_ARRAY_LEN + 1).collect::<Vec<_>>()],
            },
        });

        truncate_long_arrays(&mut value);

        assert_eq!(value["short"], json!([1, 2, 3]));

        let long = value["long"].as_array().unwrap();
        assert_eq!(long.len(), MAX_LOGGED_ARRAY_LEN + 1);
        assert_eq!(long[MAX_LOGGED_ARRAY_LEN], json!("... (100 more items)"));

        let nested = value["nested"]["long"][0].as_array().unwrap();
        assert_eq!(nested.len(), MAX_LOGGED_ARRAY_LEN + 1);
        assert_eq!(nested[MAX_LOGGED_ARRAY_LEN], json!("... (1 more items)"));
    }

    #[test]
    fn test_query_batch_log_value_is_bounded() {
        // Mimics an internally generated distance matrix batch: many queries,
        // each carrying a has_id filter with all sampled point ids.
        let ids: ahash::AHashSet<_> = (0..MAX_LOGGED_ARRAY_LEN as u64 + 36)
            .map(segment::types::PointIdType::from)
            .collect();
        let filter = Filter::new_must(Condition::HasId(HasIdCondition::from(ids)));

        let request = ShardQueryRequest {
            prefetches: vec![],
            query: None,
            filter: Some(filter),
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(false),
            with_payload: WithPayloadInterface::Bool(false),
        };
        let batch = vec![request; MAX_LOGGED_ARRAY_LEN + 100];

        let value = batch.to_log_value();

        let queries = value.as_array().unwrap();
        assert_eq!(queries.len(), MAX_LOGGED_ARRAY_LEN + 1);
        assert_eq!(queries[MAX_LOGGED_ARRAY_LEN], json!("... (100 more items)"));

        let has_id = queries[0]["filter"]["must"][0]["has_id"]
            .as_array()
            .unwrap();
        assert_eq!(has_id.len(), MAX_LOGGED_ARRAY_LEN + 1);
        assert_eq!(has_id[MAX_LOGGED_ARRAY_LEN], json!("... (36 more items)"));
    }
}
