use crate::operations::generalizer::placeholders::size_value_placeholder;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::universal_query::collection_query::CollectionQueryGroupsRequest;

impl Generalizer for CollectionQueryGroupsRequest {
    fn generalize(&self, level: GeneralizationLevel) -> serde_json::Value {
        let CollectionQueryGroupsRequest {
            prefetch,
            query,
            using,
            filter,
            params,
            score_threshold,
            with_vector,
            with_payload,
            lookup_from,
            group_by,
            group_size,
            limit,
            with_lookup,
        } = self;

        let mut result = serde_json::json!({
            "using": using,
            "with_vector": with_vector,
            "with_payload": with_payload,
            "lookup_from": lookup_from,
            "params": params,
            "score_threshold": score_threshold,
            "group_by": group_by,
            "with_lookup": with_lookup,
        });

        let prefetch_values: Vec<_> = prefetch.iter().map(|p| p.generalize(level)).collect();
        if !prefetch_values.is_empty() {
            result["prefetch"] = serde_json::Value::Array(prefetch_values);
        }

        if let Some(query) = query {
            result["query"] = query.generalize(level);
        }

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                result["group_size"] = serde_json::json!(group_size);
                result["limit"] = serde_json::json!(limit);
            }
            GeneralizationLevel::VectorAndValues => {
                result["group_size"] = size_value_placeholder(*group_size);
                result["limit"] = size_value_placeholder(*limit);
            }
        }

        result
    }
}
