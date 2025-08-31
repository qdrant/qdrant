use api::rest::SearchMatrixRequestInternal;
use serde_json::Value;

use crate::collection::distance_matrix::CollectionSearchMatrixRequest;
use crate::operations::generalizer::placeholders::size_value_placeholder;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

impl Generalizer for SearchMatrixRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let SearchMatrixRequestInternal {
            filter,
            sample,
            limit,
            using,
        } = self;

        let mut result = serde_json::json!({
            "using": using,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                if let Some(sample) = sample {
                    result["sample"] = serde_json::json!(sample);
                }
                if let Some(limit) = limit {
                    result["limit"] = serde_json::json!(limit);
                }
            }
            GeneralizationLevel::VectorAndValues => {
                if let Some(sample) = sample {
                    result["sample"] = size_value_placeholder(*sample);
                }
                if let Some(limit) = limit {
                    result["limit"] = size_value_placeholder(*limit);
                }
            }
        }

        result
    }
}

impl Generalizer for CollectionSearchMatrixRequest {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let CollectionSearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = self;

        let mut result = serde_json::json!({
            "using": using,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                result["sample_size"] = serde_json::json!(sample_size);
                result["limit_per_sample"] = serde_json::json!(limit_per_sample);
            }
            GeneralizationLevel::VectorAndValues => {
                result["sample_size"] = size_value_placeholder(*sample_size);
                result["limit_per_sample"] = size_value_placeholder(*limit_per_sample);
            }
        }

        result
    }
}
