use serde_json::Value;
use api::rest::FacetRequestInternal;
use crate::operations::generalizer::placeholders::size_value_placeholder;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

impl Generalizer for FacetRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let FacetRequestInternal {
            key,
            limit,
            filter,
            exact,
        } = self;

        let mut result = serde_json::json!({
            "key": key,
            "exact": exact,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                if let Some(limit) = limit {
                    result["limit"] = serde_json::json!(limit);
                }
            }
            GeneralizationLevel::VectorAndValues => {
                if let Some(limit) = limit {
                    result["limit"] = size_value_placeholder(*limit);
                }
            }
        }

        result
    }
}