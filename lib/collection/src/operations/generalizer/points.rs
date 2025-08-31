use serde_json::Value;
use crate::operations::generalizer::placeholders::size_value_placeholder;
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::types::{PointRequestInternal, ScrollRequestInternal};

impl Generalizer for ScrollRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let ScrollRequestInternal {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = self;

        let mut result = serde_json::json!({
            "with_payload": with_payload,
            "with_vector": with_vector,
            "order_by": order_by,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        // Offset is a point id; do not expose exact values
        if offset.is_some() {
            result["offset"] = serde_json::json!({"type": "point_id"});
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

impl Generalizer for PointRequestInternal {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let PointRequestInternal {
            ids,
            with_payload,
            with_vector,
        } = self;

        let mut result = serde_json::json!({
            "with_payload": with_payload,
            "with_vector": with_vector,
        });

        match level {
            GeneralizationLevel::OnlyVector => {
                let ids_repr: Vec<_> = ids.iter().map(|_| serde_json::json!({"type": "point_id"})).collect();
                result["ids"] = serde_json::Value::Array(ids_repr);
            }
            GeneralizationLevel::VectorAndValues => {
                result["num_ids"] = size_value_placeholder(ids.len());
            }
        }

        result
    }
}