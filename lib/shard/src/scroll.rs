use schemars::JsonSchema;
use segment::data_types::order_by::OrderByInterface;
use segment::types::{Filter, PointIdType, WithPayloadInterface, WithVector};
use serde::{Deserialize, Serialize};
use validator::Validate;

fn deserialize_optional_limit<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    common::validation::deserialize_option_usize_field(deserializer, "limit", 1)
}

/// Scroll request - paginate over all points which matches given condition
#[derive(Clone, Debug, PartialEq, Hash, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct ScrollRequestInternal {
    /// Start ID to read points from.
    pub offset: Option<PointIdType>,

    /// Page size. Default: 10
    #[serde(default, deserialize_with = "deserialize_optional_limit")]
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Look only for points which satisfies this conditions. If not provided - all points.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,

    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,

    /// Order the records by a payload field.
    pub order_by: Option<OrderByInterface>,
}

impl Default for ScrollRequestInternal {
    fn default() -> Self {
        ScrollRequestInternal {
            offset: None,
            limit: Some(Self::default_limit()),
            filter: None,
            with_payload: Some(Self::default_with_payload()),
            with_vector: Self::default_with_vector(),
            order_by: None,
        }
    }
}

impl ScrollRequestInternal {
    pub const fn default_limit() -> usize {
        10
    }

    pub const fn default_with_payload() -> WithPayloadInterface {
        WithPayloadInterface::Bool(true)
    }

    pub const fn default_with_vector() -> WithVector {
        WithVector::Bool(false)
    }
}

#[cfg(test)]
mod serde_error_tests {
    use super::*;

    #[test]
    fn scroll_limit_deserialization_error_names_field() {
        let err = serde_json::from_str::<ScrollRequestInternal>(r#"{"limit":-1}"#)
            .unwrap_err()
            .to_string();

        assert!(err.contains("limit"), "{err}");
        assert!(!err.contains("usize"), "{err}");
    }
}
