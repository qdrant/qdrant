use schemars::JsonSchema;
use segment::json_path::JsonPath;
use segment::types::Filter;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Facet Request
/// Counts the number of points for each unique value of a payload key.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct FacetRequestInternal {
    /// Payload key to facet on
    pub key: JsonPath,
    /// Maximum number of facet hits to return. Default: 10
    #[serde(default = "FacetRequestInternal::default_limit")]
    #[validate(range(min = 1))]
    pub limit: usize,
    /// Look only for points which satisfy these conditions
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// If true, count exact number of points for each value.
    /// If false, count approximate number of points faster.
    /// Default: false
    #[serde(default = "FacetRequestInternal::default_exact")]
    pub exact: bool,
}

impl FacetRequestInternal {
    pub const DEFAULT_LIMIT: usize = 10;

    pub const fn default_limit() -> usize {
        Self::DEFAULT_LIMIT
    }

    pub const fn default_exact() -> bool {
        false
    }
}
