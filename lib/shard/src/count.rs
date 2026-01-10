use schemars::JsonSchema;
use segment::types::Filter;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Count Request
/// Counts the number of points which satisfy the given filter.
/// If filter is not provided, the count of all points in the collection will be returned.
#[derive(Clone, Debug, PartialEq, Hash, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct CountRequestInternal {
    /// Look only for points which satisfies this conditions
    #[validate(nested)]
    pub filter: Option<Filter>,
    /// If true, count exact number of points. If false, count approximate number of points faster.
    /// Approximate count might be unreliable during the indexing process. Default: true
    #[serde(default = "CountRequestInternal::default_exact")]
    pub exact: bool,
}

impl CountRequestInternal {
    pub const fn default_exact() -> bool {
        true
    }
}
