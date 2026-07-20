use segment::json_path::JsonPath;
use segment::types::Filter;
use shard::facet::FacetRequestInternal;

/// Facet request — counts the number of points for each unique value of a payload key.
#[derive(Clone, Debug, PartialEq)]
pub struct FacetRequest {
    /// Payload key to facet on.
    pub key: JsonPath,
    /// Maximum number of facet hits to return. Default: 10.
    pub limit: usize,
    /// Look only for points which satisfy these conditions.
    pub filter: Option<Filter>,
    /// If true, count the exact number of points for each value.
    /// If false, count the approximate number of points faster. Default: false.
    pub exact: bool,
}

impl FacetRequest {
    pub fn new(key: JsonPath) -> Self {
        Self {
            key,
            limit: FacetRequestInternal::DEFAULT_LIMIT,
            filter: None,
            exact: false,
        }
    }
}
