//! Fluent builder for [`FacetRequest`].
//!
//! Builder fields mirror [`FacetRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::json_path::JsonPath;
use segment::types::Filter;

use crate::requests::facet::FacetRequest;

/// Fluent builder for [`FacetRequest`].
///
/// `key` is required and passed through [`Self::new`]; every other field is
/// optional and falls back to the [`FacetRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct FacetRequestBuilder {
    key: JsonPath,
    limit: usize,
    filter: Option<Filter>,
    exact: bool,
}

impl FacetRequestBuilder {
    pub fn new(key: JsonPath) -> Self {
        let FacetRequest {
            key,
            limit,
            filter,
            exact,
        } = FacetRequest::new(key);
        Self {
            key,
            limit,
            filter,
            exact,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn exact(mut self, exact: bool) -> Self {
        self.exact = exact;
        self
    }

    pub fn build(self) -> FacetRequest {
        // Exhaustively destructure Self and construct FacetRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            key,
            limit,
            filter,
            exact,
        } = self;
        FacetRequest {
            key,
            limit,
            filter,
            exact,
        }
    }
}
