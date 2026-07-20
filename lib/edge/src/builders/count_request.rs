//! Fluent builder for [`CountRequest`].
//!
//! Builder fields mirror [`CountRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::types::Filter;

use crate::requests::count::CountRequest;

/// Fluent builder for [`CountRequest`].
///
/// Every field is optional and falls back to the [`CountRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct CountRequestBuilder {
    filter: Option<Filter>,
    exact: bool,
}

impl CountRequestBuilder {
    pub fn new() -> Self {
        let CountRequest { filter, exact } = CountRequest::new();
        Self { filter, exact }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn exact(mut self, exact: bool) -> Self {
        self.exact = exact;
        self
    }

    pub fn build(self) -> CountRequest {
        // Exhaustively destructure Self and construct CountRequest:
        // adding a field to either type forces a compile error here.
        let Self { filter, exact } = self;
        CountRequest { filter, exact }
    }
}

impl Default for CountRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}
