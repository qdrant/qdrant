//! Fluent builder for [`SearchMatrixRequest`].
//!
//! Builder fields mirror [`SearchMatrixRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::types::{Filter, VectorNameBuf};

use crate::requests::matrix::SearchMatrixRequest;

/// Fluent builder for [`SearchMatrixRequest`].
///
/// `sample_size`, `limit_per_sample` and `using` are required and passed through
/// [`Self::new`]; every other field is optional and falls back to the
/// [`SearchMatrixRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct SearchMatrixRequestBuilder {
    sample_size: usize,
    limit_per_sample: usize,
    filter: Option<Filter>,
    using: VectorNameBuf,
}

impl SearchMatrixRequestBuilder {
    pub fn new(sample_size: usize, limit_per_sample: usize, using: VectorNameBuf) -> Self {
        let SearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = SearchMatrixRequest::new(sample_size, limit_per_sample, using);
        Self {
            sample_size,
            limit_per_sample,
            filter,
            using,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn build(self) -> SearchMatrixRequest {
        // Exhaustively destructure Self and construct SearchMatrixRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = self;
        SearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        }
    }
}
