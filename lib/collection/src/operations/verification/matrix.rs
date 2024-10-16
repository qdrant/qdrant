use api::rest::SearchMatrixRequestInternal;

use super::StrictModeVerification;
use crate::collection::distance_matrix::CollectionSearchMatrixRequest;

impl StrictModeVerification for SearchMatrixRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        self.limit
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for CollectionSearchMatrixRequest {
    fn query_limit(&self) -> Option<usize> {
        Some(self.limit_per_sample)
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
