use api::rest::SearchMatrixRequestInternal;

use super::StrictModeVerification;
use crate::collection::distance_matrix::CollectionSearchMatrixRequest;

impl StrictModeVerification for SearchMatrixRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        // Mirror `CollectionSearchMatrixRequest::from`: unset parameters fall back
        // to their defaults, so the effective query size always has a value.
        Some(
            self.limit
                .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_LIMIT_PER_SAMPLE)
                * self
                    .sample
                    .unwrap_or(CollectionSearchMatrixRequest::DEFAULT_SAMPLE),
        )
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
        Some(self.limit_per_sample * self.sample_size)
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
