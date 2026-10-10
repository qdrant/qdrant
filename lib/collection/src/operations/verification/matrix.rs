use api::rest::SearchMatrixRequestInternal;

use super::StrictModeVerification;
use crate::collection::distance_matrix::CollectionSearchMatrixRequest;

impl StrictModeVerification for SearchMatrixRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        match (self.limit, self.sample) {
            (Some(limit), Some(sample)) => Some(limit.saturating_mul(sample)),
            (Some(limit), None) => Some(limit),
            (None, Some(sample)) => Some(sample),
            (None, None) => None,
        }
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
        Some(self.limit_per_sample.saturating_mul(self.sample_size))
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rest_matrix_query_limit_saturates_on_overflow() {
        // (1 << 32) * (1 << 32) wraps to 0 without saturation
        let request = SearchMatrixRequestInternal {
            limit: Some(1 << 32),
            sample: Some(1 << 32),
            filter: None,
            using: None,
        };
        assert_eq!(request.query_limit(), Some(usize::MAX));
    }

    #[test]
    fn collection_matrix_query_limit_saturates_on_overflow() {
        // (1 << 32) * (1 << 32) wraps to 0 without saturation
        let request = CollectionSearchMatrixRequest {
            sample_size: 1 << 32,
            limit_per_sample: 1 << 32,
            filter: None,
            using: Default::default(),
        };
        assert_eq!(request.query_limit(), Some(usize::MAX));
    }

    #[test]
    fn collection_matrix_query_limit_normal_product() {
        let request = CollectionSearchMatrixRequest {
            sample_size: 10,
            limit_per_sample: 3,
            filter: None,
            using: Default::default(),
        };
        assert_eq!(request.query_limit(), Some(30));
    }
}
