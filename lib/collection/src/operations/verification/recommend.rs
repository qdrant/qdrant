use super::StrictModeVerification;
use crate::operations::types::RecommendRequest;

impl StrictModeVerification for RecommendRequest {
    fn query_limit(&self) -> Option<usize> {
        Some(self.recommend_request.limit)
    }

    fn timeout(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.recommend_request.filter.as_ref()
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
