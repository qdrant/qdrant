use super::StrictModeVerification;
use crate::operations::types::RecommendRequest;

impl StrictModeVerification for RecommendRequest {
    fn request_query_limit(&self) -> Option<usize> {
        None
    }

    fn request_timeout(&self) -> Option<usize> {
        None
    }

    fn request_indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
