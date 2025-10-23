use segment::types::Filter;

use super::StrictModeVerification;
use crate::operations::types::{
    AverageVectorRequest, RecommendGroupsRequestInternal, RecommendRequestInternal,
};

impl StrictModeVerification for RecommendRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        Some(self.limit)
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        self.params.as_ref()
    }
}

impl StrictModeVerification for RecommendGroupsRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        Some(self.group_request.limit as usize * self.group_request.group_size as usize)
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        self.params.as_ref()
    }
}

impl StrictModeVerification for AverageVectorRequest {
    fn query_limit(&self) -> Option<usize> {
        None // No limit for average vector computation
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None // No filtering for average vector
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
