use api::rest::FacetRequest;

use super::StrictModeVerification;

impl StrictModeVerification for FacetRequest {
    fn query_limit(&self) -> Option<usize> {
        self.facet_request.limit
    }

    fn timeout(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.facet_request.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        self.facet_request.exact
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
