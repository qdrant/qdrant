use api::rest::FacetRequestInternal;
use segment::data_types::facets::FacetParams;
use segment::types::{Filter, SearchParams};

use super::StrictModeVerification;

impl StrictModeVerification for FacetRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        self.limit
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        self.exact
    }

    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
}

impl StrictModeVerification for FacetParams {
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

    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
}
