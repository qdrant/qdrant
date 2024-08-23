use segment::types::Filter;

use super::StrictModeVerification;
use crate::operations::types::CountRequest;

impl StrictModeVerification for CountRequest {
    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.count_request.filter.as_ref()
    }

    fn request_exact(&self) -> Option<bool> {
        Some(self.count_request.exact)
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn timeout(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
