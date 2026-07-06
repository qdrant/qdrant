use segment::data_types::idf_estimate::IdfEstimateParams;
use segment::types::{Filter, SearchParams};

use super::StrictModeVerification;

impl StrictModeVerification for IdfEstimateParams {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    // The corpus filter is evaluated like a read filter: unindexed fields
    // are rejected and filter limits apply.
    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.corpus.as_ref()
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
