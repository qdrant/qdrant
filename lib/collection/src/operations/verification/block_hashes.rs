use segment::types::{Filter, SearchParams};

use super::StrictModeVerification;
use crate::operations::block_hashes::BlockHashesRequest;

impl StrictModeVerification for BlockHashesRequest {
    fn query_limit(&self) -> Option<usize> {
        Some(self.block_count as usize)
    }
    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }
    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }
    fn request_exact(&self) -> Option<bool> {
        Some(true)
    }
    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
}
