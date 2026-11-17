use segment::types::{Filter, SearchParams};
use shard::count::CountRequestInternal;

use super::{ReadScope, StrictModeVerification};

impl StrictModeVerification for CountRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn read_scope(&self) -> ReadScope<'_> {
        ReadScope::Filtered(self.filter.as_ref())
    }

    fn request_exact(&self) -> Option<bool> {
        Some(self.exact)
    }

    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
}
