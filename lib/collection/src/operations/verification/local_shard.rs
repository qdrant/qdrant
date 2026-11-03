use shard::scroll::ScrollRequestInternal;

use super::{ReadScope, StrictModeVerification};
use crate::operations::types::PointRequestInternal;

impl StrictModeVerification for ScrollRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        Some(
            self.limit
                .unwrap_or_else(ScrollRequestInternal::default_limit),
        )
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.filter.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn read_scope(&self) -> ReadScope<'_> {
        ReadScope::Filtered(self.filter.as_ref())
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for PointRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn read_scope(&self) -> ReadScope<'_> {
        ReadScope::NotApplicable
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
