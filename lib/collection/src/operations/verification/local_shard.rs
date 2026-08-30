use shard::scroll::ScrollRequestInternal;

use super::StrictModeVerification;
use crate::operations::types::PointRequestInternal;

impl StrictModeVerification for ScrollRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        // The strict-mode cap must be applied against the limit that
        // will actually be used, not the raw `Option`. If the caller
        // omits `limit`, `scroll_by` resolves the default (10) *after*
        // `check_request_query_limit` runs, so an omitted limit bypasses
        // the configured `max_query_limit` (qdrant/qdrant#10373).
        // `/points/query` resolves the default in the same way; mirror
        // it here so the cap is enforced for omitted limits too.
        Some(self.limit.unwrap_or_else(Self::default_limit))
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        self.filter.as_ref()
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

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
