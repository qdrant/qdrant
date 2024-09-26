use segment::types::Filter;

use super::StrictModeVerification;
use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointsSelector;
use crate::operations::vector_ops::DeleteVectors;

impl StrictModeVerification for PointsSelector {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        match self {
            PointsSelector::FilterSelector(filter) => Some(&filter.filter),
            PointsSelector::PointIdsSelector(_) => None,
        }
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeleteVectors {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for SetPayload {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeletePayload {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
