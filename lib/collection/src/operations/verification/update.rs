use segment::types::Filter;

use super::StrictModeVerification;
use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointsSelector;
use crate::operations::vector_ops::DeleteVectors;

impl StrictModeVerification for PointsSelector {
    fn request_indexed_filter_write(&self) -> Option<&Filter> {
        if let PointsSelector::FilterSelector(filter) = self {
            return Some(&filter.filter);
        }

        None
    }

    fn request_query_limit(&self) -> Option<usize> {
        None
    }

    fn request_timeout(&self) -> Option<usize> {
        None
    }

    fn request_indexed_filter_read(&self) -> Option<&Filter> {
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
    fn request_query_limit(&self) -> Option<usize> {
        None
    }

    fn request_timeout(&self) -> Option<usize> {
        None
    }

    fn request_indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_indexed_filter_write(&self) -> Option<&Filter> {
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
    fn request_indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn request_query_limit(&self) -> Option<usize> {
        None
    }

    fn request_timeout(&self) -> Option<usize> {
        None
    }

    fn request_indexed_filter_read(&self) -> Option<&Filter> {
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
    fn request_indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn request_query_limit(&self) -> Option<usize> {
        None
    }

    fn request_timeout(&self) -> Option<usize> {
        None
    }

    fn request_indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}
