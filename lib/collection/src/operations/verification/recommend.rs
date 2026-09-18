use segment::types::{Filter, StrictModeConfig};

use super::{StrictModeVerification, check_grouping_field};
use crate::collection::Collection;
use crate::operations::types::{
    CollectionResult, RecommendGroupsRequestInternal, RecommendRequestInternal,
};

impl StrictModeVerification for RecommendRequestInternal {
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

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        self.params.as_ref()
    }
}

impl StrictModeVerification for RecommendGroupsRequestInternal {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        // check for unindexed fields targeted by group_by
        check_grouping_field(&self.group_request.group_by, collection, strict_mode_config)?;
        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        Some(self.group_request.limit as usize * self.group_request.group_size as usize)
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

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        self.params.as_ref()
    }
}
