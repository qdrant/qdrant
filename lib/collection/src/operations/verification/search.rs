use api::rest::{SearchGroupsRequestInternal, SearchRequestInternal};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{Filter, SearchParams, StrictModeConfig};

use super::{StrictModeVerification, check_grouping_field};
use crate::collection::Collection;
use crate::operations::types::{CollectionResult, CoreSearchRequest, SearchRequestBatch};

impl StrictModeVerification for SearchRequestInternal {
    fn indexed_filter_read(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        Some(self.limit)
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_search_params(&self) -> Option<&SearchParams> {
        self.params.as_ref()
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }
}

impl StrictModeVerification for CoreSearchRequest {
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
        self.params.as_ref()
    }
}

impl StrictModeVerification for SearchRequestBatch {
    async fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
        hw_acc: &HwMeasurementAcc,
    ) -> CollectionResult<()> {
        super::check_limit_opt(
            Some(self.searches.len()),
            strict_mode_config.search_max_batchsize,
            "search batch size",
        )?;

        for search_request in &self.searches {
            search_request
                .search_request
                .check_strict_mode(collection, strict_mode_config, hw_acc)
                .await?;
        }
        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
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

impl StrictModeVerification for SearchGroupsRequestInternal {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
        _hw_acc: &HwMeasurementAcc,
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

    fn request_exact(&self) -> Option<bool> {
        // We already check this in `request_search_params`
        None
    }

    fn request_search_params(&self) -> Option<&SearchParams> {
        self.params.as_ref()
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }
}
