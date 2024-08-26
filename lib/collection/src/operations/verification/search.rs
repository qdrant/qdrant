use segment::types::Filter;

use super::{check_bool, check_limit_opt, StrictModeVerification};
use crate::collection::Collection;
use crate::operations::config_diff::StrictModeConfig;
use crate::operations::types::{CollectionError, SearchRequest, SearchRequestBatch};

impl StrictModeVerification for SearchRequest {
    fn check_custom(
        &self,
        _: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        let search_request = &self.search_request;

        if let Some(search_parameter) = &search_request.params {
            check_bool(
                search_parameter.exact,
                strict_mode_config.search_allow_exact,
                "Exact search",
                "exact",
            )?;

            check_limit_opt(
                search_parameter.quantization.and_then(|i| i.oversampling),
                strict_mode_config.search_max_oversampling,
                "oversampling",
            )?;

            check_limit_opt(
                search_parameter.hnsw_ef,
                strict_mode_config.search_max_hnsw_ef,
                "hnsw_ef",
            )?;
        }

        Ok(())
    }

    fn request_indexed_filter_read(&self) -> Option<&Filter> {
        self.search_request.filter.as_ref()
    }

    fn request_limit(&self) -> Option<usize> {
        Some(self.search_request.limit)
    }
}

impl StrictModeVerification for SearchRequestBatch {
    fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        for search_request in &self.searches {
            search_request.check_strict_mode(collection, strict_mode_config)?;
        }
        Ok(())
    }
}
