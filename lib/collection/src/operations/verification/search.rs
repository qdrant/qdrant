use tokio::sync::RwLockReadGuard;
use tonic::async_trait;

use super::{new_error, StrictModeVerification};
use crate::collection::Collection;
use crate::operations::config_diff::StrictModeConfigDiff;
use crate::operations::types::{SearchRequest, SearchRequestBatch};

#[async_trait]
impl StrictModeVerification for SearchRequest {
    async fn check_strict_mode(
        &self,
        collection: &RwLockReadGuard<'_, Collection>,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), String> {
        let search_request = &self.search_request;

        if let Some(filter_limit) = strict_mode_config.max_filter_limit {
            if filter_limit < search_request.limit {
                return Err(new_error(
                    format!(
                        "Search limit ({}) too high (Max = {filter_limit})",
                        search_request.limit
                    ),
                    "Reduce the search limit.",
                ));
            }
        }

        if let Some(search_parameter) = &search_request.params {
            if search_parameter.exact && strict_mode_config.search_allow_exact == Some(false) {
                return Err(new_error(
                    "Exact search disabled!",
                    "Set exact=false in your request.",
                ));
            }

            if let Some(oversampling_limit) = strict_mode_config.search_max_oversampling {
                if let Some(oversampling) = search_parameter
                    .quantization
                    .as_ref()
                    .and_then(|i| i.oversampling)
                {
                    if oversampling > oversampling_limit {
                        return Err(new_error(format!("Oversampling value ({oversampling}) too high (Max = {oversampling_limit})."), "Reduce the oversampling parameter."));
                    }
                }
            }

            if let Some(hnsw_ef_limit) = strict_mode_config.search_max_hnsw_ef {
                if let Some(hnsw_ef) = search_parameter.hnsw_ef {
                    if hnsw_ef > hnsw_ef_limit {
                        return Err(new_error(
                            format!(
                                "Hnsw_ef parameter ({hnsw_ef}) too large (Max = {hnsw_ef_limit})"
                            ),
                            "Decrease the hnsw_ef value.",
                        ));
                    }
                }
            }
        }

        if strict_mode_config.unindexed_filtering_retrieve == Some(false) {
            if let Some(filter) = &search_request.filter {
                if let Some(key) = collection.has_filter_without_index(filter) {
                    return Err(new_error(
                        format!("Index required but not found for \"{key}\""),
                        "Create an index or use a different filter.",
                    ));
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl StrictModeVerification for SearchRequestBatch {
    async fn check_strict_mode(
        &self,
        collection: &RwLockReadGuard<'_, Collection>,
        strict_mode_config: &StrictModeConfigDiff,
    ) -> Result<(), String> {
        for search_request in &self.searches {
            search_request
                .check_strict_mode(collection, strict_mode_config)
                .await?;
        }
        Ok(())
    }
}
