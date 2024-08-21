use tokio::sync::RwLockReadGuard;
use tonic::async_trait;

use super::{check_bool, check_limit, check_limit_opt, new_error_msg, StrictModeVerification};
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

        check_limit(
            search_request.limit,
            strict_mode_config.max_filter_limit,
            "limit",
        )?;

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

        if strict_mode_config.unindexed_filtering_retrieve == Some(false) {
            if let Some(filter) = &search_request.filter {
                if let Some(key) = collection.has_filter_without_index(filter) {
                    return Err(new_error_msg(
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
