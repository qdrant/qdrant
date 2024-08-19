use super::{new_error, StrictModeVerification};
use crate::operations::config_diff::StrictModeConfigDiff;
use crate::operations::types::{SearchRequest, SearchRequestBatch};

impl StrictModeVerification for SearchRequest {
    fn check_strict_mode(&self, strict_mode_config: &StrictModeConfigDiff) -> Result<(), String> {
        if let Some(filter_limit) = strict_mode_config.max_filter_limit {
            if self.search_request.filter.is_some() {
                if filter_limit > self.search_request.limit {
                    return Err(new_error(
                        format!("Max search filter limit of {filter_limit} exceeded"),
                        "Reduce the search limit",
                    ));
                }
            }
        }

        Ok(())
    }
}

impl StrictModeVerification for SearchRequestBatch {
    fn check_strict_mode(&self, strict_mode_config: &StrictModeConfigDiff) -> Result<(), String> {
        for search_request in &self.searches {
            search_request.check_strict_mode(&strict_mode_config)?;
        }

        Ok(())
    }
}
