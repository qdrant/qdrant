use api::rest::{Prefetch, QueryGroupsRequestInternal, QueryRequestInternal};
use segment::types::StrictModeConfig;

use super::StrictModeVerification;
use crate::collection::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::collection_query::{
    CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
};

impl StrictModeVerification for QueryRequestInternal {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), crate::operations::types::CollectionError> {
        if let Some(prefetch) = &self.prefetch {
            for prefetch in prefetch {
                prefetch
                    .check_strict_mode(collection, strict_mode_config)
                    .await?;
            }
        }

        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        self.limit
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
        self.params.as_ref()
    }
}

impl StrictModeVerification for Prefetch {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), crate::operations::types::CollectionError> {
        // Prefetch.prefetch is of type Prefetch (recursive type)
        if let Some(prefetch) = &self.prefetch {
            for prefetch in prefetch {
                Box::pin(prefetch.check_strict_mode(collection, strict_mode_config)).await?;
            }
        }

        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        self.limit
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
        self.params.as_ref()
    }
}

impl StrictModeVerification for QueryGroupsRequestInternal {
    fn query_limit(&self) -> Option<usize> {
        self.group_request.limit
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
        self.params.as_ref()
    }
}

impl Query {
    fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        if strict_mode_config.unindexed_filtering_retrieve == Some(false) {
            if let Query::Formula(formula) = self {
                if let Some((key, schemas)) =
                    collection.one_unindexed_expression_key(&formula.formula)
                {
                    let possible_schemas_str = schemas
                        .iter()
                        .map(|schema| schema.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");

                    return Err(CollectionError::strict_mode(
                        format!(
                            "Index required but not found for \"{key}\" of one of the following types: [{possible_schemas_str}]",
                        ),
                        "Create an index for this key or use a different expression.",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl StrictModeVerification for CollectionQueryRequest {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        if let Some(query) = self.query.as_ref() {
            // check for unindexed fields in formula
            query.check_strict_mode(collection, strict_mode_config)?
        }
        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        Some(self.limit)
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
        self.params.as_ref()
    }
}

impl StrictModeVerification for CollectionQueryGroupsRequest {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        if let Some(query) = self.query.as_ref() {
            // check for unindexed fields in formula
            query.check_strict_mode(collection, strict_mode_config)?
        }
        Ok(())
    }

    fn query_limit(&self) -> Option<usize> {
        Some(self.limit)
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
        self.params.as_ref()
    }
}
