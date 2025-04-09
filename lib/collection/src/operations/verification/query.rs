use segment::types::StrictModeConfig;

use super::StrictModeVerification;
use crate::collection::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
};

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
                        "Create an index for this key or use a different formula expression.",
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
        // CollectionPrefetch.prefetch is of type CollectionPrefetch (recursive type)
        for prefetch in &self.prefetch {
            Box::pin(prefetch.check_strict_mode(collection, strict_mode_config)).await?;
        }

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

impl StrictModeVerification for CollectionPrefetch {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), crate::operations::types::CollectionError> {
        // CollectionPrefetch.prefetch is of type CollectionPrefetch (recursive type)
        for prefetch in &self.prefetch {
            Box::pin(prefetch.check_strict_mode(collection, strict_mode_config)).await?;
        }

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
        Some(self.limit * self.group_size)
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
