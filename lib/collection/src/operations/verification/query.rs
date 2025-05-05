use segment::types::StrictModeConfig;

use super::{StrictModeVerification, check_grouping_field};
use crate::collection::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
};

impl Query {
    async fn check_strict_mode(
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

    /// Check that the query does not perform a fullscan based on the collection configuration.
    async fn check_fullscan(
        &self,
        using: &str,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        // Check only applies on `search_allow_exact`
        if strict_mode_config.search_allow_exact == Some(false) {
            match &self {
                Query::Fusion(_) | Query::OrderBy(_) | Query::Formula(_) | Query::Sample(_) => (),
                Query::Vector(_) => {
                    let config = collection.collection_config.read().await;

                    // ignore sparse vectors
                    let query_targets_sparse = config
                        .params
                        .sparse_vectors
                        .as_ref()
                        .is_some_and(|sparse| sparse.contains_key(using));
                    if query_targets_sparse {
                        // sparse vectors are always indexed
                        return Ok(());
                    }

                    // check HNSW configuration for vector
                    let vector_hnsw_config = &config
                        .params
                        .vectors
                        .get_params(using)
                        .and_then(|param| param.hnsw_config.as_ref());

                    let vector_hnsw_m = vector_hnsw_config.and_then(|hnsw| hnsw.m);
                    // TODO(strict-mode) check also payload_m if if there is a filter by tenant/principal
                    if vector_hnsw_m == Some(0) {
                        return Err(CollectionError::strict_mode(
                            format!(
                                "Fullscan forbidden on '{using}' – vector indexing is disabled (hnsw_config.m = 0)"
                            ),
                            "Enable vector indexing or use a prefetch query before rescoring",
                        ));
                    }
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
            prefetch
                .check_strict_mode(collection, strict_mode_config)
                .await?;
        }

        if let Some(query) = self.query.as_ref() {
            // check query can perform fullscan when not rescoring
            if self.prefetch.is_empty() {
                query
                    .check_fullscan(&self.using, collection, strict_mode_config)
                    .await?;
            }
            // check for unindexed fields in formula
            query
                .check_strict_mode(collection, strict_mode_config)
                .await?
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
    ) -> CollectionResult<()> {
        // CollectionPrefetch.prefetch is of type CollectionPrefetch (recursive type)
        for prefetch in &self.prefetch {
            Box::pin(prefetch.check_strict_mode(collection, strict_mode_config)).await?;
        }

        if let Some(query) = self.query.as_ref() {
            // check if prefetch can perform a fullscan
            query
                .check_fullscan(&self.using, collection, strict_mode_config)
                .await?;
            // check for unindexed fields in formula
            query
                .check_strict_mode(collection, strict_mode_config)
                .await?
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
            // check query can perform fullscan when not rescoring
            if self.prefetch.is_empty() {
                query
                    .check_fullscan(&self.using, collection, strict_mode_config)
                    .await?;
            }
            // check for unindexed fields in formula
            query
                .check_strict_mode(collection, strict_mode_config)
                .await?
        }
        // check for unindexed fields targeted by group_by
        check_grouping_field(&self.group_by, collection, strict_mode_config)?;
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
