use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{Filter, StrictModeConfig};

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
        filter: Option<&Filter>,
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

                    let vector_hnsw_m = vector_hnsw_config
                        .map(|hnsw_config| hnsw_config.m)
                        .flatten()
                        .unwrap_or(config.hnsw_config.m);

                    let vector_hnsw_payload_m = vector_hnsw_config
                        .map(|hnsw_config| hnsw_config.payload_m)
                        .flatten()
                        .unwrap_or(config.hnsw_config.payload_m.unwrap_or(vector_hnsw_m));

                    // no further check necessary if there is a global HNSW index
                    if vector_hnsw_m > 0 {
                        return Ok(());
                    }

                    // specialized error message if not default vector
                    let vector_error_label = if using == DEFAULT_VECTOR_NAME {
                        ""
                    } else {
                        &format!(" on '{using}'")
                    };

                    // check hnsw.payload_m if there is a filter
                    let uses_multitenant_filter = if let Some(filter) = filter {
                        filter
                            .iter_conditions()
                            .filter_map(|c| c.targeted_key())
                            .filter_map(|key| collection.payload_key_index_schema(&key))
                            .any(|index_schema| index_schema.is_tenant())
                    } else {
                        false
                    };

                    if !uses_multitenant_filter {
                        // HNSW disabled AND no filters
                        return Err(CollectionError::strict_mode(
                            format!(
                                "Request is forbidden{vector_error_label} because global vector indexing is disabled (hnsw_config.m = 0)"
                            ),
                            "Use tenant-specific filter, enable global vector indexing or enable strict mode `search_allow_exact` option",
                        ));
                    }

                    if vector_hnsw_payload_m == 0 {
                        // HNSW disabled AND no filters
                        return Err(CollectionError::strict_mode(
                            format!(
                                "Request is forbidden{vector_error_label} because vector indexing is disabled (hnsw_config.m = 0 and hnsw_config.payload_m = 0)"
                            ),
                            "Enable vector indexing, use a prefetch query with indexed vectors or enable strict mode `search_allow_exact` option",
                        ));
                    }

                    return Ok(());
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
                    .check_fullscan(
                        &self.using,
                        self.filter.as_ref(),
                        collection,
                        strict_mode_config,
                    )
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
                .check_fullscan(
                    &self.using,
                    self.filter.as_ref(),
                    collection,
                    strict_mode_config,
                )
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
                    .check_fullscan(
                        &self.using,
                        self.filter.as_ref(),
                        collection,
                        strict_mode_config,
                    )
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
