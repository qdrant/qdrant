use api::rest::{
    BatchVectorStruct, MultiDenseVector, PointInsertOperations, UpdateVectors, Vector, VectorStruct,
};
use segment::data_types::tiny_map::TinyMap;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{Filter, StrictModeConfig, StrictModeMultivectorConfig};

use super::{check_limit_opt, StrictModeVerification};
use crate::collection::Collection;
use crate::common::collection_size_stats::CollectionSizeAtomicStats;
use crate::operations::payload_ops::{DeletePayload, SetPayload};
use crate::operations::point_ops::PointsSelector;
use crate::operations::types::CollectionError;
use crate::operations::vector_ops::DeleteVectors;

impl StrictModeVerification for PointsSelector {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        match self {
            PointsSelector::FilterSelector(filter) => Some(&filter.filter),
            PointsSelector::PointIdsSelector(_) => None,
        }
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeleteVectors {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for SetPayload {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        if let Some(payload_size_limit_bytes) = strict_mode_config.max_collection_payload_size_bytes
        {
            if let Some(local_stats) = collection.estimated_collection_stats().await {
                check_collection_payload_size_limit(payload_size_limit_bytes, local_stats)?;
            }
        }

        Ok(())
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeletePayload {
    fn indexed_filter_write(&self) -> Option<&Filter> {
        self.filter.as_ref()
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for PointInsertOperations {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_limit_opt(
            Some(self.len()),
            strict_mode_config.upsert_max_batchsize,
            "upsert limit",
        )?;

        check_collection_size_limit(collection, strict_mode_config).await?;

        if let Some(multivector_config) = &strict_mode_config.multivector_config {
            check_multivectors_limits_insert(self, multivector_config).await?;
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

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for UpdateVectors {
    async fn check_custom(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> Result<(), CollectionError> {
        check_limit_opt(
            Some(self.points.len()),
            strict_mode_config.upsert_max_batchsize,
            "update limit",
        )?;

        check_collection_size_limit(collection, strict_mode_config).await?;

        if let Some(multivector_config) = &strict_mode_config.multivector_config {
            check_multivectors_limits_update(self, multivector_config).await?;
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

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

/// Checks all collection size limits that are configured in strict mode.
async fn check_collection_size_limit(
    collection: &Collection,
    strict_mode_config: &StrictModeConfig,
) -> Result<(), CollectionError> {
    let vector_limit = strict_mode_config.max_collection_vector_size_bytes;
    let payload_limit = strict_mode_config.max_collection_payload_size_bytes;

    // If all configs are disabled/unset, don't need to check anything nor update cache for performance.
    if (vector_limit, payload_limit) == (None, None) {
        return Ok(());
    }

    let Some(stats) = collection.estimated_collection_stats().await else {
        return Ok(());
    };

    if let Some(vector_storage_size_limit_bytes) = vector_limit {
        check_collection_vector_size_limit(vector_storage_size_limit_bytes, stats)?;
    }

    if let Some(payload_storage_size_limit_bytes) = payload_limit {
        check_collection_payload_size_limit(payload_storage_size_limit_bytes, stats)?;
    }

    Ok(())
}

/// Check collections vector storage size limit.
fn check_collection_vector_size_limit(
    max_vec_storage_size_bytes: usize,
    stats: &CollectionSizeAtomicStats,
) -> Result<(), CollectionError> {
    let vec_storage_size_bytes = stats.get_vector_storage_size();

    if vec_storage_size_bytes >= max_vec_storage_size_bytes {
        let size_in_mb = max_vec_storage_size_bytes as f32 / (1024.0 * 1024.0);
        return Err(CollectionError::bad_request(format!(
            "Max vector storage size limit of {size_in_mb}MB reached!",
        )));
    }

    Ok(())
}

/// Check collections payload storage size limit.
fn check_collection_payload_size_limit(
    max_payload_storage_size_bytes: usize,
    stats: &CollectionSizeAtomicStats,
) -> Result<(), CollectionError> {
    let payload_storage_size_bytes = stats.get_payload_storage_size();

    if payload_storage_size_bytes >= max_payload_storage_size_bytes {
        let size_in_mb = max_payload_storage_size_bytes as f32 / (1024.0 * 1024.0);
        return Err(CollectionError::bad_request(format!(
            "Max payload storage size limit of {size_in_mb}MB reached!",
        )));
    }

    Ok(())
}

/// Compute a non-empty mapping of multivector limits by name.
///
/// Uses a tiny map as we expect a small number of multivectors to be configured per collection in strict mode.
///
/// Return None if no multivectors are configured with strict mode
async fn multivector_limits_by_name(
    multivector_strict_config: &StrictModeMultivectorConfig,
) -> Option<TinyMap<String, usize>> {
    // If no multivectors strict mode no need to check anything.
    if multivector_strict_config.config.is_empty() {
        return None;
    }

    let multivector_max_size_by_name: TinyMap<String, usize> = multivector_strict_config
        .config
        .iter()
        .filter_map(|(name, config)| {
            config
                .max_vectors
                .map(|max_vectors| (name.clone(), max_vectors))
        })
        .collect();

    // If no multivectors are configured, no need to check anything.
    if multivector_max_size_by_name.is_empty() {
        None
    } else {
        Some(multivector_max_size_by_name)
    }
}

async fn check_multivectors_limits_update(
    point_insert: &UpdateVectors,
    multivector_strict_config: &StrictModeMultivectorConfig,
) -> Result<(), CollectionError> {
    let Some(multivector_max_size_by_name) =
        multivector_limits_by_name(multivector_strict_config).await
    else {
        return Ok(());
    };

    for point in &point_insert.points {
        check_named_multivectors_vecstruct_limit(
            DEFAULT_VECTOR_NAME,
            &point.vector,
            &multivector_max_size_by_name,
        )?;
    }

    Ok(())
}

async fn check_multivectors_limits_insert(
    point_insert: &PointInsertOperations,
    multivector_strict_config: &StrictModeMultivectorConfig,
) -> Result<(), CollectionError> {
    let Some(multivector_max_size_by_name) =
        multivector_limits_by_name(multivector_strict_config).await
    else {
        return Ok(());
    };

    match point_insert {
        PointInsertOperations::PointsBatch(batch) => match &batch.batch.vectors {
            BatchVectorStruct::MultiDense(multis) => {
                for multi in multis {
                    check_named_multivector_limit(
                        DEFAULT_VECTOR_NAME,
                        multi,
                        &multivector_max_size_by_name,
                    )?;
                }
            }
            BatchVectorStruct::Named(named_batch_vectors) => {
                for (name, vectors) in named_batch_vectors {
                    for vector in vectors {
                        check_named_multivectors_vec_limit(
                            name,
                            vector,
                            &multivector_max_size_by_name,
                        )?;
                    }
                }
            }
            BatchVectorStruct::Single(_)
            | BatchVectorStruct::Document(_)
            | BatchVectorStruct::Image(_)
            | BatchVectorStruct::Object(_) => {}
        },
        PointInsertOperations::PointsList(list) => {
            for point_struct in &list.points {
                match &point_struct.vector {
                    VectorStruct::MultiDense(multi) => {
                        check_named_multivector_limit(
                            DEFAULT_VECTOR_NAME,
                            multi,
                            &multivector_max_size_by_name,
                        )?;
                    }
                    VectorStruct::Named(named_vectors) => {
                        for (name, vector) in named_vectors {
                            check_named_multivectors_vec_limit(
                                name,
                                vector,
                                &multivector_max_size_by_name,
                            )?;
                        }
                    }
                    VectorStruct::Single(_)
                    | VectorStruct::Document(_)
                    | VectorStruct::Image(_)
                    | VectorStruct::Object(_) => {}
                }
            }
        }
    }

    Ok(())
}

fn check_named_multivectors_vecstruct_limit(
    name: &str,
    vector: &VectorStruct,
    multivector_max_size_by_name: &TinyMap<String, usize>,
) -> Result<(), CollectionError> {
    match vector {
        VectorStruct::MultiDense(multi) => {
            check_named_multivector_limit(name, multi, multivector_max_size_by_name)
        }
        VectorStruct::Named(named) => {
            for (name, vec) in named {
                check_named_multivectors_vec_limit(name, vec, multivector_max_size_by_name)?;
            }
            Ok(())
        }
        VectorStruct::Single(_)
        | VectorStruct::Document(_)
        | VectorStruct::Image(_)
        | VectorStruct::Object(_) => Ok(()),
    }
}

fn check_named_multivectors_vec_limit(
    name: &str,
    vector: &Vector,
    multivector_max_size_by_name: &TinyMap<String, usize>,
) -> Result<(), CollectionError> {
    match vector {
        Vector::MultiDense(multi) => {
            check_named_multivector_limit(name, multi, multivector_max_size_by_name)
        }
        Vector::Dense(_)
        | Vector::Sparse(_)
        | Vector::Document(_)
        | Vector::Image(_)
        | Vector::Object(_) => Ok(()),
    }
}

fn check_named_multivector_limit(
    name: &str,
    multi: &MultiDenseVector,
    multivector_max_size_by_name: &TinyMap<String, usize>,
) -> Result<(), CollectionError> {
    if let Some(strict_multi_limit) = multivector_max_size_by_name.get(name) {
        check_multivector_limit(name, multi, *strict_multi_limit)?
    }
    Ok(())
}

fn check_multivector_limit(
    name: &str,
    multi: &MultiDenseVector,
    max_size: usize,
) -> Result<(), CollectionError> {
    let multi_len = multi.len();
    if multi_len > max_size {
        return Err(CollectionError::bad_request(format!(
            "Multivector '{name}' has a limit of {max_size} vectors, but {multi_len} were provided!",
        )));
    }

    Ok(())
}
