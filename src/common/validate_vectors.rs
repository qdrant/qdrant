use collection::operations::point_ops::{
    BatchVectorStructPersisted, PointInsertOperationsInternal, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::types::{Datatype, VectorParams, VectorsConfig};
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{Distance, VectorStorageDatatype};
use shard::operations::vector_ops::PointVectorsPersisted;
use storage::content_manager::errors::StorageError;

/// Validate vector dimensions in an upsert operation against the collection config.
///
/// This performs an early O(n) check before the operation is written to WAL,
/// ensuring that dimension mismatches are reported even for async (wait=false) operations.
pub fn validate_vector_dimensions(
    operation: &PointInsertOperationsInternal,
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    match operation {
        PointInsertOperationsInternal::PointsBatch(batch) => {
            validate_batch_vectors(&batch.vectors, vectors_config)?;
        }
        PointInsertOperationsInternal::PointsList(points) => {
            for point in points {
                validate_point_vectors(&point.vector, vectors_config)?;
            }
        }
    }
    Ok(())
}

fn validate_batch_vectors(
    batch_vectors: &BatchVectorStructPersisted,
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    match batch_vectors {
        BatchVectorStructPersisted::Single(vectors) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Ok(());
            };
            let expected_dim = params.size.get() as usize;
            for vector in vectors {
                if vector.len() != expected_dim {
                    return Err(StorageError::bad_input(format!(
                        "Vector dimension error: expected dim: {expected_dim}, got {}",
                        vector.len()
                    )));
                }
            }
        }
        BatchVectorStructPersisted::MultiDense(vectors) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Ok(());
            };
            let expected_dim = params.size.get() as usize;
            for multi_vec in vectors {
                for vector in multi_vec {
                    if vector.len() != expected_dim {
                        return Err(StorageError::bad_input(format!(
                            "Vector dimension error: expected dim: {expected_dim}, got {}",
                            vector.len()
                        )));
                    }
                }
            }
        }
        BatchVectorStructPersisted::Named(named_vectors) => {
            for (name, vectors) in named_vectors {
                let Some(params) = vectors_config.get_params(name) else {
                    continue;
                };
                let expected_dim = params.size.get() as usize;
                for vector in vectors {
                    validate_single_vector_dim(vector, name, expected_dim)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_point_vectors(
    vector: &VectorStructPersisted,
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    match vector {
        VectorStructPersisted::Single(vec) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Ok(());
            };
            let expected_dim = params.size.get() as usize;
            if vec.len() != expected_dim {
                return Err(StorageError::bad_input(format!(
                    "Vector dimension error: expected dim: {expected_dim}, got {}",
                    vec.len()
                )));
            }
        }
        VectorStructPersisted::MultiDense(multi_vec) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Ok(());
            };
            let expected_dim = params.size.get() as usize;
            for vec in multi_vec {
                if vec.len() != expected_dim {
                    return Err(StorageError::bad_input(format!(
                        "Vector dimension error: expected dim: {expected_dim}, got {}",
                        vec.len()
                    )));
                }
            }
        }
        VectorStructPersisted::Named(named_vectors) => {
            for (name, vector) in named_vectors {
                let Some(params) = vectors_config.get_params(name) else {
                    continue;
                };
                let expected_dim = params.size.get() as usize;
                validate_single_vector_dim(vector, name, expected_dim)?;
            }
        }
    }
    Ok(())
}

fn validate_single_vector_dim(
    vector: &VectorPersisted,
    name: &str,
    expected_dim: usize,
) -> Result<(), StorageError> {
    match vector {
        VectorPersisted::Dense(vec) => {
            if vec.len() != expected_dim {
                return Err(StorageError::bad_input(format!(
                    "Vector dimension error: expected dim: {expected_dim}, got {} for vector '{name}'",
                    vec.len()
                )));
            }
        }
        VectorPersisted::MultiDense(multi_vec) => {
            for vec in multi_vec {
                if vec.len() != expected_dim {
                    return Err(StorageError::bad_input(format!(
                        "Vector dimension error: expected dim: {expected_dim}, got {} for vector '{name}'",
                        vec.len()
                    )));
                }
            }
        }
        VectorPersisted::Sparse(_) => {
            // Sparse vectors don't have a fixed dimension, skip validation
        }
    }
    Ok(())
}

/// Validate before WAL insertion so async writes report invalid components.
/// Raw cosine vectors need only a finiteness check; preprocessing normalizes
/// their finite components before the storage-range check.
pub fn validate_vector_values(
    operation: &PointInsertOperationsInternal,
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    match operation {
        PointInsertOperationsInternal::PointsBatch(batch) => match &batch.vectors {
            BatchVectorStructPersisted::Single(vectors) => {
                if let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) {
                    for vector in vectors {
                        check_values(vector, &params, DEFAULT_VECTOR_NAME)?;
                    }
                }
            }
            BatchVectorStructPersisted::MultiDense(vectors) => {
                if let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) {
                    for multi_vec in vectors {
                        for vector in multi_vec {
                            check_values(vector, &params, DEFAULT_VECTOR_NAME)?;
                        }
                    }
                }
            }
            BatchVectorStructPersisted::Named(named) => {
                for (name, vectors) in named {
                    if let Some(params) = vectors_config.get_params(name) {
                        for vector in vectors {
                            check_persisted(vector, &params, name)?;
                        }
                    }
                }
            }
        },
        PointInsertOperationsInternal::PointsList(points) => {
            for point in points {
                validate_point_vector(&point.vector, vectors_config)?;
            }
        }
    }
    Ok(())
}

fn validate_point_vector(
    vector: &VectorStructPersisted,
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    match vector {
        VectorStructPersisted::Single(vec) => {
            if let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) {
                check_values(vec, &params, DEFAULT_VECTOR_NAME)?;
            }
        }
        VectorStructPersisted::MultiDense(multi_vec) => {
            if let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) {
                for vec in multi_vec {
                    check_values(vec, &params, DEFAULT_VECTOR_NAME)?;
                }
            }
        }
        VectorStructPersisted::Named(named) => {
            for (name, vector) in named {
                if let Some(params) = vectors_config.get_params(name) {
                    check_persisted(vector, &params, name)?;
                }
            }
        }
    }
    Ok(())
}

/// Pre-WAL value check for the points/vectors update endpoint, so async
/// updates report invalid components the same way upserts do.
pub fn validate_update_vector_values(
    points: &[PointVectorsPersisted],
    vectors_config: &VectorsConfig,
) -> Result<(), StorageError> {
    for point in points {
        validate_point_vector(&point.vector, vectors_config)?;
    }
    Ok(())
}

fn check_persisted(
    vector: &VectorPersisted,
    params: &VectorParams,
    name: &str,
) -> Result<(), StorageError> {
    match vector {
        VectorPersisted::Dense(vec) => check_values(vec, params, name),
        VectorPersisted::MultiDense(multi_vec) => {
            for vec in multi_vec {
                check_values(vec, params, name)?;
            }
            Ok(())
        }
        VectorPersisted::Sparse(_) => Ok(()),
    }
}

fn check_values(vector: &[f32], params: &VectorParams, name: &str) -> Result<(), StorageError> {
    // Cosine preprocessing normalizes components into range, so only NaN and
    // infinity are unrepresentable there; other distances get the full check.
    let checked = match (params.datatype, params.distance) {
        (Some(Datatype::Float16), Distance::Cosine) => Some(Datatype::Float32),
        (datatype, _) => datatype,
    };
    if let Some(index) = segment::common::find_unrepresentable_component(
        vector,
        checked.map(VectorStorageDatatype::from),
    ) {
        let label = segment::common::datatype_label(
            params
                .datatype
                .map(VectorStorageDatatype::from)
                .unwrap_or_default(),
        );
        return Err(StorageError::bad_input(format!(
            "Vector value error: component at index {index} of vector '{name}' is not representable as a finite {label} value"
        )));
    }
    Ok(())
}
