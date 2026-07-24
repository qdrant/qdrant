use collection::operations::point_ops::{
    BatchVectorStructPersisted, PointInsertOperationsInternal, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::types::{VectorParams, VectorsConfig};
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use storage::content_manager::errors::StorageError;

fn get_vector_params<'a>(
    vectors_config: &'a VectorsConfig,
    vector_name: &str,
) -> Result<&'a VectorParams, StorageError> {
    vectors_config.get_params(vector_name).ok_or_else(|| {
        StorageError::bad_input(format!("Not existing vector name error: {}", vector_name))
    })
}

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
                let params = get_vector_params(vectors_config, name)?;
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
                let params = get_vector_params(vectors_config, name)?;
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
