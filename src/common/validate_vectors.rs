use std::collections::BTreeMap;

use collection::operations::point_ops::{
    BatchVectorStructPersisted, PointInsertOperationsInternal, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::types::{SparseVectorParams, VectorsConfig};
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::VectorNameBuf;
use storage::content_manager::errors::StorageError;

/// Validate vector dimensions in an upsert operation against the collection config.
///
/// This performs an early O(n) check before the operation is written to WAL,
/// ensuring that dimension mismatches are reported even for async (wait=false) operations.
pub fn validate_vector_dimensions(
    operation: &PointInsertOperationsInternal,
    vectors_config: &VectorsConfig,
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
) -> Result<(), StorageError> {
    match operation {
        PointInsertOperationsInternal::PointsBatch(batch) => {
            validate_batch_vectors(&batch.vectors, vectors_config, sparse_vectors_config)?;
        }
        PointInsertOperationsInternal::PointsList(points) => {
            for point in points {
                validate_point_vectors(&point.vector, vectors_config, sparse_vectors_config)?;
            }
        }
    }
    Ok(())
}

fn validate_batch_vectors(
    batch_vectors: &BatchVectorStructPersisted,
    vectors_config: &VectorsConfig,
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
) -> Result<(), StorageError> {
    match batch_vectors {
        BatchVectorStructPersisted::Single(vectors) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Err(unnamed_vector_error());
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
                return Err(unnamed_vector_error());
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
                for vector in vectors {
                    validate_named_vector(vector, name, vectors_config, sparse_vectors_config)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_point_vectors(
    vector: &VectorStructPersisted,
    vectors_config: &VectorsConfig,
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
) -> Result<(), StorageError> {
    match vector {
        VectorStructPersisted::Single(vec) => {
            let Some(params) = vectors_config.get_params(DEFAULT_VECTOR_NAME) else {
                return Err(unnamed_vector_error());
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
                return Err(unnamed_vector_error());
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
                validate_named_vector(vector, name, vectors_config, sparse_vectors_config)?;
            }
        }
    }
    Ok(())
}

fn unknown_vector_name_error(name: &str) -> StorageError {
    StorageError::bad_input(format!(
        "Vector name error: vector '{name}' does not exist in collection"
    ))
}

fn unnamed_vector_error() -> StorageError {
    StorageError::bad_input("Vector name error: unnamed vector does not exist in collection")
}

fn validate_named_vector(
    vector: &VectorPersisted,
    name: &VectorNameBuf,
    vectors_config: &VectorsConfig,
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
) -> Result<(), StorageError> {
    match vector {
        VectorPersisted::Dense(_) | VectorPersisted::MultiDense(_) => {
            let Some(params) = vectors_config.get_params(name) else {
                return Err(unknown_vector_name_error(name));
            };
            validate_single_vector_dim(vector, name, params.size.get() as usize)
        }
        VectorPersisted::Sparse(_) => {
            if sparse_vectors_config
                .map(|sparse_vectors| sparse_vectors.contains_key(name))
                .unwrap_or(false)
            {
                Ok(())
            } else {
                Err(unknown_vector_name_error(name))
            }
        }
    }
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
