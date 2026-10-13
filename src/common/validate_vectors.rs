use std::collections::BTreeMap;

use collection::operations::point_ops::{
    BatchVectorStructPersisted, PointInsertOperationsInternal, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::types::{SparseVectorParams, VectorsConfig};
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::VectorNameBuf;
use storage::content_manager::errors::StorageError;

/// Validate vector names, sparse payload kinds, and dimensions against the collection config.
///
/// This performs an early O(n) check before the operation is written to WAL,
/// ensuring that invalid vectors are reported even for async (wait=false) operations.
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
                    validate_sparse_vectors(name, vectors, sparse_vectors_config)?;
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
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
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
                    validate_sparse_vectors(
                        name,
                        std::slice::from_ref(vector),
                        sparse_vectors_config,
                    )?;
                    continue;
                };
                let expected_dim = params.size.get() as usize;
                validate_single_vector_dim(vector, name, expected_dim)?;
            }
        }
    }
    Ok(())
}

/// A name absent from the dense config must exist in the sparse config, and all
/// its payloads must be sparse. Reject mismatches before writing to WAL rather
/// than acknowledging an operation that will fail during apply.
fn validate_sparse_vectors(
    name: &VectorNameBuf,
    vectors: &[VectorPersisted],
    sparse_vectors_config: Option<&BTreeMap<VectorNameBuf, SparseVectorParams>>,
) -> Result<(), StorageError> {
    let is_known_sparse = sparse_vectors_config.is_some_and(|config| config.contains_key(name));
    if !is_known_sparse {
        return Err(StorageError::bad_input(format!(
            "Not existing vector name error: {name}"
        )));
    }
    if vectors
        .iter()
        .any(|vector| !matches!(vector, VectorPersisted::Sparse(_)))
    {
        return Err(StorageError::bad_input(format!(
            "Wrong vector type for sparse vector name: {name}"
        )));
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use collection::operations::point_ops::{BatchPersisted, PointStructPersisted};
    use collection::operations::vector_params_builder::VectorParamsBuilder;
    use segment::types::Distance;

    use super::*;

    fn single_collection_config(dim: u64) -> VectorsConfig {
        VectorsConfig::Single(VectorParamsBuilder::new(dim, Distance::Cosine).build())
    }

    fn named_dense_point(name: &str, dim: usize) -> PointStructPersisted {
        PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::Named(HashMap::from([(
                name.to_string(),
                VectorPersisted::Dense(vec![0.1; dim]),
            )])),
            payload: None,
        }
    }

    #[test]
    fn named_vector_into_unnamed_collection_is_rejected() {
        let config = single_collection_config(4);
        let op = PointInsertOperationsInternal::PointsList(vec![named_dense_point("name", 4)]);
        let err = validate_vector_dimensions(&op, &config, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Not existing vector name error: name"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mistyped_named_vector_is_rejected() {
        let named = BTreeMap::from([(
            "image".to_string(),
            VectorParamsBuilder::new(4, Distance::Cosine).build(),
        )]);
        let config = VectorsConfig::Multi(named);
        let op = PointInsertOperationsInternal::PointsList(vec![named_dense_point("iamge", 4)]);
        let err = validate_vector_dimensions(&op, &config, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Not existing vector name error: iamge"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn known_named_vector_still_validates_dimensions() {
        let named = BTreeMap::from([(
            "image".to_string(),
            VectorParamsBuilder::new(4, Distance::Cosine).build(),
        )]);
        let config = VectorsConfig::Multi(named);

        let ok_op = PointInsertOperationsInternal::PointsList(vec![named_dense_point("image", 4)]);
        validate_vector_dimensions(&ok_op, &config, None).unwrap();

        let bad_op = PointInsertOperationsInternal::PointsList(vec![named_dense_point("image", 6)]);
        let err = validate_vector_dimensions(&bad_op, &config, None).unwrap_err();
        assert!(
            err.to_string().contains("Vector dimension error"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn sparse_named_vector_passes_but_unknown_name_still_rejected() {
        let config = single_collection_config(4);
        let sparse_config = BTreeMap::from([(
            "splade".to_string(),
            SparseVectorParams {
                index: None,
                modifier: None,
            },
        )]);

        let sparse_point = PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::Named(HashMap::from([(
                "splade".to_string(),
                VectorPersisted::new_sparse(vec![1, 2], vec![0.5, 0.6]),
            )])),
            payload: None,
        };
        let op = PointInsertOperationsInternal::PointsList(vec![sparse_point]);
        validate_vector_dimensions(&op, &config, Some(&sparse_config)).unwrap();

        // A sparse config being present must not whitelist unrelated names
        let op = PointInsertOperationsInternal::PointsList(vec![named_dense_point("text", 4)]);
        let err = validate_vector_dimensions(&op, &config, Some(&sparse_config)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Not existing vector name error: text"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn batch_with_unknown_named_vector_is_rejected() {
        let config = single_collection_config(4);
        let op = PointInsertOperationsInternal::PointsBatch(BatchPersisted {
            ids: vec![1.into(), 2.into()],
            vectors: BatchVectorStructPersisted::Named(HashMap::from([(
                "name".to_string(),
                vec![
                    VectorPersisted::Dense(vec![0.1; 4]),
                    VectorPersisted::Dense(vec![0.2; 4]),
                ],
            )])),
            payloads: None,
        });
        let err = validate_vector_dimensions(&op, &config, None).unwrap_err();
        assert!(
            err.to_string()
                .contains("Not existing vector name error: name"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn non_sparse_payload_for_sparse_name_is_rejected() {
        let config = single_collection_config(4);
        let sparse_config = BTreeMap::from([(
            "splade".to_string(),
            SparseVectorParams {
                index: None,
                modifier: None,
            },
        )]);

        for vector in [
            VectorPersisted::Dense(vec![0.1; 4]),
            VectorPersisted::MultiDense(vec![vec![0.1; 4]]),
        ] {
            let point = PointStructPersisted {
                id: 1.into(),
                vector: VectorStructPersisted::Named(HashMap::from([(
                    "splade".to_string(),
                    vector,
                )])),
                payload: None,
            };
            let op = PointInsertOperationsInternal::PointsList(vec![point]);
            let err = validate_vector_dimensions(&op, &config, Some(&sparse_config)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("Wrong vector type for sparse vector name: splade"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn batch_with_non_sparse_payload_for_sparse_name_is_rejected() {
        let config = single_collection_config(4);
        let sparse_config = BTreeMap::from([(
            "splade".to_string(),
            SparseVectorParams {
                index: None,
                modifier: None,
            },
        )]);

        for vector in [
            VectorPersisted::Dense(vec![0.1; 4]),
            VectorPersisted::MultiDense(vec![vec![0.1; 4]]),
        ] {
            let op = PointInsertOperationsInternal::PointsBatch(BatchPersisted {
                ids: vec![1.into(), 2.into()],
                vectors: BatchVectorStructPersisted::Named(HashMap::from([(
                    "splade".to_string(),
                    vec![VectorPersisted::new_sparse(vec![1], vec![0.5]), vector],
                )])),
                payloads: None,
            });
            let err = validate_vector_dimensions(&op, &config, Some(&sparse_config)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("Wrong vector type for sparse vector name: splade"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn batch_with_sparse_named_vectors_is_accepted() {
        let config = single_collection_config(4);
        let sparse_config = BTreeMap::from([(
            "splade".to_string(),
            SparseVectorParams {
                index: None,
                modifier: None,
            },
        )]);
        let op = PointInsertOperationsInternal::PointsBatch(BatchPersisted {
            ids: vec![1.into(), 2.into()],
            vectors: BatchVectorStructPersisted::Named(HashMap::from([(
                "splade".to_string(),
                vec![
                    VectorPersisted::new_sparse(vec![1], vec![0.5]),
                    VectorPersisted::new_sparse(vec![1, 2], vec![0.5, 0.6]),
                ],
            )])),
            payloads: None,
        });
        validate_vector_dimensions(&op, &config, Some(&sparse_config)).unwrap();
    }

    #[test]
    fn unnamed_vector_dimension_mismatch_still_rejected() {
        let config = single_collection_config(4);
        let point = PointStructPersisted {
            id: 1.into(),
            vector: VectorStructPersisted::Single(vec![0.1; 6]),
            payload: None,
        };
        let op = PointInsertOperationsInternal::PointsList(vec![point]);
        let err = validate_vector_dimensions(&op, &config, None).unwrap_err();
        assert!(
            err.to_string().contains("Vector dimension error"),
            "unexpected error: {err}"
        );
    }
}
