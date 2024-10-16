use std::collections::HashMap;

use api::rest::{Batch, BatchVectorStruct, PointStruct, PointVectors, Vector, VectorStruct};
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointStructPersisted, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::vector_ops::PointVectorsPersisted;
use futures::stream::{self, StreamExt};
use storage::content_manager::errors::StorageError;

use crate::common::inference::service::InferenceService;

pub async fn convert_vectors(vectors: Vec<Vector>) -> Result<Vec<VectorPersisted>, StorageError> {
    let inference_service = {
        let guard = InferenceService::global();
        guard.clone()
    };

    let results: Vec<Result<VectorPersisted, StorageError>> = stream::iter(vectors)
        .then(|vec| async {
            match vec {
                Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
                Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
                Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),

                Vector::Document(doc) => match inference_service {
                    Some(ref service) => {
                        let vector = service
                            .infer(&doc)
                            .await
                            .map_err(|e| StorageError::inference_error(e.to_string()))?;
                        vector.into_iter().next().ok_or_else(|| {
                            StorageError::inference_error("Inference service returned empty vector")
                        })
                    }
                    None => Err(StorageError::inference_error(
                        "InferenceService not initialized",
                    )),
                },

                Vector::Image(_) => Err(StorageError::inference_error(
                    "Inference for Image is not implemented",
                )),
                Vector::Object(_) => Err(StorageError::inference_error(
                    "Inference for Object is not implemented",
                )),
            }
        })
        .collect()
        .await;

    results.into_iter().collect()
}

pub async fn convert_point_struct(
    point_structs: Vec<PointStruct>,
) -> Result<Vec<PointStructPersisted>, StorageError> {
    let mut converted_points: Vec<PointStructPersisted> = Vec::new();
    let inference_service = {
        let guard = InferenceService::global();
        guard.clone()
    };
    for point_struct in point_structs {
        let PointStruct {
            id,
            vector,
            payload,
        } = point_struct;

        let converted_vector_struct = match vector {
            VectorStruct::Single(single) => VectorStructPersisted::Single(single),
            VectorStruct::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
            VectorStruct::Named(named) => {
                let mut named_vectors = HashMap::new();
                for (name, vector) in named {
                    let converted_vector = match vector {
                        Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
                        Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
                        Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),
                        Vector::Document(doc) => match &inference_service {
                            Some(ref service) => {
                                let vector = service
                                    .infer(&doc)
                                    .await
                                    .map_err(|e| StorageError::inference_error(e.to_string()))?;
                                vector.into_iter().next().ok_or_else(|| {
                                    StorageError::inference_error(
                                        "Inference service returned empty vector",
                                    )
                                })
                            }
                            None => Err(StorageError::inference_error(
                                "InferenceService not initialized",
                            )),
                        },
                        Vector::Image(_) => Err(StorageError::inference_error(
                            "Inference for Image is not implemented",
                        )),
                        Vector::Object(_) => Err(StorageError::inference_error(
                            "Inference for Object is not implemented",
                        )),
                    }?;
                    named_vectors.insert(name, converted_vector);
                }
                VectorStructPersisted::Named(named_vectors)
            }
            VectorStruct::Document(doc) => match &inference_service {
                Some(ref service) => {
                    let vector = service
                        .infer(&doc)
                        .await
                        .map_err(|e| StorageError::inference_error(e.to_string()))?;
                    let res = vector.into_iter().next().ok_or_else(|| {
                        StorageError::inference_error("Inference service returned empty vector")
                    })?;
                    match res {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request("Sparse vector should be named"))
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
                None => {
                    return Err(StorageError::inference_error(
                        "InferenceService not initialized",
                    ))
                }
            },
            VectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Image is not implemented",
                ))
            }
            VectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Object is not implemented",
                ))
            }
        };
        let converted = PointStructPersisted {
            id,
            vector: converted_vector_struct,
            payload,
        };

        converted_points.push(converted);
    }

    Ok(converted_points)
}

pub async fn convert_batch(batch: Batch) -> Result<BatchPersisted, StorageError> {
    let Batch {
        ids,
        vectors,
        payloads,
    } = batch;

    let batch_persisted = BatchPersisted {
        ids,
        vectors: match vectors {
            BatchVectorStruct::Single(single) => BatchVectorStructPersisted::Single(single),
            BatchVectorStruct::MultiDense(multi) => BatchVectorStructPersisted::MultiDense(multi),
            BatchVectorStruct::Named(named) => {
                let mut named_vectors = HashMap::new();

                for (name, vectors) in named {
                    let converted_vectors = convert_vectors(vectors).await?;
                    named_vectors.insert(name, converted_vectors);
                }

                BatchVectorStructPersisted::Named(named_vectors)
            }
            BatchVectorStruct::Document(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Document is not implemented",
                ))
            }
            BatchVectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Image is not implemented",
                ))
            }
            BatchVectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Object is not implemented",
                ))
            }
        },
        payloads,
    };

    Ok(batch_persisted)
}

pub async fn convert_point_vectors(
    point_vectors_list: Vec<PointVectors>,
) -> Result<Vec<PointVectorsPersisted>, StorageError> {
    let mut converted_point_vectors = Vec::new();

    for point_vectors in point_vectors_list {
        let PointVectors { id, vector } = point_vectors;

        let converted_vector = match vector {
            VectorStruct::Single(dense) => VectorStructPersisted::Single(dense),
            VectorStruct::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
            VectorStruct::Named(named) => {
                let mut converted = HashMap::new();

                for (name, vec) in named {
                    let converted_vec = match vec {
                        Vector::Dense(dense) => VectorPersisted::Dense(dense),
                        Vector::Sparse(sparse) => VectorPersisted::Sparse(sparse),
                        Vector::MultiDense(multi) => VectorPersisted::MultiDense(multi),
                        Vector::Document(_) => {
                            return Err(StorageError::inference_error(
                                "Inference for Document is not implemented",
                            ))
                        }
                        Vector::Image(_) => {
                            return Err(StorageError::inference_error(
                                "Inference for Image is not implemented",
                            ))
                        }
                        Vector::Object(_) => {
                            return Err(StorageError::inference_error(
                                "Inference for Object is not implemented",
                            ))
                        }
                    };
                    converted.insert(name, converted_vec);
                }

                VectorStructPersisted::Named(converted)
            }
            VectorStruct::Document(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Document is not implemented",
                ))
            }
            VectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Image is not implemented",
                ))
            }
            VectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Inference for Object is not implemented",
                ))
            }
        };

        let converted_point_vector = PointVectorsPersisted {
            id,
            vector: converted_vector,
        };

        converted_point_vectors.push(converted_point_vector);
    }

    Ok(converted_point_vectors)
}
