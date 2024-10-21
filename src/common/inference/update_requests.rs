use std::collections::HashMap;

use api::rest::{Batch, BatchVectorStruct, PointStruct, PointVectors, Vector, VectorStruct};
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointStructPersisted, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::vector_ops::PointVectorsPersisted;
use futures::stream::{self, StreamExt};
use storage::content_manager::errors::StorageError;

use crate::common::inference::service::{InferenceData, InferenceService};

pub async fn convert_vectors(vectors: Vec<Vector>) -> Result<Vec<VectorPersisted>, StorageError> {
    let results: Vec<Result<VectorPersisted, StorageError>> = stream::iter(vectors)
        .then(convert_single_vector)
        .collect()
        .await;

    results.into_iter().collect()
}

pub async fn convert_point_struct(
    point_structs: Vec<PointStruct>,
) -> Result<Vec<PointStructPersisted>, StorageError> {
    let mut converted_points: Vec<PointStructPersisted> = Vec::new();
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
                    let converted_vector = convert_single_vector(vector).await?;
                    named_vectors.insert(name, converted_vector);
                }
                VectorStructPersisted::Named(named_vectors)
            }
            VectorStruct::Document(doc) => {
                let vector = convert_single_vector(Vector::Document(doc)).await?;
                match vector {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request("Sparse vector should be named"));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Image(img) => {
                let vector = convert_single_vector(Vector::Image(img)).await?;
                match vector {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request("Sparse vector should be named"));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Object(obj) => {
                let vector = convert_single_vector(Vector::Object(obj)).await?;
                match vector {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request("Sparse vector should be named"));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
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
                    "Document processing is not supported in batch operations.",
                ))
            }
            BatchVectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Image processing is not supported in batch operations.",
                ))
            }
            BatchVectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Object processing is not supported in batch operations.",
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
                    let converted_vec = convert_single_vector(vec).await?;
                    converted.insert(name, converted_vec);
                }

                VectorStructPersisted::Named(converted)
            }
            VectorStruct::Document(_) => {
                return Err(StorageError::inference_error(
                    "Document processing is not supported for point vectors.",
                ))
            }
            VectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Image processing is not supported for point vectors.",
                ))
            }
            VectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Object processing is not supported for point vectors.",
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

async fn convert_single_vector(vector: Vector) -> Result<VectorPersisted, StorageError> {
    let inference_service = InferenceService::global().clone();
    match vector {
        Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
        Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
        Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),
        Vector::Document(doc) => match inference_service {
            Some(service) => {
                let vector = service
                    .infer(InferenceData::Document(doc))
                    .await
                    .map_err(|e| StorageError::inference_error(e.to_string()))?;
                vector.into_iter().next().ok_or_else(|| {
                    StorageError::inference_error(
                        "Inference service returned empty vector for document",
                    )
                })
            }
            None => Err(StorageError::inference_error(
                "InferenceService not initialized for document processing",
            )),
        },
        Vector::Image(img) => match inference_service {
            Some(service) => {
                let vector = service
                    .infer(InferenceData::Image(img))
                    .await
                    .map_err(|e| StorageError::inference_error(e.to_string()))?;
                vector.into_iter().next().ok_or_else(|| {
                    StorageError::inference_error(
                        "Inference service returned empty vector for image",
                    )
                })
            }
            None => Err(StorageError::inference_error(
                "InferenceService not initialized for image processing",
            )),
        },
        Vector::Object(obj) => match inference_service {
            Some(service) => {
                let vector = service
                    .infer(InferenceData::Object(obj))
                    .await
                    .map_err(|e| StorageError::inference_error(e.to_string()))?;
                vector.into_iter().next().ok_or_else(|| {
                    StorageError::inference_error(
                        "Inference service returned empty vector for object",
                    )
                })
            }
            None => Err(StorageError::inference_error(
                "InferenceService not initialized for object processing",
            )),
        },
    }
}
