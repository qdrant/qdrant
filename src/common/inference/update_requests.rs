use std::collections::HashMap;

use api::rest::{Batch, BatchVectorStruct, PointStruct, PointVectors, Vector, VectorStruct};
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointStructPersisted, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::vector_ops::PointVectorsPersisted;
use storage::content_manager::errors::StorageError;

use crate::common::inference::batch_processing::BatchAccum;
use crate::common::inference::infer_processing::BatchAccumInferred;
use crate::common::inference::service::{InferenceData, InferenceType};

pub async fn convert_point_struct(
    point_structs: Vec<PointStruct>,
    inference_type: InferenceType,
) -> Result<Vec<PointStructPersisted>, StorageError> {
    let mut batch_accum = BatchAccum::new();

    for point_struct in &point_structs {
        match &point_struct.vector {
            VectorStruct::Named(named) => {
                for vector in named.values() {
                    match vector {
                        Vector::Document(doc) => {
                            batch_accum.add(InferenceData::Document(doc.clone()))
                        }
                        Vector::Image(img) => batch_accum.add(InferenceData::Image(img.clone())),
                        Vector::Object(obj) => batch_accum.add(InferenceData::Object(obj.clone())),
                        Vector::Dense(_) | Vector::Sparse(_) | Vector::MultiDense(_) => {}
                    }
                }
            }
            VectorStruct::Document(doc) => batch_accum.add(InferenceData::Document(doc.clone())),
            VectorStruct::Image(img) => batch_accum.add(InferenceData::Image(img.clone())),
            VectorStruct::Object(obj) => batch_accum.add(InferenceData::Object(obj.clone())),
            VectorStruct::MultiDense(_) | VectorStruct::Single(_) => {}
        }
    }

    let inferred = if !batch_accum.objects.is_empty() {
        Some(BatchAccumInferred::from_batch_accum(batch_accum, inference_type).await?)
    } else {
        None
    };

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
                    let converted_vector = match &inferred {
                        Some(inferred) => convert_vector_with_inferred(vector, inferred)?,
                        None => match vector {
                            Vector::Dense(dense) => VectorPersisted::Dense(dense),
                            Vector::Sparse(sparse) => VectorPersisted::Sparse(sparse),
                            Vector::MultiDense(multi) => VectorPersisted::MultiDense(multi),
                            Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                                return Err(StorageError::inference_error(
                                    "Inference required but service returned no results",
                                ))
                            }
                        },
                    };
                    named_vectors.insert(name, converted_vector);
                }
                VectorStructPersisted::Named(named_vectors)
            }
            VectorStruct::Document(doc) => {
                let vector = match &inferred {
                    Some(inferred) => {
                        convert_vector_with_inferred(Vector::Document(doc), inferred)?
                    }
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required but service returned no results",
                        ))
                    }
                };
                match vector {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request("Sparse vector should be named"));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Image(img) => {
                let vector = match &inferred {
                    Some(inferred) => convert_vector_with_inferred(Vector::Image(img), inferred)?,
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required but service returned no results",
                        ))
                    }
                };
                match vector {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request("Sparse vector should be named"));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Object(obj) => {
                let vector = match &inferred {
                    Some(inferred) => convert_vector_with_inferred(Vector::Object(obj), inferred)?,
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required but service returned no results",
                        ))
                    }
                };
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
                    let converted_vectors = convert_vectors(vectors, InferenceType::Update).await?;
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
    inference_type: InferenceType,
) -> Result<Vec<PointVectorsPersisted>, StorageError> {
    let mut converted_point_vectors = Vec::new();
    let mut batch_accum = BatchAccum::new();

    for point_vectors in &point_vectors_list {
        if let VectorStruct::Named(named) = &point_vectors.vector {
            for vector in named.values() {
                match vector {
                    Vector::Document(doc) => batch_accum.add(InferenceData::Document(doc.clone())),
                    Vector::Image(img) => batch_accum.add(InferenceData::Image(img.clone())),
                    Vector::Object(obj) => batch_accum.add(InferenceData::Object(obj.clone())),
                    Vector::Dense(_) | Vector::Sparse(_) | Vector::MultiDense(_) => {}
                }
            }
        }
    }

    let inferred = if !batch_accum.objects.is_empty() {
        Some(BatchAccumInferred::from_batch_accum(batch_accum, inference_type).await?)
    } else {
        None
    };

    for point_vectors in point_vectors_list {
        let PointVectors { id, vector } = point_vectors;

        let converted_vector = match vector {
            VectorStruct::Single(dense) => VectorStructPersisted::Single(dense),
            VectorStruct::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
            VectorStruct::Named(named) => {
                let mut converted = HashMap::new();

                for (name, vec) in named {
                    let converted_vec = match &inferred {
                        Some(inferred) => convert_vector_with_inferred(vec, inferred)?,
                        None => match vec {
                            Vector::Dense(dense) => VectorPersisted::Dense(dense),
                            Vector::Sparse(sparse) => VectorPersisted::Sparse(sparse),
                            Vector::MultiDense(multi) => VectorPersisted::MultiDense(multi),
                            Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                                return Err(StorageError::inference_error(
                                    "Inference required but service returned no results",
                                ))
                            }
                        },
                    };
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

fn convert_point_struct_with_inferred(
    point_structs: Vec<PointStruct>,
    inferred: &BatchAccumInferred,
) -> Result<Vec<PointStructPersisted>, StorageError> {
    point_structs
        .into_iter()
        .map(|point_struct| {
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
                        let converted_vector = convert_vector_with_inferred(vector, inferred)?;
                        named_vectors.insert(name, converted_vector);
                    }
                    VectorStructPersisted::Named(named_vectors)
                }
                VectorStruct::Document(doc) => {
                    let vector = convert_vector_with_inferred(Vector::Document(doc), inferred)?;
                    match vector {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request("Sparse vector should be named"))
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
                VectorStruct::Image(img) => {
                    let vector = convert_vector_with_inferred(Vector::Image(img), inferred)?;
                    match vector {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request("Sparse vector should be named"))
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
                VectorStruct::Object(obj) => {
                    let vector = convert_vector_with_inferred(Vector::Object(obj), inferred)?;
                    match vector {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request("Sparse vector should be named"))
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
            };

            Ok(PointStructPersisted {
                id,
                vector: converted_vector_struct,
                payload,
            })
        })
        .collect()
}

pub async fn convert_vectors(
    vectors: Vec<Vector>,
    inference_type: InferenceType,
) -> Result<Vec<VectorPersisted>, StorageError> {
    let mut batch_accum = BatchAccum::new();
    for vector in &vectors {
        match vector {
            Vector::Document(doc) => batch_accum.add(InferenceData::Document(doc.clone())),
            Vector::Image(img) => batch_accum.add(InferenceData::Image(img.clone())),
            Vector::Object(obj) => batch_accum.add(InferenceData::Object(obj.clone())),
            Vector::Dense(_) | Vector::Sparse(_) | Vector::MultiDense(_) => {}
        }
    }

    let inferred = if !batch_accum.objects.is_empty() {
        Some(BatchAccumInferred::from_batch_accum(batch_accum, inference_type).await?)
    } else {
        None
    };

    vectors
        .into_iter()
        .map(|vector| match &inferred {
            Some(inferred) => convert_vector_with_inferred(vector, inferred),
            None => match vector {
                Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
                Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
                Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),
                Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                    Err(StorageError::inference_error(
                        "Inference required but service returned no results",
                    ))
                }
            },
        })
        .collect()
}

fn convert_vector_with_inferred(
    vector: Vector,
    inferred: &BatchAccumInferred,
) -> Result<VectorPersisted, StorageError> {
    match vector {
        Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
        Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
        Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),
        Vector::Document(doc) => {
            let data = InferenceData::Document(doc);
            inferred.get_vector(&data).cloned().ok_or_else(|| {
                StorageError::inference_error("Missing inferred vector for document")
            })
        }
        Vector::Image(img) => {
            let data = InferenceData::Image(img);
            inferred
                .get_vector(&data)
                .cloned()
                .ok_or_else(|| StorageError::inference_error("Missing inferred vector for image"))
        }
        Vector::Object(obj) => {
            let data = InferenceData::Object(obj);
            inferred
                .get_vector(&data)
                .cloned()
                .ok_or_else(|| StorageError::inference_error("Missing inferred vector for object"))
        }
    }
}
