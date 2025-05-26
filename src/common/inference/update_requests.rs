use std::collections::HashMap;

use api::rest::models::InferenceUsage;
use api::rest::{Batch, BatchVectorStruct, PointStruct, PointVectors, Vector, VectorStruct};
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointStructPersisted, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::vector_ops::PointVectorsPersisted;
use storage::content_manager::errors::StorageError;

use crate::common::inference::InferenceToken;
use crate::common::inference::batch_processing::BatchAccum;
use crate::common::inference::infer_processing::BatchAccumInferred;
use crate::common::inference::service::{InferenceData, InferenceType};

pub async fn convert_point_struct(
    point_structs: Vec<PointStruct>,
    inference_type: InferenceType,
    inference_token: InferenceToken,
) -> Result<(Vec<PointStructPersisted>, Option<InferenceUsage>), StorageError> {
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

    let (inferred, usage) = if !batch_accum.objects.is_empty() {
        let (inferred_data, usage) =
            BatchAccumInferred::from_batch_accum(batch_accum, inference_type, &inference_token)
                .await?;
        (Some(inferred_data), usage)
    } else {
        (None, None)
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
                for (name, vector_data) in named {
                    let converted_vector = match &inferred {
                        Some(inferred) => convert_vector_with_inferred(vector_data, inferred)?,
                        None => match vector_data {
                            Vector::Dense(dense) => VectorPersisted::Dense(dense),
                            Vector::Sparse(sparse) => VectorPersisted::Sparse(sparse),
                            Vector::MultiDense(multi) => VectorPersisted::MultiDense(multi),
                            Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                                return Err(StorageError::inference_error(
                                    "Inference required but service returned no results for named vector",
                                ));
                            }
                        },
                    };
                    named_vectors.insert(name, converted_vector);
                }
                VectorStructPersisted::Named(named_vectors)
            }
            VectorStruct::Document(doc) => {
                let vector_data = match &inferred {
                    Some(inferred) => {
                        convert_vector_with_inferred(Vector::Document(doc), inferred)?
                    }
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required for document but service returned no results",
                        ));
                    }
                };
                match vector_data {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request(
                            "Sparse vector from document inference should be named",
                        ));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Image(img) => {
                let vector_data = match &inferred {
                    Some(inferred) => convert_vector_with_inferred(Vector::Image(img), inferred)?,
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required for image but service returned no results",
                        ));
                    }
                };
                match vector_data {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request(
                            "Sparse vector from image inference should be named",
                        ));
                    }
                    VectorPersisted::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
                }
            }
            VectorStruct::Object(obj) => {
                let vector_data = match &inferred {
                    Some(inferred) => convert_vector_with_inferred(Vector::Object(obj), inferred)?,
                    None => {
                        return Err(StorageError::inference_error(
                            "Inference required for object but service returned no results",
                        ));
                    }
                };
                match vector_data {
                    VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                    VectorPersisted::Sparse(_) => {
                        return Err(StorageError::bad_request(
                            "Sparse vector from object inference should be named",
                        ));
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

    Ok((converted_points, usage))
}

pub async fn convert_batch(
    batch: Batch,
    inference_token: InferenceToken,
) -> Result<(BatchPersisted, Option<InferenceUsage>), StorageError> {
    let Batch {
        ids,
        vectors,
        payloads,
    } = batch;

    let mut inference_usage = InferenceUsage::default();

    let batch_persisted = match vectors {
        BatchVectorStruct::Single(single) => BatchVectorStructPersisted::Single(single),
        BatchVectorStruct::MultiDense(multi) => BatchVectorStructPersisted::MultiDense(multi),
        BatchVectorStruct::Named(named) => {
            let mut named_vectors = HashMap::new();
            for (name, vecs) in named {
                let (converted_vectors, batch_usage) =
                    convert_vectors(vecs, InferenceType::Update, inference_token.clone()).await?;
                inference_usage.merge_opt(batch_usage);
                named_vectors.insert(name, converted_vectors);
            }
            BatchVectorStructPersisted::Named(named_vectors)
        }
        BatchVectorStruct::Document(_) => {
            return Err(StorageError::inference_error(
                "Direct Document processing is not supported in top-level batch vectors. Use named vectors.",
            ));
        }
        BatchVectorStruct::Image(_) => {
            return Err(StorageError::inference_error(
                "Direct Image processing is not supported in top-level batch vectors. Use named vectors.",
            ));
        }
        BatchVectorStruct::Object(_) => {
            return Err(StorageError::inference_error(
                "Direct Object processing is not supported in top-level batch vectors. Use named vectors.",
            ));
        }
    };

    let batch_persisted = BatchPersisted {
        ids,
        vectors: batch_persisted,
        payloads,
    };

    Ok((batch_persisted, inference_usage.into_non_empty()))
}

pub async fn convert_point_vectors(
    point_vectors_list: Vec<PointVectors>,
    inference_type: InferenceType,
    inference_token: InferenceToken,
) -> Result<(Vec<PointVectorsPersisted>, Option<InferenceUsage>), StorageError> {
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

    let mut inference_usage = InferenceUsage::default();

    let inferred = if !batch_accum.objects.is_empty() {
        let (inferred_data, usage) =
            BatchAccumInferred::from_batch_accum(batch_accum, inference_type, &inference_token)
                .await?;
        inference_usage.merge_opt(usage);
        Some(inferred_data)
    } else {
        None
    };

    let mut converted_point_vectors: Vec<PointVectorsPersisted> = Vec::new();
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
                                    "Inference required for named vector in PointVectors but no results",
                                ));
                            }
                        },
                    };
                    converted.insert(name, converted_vec);
                }

                VectorStructPersisted::Named(converted)
            }
            VectorStruct::Document(_) => {
                return Err(StorageError::inference_error(
                    "Direct Document processing not supported for PointVectors. Use named vectors.",
                ));
            }
            VectorStruct::Image(_) => {
                return Err(StorageError::inference_error(
                    "Direct Image processing not supported for PointVectors. Use named vectors.",
                ));
            }
            VectorStruct::Object(_) => {
                return Err(StorageError::inference_error(
                    "Direct Object processing not supported for PointVectors. Use named vectors.",
                ));
            }
        };

        converted_point_vectors.push(PointVectorsPersisted {
            id,
            vector: converted_vector,
        });
    }

    Ok((converted_point_vectors, inference_usage.into_non_empty()))
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
                    for (name, vector_data) in named {
                        let converted_vector = convert_vector_with_inferred(vector_data, inferred)?;
                        named_vectors.insert(name, converted_vector);
                    }
                    VectorStructPersisted::Named(named_vectors)
                }
                VectorStruct::Document(doc) => {
                    let vector_data =
                        convert_vector_with_inferred(Vector::Document(doc), inferred)?;
                    match vector_data {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request(
                                "Sparse vector from document inference must be named",
                            ));
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
                VectorStruct::Image(img) => {
                    let vector_data = convert_vector_with_inferred(Vector::Image(img), inferred)?;
                    match vector_data {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request(
                                "Sparse vector from image inference must be named",
                            ));
                        }
                        VectorPersisted::MultiDense(multi) => {
                            VectorStructPersisted::MultiDense(multi)
                        }
                    }
                }
                VectorStruct::Object(obj) => {
                    let vector_data = convert_vector_with_inferred(Vector::Object(obj), inferred)?;
                    match vector_data {
                        VectorPersisted::Dense(dense) => VectorStructPersisted::Single(dense),
                        VectorPersisted::Sparse(_) => {
                            return Err(StorageError::bad_request(
                                "Sparse vector from object inference must be named",
                            ));
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
    inference_token: InferenceToken,
) -> Result<(Vec<VectorPersisted>, Option<InferenceUsage>), StorageError> {
    let mut batch_accum = BatchAccum::new();
    for vector in &vectors {
        match vector {
            Vector::Document(doc) => batch_accum.add(InferenceData::Document(doc.clone())),
            Vector::Image(img) => batch_accum.add(InferenceData::Image(img.clone())),
            Vector::Object(obj) => batch_accum.add(InferenceData::Object(obj.clone())),
            Vector::Dense(_) | Vector::Sparse(_) | Vector::MultiDense(_) => {}
        }
    }

    let mut inference_usage = InferenceUsage::default();

    let inferred = if !batch_accum.objects.is_empty() {
        let (inferred_data, usage) =
            BatchAccumInferred::from_batch_accum(batch_accum, inference_type, &inference_token)
                .await?;
        inference_usage.merge_opt(usage);
        Some(inferred_data)
    } else {
        None
    };

    let converted_vectors: Result<Vec<VectorPersisted>, StorageError> = vectors
        .into_iter()
        .map(|vector_data| match &inferred {
            Some(inferred) => convert_vector_with_inferred(vector_data, inferred),
            None => match vector_data {
                Vector::Dense(dense) => Ok(VectorPersisted::Dense(dense)),
                Vector::Sparse(sparse) => Ok(VectorPersisted::Sparse(sparse)),
                Vector::MultiDense(multi) => Ok(VectorPersisted::MultiDense(multi)),
                Vector::Document(_) | Vector::Image(_) | Vector::Object(_) => {
                    Err(StorageError::inference_error(
                        "Inference required but no inference service results available",
                    ))
                }
            },
        })
        .collect();

    converted_vectors.map(|vecs| (vecs, inference_usage.into_non_empty()))
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
