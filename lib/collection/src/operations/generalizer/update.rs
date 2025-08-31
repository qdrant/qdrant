use api::rest::{
    Batch, BatchVectorStruct, Document, Image, InferenceObject, PointInsertOperations,
    PointVectors, PointsBatch, PointsList, UpdateVectors, Vector, VectorStruct,
};
use serde_json::{Value, json};
use shard::operations::payload_ops::{DeletePayload, SetPayload};

use crate::operations::generalizer::placeholders::{size_value_placeholder, text_placeholder};
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};
use crate::operations::vector_ops::DeleteVectors;

impl Generalizer for DeleteVectors {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let DeleteVectors {
            points,
            filter,
            vector,
            shard_key,
        } = self;

        let mut result = serde_json::json!({
            "shard_key": shard_key,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        if let Some(points) = points {
            match level {
                GeneralizationLevel::OnlyVector => {
                    result["points"] = serde_json::to_value(points).unwrap();
                }
                GeneralizationLevel::VectorAndValues => {
                    result["num_points"] = size_value_placeholder(points.len());
                }
            }
        }

        match level {
            GeneralizationLevel::OnlyVector => {
                // Show vector names in this mode
                result["vector"] = serde_json::to_value(vector).unwrap_or_default();
            }
            GeneralizationLevel::VectorAndValues => {
                result["num_vectors"] = size_value_placeholder(vector.len());
            }
        }

        result
    }
}

impl Generalizer for SetPayload {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let SetPayload {
            payload,
            points,
            filter,
            shard_key,
            key,
        } = self;

        let mut result = serde_json::json!({
            "shard_key": shard_key,
            "key": key,
        });

        match level {
            GeneralizationLevel::OnlyVector => {
                result["payload"] = serde_json::to_value(payload).unwrap_or_default();
            }
            GeneralizationLevel::VectorAndValues => {
                let payload_keys: Vec<_> = payload.keys().cloned().collect();
                result["payload_keys"] = serde_json::to_value(payload_keys).unwrap_or_default();
            }
        }

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        if let Some(points) = points {
            match level {
                GeneralizationLevel::OnlyVector => {
                    result["points"] = serde_json::to_value(points).unwrap();
                }
                GeneralizationLevel::VectorAndValues => {
                    result["num_points"] = size_value_placeholder(points.len());
                }
            }
        }

        result
    }
}

impl Generalizer for DeletePayload {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let DeletePayload {
            keys,
            points,
            filter,
            shard_key,
        } = self;

        let mut result = serde_json::json!({
            "keys": keys,
            "shard_key": shard_key,
        });

        if let Some(filter) = filter {
            result["filter"] = filter.generalize(level);
        }

        if let Some(points) = points {
            match level {
                GeneralizationLevel::OnlyVector => {
                    result["points"] = serde_json::to_value(points).unwrap();
                }
                GeneralizationLevel::VectorAndValues => {
                    result["num_points"] = size_value_placeholder(points.len());
                }
            }
        }

        result
    }
}

impl Generalizer for PointsBatch {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let PointsBatch {
            batch,
            shard_key,
            update_filter,
        } = self;

        let Batch {
            vectors,
            payloads,
            ids,
        } = batch;

        let batch_obj = match vectors {
            BatchVectorStruct::Single(single) => {
                json!({
                    "type": "dense_vectors_batch",
                    "len": single.len()
                })
            }
            BatchVectorStruct::MultiDense(multi) => {
                let total_subvectors: usize = multi.iter().map(|v| v.len()).sum();
                match level {
                    GeneralizationLevel::OnlyVector => {
                        json!({
                            "type": "multi_dense_vectors_batch",
                            "num_vectors": multi.len(),
                            "total_subvectors": total_subvectors,
                        })
                    }
                    GeneralizationLevel::VectorAndValues => {
                        json!({
                            "type": "multi_dense_vectors_batch",
                            "num_vectors_appx": size_value_placeholder(multi.len()),
                            "total_subvectors_appx": size_value_placeholder(total_subvectors),
                        })
                    }
                }
            }
            BatchVectorStruct::Named(named) => match level {
                GeneralizationLevel::OnlyVector => {
                    let mut obj = serde_json::Map::new();
                    for (name, vectors) in named.iter() {
                        let vectors_generalized: Vec<_> =
                            vectors.iter().map(|v| v.generalize(level)).collect();
                        obj.insert(name.clone(), serde_json::Value::Array(vectors_generalized));
                    }
                    serde_json::Value::Object(obj)
                }
                GeneralizationLevel::VectorAndValues => {
                    let mut vector_names: Vec<_> = Vec::new();
                    let mut num_vectors: usize = 0;

                    for (name, vector) in named.iter() {
                        vector_names.push(name.clone());
                        num_vectors += vector.len();
                    }

                    serde_json::json!({
                        "vector_names": vector_names,
                        "total_vectors_appx": size_value_placeholder(num_vectors),
                    })
                }
            },
            BatchVectorStruct::Document(docs) => {
                let docs_values: Vec<_> = docs.iter().map(|doc| doc.generalize(level)).collect();
                serde_json::Value::Array(docs_values)
            }
            BatchVectorStruct::Image(images) => {
                let images_values: Vec<_> =
                    images.iter().map(|img| img.generalize(level)).collect();
                serde_json::Value::Array(images_values)
            }
            BatchVectorStruct::Object(objects) => {
                let objects_values: Vec<_> =
                    objects.iter().map(|obj| obj.generalize(level)).collect();
                serde_json::Value::Array(objects_values)
            }
        };

        let mut obj = serde_json::json!({
            "shard_key": shard_key,
            "batch": batch_obj,
        });

        match level {
            GeneralizationLevel::OnlyVector => {
                obj["ids"] = serde_json::to_value(ids).unwrap_or_default();
                obj["payloads"] = serde_json::to_value(payloads).unwrap_or_default();
            }
            GeneralizationLevel::VectorAndValues => {
                obj["num_ids"] = size_value_placeholder(ids.len());
            }
        }

        if let Some(filter) = update_filter {
            obj["update_filter"] = filter.generalize(level);
        }

        obj
    }
}

impl Generalizer for PointInsertOperations {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch.generalize(level),
            PointInsertOperations::PointsList(points_list) => {
                let PointsList {
                    points,
                    shard_key,
                    update_filter,
                } = points_list;
                let mut result = serde_json::json!({
                    "type": "points_list",
                    "num_points": size_value_placeholder(points.len()),
                    "shard_key": shard_key,
                });
                if let Some(update_filter) = update_filter {
                    result["update_filter"] = update_filter.generalize(level);
                }
                result
            }
        }
    }
}

impl Generalizer for UpdateVectors {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let UpdateVectors {
            points,
            shard_key,
            update_filter,
        } = self;

        let mut result = serde_json::json!({
            "shard_key": shard_key,
            "num_points": size_value_placeholder(points.len()),
        });

        if let Some(filter) = update_filter {
            result["update_filter"] = filter.generalize(level);
        }

        result
    }
}

impl Generalizer for PointVectors {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let PointVectors { id, vector } = self;
        let mut result = serde_json::json!({
            "vector": vector.generalize(level),
        });

        match level {
            GeneralizationLevel::OnlyVector => {
                result["id"] = serde_json::to_value(id).unwrap_or_default();
            }
            GeneralizationLevel::VectorAndValues => {
                result["id"] = serde_json::json!({"type": "point_id"});
            }
        }

        result
    }
}

impl Generalizer for VectorStruct {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            VectorStruct::Single(dense) => {
                serde_json::json!({
                    "type": "dense_vector",
                    "len": dense.len()
                })
            }
            VectorStruct::MultiDense(multi) => match level {
                GeneralizationLevel::OnlyVector => {
                    serde_json::json!({
                        "type": "multi_dense_vector",
                        "num_vectors": multi.len(),
                    })
                }
                GeneralizationLevel::VectorAndValues => {
                    serde_json::json!({
                        "type": "multi_dense_vector",
                        "num_vectors": size_value_placeholder(multi.len()),
                    })
                }
            },
            VectorStruct::Named(named) => {
                let mut result = serde_json::json!({});

                for (name, vector) in named.iter() {
                    result[name] = vector.generalize(level);
                }

                result
            }
            VectorStruct::Document(document) => document.generalize(level),
            VectorStruct::Image(image) => image.generalize(level),
            VectorStruct::Object(object) => object.generalize(level),
        }
    }
}

impl Generalizer for Vector {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match self {
            Vector::Dense(dense) => {
                serde_json::json!({
                    "type": "dense_vector",
                    "len": dense.len()
                })
            }
            Vector::Sparse(sparse) => match level {
                GeneralizationLevel::OnlyVector => {
                    serde_json::json!({
                        "type": "sparse_vector",
                        "len": sparse.len()
                    })
                }
                GeneralizationLevel::VectorAndValues => {
                    serde_json::json!({
                        "type": "sparse_vector",
                        "len": size_value_placeholder(sparse.len()),
                    })
                }
            },
            Vector::MultiDense(multi) => match level {
                GeneralizationLevel::OnlyVector => {
                    serde_json::json!({
                        "type": "multi_dense_vector",
                        "num_vectors": multi.len(),
                    })
                }
                GeneralizationLevel::VectorAndValues => {
                    serde_json::json!({
                        "type": "multi_dense_vector",
                        "num_vectors": size_value_placeholder(multi.len()),
                    })
                }
            },
            Vector::Document(document) => document.generalize(level),
            Vector::Image(image) => image.generalize(level),
            Vector::Object(object) => object.generalize(level),
        }
    }
}

impl Generalizer for Document {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => serde_json::to_value(self).unwrap(),
            GeneralizationLevel::VectorAndValues => {
                let Document {
                    text,
                    model,
                    options,
                } = self;
                serde_json::json!({
                    "text": text_placeholder(text, true),
                    "model": model,
                    "options": options,
                })
            }
        }
    }
}

impl Generalizer for Image {
    fn generalize(&self, _level: GeneralizationLevel) -> Value {
        let Image {
            image: _, // Always omit image data, as it can be large
            model,
            options,
        } = self;

        serde_json::json!({
            "model": model,
            "options": options,
        })
    }
}

impl Generalizer for InferenceObject {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => serde_json::to_value(self).unwrap(),
            GeneralizationLevel::VectorAndValues => {
                let InferenceObject {
                    object: _, // Omit object data, as it can be large
                    model,
                    options,
                } = self;
                serde_json::json!({
                    "model": model,
                    "options": options,
                })
            }
        }
    }
}
