use super::schema::{BatchVectorStruct, ScoredPoint, SegmentTelemetry, Vector, VectorStruct};
use crate::rest::SegmentConfig;

impl From<segment::data_types::vectors::Vector> for Vector {
    fn from(value: segment::data_types::vectors::Vector) -> Self {
        match value {
            segment::data_types::vectors::Vector::Dense(vector) => Vector::Dense(vector),
            segment::data_types::vectors::Vector::Sparse(vector) => Vector::Sparse(vector),
            segment::data_types::vectors::Vector::MultiDense(_vector) => {
                // TODO(colbert)
                unimplemented!()
            }
        }
    }
}

impl From<Vector> for segment::data_types::vectors::Vector {
    fn from(value: Vector) -> Self {
        match value {
            Vector::Dense(vector) => segment::data_types::vectors::Vector::Dense(vector),
            Vector::Sparse(vector) => segment::data_types::vectors::Vector::Sparse(vector),
        }
    }
}

impl From<segment::data_types::vectors::VectorStruct> for VectorStruct {
    fn from(value: segment::data_types::vectors::VectorStruct) -> Self {
        match value {
            segment::data_types::vectors::VectorStruct::Single(vector) => {
                VectorStruct::Single(vector)
            }
            segment::data_types::vectors::VectorStruct::Multi(vectors) => {
                VectorStruct::Multi(vectors.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
        }
    }
}

impl From<VectorStruct> for segment::data_types::vectors::VectorStruct {
    fn from(value: VectorStruct) -> Self {
        match value {
            VectorStruct::Single(vector) => {
                segment::data_types::vectors::VectorStruct::Single(vector)
            }
            VectorStruct::Multi(vectors) => segment::data_types::vectors::VectorStruct::Multi(
                vectors.into_iter().map(|(k, v)| (k, v.into())).collect(),
            ),
        }
    }
}

impl From<segment::data_types::vectors::BatchVectorStruct> for BatchVectorStruct {
    fn from(value: segment::data_types::vectors::BatchVectorStruct) -> Self {
        match value {
            segment::data_types::vectors::BatchVectorStruct::Single(vector) => {
                BatchVectorStruct::Single(vector)
            }
            segment::data_types::vectors::BatchVectorStruct::Multi(vectors) => {
                BatchVectorStruct::Multi(
                    vectors
                        .into_iter()
                        .map(|(k, v)| (k, v.into_iter().map(|v| v.into()).collect()))
                        .collect(),
                )
            }
        }
    }
}

impl From<BatchVectorStruct> for segment::data_types::vectors::BatchVectorStruct {
    fn from(value: BatchVectorStruct) -> Self {
        match value {
            BatchVectorStruct::Single(vector) => {
                segment::data_types::vectors::BatchVectorStruct::Single(vector)
            }
            BatchVectorStruct::Multi(vectors) => {
                segment::data_types::vectors::BatchVectorStruct::Multi(
                    vectors
                        .into_iter()
                        .map(|(k, v)| (k, v.into_iter().map(|v| v.into()).collect()))
                        .collect(),
                )
            }
        }
    }
}

impl From<segment::types::ScoredPoint> for ScoredPoint {
    fn from(value: segment::types::ScoredPoint) -> Self {
        ScoredPoint {
            id: value.id,
            version: value.version,
            score: value.score,
            payload: value.payload,
            vector: value.vector.map(From::from),
            shard_key: value.shard_key,
        }
    }
}

impl From<ScoredPoint> for segment::types::ScoredPoint {
    fn from(value: ScoredPoint) -> Self {
        segment::types::ScoredPoint {
            id: value.id,
            version: value.version,
            score: value.score,
            payload: value.payload,
            vector: value.vector.map(From::from),
            shard_key: value.shard_key,
        }
    }
}

impl From<segment::types::SegmentConfig> for SegmentConfig {
    fn from(segment_config: segment::types::SegmentConfig) -> Self {
        Self {
            vector_data: segment_config.vector_data,
            sparse_vector_data: segment_config.sparse_vector_data,
            payload_storage_type: segment_config.payload_storage_type,
        }
    }
}

impl From<segment::telemetry::SegmentTelemetry> for SegmentTelemetry {
    fn from(segment_telemetry: segment::telemetry::SegmentTelemetry) -> Self {
        Self {
            info: segment_telemetry.info,
            config: SegmentConfig::from(segment_telemetry.config),
            vector_index_searches: segment_telemetry.vector_index_searches,
            payload_field_indices: segment_telemetry.payload_field_indices,
        }
    }
}
