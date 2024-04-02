use std::collections::HashMap;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::telemetry::{PayloadIndexTelemetry, VectorIndexSearchesTelemetry};
use segment::types::{PayloadStorageType, SegmentInfo, SparseVectorDataConfig, VectorDataConfig};
use serde::{Deserialize, Serialize};

/// Type for dense vector
pub type DenseVector = Vec<segment::data_types::vectors::VectorElementType>;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Vector {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
}

/// Full vector data per point separator with single and multiple vector modes
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStruct {
    Single(DenseVector),
    Multi(HashMap<String, Vector>),
}

impl VectorStruct {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStruct::Single(vector) => vector.is_empty(),
            VectorStruct::Multi(vectors) => vectors.values().all(|v| match v {
                Vector::Dense(vector) => vector.is_empty(),
                Vector::Sparse(vector) => vector.indices.is_empty(),
            }),
        }
    }

    /// TODO(colbert): remove this method and use `merge` from segment::VectorStruct
    pub fn merge(&mut self, other: Self) {
        match (self, other) {
            // If other is empty, merge nothing
            (_, VectorStruct::Multi(other)) if other.is_empty() => {}
            // Single overwrites single
            (VectorStruct::Single(this), VectorStruct::Single(other)) => {
                *this = other;
            }
            // If multi into single, convert this to multi and merge
            (this @ VectorStruct::Single(_), other @ VectorStruct::Multi(_)) => {
                let VectorStruct::Single(single) = this.clone() else {
                    unreachable!();
                };
                *this =
                    VectorStruct::Multi(HashMap::from([(String::new(), Vector::Dense(single))]));
                this.merge(other);
            }
            // Single into multi
            (VectorStruct::Multi(this), VectorStruct::Single(other)) => {
                this.insert(String::new(), Vector::Dense(other));
            }
            // Multi into multi
            (VectorStruct::Multi(this), VectorStruct::Multi(other)) => this.extend(other),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStruct {
    Single(Vec<DenseVector>),
    Multi(HashMap<String, Vec<Vector>>),
}

/// Search result
#[derive(Serialize, JsonSchema, Clone, Debug)]
pub struct ScoredPoint {
    /// Point id
    pub id: segment::types::PointIdType,
    /// Point version
    pub version: segment::types::SeqNumberType,
    /// Points vector distance to the query vector
    pub score: common::types::ScoreType,
    /// Payload - values assigned to the point
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    pub vector: Option<VectorStruct>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<segment::types::ShardKey>,
}

/// Point data
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    /// Id of the point
    pub id: segment::types::PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<segment::types::Payload>,
    /// Vector of the point
    pub vector: Option<VectorStruct>,
    /// Shard Key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<segment::types::ShardKey>,
}

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct SegmentConfig {
    #[serde(default)]
    pub vector_data: HashMap<String, VectorDataConfig>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub sparse_vector_data: HashMap<String, SparseVectorDataConfig>,
    /// Defines payload storage type
    pub payload_storage_type: PayloadStorageType,
}

impl Anonymize for SegmentConfig {
    fn anonymize(&self) -> Self {
        SegmentConfig {
            vector_data: self.vector_data.anonymize(),
            sparse_vector_data: self.sparse_vector_data.anonymize(),
            payload_storage_type: self.payload_storage_type,
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct SegmentTelemetry {
    pub info: SegmentInfo,
    pub config: SegmentConfig,
    pub vector_index_searches: Vec<VectorIndexSearchesTelemetry>,
    pub payload_field_indices: Vec<PayloadIndexTelemetry>,
}

impl Anonymize for SegmentTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            info: self.info.anonymize(),
            config: self.config.anonymize(),
            vector_index_searches: self.vector_index_searches.anonymize(),
            payload_field_indices: self.payload_field_indices.anonymize(),
        }
    }
}
