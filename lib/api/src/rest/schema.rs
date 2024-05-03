use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Type for dense vector
pub type DenseVector = Vec<segment::data_types::vectors::VectorElementType>;

/// Type for multi dense vector
pub type MultiDenseVector = Vec<DenseVector>;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum Vector {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
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
                Vector::MultiDense(vector) => vector.is_empty(),
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

/// Vector data separator for named and unnamed modes
/// Unnamed mode:
///
/// {
///   "vector": [1.0, 2.0, 3.0]
/// }
///
/// or named mode:
///
/// {
///   "vector": {
///     "vector": [1.0, 2.0, 3.0],
///     "name": "image-embeddings"
///   }
/// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum NamedVectorStruct {
    Default(segment::data_types::vectors::DenseVector),
    Dense(segment::data_types::vectors::NamedVector),
    Sparse(segment::data_types::vectors::NamedSparseVector),
    // No support for multi-dense vectors in search
}
