use segment::types::{VectorElementType, PointIdType, TheMap, PayloadKeyType, PayloadType, SeqNumberType, Filter, SearchParams, SegmentConfig};
use serde;
use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};

/// Type of vector in API
pub type VectorType = Vec<VectorElementType>;


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Point data
pub struct Record {
    /// Id of the point
    pub id: PointIdType,
    /// Payload - values assigned to the point
    pub payload: Option<TheMap<PayloadKeyType, PayloadType>>,
    /// Vector of the point
    pub vector: Option<Vec<VectorElementType>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
/// Current statistics and configuration of the collection.
pub struct CollectionInfo {
    /// Number of vectors in collection
    pub vectors_count: usize,
    /// Number of segments in collection
    pub segments_count: usize,
    /// Disk space, used by collection
    pub disk_data_size: usize,
    /// RAM used by collection
    pub ram_data_size: usize,
    /// Collection settings
    pub config: SegmentConfig,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    /// Request is saved to WAL and will be process in a queue
    Acknowledged,
    /// Request is completed, changes are actual
    Completed,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateResult {
    /// Sequential number of the operation
    pub operation_id: SeqNumberType,
    /// Update status
    pub status: UpdateStatus,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Search request
pub struct SearchRequest {
    /// Look for vectors closest to this
    pub vector: Vec<VectorElementType>,
    /// Look only for points which satisfies this conditions
    pub filter: Option<Filter>,
    /// Additional search params
    pub params: Option<SearchParams>,
    /// Max number of result to return
    pub top: usize,
}


