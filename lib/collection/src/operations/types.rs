use segment::types::{VectorElementType, PointIdType, TheMap, PayloadKeyType, PayloadType, SeqNumberType, Filter, SearchParams, SegmentConfig};
use serde;
use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};

/// Type of vector in API
pub type VectorType = Vec<VectorElementType>;


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Record {
    pub id: PointIdType,
    pub payload: Option<TheMap<PayloadKeyType, PayloadType>>,
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
    Acknowledged,
    Completed,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateResult {
    pub operation_id: SeqNumberType,
    pub status: UpdateStatus,
}


#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SearchRequest {
    pub vector: Vec<VectorElementType>,
    pub filter: Option<Filter>,
    pub params: Option<SearchParams>,
    pub top: usize,
}


