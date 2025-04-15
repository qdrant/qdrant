use schemars::JsonSchema;
use serde::Serialize;

use crate::common::anonymize::Anonymize;
use crate::common::operation_time_statistics::OperationDurationStatistics;
use crate::types::{SegmentConfig, SegmentInfo, VectorNameBuf};

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct SegmentTelemetry {
    pub info: SegmentInfo,
    pub config: SegmentConfig,
    pub vector_index_searches: Vec<VectorIndexSearchesTelemetry>,
    pub payload_field_indices: Vec<PayloadIndexTelemetry>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct PayloadIndexTelemetry {
    #[anonymize(value = None)]
    pub field_name: Option<String>,

    #[anonymize(false)]
    pub index_type: &'static str,

    /// The amount of values indexed for all points.
    pub points_values_count: usize,

    /// The amount of points that have at least one value indexed.
    pub points_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub histogram_bucket_size: Option<usize>,
}

impl PayloadIndexTelemetry {
    pub fn set_name(mut self, name: String) -> Self {
        self.field_name = Some(name);
        self
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize, Default)]
pub struct VectorIndexSearchesTelemetry {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[anonymize(value = None)]
    pub index_name: Option<VectorNameBuf>,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub unfiltered_plain: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub unfiltered_hnsw: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub unfiltered_sparse: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub filtered_plain: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub filtered_small_cardinality: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub filtered_large_cardinality: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub filtered_exact: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub filtered_sparse: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    pub unfiltered_exact: OperationDurationStatistics,
}
