use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;
use crate::common::operation_time_statistics::OperationDurationStatistics;
use crate::types::{SegmentConfig, SegmentInfo, VectorDataConfig};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct SegmentTelemetry {
    pub info: SegmentInfo,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub config: Option<SegmentConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub vector_index_searches: Option<VectorIndexSearchesTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub vector_index_searches_with_names: Option<HashMap<String, VectorIndexSearchesTelemetry>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub payload_field_indices: Option<Vec<PayloadIndexTelemetry>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PayloadIndexTelemetry {
    pub points_values_count: usize,
    pub points_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub histogram_bucket_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub struct VectorIndexSearchesTelemetry {
    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub unfiltered_plain_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub unfiltered_hnsw_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub filtered_plain_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub filtered_small_cardinality_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub filtered_large_cardinality_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub filtered_positive_check_cardinality_searches: OperationDurationStatistics,

    #[serde(skip_serializing_if = "OperationDurationStatistics::is_empty")]
    #[serde(default)]
    pub filtered_negative_check_cardinality_searches: OperationDurationStatistics,
}

impl std::ops::Add for VectorIndexSearchesTelemetry {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            unfiltered_plain_searches: self.unfiltered_plain_searches
                + other.unfiltered_plain_searches,
            unfiltered_hnsw_searches: self.unfiltered_hnsw_searches
                + other.unfiltered_hnsw_searches,
            filtered_plain_searches: self.filtered_plain_searches + other.filtered_plain_searches,
            filtered_small_cardinality_searches: self.filtered_small_cardinality_searches
                + other.filtered_small_cardinality_searches,
            filtered_large_cardinality_searches: self.filtered_large_cardinality_searches
                + other.filtered_large_cardinality_searches,
            filtered_positive_check_cardinality_searches: self
                .filtered_positive_check_cardinality_searches
                + other.filtered_positive_check_cardinality_searches,
            filtered_negative_check_cardinality_searches: self
                .filtered_negative_check_cardinality_searches
                + other.filtered_negative_check_cardinality_searches,
        }
    }
}

impl Anonymize for SegmentTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            info: self.info.anonymize(),
            config: self.config.anonymize(),
            vector_index_searches: self.vector_index_searches.anonymize(),
            vector_index_searches_with_names: self.vector_index_searches_with_names.anonymize(),
            payload_field_indices: self.payload_field_indices.anonymize(),
        }
    }
}

impl Anonymize for SegmentInfo {
    fn anonymize(&self) -> Self {
        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: self.num_vectors.anonymize(),
            num_points: self.num_points.anonymize(),
            num_deleted_vectors: self.num_deleted_vectors.anonymize(),
            ram_usage_bytes: self.ram_usage_bytes.anonymize(),
            disk_usage_bytes: self.disk_usage_bytes.anonymize(),
            is_appendable: self.is_appendable,
            index_schema: self.index_schema.clone(),
        }
    }
}

impl Anonymize for SegmentConfig {
    fn anonymize(&self) -> Self {
        SegmentConfig {
            vector_data: self.vector_data.anonymize(),
            index: self.index,
            storage_type: self.storage_type,
            payload_storage_type: self.payload_storage_type,
        }
    }
}

impl Anonymize for VectorDataConfig {
    fn anonymize(&self) -> Self {
        VectorDataConfig {
            size: self.size.anonymize(),
            distance: self.distance,
        }
    }
}

impl Anonymize for VectorIndexSearchesTelemetry {
    fn anonymize(&self) -> Self {
        VectorIndexSearchesTelemetry {
            unfiltered_plain_searches: self.unfiltered_plain_searches.anonymize(),
            unfiltered_hnsw_searches: self.unfiltered_hnsw_searches.anonymize(),
            filtered_plain_searches: self.filtered_plain_searches.anonymize(),
            filtered_small_cardinality_searches: self
                .filtered_small_cardinality_searches
                .anonymize(),
            filtered_large_cardinality_searches: self
                .filtered_large_cardinality_searches
                .anonymize(),
            filtered_positive_check_cardinality_searches: self
                .filtered_positive_check_cardinality_searches
                .anonymize(),
            filtered_negative_check_cardinality_searches: self
                .filtered_negative_check_cardinality_searches
                .anonymize(),
        }
    }
}

impl Anonymize for PayloadIndexTelemetry {
    fn anonymize(&self) -> Self {
        PayloadIndexTelemetry {
            points_count: self.points_count.anonymize(),
            points_values_count: self.points_values_count.anonymize(),
            histogram_bucket_size: self.histogram_bucket_size,
        }
    }
}
