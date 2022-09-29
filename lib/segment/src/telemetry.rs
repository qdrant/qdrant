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
    pub cardinality_searches: Option<CardinalitySearchesTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub cardinality_searches_by_vectors: Option<HashMap<String, CardinalitySearchesTelemetry>>,

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
pub struct CardinalitySearchesTelemetry {
    pub small_cardinality_searches: OperationDurationStatistics,
    pub large_cardinality_searches: OperationDurationStatistics,
    pub positive_check_cardinality_searches: OperationDurationStatistics,
    pub negative_check_cardinality_searches: OperationDurationStatistics,
}

impl std::ops::Add for CardinalitySearchesTelemetry {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            small_cardinality_searches: self.small_cardinality_searches
                + other.small_cardinality_searches,
            large_cardinality_searches: self.large_cardinality_searches
                + other.large_cardinality_searches,
            positive_check_cardinality_searches: self.positive_check_cardinality_searches
                + other.positive_check_cardinality_searches,
            negative_check_cardinality_searches: self.negative_check_cardinality_searches
                + other.negative_check_cardinality_searches,
        }
    }
}

impl CardinalitySearchesTelemetry {
    pub fn get_total_searches(&self) -> OperationDurationStatistics {
        self.small_cardinality_searches.clone()
            + self.large_cardinality_searches.clone()
            + self.positive_check_cardinality_searches.clone()
            + self.negative_check_cardinality_searches.clone()
    }
}

impl Anonymize for SegmentTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            info: self.info.anonymize(),
            config: self.config.anonymize(),
            cardinality_searches: self.cardinality_searches.anonymize(),
            cardinality_searches_by_vectors: self.cardinality_searches_by_vectors.anonymize(),
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

impl Anonymize for CardinalitySearchesTelemetry {
    fn anonymize(&self) -> Self {
        CardinalitySearchesTelemetry {
            small_cardinality_searches: self.small_cardinality_searches.anonymize(),
            large_cardinality_searches: self.large_cardinality_searches.anonymize(),
            positive_check_cardinality_searches: self
                .positive_check_cardinality_searches
                .anonymize(),
            negative_check_cardinality_searches: self
                .negative_check_cardinality_searches
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
