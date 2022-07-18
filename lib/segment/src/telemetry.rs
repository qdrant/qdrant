use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::types::{SegmentConfig, SegmentInfo};

pub trait Anonymize {
    fn anonymize(&self) -> Self;
}

#[derive(Serialize, Clone)]
pub struct SegmentTelemetry {
    pub info: SegmentInfo,
    pub config: SegmentConfig,
    pub vector_index: VectorIndexTelemetry,
    pub payload_field_indices: Vec<PayloadIndexTelemetry>,
}

#[derive(Serialize, Clone)]
pub struct PayloadIndexTelemetry {}

#[derive(Serialize, Clone)]
pub struct VectorIndexTelemetry {
    pub small_cardinality_searches: TelemetryOperationStatistics,
    pub large_cardinality_searches: TelemetryOperationStatistics,
    pub positive_check_cardinality_searches: TelemetryOperationStatistics,
    pub negative_check_cardinality_searches: TelemetryOperationStatistics,
}

#[derive(Serialize, Clone, Default)]
pub struct TelemetryOperationStatistics {
    pub ok_count: usize,
    pub fail_count: usize,
    pub ok_avg_time: Duration,
    pub ok_min_time: Duration,
    pub ok_max_time: Duration,
}

pub struct TelemetryOperationAggregator {
    ok_count: usize,
    fail_count: usize,
}

pub struct TelemetryOperationTimer {
    aggregator: Arc<Mutex<TelemetryOperationAggregator>>,
    instant: Instant,
    success: bool,
}

impl Anonymize for SegmentTelemetry {
    fn anonymize(&self) -> Self {
        Self {
            info: self.info.anonymize(),
            config: self.config.anonymize(),
            vector_index: self.vector_index.anonymize(),
            payload_field_indices: self
                .payload_field_indices
                .iter()
                .map(|t| t.anonymize())
                .collect(),
        }
    }
}

impl Anonymize for TelemetryOperationStatistics {
    fn anonymize(&self) -> Self {
        Self {
            ok_count: telemetry_round(self.ok_count),
            fail_count: telemetry_round(self.fail_count),
            ok_avg_time: self.ok_avg_time,
            ok_min_time: self.ok_min_time,
            ok_max_time: self.ok_max_time,
        }
    }
}

impl TelemetryOperationTimer {
    pub fn new(aggregator: &Arc<Mutex<TelemetryOperationAggregator>>) -> Self {
        Self {
            aggregator: aggregator.clone(),
            instant: Instant::now(),
            success: true,
        }
    }

    pub fn set_success(&mut self, success: bool) {
        self.success = success
    }
}

impl Drop for TelemetryOperationTimer {
    fn drop(&mut self) {
        if let Ok(mut aggregator) = self.aggregator.lock() {
            aggregator.add_operation_result(self.success, self.instant.elapsed());
        }
    }
}

impl TelemetryOperationAggregator {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ok_count: 0,
            fail_count: 0,
        }))
    }

    pub fn add_operation_result(&mut self, success: bool, _duration: Duration) {
        if success {
            self.ok_count += 1;
        } else {
            self.fail_count += 1;
        }
    }

    pub fn get_statistics(&self) -> TelemetryOperationStatistics {
        // todo(ivan): choose calculating avg time method
        TelemetryOperationStatistics {
            ok_count: self.ok_count,
            fail_count: self.fail_count,
            ok_avg_time: Duration::default(),
            ok_min_time: Duration::default(),
            ok_max_time: Duration::default(),
        }
    }
}

pub fn telemetry_hash(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish().to_string()
}

pub fn telemetry_round(cnt: usize) -> usize {
    let leading_zeros = cnt.leading_zeros();
    let skip_bytes_count = if leading_zeros > 4 {
        leading_zeros - 4
    } else {
        0
    };
    (cnt >> skip_bytes_count) << skip_bytes_count
}

impl Anonymize for SegmentInfo {
    fn anonymize(&self) -> Self {
        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors: telemetry_round(self.num_vectors),
            num_points: telemetry_round(self.num_points),
            num_deleted_vectors: telemetry_round(self.num_deleted_vectors),
            ram_usage_bytes: self.ram_usage_bytes,
            disk_usage_bytes: self.disk_usage_bytes,
            is_appendable: self.is_appendable,
            index_schema: self.index_schema.clone(),
        }
    }
}

impl Anonymize for SegmentConfig {
    fn anonymize(&self) -> Self {
        SegmentConfig {
            vector_size: telemetry_round(self.vector_size),
            distance: self.distance,
            index: self.index,
            storage_type: self.storage_type,
            payload_storage_type: self.payload_storage_type,
        }
    }
}

impl Anonymize for VectorIndexTelemetry {
    fn anonymize(&self) -> Self {
        VectorIndexTelemetry {
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
        PayloadIndexTelemetry {}
    }
}
