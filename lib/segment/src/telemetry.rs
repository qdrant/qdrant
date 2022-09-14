use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{Indexes, SegmentConfig, SegmentInfo, VectorDataConfig};

const AVG_DATASET_LEN: usize = 128;
const SLIDING_WINDOW_LEN: usize = 8;

pub trait Anonymize {
    fn anonymize(&self) -> Self;
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TelemetrySegmentConfig {
    storage_type: String,
    index_type: String,
}

impl From<SegmentConfig> for TelemetrySegmentConfig {
    fn from(config: SegmentConfig) -> Self {
        Self {
            storage_type: format!("{:?}", config.storage_type),
            index_type: match config.index {
                Indexes::Plain { .. } => "plain".to_string(),
                Indexes::Hnsw(_) => "hnsw".to_string(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TelemetrySegmentInfo {
    pub segment_type: String,
    pub num_vectors: usize,
    pub num_points: usize,
    pub num_deleted_vectors: usize,
    pub ram_usage_bytes: usize,
    pub disk_usage_bytes: usize,
    pub is_appendable: bool,
}

impl From<SegmentInfo> for TelemetrySegmentInfo {
    fn from(info: SegmentInfo) -> Self {
        Self {
            segment_type: format!("{:?}", info.segment_type),
            num_vectors: info.num_vectors,
            num_points: info.num_points,
            num_deleted_vectors: info.num_deleted_vectors,
            ram_usage_bytes: info.ram_usage_bytes,
            disk_usage_bytes: info.disk_usage_bytes,
            is_appendable: info.is_appendable,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct SegmentTelemetry {
    pub info: TelemetrySegmentInfo,
    pub config: TelemetrySegmentConfig,
    pub vector_index: HashMap<String, VectorIndexTelemetry>,
    pub payload_field_indices: Vec<PayloadIndexTelemetry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct PayloadIndexTelemetry {
    pub points_values_count: usize,
    pub points_count: usize,
    pub histogram_bucket_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct VectorIndexTelemetry {
    pub small_cardinality_searches: TelemetryOperationStatistics,
    pub large_cardinality_searches: TelemetryOperationStatistics,
    pub positive_check_cardinality_searches: TelemetryOperationStatistics,
    pub negative_check_cardinality_searches: TelemetryOperationStatistics,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct TelemetryOperationStatistics {
    pub ok_count: usize,
    pub fail_count: usize,
    pub ok_avg_time: Duration,
}

pub struct TelemetryOperationAggregator {
    ok_count: usize,
    fail_count: usize,
    timings: [f32; AVG_DATASET_LEN],
    timing_index: usize,
    timing_loops: usize,
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
            vector_index: self
                .vector_index
                .iter()
                .map(|(k, v)| (telemetry_hash(k), v.anonymize()))
                .collect(),
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
        self.aggregator
            .lock()
            .add_operation_result(self.success, self.instant.elapsed());
    }
}

impl TelemetryOperationAggregator {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ok_count: 0,
            fail_count: 0,
            timings: [0.; AVG_DATASET_LEN],
            timing_index: 0,
            timing_loops: 0,
        }))
    }

    pub fn add_operation_result(&mut self, success: bool, duration: Duration) {
        if success {
            self.ok_count += 1;
            self.timings[self.timing_index] = duration.as_micros() as f32;
            self.timing_index += 1;
            if self.timing_index >= AVG_DATASET_LEN {
                self.timing_index = 0;
                self.timing_loops += 1;
            }
        } else {
            self.fail_count += 1;
        }
    }

    pub fn get_statistics(&self) -> TelemetryOperationStatistics {
        TelemetryOperationStatistics {
            ok_count: self.ok_count,
            fail_count: self.fail_count,
            ok_avg_time: self.calculate_avg(),
        }
    }

    fn calculate_avg(&self) -> Duration {
        let data: Vec<f32> = if self.timing_loops > 0 {
            let mut result = Vec::new();
            result.extend_from_slice(&self.timings[self.timing_index..]);
            result.extend_from_slice(&self.timings[..self.timing_index]);
            result
        } else {
            self.timings[..self.timing_index].to_vec()
        };

        let mut sliding_window_avg = vec![0.; data.len()];
        for i in 0..data.len() {
            let from = if i < SLIDING_WINDOW_LEN {
                0
            } else {
                i - SLIDING_WINDOW_LEN
            };
            sliding_window_avg[i] = Self::simple_moving_average(&data[from..i + 1]);
        }

        let avg = Self::simple_moving_average(&sliding_window_avg);
        Duration::from_micros(avg as u64)
    }

    fn simple_moving_average(data: &[f32]) -> f32 {
        data.iter().sum::<f32>() / data.len() as f32
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

impl Anonymize for TelemetrySegmentInfo {
    fn anonymize(&self) -> Self {
        TelemetrySegmentInfo {
            segment_type: self.segment_type.clone(),
            num_vectors: telemetry_round(self.num_vectors),
            num_points: telemetry_round(self.num_points),
            num_deleted_vectors: telemetry_round(self.num_deleted_vectors),
            ram_usage_bytes: self.ram_usage_bytes,
            disk_usage_bytes: self.disk_usage_bytes,
            is_appendable: self.is_appendable,
        }
    }
}

impl Anonymize for TelemetrySegmentConfig {
    fn anonymize(&self) -> Self {
        TelemetrySegmentConfig {
            storage_type: self.storage_type.clone(),
            index_type: self.index_type.clone(),
        }
    }
}

impl Anonymize for VectorDataConfig {
    fn anonymize(&self) -> Self {
        VectorDataConfig {
            size: telemetry_round(self.size),
            distance: self.distance,
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
        PayloadIndexTelemetry {
            points_count: telemetry_round(self.points_count),
            points_values_count: telemetry_round(self.points_values_count),
            histogram_bucket_size: self.histogram_bucket_size,
        }
    }
}
