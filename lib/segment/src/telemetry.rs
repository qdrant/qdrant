use crate::types::{SegmentConfig, SegmentInfo};
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

impl SegmentTelemetry {
    pub fn anonymize(&mut self) {
        self.config.vector_size = telemetry_round(self.config.vector_size);
    }
}

impl TelemetryOperationStatistics {
    pub fn anonymize(&mut self) {
        self.ok_count = telemetry_round(self.ok_count);
        self.fail_count = telemetry_round(self.fail_count);
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
