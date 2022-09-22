use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;

const AVG_DATASET_LEN: usize = 128;
const SLIDING_WINDOW_LEN: usize = 8;

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct TelemetryOperationStatistics {
    pub count: usize,

    #[serde(skip_serializing_if = "num_traits::identities::Zero::is_zero")]
    #[serde(default)]
    pub fail_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub avg_time: Option<Duration>,
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

impl Anonymize for TelemetryOperationStatistics {
    fn anonymize(&self) -> Self {
        Self {
            count: self.count.anonymize(),
            fail_count: self.fail_count.anonymize(),
            avg_time: self.avg_time,
        }
    }
}

impl std::ops::Add for TelemetryOperationStatistics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            fail_count: self.fail_count + other.fail_count,
            avg_time: if let Some(self_avg_time) = self.avg_time {
                if let Some(other_avg_time) = other.avg_time {
                    Some(
                        (self_avg_time * self.count as u32 + other_avg_time * other.count as u32)
                            / (self.count + other.count) as u32,
                    )
                } else {
                    Some(self_avg_time)
                }
            } else {
                other.avg_time
            },
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
            count: self.ok_count,
            fail_count: self.fail_count,
            avg_time: if self.ok_count > 0 {
                Some(self.calculate_avg())
            } else {
                None
            },
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
