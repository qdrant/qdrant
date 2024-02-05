use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SubsecRound, Utc};
use common::types::TelemetryDetail;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::anonymize::Anonymize;

const AVG_DATASET_LEN: usize = 128;
const SLIDING_WINDOW_LEN: usize = 8;

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct OperationDurationStatistics {
    pub count: usize,

    #[serde(skip_serializing_if = "num_traits::identities::Zero::is_zero")]
    #[serde(default)]
    pub fail_count: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub avg_duration_micros: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub min_duration_micros: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub max_duration_micros: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_responded: Option<DateTime<Utc>>,
}

pub struct OperationDurationsAggregator {
    ok_count: usize,
    fail_count: usize,
    timings: [f32; AVG_DATASET_LEN],
    timing_index: usize,
    timing_loops: usize,
    min_value: Option<f32>,
    max_value: Option<f32>,
    last_response_date: Option<DateTime<Utc>>,
}

pub struct ScopeDurationMeasurer<'a> {
    aggregator: &'a Mutex<OperationDurationsAggregator>,
    instant: Instant,
    success: bool,
}

impl Anonymize for OperationDurationStatistics {
    fn anonymize(&self) -> Self {
        Self {
            count: self.count.anonymize(),
            fail_count: self.fail_count.anonymize(),
            last_responded: self.last_responded.anonymize(),
            ..*self
        }
    }
}

impl std::ops::Add for OperationDurationStatistics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            count: self.count + other.count,
            fail_count: self.fail_count + other.fail_count,
            avg_duration_micros: Self::weighted_mean_duration(
                self.avg_duration_micros,
                self.count,
                other.avg_duration_micros,
                other.count,
            ),
            min_duration_micros: Self::compared_duration(
                self.min_duration_micros,
                other.min_duration_micros,
                |a, b| a < b,
            ),
            max_duration_micros: Self::compared_duration(
                self.max_duration_micros,
                other.max_duration_micros,
                |a, b| a > b,
            ),
            last_responded: std::cmp::max(self.last_responded, other.last_responded),
        }
    }
}

impl OperationDurationStatistics {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn weighted_mean_duration(
        duration1: Option<f32>,
        count1: usize,
        duration2: Option<f32>,
        count2: usize,
    ) -> Option<f32> {
        if let Some(duration1) = duration1 {
            if let Some(duration2) = duration2 {
                let count1 = count1 as f32;
                let count2 = count2 as f32;
                Some((duration1 * count1 + duration2 * count2) / (count1 + count2))
            } else {
                Some(duration1)
            }
        } else {
            duration2
        }
    }

    fn compared_duration(
        duration1: Option<f32>,
        duration2: Option<f32>,
        compare: impl Fn(f32, f32) -> bool,
    ) -> Option<f32> {
        if let Some(duration1) = duration1 {
            if let Some(duration2) = duration2 {
                if compare(duration1, duration2) {
                    Some(duration1)
                } else {
                    Some(duration2)
                }
            } else {
                Some(duration1)
            }
        } else {
            duration2
        }
    }
}

impl<'a> ScopeDurationMeasurer<'a> {
    pub fn new(aggregator: &'a Mutex<OperationDurationsAggregator>) -> Self {
        Self {
            aggregator,
            instant: Instant::now(),
            success: true,
        }
    }

    pub fn new_with_instant(
        aggregator: &'a Mutex<OperationDurationsAggregator>,
        instant: Instant,
    ) -> Self {
        Self {
            aggregator,
            instant,
            success: true,
        }
    }

    pub fn set_success(&mut self, success: bool) {
        self.success = success
    }
}

impl Drop for ScopeDurationMeasurer<'_> {
    fn drop(&mut self) {
        self.aggregator
            .lock()
            .add_operation_result(self.success, self.instant.elapsed());
    }
}

impl OperationDurationsAggregator {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ok_count: 0,
            fail_count: 0,
            timings: [0.; AVG_DATASET_LEN],
            timing_index: 0,
            timing_loops: 0,
            min_value: None,
            max_value: None,
            last_response_date: Some(Utc::now().round_subsecs(2)),
        }))
    }

    pub fn add_operation_result(&mut self, success: bool, duration: Duration) {
        if success {
            let duration = duration.as_micros() as f32;
            self.min_value = Some(match self.min_value {
                Some(min_value) => min_value.min(duration),
                None => duration,
            });
            self.max_value = Some(match self.max_value {
                Some(max_value) => max_value.max(duration),
                None => duration,
            });

            self.ok_count += 1;
            self.timings[self.timing_index] = duration;
            self.timing_index += 1;
            if self.timing_index >= AVG_DATASET_LEN {
                self.timing_index = 0;
                self.timing_loops += 1;
            }
        } else {
            self.fail_count += 1;
        }

        self.last_response_date = Some(Utc::now().round_subsecs(2));
    }

    pub fn get_statistics(&self, detail: TelemetryDetail) -> OperationDurationStatistics {
        let _ = detail.histograms; // TODO: Will be used once histograms PR is merged
        OperationDurationStatistics {
            count: self.ok_count,
            fail_count: self.fail_count,
            avg_duration_micros: if self.ok_count > 0 {
                Some(self.calculate_avg())
            } else {
                None
            },
            min_duration_micros: self.min_value,
            max_duration_micros: self.max_value,
            last_responded: self.last_response_date,
        }
    }

    fn calculate_avg(&self) -> f32 {
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

        Self::simple_moving_average(&sliding_window_avg)
    }

    fn simple_moving_average(data: &[f32]) -> f32 {
        data.iter().sum::<f32>() / data.len() as f32
    }
}
