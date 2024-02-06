use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SubsecRound, Utc};
use common::types::TelemetryDetail;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::common::anonymize::Anonymize;

const AVG_DATASET_LEN: usize = 128;
const SLIDING_WINDOW_LEN: usize = 8;

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct OperationDurationStatistics {
    pub count: usize,

    #[serde(skip_serializing_if = "num_traits::identities::Zero::is_zero")]
    #[serde(default)]
    pub fail_count: usize,

    /// The average time taken by 128 latest operations, calculated as a weighted mean.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub avg_duration_micros: Option<f32>,

    /// The minimum duration of the operations across all the measurements.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub min_duration_micros: Option<f32>,

    /// The maximum duration of the operations across all the measurements.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub max_duration_micros: Option<f32>,

    /// The total duration of all operations in microseconds.
    #[serde(default)]
    pub total_duration_micros: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_responded: Option<DateTime<Utc>>,

    /// The cumulative histogram of the operation durations. Consists of a list of pairs of
    /// [upper_boundary, cumulative_count], sorted by the upper boundary. Note that the last bucket
    /// (aka `{le="+Inf"}` in Prometheus terms) is not stored in this list, and `count` should be
    /// used instead.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub duration_micros_histogram: Vec<(f32, usize)>,
}

pub struct OperationDurationsAggregator {
    ok_count: usize,
    fail_count: usize,
    timings: [f32; AVG_DATASET_LEN],
    timing_index: usize,
    timing_loops: usize,
    min_value: Option<f32>,
    max_value: Option<f32>,
    total_value: u64,
    last_response_date: Option<DateTime<Utc>>,

    /// The non-cumulative count of operations in each bucket.
    /// The total operations count (aka the last bucket, or `{le="+Inf"}` in Prometheus terms) is
    /// not stored in this vector, and `ok_count` should be used instead.
    buckets: SmallVec<[usize; 16]>,
}

pub const DEFAULT_BUCKET_BOUNDARIES_MICROS: [f32; 16] = [
    // Microseconds
    1.0,
    5.0,
    10.0,
    50.0,
    100.0,
    500.0,
    // Milliseconds
    1_000.0,
    5_000.0,
    10_000.0,
    50_000.0,
    100_000.0,
    500_000.0,
    // Seconds
    1_000_000.0,
    5_000_000.0,
    10_000_000.0,
    50_000_000.0,
];

/// A wrapper around [`OperationDurationsAggregator`] that calls
/// [`OperationDurationsAggregator::add_operation_result()`] on drop.
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
            duration_micros_histogram: self
                .duration_micros_histogram
                .iter()
                .map(|&(le, count)| (le, count.anonymize()))
                .collect(),
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
            total_duration_micros: self.total_duration_micros + other.total_duration_micros,
            last_responded: std::cmp::max(self.last_responded, other.last_responded),
            duration_micros_histogram: merge_buckets(
                self.duration_micros_histogram,
                other.duration_micros_histogram,
            ),
        }
    }
}

/// Merges two vectors of buckets, summing the counts of the same boundaries.
/// # Panics
/// Panics when merging buckets of different configurations.  In practice, we should stick to a
/// single configuration so this should not happen.
fn merge_buckets(mut a: Vec<(f32, usize)>, b: Vec<(f32, usize)>) -> Vec<(f32, usize)> {
    if a.is_empty() {
        return b;
    }
    if b.is_empty() {
        return a;
    }
    if a.len() != b.len() {
        eprintln!("a = {a:?}, b = {b:?}");
        panic!("Cannot merge buckets of different lengths");
    }
    for ((a_le, a_count), (b_le, b_count)) in a.iter_mut().zip(b.iter()) {
        if a_le != b_le {
            eprintln!("a = {a:?}, b = {b:?}");
            panic!("Cannot merge buckets with different boundaries");
        }
        *a_count += b_count;
    }
    a
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
            total_value: 0,
            last_response_date: Some(Utc::now().round_subsecs(2)),
            buckets: smallvec::smallvec![0; DEFAULT_BUCKET_BOUNDARIES_MICROS.len()],
        }))
    }

    pub fn add_operation_result(&mut self, success: bool, duration: Duration) {
        if success {
            self.total_value += duration.as_micros() as u64;
            let duration = duration.as_micros() as f32;
            self.min_value = Some(match self.min_value {
                Some(min_value) => min_value.min(duration),
                None => duration,
            });
            self.max_value = Some(match self.max_value {
                Some(max_value) => max_value.max(duration),
                None => duration,
            });

            if let Some(bucket_no) = DEFAULT_BUCKET_BOUNDARIES_MICROS
                .iter()
                .position(|&b| duration <= b)
            {
                self.buckets[bucket_no] += 1;
            }

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
        let mut duration_micros_histogram =
            Vec::with_capacity(DEFAULT_BUCKET_BOUNDARIES_MICROS.len());
        let mut cumulative_count = 0;
        for (&count, &le) in self.buckets.iter().zip(&DEFAULT_BUCKET_BOUNDARIES_MICROS) {
            cumulative_count += count;
            duration_micros_histogram.push((le, cumulative_count));
        }

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
            total_duration_micros: self.total_value,
            last_responded: self.last_response_date,
            duration_micros_histogram,
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
