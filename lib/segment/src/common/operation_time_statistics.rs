use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SubsecRound, Utc};
use common::types::TelemetryDetail;
use is_sorted::IsSorted;
use itertools::Itertools as _;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;
use smallvec::SmallVec;

use crate::common::anonymize::Anonymize;

const AVG_DATASET_LEN: usize = 128;
const SLIDING_WINDOW_LEN: usize = 8;

#[derive(Serialize, Clone, Default, Debug, JsonSchema)]
pub struct OperationDurationStatistics {
    pub count: usize,

    #[serde(skip_serializing_if = "num_traits::identities::Zero::is_zero")]
    #[serde(default)]
    pub fail_count: usize,

    /// The average time taken by 128 latest operations, calculated as a weighted mean.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_duration_micros: Option<f32>,

    /// The minimum duration of the operations across all the measurements.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_duration_micros: Option<f32>,

    /// The maximum duration of the operations across all the measurements.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_duration_micros: Option<f32>,

    /// The total duration of all operations in microseconds.
    pub total_duration_micros: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_responded: Option<DateTime<Utc>>,

    /// The cumulative histogram of the operation durations. Consists of a list of pairs of
    /// [upper_boundary, cumulative_count], sorted by the upper boundary. Note that the last bucket
    /// (aka `{le="+Inf"}` in Prometheus terms) is not stored in this list, and `count` should be
    /// used instead.
    #[serde(skip)] // openapi-generator-cli crashes on this field
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
            duration_micros_histogram: merge_histograms(
                &self.duration_micros_histogram,
                &other.duration_micros_histogram,
                self.count,
                other.count,
            ),
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
        let duration_micros_histogram = if detail.histograms {
            let mut duration_micros_histogram =
                Vec::with_capacity(DEFAULT_BUCKET_BOUNDARIES_MICROS.len());
            let mut cumulative_count = 0;
            for (&count, &le) in self.buckets.iter().zip(&DEFAULT_BUCKET_BOUNDARIES_MICROS) {
                cumulative_count += count;
                duration_micros_histogram.push((le, cumulative_count));
            }
            convert_histogram(
                &DEFAULT_BUCKET_BOUNDARIES_MICROS,
                &self.buckets,
                self.ok_count,
            )
        } else {
            Vec::new()
        };

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

/// Convert a fixed-size non-cumulative histogram to a sparse cumulative histogram.
/// Omit repeated values to reduce the size of the histogram.
fn convert_histogram(
    le_boundaries: &[f32],
    counts: &[usize],
    total_count: usize,
) -> Vec<(f32, usize)> {
    let rough_len_estimation = std::cmp::min(
        le_boundaries.len(),
        counts.iter().filter(|&&c| c != 0).count() * 2,
    );
    let mut result = Vec::with_capacity(rough_len_estimation);
    let mut cumulative_count = 0;
    let mut prev = None;
    for (idx, &le) in le_boundaries.iter().enumerate() {
        let count = counts.get(idx).copied().unwrap_or(0);
        if count == 0 {
            prev = Some(le);
        } else {
            if let Some(prev) = prev {
                result.push((prev, cumulative_count));
            }
            cumulative_count += count;
            result.push((le, cumulative_count));
            prev = None;
        }
    }
    if let Some(prev) = prev {
        if cumulative_count != total_count {
            result.push((prev, cumulative_count));
        }
    }
    result
}

/// Merge two sparse cumulative histograms, summing the counts of the same boundaries.
/// If one boundary is missing in one of the vectors, assume its value to be the same as the next
/// boundary in the same vector. NOTE: This assumption should be correct when merging histograms
/// produced by `convert_histogram` with the same set of boundaries, but it's not always the case.
fn merge_histograms(
    a: &[(f32, usize)],
    b: &[(f32, usize)],
    total_a: usize,
    total_b: usize,
) -> Vec<(f32, usize)> {
    // TODO: drop is_sorted crate and use Iterator::is_sorted once it's stable
    debug_assert!(
        IsSorted::is_sorted(&mut a.iter().map(|(le, _)| le)),
        "Boundaries are not sorted"
    );
    debug_assert!(
        IsSorted::is_sorted(&mut b.iter().map(|(le, _)| le)),
        "Boundaries are not sorted"
    );
    let unique_boundaries =
        itertools::merge(a.iter().map(|(le, _)| le), b.iter().map(|(le, _)| le))
            .dedup()
            .count();
    let mut result = Vec::with_capacity(unique_boundaries);
    let mut it_a = a.iter().copied().peekable();
    let mut it_b = b.iter().copied().peekable();
    while it_a.peek().is_some() || it_b.peek().is_some() {
        let (a_le, a_count) = it_a.peek().copied().unwrap_or((f32::INFINITY, total_a));
        let (b_le, b_count) = it_b.peek().copied().unwrap_or((f32::INFINITY, total_b));
        match a_le.partial_cmp(&b_le) {
            Some(std::cmp::Ordering::Less) => {
                result.push((a_le, a_count + b_count));
                it_a.next();
            }
            Some(std::cmp::Ordering::Equal) => {
                result.push((a_le, a_count + b_count));
                it_a.next();
                it_b.next();
            }
            Some(std::cmp::Ordering::Greater) => {
                result.push((b_le, a_count + b_count));
                it_b.next();
            }
            None => {
                // One of the boundaries is NaN, which is not supposed to happen.
                if a_le.is_nan() {
                    it_a.next();
                }
                if b_le.is_nan() {
                    it_b.next();
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_histogram() {
        // With all zeroes
        assert_eq!(
            convert_histogram(&[0., 1., 2., 3., 4., 5.], &[0, 0, 0, 0, 0, 0], 0),
            vec![],
        );

        // With all zeroes except the total count
        assert_eq!(
            convert_histogram(&[0., 1., 2., 3., 4., 5.], &[0, 0, 0, 0, 0, 0], 100),
            vec![(5., 0)],
        );

        // Full
        assert_eq!(
            convert_histogram(&[0., 1., 2., 3.], &[1, 20, 300, 4000], 5000),
            vec![(0., 1), (1., 21), (2., 321), (3., 4321)],
        );

        // Sparse
        assert_eq!(
            convert_histogram(&[0., 1., 2., 3., 4., 5., 6.], &[0, 0, 1, 0, 0, 1, 0], 1),
            vec![(1.0, 0), (2.0, 1), (4.0, 1), (5.0, 2), (6.0, 2)],
        );
    }

    #[test]
    fn test_merge_histograms() {
        // Empty vectors
        assert_eq!(merge_histograms(&[], &[], 9, 90), &[]);

        // Simple case
        #[rustfmt::skip]
        let (a, b, result) = (
            &[(0.0,  1), (1.0,  2), (2.0,  3)],
            &[(0.0, 10), (1.0, 20), (2.0, 30)],
            &[(0.0, 11), (1.0, 22), (2.0, 33)],
        );
        assert_eq!(merge_histograms(a, b, 9, 90), result);

        // Missing boundary in the middle
        #[rustfmt::skip]
        let (a, b, result) = (
            &[(0.0,  1), (1.0,  2),            (3.0,  3), (4.0,  4)],
            &[(0.0, 10), (1.0, 20), (2.0, 30),            (4.0, 40)],
            &[(0.0, 11), (1.0, 22), (2.0, 33), (3.0, 43), (4.0, 44)],
        );
        assert_eq!(merge_histograms(a, b, 9, 90), result);

        // Missing boundary at the end
        #[rustfmt::skip]
        let (a, b, result) = (
            &[(0.0,  1),          ],
            &[(0.0, 10), (1.0, 20)],
            &[(0.0, 11), (1.0, 29)],
        );
        assert_eq!(merge_histograms(a, b, 9, 90), result);
    }

    /// Check that convert-then-merge produces the same result as merge-then-convert, i.e. both
    /// functions play well together.
    #[test]
    fn test_convert_and_merge_histograms() {
        case(&[33, 23, 86, 39, 75], &[86, 50, 47, 84, 52], 256, 319);
        case(&[00, 00, 00, 00, 00], &[86, 50, 47, 84, 52], 256, 319);
        case(&[00, 23, 00, 00, 00], &[00, 00, 00, 84, 00], 30, 90);
        case(&[00, 00, 00, 00, 00], &[86, 50, 47, 84, 52], 0, 319);

        fn case(a: &[usize], b: &[usize], total_a: usize, total_b: usize) {
            assert_eq!(
                merge_histograms(
                    &convert_histogram(&DEFAULT_BUCKET_BOUNDARIES_MICROS, a, total_a),
                    &convert_histogram(&DEFAULT_BUCKET_BOUNDARIES_MICROS, b, total_b),
                    total_a,
                    total_b,
                ),
                convert_histogram(
                    &DEFAULT_BUCKET_BOUNDARIES_MICROS,
                    &std::iter::zip(a, b).map(|(a, b)| a + b).collect::<Vec<_>>(),
                    total_a + total_b
                ),
            );
        }
    }
}
