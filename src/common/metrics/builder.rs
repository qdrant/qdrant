use prometheus::proto::{Metric, MetricType};
use segment::common::operation_time_statistics::OperationDurationStatistics;

use crate::common::metrics::{MetricsData, counter, gauge, histogram, metric_family};

/// A helper struct to build a vector of [`MetricFamily`] out of a collection of
/// [`OperationDurationStatistics`].
#[derive(Default)]
pub(super) struct OperationDurationMetricsBuilder {
    total: Vec<Metric>,
    fail_total: Vec<Metric>,
    avg_secs: Vec<Metric>,
    min_secs: Vec<Metric>,
    max_secs: Vec<Metric>,
    duration_histogram_secs: Vec<Metric>,
}

impl OperationDurationMetricsBuilder {
    /// Add metrics for the provided statistics.
    /// If `add_timings` is `false`, only the total and fail_total counters will be added.
    pub fn add(
        &mut self,
        stat: &OperationDurationStatistics,
        labels: &[(&str, &str)],
        add_timings: bool,
    ) {
        self.total.push(counter(stat.count as f64, labels));
        self.fail_total
            .push(counter(stat.fail_count.unwrap_or_default() as f64, labels));

        if !add_timings {
            return;
        }

        self.avg_secs.push(gauge(
            f64::from(stat.avg_duration_micros.unwrap_or(0.0)) / 1_000_000.0,
            labels,
        ));
        self.min_secs.push(gauge(
            f64::from(stat.min_duration_micros.unwrap_or(0.0)) / 1_000_000.0,
            labels,
        ));
        self.max_secs.push(gauge(
            f64::from(stat.max_duration_micros.unwrap_or(0.0)) / 1_000_000.0,
            labels,
        ));
        self.duration_histogram_secs.push(histogram(
            stat.count as u64,
            stat.total_duration_micros.unwrap_or(0) as f64 / 1_000_000.0,
            &stat
                .duration_micros_histogram
                .iter()
                .map(|&(b, c)| (f64::from(b) / 1_000_000.0, c as u64))
                .collect::<Vec<_>>(),
            labels,
        ));
    }

    /// Build metrics and add them to the provided vector.
    pub fn build(self, global_prefix: Option<&str>, prefix: &str, metrics: &mut MetricsData) {
        let prefix = format!("{}{prefix}_", global_prefix.unwrap_or(""));

        metrics.push_metric(metric_family(
            "responses_total",
            "total number of responses",
            MetricType::COUNTER,
            self.total,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_fail_total",
            "total number of failed responses",
            MetricType::COUNTER,
            self.fail_total,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_avg_duration_seconds",
            "average response duration",
            MetricType::GAUGE,
            self.avg_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_min_duration_seconds",
            "minimum response duration",
            MetricType::GAUGE,
            self.min_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_max_duration_seconds",
            "maximum response duration",
            MetricType::GAUGE,
            self.max_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_duration_seconds",
            "response duration histogram",
            MetricType::HISTOGRAM,
            self.duration_histogram_secs,
            Some(&prefix),
        ));
    }
}
