use prometheus::proto::MetricType;

use super::helpers::{gauge, metric_family};
use super::{MetricsData, MetricsProvider};
use crate::common::telemetry_ops::memory_telemetry::MemoryTelemetry;

impl MetricsProvider for MemoryTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "memory_active_bytes",
            "Total number of bytes in active pages allocated by the application",
            MetricType::GAUGE,
            vec![gauge(self.active_bytes as f64, &[])],
            prefix,
        ));
        metrics.push_metric(metric_family(
            "memory_allocated_bytes",
            "Total number of bytes allocated by the application",
            MetricType::GAUGE,
            vec![gauge(self.allocated_bytes as f64, &[])],
            prefix,
        ));
        metrics.push_metric(metric_family(
            "memory_metadata_bytes",
            "Total number of bytes dedicated to metadata",
            MetricType::GAUGE,
            vec![gauge(self.metadata_bytes as f64, &[])],
            prefix,
        ));
        metrics.push_metric(metric_family(
            "memory_resident_bytes",
            "Maximum number of bytes in physically resident data pages mapped",
            MetricType::GAUGE,
            vec![gauge(self.resident_bytes as f64, &[])],
            prefix,
        ));
        metrics.push_metric(metric_family(
            "memory_retained_bytes",
            "Total number of bytes in virtual memory mappings",
            MetricType::GAUGE,
            vec![gauge(self.retained_bytes as f64, &[])],
            prefix,
        ));
    }
}
