use api::rest::models::HardwareUsage;
use prometheus::proto::{Metric, MetricType};

use crate::common::metrics::{MetricsData, MetricsProvider, counter, metric_family};
use crate::common::telemetry_ops::hardware::HardwareTelemetry;

impl HardwareTelemetry {
    // Helper function to create counter metrics of a single Hw type, like cpu.
    fn make_metric_counters<F: Fn(&HardwareUsage) -> usize>(&self, f: F) -> Vec<Metric> {
        self.collection_data
            .iter()
            .map(|(collection_id, hw_usage)| counter(f(hw_usage) as f64, &[("id", collection_id)]))
            .collect()
    }
}

impl MetricsProvider for HardwareTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        // MetricType::COUNTER requires non-empty collection data.
        if self.collection_data.is_empty() {
            return;
        }

        // Keep a dummy type decomposition of HwUsage here to enforce coverage of new fields in metrics.
        // This gets optimized away by the compiler: https://godbolt.org/z/9cMTzcYr4
        let HardwareUsage {
            cpu: _,
            payload_io_read: _,
            payload_io_write: _,
            payload_index_io_read: _,
            payload_index_io_write: _,
            vector_io_read: _,
            vector_io_write: _,
        } = HardwareUsage::default();

        metrics.push_metric(metric_family(
            "collection_hardware_metric_cpu",
            "CPU measurements of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.cpu),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_payload_io_read",
            "Total IO payload read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_io_read),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_payload_index_io_read",
            "Total IO payload index read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_index_io_read),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_payload_index_io_write",
            "Total IO payload index write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_index_io_write),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_payload_io_write",
            "Total IO payload write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_io_write),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_vector_io_read",
            "Total IO vector read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.vector_io_read),
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_hardware_metric_vector_io_write",
            "Total IO vector write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.vector_io_write),
            prefix,
        ));
    }
}
