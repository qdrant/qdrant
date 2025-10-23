use api::rest::models::HardwareUsage;
use prometheus::TextEncoder;
use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
use segment::common::operation_time_statistics::OperationDurationStatistics;

use super::telemetry_ops::hardware::HardwareTelemetry;
use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppFeaturesTelemetry};
use crate::common::telemetry_ops::cluster_telemetry::{ClusterStatusTelemetry, ClusterTelemetry};
use crate::common::telemetry_ops::collections_telemetry::{
    CollectionTelemetryEnum, CollectionsTelemetry,
};
use crate::common::telemetry_ops::memory_telemetry::MemoryTelemetry;
use crate::common::telemetry_ops::requests_telemetry::{
    GrpcTelemetry, RequestsTelemetry, WebApiTelemetry,
};

/// Whitelist for REST endpoints in metrics output.
///
/// Contains selection of search, recommend, scroll and upsert endpoints.
///
/// This array *must* be sorted.
const REST_ENDPOINT_WHITELIST: &[&str] = &[
    "/collections/{name}/index",
    "/collections/{name}/points",
    "/collections/{name}/points/batch",
    "/collections/{name}/points/count",
    "/collections/{name}/points/delete",
    "/collections/{name}/points/discover",
    "/collections/{name}/points/discover/batch",
    "/collections/{name}/points/facet",
    "/collections/{name}/points/payload",
    "/collections/{name}/points/payload/clear",
    "/collections/{name}/points/payload/delete",
    "/collections/{name}/points/query",
    "/collections/{name}/points/query/batch",
    "/collections/{name}/points/query/groups",
    "/collections/{name}/points/recommend",
    "/collections/{name}/points/recommend/batch",
    "/collections/{name}/points/recommend/groups",
    "/collections/{name}/points/scroll",
    "/collections/{name}/points/search",
    "/collections/{name}/points/search/batch",
    "/collections/{name}/points/search/groups",
    "/collections/{name}/points/search/matrix/offsets",
    "/collections/{name}/points/search/matrix/pairs",
    "/collections/{name}/points/vectors",
    "/collections/{name}/points/vectors/delete",
];

/// Whitelist for GRPC endpoints in metrics output.
///
/// Contains selection of search, recommend, scroll and upsert endpoints.
///
/// This array *must* be sorted.
const GRPC_ENDPOINT_WHITELIST: &[&str] = &[
    "/qdrant.Points/ClearPayload",
    "/qdrant.Points/Count",
    "/qdrant.Points/Delete",
    "/qdrant.Points/DeletePayload",
    "/qdrant.Points/Discover",
    "/qdrant.Points/DiscoverBatch",
    "/qdrant.Points/Facet",
    "/qdrant.Points/Get",
    "/qdrant.Points/OverwritePayload",
    "/qdrant.Points/Query",
    "/qdrant.Points/QueryBatch",
    "/qdrant.Points/QueryGroups",
    "/qdrant.Points/Recommend",
    "/qdrant.Points/RecommendBatch",
    "/qdrant.Points/RecommendGroups",
    "/qdrant.Points/Scroll",
    "/qdrant.Points/Search",
    "/qdrant.Points/SearchBatch",
    "/qdrant.Points/SearchGroups",
    "/qdrant.Points/SetPayload",
    "/qdrant.Points/UpdateBatch",
    "/qdrant.Points/UpdateVectors",
    "/qdrant.Points/Upsert",
];

/// For REST requests, only report timings when having this HTTP response status.
const REST_TIMINGS_FOR_STATUS: u16 = 200;

/// Encapsulates metrics data in Prometheus format.
pub struct MetricsData {
    metrics: Vec<MetricFamily>,
}

impl MetricsData {
    pub fn format_metrics(&self) -> String {
        TextEncoder::new().encode_to_string(&self.metrics).unwrap()
    }
}

impl From<TelemetryData> for MetricsData {
    fn from(telemetry_data: TelemetryData) -> Self {
        let mut metrics = vec![];
        telemetry_data.add_metrics(&mut metrics);
        Self { metrics }
    }
}

trait MetricsProvider {
    /// Add metrics definitions for this.
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>);
}

impl MetricsProvider for TelemetryData {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        self.app.add_metrics(metrics);
        self.collections.add_metrics(metrics);
        if let Some(cluster) = &self.cluster {
            cluster.add_metrics(metrics);
        }
        if let Some(requests) = &self.requests {
            requests.add_metrics(metrics);
        }
        if let Some(hardware) = &self.hardware {
            hardware.add_metrics(metrics);
        }
        if let Some(mem) = &self.memory {
            mem.add_metrics(metrics);
        }
    }
}

impl MetricsProvider for AppBuildTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        metrics.push(metric_family(
            "app_info",
            "information about qdrant server",
            MetricType::GAUGE,
            vec![gauge(
                1.0,
                &[("name", &self.name), ("version", &self.version)],
            )],
        ));
        self.features.iter().for_each(|f| f.add_metrics(metrics));
    }
}

impl MetricsProvider for AppFeaturesTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        metrics.push(metric_family(
            "app_status_recovery_mode",
            "features enabled in qdrant server",
            MetricType::GAUGE,
            vec![gauge(if self.recovery_mode { 1.0 } else { 0.0 }, &[])],
        ))
    }
}

impl MetricsProvider for CollectionsTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        let vector_count = self
            .collections
            .iter()
            .flatten()
            .map(|p| match p {
                CollectionTelemetryEnum::Aggregated(a) => a.vectors,
                CollectionTelemetryEnum::Full(c) => c.count_vectors(),
            })
            .sum::<usize>();
        metrics.push(metric_family(
            "collections_total",
            "number of collections",
            MetricType::GAUGE,
            vec![gauge(self.number_of_collections as f64, &[])],
        ));
        metrics.push(metric_family(
            "collections_vector_total",
            "total number of vectors in all collections",
            MetricType::GAUGE,
            vec![gauge(vector_count as f64, &[])],
        ));

        let mut total_optimizations_running = 0;

        for collection in self.collections.iter().flatten() {
            let collection = match collection {
                CollectionTelemetryEnum::Full(collection_telemetry) => collection_telemetry,
                CollectionTelemetryEnum::Aggregated(_) => {
                    continue;
                }
            };

            total_optimizations_running += collection.count_optimizers_running();
        }

        metrics.push(metric_family(
            "optimizer_running_processes",
            "number of currently running optimization processes",
            MetricType::GAUGE,
            vec![gauge(total_optimizations_running as f64, &[])],
        ));
    }
}

impl MetricsProvider for ClusterTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        let ClusterTelemetry {
            enabled,
            status,
            config: _,
            peers: _,
            peer_metadata: _,
            metadata: _,
        } = self;

        metrics.push(metric_family(
            "cluster_enabled",
            "is cluster support enabled",
            MetricType::GAUGE,
            vec![gauge(if *enabled { 1.0 } else { 0.0 }, &[])],
        ));

        if let Some(status) = status {
            status.add_metrics(metrics);
        }
    }
}

impl MetricsProvider for ClusterStatusTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        metrics.push(metric_family(
            "cluster_peers_total",
            "total number of cluster peers",
            MetricType::GAUGE,
            vec![gauge(self.number_of_peers as f64, &[])],
        ));
        metrics.push(metric_family(
            "cluster_term",
            "current cluster term",
            MetricType::COUNTER,
            vec![counter(self.term as f64, &[])],
        ));

        if let Some(ref peer_id) = self.peer_id.map(|p| p.to_string()) {
            metrics.push(metric_family(
                "cluster_commit",
                "index of last committed (finalized) operation cluster peer is aware of",
                MetricType::COUNTER,
                vec![counter(self.commit as f64, &[("peer_id", peer_id)])],
            ));
            metrics.push(metric_family(
                "cluster_pending_operations_total",
                "total number of pending operations for cluster peer",
                MetricType::GAUGE,
                vec![gauge(self.pending_operations as f64, &[])],
            ));
            metrics.push(metric_family(
                "cluster_voter",
                "is cluster peer a voter or learner",
                MetricType::GAUGE,
                vec![gauge(if self.is_voter { 1.0 } else { 0.0 }, &[])],
            ));
        }
    }
}

impl MetricsProvider for RequestsTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        self.rest.add_metrics(metrics);
        self.grpc.add_metrics(metrics);
    }
}

impl MetricsProvider for WebApiTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        let mut builder = OperationDurationMetricsBuilder::default();
        for (endpoint, responses) in &self.responses {
            let Some((method, endpoint)) = endpoint.split_once(' ') else {
                continue;
            };
            // Endpoint must be whitelisted
            if REST_ENDPOINT_WHITELIST.binary_search(&endpoint).is_err() {
                continue;
            }
            for (status, stats) in responses {
                builder.add(
                    stats,
                    &[
                        ("method", method),
                        ("endpoint", endpoint),
                        ("status", &status.to_string()),
                    ],
                    *status == REST_TIMINGS_FOR_STATUS,
                );
            }
        }
        builder.build("rest", metrics);
    }
}

impl MetricsProvider for GrpcTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        let mut builder = OperationDurationMetricsBuilder::default();
        for (endpoint, stats) in &self.responses {
            // Endpoint must be whitelisted
            if GRPC_ENDPOINT_WHITELIST
                .binary_search(&endpoint.as_str())
                .is_err()
            {
                continue;
            }
            builder.add(stats, &[("endpoint", endpoint.as_str())], true);
        }
        builder.build("grpc", metrics);
    }
}

impl MetricsProvider for MemoryTelemetry {
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
        metrics.push(metric_family(
            "memory_active_bytes",
            "Total number of bytes in active pages allocated by the application",
            MetricType::GAUGE,
            vec![gauge(self.active_bytes as f64, &[])],
        ));
        metrics.push(metric_family(
            "memory_allocated_bytes",
            "Total number of bytes allocated by the application",
            MetricType::GAUGE,
            vec![gauge(self.allocated_bytes as f64, &[])],
        ));
        metrics.push(metric_family(
            "memory_metadata_bytes",
            "Total number of bytes dedicated to metadata",
            MetricType::GAUGE,
            vec![gauge(self.metadata_bytes as f64, &[])],
        ));
        metrics.push(metric_family(
            "memory_resident_bytes",
            "Maximum number of bytes in physically resident data pages mapped",
            MetricType::GAUGE,
            vec![gauge(self.resident_bytes as f64, &[])],
        ));
        metrics.push(metric_family(
            "memory_retained_bytes",
            "Total number of bytes in virtual memory mappings",
            MetricType::GAUGE,
            vec![gauge(self.retained_bytes as f64, &[])],
        ));
    }
}

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
    fn add_metrics(&self, metrics: &mut Vec<MetricFamily>) {
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

        metrics.push(metric_family(
            "collection_hardware_metric_cpu",
            "CPU measurements of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.cpu),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_payload_io_read",
            "Total IO payload read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_io_read),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_payload_index_io_read",
            "Total IO payload index read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_index_io_read),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_payload_index_io_write",
            "Total IO payload index write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_index_io_write),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_payload_io_write",
            "Total IO payload write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.payload_io_write),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_vector_io_read",
            "Total IO vector read metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.vector_io_read),
        ));

        metrics.push(metric_family(
            "collection_hardware_metric_vector_io_write",
            "Total IO vector write metrics of a collection",
            MetricType::COUNTER,
            self.make_metric_counters(|hw| hw.vector_io_write),
        ));
    }
}

/// A helper struct to build a vector of [`MetricFamily`] out of a collection of
/// [`OperationDurationStatistics`].
#[derive(Default)]
struct OperationDurationMetricsBuilder {
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
    pub fn build(self, prefix: &str, metrics: &mut Vec<MetricFamily>) {
        if !self.total.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_total"),
                "total number of responses",
                MetricType::COUNTER,
                self.total,
            ));
        }
        if !self.fail_total.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_fail_total"),
                "total number of failed responses",
                MetricType::COUNTER,
                self.fail_total,
            ));
        }
        if !self.avg_secs.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_avg_duration_seconds"),
                "average response duration",
                MetricType::GAUGE,
                self.avg_secs,
            ));
        }
        if !self.min_secs.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_min_duration_seconds"),
                "minimum response duration",
                MetricType::GAUGE,
                self.min_secs,
            ));
        }
        if !self.max_secs.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_max_duration_seconds"),
                "maximum response duration",
                MetricType::GAUGE,
                self.max_secs,
            ));
        }
        if !self.duration_histogram_secs.is_empty() {
            metrics.push(metric_family(
                &format!("{prefix}_responses_duration_seconds"),
                "response duration histogram",
                MetricType::HISTOGRAM,
                self.duration_histogram_secs,
            ));
        }
    }
}

fn metric_family(name: &str, help: &str, r#type: MetricType, metrics: Vec<Metric>) -> MetricFamily {
    let mut metric_family = MetricFamily::default();
    metric_family.set_name(name.into());
    metric_family.set_help(help.into());
    metric_family.set_field_type(r#type);
    metric_family.set_metric(metrics);
    metric_family
}

fn counter(value: f64, labels: &[(&str, &str)]) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_counter({
        let mut counter = Counter::default();
        counter.set_value(value);
        counter
    });
    metric
}

fn gauge(value: f64, labels: &[(&str, &str)]) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_gauge({
        let mut gauge = Gauge::default();
        gauge.set_value(value);
        gauge
    });
    metric
}

fn histogram(
    sample_count: u64,
    sample_sum: f64,
    buckets: &[(f64, u64)],
    labels: &[(&str, &str)],
) -> Metric {
    let mut metric = Metric::default();
    metric.set_label(labels.iter().map(|(n, v)| label_pair(n, v)).collect());
    metric.set_histogram({
        let mut histogram = prometheus::proto::Histogram::default();
        histogram.set_sample_count(sample_count);
        histogram.set_sample_sum(sample_sum);
        histogram.set_bucket(
            buckets
                .iter()
                .map(|&(upper_bound, cumulative_count)| {
                    let mut bucket = prometheus::proto::Bucket::default();
                    bucket.set_cumulative_count(cumulative_count);
                    bucket.set_upper_bound(upper_bound);
                    bucket
                })
                .collect(),
        );
        histogram
    });
    metric
}

fn label_pair(name: &str, value: &str) -> LabelPair {
    let mut label = LabelPair::default();
    label.set_name(name.into());
    label.set_value(value.into());
    label
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_endpoint_whitelists_sorted() {
        use super::{GRPC_ENDPOINT_WHITELIST, REST_ENDPOINT_WHITELIST};

        assert!(
            REST_ENDPOINT_WHITELIST.windows(2).all(|n| n[0] <= n[1]),
            "REST_ENDPOINT_WHITELIST must be sorted in code to allow binary search"
        );
        assert!(
            GRPC_ENDPOINT_WHITELIST.windows(2).all(|n| n[0] <= n[1]),
            "GRPC_ENDPOINT_WHITELIST must be sorted in code to allow binary search"
        );
    }
}
