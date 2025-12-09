mod builder;
mod cluster_metrics;
mod collection_metrics;
mod common;
mod hardware_metrics;

#[cfg(target_os = "linux")]
mod procfs_metrics;

use prometheus::TextEncoder;
use prometheus::proto::{MetricFamily, MetricType};

use crate::common::metrics::builder::OperationDurationMetricsBuilder;
use crate::common::metrics::common::{counter, gauge, histogram, metric_family};
use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppFeaturesTelemetry};
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

    /// Creates a new `MetricsData` from telemetry data and an optional prefix for metrics names.
    pub fn new_from_telemetry(telemetry_data: TelemetryData, prefix: Option<&str>) -> Self {
        let mut metrics = MetricsData::empty();
        telemetry_data.add_metrics(&mut metrics, prefix);
        metrics
    }

    /// Adds the given `metrics_family` to the `MetricsData` collection, if `Some`.
    fn push_metric(&mut self, metric_family: Option<MetricFamily>) {
        if let Some(metric_family) = metric_family {
            self.metrics.push(metric_family);
        }
    }

    /// Creates an empty collection of Prometheus metric families `MetricsData`.
    /// This should only be used when you explicitly need an empty collection to gather metrics.
    ///
    /// In most cases, you should use [`MetricsData::new_from_telemetry`] to initialize new metrics data.
    fn empty() -> Self {
        Self { metrics: vec![] }
    }
}

trait MetricsProvider {
    /// Add metrics definitions for this.
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>);
}

impl MetricsProvider for TelemetryData {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        self.app.add_metrics(metrics, prefix);

        let this_peer_id = self.cluster.as_ref().and_then(|i| i.this_peer_id());
        self.collections.add_metrics(metrics, prefix, this_peer_id);

        if let Some(cluster) = &self.cluster {
            cluster.add_metrics(metrics, prefix);
        }
        if let Some(requests) = &self.requests {
            requests.add_metrics(metrics, prefix);
        }
        if let Some(hardware) = &self.hardware {
            hardware.add_metrics(metrics, prefix);
        }
        if let Some(mem) = &self.memory {
            mem.add_metrics(metrics, prefix);
        }

        #[cfg(target_os = "linux")]
        match procfs_metrics::ProcFsMetrics::collect() {
            Ok(procfs_provider) => procfs_provider.add_metrics(metrics, prefix),
            Err(err) => log::warn!("Error reading procfs infos: {err:?}"),
        };
    }
}

impl MetricsProvider for AppBuildTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "app_info",
            "information about qdrant server",
            MetricType::GAUGE,
            vec![gauge(
                1.0,
                &[("name", &self.name), ("version", &self.version)],
            )],
            prefix,
        ));
        self.features
            .iter()
            .for_each(|f| f.add_metrics(metrics, prefix));
    }
}

impl MetricsProvider for AppFeaturesTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "app_status_recovery_mode",
            "features enabled in qdrant server",
            MetricType::GAUGE,
            vec![gauge(if self.recovery_mode { 1.0 } else { 0.0 }, &[])],
            prefix,
        ))
    }
}

impl MetricsProvider for RequestsTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        self.rest.add_metrics(metrics, prefix);
        self.grpc.add_metrics(metrics, prefix);
    }
}

impl MetricsProvider for WebApiTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
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
        builder.build(prefix, "rest", metrics);
    }
}

impl MetricsProvider for GrpcTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
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
        builder.build(prefix, "grpc", metrics);
    }
}

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
