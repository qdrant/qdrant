use prometheus::{
    register_gauge_with_registry as gauge, register_int_counter_with_registry as int_counter,
    register_int_gauge_with_registry as int_gauge, Encoder, Opts, Registry, TextEncoder,
};

use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::app_telemetry::AppBuildTelemetry;
use crate::common::telemetry_ops::cluster_telemetry::{ClusterStatusTelemetry, ClusterTelemetry};
use crate::common::telemetry_ops::collections_telemetry::{
    CollectionTelemetryEnum, CollectionsTelemetry,
};
use crate::common::telemetry_ops::requests_telemetry::{
    GrpcTelemetry, RequestsTelemetry, WebApiTelemetry,
};

/// Encapsulates metrics data in Prometheus format.
pub struct MetricsData {
    registry: Registry,
}

impl MetricsData {
    pub fn format_metrics(&self) -> Vec<u8> {
        let mut buffer = vec![];
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .unwrap();
        buffer
    }
}

impl From<TelemetryData> for MetricsData {
    fn from(telemetry_data: TelemetryData) -> Self {
        let registry = Registry::new();
        telemetry_data.register_metrics(&registry);
        Self { registry }
    }
}

trait MetricsProvider {
    /// Register and add object metrics to registry.
    fn register_metrics(&self, registry: &Registry);
}

impl MetricsProvider for TelemetryData {
    fn register_metrics(&self, registry: &Registry) {
        self.app.register_metrics(registry);
        self.collections.register_metrics(registry);
        self.cluster.register_metrics(registry);
        self.requests.register_metrics(registry);
    }
}

impl MetricsProvider for AppBuildTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        int_counter!(
            Opts::new("app_info", "information about qdrant server")
                .const_label("name", &self.name)
                .const_label("version", &self.version),
            registry,
        )
        .unwrap()
        .inc_by(1);
    }
}

impl MetricsProvider for CollectionsTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        int_gauge!(
            Opts::new("collections_total", "number of collections"),
            registry
        )
        .unwrap()
        .set(self.number_of_collections as i64);

        // Count collection types
        if let Some(ref collections) = self.collections {
            let full_count = collections
                .iter()
                .filter(|p| matches!(p, CollectionTelemetryEnum::Full(_)))
                .count();
            let aggregated_count = collections
                .iter()
                .filter(|p| matches!(p, CollectionTelemetryEnum::Aggregated(_)))
                .count();
            int_gauge!(
                Opts::new("collections_full_total", "number of full collections"),
                registry
            )
            .unwrap()
            .set(full_count as i64);
            int_gauge!(
                Opts::new(
                    "collections_aggregated_total",
                    "number of aggregated collections"
                ),
                registry
            )
            .unwrap()
            .set(aggregated_count as i64);
        }
    }
}

impl MetricsProvider for ClusterTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        int_gauge!(
            Opts::new("cluster_enabled", "is cluster support enabled"),
            registry
        )
        .unwrap()
        .set(self.enabled as i64);

        if let Some(ref status) = self.status {
            status.register_metrics(registry);
        }
    }
}

impl MetricsProvider for ClusterStatusTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        int_gauge!(
            Opts::new("cluster_peers_total", "total number of cluster peers"),
            registry
        )
        .unwrap()
        .set(self.number_of_peers as i64);
        int_counter!(Opts::new("cluster_term", "current cluster term"), registry)
            .unwrap()
            .inc_by(self.term);

        if let Some(ref peer_id) = self.peer_id.map(|p| p.to_string()) {
            int_counter!(
                Opts::new(
                    "cluster_commit",
                    "index of last committed (finalized) operation cluster peer is aware of"
                )
                .const_label("peer_id", peer_id),
                registry
            )
            .unwrap()
            .inc_by(self.term);
            int_gauge!(
                Opts::new(
                    "cluster_pending_operations_total",
                    "total number of pending operations for cluster peer"
                )
                .const_label("peer_id", peer_id),
                registry
            )
            .unwrap()
            .set(self.pending_operations as i64);
            int_gauge!(
                Opts::new("cluster_voter", "is cluster peer a voter or learner")
                    .const_label("peer_id", peer_id),
                registry
            )
            .unwrap()
            .set(self.is_voter as i64);
        }
    }
}

impl MetricsProvider for RequestsTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        self.rest.register_metrics(registry);
        self.grpc.register_metrics(registry);
    }
}

impl MetricsProvider for WebApiTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        for (endpoint, responses) in &self.responses {
            let (method, endpoint) = endpoint.split_once(' ').unwrap();
            for (status, statistics) in responses {
                int_counter!(
                    Opts::new("rest_responses_total", "total number of responses")
                        .const_label("method", method)
                        .const_label("endpoint", endpoint)
                        .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .inc_by(statistics.count as u64);
                int_counter!(
                    Opts::new(
                        "rest_responses_fail_total",
                        "total number of failed responses",
                    )
                    .const_label("method", method)
                    .const_label("endpoint", endpoint)
                    .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .inc_by(statistics.fail_count as u64);
                gauge!(
                    Opts::new(
                        "rest_responses_avg_duration_seconds",
                        "average response duratoin",
                    )
                    .const_label("method", method)
                    .const_label("endpoint", endpoint)
                    .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .set(statistics.avg_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
                gauge!(
                    Opts::new(
                        "rest_responses_min_duration_seconds",
                        "minimum response duration",
                    )
                    .const_label("method", method)
                    .const_label("endpoint", endpoint)
                    .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .set(statistics.min_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
                gauge!(
                    Opts::new(
                        "rest_responses_max_duration_seconds",
                        "maximum response duration",
                    )
                    .const_label("method", method)
                    .const_label("endpoint", endpoint)
                    .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .set(statistics.max_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
            }
        }
    }
}

impl MetricsProvider for GrpcTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        for (endpoint, statistics) in &self.responses {
            int_counter!(
                Opts::new("grpc_responses_total", "total number of responses")
                    .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .inc_by(statistics.count as u64);
            int_counter!(
                Opts::new(
                    "grpc_responses_fail_total",
                    "total number of failed responses",
                )
                .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .inc_by(statistics.fail_count as u64);
            gauge!(
                Opts::new(
                    "grpc_responses_avg_duration_seconds",
                    "average response duratoin",
                )
                .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .set(statistics.avg_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
            gauge!(
                Opts::new(
                    "grpc_responses_min_duration_seconds",
                    "minimum response duration",
                )
                .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .set(statistics.min_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
            gauge!(
                Opts::new(
                    "grpc_responses_max_duration_seconds",
                    "maximum response duration",
                )
                .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .set(statistics.max_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
        }
    }
}
