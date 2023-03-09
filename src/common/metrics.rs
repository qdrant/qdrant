use prometheus::{
    register_gauge_with_registry as gauge, register_int_gauge_with_registry as int_gauge, Encoder,
    Opts, Registry, TextEncoder,
};

use crate::common::telemetry::TelemetryData;
use crate::common::telemetry_ops::app_telemetry::AppBuildTelemetry;
use crate::common::telemetry_ops::collections_telemetry::CollectionsTelemetry;
use crate::common::telemetry_ops::requests_telemetry::{
    GrpcTelemetry, RequestsTelemetry, WebApiTelemetry,
};

/// Gather and return metrics in Prometheus format.
pub fn gather_metrics(telemetry_data: &TelemetryData) -> Vec<u8> {
    let registry = Registry::new();
    telemetry_data.register_metrics(&registry);

    // Format metrics
    let mut buffer = vec![];
    TextEncoder::new()
        .encode(&registry.gather(), &mut buffer)
        .unwrap();
    buffer
}

trait MetricsProvider {
    /// Register and add object metrics to registry.
    fn register_metrics(&self, registry: &Registry);
}

impl MetricsProvider for TelemetryData {
    fn register_metrics(&self, registry: &Registry) {
        self.app.register_metrics(registry);
        self.collections.register_metrics(registry);
        self.requests.register_metrics(registry);
    }
}

impl MetricsProvider for AppBuildTelemetry {
    fn register_metrics(&self, registry: &Registry) {
        int_gauge!(
            Opts::new("app_info", "information about qdrant server")
                .const_label("name", &self.name)
                .const_label("version", &self.version),
            registry,
        )
        .unwrap()
        .set(1);
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
                int_gauge!(
                    Opts::new("rest_responses_total", "total number of responses")
                        .const_label("method", method)
                        .const_label("endpoint", endpoint)
                        .const_label("status", status.to_string()),
                    registry,
                )
                .unwrap()
                .set(statistics.count as i64);
                int_gauge!(
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
                .set(statistics.fail_count as i64);
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
            int_gauge!(
                Opts::new("grpc_responses_total", "total number of responses")
                    .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .set(statistics.count as i64);
            int_gauge!(
                Opts::new(
                    "grpc_responses_fail_total",
                    "total number of failed responses",
                )
                .const_label("endpoint", endpoint),
                registry,
            )
            .unwrap()
            .set(statistics.fail_count as i64);
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
