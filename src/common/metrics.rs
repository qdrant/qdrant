pub use prometheus::{
    register_gauge_with_registry as gauge, register_int_gauge_with_registry as int_gauge, Encoder,
    Opts, Registry, TextEncoder,
};

use super::telemetry::TelemetryData;

/// Gather and return metrics in Prometheus format.
pub fn gather_metrics(telemetry_data: &TelemetryData) -> Vec<u8> {
    let r = Registry::new();

    int_gauge!(
        Opts::new("app_info", "information about qdrant server")
            .const_label("version", &telemetry_data.app.version),
        r,
    )
    .unwrap()
    .set(1);

    int_gauge!(Opts::new("collections_total", "number of collections"), r)
        .unwrap()
        .set(telemetry_data.collections.number_of_collections as i64);

    // REST request details
    for (endpoint, responses) in &telemetry_data.requests.rest.responses {
        let (method, endpoint) = endpoint.split_once(' ').unwrap();
        for (status, statistics) in responses {
            int_gauge!(
                Opts::new("rest_responses_total", "total number of responses")
                    .const_label("method", method)
                    .const_label("endpoint", endpoint)
                    .const_label("status", status.to_string()),
                r,
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
                r,
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
                r,
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
                r,
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
                r,
            )
            .unwrap()
            .set(statistics.max_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
        }
    }

    // gRPC request details
    for (endpoint, statistics) in &telemetry_data.requests.grpc.responses {
        int_gauge!(
            Opts::new("grpc_responses_total", "total number of responses")
                .const_label("endpoint", endpoint),
            r,
        )
        .unwrap()
        .set(statistics.count as i64);
        int_gauge!(
            Opts::new(
                "grpc_responses_fail_total",
                "total number of failed responses",
            )
            .const_label("endpoint", endpoint),
            r,
        )
        .unwrap()
        .set(statistics.fail_count as i64);
        gauge!(
            Opts::new(
                "grpc_responses_avg_duration_seconds",
                "average response duratoin",
            )
            .const_label("endpoint", endpoint),
            r,
        )
        .unwrap()
        .set(statistics.avg_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
        gauge!(
            Opts::new(
                "grpc_responses_min_duration_seconds",
                "minimum response duration",
            )
            .const_label("endpoint", endpoint),
            r,
        )
        .unwrap()
        .set(statistics.min_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
        gauge!(
            Opts::new(
                "grpc_responses_max_duration_seconds",
                "maximum response duration",
            )
            .const_label("endpoint", endpoint),
            r,
        )
        .unwrap()
        .set(statistics.max_duration_micros.unwrap_or(0.0) as f64 / 1_000_000.0);
    }

    // Format metrics
    let mut buffer = vec![];
    TextEncoder::new().encode(&r.gather(), &mut buffer).unwrap();
    buffer
}
