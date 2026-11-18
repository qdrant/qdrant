use prometheus::proto::{Metric, MetricType};
use segment::common::operation_time_statistics::OperationDurationStatistics;

use super::helpers::{counter, gauge, histogram, metric_family};
use super::{MetricsData, MetricsProvider};
use crate::common::telemetry_ops::requests_telemetry::{
    GrpcTelemetry, RequestsTelemetry, WebApiTelemetry,
};

/// Whitelist for REST endpoints in metrics output.
///
/// Contains selection of search, recommend, scroll and upsert endpoints.
///
/// This array *must* be sorted.
const REST_ENDPOINT_WHITELIST: &[&str] = &[
    "/collections/{collection_name}/index",
    "/collections/{collection_name}/points",
    "/collections/{collection_name}/points/batch",
    "/collections/{collection_name}/points/count",
    "/collections/{collection_name}/points/delete",
    "/collections/{collection_name}/points/discover",
    "/collections/{collection_name}/points/discover/batch",
    "/collections/{collection_name}/points/facet",
    "/collections/{collection_name}/points/payload",
    "/collections/{collection_name}/points/payload/clear",
    "/collections/{collection_name}/points/payload/delete",
    "/collections/{collection_name}/points/query",
    "/collections/{collection_name}/points/query/batch",
    "/collections/{collection_name}/points/query/groups",
    "/collections/{collection_name}/points/recommend",
    "/collections/{collection_name}/points/recommend/batch",
    "/collections/{collection_name}/points/recommend/groups",
    "/collections/{collection_name}/points/scroll",
    "/collections/{collection_name}/points/search",
    "/collections/{collection_name}/points/search/batch",
    "/collections/{collection_name}/points/search/groups",
    "/collections/{collection_name}/points/search/matrix/offsets",
    "/collections/{collection_name}/points/search/matrix/pairs",
    "/collections/{collection_name}/points/vectors",
    "/collections/{collection_name}/points/vectors/delete",
    "/collections/{collection_name}/vectors/{vector_name}",
];

/// Whitelist for GRPC endpoints in metrics output.
///
/// Contains selection of search, recommend, scroll and upsert endpoints.
///
/// This array *must* be sorted.
const GRPC_ENDPOINT_WHITELIST: &[&str] = &[
    "/qdrant.Points/ClearPayload",
    "/qdrant.Points/Count",
    "/qdrant.Points/CreateFieldIndex",
    "/qdrant.Points/CreateVectorName",
    "/qdrant.Points/Delete",
    "/qdrant.Points/DeleteFieldIndex",
    "/qdrant.Points/DeletePayload",
    "/qdrant.Points/DeleteVectorName",
    "/qdrant.Points/DeleteVectors",
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
    "/qdrant.Points/SearchMatrixOffsets",
    "/qdrant.Points/SearchMatrixPairs",
    "/qdrant.Points/SetPayload",
    "/qdrant.Points/UpdateBatch",
    "/qdrant.Points/UpdateVectors",
    "/qdrant.Points/Upsert",
];

/// For REST requests, only report timings when having this HTTP response status.
const REST_TIMINGS_FOR_STATUS: u16 = 200;

impl MetricsProvider for RequestsTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        self.rest.add_metrics(metrics, prefix);
        self.grpc.add_metrics(metrics, prefix);
    }
}

impl MetricsProvider for WebApiTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        // Mode decision: when `per_collection_responses` is populated (i.e. per_collection
        // was requested via query parameter), we render per-collection metrics with a
        // `collection` label and skip global ones.
        if self.per_collection_responses.is_empty() {
            // Global mode: render global metrics as before
            let mut builder = OperationDurationMetricsBuilder::default();
            for (endpoint, responses) in &self.responses {
                let Some((method, endpoint)) = endpoint.split_once(' ') else {
                    continue;
                };
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
        } else {
            // Per-collection mode: render per-collection metrics with `collection` label
            let mut builder = OperationDurationMetricsBuilder::default();
            for (collection, methods) in &self.per_collection_responses {
                for (endpoint, responses) in methods {
                    let Some((method, endpoint)) = endpoint.split_once(' ') else {
                        continue;
                    };
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
                                ("collection", collection),
                            ],
                            *status == REST_TIMINGS_FOR_STATUS,
                        );
                    }
                }
            }
            builder.build(prefix, "rest", metrics);
        }
    }
}

impl MetricsProvider for GrpcTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        // Same mode-switching logic as WebApiTelemetry::add_metrics — see comment there.
        if self.per_collection_responses.is_empty() {
            // Global mode: render global metrics as before
            let mut builder = OperationDurationMetricsBuilder::default();
            for (endpoint, responses) in &self.responses {
                if GRPC_ENDPOINT_WHITELIST
                    .binary_search(&endpoint.as_str())
                    .is_err()
                {
                    continue;
                }
                for (status, stats) in responses {
                    builder.add(
                        stats,
                        &[
                            ("endpoint", endpoint.as_str()),
                            ("status", &status.to_string()),
                        ],
                        true,
                    );
                }
            }
            builder.build(prefix, "grpc", metrics);
        } else {
            // Per-collection mode: render per-collection metrics with `collection` label
            let mut builder = OperationDurationMetricsBuilder::default();
            for (collection, methods) in &self.per_collection_responses {
                for (endpoint, responses) in methods {
                    if GRPC_ENDPOINT_WHITELIST
                        .binary_search(&endpoint.as_str())
                        .is_err()
                    {
                        continue;
                    }
                    for (status, stats) in responses {
                        builder.add(
                            stats,
                            &[
                                ("endpoint", endpoint.as_str()),
                                ("status", &status.to_string()),
                                ("collection", collection),
                            ],
                            true,
                        );
                    }
                }
            }
            builder.build(prefix, "grpc", metrics);
        }
    }
}

/// A helper struct to build a vector of [`prometheus::proto::MetricFamily`] out of a collection of
/// [`OperationDurationStatistics`].
#[derive(Default)]
struct OperationDurationMetricsBuilder {
    total: Vec<Metric>,
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
        let OperationDurationMetricsBuilder {
            total,
            avg_secs,
            min_secs,
            max_secs,
            duration_histogram_secs,
        } = self;

        let prefix = format!("{}{prefix}_", global_prefix.unwrap_or(""));

        metrics.push_metric(metric_family(
            "responses_total",
            "total number of responses",
            MetricType::COUNTER,
            total,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_avg_duration_seconds",
            "average response duration",
            MetricType::GAUGE,
            avg_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_min_duration_seconds",
            "minimum response duration",
            MetricType::GAUGE,
            min_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_max_duration_seconds",
            "maximum response duration",
            MetricType::GAUGE,
            max_secs,
            Some(&prefix),
        ));
        metrics.push_metric(metric_family(
            "responses_duration_seconds",
            "response duration histogram",
            MetricType::HISTOGRAM,
            duration_histogram_secs,
            Some(&prefix),
        ));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_endpoint_whitelists_sorted() {
        use super::{GRPC_ENDPOINT_WHITELIST, REST_ENDPOINT_WHITELIST};

        assert!(
            REST_ENDPOINT_WHITELIST.array_windows().all(|[a, b]| a <= b),
            "REST_ENDPOINT_WHITELIST must be sorted in code to allow binary search",
        );
        assert!(
            GRPC_ENDPOINT_WHITELIST.array_windows().all(|[a, b]| a <= b),
            "GRPC_ENDPOINT_WHITELIST must be sorted in code to allow binary search",
        );
    }

    #[test]
    fn test_rest_whitelist_uses_collection_name_param() {
        use super::REST_ENDPOINT_WHITELIST;

        for endpoint in REST_ENDPOINT_WHITELIST {
            assert!(
                endpoint.contains("{collection_name}"),
                "REST_ENDPOINT_WHITELIST entry `{endpoint}` must use \
                 `{{collection_name}}` as the collection path parameter",
            );
        }
    }

    #[test]
    fn test_rest_metrics_global_mode() {
        use std::collections::HashMap;

        use segment::common::operation_time_statistics::OperationDurationStatistics;

        use super::{MetricsData, MetricsProvider, WebApiTelemetry};

        let mut responses = HashMap::new();
        let mut status_map = HashMap::new();
        status_map.insert(
            200u16,
            OperationDurationStatistics {
                count: 10,
                ..Default::default()
            },
        );
        responses.insert(
            "POST /collections/{collection_name}/points/search".to_string(),
            status_map,
        );

        let telemetry = WebApiTelemetry {
            responses,
            per_collection_responses: HashMap::new(),
        };

        let mut metrics = MetricsData::empty();
        telemetry.add_metrics(&mut metrics, None);
        let output = metrics.format_metrics();

        // Should contain global metrics without collection label
        assert!(output.contains("rest_responses_total"));
        assert!(!output.contains("collection="));
    }

    #[test]
    fn test_rest_metrics_per_collection_mode() {
        use std::collections::HashMap;

        use segment::common::operation_time_statistics::OperationDurationStatistics;

        use super::{MetricsData, MetricsProvider, WebApiTelemetry};

        // Global responses present but per_collection too
        let mut responses = HashMap::new();
        let mut status_map = HashMap::new();
        status_map.insert(
            200u16,
            OperationDurationStatistics {
                count: 10,
                ..Default::default()
            },
        );
        responses.insert(
            "POST /collections/{collection_name}/points/search".to_string(),
            status_map,
        );

        let mut per_collection = HashMap::new();
        let mut methods = HashMap::new();
        let mut col_status_map = HashMap::new();
        col_status_map.insert(
            200u16,
            OperationDurationStatistics {
                count: 5,
                ..Default::default()
            },
        );
        methods.insert(
            "POST /collections/{collection_name}/points/search".to_string(),
            col_status_map,
        );
        per_collection.insert("my_collection".to_string(), methods);

        let telemetry = WebApiTelemetry {
            responses,
            per_collection_responses: per_collection,
        };

        let mut metrics = MetricsData::empty();
        telemetry.add_metrics(&mut metrics, None);
        let output = metrics.format_metrics();

        // Should contain collection label
        assert!(
            output.contains("collection=\"my_collection\""),
            "Expected collection label in output:\n{output}"
        );
        // Should still have rest_ prefix metrics
        assert!(output.contains("rest_responses_total"));
    }

    #[test]
    fn test_grpc_metrics_per_collection_mode() {
        use std::collections::HashMap;

        use segment::common::operation_time_statistics::OperationDurationStatistics;

        use super::{GrpcTelemetry, MetricsData, MetricsProvider};

        let mut per_collection = HashMap::new();
        let mut methods = HashMap::new();
        let mut status_map = HashMap::new();
        status_map.insert(
            0i32,
            OperationDurationStatistics {
                count: 7,
                ..Default::default()
            },
        );
        methods.insert("/qdrant.Points/Search".to_string(), status_map);
        per_collection.insert("test_col".to_string(), methods);

        let telemetry = GrpcTelemetry {
            responses: HashMap::new(),
            per_collection_responses: per_collection,
        };

        let mut metrics = MetricsData::empty();
        telemetry.add_metrics(&mut metrics, None);
        let output = metrics.format_metrics();

        assert!(
            output.contains("collection=\"test_col\""),
            "Expected collection label in output:\n{output}"
        );
        assert!(output.contains("grpc_responses_total"));
    }

    #[test]
    fn test_per_collection_skips_non_whitelisted() {
        use std::collections::HashMap;

        use segment::common::operation_time_statistics::OperationDurationStatistics;

        use super::{MetricsData, MetricsProvider, WebApiTelemetry};

        let mut per_collection = HashMap::new();
        let mut methods = HashMap::new();
        let mut status_map = HashMap::new();
        status_map.insert(
            200u16,
            OperationDurationStatistics {
                count: 3,
                ..Default::default()
            },
        );
        // This endpoint is NOT in the whitelist
        methods.insert("GET /collections".to_string(), status_map);
        per_collection.insert("col".to_string(), methods);

        let telemetry = WebApiTelemetry {
            responses: HashMap::new(),
            per_collection_responses: per_collection,
        };

        let mut metrics = MetricsData::empty();
        telemetry.add_metrics(&mut metrics, None);
        let output = metrics.format_metrics();

        // Non-whitelisted endpoints should not appear
        assert!(
            !output.contains("collection=\"col\""),
            "Non-whitelisted endpoint should not appear:\n{output}"
        );
    }
}
