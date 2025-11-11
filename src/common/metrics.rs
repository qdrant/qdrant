use std::collections::HashMap;

use api::rest::models::HardwareUsage;
use collection::shards::replica_set::ReplicaState;
use itertools::Itertools;
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
        self.collections.add_metrics(metrics, prefix);
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
        match ProcFsMetrics::collect() {
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

impl MetricsProvider for CollectionsTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "collections_total",
            "number of collections",
            MetricType::GAUGE,
            vec![gauge(self.number_of_collections as f64, &[])],
            prefix,
        ));

        // Optimizers
        let mut total_optimizations_running = 0;

        // Min/Max/Expected/Active replicas over all shards.
        let mut total_min_active_replicas = usize::MAX;
        let mut total_max_active_replicas = 0;

        // Points per collection
        let mut points_per_collection = vec![];

        // Vectors excluded from index-only requests.
        let mut indexed_only_excluded = vec![];

        let mut total_dead_replicas = 0;

        let mut vector_count_by_name = vec![];

        for collection in self.collections.iter().flatten() {
            let collection = match collection {
                CollectionTelemetryEnum::Full(collection_telemetry) => collection_telemetry,
                CollectionTelemetryEnum::Aggregated(_) => {
                    continue;
                }
            };

            total_optimizations_running += collection.count_optimizers_running();

            let min_max_active_replicas = collection
                .shards
                .iter()
                .flatten()
                // While resharding up, some (shard) replica sets may still be incomplete during
                // the resharding process. In that case we don't want to consider these replica
                // sets at all in the active replica calculation. This is fine because searches nor
                // updates don't depend on them being available yet.
                //
                // More specifically:
                // - in stage 2 (migrate points) of resharding up we don't rely on the replica
                //   to be available yet. In this stage, these replicas will have the `Resharding`
                //   state.
                // - in stage 3 (replicate) of resharding up we activate the the replica and
                //   replicate to match the configuration replication factor. From this point on we
                //   do rely on the replica to be available. Now one replica will be `Active`, and
                //   the other replicas will be in a transfer state. No replica will have `Resharding`
                //   state.
                //
                // So, during stage 2 of resharding up we don't want to adjust the minimum number
                // of active replicas downwards. During stage 3 we do want it to affect the minimum
                // available replica number. It will be 1 for some time until replication transfers
                // complete.
                //
                // To ignore a (shard) replica set that is in stage 2 of resharding up, we simply
                // check if any of it's replicas is in `Resharding` state.
                .filter(|shard| {
                    !shard
                        .replicate_states
                        .values()
                        .any(|i| matches!(i, ReplicaState::Resharding))
                })
                .map(|shard| {
                    shard
                        .replicate_states
                        .values()
                        // While resharding down, all the replicas that we keep will get the
                        // `ReshardingScaleDown` state for a period of time. We simply consider
                        // these replicas to be active. The `is_active` function already accounts
                        // this.
                        .filter(|state| state.is_active())
                        .count()
                })
                .minmax();

            let min_max_active_replicas = match min_max_active_replicas {
                itertools::MinMaxResult::NoElements => None,
                itertools::MinMaxResult::OneElement(one) => Some((one, one)),
                itertools::MinMaxResult::MinMax(min, max) => Some((min, max)),
            };

            if let Some((min, max)) = min_max_active_replicas {
                total_min_active_replicas = total_min_active_replicas.min(min);
                total_max_active_replicas = total_max_active_replicas.max(max);
            }

            points_per_collection.push(gauge(
                collection.count_points() as f64,
                &[("id", &collection.id)],
            ));

            for (vec_name, count) in collection.count_points_per_vector() {
                vector_count_by_name.push(gauge(
                    count as f64,
                    &[("collection", &collection.id), ("vector", &vec_name)],
                ))
            }

            let points_excluded_from_index_only = collection
                .shards
                .iter()
                .flatten()
                .filter_map(|shard| shard.local.as_ref())
                .filter_map(|local| local.indexed_only_excluded_vectors.as_ref())
                .flatten()
                .fold(
                    HashMap::<&str, usize>::default(),
                    |mut acc, (name, vector_size)| {
                        *acc.entry(name).or_insert(0) += vector_size;
                        acc
                    },
                );

            for (name, vector_size) in points_excluded_from_index_only {
                indexed_only_excluded.push(gauge(
                    vector_size as f64,
                    &[("id", &collection.id), ("vector", name)],
                ))
            }

            total_dead_replicas += collection
                .shards
                .iter()
                .flatten()
                .filter(|i| i.replicate_states.values().any(|state| !state.is_active()))
                .count();
        }

        metrics.push_metric(metric_family(
            "collection_vectors",
            "amount of vectors grouped by vector name",
            MetricType::GAUGE,
            vector_count_by_name,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_indexed_only_excluded_points",
            "amount of points excluded in indexed_only requests",
            MetricType::GAUGE,
            indexed_only_excluded,
            prefix,
        ));

        let total_min_active_replicas = if total_min_active_replicas == usize::MAX {
            0
        } else {
            total_min_active_replicas
        };

        metrics.push_metric(metric_family(
            "active_replicas_min",
            "minimum number of active replicas across all shards",
            MetricType::GAUGE,
            vec![gauge(total_min_active_replicas as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "active_replicas_max",
            "maximum number of active replicas across all shards",
            MetricType::GAUGE,
            vec![gauge(total_max_active_replicas as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "optimizer_running_processes",
            "number of currently running optimization processes",
            MetricType::GAUGE,
            vec![gauge(total_optimizations_running as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_points",
            "approximate amount of points per collection",
            MetricType::GAUGE,
            points_per_collection,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "dead_replicas",
            "total amount of shard replicas in non-active state",
            MetricType::GAUGE,
            vec![gauge(total_dead_replicas as f64, &[])],
            prefix,
        ));
    }
}

impl MetricsProvider for ClusterTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        let ClusterTelemetry {
            enabled,
            status,
            config: _,
            peers: _,
            peer_metadata: _,
            metadata: _,
        } = self;

        metrics.push_metric(metric_family(
            "cluster_enabled",
            "is cluster support enabled",
            MetricType::GAUGE,
            vec![gauge(if *enabled { 1.0 } else { 0.0 }, &[])],
            prefix,
        ));

        if let Some(status) = status {
            status.add_metrics(metrics, prefix);
        }
    }
}

impl MetricsProvider for ClusterStatusTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "cluster_peers_total",
            "total number of cluster peers",
            MetricType::GAUGE,
            vec![gauge(self.number_of_peers as f64, &[])],
            prefix,
        ));
        metrics.push_metric(metric_family(
            "cluster_term",
            "current cluster term",
            MetricType::COUNTER,
            vec![counter(self.term as f64, &[])],
            prefix,
        ));

        if let Some(ref peer_id) = self.peer_id.map(|p| p.to_string()) {
            metrics.push_metric(metric_family(
                "cluster_commit",
                "index of last committed (finalized) operation cluster peer is aware of",
                MetricType::COUNTER,
                vec![counter(self.commit as f64, &[("peer_id", peer_id)])],
                prefix,
            ));
            metrics.push_metric(metric_family(
                "cluster_pending_operations_total",
                "total number of pending operations for cluster peer",
                MetricType::GAUGE,
                vec![gauge(self.pending_operations as f64, &[])],
                prefix,
            ));
            metrics.push_metric(metric_family(
                "cluster_voter",
                "is cluster peer a voter or learner",
                MetricType::GAUGE,
                vec![gauge(if self.is_voter { 1.0 } else { 0.0 }, &[])],
                prefix,
            ));
        }
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

fn metric_family(
    name: &str,
    help: &str,
    r#type: MetricType,
    metrics: Vec<Metric>,
    prefix: Option<&str>,
) -> Option<MetricFamily> {
    // We can't create a new `MetricsFamily` without metrics.
    if metrics.is_empty() {
        return None;
    }

    let mut metric_family = MetricFamily::default();

    let name_with_prefix = prefix
        .map(|prefix| format!("{prefix}{name}"))
        .unwrap_or_else(|| name.to_string());

    metric_family.set_name(name_with_prefix);
    metric_family.set_help(help.into());
    metric_family.set_field_type(r#type);
    metric_family.set_metric(metrics);

    Some(metric_family)
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

/// Structure for holding /procfs metrics, that can be easily populated in metrics API.
#[cfg(target_os = "linux")]
struct ProcFsMetrics {
    thread_count: usize,
    mmap_count: usize,
    system_mmap_limit: u64,
    open_fds: usize,
    max_fds_soft: u64,
    max_fds_hard: u64,
    minor_page_faults: u64,
    major_page_faults: u64,
    minor_children_page_faults: u64,
    major_children_page_faults: u64,
}

#[cfg(target_os = "linux")]
impl ProcFsMetrics {
    /// Collect metrics from /procfs.
    #[cfg(target_os = "linux")]
    fn collect() -> Result<Self, procfs::ProcError> {
        use procfs::process::{LimitValue, Process};

        let current_process = Process::myself()?;

        let thread_count = current_process.tasks()?.flatten().count();
        let stat = current_process.stat()?;
        let limits = current_process.limits()?;

        fn format_limit(limit: LimitValue) -> u64 {
            match limit {
                LimitValue::Unlimited => 0,
                LimitValue::Value(v) => v,
            }
        }

        let max_fds_soft = format_limit(limits.max_open_files.soft_limit);
        let max_fds_hard = format_limit(limits.max_open_files.hard_limit);

        Ok(Self {
            thread_count,
            mmap_count: current_process.maps()?.len(),
            system_mmap_limit: procfs::sys::vm::max_map_count()?,
            open_fds: current_process.fd_count()?,
            max_fds_soft,
            max_fds_hard,
            minor_page_faults: stat.minflt,
            major_page_faults: stat.majflt,
            minor_children_page_faults: stat.cminflt,
            major_children_page_faults: stat.cmajflt,
        })
    }
}

#[cfg(target_os = "linux")]
impl MetricsProvider for ProcFsMetrics {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        metrics.push_metric(metric_family(
            "process_threads",
            "count of active threads",
            MetricType::GAUGE,
            vec![gauge(self.thread_count as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_open_mmaps",
            "count of open mmaps",
            MetricType::GAUGE,
            vec![gauge(self.mmap_count as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "system_max_mmaps",
            "system wide limit of open mmaps",
            MetricType::GAUGE,
            vec![gauge(self.system_mmap_limit as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_open_fds",
            "count of currently open file descriptors",
            MetricType::GAUGE,
            vec![gauge(self.open_fds as f64, &[])],
            prefix,
        ));

        let fds_limit = match (self.max_fds_soft, self.max_fds_hard) {
            (0, hard) => hard,                         // soft unlimited, use hard
            (soft, 0) => soft,                         // hard unlimited, use soft
            (soft, hard) => std::cmp::min(soft, hard), // both limited, use minimum
        };
        metrics.push_metric(metric_family(
            "process_max_fds",
            "limit for open file descriptors",
            MetricType::GAUGE,
            vec![gauge(fds_limit as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_minor_page_faults_total",
            "count of minor page faults which didn't cause a disk access",
            MetricType::COUNTER,
            vec![counter(self.minor_page_faults as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_major_page_faults_total",
            "count of disk accesses caused by a mmap page fault",
            MetricType::COUNTER,
            vec![counter(self.major_page_faults as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_minor_page_faults_children_total",
            "count of minor page faults caused by waited-for child processes",
            MetricType::COUNTER,
            vec![counter(self.minor_children_page_faults as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "process_major_page_faults_children_total",
            "count of major page faults caused by waited-for child processes",
            MetricType::COUNTER,
            vec![counter(self.major_children_page_faults as f64, &[])],
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
