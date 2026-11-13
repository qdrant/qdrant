use prometheus::proto::MetricType;
use segment::index::hnsw_index::remote::metrics::REMOTE_INDEXING_METRICS;
use segment::index::hnsw_index::remote::registry::REMOTE_INDEXING_TASKS;

use super::MetricsData;
use super::helpers::{counter, gauge, metric_family};

/// Remote indexing counters (§4.9), exported when remote indexing is configured.
pub(super) fn add_metrics(metrics: &mut MetricsData, prefix: Option<&str>) {
    for (name, help, value) in REMOTE_INDEXING_METRICS.snapshot() {
        metrics.push_metric(metric_family(
            &format!("remote_indexing_{name}_total"),
            help,
            MetricType::COUNTER,
            vec![counter(value as f64, &[])],
            prefix,
        ));
    }
    metrics.push_metric(metric_family(
        "remote_indexing_tasks_active",
        "remote indexing tasks waiting for an indexer",
        MetricType::GAUGE,
        vec![gauge(REMOTE_INDEXING_TASKS.len() as f64, &[])],
        prefix,
    ));
}
