//! Render telemetry as Prometheus metrics.

mod app;
mod cluster;
mod collections;
mod hardware;
mod helpers;
mod memory;
#[cfg(target_os = "linux")]
mod procfs_metrics;
mod quota;
mod requests;

use prometheus::TextEncoder;
use prometheus::proto::MetricFamily;

use crate::common::telemetry::TelemetryData;

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
        if let Some(app) = &self.app {
            app.add_metrics(metrics, prefix);
        }

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
        if let Some(quota) = &self.quota {
            quota.add_metrics(metrics, prefix);
        }

        #[cfg(target_os = "linux")]
        match procfs_metrics::ProcFsMetrics::collect() {
            Ok(procfs_provider) => procfs_provider.add_metrics(metrics, prefix),
            Err(err) => log::warn!("Error reading procfs infos: {err:?}"),
        };
    }
}
