use prometheus::proto::MetricType;

use super::helpers::{gauge, metric_family};
use super::{MetricsData, MetricsProvider};
use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppFeaturesTelemetry};

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

        if let Some(audit) = &self.audit
            && let Some(size) = audit.dir_size_bytes
        {
            metrics.push_metric(metric_family(
                "audit_log_dir_size_bytes",
                "size of the audit log directory on disk in bytes",
                MetricType::GAUGE,
                vec![gauge(size as f64, &[])],
                prefix,
            ));
        }

        if let Some(system) = &self.system
            && let Some(cpu_cores_used) = system.cpu_cores_used
        {
            metrics.push_metric(metric_family(
                "cpu_cores_used",
                "average number of CPU cores used by this process over roughly the last two seconds",
                MetricType::GAUGE,
                vec![gauge(f64::from(cpu_cores_used), &[])],
                prefix,
            ));
        }
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
