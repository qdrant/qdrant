use prometheus::proto::MetricType;
use storage::quota::{QuotaExceeded, QuotaTelemetry};

use super::helpers::{gauge, metric_family};
use super::{MetricsData, MetricsProvider};

impl MetricsProvider for QuotaTelemetry {
    fn add_metrics(&self, metrics: &mut MetricsData, prefix: Option<&str>) {
        let QuotaExceeded {
            resident_memory,
            disk_usage,
        } = self.exceeded;

        // One series per capped resource, and none for a resource with no limit:
        // a series that can never reach 1 reads as "healthy" and would silently
        // carry an alert that cannot fire. So the quota being disabled, or a
        // resource being left uncapped, shows up as an absent series rather than
        // a reassuring zero.
        let series = [("memory", resident_memory), ("disk", disk_usage)]
            .into_iter()
            .filter_map(|(resource, exceeded)| {
                let exceeded = exceeded?;
                Some(gauge(
                    f64::from(u8::from(exceeded)),
                    &[("resource", resource)],
                ))
            })
            .collect();

        metrics.push_metric(metric_family(
            "quota_exceeded",
            "whether this node is at or over the configured quota for a resource",
            MetricType::GAUGE,
            series,
            prefix,
        ));
    }
}
