use prometheus::proto::MetricType;
use storage::types::ConsensusThreadStatus;

use crate::common::metrics::{MetricsData, MetricsProvider, counter, gauge, metric_family};
use crate::common::telemetry_ops::cluster_telemetry::{ClusterStatusTelemetry, ClusterTelemetry};

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

        // Initialize all states so that every state has a zeroed metric by default.
        let mut state_working = 0.0;
        let mut state_stopped = 0.0;

        match &self.consensus_thread_status {
            ConsensusThreadStatus::Working { last_update } => {
                let timestamp = last_update.timestamp();

                metrics.push_metric(metric_family(
                    "cluster_last_update_timestamp_seconds",
                    "unix timestamp of last update",
                    MetricType::COUNTER,
                    vec![counter(timestamp as f64, &[])],
                    prefix,
                ));

                state_working = 1.0;
            }
            ConsensusThreadStatus::Stopped | ConsensusThreadStatus::StoppedWithErr { err: _ } => {
                state_stopped = 1.0;
            }
        }

        let working_states = vec![
            gauge(state_working, &[("state", "working")]),
            gauge(state_stopped, &[("state", "stopped")]),
        ];

        metrics.push_metric(metric_family(
            "cluster_working_state",
            "working state of the cluster",
            MetricType::GAUGE,
            working_states,
            prefix,
        ));
    }
}
