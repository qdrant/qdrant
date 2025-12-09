use ahash::HashMap;
use collection::telemetry::CollectionTelemetry;
use prometheus::proto::{Metric, MetricType};
use shard::PeerId;

use crate::common::metrics::MetricsData;
use crate::common::metrics::common::{counter, gauge, metric_family};
use crate::common::telemetry_ops::collections_telemetry::{
    CollectionTelemetryEnum, CollectionsTelemetry,
};

impl CollectionsTelemetry {
    pub(super) fn add_metrics(
        &self,
        metrics: &mut MetricsData,
        prefix: Option<&str>,
        peer_id: Option<PeerId>,
    ) {
        metrics.push_metric(metric_family(
            "collections_total",
            "number of collections",
            MetricType::GAUGE,
            vec![gauge(self.number_of_collections as f64, &[])],
            prefix,
        ));

        let num_collections = self.collections.as_ref().map_or(0, |c| c.len());

        // Optimizers
        let mut total_optimizations_running = Vec::with_capacity(num_collections);

        // Min/Max/Expected/Active replicas over all shards.
        let mut total_min_active_replicas = usize::MAX;
        let mut total_max_active_replicas = 0;
        let mut total_dead_replicas = 0;

        // Points per collection
        let mut points_per_collection = Vec::with_capacity(num_collections);

        // Vectors excluded from index-only requests.
        let mut indexed_only_excluded = Vec::with_capacity(num_collections);

        // Snapshot metrics
        let mut snapshots_creation_running = Vec::with_capacity(num_collections);
        let mut snapshots_recovery_running = Vec::with_capacity(num_collections);
        let mut snapshots_created_total = Vec::with_capacity(num_collections);

        let mut vector_count_by_name = Vec::with_capacity(num_collections);

        // Shard transfers
        let mut shard_transfers_in = Vec::with_capacity(num_collections);
        let mut shard_transfers_out = Vec::with_capacity(num_collections);
        let mut shard_transfers_failed = Vec::with_capacity(num_collections);
        let mut shard_transfers_points_transferred = Vec::with_capacity(num_collections);

        for collection in self.collections.iter().flatten() {
            let collection = match collection {
                CollectionTelemetryEnum::Full(collection_telemetry) => collection_telemetry,
                CollectionTelemetryEnum::Aggregated(_) => {
                    continue;
                }
            };

            total_optimizations_running.push(gauge(
                collection.count_optimizers_running() as f64,
                &[("id", &collection.id)],
            ));

            if let Some((min, max)) = collection.active_replica_min_max() {
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

            points_excluded_from_index_only_metric(collection, &mut indexed_only_excluded);

            total_dead_replicas += collection.dead_replicas();

            shard_transfer_metrics(
                collection,
                peer_id,
                &mut shard_transfers_in,
                &mut shard_transfers_out,
                &mut shard_transfers_failed,
                &mut shard_transfers_points_transferred,
            );
        }

        snapshot_metrics(
            self,
            &mut snapshots_recovery_running,
            &mut snapshots_creation_running,
            &mut snapshots_created_total,
        );

        let vector_count = vector_count_by_name
            .iter()
            .map(|m| m.get_gauge().get_value())
            .sum::<f64>()
            // The sum of an empty f64 iterator returns `-0`. Since a negative
            // number of vectors is impossible, taking the absolute value is always safe.
            .abs();

        metrics.push_metric(metric_family(
            "collections_vector_total",
            "total number of vectors in all collections",
            MetricType::GAUGE,
            vec![gauge(vector_count, &[])],
            prefix,
        ));

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
            "collection_active_replicas_min",
            "minimum number of active replicas across all shards",
            MetricType::GAUGE,
            vec![gauge(total_min_active_replicas as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_active_replicas_max",
            "maximum number of active replicas across all shards",
            MetricType::GAUGE,
            vec![gauge(total_max_active_replicas as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_running_optimizations",
            "number of currently running optimization tasks per collection",
            MetricType::GAUGE,
            total_optimizations_running,
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
            "collection_dead_replicas",
            "total amount of shard replicas in non-active state",
            MetricType::GAUGE,
            vec![gauge(total_dead_replicas as f64, &[])],
            prefix,
        ));

        metrics.push_metric(metric_family(
            "snapshot_creation_running",
            "amount of snapshot creations that are currently running",
            MetricType::GAUGE,
            snapshots_creation_running,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "snapshot_recovery_running",
            "amount of snapshot recovery operations currently running",
            MetricType::GAUGE,
            snapshots_recovery_running,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "snapshot_created_total",
            "total amount of snapshots created",
            MetricType::COUNTER,
            snapshots_created_total,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_shard_transfer_incoming",
            "incoming shard transfers currently running",
            MetricType::GAUGE,
            shard_transfers_in,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_shard_transfer_outgoing",
            "outgoing shard transfers currently running",
            MetricType::GAUGE,
            shard_transfers_out,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_shard_transfer_failed",
            "number of failed shard transfers",
            MetricType::GAUGE,
            shard_transfers_failed,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_shard_transfer_transferred_points",
            "number of points already transferred",
            MetricType::GAUGE,
            shard_transfers_points_transferred,
            prefix,
        ));
    }
}

fn points_excluded_from_index_only_metric(collection: &CollectionTelemetry, out: &mut Vec<Metric>) {
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
        out.push(gauge(
            vector_size as f64,
            &[("id", &collection.id), ("vector", name)],
        ))
    }
}

fn shard_transfer_metrics(
    collection: &CollectionTelemetry,
    peer_id: Option<PeerId>,
    shard_transfers_in: &mut Vec<Metric>,
    shard_transfers_out: &mut Vec<Metric>,
    shard_transfers_failed: &mut Vec<Metric>,
    shard_transfers_points_transferred: &mut Vec<Metric>,
) {
    let mut incoming_transfers = 0;
    let mut outgoing_transfers = 0;
    let mut failed_transfers = 0;
    let mut points_transferred = 0;

    if let Some(this_peer_id) = peer_id {
        for transfer in collection.transfers.iter().flatten() {
            let status = transfer.status.as_ref();

            // On the receiving peer, we don't have a status so we assume `true` if not set.
            let running = status.map(|i| i.running()).unwrap_or(true);

            // Non running transfers
            if !running {
                if status.is_some_and(|i| i.failed()) {
                    failed_transfers += 1;
                }
                continue;
            }

            if transfer.to == this_peer_id {
                incoming_transfers += 1;
            }
            if transfer.from == this_peer_id {
                outgoing_transfers += 1;
            }

            points_transferred += status.map(|i| i.points_transferred).unwrap_or_default();
        }
    }

    shard_transfers_in.push(gauge(
        f64::from(incoming_transfers),
        &[("id", &collection.id)],
    ));
    shard_transfers_out.push(gauge(
        f64::from(outgoing_transfers),
        &[("id", &collection.id)],
    ));
    shard_transfers_failed.push(gauge(
        f64::from(failed_transfers),
        &[("id", &collection.id)],
    ));
    shard_transfers_points_transferred
        .push(gauge(points_transferred as f64, &[("id", &collection.id)]));
}

fn snapshot_metrics(
    collection: &CollectionsTelemetry,
    snapshots_recovery_running: &mut Vec<Metric>,
    snapshots_creation_running: &mut Vec<Metric>,
    snapshots_created_total: &mut Vec<Metric>,
) {
    for snapshot_telemetry in collection.snapshots.iter().flatten() {
        let id = &snapshot_telemetry.id;

        snapshots_recovery_running.push(gauge(
            snapshot_telemetry
                .running_snapshot_recovery
                .unwrap_or_default() as f64,
            &[("id", id)],
        ));
        snapshots_creation_running.push(gauge(
            snapshot_telemetry.running_snapshots.unwrap_or_default() as f64,
            &[("id", id)],
        ));

        snapshots_created_total.push(counter(
            snapshot_telemetry
                .total_snapshot_creations
                .unwrap_or_default() as f64,
            &[("id", id)],
        ));
    }
}
