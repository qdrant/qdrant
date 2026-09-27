use std::collections::HashMap;

use collection::shards::replica_set::replica_set_state::ReplicaState;
use itertools::Itertools;
use prometheus::proto::MetricType;
use shard::PeerId;

use super::MetricsData;
use super::helpers::{counter, gauge, metric_family};
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

        // Points per collection
        let mut points_per_collection = Vec::with_capacity(num_collections);

        // Vectors excluded from index-only requests.
        let mut indexed_only_excluded = Vec::with_capacity(num_collections);

        let mut total_dead_replicas = 0;

        // Snapshot metrics
        let mut snapshots_creation_running = Vec::with_capacity(num_collections);
        let mut snapshots_recovery_running = Vec::with_capacity(num_collections);
        let mut snapshots_created_total = Vec::with_capacity(num_collections);

        let mut vector_count_by_name = Vec::with_capacity(num_collections);

        // Shard transfers
        let mut shard_transfers_in = Vec::with_capacity(num_collections);
        let mut shard_transfers_out = Vec::with_capacity(num_collections);

        // Update queue
        let mut update_queue_length = Vec::with_capacity(num_collections);
        let mut deferred_points_count = Vec::with_capacity(num_collections);

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
                // - in stage 3 (replicate) of resharding up we activate the replica and
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

            // Shard Transfers

            let mut incoming_transfers = 0;
            let mut outgoing_transfers = 0;

            if let Some(this_peer_id) = peer_id {
                for transfer in collection.transfers.iter().flatten() {
                    if transfer.to == this_peer_id {
                        incoming_transfers += 1;
                    }
                    if transfer.from == this_peer_id {
                        outgoing_transfers += 1;
                    }
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

            // Update queue
            let (total_queue_length, total_deferred_count): (usize, usize) = collection
                .shards
                .iter()
                .flatten()
                .filter_map(|shard| shard.local.as_ref())
                .filter_map(|local| local.update_queue.as_ref())
                .map(|uq| (uq.length, uq.deferred_points))
                .fold((0, 0), |(acc_queue, acc_deferred), (queue, deferred)| {
                    (
                        acc_queue + queue,
                        acc_deferred + deferred.unwrap_or_default(),
                    )
                });

            update_queue_length.push(gauge(total_queue_length as f64, &[("id", &collection.id)]));
            deferred_points_count.push(gauge(
                total_deferred_count as f64,
                &[("id", &collection.id)],
            ));
        }

        for snapshot_telemetry in self.snapshots.iter().flatten() {
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
            "collection_update_queue_length",
            "number of pending operations in update queues per collection",
            MetricType::GAUGE,
            update_queue_length,
            prefix,
        ));

        metrics.push_metric(metric_family(
            "collection_update_queue_deferred_points",
            "number of points currently hidden during read operations as they're not yet optimized",
            MetricType::GAUGE,
            deferred_points_count,
            prefix,
        ));
    }
}
