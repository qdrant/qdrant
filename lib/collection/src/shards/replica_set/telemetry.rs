use std::ops::Deref as _;
use std::time::Duration;

use common::types::TelemetryDetail;
use segment::types::SizeStats;

use crate::operations::types::{CollectionResult, OptimizersStatus};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::telemetry::{PartialSnapshotTelemetry, ReplicaSetTelemetry};

impl ShardReplicaSet {
    pub(crate) async fn get_telemetry_data(
        &self,
        detail: TelemetryDetail,
        timeout: Duration,
    ) -> CollectionResult<ReplicaSetTelemetry> {
        let local_shard = self.local.read().await;
        let local = local_shard.as_ref();

        let local_telemetry = match local {
            Some(local_shard) => Some(local_shard.get_telemetry_data(detail, timeout).await?),
            None => None,
        };

        Ok(ReplicaSetTelemetry {
            id: self.shard_id,
            key: self.shard_key.clone(),
            local: local_telemetry,
            remote: self
                .remotes
                .read()
                .await
                .iter()
                .map(|remote| remote.get_telemetry_data(detail))
                .collect(),
            replicate_states: self.replica_state.read().peers(),
            partial_snapshot: Some(PartialSnapshotTelemetry {
                ongoing_create_snapshot_requests: self
                    .partial_snapshot_meta
                    .ongoing_create_snapshot_requests(),
                is_recovering: self.partial_snapshot_meta.is_recovery_lock_taken(),
                recovery_timestamp: self.partial_snapshot_meta.recovery_timestamp(),
            }),
        })
    }

    pub(crate) async fn get_optimization_status(
        &self,
        timeout: Duration,
    ) -> Option<CollectionResult<OptimizersStatus>> {
        let local_shard = self.local.read().await;

        let Some(local) = local_shard.deref() else {
            return None;
        };

        Some(local.get_optimization_status(timeout).await)
    }

    pub(crate) async fn get_size_stats(&self) -> SizeStats {
        let local_shard = self.local.read().await;

        let Some(local) = local_shard.deref() else {
            return SizeStats::default();
        };

        local.get_size_stats().await
    }
}
