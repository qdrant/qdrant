use common::types::TelemetryDetail;
use segment::types::SizeStats;

use crate::operations::types::OptimizersStatus;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::telemetry::ReplicaSetTelemetry;

impl ShardReplicaSet {
    pub(crate) async fn get_telemetry_data(&self, detail: TelemetryDetail) -> ReplicaSetTelemetry {
        let local_shard = self.local.read().await;
        let local = local_shard.as_ref();

        let local_telemetry = match local {
            Some(local_shard) => Some(local_shard.get_telemetry_data(detail).await),
            None => None,
        };

        ReplicaSetTelemetry {
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
        }
    }

    pub(crate) async fn get_optimization_status(&self) -> Option<OptimizersStatus> {
        let local_shard = self.local.read().await;
        let local = local_shard.as_ref();

        local.map(|local_shard| local_shard.get_optimization_status())
    }

    pub(crate) async fn get_size_stats(&self) -> SizeStats {
        let local_shard = self.local.read().await;
        let local = local_shard.as_ref();

        local
            .map(|local_shard| local_shard.get_size_stats())
            .unwrap_or_default()
    }
}
