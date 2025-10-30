use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::types::{DetailsLevel, TelemetryDetail};

use crate::collection::Collection;
use crate::telemetry::{CollectionConfigTelemetry, CollectionTelemetry};

/// Collects telemetry data for a collection.
#[derive(Default)]
pub struct CollectionTelemetryCollector {
    // Counter for currently running snapshot tasks.
    pub running_snapshots: Arc<AtomicUsize>,

    // Counter for snapshot creations since startup, until now.
    pub snapshots_total: Arc<AtomicUsize>,
}

impl Collection {
    pub async fn get_telemetry_data(&self, detail: TelemetryDetail) -> CollectionTelemetry {
        let (shards_telemetry, transfers, resharding) = {
            if detail.level >= DetailsLevel::Level3 {
                let shards_holder = self.shards_holder.read().await;
                let mut shards_telemetry = Vec::new();
                for shard in shards_holder.all_shards() {
                    shards_telemetry.push(shard.get_telemetry_data(detail).await)
                }
                (
                    Some(shards_telemetry),
                    Some(shards_holder.get_shard_transfer_info(&*self.transfer_tasks.lock().await)),
                    Some(
                        shards_holder
                            .get_resharding_operations_info()
                            .unwrap_or_default(),
                    ),
                )
            } else {
                (None, None, None)
            }
        };

        let shard_clean_tasks = self.clean_local_shards_statuses();

        let running_snapshots = self
            .telemetry_stats
            .running_snapshots
            .load(Ordering::Relaxed);

        let total_snapshots = self.telemetry_stats.snapshots_total.load(Ordering::Relaxed);

        CollectionTelemetry {
            id: self.name(),
            init_time_ms: self.init_time.as_millis() as u64,
            config: CollectionConfigTelemetry::from(self.collection_config.read().await.clone()),
            shards: shards_telemetry,
            transfers,
            resharding,
            shard_clean_tasks: (!shard_clean_tasks.is_empty()).then_some(shard_clean_tasks),
            running_snapshots: Some(running_snapshots),
            total_snapshot_creations: Some(total_snapshots),
        }
    }
}
