use std::time::Duration;

use common::types::{DetailsLevel, TelemetryDetail};

use crate::collection::Collection;
use crate::operations::types::CollectionResult;
use crate::telemetry::{CollectionConfigTelemetry, CollectionTelemetry};

impl Collection {
    pub async fn get_telemetry_data(
        &self,
        detail: TelemetryDetail,
        timeout: Duration,
    ) -> CollectionResult<CollectionTelemetry> {
        let (shards_telemetry, transfers, resharding) = {
            if detail.level >= DetailsLevel::Level3 {
                let shards_holder = self.shards_holder.read().await;
                let mut shards_telemetry = Vec::new();
                for shard in shards_holder.all_shards() {
                    shards_telemetry.push(shard.get_telemetry_data(detail, timeout).await?)
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

        Ok(CollectionTelemetry {
            id: self.name().to_string(),
            init_time_ms: self.init_time.as_millis() as u64,
            config: CollectionConfigTelemetry::from(self.collection_config.read().await.clone()),
            shards: shards_telemetry,
            transfers,
            resharding,
            shard_clean_tasks: (!shard_clean_tasks.is_empty()).then_some(shard_clean_tasks),
        })
    }
}
