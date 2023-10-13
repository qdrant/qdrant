use std::collections::HashSet;

use crate::collection::Collection;
use crate::config::ShardingMethod;
use crate::operations::types::CollectionError;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardKey};

impl Collection {
    pub async fn create_shard_key(
        &self,
        shard_key: ShardKey,
        placement: Vec<Vec<PeerId>>,
    ) -> Result<(), CollectionError> {
        let state = self.state().await;
        match state.config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "Shard Key {} cannot be created with Auto sharding method",
                    shard_key
                )));
            }
            ShardingMethod::Custom => {}
        }

        if state.shards_key_mapping.contains_key(&shard_key) {
            return Err(CollectionError::bad_request(format!(
                "Shard key {} already exists",
                shard_key
            )));
        }

        let all_peers: HashSet<_> = self
            .channel_service
            .id_to_address
            .read()
            .keys()
            .cloned()
            .collect();

        let unknown_peers: Vec<_> = placement
            .iter()
            .flatten()
            .filter(|peer_id| !all_peers.contains(peer_id))
            .collect();

        if !unknown_peers.is_empty() {
            return Err(CollectionError::bad_request(format!(
                "Shard Key {} placement contains unknown peers: {:?}",
                shard_key, unknown_peers
            )));
        }

        let max_shard_id = state
            .shards_key_mapping
            .values()
            .flat_map(|shard_ids| shard_ids.iter())
            .max()
            .copied()
            .unwrap_or(0);

        for shard_replicas_placement in placement {
            let shard_id = max_shard_id + 1;

            let is_local = shard_replicas_placement.contains(&self.this_peer_id);

            let peers = shard_replicas_placement
                .iter()
                .copied()
                .filter(|peer_id| *peer_id != self.this_peer_id)
                .collect();

            let replica_set = ShardReplicaSet::build(
                shard_id,
                self.name(),
                self.this_peer_id,
                is_local,
                peers,
                self.notify_peer_failure_cb.clone(),
                &self.path,
                self.collection_config.clone(),
                self.shared_storage_config.clone(),
                self.channel_service.clone(),
                self.update_runtime.clone(),
                self.search_runtime.clone(),
            )
            .await?;

            self.shards_holder.write().await.add_shard(
                shard_id,
                replica_set,
                Some(shard_key.clone()),
            )?;
        }
        Ok(())
    }

    pub async fn drop_shard_key(&self, shard_key: ShardKey) -> Result<(), CollectionError> {
        let state = self.state().await;

        match state.config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "Shard Key {} cannot be removed with Auto sharding method",
                    shard_key
                )));
            }
            ShardingMethod::Custom => {}
        }

        self.shards_holder
            .write()
            .await
            .remove_shard_key(&shard_key)
    }
}
