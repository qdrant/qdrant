use std::collections::HashSet;

use segment::types::ShardKey;

use crate::collection::Collection;
use crate::config::ShardingMethod;
use crate::operations::types::CollectionError;
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::shards::replica_set::{ReplicaState, ShardReplicaSet};
use crate::shards::shard::{PeerId, ShardId, ShardsPlacement};

impl Collection {
    pub async fn create_replica_set(
        &self,
        shard_id: ShardId,
        replicas: &[PeerId],
    ) -> Result<ShardReplicaSet, CollectionError> {
        let is_local = replicas.contains(&self.this_peer_id);

        let peers = replicas
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != self.this_peer_id)
            .collect();

        ShardReplicaSet::build(
            shard_id,
            self.name(),
            self.this_peer_id,
            is_local,
            peers,
            self.notify_peer_failure_cb.clone(),
            self.abort_shard_transfer_cb.clone(),
            &self.path,
            self.collection_config.clone(),
            self.shared_storage_config.clone(),
            self.channel_service.clone(),
            self.update_runtime.clone(),
            self.search_runtime.clone(),
            Some(ReplicaState::Active),
        )
        .await
    }

    pub async fn create_shard_key(
        &self,
        shard_key: ShardKey,
        placement: ShardsPlacement,
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

        let max_shard_id = state.max_shard_id();

        let payload_schema = self.payload_index_schema.read().schema.clone();

        for (idx, shard_replicas_placement) in placement.iter().enumerate() {
            let shard_id = max_shard_id + idx as ShardId + 1;

            let replica_set = self
                .create_replica_set(shard_id, shard_replicas_placement)
                .await?;

            for (field_name, field_schema) in payload_schema.iter() {
                let create_index_op = CollectionUpdateOperations::FieldIndexOperation(
                    FieldIndexOperations::CreateIndex(CreateIndex {
                        field_name: field_name.clone(),
                        field_schema: Some(field_schema.clone()),
                    }),
                );

                replica_set.update_local(create_index_op, true).await?;
            }

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
            .await
    }
}
