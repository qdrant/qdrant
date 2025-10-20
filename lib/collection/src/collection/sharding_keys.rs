use std::collections::HashSet;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::ShardKey;

use crate::collection::Collection;
use crate::config::ShardingMethod;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::{
    CollectionUpdateOperations, CreateIndex, FieldIndexOperations, OperationWithClockTag,
};
use crate::shards::replica_set::{ReplicaState, ShardReplicaSet};
use crate::shards::shard::{PeerId, ShardId, ShardsPlacement};

impl Collection {
    pub async fn create_replica_set(
        &self,
        shard_id: ShardId,
        shard_key: Option<ShardKey>,
        replicas: &[PeerId],
        init_state: Option<ReplicaState>,
    ) -> CollectionResult<ShardReplicaSet> {
        let is_local = replicas.contains(&self.this_peer_id);

        let peers = replicas
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != self.this_peer_id)
            .collect();

        let effective_optimizers_config = self.effective_optimizers_config().await?;

        ShardReplicaSet::build(
            shard_id,
            shard_key,
            self.name(),
            self.this_peer_id,
            is_local,
            peers,
            self.notify_peer_failure_cb.clone(),
            self.abort_shard_transfer_cb.clone(),
            &self.path,
            self.collection_config.clone(),
            effective_optimizers_config,
            self.shared_storage_config.clone(),
            self.payload_index_schema.clone(),
            self.channel_service.clone(),
            self.update_runtime.clone(),
            self.search_runtime.clone(),
            self.optimizer_resource_budget.clone(),
            Some(init_state.unwrap_or(ReplicaState::Active)),
        )
        .await
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn create_shard_key(
        &self,
        shard_key: ShardKey,
        placement: ShardsPlacement,
        init_state: ReplicaState,
    ) -> CollectionResult<()> {
        let hw_counter = HwMeasurementAcc::disposable(); // Internal operation. No measurement needed.

        let state = self.state().await;
        match state.config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "Shard Key {shard_key} cannot be created with Auto sharding method"
                )));
            }
            ShardingMethod::Custom => {}
        }

        if state.shards_key_mapping.contains_key(&shard_key) {
            return Err(CollectionError::bad_request(format!(
                "Shard key {shard_key} already exists"
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
                "Shard Key {shard_key} placement contains unknown peers: {unknown_peers:?}"
            )));
        }

        let max_shard_id = state.max_shard_id();
        let payload_schema = self.payload_index_schema.read().schema.clone();

        for (idx, shard_replicas_placement) in placement.iter().enumerate() {
            let shard_id = max_shard_id + idx as ShardId + 1;

            let replica_set = self
                .create_replica_set(
                    shard_id,
                    Some(shard_key.clone()),
                    shard_replicas_placement,
                    Some(init_state),
                )
                .await?;

            for (field_name, field_schema) in payload_schema.iter() {
                let create_index_op = CollectionUpdateOperations::FieldIndexOperation(
                    FieldIndexOperations::CreateIndex(CreateIndex {
                        field_name: field_name.clone(),
                        field_schema: Some(field_schema.clone()),
                    }),
                );

                replica_set
                    .update_local(
                        OperationWithClockTag::from(create_index_op),
                        true,
                        hw_counter.clone(),
                        false,
                    ) // TODO: Assign clock tag!? 🤔
                    .await?;
            }

            self.shards_holder.write().await.add_shard(
                shard_id,
                replica_set,
                Some(shard_key.clone()),
            )?;
        }

        Ok(())
    }

    pub async fn drop_shard_key(&self, shard_key: ShardKey) -> CollectionResult<()> {
        let state = self.state().await;

        match state.config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "Shard Key {shard_key} cannot be removed with Auto sharding method"
                )));
            }
            ShardingMethod::Custom => {}
        }

        let resharding_state = self
            .resharding_state()
            .await
            .filter(|state| state.shard_key.as_ref() == Some(&shard_key));

        if let Some(state) = resharding_state
            && let Err(err) = self.abort_resharding(state.key(), true).await
        {
            log::error!(
                "failed to abort resharding {} while deleting shard key {shard_key}: {err}",
                state.key(),
            );
        }

        // Invalidate local shard cleaning tasks
        match self
            .shards_holder
            .read()
            .await
            .get_shard_ids_by_key(&shard_key)
        {
            Ok(shard_ids) => self.invalidate_clean_local_shards(shard_ids).await,
            Err(err) => {
                log::warn!("Failed to invalidate local shard cleaning task, ignoring: {err}");
            }
        }

        self.shards_holder
            .write()
            .await
            .remove_shard_key(&shard_key)
            .await
    }

    pub async fn get_shard_ids(&self, shard_key: &ShardKey) -> CollectionResult<Vec<ShardId>> {
        self.shards_holder
            .read()
            .await
            .get_shard_key_to_ids_mapping()
            .get(shard_key)
            .map(|ids| ids.iter().cloned().collect())
            .ok_or_else(|| {
                CollectionError::bad_input(format!(
                    "Shard key {shard_key} does not exist for collection {}",
                    self.name()
                ))
            })
    }

    pub async fn get_replicas(
        &self,
        shard_key: &ShardKey,
    ) -> CollectionResult<Vec<(ShardId, PeerId)>> {
        let shard_ids = self.get_shard_ids(shard_key).await?;
        let shard_holder = self.shards_holder.read().await;
        let mut replicas = Vec::new();
        for shard_id in shard_ids {
            if let Some(replica_set) = shard_holder.get_shard(shard_id) {
                for (peer_id, _) in replica_set.peers() {
                    replicas.push((shard_id, peer_id));
                }
            }
        }
        Ok(replicas)
    }
}
