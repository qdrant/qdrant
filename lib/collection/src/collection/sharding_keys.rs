use std::collections::HashSet;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::ShardKey;

use crate::collection::{AbortReshardingScope, Collection};
use crate::config::ShardingMethod;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::{
    CollectionUpdateOperations, CreateIndex, FieldIndexOperations, OperationWithClockTag,
};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::{PeerId, ShardId, ShardsPlacement};
use crate::shards::shard_trait::WaitUntil;

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
            self.name().to_string(),
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

        let (sharding_method, key_mapping) = {
            let shards_holder = self.shards_holder.read().await;
            (
                shards_holder.get_sharding_method(),
                shards_holder.get_shard_key_to_ids_mapping(),
            )
        };
        match sharding_method {
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "Shard Key {shard_key} cannot be created with Auto sharding method"
                )));
            }
            ShardingMethod::Custom => {}
        }

        // Shard key mapping is updated atomically at the end of `create_shard_key`.
        // If shard key already exists, either user submitted invalid/duplicate operation,
        // or we are re-applying fully applied and persisted operation after crash.
        // Returning "bad request" error is fine in both cases.
        if key_mapping.contains_key(&shard_key) {
            return Err(CollectionError::bad_request(format!(
                "Shard key {shard_key} already exists"
            )));
        }

        if placement.is_empty() {
            return Err(CollectionError::bad_request(format!(
                "Shard key {shard_key} placement cannot be empty"
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

        let base_id = key_mapping.iter_shard_ids().max().unwrap_or(0) + 1;
        let payload_schema = self.payload_index_schema.read().schema.clone();

        // Create shards on disk *before* updating shard key mapping.
        //
        // `ShardHolder::load_shards` only loads shards that are present in the mapping,
        // so directories that are not in the mapping do not affect startup and state after crash.
        //
        // On re-apply, `Collection::create_replica_set` cleanly re-creates any leftover directories.

        let mut shards = Vec::with_capacity(placement.len());

        for (idx, replicas) in placement.iter().enumerate() {
            let shard_id = base_id + idx as ShardId;

            let replica_set = self
                .create_replica_set(
                    shard_id,
                    Some(shard_key.clone()),
                    replicas,
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
                        WaitUntil::Visible,
                        None,
                        hw_counter.clone(),
                        false,
                    )
                    .await?;
            }

            shards.push((shard_id, replica_set));
        }

        // Persist mapping and register new shards
        self.shards_holder
            .write()
            .await
            .add_shards(shards, Some(shard_key))
            .await
    }

    pub async fn drop_shard_key(&self, shard_key: ShardKey) -> CollectionResult<()> {
        let state = self.state().await;

        match state.config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Custom => {}
            ShardingMethod::Auto => {
                return Err(CollectionError::bad_request(format!(
                    "shard key {shard_key} cannot be removed with Auto sharding method"
                )));
            }
        }

        // Abort resharding and propagate abort error, so we don't leave active resharding
        // referencing deleted shard key
        let resharding_state = self
            .resharding_state()
            .await
            .filter(|state| state.shard_key.as_ref() == Some(&shard_key));

        if let Some(state) = resharding_state {
            self.abort_resharding(state.key(), true, AbortReshardingScope::default())
                .await?;
        }

        // Invalidate local shard cleanup tasks
        let shard_ids = self
            .shards_holder
            .read()
            .await
            .get_shard_ids_by_key(&shard_key);

        match shard_ids {
            Ok(shard_ids) => self.invalidate_clean_local_shards(shard_ids).await,
            Err(err) => {
                log::warn!("Failed to invalidate local shard cleanup task, ignoring: {err}");
            }
        }

        // Remove shard key
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
