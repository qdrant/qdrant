use std::collections::{HashMap, HashSet};

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection::Collection;
use crate::collection_state::{ShardInfo, State};
use crate::config::CollectionConfigInternal;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{ShardKeyMapping, ShardTransferChange};
use crate::shards::transfer::ShardTransfer;

impl Collection {
    pub async fn check_config_compatible(
        &self,
        config: &CollectionConfigInternal,
    ) -> CollectionResult<()> {
        self.collection_config
            .read()
            .await
            .params
            .check_compatible(&config.params)
    }

    pub async fn apply_state(
        &self,
        state: State,
        this_peer_id: PeerId,
        abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        self.apply_config(state.config).await?;
        self.apply_shard_transfers(state.transfers, this_peer_id, abort_transfer)
            .await?;
        self.apply_shard_info(state.shards, state.shards_key_mapping)
            .await?;
        self.apply_payload_index_schema(state.payload_index_schema)
            .await?;
        Ok(())
    }

    async fn apply_shard_transfers(
        &self,
        shard_transfers: HashSet<ShardTransfer>,
        this_peer_id: PeerId,
        mut abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        let old_transfers = self
            .shards_holder
            .read()
            .await
            .shard_transfers
            .read()
            .clone();
        for transfer in shard_transfers.difference(&old_transfers) {
            if transfer.from == this_peer_id {
                // Abort transfer as sender should not learn about the transfer from snapshot
                // If this happens it mean the sender is probably outdated and it is safer to abort
                abort_transfer(transfer.clone());
                // Since we remove the transfer from our list below, we don't invoke regular abort logic on this node
                // Do it here explicitly so we don't miss a silent abort change
                let _ = self
                    .shards_holder
                    .read()
                    .await
                    .shard_transfer_changes
                    .send(ShardTransferChange::Abort(transfer.key()));
            }
        }
        self.shards_holder
            .write()
            .await
            .shard_transfers
            .write(|transfers| *transfers = shard_transfers)?;
        Ok(())
    }

    async fn apply_config(&self, new_config: CollectionConfigInternal) -> CollectionResult<()> {
        let recreate_optimizers;

        {
            let mut config = self.collection_config.write().await;

            if config.uuid != new_config.uuid {
                return Err(CollectionError::service_error(format!(
                    "collection {} UUID mismatch: \
                     UUID of existing collection is different from UUID of collection in Raft snapshot: \
                     existing collection UUID: {:?}, Raft snapshot collection UUID: {:?}",
                    self.id,
                    config.uuid,
                    new_config.uuid,
                )));
            }

            if let Err(err) = config.params.check_compatible(&new_config.params) {
                // Stop consensus with a service error, if new config is incompatible with current one.
                //
                // We expect that `apply_config` is only called when configs are compatible, otherwise
                // collection have to be *recreated*.
                return Err(CollectionError::service_error(err.to_string()));
            }

            // Destructure `new_config`, to ensure we compare all config fields. Compiler would
            // complain, if new field is added to `CollectionConfig` struct, but not destructured
            // explicitly. We have to explicitly compare config fields, because we want to compare
            // `wal_config` and `strict_mode_config` independently of other fields.
            let CollectionConfigInternal {
                params,
                hnsw_config,
                optimizer_config,
                wal_config,
                quantization_config,
                strict_mode_config,
                uuid: _,
            } = &new_config;

            let is_core_config_updated = params != &config.params
                || hnsw_config != &config.hnsw_config
                || optimizer_config != &config.optimizer_config
                || quantization_config != &config.quantization_config;

            let is_wal_config_updated = wal_config != &config.wal_config;
            let is_strict_mode_config_updated = strict_mode_config != &config.strict_mode_config;

            let is_config_updated =
                is_core_config_updated || is_wal_config_updated || is_strict_mode_config_updated;

            if !is_config_updated {
                return Ok(());
            }

            if is_wal_config_updated {
                log::warn!(
                    "WAL config of collection {} updated when applying Raft snapshot, \
                     but updated WAL config will only be applied on Qdrant restart",
                    self.id,
                );
            }

            *config = new_config;

            // We need to recreate optimizers, if "core" config was updated
            recreate_optimizers = is_core_config_updated;
        }

        self.collection_config.read().await.save(&self.path)?;

        if recreate_optimizers {
            self.recreate_optimizers_blocking().await?;
        }

        Ok(())
    }

    async fn apply_shard_info(
        &self,
        shards: HashMap<ShardId, ShardInfo>,
        shards_key_mapping: ShardKeyMapping,
    ) -> CollectionResult<()> {
        let mut extra_shards: HashMap<ShardId, ShardReplicaSet> = HashMap::new();

        let shard_ids = shards.keys().copied().collect::<HashSet<_>>();

        // There are two components, where shard-related info is stored:
        // Shard objects themselves and shard_holder, that maps shard_keys to shards.

        // On the first state of the update, we update state of shards themselves
        // and create new shards if needed

        for (shard_id, shard_info) in shards {
            match self.shards_holder.read().await.get_shard(shard_id) {
                Some(replica_set) => replica_set.apply_state(shard_info.replicas).await?,
                None => {
                    let shard_key = shards_key_mapping
                        .iter()
                        .find(|(_, ids)| ids.contains(&shard_id))
                        .map(|(key, _)| key.clone());
                    let shard_replicas: Vec<_> = shard_info.replicas.keys().copied().collect();
                    let replica_set = self
                        .create_replica_set(shard_id, shard_key, &shard_replicas, None)
                        .await?;
                    replica_set.apply_state(shard_info.replicas).await?;
                    extra_shards.insert(shard_id, replica_set);
                }
            }
        }

        // On the second step, we register missing shards and remove extra shards

        self.shards_holder
            .write()
            .await
            .apply_shards_state(shard_ids, shards_key_mapping, extra_shards)
            .await
    }

    async fn apply_payload_index_schema(
        &self,
        payload_index_schema: PayloadIndexSchema,
    ) -> CollectionResult<()> {
        let state = self.state().await;

        for field_name in state.payload_index_schema.schema.keys() {
            if !payload_index_schema.schema.contains_key(field_name) {
                self.drop_payload_index(field_name.clone()).await?;
            }
        }

        for (field_name, field_schema) in payload_index_schema.schema {
            self.create_payload_index(field_name, field_schema).await?;
        }
        Ok(())
    }
}
