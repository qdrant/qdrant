use std::collections::{HashMap, HashSet};

use crate::collection::Collection;
use crate::collection_state::{ShardInfo, State};
use crate::config::CollectionConfig;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ShardKeyMapping;
use crate::shards::transfer::shard_transfer::ShardTransfer;

impl Collection {
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
                abort_transfer(transfer.clone())
            }
        }
        self.shards_holder
            .write()
            .await
            .shard_transfers
            .write(|transfers| *transfers = shard_transfers)?;
        Ok(())
    }

    async fn apply_config(&self, new_config: CollectionConfig) -> CollectionResult<()> {
        log::warn!("Applying only optimizers config snapshot. Other config updates are not yet implemented.");
        self.update_optimizer_params(new_config.optimizer_config)
            .await?;

        // Update replication factor
        {
            let mut config = self.collection_config.write().await;
            config.params.replication_factor = new_config.params.replication_factor;
            config.params.write_consistency_factor = new_config.params.write_consistency_factor;
        }

        self.recreate_optimizers_blocking().await?;

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
            match self.shards_holder.read().await.get_shard(&shard_id) {
                Some(replica_set) => replica_set.apply_state(shard_info.replicas).await?,
                None => {
                    let shard_replicas: Vec<_> = shard_info.replicas.keys().copied().collect();
                    let replica_set = self.create_replica_set(shard_id, &shard_replicas).await?;
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
}
