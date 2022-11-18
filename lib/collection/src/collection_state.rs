use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::collection::Collection;
use crate::config::CollectionConfig;
use crate::operations::types::CollectionResult;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::transfer::shard_transfer::ShardTransfer;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub replicas: HashMap<PeerId, ReplicaState>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct State {
    pub config: CollectionConfig,
    pub shards: HashMap<ShardId, ShardInfo>,
    #[serde(default)]
    pub transfers: HashSet<ShardTransfer>,
}

impl State {
    pub async fn apply(
        self,
        this_peer_id: PeerId,
        collection: &Collection,
        abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        Self::apply_config(self.config, collection).await?;
        Self::apply_shard_transfers(self.transfers, collection, this_peer_id, abort_transfer)
            .await?;
        Self::apply_shard_info(self.shards, collection).await?;
        Ok(())
    }

    async fn apply_shard_transfers(
        shard_transfers: HashSet<ShardTransfer>,
        collection: &Collection,
        this_peer_id: PeerId,
        mut abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        let old_transfers = collection
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
        collection
            .shards_holder
            .write()
            .await
            .shard_transfers
            .write(|transfers| *transfers = shard_transfers)?;
        Ok(())
    }

    async fn apply_config(
        new_config: CollectionConfig,
        collection: &Collection,
    ) -> CollectionResult<()> {
        log::warn!("Applying only optimizers config snapshot. Other config updates are not yet implemented.");
        collection
            .update_optimizer_params(new_config.optimizer_config)
            .await?;
        // updating replication factor
        let mut config = collection.config.write().await;
        config.params.replication_factor = new_config.params.replication_factor;
        config.params.write_consistency_factor = new_config.params.write_consistency_factor;
        Ok(())
    }

    async fn apply_shard_info(
        shards: HashMap<ShardId, ShardInfo>,
        collection: &Collection,
    ) -> CollectionResult<()> {
        for (shard_id, shard_info) in shards {
            let shards_holder = collection.shards_holder.read().await;
            match shards_holder.get_shard(&shard_id) {
                Some(replica_set) => replica_set.apply_state(shard_info.replicas).await?,
                None => {
                    // If collection exists - it means it should know about all of its shards.
                    // This holds true until shard number scaling is implemented.
                    log::warn!("Shard number scaling is not yet implemented. Failed to add shard {shard_id} to an initialized collection {}", collection.name());
                }
            }
        }
        Ok(())
    }
}
