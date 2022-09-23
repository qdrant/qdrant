use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::collection::Collection;
use crate::config::CollectionConfig;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shard::replica_set::IsActive;
use crate::shard::{PeerId, Shard, ShardId, ShardTransfer};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum ShardInfo {
    ReplicaSet { replicas: HashMap<PeerId, IsActive> },
    Single(PeerId),
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
        Self::apply_shard_info(self.shards, this_peer_id, collection).await?;
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
        let old_repl_factor = config.params.replication_factor;
        config.params.replication_factor = new_config.params.replication_factor;
        collection.handle_repl_factor_change(old_repl_factor, config.params.replication_factor);
        Ok(())
    }

    async fn apply_shard_info(
        shards: HashMap<ShardId, ShardInfo>,
        this_peer_id: PeerId,
        collection: &Collection,
    ) -> CollectionResult<()> {
        for (shard_id, shard_info) in shards {
            let mut shards_holder = collection.shards_holder.write().await;
            match (shards_holder.get_mut_shard(&shard_id), shard_info) {
                (Some(shard), ShardInfo::Single(peer_id)) => {
                    let old_peer_id = match &shard.peer_ids(this_peer_id)[..] {
                        [id] => *id,
                        _ => return Err(CollectionError::ServiceError { error: format!("Shard {shard_id} should have only 1 peer id as it is not a replica set") }),
                    };
                    // shard was moved to a different peer
                    if old_peer_id != peer_id {
                        collection
                            .finish_shard_transfer(ShardTransfer {
                                shard_id,
                                from: old_peer_id,
                                to: peer_id,
                            })
                            .await?;
                    }
                }
                (Some(shard), ShardInfo::ReplicaSet { replicas }) => {
                    if let Shard::ReplicaSet(replica_set) = shard {
                        replica_set.apply_state(replicas).await?;
                    } else {
                        todo!("check if replication factor was increased and upgrade shard to replica set")
                    }
                }
                (None, _) => {
                    // If collection exists - it means it should know about all of its shards.
                    // This holds true until shard number scaling is implemented.
                    log::warn!("Shard number scaling is not yet implemented. Failed to add shard {shard_id} to an initialized collection {}", collection.name());
                }
            }
        }
        Ok(())
    }
}
