use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::collection::Collection;
use crate::config::CollectionConfig;
use crate::operations::types::CollectionResult;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::{create_shard_dir, ChannelService, PeerId, Shard, ShardId, ShardTransfer};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct State {
    pub config: CollectionConfig,
    pub shard_to_peer: HashMap<ShardId, PeerId>,
    #[serde(default)]
    pub transfers: HashSet<ShardTransfer>,
}

impl State {
    pub async fn apply(
        self,
        this_peer_id: PeerId,
        collection: &Collection,
        collection_path: &Path,
        channel_service: ChannelService,
        abort_transfer: impl FnMut(ShardTransfer),
    ) -> CollectionResult<()> {
        Self::apply_config(self.config, collection).await?;
        Self::apply_shard_transfers(self.transfers, collection, this_peer_id, abort_transfer)
            .await?;
        Self::apply_shard_to_peer(
            self.shard_to_peer,
            this_peer_id,
            collection,
            collection_path,
            channel_service,
        )
        .await?;
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

    async fn apply_shard_to_peer(
        shard_to_peer: HashMap<ShardId, PeerId>,
        this_peer_id: PeerId,
        collection: &Collection,
        collection_path: &Path,
        channel_service: ChannelService,
    ) -> CollectionResult<()> {
        for (shard_id, peer_id) in shard_to_peer {
            let mut shards_holder = collection.shards_holder.write().await;
            match shards_holder.get_shard(&shard_id) {
                Some(shard) => {
                    let old_peer_id = shard.peer_id(this_peer_id);
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
                None => {
                    if peer_id == this_peer_id {
                        // missing local shard
                        log::warn!("Shard addition is not yet implemented. Failed to add local shard {shard_id}");
                    } else {
                        // missing remote shard
                        let collection_id = collection.id.clone();
                        let shard_path = create_shard_dir(collection_path, shard_id).await?;
                        let shard = RemoteShard::init(
                            shard_id,
                            collection_id,
                            peer_id,
                            shard_path,
                            channel_service.clone(),
                        )?;
                        shards_holder.add_shard(shard_id, Shard::Remote(shard));
                    }
                }
            }
        }
        Ok(())
    }
}
