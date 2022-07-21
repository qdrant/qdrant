use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::collection::Collection;
use crate::config::CollectionConfig;
use crate::operations::types::CollectionResult;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::{create_shard_dir, ChannelService, PeerId, Shard, ShardId};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct State {
    pub config: CollectionConfig,
    pub shard_to_peer: HashMap<ShardId, PeerId>,
}

impl State {
    pub async fn apply(
        self,
        this_peer_id: PeerId,
        collection: &Collection,
        collection_path: &Path,
        channel_service: ChannelService,
    ) -> CollectionResult<()> {
        Self::apply_config(self.config, collection).await?;
        Self::apply_shard_to_peer(
            self.shard_to_peer,
            this_peer_id,
            collection,
            collection_path,
            channel_service,
        )
        .await
    }

    async fn apply_config(
        config: CollectionConfig,
        collection: &Collection,
    ) -> CollectionResult<()> {
        log::warn!("Applying only optimizers config snapshot. Other config updates are not yet implemented.");
        collection
            .update_optimizer_params(config.optimizer_config)
            .await
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
                    if shard.peer_id(this_peer_id) != peer_id {
                        // shard registered on a different peer
                        log::warn!("Shard movement between peers is not yet implemented. Failed to move shard {shard_id} to peer {peer_id}")
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
