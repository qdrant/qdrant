use std::cmp;
use std::sync::Arc;

use futures::{future, TryStreamExt as _};
use lazy_static::lazy_static;
use segment::types::QuantizationConfig;
use semver::Version;

use super::Collection;
use crate::operations::config_diff::*;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::*;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::replica_set::{Change, ReplicaState};
use crate::shards::shard::PeerId;

lazy_static! {
    /// When dropping a shard, only cancel all related shard transfers to and from it when all nodes
    /// are running at least this version. That way, we avoid getting an inconsistent state in
    /// consensus if some nodes are still running an older version.
    static ref ABORT_TRANSFERS_ON_SHARD_DROP_FROM_VERSION: Version = Version::parse("1.9.0-dev").unwrap();
}

impl Collection {
    /// Updates collection params:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_params_from_diff(
        &self,
        params_diff: CollectionParamsDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.params = params_diff.update(&config.params)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates HNSW config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_hnsw_config_from_diff(
        &self,
        hnsw_config_diff: HnswConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.hnsw_config = hnsw_config_diff.update(&config.hnsw_config)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates vectors config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_vectors_from_diff(
        &self,
        update_vectors_diff: &VectorsConfigDiff,
    ) -> CollectionResult<()> {
        let mut config = self.collection_config.write().await;
        update_vectors_diff.check_vector_names(&config.params)?;
        config
            .params
            .update_vectors_from_diff(update_vectors_diff)?;
        config.save(&self.path)?;
        Ok(())
    }

    /// Updates sparse vectors config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_sparse_vectors_from_other(
        &self,
        update_vectors_diff: &SparseVectorsConfig,
    ) -> CollectionResult<()> {
        let mut config = self.collection_config.write().await;
        update_vectors_diff.check_vector_names(&config.params)?;
        config
            .params
            .update_sparse_vectors_from_other(update_vectors_diff)?;
        config.save(&self.path)?;
        Ok(())
    }

    /// Updates shard optimization params:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_optimizer_params_from_diff(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.optimizer_config =
                DiffConfig::update(optimizer_config_diff, &config.optimizer_config)?;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates shard optimization params: Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_optimizer_params(
        &self,
        optimizer_config: OptimizersConfig,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            config.optimizer_config = optimizer_config;
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates quantization config:
    /// Saves new params on disk
    ///
    /// After this, `recreate_optimizers_blocking` must be called to create new optimizers using
    /// the updated configuration.
    pub async fn update_quantization_config_from_diff(
        &self,
        quantization_config_diff: QuantizationConfigDiff,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            match quantization_config_diff {
                QuantizationConfigDiff::Scalar(scalar) => {
                    config
                        .quantization_config
                        .replace(QuantizationConfig::Scalar(scalar));
                }
                QuantizationConfigDiff::Product(product) => {
                    config
                        .quantization_config
                        .replace(QuantizationConfig::Product(product));
                }
                QuantizationConfigDiff::Binary(binary) => {
                    config
                        .quantization_config
                        .replace(QuantizationConfig::Binary(binary));
                }
                QuantizationConfigDiff::Disabled(_) => {
                    config.quantization_config = None;
                }
            }
        }
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Handle replica changes
    ///
    /// add and remove replicas from replica set
    pub async fn handle_replica_changes(
        &self,
        replica_changes: Vec<Change>,
    ) -> CollectionResult<()> {
        if replica_changes.is_empty() {
            return Ok(());
        }

        let shard_holder = self.shards_holder.read().await;

        for change in replica_changes {
            let (shard_id, peer_id) = match change {
                Change::Remove(shard_id, peer_id) => (shard_id, peer_id),
            };

            let Some(replica_set) = shard_holder.get_shard(&shard_id) else {
                return Err(CollectionError::BadRequest {
                    description: format!("Shard {} of {} not found", shard_id, self.name()),
                });
            };

            let peers = replica_set.peers();

            if !peers.contains_key(&peer_id) {
                return Err(CollectionError::BadRequest {
                    description: format!("Peer {peer_id} has no replica of shard {shard_id}"),
                });
            }

            if peers.len() == 1 {
                return Err(CollectionError::BadRequest {
                    description: format!("Shard {shard_id} must have at least one replica"),
                });
            }

            replica_set.remove_peer(peer_id).await?;

            let all_nodes_cancel_transfers = self
                .channel_service
                .all_peers_at_version(ABORT_TRANSFERS_ON_SHARD_DROP_FROM_VERSION.clone());
            if all_nodes_cancel_transfers {
                // Collect shard transfers related to removed shard...
                let transfers = shard_holder
                    .get_transfers(|transfer| transfer.from == peer_id || transfer.to == peer_id);

                // ...and cancel transfer tasks and remove transfers from internal state
                for transfer in transfers {
                    self.finish_shard_transfer(transfer, Some(&shard_holder))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Recreate the optimizers on all shards for this collection
    ///
    /// This will stop existing optimizers, and start new ones with new configurations.
    ///
    /// # Blocking
    ///
    /// Partially blocking. Stopping existing optimizers is blocking. Starting new optimizers is
    /// not blocking.
    pub async fn recreate_optimizers_blocking(&self) -> CollectionResult<()> {
        let effective_config = self.effective_optimizers_config().await?;
        let shard_holder = self.shards_holder.read().await;
        let updates = shard_holder
            .all_shards()
            .map(|replica_set| replica_set.on_optimizer_config_update(effective_config.clone()));
        future::try_join_all(updates).await?;
        Ok(())
    }

    pub async fn info(
        &self,
        shard_selection: &ShardSelectorInternal,
    ) -> CollectionResult<CollectionInfo> {
        let shards_holder = self.shards_holder.read().await;
        let shards = shards_holder.select_shards(shard_selection)?;
        let mut requests: futures::stream::FuturesUnordered<_> = shards
            .into_iter()
            // `info` requests received through internal gRPC *always* have `shard_selection`
            .map(|(shard, _shard_key)| shard.info(shard_selection.is_shard_id()))
            .collect();

        let mut info = match requests.try_next().await? {
            Some(info) => info,
            None => CollectionInfo::empty(self.collection_config.read().await.clone()),
        };

        while let Some(response) = requests.try_next().await? {
            info.status = cmp::max(info.status, response.status);
            info.optimizer_status = cmp::max(info.optimizer_status, response.optimizer_status);
            info.vectors_count = info
                .vectors_count
                .zip(response.vectors_count)
                .map(|(a, b)| a + b);
            info.indexed_vectors_count = info
                .indexed_vectors_count
                .zip(response.indexed_vectors_count)
                .map(|(a, b)| a + b);
            info.points_count = info
                .points_count
                .zip(response.points_count)
                .map(|(a, b)| a + b);
            info.segments_count += response.segments_count;

            for (key, response_schema) in response.payload_schema {
                info.payload_schema
                    .entry(key)
                    .and_modify(|info_schema| info_schema.points += response_schema.points)
                    .or_insert(response_schema);
            }
        }

        // Do not display vectors count, as it is an approximate number
        // and many users are confused by its behavior
        info.vectors_count = None;

        Ok(info)
    }

    pub async fn cluster_info(&self, peer_id: PeerId) -> CollectionResult<CollectionClusterInfo> {
        let shards_holder = self.shards_holder.read().await;
        let shard_count = shards_holder.len();
        let mut local_shards = Vec::new();
        let mut remote_shards = Vec::new();
        let count_request = Arc::new(CountRequestInternal {
            filter: None,
            exact: false, // Don't need exact count of unique ids here, only size estimation
        });
        let shard_to_key = shards_holder.get_shard_id_to_key_mapping();

        // extract shards info
        for (shard_id, replica_set) in shards_holder.get_shards() {
            let shard_id = *shard_id;
            let peers = replica_set.peers();

            if replica_set.has_local_shard().await {
                let state = peers
                    .get(&replica_set.this_peer_id())
                    .copied()
                    .unwrap_or(ReplicaState::Dead);
                let count_result = replica_set
                    .count_local(count_request.clone())
                    .await
                    .unwrap_or_default();
                let points_count = count_result.map(|x| x.count).unwrap_or(0);
                local_shards.push(LocalShardInfo {
                    shard_id,
                    points_count,
                    state,
                    shard_key: shard_to_key.get(&shard_id).cloned(),
                })
            }
            for (peer_id, state) in replica_set.peers().into_iter() {
                if peer_id == replica_set.this_peer_id() {
                    continue;
                }
                remote_shards.push(RemoteShardInfo {
                    shard_id,
                    peer_id,
                    state,
                    shard_key: shard_to_key.get(&shard_id).cloned(),
                });
            }
        }
        let shard_transfers =
            shards_holder.get_shard_transfer_info(&*self.transfer_tasks.lock().await);

        // sort by shard_id
        local_shards.sort_by_key(|k| k.shard_id);
        remote_shards.sort_by_key(|k| k.shard_id);

        let info = CollectionClusterInfo {
            peer_id,
            shard_count,
            local_shards,
            remote_shards,
            shard_transfers,
        };
        Ok(info)
    }
}
