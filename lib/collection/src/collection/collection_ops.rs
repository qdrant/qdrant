use std::cmp;
use std::sync::{Arc, LazyLock};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::{TryStreamExt as _, future};
use segment::types::{Payload, QuantizationConfig, StrictModeConfig};
use semver::Version;
use shard::count::CountRequestInternal;

use super::Collection;
use crate::operations::config_diff::*;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::*;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::replica_set::Change;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::PeerId;

/// Old logic for aborting shard transfers on shard drop, had a bug: it dropped all transfers
/// regardless of the shard id. In order to keep consensus consistent, we can only
/// enable new fixed logic once cluster fully switched to this version.
/// Otherwise, some node might follow old logic and some - new logic.
///
/// See: <https://github.com/qdrant/qdrant/pull/7792>
pub static ABORT_TRANSFERS_ON_SHARD_DROP_FIX_FROM_VERSION: LazyLock<Version> =
    LazyLock::new(|| Version::parse("1.16.3-dev").expect("valid version string"));

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
            config.params = config.params.update(&params_diff);
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
            config.hnsw_config = config.hnsw_config.update(&hnsw_config_diff);
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
            config.optimizer_config = config.optimizer_config.update(&optimizer_config_diff);
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

    pub async fn update_metadata(&self, metadata: Payload) -> CollectionResult<()> {
        let mut collection_config_guard: tokio::sync::RwLockWriteGuard<
            '_,
            crate::config::CollectionConfigInternal,
        > = self.collection_config.write().await;

        if let Some(current_metadata) = collection_config_guard.metadata.as_mut() {
            current_metadata.merge(&metadata);
        } else {
            collection_config_guard.metadata = Some(metadata);
        }
        drop(collection_config_guard);
        self.collection_config.read().await.save(&self.path)?;
        Ok(())
    }

    /// Updates the strict mode configuration and saves it to disk.
    pub async fn update_strict_mode_config(
        &self,
        strict_mode_diff: StrictModeConfig,
    ) -> CollectionResult<()> {
        {
            let mut config = self.collection_config.write().await;
            if let Some(current_config) = config.strict_mode_config.as_mut() {
                *current_config = current_config.update(&strict_mode_diff);
            } else {
                config.strict_mode_config = Some(strict_mode_diff);
            }
        }
        // update collection config
        self.collection_config.read().await.save(&self.path)?;
        // apply config change to all shards
        let mut shard_holder = self.shards_holder.write().await;
        let updates = shard_holder
            .all_shards_mut()
            .map(|replica_set| replica_set.on_strict_mode_config_update());
        future::try_join_all(updates).await?;
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

            let Some(replica_set) = shard_holder.get_shard(shard_id) else {
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

            // Check that we are not removing the *last* replica or the last *active* replica
            //
            // `is_last_active_replica` counts both `Active` and `ReshardingScaleDown` replicas!
            if peers.len() == 1 || replica_set.is_last_source_of_truth_replica(peer_id) {
                return Err(CollectionError::BadRequest {
                    description: format!(
                        "Shard {shard_id} must have at least one active replica after removing {peer_id}",
                    ),
                });
            }

            let all_nodes_fixed_cancellation = self
                .channel_service
                .all_peers_at_version(&ABORT_TRANSFERS_ON_SHARD_DROP_FIX_FROM_VERSION);

            // Collect shard transfers related to removed shard...
            let transfers = if all_nodes_fixed_cancellation {
                shard_holder.get_related_transfers(peer_id, shard_id)
            } else {
                // This is the old buggy logic, but we have to keep it
                // for maintaining consistency in a cluster with mixed versions.
                shard_holder
                    .get_transfers(|transfer| transfer.from == peer_id || transfer.to == peer_id)
            };

            // ...and cancel transfer tasks and remove transfers from internal state
            for transfer in transfers {
                self.abort_shard_transfer_and_resharding(transfer.key(), Some(&shard_holder))
                    .await?;
            }

            replica_set.remove_peer(peer_id).await?;

            // We can't remove the last repilca of a shard, so this should prevent removing
            // resharding shard, because it's always the *only* replica.
            //
            // And if we remove some other shard, that is currently doing resharding transfer,
            // the transfer should be cancelled (see the block right above this comment),
            // so no special handling is needed.
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
    ///
    /// ## Cancel safety
    ///
    /// This function is cancel safe, and will always run to completion.
    pub async fn recreate_optimizers_blocking(&self) -> CollectionResult<()> {
        let shards_holder = self.shards_holder.clone();
        tokio::task::spawn(async move {
            let shard_holder = shards_holder.read().await;
            let updates = shard_holder
                .all_shards()
                .map(|replica_set| replica_set.on_optimizer_config_update());
            future::try_join_all(updates).await
        })
        .await??;
        Ok(())
    }

    pub async fn strict_mode_config(&self) -> Option<StrictModeConfig> {
        self.collection_config
            .read()
            .await
            .strict_mode_config
            .clone()
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
            None => CollectionInfo::empty(
                self.collection_config.read().await.clone(),
                self.payload_index_schema.read().clone(),
            ),
        };

        while let Some(response) = requests.try_next().await? {
            let CollectionInfo {
                status,
                optimizer_status,
                warnings,
                indexed_vectors_count,
                points_count,
                segments_count,
                config: _,
                payload_schema,
                update_queue,
            } = response;
            info.status = cmp::max(info.status, status);
            info.optimizer_status = cmp::max(info.optimizer_status, optimizer_status);
            info.indexed_vectors_count = info
                .indexed_vectors_count
                .zip(indexed_vectors_count)
                .map(|(a, b)| a + b);
            info.points_count = info.points_count.zip(points_count).map(|(a, b)| a + b);
            info.segments_count += segments_count;
            info.warnings.extend(warnings);
            if let Some(queue) = &mut info.update_queue {
                queue.length += update_queue.map(|q| q.length).unwrap_or(0);
            } else {
                info.update_queue = update_queue;
            }
            for (key, response_schema) in payload_schema {
                info.payload_schema
                    .entry(key)
                    .and_modify(|info_schema| info_schema.points += response_schema.points)
                    .or_insert(response_schema);
            }
        }

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
            let peers = replica_set.peers();

            if replica_set.has_local_shard().await {
                let state = peers
                    .get(&replica_set.this_peer_id())
                    .copied()
                    .unwrap_or(ReplicaState::Dead);

                // Cluster info is explicitly excluded from hardware measurements
                // So that we can monitor hardware usage without interference
                let hw_acc = HwMeasurementAcc::disposable();
                let count_result = replica_set
                    .count_local(count_request.clone(), None, hw_acc)
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
            for (peer_id, state) in replica_set.peers() {
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
        let resharding_operations = shards_holder.get_resharding_operations_info();

        // sort by shard_id
        local_shards.sort_by_key(|k| k.shard_id);
        remote_shards.sort_by_key(|k| k.shard_id);

        let info = CollectionClusterInfo {
            peer_id,
            shard_count,
            local_shards,
            remote_shards,
            shard_transfers,
            resharding_operations,
        };
        Ok(info)
    }

    pub async fn optimizations(
        &self,
        options: OptimizationsRequestOptions,
    ) -> CollectionResult<OptimizationsResponse> {
        let shards_holder = self.shards_holder.read().await;

        let futures: Vec<_> = shards_holder
            .all_shards()
            .map(|shard| shard.optimizations(options))
            .collect();

        let shard_responses = future::try_join_all(futures).await?;

        let mut merged = OptimizationsResponse::default();
        for shard_response in shard_responses {
            merged.merge(shard_response);
        }

        // Sort from newest to oldest
        merged
            .running
            .sort_by_key(|v| cmp::Reverse(v.progress.started_at));

        if let Some(completed) = &mut merged.completed {
            completed.sort_by_key(|v| cmp::Reverse(v.progress.started_at));
            if let Some(limit) = options.completed_limit {
                completed.truncate(limit);
            }
        }

        Ok(merged)
    }

    pub async fn print_warnings(&self) {
        let warnings = self.collection_config.read().await.get_warnings();
        for warning in warnings {
            log::warn!("Collection {}: {}", self.name(), warning.message);
        }
    }
}
