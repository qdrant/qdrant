use std::sync::Arc;

use collection::collection::Collection;
use collection::collection_state;
use collection::events::IndexCreatedEvent;
use collection::shards::collection_shard_distribution::CollectionShardDistribution;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::transfer::driver::revert_proxy_shard_to_local;
use common::ambient::{AmbientContext, AmbientFutureExt};

use super::TableOfContent;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::{
    Action, CollectionConfigDiff, LocalShardInitMode, TransferOutcome,
};
use crate::content_manager::errors::{StorageError, StorageResult};

impl TableOfContent {
    pub(super) fn apply_action_sync(&self, action: Action) -> StorageResult<()> {
        self.general_runtime
            .block_on(self.apply_action(&action))
            .map_err(|error| {
                if let StorageError::ServiceError { .. } = error {
                    error
                } else {
                    StorageError::service_error(format!("Failed to apply {action:?}: {error}"))
                }
            })
    }

    async fn apply_action(&self, action: &Action) -> StorageResult<()> {
        match action {
            Action::CreateCollection { collection, state } => {
                self.create_collection_action(collection, state).await
            }

            Action::DropCollection { collection } => {
                self.delete_collection(collection).await?;
                Ok(())
            }

            Action::UpdateCollectionConfig { collection, diff } => {
                let collection = self.get_collection_unchecked(collection).await?;

                let recreate_optimizers = match diff.as_ref() {
                    CollectionConfigDiff::Optimizers(diff) => {
                        collection
                            .update_optimizer_params_from_diff(diff.clone())
                            .await?;
                        true
                    }
                    CollectionConfigDiff::Params(diff) => {
                        collection.update_params_from_diff(diff.clone()).await?;
                        true
                    }
                    CollectionConfigDiff::Hnsw(diff) => {
                        collection.update_hnsw_config_from_diff(*diff).await?;
                        true
                    }
                    CollectionConfigDiff::Vectors(diff) => {
                        collection.update_vectors_from_diff(diff).await?;
                        true
                    }
                    CollectionConfigDiff::Quantization(diff) => {
                        collection
                            .update_quantization_config_from_diff(diff.clone())
                            .await?;
                        true
                    }
                    CollectionConfigDiff::SparseVectors(diff) => {
                        collection.update_sparse_vectors_from_other(diff).await?;
                        true
                    }
                    CollectionConfigDiff::StrictMode(diff) => {
                        collection.update_strict_mode_config(diff.clone()).await?;
                        false
                    }
                    CollectionConfigDiff::Metadata(metadata) => {
                        collection.update_metadata(metadata.clone()).await?;
                        false
                    }
                };

                collection.print_warnings().await;
                if recreate_optimizers {
                    collection.recreate_optimizers_background();
                }

                Ok(())
            }

            Action::AddNamedVector {
                collection,
                vector_name,
                config,
            } => {
                let collection_ctx = AmbientContext::request(
                    collection,
                    self.get_collection_hw_metrics(collection.clone()),
                );

                self.get_collection_unchecked(collection)
                    .await?
                    .create_named_vector(vector_name.clone(), (**config).clone())
                    .measured(collection_ctx)
                    .await?;

                Ok(())
            }

            Action::DropNamedVector {
                collection,
                vector_name,
            } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .delete_named_vector(vector_name.clone())
                    .await?;

                Ok(())
            }

            Action::SetPayloadIndex {
                collection,
                field_name,
                field_schema,
            } => {
                let collection_ctx = AmbientContext::request(
                    collection,
                    self.get_collection_hw_metrics(collection.clone()),
                );

                self.get_collection_unchecked(collection)
                    .await?
                    .create_payload_index(field_name.clone(), field_schema.clone())
                    .measured(collection_ctx)
                    .await?;

                issues::publish(IndexCreatedEvent {
                    collection_id: collection.clone(),
                    field_name: field_name.clone(),
                });

                Ok(())
            }

            Action::DropPayloadIndex {
                collection,
                field_name,
            } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .drop_payload_index(field_name.clone())
                    .await?;

                Ok(())
            }

            Action::RemoveShardKey {
                collection,
                shard_key,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .write()
                    .await
                    .remove_shard_key_mapping(shard_key)?;

                Ok(())
            }

            Action::RemoveShardFromKeyMapping {
                collection,
                shard_id,
                shard_key,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .write()
                    .await
                    .remove_shard_from_key_mapping(*shard_id, shard_key)?;

                Ok(())
            }

            Action::DropShard {
                collection,
                shard_id,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .write()
                    .await
                    .drop_and_remove_shard(*shard_id)
                    .await?;

                Ok(())
            }

            Action::SetShardNumber {
                collection,
                shard_number,
            } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .set_shard_number(*shard_number)
                    .await?;

                Ok(())
            }

            Action::SetReplicaState {
                collection,
                shard_id,
                peer_id,
                state,
            } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .apply_replica_state(*shard_id, *peer_id, *state)
                    .await?;

                Ok(())
            }

            Action::InvalidateCleanLocalShards {
                collection,
                shard_ids,
            } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .invalidate_clean_local_shards(shard_ids.iter().copied())
                    .await;

                Ok(())
            }

            Action::RemoveReplica {
                collection,
                shard_id,
                peer_id,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                let replica_set = shard_holder.read().await.get_shard(*shard_id).cloned();
                let replica_set = replica_set.ok_or_else(|| {
                    StorageError::service_error(format!("Shard {shard_id} does not exist"))
                })?;

                replica_set.remove_peer(*peer_id).await?;

                Ok(())
            }

            Action::InitLocalShard {
                collection,
                shard_id,
                mode,
            } => {
                let collection_path = self.get_collection_path(collection);
                let collection = self.get_collection_unchecked(collection).await?;

                match mode {
                    LocalShardInitMode::EnsureExists => {
                        collection.ensure_local_shard(*shard_id).await?;
                    }

                    LocalShardInitMode::ResetToEmpty => {
                        let shard_holder = collection.shards_holder();
                        let shard_holder = shard_holder.read().await;
                        let replica_set = shard_holder.get_shard(*shard_id).ok_or_else(|| {
                            StorageError::service_error(format!("Shard {shard_id} does not exist"))
                        })?;

                        replica_set.init_empty_local_shard(&collection_path).await?;
                    }
                }

                Ok(())
            }

            Action::SpawnTransferDriver {
                collection,
                transfer,
            } => {
                let collection_id = collection.clone();
                let collection = self.get_collection_unchecked(collection).await?;
                let proposal_sender = self.get_consensus_proposal_sender()?.clone();
                let consensus = self.toc_dispatcher.lock().clone().ok_or_else(|| {
                    StorageError::service_error(
                        "Can't spawn transfer driver without a consensus dispatcher",
                    )
                })?;

                if transfer.method.is_none() {
                    return Err(StorageError::service_error(
                        "Can't spawn transfer driver without a transfer method",
                    ));
                }

                let temp_dir = self.optional_temp_or_storage_temp_path()?;
                let on_finish = {
                    let collection_id = collection_id.clone();
                    let transfer = transfer.clone();
                    let proposal_sender = proposal_sender.clone();
                    async move {
                        let operation =
                            ConsensusOperations::finish_transfer(collection_id, transfer);
                        if let Err(error) = proposal_sender.send(operation) {
                            log::error!("Can't report transfer progress to consensus: {error}");
                        }
                    }
                };
                let on_error = {
                    let transfer = transfer.clone();
                    async move {
                        let operation = ConsensusOperations::abort_transfer(
                            collection_id,
                            transfer,
                            "transmission failed",
                        );
                        if let Err(error) = proposal_sender.send(operation) {
                            log::error!("Can't report transfer progress to consensus: {error}");
                        }
                    }
                };

                collection
                    .send_shard(
                        transfer.clone(),
                        Box::new(consensus),
                        temp_dir,
                        on_finish,
                        on_error,
                    )
                    .await;

                Ok(())
            }

            Action::StopTransferDriver { collection, key } => {
                self.get_collection_unchecked(collection)
                    .await?
                    .stop_shard_transfer(key)
                    .await;

                Ok(())
            }

            Action::RevertProxyShard {
                collection,
                shard_id,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                let shard_holder = shard_holder.read().await;

                // Resharding abort can drop the shard before reverting its related transfers
                revert_proxy_shard_to_local(&shard_holder, *shard_id).await?;

                Ok(())
            }

            Action::UnproxifyShard {
                collection,
                shard_id,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                let replica_set = shard_holder.read().await.get_shard(*shard_id).cloned();
                let replica_set = replica_set.ok_or_else(|| {
                    StorageError::service_error(format!("Shard {shard_id} does not exist"))
                })?;

                replica_set.un_proxify_local().await?;

                Ok(())
            }

            Action::SetReshardingState { collection, state } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .write()
                    .await
                    .set_resharding_state(state.clone())?;

                Ok(())
            }

            Action::SetReshardingStage { collection, stage } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder.write().await.set_resharding_stage(*stage)?;

                Ok(())
            }

            Action::DeleteMigratedPoints { collection, key } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .read()
                    .await
                    .scale_down_cleanup_points(key)
                    .await?;

                Ok(())
            }

            Action::RevertHashRing { collection, key } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder.write().await.revert_resharding_hashring(key);

                Ok(())
            }

            Action::RegisterTransfer {
                collection,
                transfer,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                shard_holder
                    .read()
                    .await
                    .register_start_shard_transfer(transfer.clone())?;

                Ok(())
            }

            Action::SetTransferMethod {
                collection,
                key,
                method,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                let transfer = shard_holder
                    .read()
                    .await
                    .register_restart_transfer(key, *method)?;

                // Planning accepted an existing transfer, so a missing record is an apply failure
                transfer.ok_or_else(|| {
                    StorageError::service_error(format!("Transfer {key:?} does not exist"))
                })?;

                Ok(())
            }

            Action::UnregisterTransfer {
                collection,
                key,
                outcome,
            } => {
                let collection = self.get_collection_unchecked(collection).await?;
                let shard_holder = collection.shards_holder();
                let shard_holder = shard_holder.read().await;

                match outcome {
                    TransferOutcome::Finish => shard_holder.register_finish_transfer(key)?,
                    TransferOutcome::Abort => shard_holder.register_abort_transfer(key)?,
                };

                Ok(())
            }

            Action::UpdateAliases { set, remove } => {
                // Keep searches from observing a mapping while it is being replaced.
                let _collections = self.collections.write().await;
                let mut persistence = self.alias_persistence.write().await;
                let mut aliases = persistence.state().clone();

                for alias in remove {
                    aliases.remove(alias);
                }
                for (alias, collection) in set {
                    aliases.insert(alias.clone(), collection.clone());
                }

                persistence.apply_state(aliases)
            }

            Action::AddPeer { .. }
            | Action::RemovePeer { .. }
            | Action::CreateAndRegisterShards { .. }
            | Action::SetPeerMetadata { .. }
            | Action::SetClusterMetadataKey { .. }
            | Action::SetQuotaConfig { .. } => Err(StorageError::service_error(format!(
                "Action applier does not support {action:?}"
            ))),

            #[cfg(feature = "staging")]
            Action::TestSlowDown(operation) => {
                operation.execute(self.this_peer_id).await;
                Ok(())
            }

            #[cfg(feature = "staging")]
            Action::TestTransientError(operation) => operation.execute(self.this_peer_id),
        }
    }

    async fn create_collection_action(
        &self,
        name: &str,
        state: &collection_state::State,
    ) -> StorageResult<()> {
        let guard = self.collection_create_lock.lock().await;
        let existing = self.collections.read().await.get(name).cloned();

        let collection = if let Some(collection) = existing {
            if collection.uuid().await != state.config.uuid {
                return Err(StorageError::service_error(format!(
                    "Collection {name} already exists with a different UUID",
                )));
            }

            collection
        } else {
            let path = self.create_collection_path(name).await?;
            let snapshots_path = self.snapshots_path_for_collection(name);
            let distribution = CollectionShardDistribution::from_shards_info(state.shards.clone());
            let collection = Collection::new(
                name.to_string(),
                self.this_peer_id,
                &path,
                &snapshots_path,
                &state.config,
                self.storage_config
                    .to_shared_storage_config(self.is_distributed())
                    .into(),
                distribution,
                None,
                self.channel_service.clone(),
                Self::change_peer_from_state_callback(
                    self.consensus_proposal_sender.clone(),
                    name.to_string(),
                    ReplicaState::Dead,
                ),
                Self::request_shard_transfer_callback(
                    self.consensus_proposal_sender.clone(),
                    name.to_string(),
                ),
                Self::abort_shard_transfer_callback(
                    self.consensus_proposal_sender.clone(),
                    name.to_string(),
                ),
                Some(self.adaptive_search_handle.clone()),
                Some(self.update_runtime.handle().clone()),
                self.optimizer_resource_budget.clone(),
                self.storage_config.optimizers_overwrite.clone(),
            )
            .await?;
            collection.print_warnings().await;
            let collection = Arc::new(collection);
            self.collections
                .write()
                .await
                .insert(name.to_string(), collection.clone());
            self.telemetry.init_snapshot_telemetry(name);
            collection
        };

        let local_shards = collection.get_local_shards().await;
        drop(guard);

        // Creation is durable before initialization is proposed. Replay must send the
        // proposals again without rebuilding shards that may already contain points.
        for shard_id in local_shards {
            self.on_peer_created(name.to_string(), self.this_peer_id, shard_id)
                .await?;
        }

        Ok(())
    }
}
