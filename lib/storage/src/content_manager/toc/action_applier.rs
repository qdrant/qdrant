use collection::events::IndexCreatedEvent;
use common::ambient::{AmbientContext, AmbientFutureExt};

use super::TableOfContent;
use crate::content_manager::consensus_state_machine::{Action, CollectionConfigDiff};
use crate::content_manager::errors::{StorageError, StorageResult};

impl TableOfContent {
    pub(super) fn apply_action_sync(&self, action: Action) -> StorageResult<()> {
        self.general_runtime.block_on(self.apply_action(action))
    }

    async fn apply_action(&self, action: Action) -> StorageResult<()> {
        match &action {
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
                        collection.update_hnsw_config_from_diff(diff.clone()).await?;
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
                let collection_ctx =
                    AmbientContext::request(self.get_collection_hw_metrics(collection.clone()));

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
                let collection_ctx =
                    AmbientContext::request(self.get_collection_hw_metrics(collection.clone()));

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
                shard_holder.write().await.drop_and_remove_shard(*shard_id).await?;

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

            Action::CreateCollection { .. }
            | Action::DropCollection { .. }
            | Action::CreateAndRegisterShards { .. }
            | Action::InvalidateCleanLocalShards { .. }
            | Action::RemoveShardKey { .. }
            | Action::SetReplicaState { .. }
            | Action::RemoveReplica { .. }
            | Action::InitLocalShard { .. }
            | Action::RegisterTransfer { .. }
            | Action::SetTransferMethod { .. }
            | Action::DeleteMigratedPoints { .. }
            | Action::RevertHashRing { .. }
            | Action::SetReshardingState { .. }
            | Action::SetReshardingStage { .. }
            | Action::StopTransferDriver { .. }
            | Action::RevertProxyShard { .. }
            | Action::UnproxifyShard { .. }
            | Action::SpawnTransferDriver { .. }
            | Action::UnregisterTransfer { .. }
            | Action::SetPeerMetadata { .. }
            | Action::SetClusterMetadataKey { .. }
            | Action::SetQuotaConfig { .. } => Err(StorageError::service_error(format!(
                "Action applier does not support {action:?}"
            ))),

            #[cfg(feature = "staging")]
            Action::TestSlowDown(_) | Action::TestTransientError(_) => Err(
                StorageError::service_error(format!("Action applier does not support {action:?}")),
            ),
        }
    }
}
