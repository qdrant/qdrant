#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use ahash::AHashMap;
use common::ambient::AmbientFutureExt;
use common::reason::reason;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::types::{VectorNameBuf, VectorStorageDatatype};
use shard::operations::{
    CollectionUpdateOperations, CreateVectorName, DeleteVectorName, VectorNameOperations,
};

use crate::collection::Collection;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection_state::{ShardInfo, State};
use crate::config::CollectionConfigInternal;
use crate::operations::OperationWithClockTag;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::resharding::ReshardState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ShardTransferChange;
use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;
use crate::shards::shard_trait::WaitUntil;
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
        let State {
            config,
            shards,
            resharding,
            transfers,
            shards_key_mapping,
            payload_index_schema,
        } = state;

        // Apply config first so that any replica created by apply_shard_info uses the target schema.
        let new_config = config.clone();
        self.apply_config(config).await?;
        self.apply_shard_transfers(transfers, this_peer_id, abort_transfer)
            .await?;
        self.apply_reshard_state(resharding).await?;
        self.apply_shard_info(shards, shards_key_mapping).await?;
        self.apply_payload_index_schema(payload_index_schema)
            .await?;
        // Reconcile named vectors at the segment level to match the new config.
        // This inspects each local shard's actual segments so retries after a partial
        // failure or restart repair any remaining schema divergence.
        self.apply_vector_name_schema(&new_config).await?;
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
        for transfer in old_transfers.difference(&shard_transfers) {
            // This transfer finished or was aborted in consensus while this peer was too far
            // behind to receive the corresponding log entries, so the regular finish/abort
            // handlers never ran here. Clean up leftover transfer state explicitly. Otherwise, if
            // this peer is the transfer source, its local shard stays proxified forever and keeps
            // forwarding updates to a peer that may not have the shard anymore — which fails
            // snapshot application itself (on payload index updates) and prevents this peer from
            // ever catching up with consensus.
            log::debug!("Cleaning up stale shard transfer: {transfer:?}");

            self.transfer_tasks
                .lock()
                .await
                .stop_task(&transfer.key())
                .await;

            if transfer.from == this_peer_id {
                let shard_holder = self.shards_holder.read().await;
                if let Some(replica_set) = shard_holder.get_shard(transfer.shard_id) {
                    // Discard proxy state instead of flushing it to the remote: replica states in
                    // the same snapshot already reflect the transfer outcome, and the target may
                    // not have the shard anymore
                    replica_set.discard_proxy_local().await;
                }
            }

            // Notify any tasks waiting for this transfer to end
            let _ = self
                .shards_holder
                .read()
                .await
                .shard_transfer_changes
                .send(ShardTransferChange::Abort(transfer.key()));
        }
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

    async fn apply_reshard_state(&self, resharding: Option<ReshardState>) -> CollectionResult<()> {
        // We don't have to explicitly abort resharding or bump shard replica states, because:
        // - peers are not driving resharding themselves
        // - ongoing (resharding) shard transfers are explicitly updated
        // - shard replica set states are explicitly updated
        self.shards_holder
            .write()
            .await
            .resharding_state
            .write(|state| *state = resharding)?;
        Ok(())
    }

    async fn apply_config(&self, mut new_config: CollectionConfigInternal) -> CollectionResult<()> {
        let recreate_optimizers;

        {
            let mut config = self.collection_config.write().await;

            // A missing UUID on either side belongs to a collection created outside of consensus,
            // so take whichever UUID is known
            let is_uuid_assigned = match (config.uuid, new_config.uuid) {
                (Some(existing), Some(snapshot)) => {
                    if existing != snapshot {
                        return Err(CollectionError::service_error(format!(
                            "collection {} UUID mismatch: \
                             UUID of existing collection is different from UUID of collection in Raft snapshot: \
                             existing collection UUID: {existing}, Raft snapshot collection UUID: {snapshot}",
                            self.id,
                        )));
                    }
                    false
                }
                (Some(existing), None) => {
                    new_config.uuid = Some(existing);
                    false
                }
                (None, Some(_)) => true,
                (None, None) => false,
            };

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
                metadata,
                created_at: _,
            } = &new_config;

            let is_core_config_updated = params != &config.params
                || hnsw_config != &config.hnsw_config
                || optimizer_config != &config.optimizer_config
                || quantization_config != &config.quantization_config;

            let is_metadata_updated = metadata != &config.metadata;

            let is_wal_config_updated = wal_config != &config.wal_config;
            let is_strict_mode_config_updated = strict_mode_config != &config.strict_mode_config;

            let is_config_updated = is_uuid_assigned
                || is_core_config_updated
                || is_wal_config_updated
                || is_strict_mode_config_updated
                || is_metadata_updated;

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

            // Save before updating config in memory. If saving fails, the in-memory config
            // must stay unchanged, so retrying the same state applies and saves it again,
            // instead of seeing no changes.
            new_config.save(&self.path)?;
            *config = new_config;

            // We need to recreate optimizers, if "core" config was updated
            recreate_optimizers = is_core_config_updated;
        }

        self.print_warnings().await;

        // Recreate optimizers in the background: this path is reached from consensus (Raft snapshot
        // application), and stopping the existing optimizers can take a long time, which would
        // otherwise stall the consensus loop.
        if recreate_optimizers {
            self.recreate_optimizers_background();
        }

        Ok(())
    }

    async fn apply_shard_info(
        &self,
        shards: AHashMap<ShardId, ShardInfo>,
        shards_key_mapping: ShardKeyMapping,
    ) -> CollectionResult<()> {
        let mut extra_shards: AHashMap<ShardId, ShardReplicaSet> = AHashMap::new();

        let shard_ids = shards.keys().copied().collect::<HashSet<_>>();

        // There are two components, where shard-related info is stored:
        // Shard objects themselves and shard_holder, that maps shard_keys to shards.

        // On the first state of the update, we update state of shards themselves
        // and create new shards if needed

        let mut shards_holder = self.shards_holder.write().await;

        for (shard_id, shard_info) in shards {
            let shard_key = shards_key_mapping.shard_key(shard_id);
            match shards_holder.get_shard(shard_id) {
                Some(replica_set) => {
                    replica_set
                        .apply_state(shard_info.replicas, shard_key)
                        .await?;
                }
                None => {
                    let mut shard_replicas: Vec<_> = shard_info.replicas.keys().copied().collect();
                    shard_replicas.sort_unstable();
                    let replica_set = self
                        .create_replica_set(shard_id, shard_key.clone(), &shard_replicas, None)
                        .await?;
                    replica_set
                        .apply_state(shard_info.replicas, shard_key)
                        .await?;
                    extra_shards.insert(shard_id, replica_set);
                }
            }
        }

        // On the second step, we register missing shards and remove extra shards
        shards_holder
            .apply_shards_state(shard_ids, shards_key_mapping, extra_shards)
            .await
    }

    async fn apply_payload_index_schema(
        &self,
        payload_index_schema: PayloadIndexSchema,
    ) -> CollectionResult<()> {
        // Collect keys first, `drop_payload_index` takes the schema write lock
        let existing_fields: Vec<_> = self
            .payload_index_schema
            .read()
            .schema
            .keys()
            .cloned()
            .collect();

        for field_name in existing_fields {
            if !payload_index_schema.schema.contains_key(&field_name) {
                self.drop_payload_index(field_name).await?;
            }
        }

        for (field_name, field_schema) in payload_index_schema.schema {
            self.create_payload_index(field_name, field_schema)
                .unmeasured(reason("This function is only used in collection state recovery and thus an unmeasured internal operation."))
                .await?;
        }
        Ok(())
    }

    /// Reconcile named vectors at the segment level to match the given config.
    ///
    /// This is called during state application (Raft snapshot recovery) to ensure
    /// that segments have the correct vector storages, even if the node missed the
    /// original create/delete vector name operations or is retrying after a partial
    /// reconciliation failure.
    async fn apply_vector_name_schema(
        &self,
        target_config: &CollectionConfigInternal,
    ) -> CollectionResult<()> {
        let target_dense: BTreeMap<VectorNameBuf, DenseVectorConfig> = target_config
            .params
            .vectors
            .params_iter()
            .map(|(vector_name, target_params)| {
                (
                    vector_name.to_owned(),
                    DenseVectorConfig {
                        size: target_params.size.get() as usize,
                        distance: target_params.distance,
                        multivector_config: target_params.multivector_config,
                        datatype: target_params.datatype.map(VectorStorageDatatype::from),
                    },
                )
            })
            .collect();

        let target_sparse: BTreeMap<VectorNameBuf, SparseVectorConfig> = target_config
            .params
            .sparse_vectors
            .as_ref()
            .map(|sparse_vectors| {
                sparse_vectors
                    .iter()
                    .map(|(vector_name, target_params)| {
                        (
                            vector_name.clone(),
                            SparseVectorConfig {
                                modifier: target_params.modifier,
                                datatype: target_params
                                    .index
                                    .as_ref()
                                    .and_then(|idx| idx.datatype)
                                    .map(VectorStorageDatatype::from),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let shards: Vec<Arc<ShardReplicaSet>> = self
            .shards_holder
            .read()
            .await
            .all_shards()
            .map(Arc::clone)
            .collect();

        let mut shard_plans = Vec::new();
        for shard in shards {
            let Some(segments) = shard.local_segments().await else {
                continue;
            };

            let (to_delete, to_create) = {
                let segments = segments.read();
                let mut to_delete: BTreeSet<VectorNameBuf> = BTreeSet::new();
                let mut to_create: BTreeMap<VectorNameBuf, VectorNameConfig> = BTreeMap::new();

                for (_, segment) in segments.iter() {
                    let segment_guard = segment.get().read();
                    let segment_config = segment_guard.config();

                    for (vector_name, expected_dense) in &target_dense {
                        match segment_config.vector_data.get(vector_name.as_str()) {
                            None => {
                                to_create.insert(
                                    vector_name.clone(),
                                    VectorNameConfig::dense(expected_dense.clone()),
                                );
                            }
                            Some(current_cfg) => {
                                let current_dense = DenseVectorConfig {
                                    size: current_cfg.size,
                                    distance: current_cfg.distance,
                                    multivector_config: current_cfg.multivector_config,
                                    datatype: current_cfg.datatype,
                                };
                                if current_dense != *expected_dense {
                                    to_delete.insert(vector_name.clone());
                                    to_create.insert(
                                        vector_name.clone(),
                                        VectorNameConfig::dense(expected_dense.clone()),
                                    );
                                }
                            }
                        }
                    }

                    for vector_name in segment_config.vector_data.keys() {
                        if !target_dense.contains_key(vector_name) {
                            to_delete.insert(vector_name.clone());
                        }
                    }

                    for (vector_name, expected_sparse) in &target_sparse {
                        match segment_config.sparse_vector_data.get(vector_name.as_str()) {
                            None => {
                                to_create.insert(
                                    vector_name.clone(),
                                    VectorNameConfig::sparse(*expected_sparse),
                                );
                            }
                            Some(current_cfg) => {
                                let current_sparse = SparseVectorConfig {
                                    modifier: current_cfg.modifier,
                                    datatype: current_cfg.index.datatype,
                                };
                                if current_sparse != *expected_sparse {
                                    to_delete.insert(vector_name.clone());
                                    to_create.insert(
                                        vector_name.clone(),
                                        VectorNameConfig::sparse(*expected_sparse),
                                    );
                                }
                            }
                        }
                    }

                    for vector_name in segment_config.sparse_vector_data.keys() {
                        if !target_sparse.contains_key(vector_name) {
                            to_delete.insert(vector_name.clone());
                        }
                    }
                }

                (to_delete, to_create)
            };

            if !to_delete.is_empty() || !to_create.is_empty() {
                shard_plans.push((shard, to_delete, to_create));
            }
        }

        if shard_plans.is_empty() {
            return Ok(());
        }

        // The collection config already contains the target schema. Apply the required
        // deletions and creations per local shard, running all shard tasks to completion
        // because `ShardReplicaSet::update_local` is not cancel safe.
        let results = self
            .update_runtime
            .spawn(
                async move {
                    let local_updates = FuturesUnordered::new();

                    for (shard, to_delete, to_create) in shard_plans {
                        local_updates.push(async move {
                            for vector_name in to_delete {
                                let operation = OperationWithClockTag::from(
                                    CollectionUpdateOperations::VectorNameOperation(
                                        VectorNameOperations::DeleteVectorName(DeleteVectorName {
                                            vector_name,
                                        }),
                                    ),
                                );
                                shard
                                    .update_local(operation, WaitUntil::Segment, None, true)
                                    .unmeasured(reason("Schema changes aren't measured"))
                                    .await?;
                            }

                            for (vector_name, config) in to_create {
                                let operation = OperationWithClockTag::from(
                                    CollectionUpdateOperations::VectorNameOperation(
                                        VectorNameOperations::CreateVectorName(CreateVectorName {
                                            vector_name,
                                            config,
                                        }),
                                    ),
                                );
                                shard
                                    .update_local(operation, WaitUntil::Segment, None, true)
                                    .unmeasured(reason(
                                        "Raft snapshot vector schema reconciliation",
                                    ))
                                    .await?;
                            }

                            Ok::<(), CollectionError>(())
                        });
                    }

                    local_updates.collect::<Vec<_>>().await
                }
                .in_current_ambient(),
            )
            .await?;

        // Refresh optimizers after segment updates, including when only some shards succeeded.
        self.recreate_optimizers_background();

        for result in results {
            result?;
        }

        Ok(())
    }

    /// Truncate unapplied WAL records for all local shards in the collection.
    /// Returns amount of removed records.
    pub async fn truncate_unapplied_wal(&self) -> CollectionResult<usize> {
        let shard_holder = self.shards_holder.clone().read_owned().await;

        let results = self
            .update_runtime
            .spawn(async move {
                let local_updates: FuturesUnordered<_> = shard_holder
                    .all_shards()
                    .map(|shard| shard.truncate_unapplied_wal())
                    .collect();

                let results: Vec<_> = local_updates.collect().await;

                results
            })
            .await?;

        results.into_iter().sum()
    }
}
