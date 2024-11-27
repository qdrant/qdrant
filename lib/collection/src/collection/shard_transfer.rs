use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::defaults;
use parking_lot::Mutex;

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::LocalShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::ShardHolder;
use crate::shards::transfer;
use crate::shards::transfer::transfer_tasks_pool::{TransferTaskItem, TransferTaskProgress};
use crate::shards::transfer::{
    ShardTransfer, ShardTransferConsensus, ShardTransferKey, ShardTransferMethod,
};

impl Collection {
    pub async fn get_related_transfers(&self, current_peer_id: PeerId) -> Vec<ShardTransfer> {
        self.shards_holder.read().await.get_transfers(|transfer| {
            transfer.from == current_peer_id || transfer.to == current_peer_id
        })
    }

    pub async fn check_transfer_exists(&self, transfer_key: &ShardTransferKey) -> bool {
        self.shards_holder
            .read()
            .await
            .check_transfer_exists(transfer_key)
    }

    pub async fn start_shard_transfer<T, F>(
        &self,
        mut shard_transfer: ShardTransfer,
        consensus: Box<dyn ShardTransferConsensus>,
        temp_dir: PathBuf,
        on_finish: T,
        on_error: F,
    ) -> CollectionResult<bool>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        // Select transfer method
        if shard_transfer.method.is_none() {
            let method = self
                .shared_storage_config
                .default_shard_transfer_method
                .unwrap_or_default();
            log::warn!("No shard transfer method selected, defaulting to {method:?}");
            shard_transfer.method.replace(method);
        }

        let do_transfer = {
            let this_peer_id = consensus.this_peer_id();
            let is_receiver = this_peer_id == shard_transfer.to;
            let is_sender = this_peer_id == shard_transfer.from;

            // Get the source and target shards, in case of resharding the target shard is different
            let from_shard_id = shard_transfer.shard_id;
            let to_shard_id = shard_transfer
                .to_shard_id
                .unwrap_or(shard_transfer.shard_id);

            let shards_holder = self.shards_holder.read().await;
            let from_replica_set = shards_holder.get_shard(from_shard_id).ok_or_else(|| {
                CollectionError::service_error(format!("Shard {from_shard_id} doesn't exist"))
            })?;
            let to_replica_set = shards_holder.get_shard(to_shard_id).ok_or_else(|| {
                CollectionError::service_error(format!("Shard {to_shard_id} doesn't exist"))
            })?;
            let _was_not_transferred =
                shards_holder.register_start_shard_transfer(shard_transfer.clone())?;

            let from_is_local = from_replica_set.is_local().await;
            let to_is_local = to_replica_set.is_local().await;

            let initial_state = match shard_transfer.method.unwrap_or_default() {
                ShardTransferMethod::StreamRecords => ReplicaState::Partial,
                ShardTransferMethod::Snapshot | ShardTransferMethod::WalDelta => {
                    ReplicaState::Recovery
                }
                ShardTransferMethod::ReshardingStreamRecords => ReplicaState::Resharding,
            };

            // Create local shard if it does not exist on receiver, or simply set replica state otherwise
            // (on all peers, regardless if shard is local or remote on that peer).
            //
            // This should disable queries to receiver replica even if it was active before.
            if !to_is_local && is_receiver {
                let effective_optimizers_config = self.effective_optimizers_config().await?;

                let shard = LocalShard::build(
                    to_shard_id,
                    self.name(),
                    &to_replica_set.shard_path,
                    self.collection_config.clone(),
                    self.shared_storage_config.clone(),
                    self.payload_index_schema.clone(),
                    self.update_runtime.clone(),
                    self.search_runtime.clone(),
                    self.optimizer_cpu_budget.clone(),
                    effective_optimizers_config,
                )
                .await?;

                let old_shard = to_replica_set.set_local(shard, Some(initial_state)).await?;
                debug_assert!(old_shard.is_none(), "We should not have a local shard yet");
            } else {
                to_replica_set
                    .ensure_replica_with_state(shard_transfer.to, initial_state)
                    .await?;
            }

            from_is_local && is_sender
        };
        if do_transfer {
            self.send_shard(shard_transfer, consensus, temp_dir, on_finish, on_error)
                .await;
        }
        Ok(do_transfer)
    }

    async fn send_shard<OF, OE>(
        &self,
        transfer: ShardTransfer,
        consensus: Box<dyn ShardTransferConsensus>,
        temp_dir: PathBuf,
        on_finish: OF,
        on_error: OE,
    ) where
        OF: Future<Output = ()> + Send + 'static,
        OE: Future<Output = ()> + Send + 'static,
    {
        let mut active_transfer_tasks = self.transfer_tasks.lock().await;
        let task_result = active_transfer_tasks.stop_task(&transfer.key()).await;

        debug_assert!(task_result.is_none(), "Transfer task already exists");
        debug_assert!(
            transfer.method.is_some(),
            "When sending shard, a transfer method must have been selected",
        );

        let shard_holder = self.shards_holder.clone();
        let collection_id = self.id.clone();
        let channel_service = self.channel_service.clone();

        let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));

        let transfer_task = transfer::driver::spawn_transfer_task(
            shard_holder,
            progress.clone(),
            transfer.clone(),
            consensus,
            collection_id,
            channel_service,
            self.snapshots_path.clone(),
            temp_dir,
            on_finish,
            on_error,
        );

        active_transfer_tasks.add_task(
            &transfer,
            TransferTaskItem {
                task: transfer_task,
                started_at: chrono::Utc::now(),
                progress,
            },
        );
    }

    /// Handles finishing of the shard transfer.
    ///
    /// Returns true if state was changed, false otherwise.
    pub async fn finish_shard_transfer(
        &self,
        transfer: ShardTransfer,
        shard_holder: Option<&ShardHolder>,
    ) -> CollectionResult<()> {
        let transfer_result = self
            .transfer_tasks
            .lock()
            .await
            .stop_task(&transfer.key())
            .await;

        log::debug!("Transfer result: {transfer_result:?}");

        let mut shard_holder_guard = None;

        let shard_holder = match shard_holder {
            Some(shard_holder) => shard_holder,
            None => shard_holder_guard.insert(self.shards_holder.read().await),
        };

        let is_resharding_transfer = transfer.method.is_some_and(|method| method.is_resharding());

        // Handle *destination* replica
        let mut is_dest_replica_active = false;

        let dest_replica_set =
            shard_holder.get_shard(transfer.to_shard_id.unwrap_or(transfer.shard_id));

        if let Some(replica_set) = dest_replica_set {
            if replica_set.peer_state(transfer.to).is_some() {
                // Promote *destination* replica/shard to `Active` if:
                //
                // - replica *exists*
                //   - replica should be created when shard transfer (or resharding) is started
                //   - replica might *not* exist, if it (or the whole *peer*) was removed right
                //     before transfer is finished
                // - transfer is *not* resharding
                //   - resharding requires multiple transfers, so destination shard is promoted
                //     *explicitly* when all transfers are finished

                let state = if is_resharding_transfer {
                    ReplicaState::Resharding
                } else {
                    ReplicaState::Active
                };

                if transfer.to == self.this_peer_id {
                    replica_set.set_replica_state(transfer.to, state)?;
                } else {
                    replica_set.add_remote(transfer.to, state).await?;
                }

                is_dest_replica_active = state == ReplicaState::Active;
            }
        }

        // Handle *source* replica
        let src_replica_set = shard_holder.get_shard(transfer.shard_id);

        if let Some(replica_set) = src_replica_set {
            if transfer.sync || is_resharding_transfer {
                // If transfer is *sync* (or *resharding*), we *keep* source replica

                if transfer.from == self.this_peer_id {
                    // If current peer is *transfer-sender*, we need to unproxify local shard

                    replica_set.un_proxify_local().await?;
                }
            } else if is_dest_replica_active {
                // If transfer is *not* sync (and *not* resharding) and *destination* replica is `Active`,
                // we *remove* source replica

                if transfer.from == self.this_peer_id {
                    replica_set.remove_local().await?;
                } else {
                    replica_set.remove_remote(transfer.from).await?;
                }
            }
        }

        let is_finish_registered = shard_holder.register_finish_transfer(&transfer.key())?;
        log::debug!("Transfer finish registered: {is_finish_registered}");

        Ok(())
    }

    /// Handles abort of the transfer
    ///
    /// 1. Unregister the transfer
    /// 2. Stop transfer task
    /// 3. Unwrap the proxy
    /// 4. Remove temp shard, or mark it as dead
    pub async fn abort_shard_transfer(
        &self,
        transfer_key: ShardTransferKey,
        shard_holder: Option<&ShardHolder>,
    ) -> CollectionResult<()> {
        // TODO: Ensure cancel safety!

        log::debug!("Aborting shard transfer {transfer_key:?}");

        let _transfer_result = self
            .transfer_tasks
            .lock()
            .await
            .stop_task(&transfer_key)
            .await;

        let mut shard_holder_guard = None;

        let shard_holder = match shard_holder {
            Some(shard_holder) => shard_holder,
            None => shard_holder_guard.insert(self.shards_holder.read().await),
        };

        let Some(transfer) = shard_holder.get_transfer(&transfer_key) else {
            return Ok(());
        };

        let is_resharding_transfer = transfer.method.is_some_and(|method| method.is_resharding());

        let shard_id = transfer_key.to_shard_id.unwrap_or(transfer_key.shard_id);

        if let Some(replica_set) = shard_holder.get_shard(shard_id) {
            if replica_set.peer_state(transfer.to).is_some() {
                if is_resharding_transfer {
                    // If *resharding* shard transfer failed, we don't need/want to change replica state:
                    // - on transfer failure, the whole resharding would be aborted (see below),
                    //   and so all changes to replicas would be discarded/rolled-back anyway
                    // - during resharding *up*, we transfer points to a single new shard replica;
                    //   it is expected that this node is initially empty/incomplete, and so failed
                    //   transfer should not strictly introduce inconsistency (it just means the node
                    //   is *still* empty/incomplete); marking this new replica as `Dead` would only
                    //   make requests to return explicit errors
                    // - during resharding *down*, we transfer points from shard-to-be-removed
                    //   to all other shards; all other shards are expected to be `Active`,
                    //   and so failed transfer does not introduce any inconsistencies to points
                    //   that are not affected by resharding in all other shards
                } else if transfer.sync {
                    replica_set.set_replica_state(transfer.to, ReplicaState::Dead)?;
                } else {
                    replica_set.remove_peer(transfer.to).await?;
                }
            }
        } else {
            log::warn!(
                "Aborting shard transfer {transfer_key:?}, but shard {shard_id} does not exist"
            );
        }

        if transfer.from == self.this_peer_id {
            transfer::driver::revert_proxy_shard_to_local(shard_holder, transfer.shard_id).await?;
        }

        shard_holder.register_abort_transfer(&transfer_key)?;

        if is_resharding_transfer {
            let resharding_state = shard_holder.resharding_state.read().clone();

            // `abort_resharding` locks `shard_holder`!
            drop(shard_holder_guard);

            if let Some(state) = resharding_state {
                self.abort_resharding(state.key(), false).await?;
            }
        }

        Ok(())
    }

    /// Initiate local partial shard
    pub fn initiate_shard_transfer(
        &self,
        shard_id: ShardId,
    ) -> impl Future<Output = CollectionResult<()>> + 'static {
        // TODO: Ensure cancel safety!

        let shards_holder = self.shards_holder.clone();

        async move {
            let shards_holder = shards_holder.read_owned().await;

            let Some(replica_set) = shards_holder.get_shard(shard_id) else {
                return Err(CollectionError::service_error(format!(
                    "Shard {shard_id} doesn't exist, repartition is not supported yet"
                )));
            };

            // Wait for the replica set to have the local shard initialized
            // This can take some time as this is arranged through consensus
            replica_set
                .wait_for_local(defaults::CONSENSUS_META_OP_WAIT)
                .await?;

            if !replica_set.is_local().await {
                // We have proxy or something, we need to unwrap it
                log::warn!("Unwrapping proxy shard {}", shard_id);
                replica_set.un_proxify_local().await?;
            }

            if replica_set.is_dummy().await {
                replica_set.init_empty_local_shard().await?;
            }

            let this_peer_id = replica_set.this_peer_id();

            let shard_transfer_requested = tokio::task::spawn_blocking(move || {
                // We can guarantee that replica_set is not None, cause we checked it before
                // and `shards_holder` is holding the lock.
                // This is a workaround for lifetime checker.
                let replica_set = shards_holder.get_shard(shard_id).unwrap();
                let shard_transfer_registered = shards_holder.shard_transfers.wait_for(
                    |shard_transfers| {
                        shard_transfers.iter().any(|shard_transfer| {
                            let to_shard_id = shard_transfer
                                .to_shard_id
                                .unwrap_or(shard_transfer.shard_id);
                            to_shard_id == shard_id && shard_transfer.to == this_peer_id
                        })
                    },
                    Duration::from_secs(60),
                );

                // It is not enough to check for shard_transfer_registered,
                // because it is registered before the state of the shard is changed.
                shard_transfer_registered
                    && replica_set.wait_for_state_condition_sync(
                        |state| {
                            state
                                .get_peer_state(this_peer_id)
                                .is_some_and(|peer_state| peer_state.is_partial_or_recovery())
                        },
                        defaults::CONSENSUS_META_OP_WAIT,
                    )
            });

            match shard_transfer_requested.await {
                Ok(true) => Ok(()),

                Ok(false) => {
                    let description = "\
                        Failed to initiate shard transfer: \
                        Didn't receive shard transfer notification from consensus in 60 seconds";

                    Err(CollectionError::Timeout {
                        description: description.into(),
                    })
                }

                Err(err) => Err(CollectionError::service_error(format!(
                    "Failed to initiate shard transfer: \
                     Failed to execute wait-for-consensus-notification task: \
                     {err}"
                ))),
            }
        }
    }

    /// Whether we have reached the automatic shard transfer limit based on the given incoming and
    /// outgoing transfers.
    pub(super) fn check_auto_shard_transfer_limit(&self, incoming: usize, outgoing: usize) -> bool {
        let incoming_shard_transfer_limit_reached = self
            .shared_storage_config
            .incoming_shard_transfers_limit
            .is_some_and(|limit| incoming >= limit);

        let outgoing_shard_transfer_limit_reached = self
            .shared_storage_config
            .outgoing_shard_transfers_limit
            .is_some_and(|limit| outgoing >= limit);

        incoming_shard_transfer_limit_reached || outgoing_shard_transfer_limit_reached
    }
}
