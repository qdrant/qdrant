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
use crate::shards::transfer::transfer_tasks_pool::{
    TaskResult, TransferTaskItem, TransferTaskProgress,
};
use crate::shards::transfer::{
    ShardTransfer, ShardTransferConsensus, ShardTransferKey, ShardTransferMethod,
};

impl Collection {
    pub async fn get_outgoing_transfers(&self, current_peer_id: &PeerId) -> Vec<ShardTransfer> {
        self.shards_holder
            .read()
            .await
            .get_outgoing_transfers(current_peer_id)
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

        let shard_id = shard_transfer.shard_id;
        let do_transfer = {
            let shards_holder = self.shards_holder.read().await;
            let _was_not_transferred =
                shards_holder.register_start_shard_transfer(shard_transfer.clone())?;
            let replica_set_opt = shards_holder.get_shard(&shard_id);

            // Check if current node owns the shard which should be transferred
            // and therefore able to transfer
            let replica_set = if let Some(replica_set) = replica_set_opt {
                replica_set
            } else {
                // Service error, because it means the validation was incorrect
                return Err(CollectionError::service_error(format!(
                    "Shard {shard_id} doesn't exist"
                )));
            };

            let is_local = replica_set.is_local().await;
            let is_receiver = replica_set.this_peer_id() == shard_transfer.to;
            let is_sender = replica_set.this_peer_id() == shard_transfer.from;

            let initial_state = match shard_transfer.method.unwrap_or_default() {
                ShardTransferMethod::StreamRecords => ReplicaState::Partial,
                ShardTransferMethod::Snapshot | ShardTransferMethod::WalDelta => {
                    ReplicaState::Recovery
                }
            };

            // Create local shard if it does not exist on receiver, or simply set replica state otherwise
            // (on all peers, regardless if shard is local or remote on that peer).
            //
            // This should disable queries to receiver replica even if it was active before.
            if !is_local && is_receiver {
                let shard = LocalShard::build(
                    shard_id,
                    self.name(),
                    &replica_set.shard_path,
                    self.collection_config.clone(),
                    self.shared_storage_config.clone(),
                    self.update_runtime.clone(),
                    self.optimizer_cpu_budget.clone(),
                )
                .await?;

                replica_set.set_local(shard, Some(initial_state)).await?;
            } else {
                replica_set.set_replica_state(&shard_transfer.to, initial_state)?;
            }

            is_local && is_sender
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
        let default_shard_transfer_method = self
            .shared_storage_config
            .default_shard_transfer_method
            .unwrap_or_default();

        let progress = Arc::new(Mutex::new(TransferTaskProgress::new()));

        let transfer_task = transfer::driver::spawn_transfer_task(
            shard_holder,
            progress.clone(),
            transfer.clone(),
            consensus,
            collection_id,
            channel_service,
            self.snapshots_path.clone(),
            self.name(),
            default_shard_transfer_method,
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
    pub async fn finish_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<()> {
        let transfer_finished = self
            .transfer_tasks
            .lock()
            .await
            .stop_task(&transfer.key())
            .await
            == Some(TaskResult::Finished);
        log::debug!("transfer_finished: {transfer_finished}");

        let shards_holder_guard = self.shards_holder.read().await;

        // Should happen on transfer side
        // Unwrap forward proxy into local shard, or replace it with remote shard
        // depending on the `sync` flag.
        if self.this_peer_id == transfer.from {
            let proxy_promoted = transfer::driver::handle_transferred_shard_proxy(
                &shards_holder_guard,
                transfer.shard_id,
                transfer.to,
                transfer.sync,
            )
            .await?;
            log::debug!("proxy_promoted: {proxy_promoted}");
        }

        // Should happen on receiving side
        // Promote partial shard to active shard
        if self.this_peer_id == transfer.to {
            let shard_promoted =
                transfer::driver::finalize_partial_shard(&shards_holder_guard, transfer.shard_id)
                    .await?;
            log::debug!(
                "shard_promoted: {shard_promoted}, shard_id: {}, peer_id: {}",
                transfer.shard_id,
                self.this_peer_id,
            );
        }

        // Should happen on a third-party side
        // Change direction of the remote shards or add a new remote shard
        if self.this_peer_id != transfer.from {
            let remote_shard_rerouted = transfer::driver::change_remote_shard_route(
                &shards_holder_guard,
                transfer.shard_id,
                transfer.from,
                transfer.to,
                transfer.sync,
            )
            .await?;
            log::debug!("remote_shard_rerouted: {remote_shard_rerouted}");
        }
        let finish_was_registered =
            shards_holder_guard.register_finish_transfer(&transfer.key())?;
        log::debug!("finish_was_registered: {finish_was_registered}");
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
    ) -> CollectionResult<()> {
        let shard_holder_guard = self.shards_holder.read().await;
        // Internal implementation, used to prevents double-read deadlock
        self._abort_shard_transfer(transfer_key, &shard_holder_guard)
            .await
    }

    pub(super) async fn _abort_shard_transfer(
        &self,
        transfer_key: ShardTransferKey,
        shard_holder_guard: &ShardHolder,
    ) -> CollectionResult<()> {
        // TODO: Ensure cancel safety!

        let _transfer_result = self
            .transfer_tasks
            .lock()
            .await
            .stop_task(&transfer_key)
            .await;

        let replica_set =
            if let Some(replica_set) = shard_holder_guard.get_shard(&transfer_key.shard_id) {
                replica_set
            } else {
                return Err(CollectionError::bad_request(format!(
                    "Shard {} doesn't exist",
                    transfer_key.shard_id
                )));
            };

        let transfer = shard_holder_guard.get_transfer(&transfer_key);

        if transfer.map(|x| x.sync).unwrap_or(false) {
            replica_set.set_replica_state(&transfer_key.to, ReplicaState::Dead)?;
        } else {
            replica_set.remove_peer(transfer_key.to).await?;
        }

        if self.this_peer_id == transfer_key.from {
            transfer::driver::revert_proxy_shard_to_local(
                shard_holder_guard,
                transfer_key.shard_id,
            )
            .await?;
        }

        let _finish_was_registered = shard_holder_guard.register_finish_transfer(&transfer_key)?;

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

            let Some(replica_set) = shards_holder.get_shard(&shard_id) else {
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
                let replica_set = shards_holder.get_shard(&shard_id).unwrap();
                let shard_transfer_registered = shards_holder.shard_transfers.wait_for(
                    |shard_transfers| {
                        shard_transfers.iter().any(|shard_transfer| {
                            shard_transfer.shard_id == shard_id && shard_transfer.to == this_peer_id
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
                                .get_peer_state(&this_peer_id)
                                .map_or(false, |peer_state| peer_state.is_partial_or_recovery())
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
            .map_or(false, |limit| incoming >= limit);

        let outgoing_shard_transfer_limit_reached = self
            .shared_storage_config
            .outgoing_shard_transfers_limit
            .map_or(false, |limit| outgoing >= limit);

        incoming_shard_transfer_limit_reached || outgoing_shard_transfer_limit_reached
    }
}
