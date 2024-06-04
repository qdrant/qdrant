use std::ops::Deref as _;
use std::path::Path;

use super::{ReplicaSetState, ReplicaState, ShardReplicaSet, REPLICA_STATE_FILE};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard::{PeerId, Shard};
use crate::shards::shard_config::ShardConfig;

impl ShardReplicaSet {
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        target_path: &Path,
        save_wal: bool,
    ) -> CollectionResult<()> {
        let local_read = self.local.read().await;

        if let Some(local) = &*local_read {
            local
                .create_snapshot(temp_path, target_path, save_wal)
                .await?
        }

        self.replica_state
            .save_to(target_path.join(REPLICA_STATE_FILE))?;

        let shard_config = ShardConfig::new_replica_set();
        shard_config.save(target_path)?;
        Ok(())
    }

    pub fn restore_snapshot(
        snapshot_path: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init_default(snapshot_path.join(REPLICA_STATE_FILE))?;

        // If this shard have local data
        let is_snapshot_local = replica_state.read().is_local;

        if !is_distributed && !is_snapshot_local {
            return Err(CollectionError::service_error(format!(
                "Can't restore snapshot in local mode with missing data at shard: {}",
                snapshot_path.display()
            )));
        }

        replica_state.write(|state| {
            state.this_peer_id = this_peer_id;
            if is_distributed {
                state
                    .peers
                    .remove(&this_peer_id)
                    .and_then(|replica_state| state.peers.insert(this_peer_id, replica_state));
            } else {
                // In local mode we don't want any remote peers
                state.peers.clear();
                state.peers.insert(this_peer_id, ReplicaState::Active);
            }
        })?;

        if replica_state.read().is_local {
            LocalShard::restore_snapshot(snapshot_path)?;
        }
        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn restore_local_replica_from(
        &self,
        replica_path: &Path,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // `local.take()` call and `restore` task have to be executed as a single transaction

        if !LocalShard::check_data(replica_path) {
            return Ok(false);
        }

        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let mut local = cancel::future::cancel_on_token(cancel.clone(), self.local.write()).await?;

        // Check `cancel` token one last time before starting non-cancellable section
        if cancel.is_cancelled() {
            return Err(cancel::Error::Cancelled.into());
        }

        // Drop `LocalShard` instance to free resources and clear shard data
        let clear = local.take().is_some();

        // Try to restore local replica from specified shard snapshot directory
        let restore = async {
            if clear {
                LocalShard::clear(&self.shard_path).await?;
            }

            LocalShard::move_data(replica_path, &self.shard_path).await?;

            LocalShard::load(
                self.shard_id,
                self.collection_id.clone(),
                &self.shard_path,
                self.collection_config.clone(),
                self.optimizers_config.clone(),
                self.shared_storage_config.clone(),
                self.update_runtime.clone(),
                self.optimizer_cpu_budget.clone(),
            )
            .await
        };

        match restore.await {
            Ok(new_local) => {
                local.replace(Shard::Local(new_local));
                Ok(true)
            }

            Err(restore_err) => {
                // Initialize "dummy" replica
                local.replace(Shard::Dummy(DummyShard::new(
                    "Failed to restore local replica",
                )));

                // TODO: Handle single-node mode!? (How!? 😰)

                // Mark this peer as "locally disabled"...
                let has_other_active_peers = self.active_remote_shards().await.is_empty();

                // ...if this peer is *not* the last active replica
                if has_other_active_peers {
                    let notify = self
                        .locally_disabled_peers
                        .write()
                        .disable_peer_and_notify_if_elapsed(self.this_peer_id());

                    if notify {
                        self.notify_peer_failure_cb.deref()(self.this_peer_id(), self.shard_id);
                    }
                }

                // Remove shard directory, so we don't leave empty directory/corrupted data
                match tokio::fs::remove_dir_all(&self.shard_path).await {
                    Ok(()) => Err(restore_err),

                    Err(cleanup_err) => {
                        log::error!(
                            "Failed to cleanup shard {} directory ({}) after restore failed: \
                             {cleanup_err}",
                            self.shard_id,
                            self.shard_path.display(),
                        );

                        // TODO: Contextualize `restore_err` with `cleanup_err` details!?
                        Err(restore_err)
                    }
                }
            }
        }
    }
}
