use std::path::Path;

use common::fs::{safe_delete_with_suffix, sync_parent_dir_async};
use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err::tokio as tokio_fs;
use segment::types::SnapshotFormat;
use shard::snapshots::snapshot_manifest::{RecoveryType, SnapshotManifest};
use shard::snapshots::snapshot_utils::{SnapshotMergePlan, SnapshotUtils};

use super::{REPLICA_STATE_FILE, ShardReplicaSet};
use crate::common::file_utils::{move_dir, move_file};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::dummy_shard::DummyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::replica_set::replica_set_state::ReplicaSetState;
use crate::shards::shard::{PeerId, Shard};
use crate::shards::shard_config::ShardConfig;
use crate::shards::shard_initializing_flag_path;

impl ShardReplicaSet {
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<SnapshotManifest>,
        save_wal: bool,
    ) -> CollectionResult<impl Future<Output = CollectionResult<()>> + use<>> {
        // Track concurrent `create_partial_snapshot` requests, so that cluster manager can load-balance them
        let partial_snapshot_create_request_guard = if manifest.is_some() {
            Some(self.partial_snapshot_meta.track_create_snapshot_request())
        } else {
            None
        };

        let replica_state = self.replica_state.clone();
        let temp_path = temp_path.to_path_buf();

        let local_read = self.local.read().await;

        let maybe_local_snapshot_future = if let Some(local) = &*local_read {
            Some(
                local
                    .get_snapshot_creator(&temp_path, &tar, format, manifest, save_wal)
                    .await?,
            )
        } else {
            None
        };

        drop(local_read);

        let future = async move {
            let _partial_snapshot_create_request_guard = partial_snapshot_create_request_guard;

            if let Some(local_snapshot_future) = maybe_local_snapshot_future {
                local_snapshot_future.await?;
            }

            replica_state.save_to_tar(&tar, REPLICA_STATE_FILE).await?;

            let shard_config = ShardConfig::new_replica_set();
            shard_config.save_to_tar(&tar).await?;
            Ok(())
        };

        Ok(future)
    }

    pub fn try_take_partial_snapshot_recovery_lock(
        &self,
    ) -> CollectionResult<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.partial_snapshot_meta.try_take_recovery_lock()
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
            state.switch_peer_id(this_peer_id);
            if !is_distributed {
                state.force_local_active()
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
        recovery_type: RecoveryType,
        collection_path: &Path,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // `local.take()` call and `restore` task have to be executed as a single transaction

        if !LocalShard::check_data(replica_path) {
            return Ok(false);
        }

        let snapshot_manifest =
            SnapshotManifest::load_from_snapshot(replica_path, Some(recovery_type))?;

        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let _partial_snapshot_search_lock = match recovery_type {
            RecoveryType::Full => None,
            RecoveryType::Partial => {
                Some(self.partial_snapshot_meta.take_search_write_lock().await)
            }
        };

        let mut local = cancel::future::cancel_on_token(cancel.clone(), self.local.write()).await?;

        // set shard_id initialization flag
        // the file is removed after full recovery to indicate a well-formed shard
        // for example: some of the files may go missing if node gets killed during shard directory move/replace
        let shard_flag = shard_initializing_flag_path(collection_path, self.shard_id);
        let flag_file = tokio_fs::File::create(&shard_flag).await?;
        flag_file.sync_all().await?;
        sync_parent_dir_async(&shard_flag).await?;

        // Check `cancel` token one last time before starting non-cancellable section
        if cancel.is_cancelled() {
            return Err(cancel::Error::Cancelled.into());
        }

        let local_manifest = match local.take() {
            Some(shard) if snapshot_manifest.is_empty() => {
                // Shard is no longer needed and can be dropped
                shard.stop_gracefully().await;

                None
            }
            Some(shard) => {
                let local_manifest = shard.snapshot_manifest().await;

                // If local shard produces a valid manifest, it can be replaced and no longer needed
                // If it fails, we return it back.

                match local_manifest {
                    Ok(local_manifest) => {
                        local_manifest.validate().map_err(|err| {
                            CollectionError::service_error(format!(
                                "failed to restore partial shard snapshot for shard {}:{}: \
                                 local shard produces invalid snapshot manifest: \
                                 {err}",
                                self.collection_id, self.shard_id,
                            ))
                        })?;

                        // Shard is no longer needed and can be dropped
                        shard.stop_gracefully().await;

                        Some(local_manifest)
                    }

                    Err(err) => {
                        let _ = local.insert(shard);

                        return Err(CollectionError::service_error(format!(
                            "failed to restore partial shard snapshot for shard {}:{}: \
                             failed to collect snapshot manifest: \
                             {err}",
                            self.collection_id, self.shard_id,
                        )));
                    }
                }
            }
            None => None,
        };

        // Try to restore local replica from specified shard snapshot directory
        let restore = async {
            if let Some(local_manifest) = local_manifest {
                // ToDo: Replace with `partial_snapshot_merge_plan` when rocksdb is removed
                let merge_plan = SnapshotUtils::partial_snapshot_merge_plan(
                    &self.shard_path,
                    &local_manifest,
                    replica_path,
                    &snapshot_manifest,
                );

                let SnapshotMergePlan {
                    move_files,
                    replace_directories,
                    merge_directories,
                    delete_files,
                    delete_directories,
                } = merge_plan;

                // Clean up files and directories according to merge plan
                for path in delete_files {
                    if !path.exists() {
                        continue;
                    }
                    log::debug!("Deleting file {}", path.display());
                    tokio_fs::remove_file(&path).await?;
                }

                for path in delete_directories {
                    if !path.exists() {
                        continue;
                    }
                    log::debug!("Deleting directory {}", path.display());
                    tokio::task::spawn_blocking({
                        let path = path.clone();
                        move || safe_delete_with_suffix(&path)
                    })
                    .await??;
                }

                // Execute merge plan
                for (from, to) in move_files {
                    log::debug!("Moving file from {} to {}", from.display(), to.display());
                    move_file(&from, &to).await?;
                }

                for (from, to) in replace_directories {
                    log::debug!(
                        "Replacing directory {} with {}",
                        to.display(),
                        from.display()
                    );
                    if to.exists() {
                        tokio::task::spawn_blocking({
                            let to = to.clone();
                            move || safe_delete_with_suffix(&to)
                        })
                        .await??;
                    }
                    move_dir(from, to).await?;
                }

                for (from, to) in merge_directories {
                    log::debug!(
                        "Merging directory from {} to {}",
                        from.display(),
                        to.display()
                    );
                    move_dir(from, to).await?;
                }
            } else {
                // Remove shard data but not configuration files
                LocalShard::clear(&self.shard_path).await?;
                LocalShard::move_data(replica_path, &self.shard_path).await?;
            }

            LocalShard::load(
                self.shard_id,
                self.collection_id.clone(),
                &self.shard_path,
                self.collection_config.clone(),
                self.optimizers_config.clone(),
                self.shared_storage_config.clone(),
                self.payload_index_schema.clone(),
                recovery_type.is_full(),
                self.update_runtime.clone(),
                self.search_runtime.clone(),
                self.optimizer_resource_budget.clone(),
            )
            .await
        };

        match restore.await {
            Ok(new_local) => {
                local.replace(Shard::Local(new_local));
                // remove shard_id initialization flag because shard is fully recovered
                tokio_fs::remove_file(&shard_flag).await?;

                if recovery_type.is_partial() {
                    self.partial_snapshot_meta.snapshot_recovered();
                }

                Ok(true)
            }

            Err(restore_err) => {
                // Initialize "dummy" replica
                local.replace(Shard::Dummy(DummyShard::new(
                    "Failed to restore local replica",
                )));

                // Mark local replica as Dead since it's dummy and dirty
                self.add_locally_disabled(None, self.this_peer_id(), None);

                // Remove inner shard data but keep the shard folder with its configuration files.
                // This way the shard can be read on startup and the user can decide what to do next.
                match LocalShard::clear(&self.shard_path).await {
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

    pub async fn get_partial_snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        self.local
            .read()
            .await
            .as_ref()
            .ok_or_else(|| {
                CollectionError::not_found(format!(
                    "local shard {}:{} does not exist on peer {}",
                    self.collection_id,
                    self.shard_id,
                    self.this_peer_id(),
                ))
            })?
            .snapshot_manifest()
            .await
    }
}
