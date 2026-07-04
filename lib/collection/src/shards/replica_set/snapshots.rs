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

#[cfg(test)]
type RestoreLocalReplicaBeforeFlagHook = (
    std::path::PathBuf,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

#[cfg(test)]
static RESTORE_LOCAL_REPLICA_BEFORE_FLAG_HOOK: std::sync::Mutex<
    Option<RestoreLocalReplicaBeforeFlagHook>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
async fn wait_restore_local_replica_before_flag_hook_for_test(shard_flag: &Path) {
    let hook = {
        let mut hook = RESTORE_LOCAL_REPLICA_BEFORE_FLAG_HOOK.lock().unwrap();
        if hook
            .as_ref()
            .is_some_and(|(expected_shard_flag, _, _)| expected_shard_flag.as_path() == shard_flag)
        {
            hook.take()
        } else {
            None
        }
    };

    if let Some((_expected_shard_flag, reached, release)) = hook {
        let _ = reached.send(());
        let _ = release.await;
    }
}

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

        let shard_flag = shard_initializing_flag_path(collection_path, self.shard_id);

        #[cfg(test)]
        wait_restore_local_replica_before_flag_hook_for_test(&shard_flag).await;

        // Check `cancel` token one last time before creating the durable initializing flag. Once
        // the flag exists, recovery must finish to a marker-consistent state instead of returning
        // `Cancelled` and leaving a false dirty marker behind.
        if cancel.is_cancelled() {
            return Err(CollectionError::from(cancel::Error::Cancelled));
        }

        // set shard_id initialization flag
        // the file is removed after full recovery to indicate a well-formed shard
        // for example: some of the files may go missing if node gets killed during shard directory move/replace
        let flag_file = tokio_fs::File::create(&shard_flag).await?;
        flag_file.sync_all().await?;
        sync_parent_dir_async(&shard_flag).await?;

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

    /// Replace the in-memory local shard (if any) with a dummy and remove its on-disk
    /// data files.
    ///
    /// Used by shard snapshot transfers to free disk space before the receiving node
    /// downloads the new snapshot, avoiding having both the old data and the incoming
    /// snapshot on disk at the same time. The configuration files and replica state
    /// are preserved, so the shard directory remains a valid (empty) shard.
    ///
    /// A dummy shard is left in place of the real one so that APIs still report a local
    /// shard while the data is being cleared and recovered, rather than reporting none.
    /// The subsequent `restore_local_replica_from` call drops this dummy and installs
    /// the recovered shard.
    ///
    /// Only safe to call while the shard is in a state that prevents user requests
    /// (`PartialSnapshot` during a shard transfer). Do NOT call this from a
    /// user-triggered URL recovery path, where the shard may still be serving queries.
    ///
    /// Writes the shard initializing flag before clearing, so that a crash between
    /// here and the end of the subsequent `restore_local_replica_from` call causes
    /// the shard to be loaded as a dummy on startup and re-recovered.
    pub async fn clear_local_for_snapshot_recovery(
        &self,
        collection_path: &Path,
    ) -> CollectionResult<()> {
        // Callers must only invoke this while the shard is in a state that cannot
        // be a source of truth (e.g. `PartialSnapshot` during a shard transfer).
        // Clearing a source-of-truth replica would silently drop data that may
        // still be serving queries.
        if self
            .peer_state(self.this_peer_id())
            .is_some_and(|s| s.can_be_source_of_truth())
        {
            return Err(CollectionError::service_error(format!(
                "clear_local_for_snapshot_recovery called on a peer that can be source-of-truth {}:{}",
                self.collection_id, self.shard_id,
            )));
        }

        let mut local = self.local.write().await;

        // Mark the shard as initializing before touching disk, so a crash during or
        // after clearing is detected on next startup and the shard is reloaded as a
        // dummy that triggers recovery.
        let shard_flag = shard_initializing_flag_path(collection_path, self.shard_id);
        let flag_file = tokio_fs::File::create(&shard_flag).await?;
        flag_file.sync_all().await?;
        sync_parent_dir_async(&shard_flag).await?;

        // Replace the local shard with a dummy rather than removing it entirely, so APIs
        // keep reporting a local shard while the data is cleared and the replacement
        // snapshot is recovered. `restore_local_replica_from` drops this dummy afterwards.
        let dummy = Shard::Dummy(DummyShard::new(
            "Local shard is being cleared for snapshot recovery",
        ));
        if let Some(shard) = local.replace(dummy) {
            shard.stop_gracefully().await;
        }

        LocalShard::clear(&self.shard_path).await?;

        Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use common::budget::ResourceBudget;
    use common::save_on_disk::SaveOnDisk;
    use segment::types::Distance;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Handle;
    use tokio::sync::{RwLock, oneshot};

    use super::*;
    use crate::collection::payload_index_schema::PayloadIndexSchema;
    use crate::common::adaptive_handle::AdaptiveSearchHandle;
    use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
    use crate::operations::shared_storage_config::SharedStorageConfig;
    use crate::operations::types::VectorsConfig;
    use crate::operations::vector_params_builder::VectorParamsBuilder;
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::channel_service::ChannelService;
    use crate::shards::replica_set::replica_set_state::ReplicaState;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
    use crate::shards::shard::ShardId;

    const TEST_COLLECTION_ID: &str = "test_collection";
    const TEST_TARGET_SHARD_ID: ShardId = 1;
    const TEST_SOURCE_SHARD_ID: ShardId = 2;
    const TEST_PEER_ID: PeerId = 1;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_cancel_snapshot_recovery_before_initializing_flag_does_not_mark_dirty() {
        let target_collection_dir = Builder::new()
            .prefix("snapshot-recovery-cancel-target")
            .tempdir()
            .unwrap();
        let source_collection_dir = Builder::new()
            .prefix("snapshot-recovery-cancel-source")
            .tempdir()
            .unwrap();

        let target_replica_set =
            new_shard_replica_set(&target_collection_dir, TEST_TARGET_SHARD_ID).await;
        let source_replica_set =
            new_shard_replica_set(&source_collection_dir, TEST_SOURCE_SHARD_ID).await;

        let source_shard_path = source_collection_dir
            .path()
            .join(TEST_SOURCE_SHARD_ID.to_string());
        assert!(
            LocalShard::check_data(&source_shard_path),
            "test fixture must create a valid source local shard"
        );

        let shard_flag =
            shard_initializing_flag_path(target_collection_dir.path(), TEST_TARGET_SHARD_ID);
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        install_restore_local_replica_before_flag_hook(shard_flag.clone(), reached_tx, release_rx);

        let cancel = cancel::CancellationToken::new();
        let restore = target_replica_set.restore_local_replica_from(
            &source_shard_path,
            RecoveryType::Full,
            target_collection_dir.path(),
            cancel.clone(),
        );
        tokio::pin!(restore);

        tokio::select! {
            _ = reached_rx => {}
            result = &mut restore => {
                panic!("snapshot recovery completed before the cancellation window: {result:?}");
            }
        }

        cancel.cancel();
        let _ = release_tx.send(());

        let result = restore.await;
        assert!(
            matches!(result, Err(CollectionError::Cancelled { .. })),
            "recovery should observe cancellation before creating the initializing flag, got {result:?}"
        );
        assert!(
            !shard_flag.exists(),
            "cancellation before destructive restore starts must not leave a false dirty marker"
        );
        assert!(
            !target_replica_set.is_dummy().await,
            "cancellation before destructive restore starts must keep the old local shard installed"
        );

        let local = target_replica_set.local.read().await;
        assert!(
            matches!(local.as_ref(), Some(Shard::Local(_))),
            "the old local shard should remain installed"
        );
        drop(local);

        target_replica_set.stop_gracefully().await;
        source_replica_set.stop_gracefully().await;
    }

    fn install_restore_local_replica_before_flag_hook(
        shard_flag: std::path::PathBuf,
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) {
        let mut hook = RESTORE_LOCAL_REPLICA_BEFORE_FLAG_HOOK.lock().unwrap();
        assert!(
            hook.is_none(),
            "restore-local-replica test hook is already installed"
        );
        *hook = Some((shard_flag, reached, release));
    }

    async fn new_shard_replica_set(collection_dir: &TempDir, shard_id: ShardId) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = AdaptiveSearchHandle::current_for_tests();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
            shard_number: NonZeroU32::new(1).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        };

        let optimizers_config = OptimizersConfig::fixture();
        let config = CollectionConfigInternal {
            params: collection_params,
            optimizer_config: optimizers_config.clone(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: None,
            strict_mode_config: None,
            uuid: None,
            metadata: None,
        };

        let payload_index_schema_file = collection_dir.path().join("payload-schema.json");
        let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
            Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());
        let shared_config = Arc::new(RwLock::new(config));

        ShardReplicaSet::build(
            shard_id,
            None,
            TEST_COLLECTION_ID.to_string(),
            TEST_PEER_ID,
            true,
            HashSet::new(),
            dummy_on_replica_failure(),
            dummy_abort_shard_transfer(),
            collection_dir.path(),
            shared_config,
            optimizers_config.clone(),
            Arc::new(SharedStorageConfig::default()),
            payload_index_schema,
            ChannelService::default(),
            update_runtime,
            search_runtime,
            ResourceBudget::default(),
            Some(ReplicaState::Active),
        )
        .await
        .unwrap()
    }

    fn dummy_on_replica_failure() -> ChangePeerFromState {
        Arc::new(move |_peer_id, _shard_id, _from_state| {})
    }

    fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_shard_transfer, _reason| {})
    }
}
