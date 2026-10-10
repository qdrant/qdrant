use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use common::fs::read_json;
use common::storage_version::StorageVersion as _;
use common::tar_ext::BuilderExt;
use common::tar_unpack::tar_unpack_file;
use fs_err::File;
use segment::types::SnapshotFormat;
use segment::utils::fs::move_all;
use shard::files::PAYLOAD_INDEX_CONFIG_FILE;
use shard::snapshots::snapshot_data::SnapshotData;
use shard::snapshots::snapshot_manifest::{RecoveryType, SnapshotManifest};
use tokio::sync::OwnedRwLockReadGuard;

use super::Collection;
use crate::collection::CollectionVersion;
use crate::common::snapshot_stream::SnapshotStream;
use crate::common::snapshots_manager::SnapshotStorageManager;
use crate::config::{COLLECTION_CONFIG_FILE, CollectionConfigInternal, ShardingMethod};
use crate::operations::snapshot_ops::SnapshotDescription;
use crate::operations::types::{CollectionError, CollectionResult, NodeType};
use crate::shards::local_shard::LocalShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{self, ShardConfig};
use crate::shards::shard_holder::recovery_guard::{RecoveryProgressHandle, ShardRecoveryGuard};
use crate::shards::shard_holder::shard_mapping::ShardKeyMapping;
use crate::shards::shard_holder::{SHARD_KEY_MAPPING_FILE, ShardHolder, shard_not_found_error};
use crate::shards::shard_path;

impl Collection {
    pub fn get_snapshots_storage_manager(&self) -> CollectionResult<SnapshotStorageManager> {
        SnapshotStorageManager::new(&self.shared_storage_config.snapshots_config)
    }

    pub async fn list_snapshots(&self) -> CollectionResult<Vec<SnapshotDescription>> {
        let snapshot_manager = self.get_snapshots_storage_manager()?;
        snapshot_manager.list_snapshots(&self.snapshots_path).await
    }

    /// Creates a snapshot of the collection.
    ///
    /// The snapshot is created in three steps:
    /// 1. Create a temporary directory and create a snapshot of each shard in it.
    /// 2. Archive the temporary directory into a single file.
    /// 3. Move the archive to the final location.
    ///
    /// # Arguments
    ///
    /// * `global_temp_dir`: directory used to host snapshots while they are being created
    /// * `this_peer_id`: current peer id
    ///
    /// returns: Result<SnapshotDescription, CollectionError>
    pub async fn create_snapshot(
        &self,
        global_temp_dir: &Path,
        this_peer_id: PeerId,
    ) -> CollectionResult<SnapshotDescription> {
        // Generate a unique snapshot name. The base format is
        // `{name}-{peer_id}-{millis_resolution}.snapshot`. If a snapshot with
        // that name already exists (e.g., two requests racing within the
        // same millisecond on a fast loop), append a numeric suffix until
        // unique. Without this, the second `store_file` would silently
        // rename-overwrite the first archive and the first client would be
        // told a snapshot exists that has been deleted from disk. See
        // issue #10554.
        let snapshot_name = unique_snapshot_name(
            self.name(),
            this_peer_id,
            chrono::Utc::now()
                .format("%Y-%m-%d-%H-%M-%S-%3f")
                .to_string(),
            &self.snapshots_path,
        )?;

        // Final location of snapshot
        let snapshot_path = self.snapshots_path.join(&snapshot_name);
        log::info!("Creating collection snapshot {snapshot_name} into {snapshot_path:?}");

        // Dedicated temporary file for archiving this snapshot (deleted on drop)
        let snapshot_temp_arc_file = tempfile::Builder::new()
            .prefix(&format!("{snapshot_name}-arc-"))
            .tempfile_in(global_temp_dir)
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "failed to create temporary snapshot directory {}/{snapshot_name}-arc-XXXX: \
                     {err}",
                    global_temp_dir.display(),
                ))
            })?;

        let tar = BuilderExt::new_seekable_owned(File::create(snapshot_temp_arc_file.path())?);

        // Create snapshot of each shard
        {
            let snapshot_temp_temp_dir = tempfile::Builder::new()
                .prefix(&format!("{snapshot_name}-temp-"))
                .tempdir_in(global_temp_dir)
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "failed to create temporary snapshot directory {}/{snapshot_name}-temp-XXXX: \
                         {err}",
                        global_temp_dir.display(),
                    ))
                })?;

            let mut futures = Vec::new();
            {
                let shards_holder = self.shards_holder.read().await;

                // Create snapshot of each shard
                for (shard_id, replica_set) in shards_holder.get_shards() {
                    let shard_snapshot_path = shard_path(Path::new(""), shard_id);

                    // If node is listener, we can save whatever currently is in the storage
                    let save_wal = self.shared_storage_config.node_type != NodeType::Listener;
                    let future = replica_set
                        .create_snapshot(
                            snapshot_temp_temp_dir.path(),
                            tar.descend(&shard_snapshot_path)?,
                            SnapshotFormat::Regular,
                            None,
                            save_wal,
                        )
                        .await?;
                    futures.push(future);
                }
            }

            for future in futures {
                future.await.map_err(|err| {
                    CollectionError::service_error(format!("failed to create snapshot: {err}"))
                })?;
            }
        }

        // Save collection config and version
        tar.append_data(
            CollectionVersion::current_raw().as_bytes().to_vec(),
            Path::new(common::storage_version::VERSION_FILE),
        )
        .await?;

        tar.append_data(
            self.collection_config.read().await.to_bytes()?,
            Path::new(COLLECTION_CONFIG_FILE),
        )
        .await?;

        self.shards_holder
            .read()
            .await
            .save_key_mapping_to_tar(&tar)
            .await?;

        self.payload_index_schema
            .save_to_tar(&tar, Path::new(PAYLOAD_INDEX_CONFIG_FILE))
            .await?;

        tar.finish().await.map_err(|err| {
            CollectionError::service_error(format!("failed to create snapshot archive: {err}"))
        })?;

        let snapshot_manager = self.get_snapshots_storage_manager()?;
        snapshot_manager
            .store_file(snapshot_temp_arc_file.path(), snapshot_path.as_path())
            .await
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "failed to store snapshot archive to {}: {err}",
                    snapshot_temp_arc_file.path().display()
                ))
            })
    }

    /// Restore collection from snapshot
    ///
    /// This method performs blocking IO.
    pub fn restore_snapshot(
        snapshot_data: SnapshotData,
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        match snapshot_data {
            SnapshotData::Packed(snapshot_path) => {
                tar_unpack_file(&snapshot_path, target_dir)?;
                snapshot_path.close()?;
            }
            SnapshotData::Unpacked(snapshot_dir) => {
                // already unpacked snapshot, validate files and move to target dir
                let snapshot_dir_path = snapshot_dir.path();
                move_all(snapshot_dir_path, target_dir)?;
            }
        }

        // A snapshot without a collection config is a malformed archive. Reject it
        // explicitly: the raw IO error from `load` would embed the server-side
        // temporary path in the API response.
        if !CollectionConfigInternal::check(target_dir) {
            return Err(CollectionError::bad_input(
                "Snapshot archive does not contain a collection config",
            ));
        }

        let config = CollectionConfigInternal::load(target_dir)?;
        config.validate_and_warn();
        let configured_shards = config.params.shard_number.get();

        let shard_ids_list: Vec<_> = match config.params.sharding_method.unwrap_or_default() {
            ShardingMethod::Auto => (0..configured_shards).collect(),
            ShardingMethod::Custom => {
                // Load shard mapping from disk
                let mapping_path = target_dir.join(SHARD_KEY_MAPPING_FILE);
                debug_assert!(
                    mapping_path.exists(),
                    "Shard mapping file must exist once custom sharding is used"
                );
                if !mapping_path.exists() {
                    Vec::new()
                } else {
                    let shard_key_mapping: ShardKeyMapping = read_json(&mapping_path)?;
                    shard_key_mapping.shard_ids()
                }
            }
        };

        // Check that all shard ids are unique
        debug_assert_eq!(
            shard_ids_list.len(),
            shard_ids_list.iter().collect::<HashSet<_>>().len(),
            "Shard mapping must contain all shards",
        );

        for shard_id in shard_ids_list {
            let shard_path = shard_path(target_dir, shard_id);
            let shard_config_opt = ShardConfig::load(&shard_path)?;
            if let Some(shard_config) = shard_config_opt {
                match shard_config.r#type {
                    shard_config::ShardType::Local => LocalShard::restore_snapshot(&shard_path)?,
                    shard_config::ShardType::Remote { .. } => {
                        RemoteShard::restore_snapshot(&shard_path)
                    }
                    shard_config::ShardType::Temporary => {}
                    shard_config::ShardType::ReplicaSet => ShardReplicaSet::restore_snapshot(
                        &shard_path,
                        this_peer_id,
                        is_distributed,
                    )?,
                }
            } else {
                return Err(CollectionError::service_error(format!(
                    "Can't read shard config at {}",
                    shard_path.display()
                )));
            }
        }

        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn recover_local_shard_from(
        &self,
        snapshot_shard_path: &Path,
        recovery_type: RecoveryType,
        shard_id: ShardId,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        // `ShardHolder::recover_local_shard_from` is *not* cancel safe
        // (see `ShardReplicaSet::restore_local_replica_from`)
        let res = self
            .shards_holder
            .read()
            .await
            .recover_local_shard_from(
                snapshot_shard_path,
                recovery_type,
                &self.path,
                shard_id,
                cancel,
            )
            .await?;

        Ok(res)
    }

    pub async fn list_shard_snapshots(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        self.shards_holder
            .read()
            .await
            .list_shard_snapshots(&self.snapshots_path, shard_id)
            .await
    }

    pub async fn create_shard_snapshot(
        &self,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        let snapshot_creator = self
            .shards_holder
            .read()
            .await
            .create_shard_snapshot(&self.snapshots_path, self.name(), shard_id, temp_dir)
            .await?;
        // We don't hold shards_holder lock here on purpose,
        // because snapshot creation may take a long time,
        // and we don't want to block other operations on the collection.
        snapshot_creator.await
    }

    pub async fn stream_shard_snapshot(
        &self,
        shard_id: ShardId,
        manifest: Option<SnapshotManifest>,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotStream> {
        let shard = OwnedRwLockReadGuard::try_map(
            self.shards_holder.clone().read_owned().await,
            |shard_holder| shard_holder.get_shard(shard_id),
        )
        .map_err(|_| shard_not_found_error(shard_id))?;

        ShardHolder::stream_shard_snapshot(shard, self.name(), shard_id, manifest, temp_dir).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    #[expect(clippy::too_many_arguments)]
    pub async fn restore_shard_snapshot(
        &self,
        shard_id: ShardId,
        snapshot_data: SnapshotData,
        recovery_type: RecoveryType,
        this_peer_id: PeerId,
        is_distributed: bool,
        temp_dir: &Path,
        recovery_progress: Option<RecoveryProgressHandle>,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<impl Future<Output = CollectionResult<()>> + 'static> {
        let replica_set = self
            .shards_holder
            .read()
            .await
            .get_shard(shard_id)
            .cloned()
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        let snapshot_temp_dir = ShardHolder::prepare_shard_snapshot(
            snapshot_data,
            self.name(),
            shard_id,
            this_peer_id,
            is_distributed,
            temp_dir,
            recovery_progress.as_ref(),
            cancel.clone(),
        )
        .await?;

        // Acquire the shard holder lock, check that the replica set was not removed or
        // replaced during snapshot preparation, and hold the lock until the snapshot is
        // recovered so the shard cannot be replaced or removed
        let shard_holder = self.shards_holder.clone().read_owned().await;

        // Check that the replica set is unchanged
        let current_replica_set = shard_holder
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        if !Arc::ptr_eq(&replica_set, current_replica_set) {
            return Err(CollectionError::bad_request(format!(
                "Shard {shard_id} was replaced during snapshot preparation"
            )));
        }

        // `ShardHolder::restore_shard_snapshot` is *not* cancel-safe,
        // it must be spawned onto runtime
        let collection_path = self.path.clone();
        let restore = self.update_runtime.spawn(async move {
            shard_holder
                .restore_shard_snapshot(
                    snapshot_temp_dir.path(),
                    recovery_type,
                    &collection_path,
                    shard_id,
                    recovery_progress,
                    cancel,
                )
                .await
        });

        // Flatten nested `Result<Result<()>>` into `Result<()>`
        let restore = async move {
            restore.await.map_err(CollectionError::from)??;
            Ok(())
        };

        Ok(restore)
    }

    pub async fn assert_shard_exists(&self, shard_id: ShardId) -> CollectionResult<()> {
        self.shards_holder
            .read()
            .await
            .assert_shard_exists(shard_id)
    }

    /// Start a snapshot recovery of `shard_id`, waiting for one already in progress to
    /// finish.
    ///
    /// The returned guard holds the shard's recovery lock and must be held for the whole
    /// recovery - clear, download and restore. See [`ShardRecoveryGuard`].
    pub async fn start_shard_recovery(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<ShardRecoveryGuard> {
        // Release the shard holder before awaiting: the lock is held for the length of a
        // snapshot download, which would stall the whole collection.
        let replica_set = self
            .shards_holder
            .read()
            .await
            .get_shard(shard_id)
            .cloned()
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        let recovery_lock = replica_set.take_snapshot_recovery_lock().await;

        Ok(self
            .shards_holder
            .read()
            .await
            .start_shard_recovery(shard_id, recovery_lock))
    }

    /// Drop the local shard and clear its on-disk data, before a shard snapshot
    /// transfer downloads a replacement snapshot. See
    /// [`ShardReplicaSet::clear_local_for_snapshot_recovery`] for details and safety
    /// constraints.
    ///
    /// A shard transfer into this shard must be registered, from `from_peer_id` if that is given.
    /// This is destructive, so a sender that drives a transfer consensus has since aborted must
    /// not get to wipe a replica that another transfer is populating. Senders running an older
    /// version don't identify themselves, they are only held to *some* transfer being registered.
    pub async fn clear_local_shard_for_snapshot_recovery(
        &self,
        shard_id: ShardId,
        from_peer_id: Option<PeerId>,
    ) -> CollectionResult<()> {
        let shard_holder = self.shards_holder.read().await;

        let transfers =
            shard_holder.get_transfers(|transfer| transfer.is_target(self.this_peer_id, shard_id));

        let is_registered = if let Some(from_peer_id) = from_peer_id {
            transfers
                .iter()
                .any(|transfer| transfer.from == from_peer_id)
        } else {
            !transfers.is_empty()
        };

        if !is_registered {
            let from = match from_peer_id {
                Some(from_peer_id) => format!("from peer {from_peer_id}"),
                None => "from any peer".into(),
            };

            return Err(CollectionError::bad_request(format!(
                "Refusing to clear shard {shard_id} for snapshot recovery: \
                 there is no registered transfer {from}",
            )));
        }

        shard_holder
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?
            .clear_local_for_snapshot_recovery(&self.path)
            .await
    }

    pub async fn try_take_partial_snapshot_recovery_lock(
        &self,
        shard_id: ShardId,
        recovery_type: RecoveryType,
    ) -> CollectionResult<Option<tokio::sync::OwnedRwLockWriteGuard<()>>> {
        self.shards_holder
            .read()
            .await
            .try_take_partial_snapshot_recovery_lock(shard_id, recovery_type)
    }

    pub async fn get_partial_snapshot_manifest(
        &self,
        shard_id: ShardId,
    ) -> CollectionResult<SnapshotManifest> {
        self.shards_holder
            .read()
            .await
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?
            .get_partial_snapshot_manifest()
            .await
    }
}

/// Generate a unique snapshot file name for the given collection and peer.
///
/// The base name is `{name}-{peer_id}-{timestamp}.snapshot` where `timestamp`
/// is provided by the caller in millisecond resolution (e.g. `"2026-10-08-01-43-00-123"`).
/// If a file with that name already exists in `snapshots_path`, append a
/// numeric suffix (`-2`, `-3`, ...) until a free name is found.
///
/// The pre-existence check is racy in the strict sense: two requests can
/// pass the `exists()` check before either writes. In practice the
/// millisecond timestamp plus the bounded retry counter covers the
/// observed patterns (concurrent pairs and 10ms-spaced sequential loops).
/// The two real "save point" failures that would still cause a silent
/// overwrite would be many requests in a single millisecond on a single
/// peer, which is far outside the issue's repro envelope.
fn unique_snapshot_name(
    collection_name: &str,
    peer_id: PeerId,
    timestamp: String,
    snapshots_path: &Path,
) -> CollectionResult<String> {
    const MAX_COLLISION_ATTEMPTS: u32 = 1000;

    let base = format!("{collection_name}-{peer_id}-{timestamp}.snapshot");
    if !snapshots_path.join(&base).exists() {
        return Ok(base);
    }

    for counter in 2..=MAX_COLLISION_ATTEMPTS {
        let candidate = format!("{collection_name}-{peer_id}-{timestamp}-{counter}.snapshot");
        if !snapshots_path.join(&candidate).exists() {
            return Ok(candidate);
        }
    }

    Err(CollectionError::service_error(format!(
        "could not find a unique snapshot name for {collection_name} after {MAX_COLLISION_ATTEMPTS} attempts"
    )))
}

#[cfg(test)]
mod unique_snapshot_name_tests {
    use std::fs;
    use std::path::PathBuf;

    use super::unique_snapshot_name;

    fn tempdir() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "qdrant-snapshot-name-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    /// Regression for issue #10554: when no file with the base name
    /// exists, the helper returns the base name unchanged. This is the
    /// common case; the millisecond resolution in the timestamp string
    /// already covers the typical intra-second collision.
    #[test]
    fn test_unique_snapshot_name_base_when_no_collision() {
        let dir = tempdir();
        let timestamp = "2026-10-08-01-43-00-123".to_string();
        let name = unique_snapshot_name("coll", 7, timestamp, &dir).unwrap();
        assert_eq!(name, "coll-7-2026-10-08-01-43-00-123.snapshot");
    }

    /// Regression for issue #10554: when the base name is taken (e.g., a
    /// concurrent request that completed first), the helper returns the
    /// base with a numeric suffix (`-2`). This is the second-fast path.
    #[test]
    fn test_unique_snapshot_name_appends_counter_on_collision() {
        let dir = tempdir();
        let timestamp = "2026-10-08-01-43-00-456".to_string();
        // Pre-create the base file to simulate a prior request that
        // already wrote to that name.
        fs::write(
            dir.join("coll-3-2026-10-08-01-43-00-456.snapshot"),
            b"existing",
        )
        .unwrap();
        let name = unique_snapshot_name("coll", 3, timestamp, &dir).unwrap();
        assert_eq!(name, "coll-3-2026-10-08-01-43-00-456-2.snapshot");
    }

    /// Regression for issue #10554: the counter increments past `-2` if
    /// `-2` is also taken. Picks the smallest free counter.
    #[test]
    fn test_unique_snapshot_name_skips_taken_counters() {
        let dir = tempdir();
        let timestamp = "2026-10-08-01-43-00-789".to_string();
        // Pre-create base and -2; -3 should be free.
        fs::write(
            dir.join("coll-9-2026-10-08-01-43-00-789.snapshot"),
            b"existing",
        )
        .unwrap();
        fs::write(
            dir.join("coll-9-2026-10-08-01-43-00-789-2.snapshot"),
            b"existing",
        )
        .unwrap();
        let name = unique_snapshot_name("coll", 9, timestamp, &dir).unwrap();
        assert_eq!(name, "coll-9-2026-10-08-01-43-00-789-3.snapshot");
    }
}
