use std::collections::HashSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use common::tar_ext::BuilderExt;
use io::file_operations::read_json;
use io::storage_version::StorageVersion as _;
use segment::common::validate_snapshot_archive::open_snapshot_archive_with_validation;
use segment::types::SnapshotFormat;
use tokio::sync::OwnedRwLockReadGuard;

use super::Collection;
use crate::collection::payload_index_schema::PAYLOAD_INDEX_CONFIG_FILE;
use crate::collection::CollectionVersion;
use crate::common::snapshot_stream::SnapshotStream;
use crate::common::snapshots_manager::SnapshotStorageManager;
use crate::config::{CollectionConfigInternal, ShardingMethod, COLLECTION_CONFIG_FILE};
use crate::operations::snapshot_ops::SnapshotDescription;
use crate::operations::types::{CollectionError, CollectionResult, NodeType};
use crate::shards::local_shard::LocalShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{self, ShardConfig};
use crate::shards::shard_holder::{
    shard_not_found_error, ShardHolder, ShardKeyMapping, SHARD_KEY_MAPPING_FILE,
};
use crate::shards::shard_versioning;

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
        let snapshot_name = format!(
            "{}-{this_peer_id}-{}.snapshot",
            self.name(),
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S"),
        );

        // Final location of snapshot
        let snapshot_path = self.snapshots_path.join(&snapshot_name);
        log::info!(
            "Creating collection snapshot {} into {:?}",
            snapshot_name,
            snapshot_path
        );

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
            let shards_holder = self.shards_holder.read().await;
            // Create snapshot of each shard
            for (shard_id, replica_set) in shards_holder.get_shards() {
                let shard_snapshot_path =
                    shard_versioning::versioned_shard_path(Path::new(""), shard_id, 0);

                // If node is listener, we can save whatever currently is in the storage
                let save_wal = self.shared_storage_config.node_type != NodeType::Listener;
                replica_set
                    .create_snapshot(
                        snapshot_temp_temp_dir.path(),
                        &tar.descend(&shard_snapshot_path)?,
                        SnapshotFormat::Regular,
                        save_wal,
                    )
                    .await
                    .map_err(|err| {
                        CollectionError::service_error(format!("failed to create snapshot: {err}"))
                    })?;
            }
        }

        // Save collection config and version
        tar.append_data(
            CollectionVersion::current_raw().as_bytes().to_vec(),
            Path::new(io::storage_version::VERSION_FILE),
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
        snapshot_path: &Path,
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        // decompress archive
        let mut ar = open_snapshot_archive_with_validation(snapshot_path)?;
        ar.unpack(target_dir)?;

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
                    shard_key_mapping
                        .values()
                        .flat_map(|v| v.iter())
                        .copied()
                        .collect()
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
            let shard_path = shard_versioning::versioned_shard_path(target_dir, shard_id, 0);
            let shard_config_opt = ShardConfig::load(&shard_path)?;
            if let Some(shard_config) = shard_config_opt {
                match shard_config.r#type {
                    shard_config::ShardType::Local => LocalShard::restore_snapshot(&shard_path)?,
                    shard_config::ShardType::Remote { .. } => {
                        RemoteShard::restore_snapshot(&shard_path)
                    }
                    shard_config::ShardType::Temporary => {}
                    shard_config::ShardType::ReplicaSet { .. } => {
                        ShardReplicaSet::restore_snapshot(
                            &shard_path,
                            this_peer_id,
                            is_distributed,
                        )?
                    }
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
        shard_id: ShardId,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        // `ShardHolder::recover_local_shard_from` is *not* cancel safe
        // (see `ShardReplicaSet::restore_local_replica_from`)
        self.shards_holder
            .read()
            .await
            .recover_local_shard_from(snapshot_shard_path, shard_id, cancel)
            .await
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
        self.shards_holder
            .read()
            .await
            .create_shard_snapshot(&self.snapshots_path, &self.name(), shard_id, temp_dir)
            .await
    }

    pub async fn stream_shard_snapshot(
        &self,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotStream> {
        let shard = OwnedRwLockReadGuard::try_map(
            Arc::clone(&self.shards_holder).read_owned().await,
            |x| x.get_shard(shard_id),
        )
        .map_err(|_| shard_not_found_error(shard_id))?;

        ShardHolder::stream_shard_snapshot(shard, &self.name(), shard_id, temp_dir).await
    }

    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn restore_shard_snapshot(
        &self,
        shard_id: ShardId,
        snapshot_path: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
        temp_dir: &Path,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<()> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        // `ShardHolder::restore_shard_snapshot` is *not* cancel safe
        // (see `ShardReplicaSet::restore_local_replica_from`)
        self.shards_holder
            .read()
            .await
            .restore_shard_snapshot(
                snapshot_path,
                &self.name(),
                shard_id,
                this_peer_id,
                is_distributed,
                temp_dir,
                cancel,
            )
            .await
    }

    pub async fn assert_shard_exists(&self, shard_id: ShardId) -> CollectionResult<()> {
        self.shards_holder
            .read()
            .await
            .assert_shard_exists(shard_id)
    }
}
