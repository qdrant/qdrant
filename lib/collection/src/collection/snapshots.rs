use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

use io::file_operations::read_json;
use segment::common::version::StorageVersion as _;
use snapshot_manager::file::SnapshotFile;
use snapshot_manager::{SnapshotDescription, SnapshotManager};
use tempfile::TempPath;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::Collection;
use crate::collection::CollectionVersion;
use crate::common::sha_256::hash_file;
use crate::config::{CollectionConfig, ShardingMethod};
use crate::operations::types::{CollectionError, CollectionResult, NodeType};
use crate::shards::local_shard::LocalShard;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_config::{self, ShardConfig};
use crate::shards::shard_holder::{ShardKeyMapping, SHARD_KEY_MAPPING_FILE};
use crate::shards::shard_versioning;

impl Collection {
    pub async fn list_snapshots(&self) -> CollectionResult<Vec<SnapshotDescription>> {
        Ok(self
            .snapshot_manager
            .do_list_collection_snapshots(&self.name())
            .await?)
    }

    pub async fn create_temp_snapshot(
        &self,
        global_temp_dir: &Path,
        this_peer_id: PeerId,
    ) -> CollectionResult<(SnapshotFile, TempPath, TempPath)> {
        let snapshot_name = format!(
            "{}-{this_peer_id}-{}.snapshot",
            self.name(),
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S"),
        );

        let base = self.snapshot_manager.temp_path();
        let snapshot = SnapshotFile::new_full(&snapshot_name); // NOTE: new_full is used here to stay in an existing directory (./snapshots/tmp/)

        // Final location of snapshot
        let snapshot_path = snapshot.get_path(&base);
        tokio::fs::create_dir_all(snapshot_path.parent().unwrap()).await?;
        log::info!("Creating collection snapshot {}", snapshot_name);

        // Dedicated temporary directory for this snapshot (deleted on drop)
        let snapshot_temp_target_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_name}-target-"))
            .tempdir_in(global_temp_dir)?;

        let snapshot_temp_target_dir_path = snapshot_temp_target_dir.path().to_path_buf();
        // Create snapshot of each shard
        {
            let snapshot_temp_temp_dir = tempfile::Builder::new()
                .prefix(&format!("{snapshot_name}-temp-"))
                .tempdir_in(global_temp_dir)?;
            let shards_holder = self.shards_holder.read().await;
            // Create snapshot of each shard
            for (shard_id, replica_set) in shards_holder.get_shards() {
                let shard_snapshot_path = shard_versioning::versioned_shard_path(
                    &snapshot_temp_target_dir_path,
                    *shard_id,
                    0,
                );
                fs::create_dir_all(&shard_snapshot_path).await?;
                // If node is listener, we can save whatever currently is in the storage
                let save_wal = self.shared_storage_config.node_type != NodeType::Listener;
                replica_set
                    .create_snapshot(
                        snapshot_temp_temp_dir.path(),
                        &shard_snapshot_path,
                        save_wal,
                    )
                    .await?;
            }
        }

        // Save collection config and version
        CollectionVersion::save(&snapshot_temp_target_dir_path)?;
        self.collection_config
            .read()
            .await
            .save(&snapshot_temp_target_dir_path)?;

        self.shards_holder
            .read()
            .await
            .save_key_mapping_to_dir(&snapshot_temp_target_dir_path)?;

        let payload_index_schema_tmp_path =
            Self::payload_index_file(&snapshot_temp_target_dir_path);
        self.payload_index_schema
            .save_to(&payload_index_schema_tmp_path)?;

        // Dedicated temporary file for archiving this snapshot (deleted on drop)
        let mut snapshot_temp_arc_file = tempfile::Builder::new()
            .prefix(&format!("{snapshot_name}-arc-"))
            .tempfile_in(global_temp_dir)?;

        // Archive snapshot folder into a single file
        log::debug!("Archiving snapshot {snapshot_temp_target_dir_path:?}");
        let archiving = tokio::task::spawn_blocking(move || -> CollectionResult<_> {
            let mut builder = tar::Builder::new(snapshot_temp_arc_file.as_file_mut());
            // archive recursively collection directory `snapshot_path_with_arc_extension` into `snapshot_path`
            builder.append_dir_all(".", &snapshot_temp_target_dir_path)?;
            builder.finish()?;
            drop(builder);
            // return ownership of the file
            Ok(snapshot_temp_arc_file)
        });
        snapshot_temp_arc_file = archiving.await??;

        // Move snapshot to permanent location.
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, because copy is not atomic.
        // So we copy to the final location with a temporary name and then rename atomically.
        let snapshot_path_tmp_move = snapshot_path.with_extension("tmp");

        // Ensure that the temporary file is deleted on error
        let snapshot_file = TempPath::from_path(&snapshot_path_tmp_move);
        fs::copy(&snapshot_temp_arc_file.path(), &snapshot_path_tmp_move).await?;

        // compute and store the file's checksum before the final snapshot file is saved
        // to avoid making snapshot available without checksum
        let checksum_path = snapshot.get_checksum_path(&base);
        let checksum = hash_file(&snapshot_path_tmp_move).await?;
        let checksum_file = tempfile::TempPath::from_path(&checksum_path);
        let mut file = tokio::fs::File::create(checksum_path.as_path()).await?;
        file.write_all(checksum.as_bytes()).await?;

        // Snapshot files are ready now, hand them off to the manager
        let snapshot = SnapshotFile::new_collection(snapshot.name(), self.name());

        Ok((snapshot, snapshot_file, checksum_file))
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
    /// returns: Result<(SnapshotFile, SnapshotDescription), CollectionError>
    pub async fn create_snapshot(
        &self,
        global_temp_dir: &Path,
        this_peer_id: PeerId,
    ) -> CollectionResult<(SnapshotFile, SnapshotDescription)> {
        let (snapshot, snapshot_file, checksum_file) = self
            .create_temp_snapshot(global_temp_dir, this_peer_id)
            .await?;

        self.snapshot_manager
            .save_snapshot(&snapshot, snapshot_file, checksum_file)
            .await?;

        log::info!("Collection snapshot {} completed", snapshot.name());
        Ok((
            snapshot.clone(),
            self.snapshot_manager
                .get_snapshot_description(&snapshot)
                .await?,
        ))
    }

    /// Restore collection from snapshot
    pub async fn restore_snapshot(
        snapshot_manager: SnapshotManager,
        snapshot: &SnapshotFile,
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        let ar = snapshot_manager.get_snapshot_file(snapshot).await?;

        let target_dir = target_dir.to_owned();
        tokio::task::spawn_blocking(move || {
            // Unpack snapshot collection to the target folder
            Self::_restore_snapshot(&target_dir, this_peer_id, is_distributed, ar)
        })
        .await??;

        Ok(())
    }

    /// Restore collection from snapshot
    ///
    /// This method performs blocking IO.
    pub fn restore_snapshot_sync(
        snapshot_manager: SnapshotManager,
        snapshot: &SnapshotFile,
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
    ) -> CollectionResult<()> {
        Self::_restore_snapshot(
            target_dir,
            this_peer_id,
            is_distributed,
            snapshot_manager.get_snapshot_file_sync(snapshot)?,
        )
    }

    fn _restore_snapshot(
        target_dir: &Path,
        this_peer_id: PeerId,
        is_distributed: bool,
        (archive_file, archive_file_keep): (File, Option<TempPath>),
    ) -> CollectionResult<()> {
        // decompress archive

        let mut ar = tar::Archive::new(archive_file);
        ar.unpack(target_dir)?;

        let config = CollectionConfig::load(target_dir)?;
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

        // MOGTODO: is this needed? when would it be dropped?
        if let Some(temp) = archive_file_keep {
            let _ = temp.close();
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
            .list_shard_snapshots(&self.snapshot_manager, &self.name(), shard_id)
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
            .create_shard_snapshot(&self.snapshot_manager, &self.name(), shard_id, temp_dir)
            .await
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
            .await
    }

    pub async fn get_shard_snapshot(
        &self,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<SnapshotFile> {
        self.shards_holder
            .read()
            .await
            .get_shard_snapshot(&self.name(), shard_id, snapshot_file_name)
            .await
    }
}
