use std::collections::HashSet;
use std::path::Path;
use std::{fs, io};

use common::tar_ext;
use segment::data_types::segment_manifest::{SegmentManifest, SegmentManifests};
use segment::segment::destroy_rocksdb;
use segment::segment::snapshot::{PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE, ROCKS_DB_VIRT_FILE};
use segment::segment_constructor::PAYLOAD_INDEX_PATH;
use segment::types::SnapshotFormat;

use super::{REPLICA_STATE_FILE, ReplicaSetState, ReplicaState, ShardReplicaSet};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::save_on_disk::SaveOnDisk;
use crate::shards::dummy_shard::DummyShard;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard::{PeerId, Shard};
use crate::shards::shard_config::ShardConfig;
use crate::shards::shard_initializing_flag_path;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RecoveryType {
    Full,
    Partial,
}

impl RecoveryType {
    pub fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }

    pub fn is_partial(self) -> bool {
        matches!(self, Self::Partial)
    }
}

impl ShardReplicaSet {
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        save_wal: bool,
    ) -> CollectionResult<()> {
        let local_read = self.local.read().await;

        if let Some(local) = &*local_read {
            local
                .create_snapshot(temp_path, tar, format, save_wal)
                .await?
        }

        self.replica_state
            .save_to_tar(tar, REPLICA_STATE_FILE)
            .await?;

        let shard_config = ShardConfig::new_replica_set();
        shard_config.save_to_tar(tar).await?;
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
        recovery_type: RecoveryType,
        collection_path: &Path,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // `local.take()` call and `restore` task have to be executed as a single transaction

        if !LocalShard::check_data(replica_path) {
            return Ok(false);
        }

        let segments_path = LocalShard::segments_path(replica_path);

        let mut snapshot_segments = HashSet::new();
        let mut snapshot_manifests = SegmentManifests::default();

        for segment_entry in segments_path.read_dir()? {
            let segment_path = segment_entry?.path();

            if !segment_path.is_dir() {
                log::warn!(
                    "segment path {} in extracted snapshot {} is not a directory",
                    segment_path.display(),
                    replica_path.display(),
                );

                continue;
            }

            let segment_id = segment_path
                .file_name()
                .and_then(|segment_id| segment_id.to_str())
                .expect("segment path ends with a valid segment id");

            let added = snapshot_segments.insert(segment_id.to_string());
            debug_assert!(added);

            let manifest_path = segment_path.join("segment_manifest.json");

            if recovery_type.is_full() {
                if manifest_path.exists() {
                    return Err(CollectionError::bad_request(format!(
                        "invalid shard snapshot: \
                         segment {segment_id} contains segment manifest; \
                         ensure you are not recovering partial snapshot on shard snapshot endpoint",
                    )));
                }

                continue;
            }

            if !manifest_path.exists() {
                return Err(CollectionError::bad_request(format!(
                    "invalid partial snapshot: \
                     segment {segment_id} does not contain segment manifest; \
                     ensure you are not recovering shard snapshot on partial snapshot endpoint",
                )));
            }

            let manifest = fs::File::open(&manifest_path).map_err(|err| {
                CollectionError::service_error(format!(
                    "failed to open segment {segment_id} manifest: {err}",
                ))
            })?;

            let manifest = io::BufReader::new(manifest);

            let manifest: SegmentManifest = serde_json::from_reader(manifest).map_err(|err| {
                CollectionError::bad_request(format!(
                    "failed to deserialize segment {segment_id} manifest: {err}",
                ))
            })?;

            if segment_id != manifest.segment_id {
                return Err(CollectionError::bad_request(format!(
                    "invalid partial snapshot: \
                     segment {segment_id} contains segment manifest with segment ID {}",
                    manifest.segment_id,
                )));
            }

            let added = snapshot_manifests.add(manifest);
            debug_assert!(added);
        }

        snapshot_manifests.validate().map_err(|err| {
            CollectionError::bad_request(format!("invalid partial snapshot: {err}"))
        })?;

        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let mut local = cancel::future::cancel_on_token(cancel.clone(), self.local.write()).await?;

        // set shard_id initialization flag
        // the file is removed after full recovery to indicate a well-formed shard
        // for example: some of the files may go missing if node gets killed during shard directory move/replace
        let shard_flag = shard_initializing_flag_path(collection_path, self.shard_id);
        let flag_file = tokio::fs::File::create(&shard_flag).await?;
        flag_file.sync_all().await?;

        // Check `cancel` token one last time before starting non-cancellable section
        if cancel.is_cancelled() {
            return Err(cancel::Error::Cancelled.into());
        }

        let local_manifests = match local.take() {
            _ if snapshot_manifests.is_empty() => None,

            Some(shard) => {
                let local_manifests = shard.segment_manifests();

                match local_manifests {
                    Ok(local_manifests) => {
                        local_manifests.validate().map_err(|err| {
                            CollectionError::service_error(format!(
                                "failed to restore partial shard snapshot for shard {}:{}: \
                                 local shard produces invalid segment manifests: \
                                 {err}",
                                self.collection_id, self.shard_id,
                            ))
                        })?;

                        Some(local_manifests)
                    }

                    Err(err) => {
                        let _ = local.insert(shard);

                        return Err(CollectionError::service_error(format!(
                            "failed to restore partial shard snapshot for shard {}:{}: \
                             failed to collect segment manifests: \
                             {err}",
                            self.collection_id, self.shard_id,
                        )));
                    }
                }
            }

            None => {
                return Err(CollectionError::bad_request(format!(
                    "failed to restore partial shard snapshot for shard {}:{}: \
                     shard does not exist on peer {}",
                    self.collection_id,
                    self.shard_id,
                    self.this_peer_id(),
                )));
            }
        };

        // Try to restore local replica from specified shard snapshot directory
        let restore = async {
            if let Some(local_manifests) = local_manifests {
                for (segment_id, local_manifest) in local_manifests.iter() {
                    let segment_path = segments_path.join(segment_id);

                    // Delete local segment, if it's not present in partial snapshot
                    let Some(snapshot_manifest) = snapshot_manifests.get(segment_id) else {
                        tokio::fs::remove_dir_all(&segment_path).await?;
                        continue;
                    };

                    for (file, &local_version) in &local_manifest.file_versions {
                        let snapshot_version = snapshot_manifest.file_versions.get(file).copied();

                        let is_removed = snapshot_version.is_none();

                        let is_outdated = snapshot_version.is_none_or(|snapshot_version| {
                            let local_version =
                                local_version.or_segment_version(local_manifest.segment_version);

                            let snapshot_version = snapshot_version
                                .or_segment_version(snapshot_manifest.segment_version);

                            // Compare versions to determine local file is outdated relative to snapshot
                            local_version < snapshot_version
                        });

                        let is_rocksdb = file == Path::new(ROCKS_DB_VIRT_FILE);
                        let is_payload_index_rocksdb =
                            file == Path::new(PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE);

                        if is_removed {
                            // If `file` is a regular file, delete it from disk, if it was
                            // *removed* from the snapshot

                            if !is_rocksdb && !is_payload_index_rocksdb {
                                tokio::fs::remove_file(segment_path.join(file)).await?;
                            }
                        } else if is_outdated {
                            // If `file` is a RocksDB "virtual" file, remove RocksDB from disk,
                            // if it was *updated* in or *removed* from the snapshot

                            if is_rocksdb {
                                destroy_rocksdb(&segment_path)?;
                            } else if is_payload_index_rocksdb {
                                destroy_rocksdb(&segment_path.join(PAYLOAD_INDEX_PATH))?;
                            }
                        }
                    }
                }
            } else {
                // Remove shard data but not configuration files
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
                self.payload_index_schema.clone(),
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
                tokio::fs::remove_file(&shard_flag).await?;
                Ok(true)
            }

            Err(restore_err) => {
                // Initialize "dummy" replica
                local.replace(Shard::Dummy(DummyShard::new(
                    "Failed to restore local replica",
                )));

                // Mark local replica as Dead since it's dummy and dirty
                {
                    let replica_state = self.replica_state.read();
                    self.add_locally_disabled(&replica_state, self.this_peer_id(), None);
                }

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
}
