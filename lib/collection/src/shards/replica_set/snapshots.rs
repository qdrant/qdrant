use std::collections::HashSet;
use std::ops::Deref as _;
use std::path::Path;
use std::{fs, io};

use common::tar_ext;
use segment::data_types::segment_manifest::{SegmentManifest, SegmentManifests};
use segment::segment::destroy_rocksdb;
use segment::segment::snapshot::{PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE, ROCKS_DB_VIRT_FILE};
use segment::types::SnapshotFormat;

use super::{REPLICA_STATE_FILE, ReplicaSetState, ReplicaState, ShardReplicaSet};
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
                .expect("segment path ends with a valid segment id segment id");

            let segment_added = snapshot_segments.insert(segment_id.to_string());
            debug_assert!(segment_added);

            let manifest_path = segment_path.join("segment_manifest.json");

            if !manifest_path.exists() {
                continue;
            }

            let manifest: SegmentManifest =
                serde_json::from_reader(io::BufReader::new(fs::File::open(manifest_path)?))?;

            let manifest_added = snapshot_manifests.add(manifest);
            debug_assert!(manifest_added);
        }

        debug_assert!(
            snapshot_manifests.is_empty() || snapshot_segments.len() == snapshot_manifests.len()
        );

        for (segment_id, segment_manifest) in snapshot_manifests.iter() {
            debug_assert_eq!(segment_id, &segment_manifest.segment_id);
        }

        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let mut local = cancel::future::cancel_on_token(cancel.clone(), self.local.write()).await?;

        // Check `cancel` token one last time before starting non-cancellable section
        if cancel.is_cancelled() {
            return Err(cancel::Error::Cancelled.into());
        }

        let local_manifests = match local.take() {
            _ if snapshot_manifests.is_empty() => None,

            // TODO: Generalize to handle both local and proxy shards 🤔
            Some(Shard::Local(shard)) => {
                let local_manifests = shard.segments().read().segment_manifests();

                match local_manifests {
                    Ok(local_manifests) => {
                        // TODO: Validate that all segments in `local_manifests` are *older* than segments in `snapshot_manifests` 😵‍💫

                        Some(local_manifests)
                    }

                    Err(err) => {
                        let _ = local.insert(Shard::Local(shard));

                        return Err(CollectionError::service_error(format!(
                            "failed to restore partial shard snapshot for shard {}:{}: \
                             failed to collect segment manifests for shard {}:{}: \
                             {err}",
                            self.collection_id, self.shard_id, self.collection_id, self.shard_id,
                        )));
                    }
                }
            }

            Some(shard) => {
                let proxy_type = shard.variant_name();

                let _ = local.insert(shard);

                return Err(CollectionError::bad_request(format!(
                    "failed to restore partial shard snapshot for shard {}:{}: \
                     shard {}:{} is a {proxy_type}",
                    self.collection_id, self.shard_id, self.collection_id, self.shard_id,
                )));
            }

            None => {
                return Err(CollectionError::bad_request(format!(
                    "failed to restore partial shard snapshot for shard {}:{}: \
                     shard {}:{} does not exist on peer {}",
                    self.collection_id,
                    self.shard_id,
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

                    let Some(snapshot_manifest) = snapshot_manifests.get(segment_id) else {
                        tokio::fs::remove_dir_all(&segment_path).await?;
                        continue;
                    };

                    for (file, &local_version) in &local_manifest.file_versions {
                        let is_rocksdb = file == Path::new(ROCKS_DB_VIRT_FILE)
                            || file == Path::new(PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE);

                        let snapshot_version = snapshot_manifest.file_versions.get(file).copied();

                        let is_outdated = snapshot_version.is_none_or(|snapshot_version| {
                            let local_version =
                                local_version.or_segment_version(local_manifest.segment_version);

                            let snapshot_version = snapshot_version
                                .or_segment_version(snapshot_manifest.segment_version);

                            local_version < snapshot_version
                        });

                        let is_removed = snapshot_version.is_none();

                        if is_rocksdb && is_outdated {
                            destroy_rocksdb(&segment_path)?;
                        } else if !is_rocksdb && is_removed {
                            tokio::fs::remove_file(segment_path.join(file)).await?;
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
                Ok(true)
            }

            Err(restore_err) => {
                // Initialize "dummy" replica
                local.replace(Shard::Dummy(DummyShard::new(
                    "Failed to restore local replica",
                )));

                // Mark this peer as "locally disabled"...
                //
                // `active_remote_shards` includes `Active` and `ReshardingScaleDown` replicas!
                let has_other_active_peers = self.active_remote_shards().is_empty();

                // ...if this peer is *not* the last active replica
                if has_other_active_peers {
                    let notify = self
                        .locally_disabled_peers
                        .write()
                        .disable_peer_and_notify_if_elapsed(self.this_peer_id(), None);

                    if notify {
                        self.notify_peer_failure_cb.deref()(
                            self.this_peer_id(),
                            self.shard_id,
                            None,
                        );
                    }
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
