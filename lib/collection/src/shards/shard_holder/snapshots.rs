//! Creating, streaming, listing and restoring shard snapshots.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::tar_ext::BuilderExt;
use common::tar_unpack::tar_unpack_file;
use fs_err as fs;
use fs_err::File;
use futures::{Future, TryStreamExt as _};
use segment::types::SnapshotFormat;
use segment::utils::fs::move_all;
use shard::snapshots::snapshot_data::SnapshotData;
use shard::snapshots::snapshot_manifest::{RecoveryType, SnapshotManifest};
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::SyncIoBridge;

use super::{ShardHolder, shard_not_found_error};
use crate::common::snapshot_stream::SnapshotStream;
use crate::common::timeout_writer::TimeoutWriter;
use crate::operations::snapshot_ops::{SnapshotDescription, unique_snapshot_timestamp};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::recovery_guard::RecoveryProgressHandle;
use crate::shards::transfer::RecoveryStage;

/// Abort a streamed snapshot if its consumer reads no data for this long.
///
/// The streaming write holds a read lock on the shard's segment holder and
/// occupies a blocking thread, so a stalled consumer must not be allowed to
/// block it forever. See [`TimeoutWriter`].
const SNAPSHOT_STREAM_WRITE_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Pipe and read-buffer size for streaming shard snapshots.
/// A small pipe makes the tar writer and the HTTP body ping-pong in tiny chunks.
const SNAPSHOT_STREAM_BUFFER_SIZE: usize = 4 * 1024 * 1024;

impl ShardHolder {
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn list_shard_snapshots(
        &self,
        snapshots_path: &Path,
        shard_id: ShardId,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        self.assert_shard_is_local(shard_id).await?;

        let snapshots_path = Self::snapshots_path_for_shard_unchecked(snapshots_path, shard_id);

        let shard = self
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;
        let snapshot_manager = shard.get_snapshots_storage_manager()?;
        snapshot_manager.list_snapshots(&snapshots_path).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn create_shard_snapshot(
        shard: &ShardReplicaSet,
        snapshots_path: &Path,
        collection_name: &str,
        shard_id: ShardId,
        temp_dir: &Path,
    ) -> CollectionResult<impl Future<Output = CollectionResult<SnapshotDescription>> + use<>> {
        // - `snapshot_temp_dir` and `temp_file` are handled by `tempfile`
        //   and would be deleted, if future is canceled

        let snapshot_file_name = format!(
            "{collection_name}-shard-{shard_id}-{}.snapshot",
            unique_snapshot_timestamp(),
        );

        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-temp-"))
            .tempdir_in(temp_dir)?;

        let temp_file = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-"))
            .suffix(".tar")
            .tempfile_in(temp_dir)?;

        let snapshots_path = snapshots_path.to_path_buf();
        let snapshot_manager = shard.get_snapshots_storage_manager()?;

        let tar = BuilderExt::new_seekable_owned(File::create(temp_file.path())?);

        let snapshot_creator = shard
            .create_snapshot(
                snapshot_temp_dir.path(),
                tar.clone(),
                SnapshotFormat::Regular,
                None,
                false,
                true,
            )
            .await?;

        let future = async move {
            snapshot_creator.await?;

            let snapshot_temp_dir_path = snapshot_temp_dir.path().to_path_buf();
            if let Err(err) = snapshot_temp_dir.close() {
                log::error!(
                    "Failed to remove temporary directory {}: {err}",
                    snapshot_temp_dir_path.display(),
                );
            }

            tar.finish().await?;

            let snapshot_path =
                Self::shard_snapshot_path_unchecked(&snapshots_path, shard_id, snapshot_file_name)?;

            let snapshot_description = snapshot_manager
                .store_file(temp_file.path(), &snapshot_path)
                .await;
            if snapshot_description.is_ok() {
                let _ = temp_file.keep();
            }
            snapshot_description
        };

        Ok(future)
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn stream_shard_snapshot(
        shard: Arc<ShardReplicaSet>,
        collection_name: &str,
        shard_id: ShardId,
        manifest: Option<SnapshotManifest>,
        temp_dir: &Path,
    ) -> CollectionResult<SnapshotStream> {
        // - `snapshot_temp_dir` and `temp_file` are handled by `tempfile`
        //   and would be deleted, if future is cancelled

        let snapshot_file_name = format!(
            "{collection_name}-shard-{shard_id}-{}.snapshot",
            unique_snapshot_timestamp(),
        );

        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!("{snapshot_file_name}-temp-"))
            .tempdir_in(temp_dir)?;

        let (read_half, write_half) = tokio::io::duplex(SNAPSHOT_STREAM_BUFFER_SIZE);

        // Abort the snapshot if the consumer stops draining the stream. This
        // write holds a read lock on the shard's segment holder and occupies a
        // blocking thread; without a timeout, a stalled consumer (e.g. a slow or
        // hung client) would block the segment holder and leak the thread
        // indefinitely, wedging the whole shard.
        let write_half = TimeoutWriter::new(write_half, SNAPSHOT_STREAM_WRITE_IDLE_TIMEOUT);

        let tar = BuilderExt::new_streaming_owned(SyncIoBridge::new(write_half));

        let snapshot_creator = shard
            .create_snapshot(
                snapshot_temp_dir.path(),
                tar.clone(),
                SnapshotFormat::Streamable,
                manifest,
                false,
                true,
            )
            .await?;

        drop(shard);

        let future = async move {
            snapshot_creator.await?;

            let snapshot_temp_dir_path = snapshot_temp_dir.path().to_path_buf();
            if let Err(err) = snapshot_temp_dir.close() {
                log::error!(
                    "Failed to remove temporary directory {}: {err}",
                    snapshot_temp_dir_path.display(),
                );
            }

            tar.finish().await?;

            CollectionResult::Ok(())
        };

        tokio::spawn(async move {
            if let Err(err) = future.await {
                log::error!("Failed to stream shard snapshot: {err}");
            }
        });

        Ok(SnapshotStream::new_stream(
            FramedRead::with_capacity(read_half, BytesCodec::new(), SNAPSHOT_STREAM_BUFFER_SIZE)
                .map_ok(|bytes| bytes.freeze()),
            Some(snapshot_file_name),
        ))
    }

    /// Extract a shard snapshot and prepare its files for recovering the local replica.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    #[expect(clippy::too_many_arguments)]
    pub async fn prepare_shard_snapshot(
        snapshot_data: SnapshotData,
        collection_name: &str,
        shard_id: ShardId,
        this_peer_id: PeerId,
        is_distributed: bool,
        temp_dir: &Path,
        recovery_progress: Option<&RecoveryProgressHandle>,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<tempfile::TempDir> {
        if !temp_dir.exists() {
            fs::create_dir_all(temp_dir)?;
        }

        let snapshot_temp_dir = tempfile::Builder::new()
            .prefix(&format!("{collection_name}-shard-{shard_id}"))
            .tempdir_in(temp_dir)?;

        // Set unpacking stage
        if let Some(recovery_progress) = recovery_progress {
            recovery_progress.lock().set_stage(RecoveryStage::Unpacking);
        }

        #[cfg(test)]
        super::snapshot_preparation_tests::pause_preparation(temp_dir).await;

        let extract = cancel::blocking::spawn_cancel_on_token(
            cancel.child_token(),
            move |cancel| -> CollectionResult<_> {
                // TODO: Make `tar_unpack_file`/`move_all` cancellable?
                match snapshot_data {
                    SnapshotData::Packed(snapshot_path) => {
                        if cancel.is_cancelled() {
                            return Err(cancel::Error::Cancelled.into());
                        }

                        tar_unpack_file(&snapshot_path, snapshot_temp_dir.path())?;
                        snapshot_path.close()?;
                    }

                    SnapshotData::Unpacked(snapshot_dir) => {
                        move_all(snapshot_dir.path(), snapshot_temp_dir.path())?;
                    }
                }

                #[cfg(test)]
                super::snapshot_preparation_tests::pause_extraction(snapshot_temp_dir.path());

                if cancel.is_cancelled() {
                    return Err(cancel::Error::Cancelled.into());
                }

                ShardReplicaSet::restore_snapshot(
                    snapshot_temp_dir.path(),
                    this_peer_id,
                    is_distributed,
                )?;

                common::fs::bulk_sync_dir(snapshot_temp_dir.path())?;
                Ok(snapshot_temp_dir)
            },
        );

        extract.await?
    }

    /// Replace or merge the local replica's files with files from the extracted snapshot directory,
    /// then load the recovered shard.
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancel safe.
    pub async fn restore_shard_snapshot(
        &self,
        snapshot_temp_dir: &Path,
        recovery_type: RecoveryType,
        collection_path: &Path,
        shard_id: ShardId,
        recovery_progress: Option<RecoveryProgressHandle>,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<()> {
        // Set restoring stage
        if let Some(recovery_progress) = &recovery_progress {
            recovery_progress.lock().set_stage(RecoveryStage::Restoring);
        }

        // `ShardHolder::recover_local_shard_from` is *not* cancel safe
        // (see `ShardReplicaSet::restore_local_replica_from`)
        let recovered = self
            .recover_local_shard_from(
                snapshot_temp_dir,
                recovery_type,
                collection_path,
                shard_id,
                cancel,
            )
            .await?;

        if !recovered {
            return Err(CollectionError::bad_request("Invalid snapshot"));
        }

        if recovery_type.is_partial() {
            self.update_payload_index_schema().await.map_err(|err| {
                CollectionError::service_error(format!(
                    "failed to update payload index schema after recovering partial snapshot: {err}"
                ))
            })?;
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
        collection_path: &Path,
        shard_id: ShardId,
        cancel: cancel::CancellationToken,
    ) -> CollectionResult<bool> {
        // TODO:
        //   Check that shard snapshot is compatible with the collection
        //   (see `VectorsConfig::check_compatible_with_segment_config`)

        let replica_set = self
            .get_shard(shard_id)
            .ok_or_else(|| shard_not_found_error(shard_id))?;

        // `ShardReplicaSet::restore_local_replica_from` is *not* cancel safe
        let res = replica_set
            .restore_local_replica_from(snapshot_shard_path, recovery_type, collection_path, cancel)
            .await?;

        Ok(res)
    }

    pub fn try_take_partial_snapshot_recovery_lock(
        &self,
        shard_id: ShardId,
        recovery_type: RecoveryType,
    ) -> CollectionResult<Option<tokio::sync::OwnedRwLockWriteGuard<()>>> {
        match recovery_type {
            RecoveryType::Full => Ok(None),
            RecoveryType::Partial => {
                let lock = self
                    .get_shard(shard_id)
                    .ok_or_else(|| shard_not_found_error(shard_id))?
                    .try_take_partial_snapshot_recovery_lock()?;

                Ok(Some(lock))
            }
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn get_shard_snapshot_path(
        &self,
        snapshots_path: &Path,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        self.assert_shard_is_local_or_queue_proxy(shard_id).await?;
        Self::shard_snapshot_path_unchecked(snapshots_path, shard_id, snapshot_file_name)
    }

    fn snapshots_path_for_shard_unchecked(snapshots_path: &Path, shard_id: ShardId) -> PathBuf {
        snapshots_path.join(format!("shards/{shard_id}"))
    }

    fn shard_snapshot_path_unchecked(
        snapshots_path: &Path,
        shard_id: ShardId,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        let snapshots_path = Self::snapshots_path_for_shard_unchecked(snapshots_path, shard_id);

        let snapshot_file_name = snapshot_file_name.as_ref();

        if snapshot_file_name.file_name() != Some(snapshot_file_name.as_os_str()) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {}",
                snapshot_file_name.display(),
            )));
        }

        let snapshot_path = snapshots_path.join(snapshot_file_name);

        Ok(snapshot_path)
    }
}
