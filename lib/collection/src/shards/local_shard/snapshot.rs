use std::num::NonZeroUsize;
use std::path::Path;

use common::tar_ext;
use segment::data_types::manifest::SnapshotManifest;
use segment::segment::Segment;
use segment::types::SnapshotFormat;
use shard::segment_holder::SegmentHolder;
use tokio::sync::oneshot;
use wal::{Wal, WalOptions};

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::{LocalShard, LocalShardClocks, SEGMENTS_PATH, WAL_PATH};
use crate::update_handler::UpdateSignal;
use crate::wal_delta::LockedWal;

impl LocalShard {
    pub fn snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        self.segments()
            .read()
            .snapshot_manifest()
            .map_err(CollectionError::from)
    }

    pub fn restore_snapshot(snapshot_path: &Path) -> CollectionResult<()> {
        log::info!("Restoring shard snapshot {}", snapshot_path.display());
        // Read dir first as the directory contents would change during restore
        let entries = std::fs::read_dir(LocalShard::segments_path(snapshot_path))?
            .collect::<Result<Vec<_>, _>>()?;

        // Filter out hidden entries
        let entries = entries.into_iter().filter(|entry| {
            let is_hidden = entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.starts_with('.'));
            if is_hidden {
                log::debug!(
                    "Ignoring hidden segment in local shard during snapshot recovery: {}",
                    entry.path().display(),
                );
            }
            !is_hidden
        });

        for entry in entries {
            Segment::restore_snapshot_in_place(&entry.path())?;
        }

        Ok(())
    }

    /// Create snapshot for local shard into `target_path`
    pub async fn create_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<SnapshotManifest>,
        save_wal: bool,
    ) -> CollectionResult<()> {
        let segments = self.segments.clone();
        let wal = self.wal.wal.clone();

        if !save_wal {
            // If we are not saving WAL, we still need to make sure that all submitted by this point
            // updates have made it to the segments. So we use the Plunger to achieve that.
            // It will notify us when all submitted updates so far have been processed.
            let (tx, rx) = oneshot::channel();
            let plunger = UpdateSignal::Plunger(tx);
            self.update_sender.load().send(plunger).await?;
            rx.await?;
        }

        let segments_path = Self::segments_path(&self.path);
        let segment_config = self
            .collection_config
            .read()
            .await
            .to_base_segment_config()?;
        let payload_index_schema = self.payload_index_schema.clone();
        let temp_path = temp_path.to_owned();

        let tar_c = tar.clone();
        tokio::task::spawn_blocking(move || {
            // Do not change segments while snapshotting
            SegmentHolder::snapshot_all_segments(
                segments.clone(),
                &segments_path,
                Some(segment_config),
                payload_index_schema.clone(),
                &temp_path,
                &tar_c.descend(Path::new(SEGMENTS_PATH))?,
                format,
                manifest.as_ref(),
            )?;

            if save_wal {
                // snapshot all shard's WAL
                Self::snapshot_wal(wal, &tar_c)
            } else {
                Self::snapshot_empty_wal(wal, &temp_path, &tar_c)
            }
        })
        .await??;

        LocalShardClocks::archive_data(&self.path, tar).await?;

        Ok(())
    }

    /// Create empty WAL which is compatible with currently stored data
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn snapshot_empty_wal(
        wal: LockedWal,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        let (segment_capacity, latest_op_num) = {
            let wal_guard = wal.blocking_lock();
            (wal_guard.segment_capacity(), wal_guard.last_index())
        };

        let temp_dir = tempfile::tempdir_in(temp_path).map_err(|err| {
            CollectionError::service_error(format!(
                "Can not create temporary directory for WAL: {err}",
            ))
        })?;

        Wal::generate_empty_wal_starting_at_index(
            temp_dir.path(),
            &WalOptions {
                segment_capacity,
                segment_queue_len: 0,
                retain_closed: NonZeroUsize::new(1).unwrap(),
            },
            latest_op_num,
        )
        .map_err(|err| {
            CollectionError::service_error(format!("Error while create empty WAL: {err}"))
        })?;

        tar.blocking_append_dir_all(temp_dir.path(), Path::new(WAL_PATH))
            .map_err(|err| {
                CollectionError::service_error(format!("Error while archiving WAL: {err}"))
            })
    }

    /// snapshot WAL
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn snapshot_wal(wal: LockedWal, tar: &tar_ext::BuilderExt) -> CollectionResult<()> {
        // lock wal during snapshot
        let mut wal_guard = wal.blocking_lock();
        wal_guard.flush()?;
        let source_wal_path = wal_guard.path();

        let tar = tar.descend(Path::new(WAL_PATH))?;
        for entry in std::fs::read_dir(source_wal_path).map_err(|err| {
            CollectionError::service_error(format!("Can't read WAL directory: {err}",))
        })? {
            let entry = entry.map_err(|err| {
                CollectionError::service_error(format!("Can't read WAL directory: {err}",))
            })?;

            if entry.file_name() == ".wal" {
                // This sentinel file is used for WAL locking. Trying to archive
                // or open it will cause the following error on Windows:
                // > The process cannot access the file because another process
                // > has locked a portion of the file. (os error 33)
                // https://github.com/qdrant/wal/blob/7c9202d0874/src/lib.rs#L125-L145
                continue;
            }

            tar.blocking_append_file(&entry.path(), Path::new(&entry.file_name()))
                .map_err(|err| {
                    CollectionError::service_error(format!("Error while archiving WAL: {err}"))
                })?;
        }
        Ok(())
    }
}
