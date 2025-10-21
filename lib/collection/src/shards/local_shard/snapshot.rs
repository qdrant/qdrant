use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err as fs;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SnapshotManifest;
use segment::entry::SegmentEntry;
use segment::segment::Segment;
use segment::types::{SegmentConfig, SnapshotFormat};
use shard::locked_segment::LockedSegment;
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::{LockedSegmentHolder, SegmentHolder};
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
        let entries = fs::read_dir(LocalShard::segments_path(snapshot_path))?
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
        let update_lock = self.update_operation_lock.clone();

        tokio::task::spawn_blocking(move || {
            // Do not change segments while snapshotting
            snapshot_all_segments(
                segments.clone(),
                &segments_path,
                Some(segment_config),
                payload_index_schema.clone(),
                &temp_path,
                &tar_c.descend(Path::new(SEGMENTS_PATH))?,
                format,
                manifest.as_ref(),
                update_lock,
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
        for entry in fs::read_dir(source_wal_path).map_err(|err| {
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

/// Take a snapshot of all segments into `snapshot_dir_path`
///
/// It is recommended to provide collection parameters. This function internally creates a
/// temporary segment, which will source the configuration from it.
///
/// Shortcuts at the first failing segment snapshot.
#[expect(clippy::too_many_arguments)]
pub fn snapshot_all_segments(
    segments: LockedSegmentHolder,
    segments_path: &Path,
    segment_config: Option<SegmentConfig>,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    temp_dir: &Path,
    tar: &tar_ext::BuilderExt,
    format: SnapshotFormat,
    manifest: Option<&SnapshotManifest>,
    // Update lock prevents segment operations during update.
    // For instance, we can't unproxy segments while update operation is in progress.
    update_lock: Arc<tokio::sync::RwLock<()>>,
) -> OperationResult<()> {
    // Snapshotting may take long-running read locks on segments blocking incoming writes, do
    // this through proxied segments to allow writes to continue.

    proxy_all_segments_and_apply(
        segments,
        segments_path,
        segment_config,
        payload_index_schema,
        |segment| {
            let read_segment = segment.read();
            read_segment.take_snapshot(temp_dir, tar, format, manifest)?;
            Ok(())
        },
        update_lock,
    )
}

/// Temporarily proxify all segments and apply function `f` to it.
///
/// Intended to smoothly accept writes while performing long-running read operations on each
/// segment, such as during snapshotting. It should prevent blocking reads on segments for any
/// significant amount of time.
///
/// This calls function `f` on all segments, but each segment is temporarily proxified while
/// the function is called.
///
/// All segments are proxified at the same time on start. That ensures each wrapped (proxied)
/// segment is kept at the same point in time. Each segment is unproxied one by one, right
/// after function `f` has been applied. That helps keeping proxies as shortlived as possible.
///
/// A read lock is kept during the whole process to prevent external actors from messing with
/// the segment holder while segments are in proxified state. That means no other actors can
/// take a write lock while this operation is running.
///
/// As part of this process, a new segment is created. All proxies direct their writes to this
/// segment. The segment is added to the collection if it has any operations, otherwise it is
/// deleted when all segments are unproxied again.
///
/// It is recommended to provide collection parameters. The segment configuration will be
/// sourced from it.
///
/// Before snapshotting all segments are forcefully flushed to ensure all data is persisted.
pub fn proxy_all_segments_and_apply<F>(
    segments: LockedSegmentHolder,
    segments_path: &Path,
    segment_config: Option<SegmentConfig>,
    payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>>,
    mut operation: F,
    update_lock: Arc<tokio::sync::RwLock<()>>,
) -> OperationResult<()>
where
    F: FnMut(&RwLock<dyn SegmentEntry>) -> OperationResult<()>,
{
    let segments_lock = segments.upgradable_read();

    // Proxy all segments
    // Proxied segments are sorted by flush ordering
    log::trace!("Proxying all shard segments to apply function");
    let (mut proxies, tmp_segment_id, mut segments_lock) = SegmentHolder::proxy_all_segments(
        segments_lock,
        segments_path,
        segment_config,
        payload_index_schema,
    )?;

    // Flush all pending changes of each segment, now wrapped segments won't change anymore
    segments_lock.flush_all(true, true)?;

    // Apply provided function
    log::trace!("Applying function on all proxied shard segments");
    let mut result = Ok(());
    let mut unproxied_segment_ids = Vec::with_capacity(proxies.len());

    // Proxied segments are sorted by flush ordering, important because the operation we apply
    // might explicitly flush segments
    for (segment_id, proxy_segment) in &proxies {
        // Get segment to snapshot
        let op_result = match proxy_segment {
            LockedSegment::Proxy(proxy_segment) => {
                let guard = proxy_segment.read();
                let segment = guard.wrapped_segment.get();
                // Call provided function on wrapped segment while holding guard to parent segment
                operation(segment)
            }
            // All segments to snapshot should be proxy, warn if this is not the case
            LockedSegment::Original(segment) => {
                debug_assert!(
                    false,
                    "Reached non-proxy segment while applying function to proxies, this should not happen, ignoring",
                );
                // Call provided function on segment
                operation(segment.as_ref())
            }
        };

        if let Err(err) = op_result {
            result = Err(OperationError::service_error(format!(
                "Applying function to a proxied shard segment {segment_id} failed: {err}"
            )));
            break;
        }

        // Try to unproxy/release this segment since we don't use it anymore
        // Unproxying now lets us release the segment earlier, prevent unnecessary writes to the temporary segment.
        // Make sure to keep at least one proxy segment to maintain access to the points in the shared write segment.
        // The last proxy and the shared write segment will be promoted into the segment_holder atomically
        // by `Self::unproxy_all_segments` afterwards to maintain the read consistency.
        let remaining = proxies.len() - unproxied_segment_ids.len();
        if remaining > 1 {
            let _update_guard = update_lock.blocking_write();
            match SegmentHolder::try_unproxy_segment(
                segments_lock,
                *segment_id,
                proxy_segment.clone(),
            ) {
                Ok(lock) => {
                    segments_lock = lock;
                    unproxied_segment_ids.push(*segment_id);
                }
                Err(lock) => segments_lock = lock,
            }
        }
    }
    proxies.retain(|(id, _)| !unproxied_segment_ids.contains(id));

    // Unproxy all segments
    // Always do this to prevent leaving proxy segments behind
    log::trace!("Unproxying all shard segments after function is applied");
    let _update_guard = update_lock.blocking_write();
    SegmentHolder::unproxy_all_segments(segments_lock, proxies, tmp_segment_id)?;

    result
}
