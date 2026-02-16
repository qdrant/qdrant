use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::save_on_disk::SaveOnDisk;
use common::tar_ext;
use fs_err as fs;
use parking_lot::RwLock;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SegmentManifest;
use segment::entry::NonAppendableSegmentEntry;
use segment::types::{SegmentConfig, SnapshotFormat};
use shard::files::{APPLIED_SEQ_FILE, SEGMENTS_PATH, WAL_PATH};
use shard::locked_segment::LockedSegment;
use shard::operations::OperationWithClockTag;
use shard::payload_index_schema::PayloadIndexSchema;
use shard::segment_holder::SegmentHolder;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::snapshots::snapshot_manifest::SnapshotManifest;
use shard::snapshots::snapshot_utils::SnapshotUtils;
use shard::wal::SerdeWal;
use tokio::sync::OwnedMutexGuard;
use tokio_util::task::AbortOnDropHandle;
use wal::{Wal, WalOptions};

use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::{LocalShard, LocalShardClocks};

impl LocalShard {
    pub async fn snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        let task = {
            let _runtime = self.search_runtime.enter();

            let segments = self.segments.clone();
            cancel::blocking::spawn_cancel_on_drop(move |_| segments.read().snapshot_manifest())
        };

        Ok(task.await??)
    }

    pub fn restore_snapshot(snapshot_path: &Path) -> CollectionResult<()> {
        log::info!("Restoring shard snapshot {}", snapshot_path.display());
        SnapshotUtils::restore_unpacked_snapshot(snapshot_path)?;
        Ok(())
    }

    /// Create snapshot for local shard into `target_path`
    pub async fn get_snapshot_creator(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<SnapshotManifest>,
        save_wal: bool,
    ) -> CollectionResult<impl Future<Output = CollectionResult<()>> + use<>> {
        let segments = self.segments.clone();
        let wal = self.wal.wal.clone();
        let payload_index_schema = self.payload_index_schema.clone();

        let shard_path = self.path.clone();

        let segments_path = Self::segments_path(&self.path);
        let segment_config = self
            .collection_config
            .read()
            .await
            .to_base_segment_config()?;

        let applied_seq_path = self.applied_seq_handler.path().to_path_buf();

        let tar = tar.clone();
        let temp_path = temp_path.to_path_buf();

        let plunger_notify = if !save_wal {
            // If we are not saving WAL, we still need to make sure that all submitted by this point
            // updates have made it to the segments. So we use the Plunger to achieve that.
            // It will notify us when all submitted updates so far have been processed.
            Some(self.plunge_async().await?)
        } else {
            None
        };

        let future = async move {
            if let Some(plunger_notify) = plunger_notify {
                plunger_notify.await?;
            }

            let handle = tokio::task::spawn_blocking(move || {
                // Do not change segments while snapshotting
                snapshot_all_segments(
                    segments.clone(),
                    &segments_path,
                    Some(segment_config),
                    payload_index_schema,
                    &temp_path,
                    &tar.descend(Path::new(SEGMENTS_PATH))?,
                    format,
                    manifest.as_ref(),
                )?;

                let wal_guard = wal.blocking_lock_owned();

                LocalShardClocks::archive_data(&shard_path, &tar)?;

                // Staging delay
                #[cfg(feature = "staging")]
                {
                    let delay_secs: f64 =
                        std::env::var("QDRANT__STAGING__SNAPSHOT_SHARD_CLOCKS_DELAY")
                            .ok()
                            .and_then(|str| str.parse().ok())
                            .unwrap_or(0.0);

                    if delay_secs > 0.0 {
                        log::debug!("Staging: Delaying snapshotting WAL for {delay_secs}s");
                        std::thread::sleep(std::time::Duration::from_secs_f64(delay_secs));
                        log::debug!("Staging: Delay complete, snapshotting WAL");
                    }
                }

                if save_wal {
                    // snapshot all shard's WAL
                    Self::snapshot_wal(wal_guard, &tar)?;
                    // snapshot applied_seq, it is preferred to save applied_seq later,
                    // as higher applied_seq means more updates will be processed on restore,
                    // which is more safe than having applied_seq too old.
                    Self::snapshot_applied_seq(applied_seq_path, &tar)?;
                } else {
                    Self::snapshot_empty_wal(wal_guard, &temp_path, &tar)?;
                }

                CollectionResult::Ok(())
            });

            AbortOnDropHandle::new(handle).await??;

            Ok(())
        };

        Ok(future)
    }

    /// Create empty WAL which is compatible with currently stored data
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    fn snapshot_empty_wal(
        wal_guard: OwnedMutexGuard<SerdeWal<OperationWithClockTag>>,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        let wal_segment_capacity = wal_guard.segment_capacity();
        let wal_last_index = wal_guard.last_index();

        // Empty snapshot only need indexes to be correct
        drop(wal_guard);

        let temp_dir = tempfile::tempdir_in(temp_path).map_err(|err| {
            CollectionError::service_error(format!(
                "Can not create temporary directory for WAL: {err}",
            ))
        })?;

        Wal::generate_empty_wal_starting_at_index(
            temp_dir.path(),
            &WalOptions {
                segment_capacity: wal_segment_capacity,
                segment_queue_len: 0,
                retain_closed: NonZeroUsize::new(1).unwrap(),
            },
            wal_last_index,
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
    fn snapshot_wal(
        mut wal_guard: OwnedMutexGuard<SerdeWal<OperationWithClockTag>>,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
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

    /// snapshot the applied_seq file
    fn snapshot_applied_seq(
        applied_seq_path: PathBuf,
        tar: &tar_ext::BuilderExt,
    ) -> CollectionResult<()> {
        if applied_seq_path.exists() {
            tar.blocking_append_file(applied_seq_path.as_path(), Path::new(APPLIED_SEQ_FILE))
                .map_err(|err| {
                    CollectionError::service_error(format!(
                        "Error while archiving applied_seq: {err}"
                    ))
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
            let request_segment_manifest = if let Some(manifest) = manifest {
                let segment_id = read_segment.segment_id()?;
                Some(
                    manifest
                        .get(&segment_id)
                        .map(Cow::Borrowed)
                        .unwrap_or_else(|| Cow::Owned(SegmentManifest::empty(segment_id))),
                )
            } else {
                None
            };
            let segment_manifest_ref = request_segment_manifest.as_ref().map(|m| m.as_ref());
            read_segment.take_snapshot(temp_dir, tar, format, segment_manifest_ref)?;
            Ok(())
        },
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
) -> OperationResult<()>
where
    F: FnMut(&RwLock<dyn NonAppendableSegmentEntry>) -> OperationResult<()>,
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
                let wrapped_segment = proxy_segment.read().wrapped_segment.clone();
                let segment = wrapped_segment.get();
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
            match SegmentHolder::try_unproxy_segment(
                segments_lock,
                *segment_id,
                proxy_segment.clone(),
                segments.acquire_updates_lock(),
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
    SegmentHolder::unproxy_all_segments(
        segments_lock,
        proxies,
        tmp_segment_id,
        segments.acquire_updates_lock(),
    )?;

    result
}
