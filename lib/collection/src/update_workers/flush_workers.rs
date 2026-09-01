use std::cmp::min;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use common::panic;
use segment::common::operation_error::OperationResult;
use segment::types::SeqNumberType;
use shard::segment_holder::FlushMode;
use shard::segment_holder::locked::LockedSegmentHolder;
use shard::wal::WalError;
use tokio::sync::oneshot;

use crate::shards::local_shard::LocalShardClocks;
use crate::update_workers::UpdateWorkers;
use crate::wal_delta::LockedWal;

impl UpdateWorkers {
    /// Returns confirmed version after flush of all segments
    ///
    /// Runs at an operation boundary: the updates lock is held while flushers are captured and
    /// the persisted waterline is read, so this pass can never observe a half-applied operation.
    ///
    /// A single update operation writes to segments in several separately locked phases. E.g.
    /// `upsert_points_impl` first updates the points that already exist, then takes a fresh write
    /// lock on the smallest appendable segment to insert the new ones. `flush_all` read-locks
    /// every segment, which prevents interleaving *within* a phase, but without this guard a pass
    /// could still start *between* two phases of the same operation. It would then capture a
    /// segment at `version = op_num` with only part of that operation applied, and stamp
    /// `persisted_version = op_num` once the flush completes. The segment looks clean from then on
    /// (`version == persisted_version`), so the rest of the operation is never written, while the
    /// waterline reports it as fully saved and the WAL is acknowledged past it, dropping the only
    /// recoverable copy. See #10402.
    ///
    /// # Errors
    /// Returns an error on flush failure
    fn flush_segments(segments: LockedSegmentHolder) -> OperationResult<SeqNumberType> {
        // Taken before the holder read lock, matching `CollectionUpdater::update`, so waiting for
        // it never blocks a writer that is waiting for the holder lock.
        let _updates_guard = segments.acquire_updates_lock();
        let read_segments = segments.read();
        let flushed_version = read_segments.flush_all(FlushMode::Background, false)?;
        Ok(match read_segments.failed_operation.iter().cloned().min() {
            None => flushed_version,
            Some(failed_operation) => min(failed_operation, flushed_version),
        })
    }

    fn flush_worker_internal(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_keep_from: Arc<AtomicU64>,
        clocks: LocalShardClocks,
        shard_path: PathBuf,
    ) {
        log::trace!("Attempting flushing");
        let wal_flush_job = wal.blocking_lock().flush_async();

        let wal_flush_res = match wal_flush_job.join() {
            Ok(Ok(())) => Ok(()),

            Ok(Err(err)) => Err(WalError::WriteWalError(format!(
                "failed to flush WAL: {err}"
            ))),

            Err(panic) => {
                let message = panic::downcast_str(&panic).unwrap_or("");
                let separator = if !message.is_empty() { ": " } else { "" };
                Err(WalError::WriteWalError(format!(
                    "failed to flush WAL: flush task panicked{separator}{message}"
                )))
            }
        };

        if let Err(err) = wal_flush_res {
            log::error!("{err}");
            segments.write().report_optimizer_error(err);
            return;
        }

        let confirmed_version = Self::flush_segments(segments.clone());
        let confirmed_version = match confirmed_version {
            Ok(version) => version,
            Err(err) => {
                // Since Self::flush_segments is flushing asynchronously, we can get the error
                // from the previous flush cycle, not necessarily this one.
                log::error!("Failed to flush: {err}");
                segments.write().report_optimizer_error(err);
                return;
            }
        };

        // Acknowledge confirmed version in WAL, but don't acknowledge the specified
        // `keep_from` index or higher.
        // This is to prevent truncating WAL entries that other bits of code still depend on
        // such as the queue proxy shard.
        // Default keep_from is `u64::MAX` to allow acknowledging all confirmed.
        let keep_from = wal_keep_from.load(std::sync::atomic::Ordering::Relaxed);

        // If we should keep the first message, do not acknowledge at all
        if keep_from == 0 {
            return;
        }

        let ack = confirmed_version.min(keep_from.saturating_sub(1));

        if let Err(err) = clocks.store_if_changed(&shard_path) {
            log::warn!("Failed to store clock maps to disk: {err}");
            segments.write().report_optimizer_error(err);
        }

        if let Err(err) = wal.blocking_lock().ack(ack) {
            log::warn!("Failed to acknowledge WAL version: {err}");
            segments.write().report_optimizer_error(err);
        }
    }

    pub async fn flush_worker_fn(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_keep_from: Arc<AtomicU64>,
        clocks: LocalShardClocks,
        flush_interval_sec: u64,
        mut stop_receiver: oneshot::Receiver<()>,
        shard_path: PathBuf,
    ) {
        loop {
            tokio::select! {
                biased;
                // Stop flush worker on signal or if sender was dropped
                _ = &mut stop_receiver => {
                    log::debug!("Stopping flush worker for shard {}", shard_path.display());
                    return;
                },
                // Flush at the configured flush interval
                _ = tokio::time::sleep(Duration::from_secs(flush_interval_sec)) => {},
            }

            let segments_clone = segments.clone();
            let wal_clone = wal.clone();
            let wal_keep_from_clone = wal_keep_from.clone();
            let clocks_clone = clocks.clone();
            let shard_path_clone = shard_path.clone();

            tokio::task::spawn_blocking(move || {
                Self::flush_worker_internal(
                    segments_clone,
                    wal_clone,
                    wal_keep_from_clone,
                    clocks_clone,
                    shard_path_clone,
                )
            })
            .await
            .unwrap_or_else(|error| {
                log::error!("Flush worker failed: {error}",);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;

    /// A flush pass must not start in the middle of an update operation.
    ///
    /// Operations write to segments in several separately locked phases; a flush that lands
    /// between two of them captures a segment with the operation half applied and then marks it
    /// as fully persisted, so the remainder is never written (#10402). Holding the updates lock
    /// for the duration of an operation is what keeps flushes on operation boundaries.
    #[test]
    fn flush_segments_waits_for_operation_boundary() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let holder = build_test_holder(dir.path());

        let updates_guard = holder.acquire_updates_lock();

        let flushed = Arc::new(AtomicBool::new(false));
        let handle = thread::spawn({
            let holder = holder.clone();
            let flushed = flushed.clone();
            move || {
                UpdateWorkers::flush_segments(holder).unwrap();
                flushed.store(true, Ordering::SeqCst);
            }
        });

        thread::sleep(Duration::from_millis(100));
        assert!(
            !flushed.load(Ordering::SeqCst),
            "flush must block while an update operation holds the updates lock",
        );

        drop(updates_guard);
        handle.join().unwrap();
        assert!(flushed.load(Ordering::SeqCst));
    }
}
