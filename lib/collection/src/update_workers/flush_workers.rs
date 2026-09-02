use std::cmp::min;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// The confirmed version never exceeds `applied_version`, which is read *before* flushing.
    /// An operation writes to segments in several separately locked steps, so the pass may
    /// capture an operation that is still in flight: its segments already carry the new version
    /// while the update worker has not bumped `applied_version` yet. Acknowledging it would drop
    /// the only replayable copy of whatever part of it is not on disk.
    ///
    /// This bounds the WAL acknowledge only. It does not keep a pass from capturing a segment in
    /// the middle of an operation: such a segment is stamped with the in-flight version once its
    /// flush completes, and later passes report it as persisted.
    ///
    /// # Errors
    /// Returns an error on flush failure
    fn flush_segments(
        segments: LockedSegmentHolder,
        applied_version: &AtomicU64,
    ) -> OperationResult<SeqNumberType> {
        // Read before flushing, so the bound predates anything this pass captures
        let applied_version = applied_version.load(Ordering::Acquire);
        let read_segments = segments.read();
        let flushed_version = read_segments.flush_all(FlushMode::Background, false)?;
        let confirmed_version = min(flushed_version, applied_version);
        Ok(match read_segments.failed_operation.iter().cloned().min() {
            None => confirmed_version,
            Some(failed_operation) => min(failed_operation, confirmed_version),
        })
    }

    fn flush_worker_internal(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_keep_from: Arc<AtomicU64>,
        applied_version: Arc<AtomicU64>,
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

        let confirmed_version = Self::flush_segments(segments.clone(), &applied_version);
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
        let keep_from = wal_keep_from.load(Ordering::Relaxed);

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

    #[allow(clippy::too_many_arguments)]
    pub async fn flush_worker_fn(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_keep_from: Arc<AtomicU64>,
        applied_version: Arc<AtomicU64>,
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
            let applied_version_clone = applied_version.clone();
            let clocks_clone = clocks.clone();
            let shard_path_clone = shard_path.clone();

            tokio::task::spawn_blocking(move || {
                Self::flush_worker_internal(
                    segments_clone,
                    wal_clone,
                    wal_keep_from_clone,
                    applied_version_clone,
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
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::build_test_holder;

    /// The confirmed version never exceeds `applied_version`, even once every segment change is
    /// on disk: an operation that is still in flight has to stay replayable.
    #[test]
    fn flush_segments_caps_confirmed_version_at_applied_version() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let holder = build_test_holder(dir.path());
        let max_version = holder
            .read()
            .iter()
            .map(|(_, segment)| segment.get().read().version())
            .max()
            .unwrap();
        assert!(max_version > 1);

        // Persist everything up front, so the background passes below have nothing left to
        // flush and report the full persisted version right away
        holder.read().flush_all(FlushMode::Sync, true).unwrap();

        // Everything applied: the persisted version is confirmed as is
        let applied_version = AtomicU64::new(max_version);
        assert_eq!(
            UpdateWorkers::flush_segments(holder.clone(), &applied_version).unwrap(),
            max_version,
        );

        // Last operation still in flight: it is on disk, but must not be confirmed
        applied_version.store(max_version - 1, Ordering::Release);
        assert_eq!(
            UpdateWorkers::flush_segments(holder.clone(), &applied_version).unwrap(),
            max_version - 1,
        );

        // Nothing applied by this process yet
        applied_version.store(0, Ordering::Release);
        assert_eq!(
            UpdateWorkers::flush_segments(holder, &applied_version).unwrap(),
            0,
        );
    }
}
