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
    /// # Errors
    /// Returns an error on flush failure
    fn flush_segments(segments: LockedSegmentHolder) -> OperationResult<SeqNumberType> {
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
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::data_types::vectors::only_default_vector;
    use segment::entry::entry_point::SegmentEntry as _;
    use shard::segment_holder::SegmentHolder;
    use tempfile::Builder;

    use super::*;
    use crate::collection_manager::fixtures::empty_segment;

    /// `failed_operation` is what keeps a failed operation replayable: it caps the version the
    /// flush worker acknowledges in the WAL, so the entry stays on disk until the operation is
    /// re-applied. Both halves of the recovery contract rest on this, queueing a transient failure
    /// and dropping one that can never succeed again.
    #[test]
    fn test_failed_operation_pins_wal_acknowledge() {
        let dir = Builder::new().prefix("segments").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let mut segment = empty_segment(dir.path());
        segment
            .upsert_point(
                10,
                1.into(),
                only_default_vector(&[1.0, 0.0, 1.0, 1.0]),
                &hw_counter,
            )
            .unwrap();

        let mut holder = SegmentHolder::default();
        holder.add_new(segment);
        let segments = LockedSegmentHolder::new(holder);
        segments.read().flush_all(FlushMode::Sync, true).unwrap();

        assert_eq!(
            UpdateWorkers::flush_segments(segments.clone()).unwrap(),
            10,
            "with nothing queued the acknowledge follows the persisted version",
        );

        segments.write().failed_operation.insert(5);
        assert_eq!(
            UpdateWorkers::flush_segments(segments.clone()).unwrap(),
            5,
            "a queued operation must hold the acknowledge below itself",
        );

        // Dropping the entry is what a non-transient decline does, and it must release the pin.
        segments.write().failed_operation.remove(&5);
        assert_eq!(
            UpdateWorkers::flush_segments(segments.clone()).unwrap(),
            10,
            "dropping the queued operation must let the acknowledge advance again",
        );
    }
}
