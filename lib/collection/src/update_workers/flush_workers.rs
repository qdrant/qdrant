use std::cmp::min;
use std::path::PathBuf;
use std::sync::Arc;
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
use crate::update_workers::applied_seq::AppliedSeqHandler;
use crate::wal_ack_pin::WalAckPins;
use crate::wal_delta::LockedWal;

/// The version to acknowledge in the WAL after a flush pass confirmed `confirmed_version` durable.
///
/// Never at or past the lowest live WAL acknowledge pin: those entries are still needed by
/// whoever holds the pin, such as a snapshot in progress that has not captured the WAL yet, or the
/// queue proxy shard replaying operations to a remote. Without pins everything confirmed is
/// acknowledged.
///
/// `None` means nothing may be acknowledged at all, because the very first entry is pinned.
pub(crate) fn wal_ack_version(
    confirmed_version: SeqNumberType,
    wal_ack_pins: &WalAckPins,
) -> Option<SeqNumberType> {
    match wal_ack_pins.lowest() {
        // If the very first message is pinned, we cannot acknowledge anything at all
        Some(0) => None,
        Some(lowest_pin) => Some(confirmed_version.min(lowest_pin - 1)),
        None => Some(confirmed_version),
    }
}

impl UpdateWorkers {
    /// Returns confirmed version after flush of all segments
    ///
    /// `applied_up_to` is the last operation the update worker finished applying. This pass runs
    /// concurrently with the update worker and can start between the phases of one operation, so
    /// no segment may claim a version past it. See [`StorageSegmentEntry::flusher`].
    ///
    /// # Errors
    /// Returns an error on flush failure
    fn flush_segments(
        segments: LockedSegmentHolder,
        applied_up_to: Option<SeqNumberType>,
    ) -> OperationResult<SeqNumberType> {
        let read_segments = segments.read();
        let flushed_version =
            read_segments.flush_all_up_to(FlushMode::Background, false, applied_up_to)?;
        Ok(match read_segments.failed_operation.iter().cloned().min() {
            None => flushed_version,
            Some(failed_operation) => min(failed_operation, flushed_version),
        })
    }

    fn flush_worker_internal(
        segments: LockedSegmentHolder,
        wal: LockedWal,
        wal_ack_pins: Arc<WalAckPins>,
        clocks: LocalShardClocks,
        shard_path: PathBuf,
        applied_seq_handler: Arc<AppliedSeqHandler>,
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

        // Read before capturing anything: an operation that finishes during the flush must not
        // raise the cap for segments this pass already captured half of.
        let applied_up_to = applied_seq_handler.applied_op_num();

        let confirmed_version = Self::flush_segments(segments.clone(), Some(applied_up_to));
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

        let Some(ack) = wal_ack_version(confirmed_version, &wal_ack_pins) else {
            return;
        };

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
        wal_ack_pins: Arc<WalAckPins>,
        clocks: LocalShardClocks,
        flush_interval_sec: u64,
        mut stop_receiver: oneshot::Receiver<()>,
        shard_path: PathBuf,
        applied_seq_handler: Arc<AppliedSeqHandler>,
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
            let wal_ack_pins_clone = wal_ack_pins.clone();
            let clocks_clone = clocks.clone();
            let shard_path_clone = shard_path.clone();
            let applied_seq_handler_clone = applied_seq_handler.clone();

            tokio::task::spawn_blocking(move || {
                Self::flush_worker_internal(
                    segments_clone,
                    wal_clone,
                    wal_ack_pins_clone,
                    clocks_clone,
                    shard_path_clone,
                    applied_seq_handler_clone,
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
    use super::*;

    /// Without pins everything a flush pass confirmed durable is acknowledged.
    #[test]
    fn test_acknowledges_everything_confirmed_without_pins() {
        let pins = WalAckPins::default();
        assert_eq!(wal_ack_version(100, &pins), Some(100));
    }

    /// A pin holds the acknowledge below itself, so its entries stay in the WAL. This is what a
    /// snapshot that includes the WAL relies on: it pins before copying the segment files and
    /// holds until the WAL is archived, so operations applied meanwhile stay replayable.
    #[test]
    fn test_pin_holds_acknowledge_below_itself() {
        let pins = WalAckPins::default();

        let pin = pins.pin(50);
        assert_eq!(
            wal_ack_version(100, &pins),
            Some(49),
            "a pin must hold the acknowledge below itself, even when more is durable",
        );

        // Confirmed below the pin is not raised to it
        assert_eq!(wal_ack_version(10, &pins), Some(10));

        drop(pin);
        assert_eq!(
            wal_ack_version(100, &pins),
            Some(100),
            "releasing the last pin lifts the hold",
        );
    }

    /// The lowest pin wins, and the hold lasts until the last one is released, in any order.
    #[test]
    fn test_lowest_pin_holds_the_acknowledge() {
        let pins = WalAckPins::default();

        let low = pins.pin(20);
        let high = pins.pin(80);
        assert_eq!(wal_ack_version(100, &pins), Some(19));

        drop(low);
        assert_eq!(wal_ack_version(100, &pins), Some(79));

        drop(high);
        assert_eq!(wal_ack_version(100, &pins), Some(100));
    }

    /// Pinning the very first entry means nothing may be acknowledged at all. A snapshot of a
    /// shard whose WAL was never acknowledged pins at index 0 and must not truncate anything.
    #[test]
    fn test_pinning_the_first_entry_acknowledges_nothing() {
        let pins = WalAckPins::default();
        let _pin = pins.pin(0);
        assert_eq!(wal_ack_version(100, &pins), None);
    }
}
