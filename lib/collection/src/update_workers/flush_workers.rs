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

        if let Err(err) = clocks.store_if_changed(&shard_path) {
            log::warn!("Failed to store clock maps to disk: {err}");
            segments.write().report_optimizer_error(err);
        }

        if let Err(err) = Self::ack_wal(&wal, &wal_ack_pins, confirmed_version) {
            log::warn!("Failed to acknowledge WAL version: {err}");
            segments.write().report_optimizer_error(err);
        }
    }

    /// Acknowledge `confirmed_version` in the WAL, but never past the lowest WAL acknowledge pin.
    ///
    /// Pins prevent truncating WAL entries that other bits of code still depend on, such as the
    /// queue proxy shard. Without pins we acknowledge all confirmed versions.
    ///
    /// The pins are read *under the WAL lock*, so that a pin installed concurrently is either
    /// fully visible here or cannot have been installed yet. Reading them before taking the lock
    /// leaves a window in which a queue proxy pins a version we then truncate away. See the lock
    /// order documented on [`WalAckPins`].
    fn ack_wal(
        wal: &LockedWal,
        wal_ack_pins: &WalAckPins,
        confirmed_version: SeqNumberType,
    ) -> Result<(), WalError> {
        let mut wal = wal.blocking_lock();
        let ack = wal_ack_pins.max_ack(confirmed_version);
        wal.ack(ack)
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
    use std::num::NonZeroUsize;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use segment::data_types::vectors::VectorStructInternal;
    use shard::wal::{SerdeWal, WalRawRecord};
    use tempfile::{Builder, TempDir};
    use tokio::sync::Mutex as TokioMutex;
    use wal::WalOptions;

    use super::*;
    use crate::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted,
    };
    use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};

    /// A WAL holding `entries` operations, at indices `0..entries`.
    fn fixture_wal(entries: u64) -> (LockedWal, TempDir) {
        let dir = Builder::new()
            .prefix("flush_worker_test")
            .tempdir()
            .unwrap();
        let options = WalOptions {
            segment_capacity: 1024 * 1024,
            segment_queue_len: 0,
            retain_closed: NonZeroUsize::new(1).unwrap(),
        };
        let mut wal: SerdeWal<OperationWithClockTag> = SerdeWal::new(dir.path(), options).unwrap();

        for id in 0..entries {
            let operation =
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
                        id: id.into(),
                        vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0]).into(),
                        payload: None,
                    }]),
                ));
            let record = WalRawRecord::new(&OperationWithClockTag::new(operation, None)).unwrap();
            assert_eq!(wal.write(&record).unwrap(), id);
        }

        (Arc::new(TokioMutex::new(wal)), dir)
    }

    #[test]
    fn test_ack_without_pins_acknowledges_everything_confirmed() {
        let (wal, _dir) = fixture_wal(10);
        let pins = WalAckPins::default();

        UpdateWorkers::ack_wal(&wal, &pins, 9).unwrap();

        assert_eq!(wal.blocking_lock().first_index(), 9);
    }

    /// A pin installed while the flush worker is acknowledging must still be honoured.
    ///
    /// Regression test: the pins used to be read *before* the WAL lock was taken, so a queue
    /// proxy that pinned in that window had its entries acknowledged away regardless, and its
    /// first WAL read failed. Both sides now take the WAL lock first, see the lock order
    /// documented on [`WalAckPins`].
    #[test]
    fn test_ack_honours_pin_installed_under_wal_lock() {
        const PINNED: u64 = 3;
        const CONFIRMED: SeqNumberType = 9;

        let (wal, _dir) = fixture_wal(10);
        let pins = Arc::new(WalAckPins::default());

        // Hold the WAL lock, the way `queue_proxify_local` does while it builds a queue proxy
        let wal_guard = wal.blocking_lock();

        let acker = {
            let wal = wal.clone();
            let pins = pins.clone();
            thread::spawn(move || UpdateWorkers::ack_wal(&wal, &pins, CONFIRMED).unwrap())
        };

        // Let the flush worker get as far as it can, which must be no further than the WAL lock.
        // Reading the pins before that point sees none of them, and acknowledges everything.
        thread::sleep(Duration::from_millis(100));

        // Install the pin while still under the WAL lock, as `new_from_version` does
        let _pin = pins.pin(PINNED);

        drop(wal_guard);
        acker.join().unwrap();

        assert!(
            wal.blocking_lock().first_index() <= PINNED,
            "acknowledged past a live pin, the entries it still needs are gone",
        );
    }

    /// A pin at the very first WAL index caps the acknowledge at 0, which truncates nothing.
    ///
    /// Regression test: this used to be a special case that returned early from the whole flush
    /// pass, so clock maps were not persisted either for as long as the pin was held.
    #[test]
    fn test_ack_with_pin_at_zero_keeps_the_whole_wal() {
        let (wal, _dir) = fixture_wal(10);
        let pins = WalAckPins::default();
        let _pin = pins.pin(0);

        UpdateWorkers::ack_wal(&wal, &pins, 9).unwrap();

        let wal = wal.blocking_lock();
        assert_eq!(wal.first_index(), 0);
        assert_eq!(wal.len(false), 10);
    }

    /// Acknowledging and pinning must take the WAL lock before the pin lock, both of them.
    ///
    /// Regression test for the deadlock the obvious fix for the race above invites: holding the
    /// pin lock and then waiting for the WAL lock inverts the order that
    /// `QueueProxyShard::new_from_version` establishes, and hangs both sides.
    #[test]
    fn test_ack_and_pin_do_not_deadlock() {
        const ITERATIONS: usize = 1_000;

        let (wal, _dir) = fixture_wal(10);
        let pins = Arc::new(WalAckPins::default());

        let acker = {
            let wal = wal.clone();
            let pins = pins.clone();
            thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    UpdateWorkers::ack_wal(&wal, &pins, 0).unwrap();
                }
            })
        };

        let pinner = {
            let wal = wal.clone();
            let pins = pins.clone();
            thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    // The order `new_from_version` establishes: WAL lock, then pin
                    let wal_guard = wal.blocking_lock();
                    let pin = pins.pin(0);
                    drop(wal_guard);
                    drop(pin);
                }
            })
        };

        // An inverted lock order shows up as both threads hanging on each other forever
        let (done_sender, done_receiver) = mpsc::channel();
        thread::spawn(move || {
            acker.join().unwrap();
            pinner.join().unwrap();
            let _ = done_sender.send(());
        });

        done_receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("acknowledging and pinning deadlocked, check the WAL and pin lock order");
    }
}
