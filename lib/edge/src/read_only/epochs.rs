//! Read epochs: when a follower may delete a copy that a moved point's new copy supersedes.
//!
//! A read locks the segments one at a time, and a query visits them several times (prefetch,
//! rescore, payload retrieval). A reload pass installs new data first and deletes superseded copies
//! last, but a read that spans the pass could still visit the target before its new data and the
//! source after its delete, and miss the point. So every top-level read holds a token of the epoch
//! it started in, and before a pass masks copies it starts a new epoch and waits until no read of
//! the previous one is left. Reads are never blocked by this: they take the new epoch's token.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{Mutex, RawRwLock, RwLock};

/// How often a wait for older reads checks whether its caller was stopped.
const STOP_CHECK_INTERVAL: Duration = Duration::from_millis(50);

/// The current read epoch of a follower, see the [module docs](self).
#[derive(Debug, Default)]
pub(crate) struct ReadEpochs {
    /// The epoch new reads join. Reads take this lock shared; only starting a new epoch takes it
    /// exclusively, so no read can join an epoch once it has ended.
    current: RwLock<Epoch>,
    /// Ended epochs with reads a wait did not see finish before it timed out. Later waits wait
    /// for them too: a read that outlives one wait may still miss a point masked after it.
    pending: Mutex<Vec<Epoch>>,
}

/// Held by a read for its whole lifetime, see [`ReadEpochs::enter`].
#[derive(Debug)]
pub struct ReadEpochGuard {
    _read: EpochRead,
}

impl ReadEpochs {
    /// Join the current epoch, for a read about to snapshot the segments.
    pub(crate) fn enter(&self) -> ReadEpochGuard {
        ReadEpochGuard {
            _read: self.current.read().enter(),
        }
    }

    /// Start a new epoch, and wait until every read of the previous ones has finished, including
    /// the reads earlier waits gave up on. Returns `false` if `timeout` passed first, or the caller
    /// was stopped.
    pub(crate) fn advance_and_wait(&self, timeout: Duration, is_stopped: &AtomicBool) -> bool {
        let previous = std::mem::take(&mut *self.current.write());
        let mut pending = self.pending.lock();
        pending.push(previous);
        let deadline = Instant::now() + timeout;
        loop {
            let until = deadline.min(Instant::now() + STOP_CHECK_INTERVAL);
            pending.retain(|epoch| !epoch.wait_drained_until(until));
            if pending.is_empty() {
                return true;
            }
            if Instant::now() >= deadline || is_stopped.load(Ordering::Relaxed) {
                return false;
            }
        }
    }
}

/// One read epoch, and the reads that belong to it.
///
/// This uses a [`RwLock`] that guards no data, only for its bookkeeping of readers:
///
/// - every read of the epoch holds a shared lock on it for its whole lifetime, see
///   [`Self::enter`];
/// - waiting for those reads to finish is acquiring the lock exclusively, see
///   [`Self::wait_drained_until`]. An exclusive lock is only granted once no shared lock is left,
///   and the waiting thread is parked until then, so the wait needs neither polling nor sleeping.
///
/// The exclusive lock is released right away: it never excludes anyone, it only signals that the
/// reads have finished. Reads only join the current epoch, see [`ReadEpochs::current`], so no read
/// can take a shared lock on an epoch while or after it is waited for.
#[derive(Debug, Default)]
struct Epoch(Arc<RwLock<()>>);

/// A read's membership of an [`Epoch`]: a shared lock on it, held until dropped.
type EpochRead = ArcRwLockReadGuard<RawRwLock, ()>;

impl Epoch {
    /// Join the epoch until the returned guard is dropped.
    fn enter(&self) -> EpochRead {
        self.0.read_arc()
    }

    /// Wait until every read of the epoch has finished, or `deadline` passes. Returns whether they
    /// all finished.
    fn wait_drained_until(&self, deadline: Instant) -> bool {
        // Granted once no read holds a shared lock. Dropped at once: it only signals that.
        self.0.try_write_until(deadline).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A running read of the ending epoch keeps the wait from finishing, until it is dropped.
    #[test]
    fn waits_for_reads_of_the_ending_epoch() {
        let epochs = ReadEpochs::default();
        let not_stopped = AtomicBool::new(false);

        let first = epochs.enter();
        assert!(!epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));

        // `first` belongs to an epoch that already ended, `second` to the one ending next
        let second = epochs.enter();
        drop(first);
        assert!(!epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));

        drop(second);
        assert!(epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));
    }

    /// Reads that start while a pass waits join the new epoch, so they do not prolong the wait.
    #[test]
    fn reads_of_the_new_epoch_do_not_block_the_wait() {
        let epochs = Arc::new(ReadEpochs::default());

        let old_read = epochs.enter();
        let waiter = {
            let epochs = epochs.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                let drained =
                    epochs.advance_and_wait(Duration::from_secs(1), &AtomicBool::new(false));
                (drained, started.elapsed())
            })
        };

        // A read that starts after the epoch ended joins the new one
        std::thread::sleep(Duration::from_millis(10));
        let late_read = epochs.enter();
        std::thread::sleep(Duration::from_millis(40));
        drop(old_read);

        let (drained, waited) = waiter.join().unwrap();
        assert!(drained);
        assert!(waited < Duration::from_secs(1));
        drop(late_read);
    }

    /// A read that outlives a timed-out wait is still waited for by the next one, even though the
    /// epoch that next wait ends has no reads.
    #[test]
    fn a_timed_out_wait_leaves_its_reads_to_the_next() {
        let epochs = ReadEpochs::default();
        let not_stopped = AtomicBool::new(false);

        let old_read = epochs.enter();
        assert!(!epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));
        assert!(!epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));

        drop(old_read);
        assert!(epochs.advance_and_wait(Duration::from_millis(20), &not_stopped));
    }

    #[test]
    fn a_stopped_wait_gives_up() {
        let epochs = ReadEpochs::default();
        let _read = epochs.enter();
        assert!(!epochs.advance_and_wait(Duration::from_secs(10), &AtomicBool::new(true)));
    }
}
