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

use parking_lot::Mutex;

/// The current read epoch of a follower, see the [module docs](self).
#[derive(Debug, Default)]
pub(crate) struct ReadEpochs {
    /// Cloned by every read for its lifetime: the strong count of an epoch's token is the number of
    /// its reads still running, plus one.
    current: Mutex<Arc<()>>,
}

/// Held by a read for its whole lifetime, see [`ReadEpochs::enter`].
#[derive(Debug)]
pub struct ReadEpochGuard {
    _token: Arc<()>,
}

impl ReadEpochs {
    /// Join the current epoch, for a read about to snapshot the segments.
    pub(crate) fn enter(&self) -> ReadEpochGuard {
        ReadEpochGuard {
            _token: self.current.lock().clone(),
        }
    }

    /// Start a new epoch, and wait until every read of the previous one has finished. Returns
    /// `false` if `timeout` passed first, or the caller was stopped.
    pub(crate) fn advance_and_wait(&self, timeout: Duration, is_stopped: &AtomicBool) -> bool {
        let previous = std::mem::replace(&mut *self.current.lock(), Arc::new(()));
        let deadline = Instant::now() + timeout;
        while Arc::strong_count(&previous) > 1 {
            if Instant::now() >= deadline || is_stopped.load(Ordering::Relaxed) {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
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
        let not_stopped = AtomicBool::new(false);

        let old_read = epochs.enter();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(old_read);
        });
        let late_reader = {
            let epochs = epochs.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                let read = epochs.enter();
                std::thread::sleep(Duration::from_secs(2));
                drop(read);
            })
        };

        let started = Instant::now();
        assert!(epochs.advance_and_wait(Duration::from_secs(1), &not_stopped));
        assert!(started.elapsed() < Duration::from_secs(1));

        releaser.join().unwrap();
        late_reader.join().unwrap();
    }

    #[test]
    fn a_stopped_wait_gives_up() {
        let epochs = ReadEpochs::default();
        let _read = epochs.enter();
        assert!(!epochs.advance_and_wait(Duration::from_secs(10), &AtomicBool::new(true)));
    }
}
