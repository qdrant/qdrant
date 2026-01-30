#![allow(dead_code)]

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

/// A seqlock is a lock-free synchronization mechanism that provides
/// sequential consistency for reads and writes.
///
/// It allows a single writer to mutate the inner type uncontended, at the cost of
/// readers repeating the read as long as:
/// 1. The writer is not currently accessing the resource
/// 2. The resource did not change in between the start and end of the read.
///
/// The [`SeqLock`] struct itself is not Send, nor Sync, so it is kind of useless on its own.
///
/// To allow multiple readers, and ensure a single writer, a `new_reader_writer` method is provided.
///
/// # SAFETY
/// While this lock ensures that a read is consistent, it does not protect against
/// writers creating use-after-free errors. This is because readers can see the
/// shared resource even as it is being modified. It is the user responsibility to make
/// sure the underlying resource does not change allocations, and that the worst that
/// can happen is a garbage/torn read.
///
/// ```ignore
/// use crate::seqlock::SeqLock;
///
/// fn main() {
///
///     let shared_resource = String::from("banner");
///
///     let (reader, writer) = SeqLock::new_reader_writer(shared_resource);
///
///     let reader_2 = reader.clone(); // This can be cheaply copied, pointing to the same resource.
///
///     std::thread::spawn(move || {
///         let value = reader_2.read(|value| value.to_owned());
///         println!("Value: {}", value);
///     });
///
///     // writer can be sent to a thread, but can't be shared between them
///     std::thread::spawn(move || {
///         writer.write(|value| value.push('s'));
///     });
/// }
/// ```
pub struct SeqLock<T> {
    seq: AtomicUsize,
    inner: UnsafeCell<T>,
}

impl<T> SeqLock<T> {
    fn new(v: T) -> Self {
        SeqLock {
            seq: AtomicUsize::new(0),
            inner: UnsafeCell::new(v),
        }
    }

    /// Wrap the value in a seqlock.
    ///
    /// Returns a reader which is Sync and Send; and a writer, which is only Send.
    ///
    /// The reader can be cloned and is safe to use between threads
    ///
    /// The writer should only have a single thread using it.
    pub fn new_reader_writer(v: T) -> (SeqLockReader<T>, SeqLockWriter<T>) {
        let lock = Arc::new(SeqLock::new(v));
        let reader = SeqLockReader { lock: lock.clone() };
        let writer = SeqLockWriter { lock: lock.clone() };
        (reader, writer)
    }

    fn read<U, F: Fn(&T) -> U>(&self, callback: F) -> U {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);

            // if odd, it is locked, wait until unlocked.
            if seq1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }

            let result = callback(unsafe { &*self.inner.get() });

            fence(Ordering::Acquire);

            let seq2 = self.seq.load(Ordering::Relaxed);

            // only return if seq did not change.
            if seq1 == seq2 {
                return result;
            }
        }
    }

    unsafe fn write(&self, callback: impl FnOnce(&mut T)) {
        let seq = self.seq.load(Ordering::Acquire);
        self.seq.store(seq + 1, Ordering::Release);

        callback(unsafe { &mut *self.inner.get() });

        self.seq.store(seq + 2, Ordering::Release);
    }
}

/// This structure can read the protected resource at any time, but it will:
/// - busy-wait if a write is taking place.
/// - or repeat the read if a write took place in between the start and end
///   of the read.
pub struct SeqLockReader<T> {
    lock: Arc<SeqLock<T>>,
}

unsafe impl<T: Send> Send for SeqLockReader<T> {}
unsafe impl<T: Sync> Sync for SeqLockReader<T> {}

impl<T> Clone for SeqLockReader<T> {
    fn clone(&self) -> Self {
        SeqLockReader {
            lock: Arc::clone(&self.lock),
        }
    }
}

impl<T> SeqLockReader<T> {
    pub fn read<U, F: Fn(&T) -> U>(&self, callback: F) -> U {
        self.lock.read(callback)
    }
}

/// This structure can get mutable access to the structure at any time.
/// It is intentionally `!Sync`, so there should not be other writers.
///
/// Due to the nature of a seqlock, this will not contend with the
/// readers. The readers will wait until the write finishes, or repeat
/// the read so that they don't get a torn read.
pub struct SeqLockWriter<T> {
    lock: Arc<SeqLock<T>>,
}

unsafe impl<T> Send for SeqLockWriter<T> where T: Send {}

impl<T> SeqLockWriter<T> {
    /// Get mutable access to the protected resource through a closure.
    ///
    /// # SAFETY
    /// The caller must ensure that at ANY point during the mutation, a reader
    /// will not get into a panic. This might mean:
    /// - No use-after-free errors. Which includes not changing allocations.
    /// - No out-of-bounds indexing.
    /// - ...etc.
    ///
    /// It is fine if the reader reads garbage or get torn reads, since it
    /// will know that a mutation took place, and retry the operation.
    pub unsafe fn write<F: FnOnce(&mut T)>(&self, callback: F) {
        unsafe { self.lock.write(callback) }
    }
}

/// ```compile_fail,E0277
/// use trififo::seqlock::SeqLock;
///
/// fn is_sync<T: Sync>(_: &T) {}
///
/// fn main() {
///     let (reader, writer) = SeqLock::new_reader_writer(0u32);
///
///     is_sync(&writer);
/// }
/// ```
///
/// `SeqLockReader<T>` is not `Send`/`Sync` when `T: !Send`/`!Sync`:
/// ```compile_fail,E0277
/// use trififo::seqlock::SeqLock;
/// use std::rc::Rc;
///
/// fn is_send<T: Send>(_: &T) {}
/// fn is_sync<T: Sync>(_: &T) {}
///
/// fn main() {
///     // Rc is !Send and !Sync
///     let (reader, _writer) = SeqLock::new_reader_writer(Rc::new(0u32));
///
///     is_send(&reader);
///     is_sync(&reader);
/// }
/// ```

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use static_assertions::assert_impl_all;

    use super::*;

    #[derive(Debug)]
    struct Pair {
        a: usize,
        b: usize,
    }

    #[test]
    fn multi_threaded_readers_consistent() {
        // Create a seqlock-wrapped Pair.
        let (reader, writer) = SeqLock::new_reader_writer(Pair { a: 0, b: 0 });

        // Signal to readers when the writer is finished.
        let writer_done = Arc::new(AtomicBool::new(false));

        // Spawn several reader threads that continuously read and assert consistency.
        let mut reader_handles = Vec::new();
        for _ in 0..8 {
            let r = SeqLockReader::clone(&reader);
            let done = Arc::clone(&writer_done);
            let handle = thread::spawn(move || {
                // Count successful reads to ensure readers actually run.
                let mut reads = 0usize;
                while !done.load(Ordering::Relaxed) {
                    let (a, b) = r.read(|p| (p.a, p.b));
                    // The seqlock guarantees that a reader never observes a partially-updated value.
                    assert_eq!(
                        a, b,
                        "Reader observed inconsistent values: a = {a}, b = {b}",
                    );
                    reads += 1;
                }
                // After writer is done, do a few more final checks.
                for _ in 0..10 {
                    let (a, b) = r.read(|p| (p.a, p.b));
                    assert_eq!(a, b);
                    reads += 1;
                }
                reads
            });
            reader_handles.push(handle);
        }

        let num_writes = 20000;
        // Move the writer into its own thread and perform many updates.
        let writer_handle = {
            thread::spawn(move || {
                // Perform many updates. Within the write callback we intentionally
                // perform a split update (write 'a' then sleep then write 'b')
                // to create a window where a naive reader could observe inconsistent state.
                for i in 1..=num_writes {
                    unsafe {
                        writer.write(|p| {
                            p.a = i;
                            // Small pause to widen the race window if seqlock were broken.
                            thread::sleep(Duration::from_nanos(100));
                            p.b = i;
                        })
                    };
                    // Give readers some time to run between writes.
                    if i % 100 == 0 {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
            })
        };

        // Wait for writer to finish, then tell readers to stop.
        writer_handle.join().expect("writer panicked");
        writer_done.store(true, Ordering::Relaxed);

        // Collect reader results and ensure they performed some reads.
        let mut total_reads = 0usize;
        for h in reader_handles {
            let reads = h.join().expect("reader panicked");
            total_reads += reads;
        }

        dbg!(total_reads);
        assert!(
            total_reads > 10,
            "Readers should have performed at least ten successful reads"
        );

        // Final sanity check: read final value and ensure a == b with expected final value.
        let final_pair = reader.read(|p| (p.a, p.b));
        assert_eq!(
            final_pair.0, final_pair.1,
            "Final read observed inconsistent values"
        );
        assert_eq!(
            final_pair.0, num_writes,
            "Final value should be last writer value"
        );
    }

    fn assert_correct_send_sync() {
        use std::cell::Cell;
        use std::rc::Rc;
        use std::sync::Mutex;

        use static_assertions::{assert_impl_all, assert_not_impl_any};

        // Writer is Send but not Sync
        assert_impl_all!(SeqLockWriter<u32>: Send);
        assert_not_impl_any!(SeqLockWriter<u32>: Sync);
        // it works only if we wrap it in a sync abstraction.
        assert_impl_all!(Mutex<SeqLockWriter<u32>>: Sync);
        // if the wrapped type is not Send, Send doesn't work.
        assert_not_impl_any!(SeqLockWriter<Rc<u32>>: Send);

        // Reader is Send AND Sync
        assert_impl_all!(SeqLockReader<u32>: Sync, Send);

        // but only if the wrapped type is also Sync/Send
        //
        // Cell is Send, but not Sync
        assert_impl_all!(SeqLockReader<Cell<u32>>: Send);
        assert_not_impl_any!(SeqLockReader<Cell<u32>>: Sync);
        // Rc is neither
        assert_not_impl_any!(SeqLockReader<Rc<u32>>: Sync, Send);
        assert_not_impl_any!(SeqLockReader<Arc<Cell<u32>>>: Sync, Send);
    }
}
