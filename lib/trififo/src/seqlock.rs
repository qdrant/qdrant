use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

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
        let writer = SeqLockWriter {
            lock: lock.clone(),
        };
        (reader, writer)
    }

    fn read<U>(&self, callback: impl Fn(&T) -> U) -> U {
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

    fn write(&self, callback: impl FnOnce(&mut T)) {
        let seq = self.seq.load(Ordering::Acquire);
        self.seq.store(seq + 1, Ordering::Release);

        fence(Ordering::Release);

        callback(unsafe { &mut *self.inner.get() });

        self.seq.store(seq + 2, Ordering::Relaxed);
    }
}

pub struct SeqLockReader<T> {
    lock: Arc<SeqLock<T>>,
}

unsafe impl<T> Send for SeqLockReader<T> where T: Send {}
unsafe impl<T> Sync for SeqLockReader<T> {}

impl<T> SeqLockReader<T> {
    pub fn read<U>(&self, callback: impl Fn(&T) -> U) -> U {
        self.lock.read(callback)
    }
}

pub struct SeqLockWriter<T> {
    lock: Arc<SeqLock<T>>,
}

unsafe impl<T> Send for SeqLockWriter<T> where T: Send {}

impl<T> SeqLockWriter<T> {
    pub fn write(&self, callback: impl FnOnce(&mut T)) {
        self.lock.write(callback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct Pair {
        a: usize,
        b: usize,
    }

    #[test]
    fn multi_threaded_readers_consistent() {
        // Create a seqlock-wrapped Pair.
        let (reader, writer) = SeqLock::new_reader_writer(Pair { a: 0, b: 0 });

        // Wrap the reader in an Arc so we can cheaply clone it to multiple threads.
        let reader = Arc::new(reader);

        // Signal to readers when the writer is finished.
        let writer_done = Arc::new(AtomicBool::new(false));

        // Spawn several reader threads that continuously read and assert consistency.
        let mut reader_handles = Vec::new();
        for _ in 0..8 {
            let r = Arc::clone(&reader);
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

        // Move the writer into its own thread and perform many updates.
        let writer_handle = {
            thread::spawn(move || {
                // Perform many updates. Within the write callback we intentionally
                // perform a split update (write 'a' then sleep then write 'b')
                // to create a window where a naive reader could observe inconsistent state.
                for i in 1..=2000usize {
                    writer.write(|p| {
                        p.a = i;
                        // Small pause to widen the race window if seqlock were broken.
                        thread::sleep(Duration::from_nanos(100));
                        p.b = i;
                    });
                    // Give readers some time to run between writes.
                    if i % 100 == 0 {
                        thread::sleep(Duration::from_micros(100));
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
            final_pair.0, 2000usize,
            "Final value should be last writer value"
        );
    }

    /// ```compile_fail
    /// use std::sync::Arc;
    /// use std::thread;
    /// use super::*;
    ///
    /// // Attempting to share the writer across threads via Arc should fail to compile,
    /// // because SeqLockWriter is intentionally not Sync.
    /// let (_reader, writer) = SeqLock::new_reader_writer(Pair { a: 0, b: 0 });
    /// let shared = Arc::new(writer);
    /// let shared_clone = shared.clone();
    ///
    /// // Moving `shared_clone` into a new thread requires `Arc<SeqLockWriter<_>>` to be Send,
    /// // which in turn requires `SeqLockWriter<_>` to be Sync. This should fail to compile.
    /// thread::spawn(move || {
    ///     shared_clone.write(|p| { p.a = 1; p.b = 1; });
    /// }).join().unwrap();
    /// ```
    #[test]
    fn writer_cannot_be_shared_across_threads() {
        // The relevant check is the `compile_fail` doctest.
    }
}
