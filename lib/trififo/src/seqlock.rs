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
            _unsend: std::marker::PhantomData,
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
    _unsend: std::marker::PhantomData<*mut ()>,
}

unsafe impl<T> Send for SeqLockWriter<T> where T: Send {}

impl<T> SeqLockWriter<T> {
    pub fn write(&self, callback: impl FnOnce(&mut T)) {
        self.lock.write(callback)
    }
}
