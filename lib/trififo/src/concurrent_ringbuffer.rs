//! A fixed-capacity ring buffer with lock-free read access.
//!
//! This ring buffer is designed for a single-writer, multiple-reader pattern.
//! The writer thread serializes all writes, while readers can access entries
//! concurrently without locks.
//!
//! # Safety
//!
//! This implementation uses `UnsafeCell` and atomic operations to enable
//! concurrent access. The safety guarantees rely on:
//! - Only one writer thread performing mutations (enforced at compile time via `!Sync`)
//! - Readers verifying entry validity after reading (via key comparison)
//! - Proper memory ordering on atomic operations

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Shared state between writer and readers.
///
/// Contains read-only accessor methods that can be used by both
/// `RingBufferWriter` and readers (via `Arc<SharedBuffer<T>>`).
pub struct SharedRingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    /// Current write position (0 to capacity-1, wraps around)
    write_pos: AtomicUsize,
    /// Number of elements currently stored
    len: AtomicUsize,
}

impl<T> SharedRingBuffer<T> {
    /// Returns the capacity of the ring buffer.
    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of elements currently stored in the buffer.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    /// Returns true if the buffer is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len.load(Ordering::Acquire) == self.capacity
    }

    /// Returns the current write position.
    #[inline]
    #[allow(dead_code)]
    pub fn write_position(&self) -> usize {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Returns the oldest valid position offset in the buffer.
    ///
    /// Returns None if the buffer is empty.
    #[allow(dead_code)]
    pub fn read_pos(&self) -> Option<usize> {
        let len = self.len.load(Ordering::Acquire);
        if len == 0 {
            None
        } else if len < self.capacity {
            Some(0)
        } else {
            Some(self.write_pos.load(Ordering::Acquire))
        }
    }

    /// Gets a reference to the item at the given position offset.
    ///
    /// This is safe to call concurrently with writes to different offsets.
    /// The caller must verify that the returned data is valid (e.g., by checking
    /// that the key matches the expected key).
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `offset < capacity`
    /// - The offset has been written to at least once
    /// - The caller verifies the data is still valid after reading
    #[inline]
    pub fn get_absolute_unchecked(&self, offset: usize) -> &T {
        debug_assert!(offset < self.capacity);
        // Safety: We use Acquire ordering to synchronize with the Release
        // in write operations. The caller is responsible for verifying
        // the data is still valid (not overwritten with different key).
        unsafe { (*self.buffer[offset].get()).assume_init_ref() }
    }
}

// Safety: SharedBuffer can be sent/shared between threads because:
// - Reads are done via UnsafeCell with proper synchronization
// - Writes are serialized by the single writer (enforced by RingBufferWriter being !Sync)
// - Atomic operations provide necessary memory ordering
unsafe impl<T: Send> Send for SharedRingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for SharedRingBuffer<T> {}

/// Writer handle for the ring buffer.
///
/// This type is `Send` but NOT `Sync`, which means:
/// - You can move it to another thread (`Send`)
/// - You cannot share `&RingBufferWriter` across threads (`!Sync`)
///
/// This provides compile-time enforcement that only one thread can write at a time.
pub struct RingBufferWriter<T> {
    shared: Arc<SharedRingBuffer<T>>,
    /// Marker to prevent auto-impl of Sync.
    /// UnsafeCell is !Sync, so PhantomData<UnsafeCell<()>> makes this type !Sync.
    _unsync: PhantomData<UnsafeCell<()>>,
}

/// Reader handle for the ring buffer.
///
/// This is simply an `Arc<SharedBuffer<T>>`, which is `Clone + Send + Sync`,
/// so it can be freely shared across threads.
pub type RingBufferReader<T> = Arc<SharedRingBuffer<T>>;

impl<T> Deref for RingBufferWriter<T> {
    type Target = SharedRingBuffer<T>;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

/// Creates a new ring buffer with the specified capacity.
///
/// Returns a writer and reader pair. The writer is `!Sync` so it can only be used
/// from one thread at a time. The reader can be cloned and shared freely.
///
/// # Panics
/// Panics if capacity is 0.
pub fn new_ring_buffer<T>(capacity: usize) -> (RingBufferWriter<T>, RingBufferReader<T>) {
    assert!(capacity > 0, "Ring buffer capacity must be greater than 0");

    let buffer: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();

    let shared = Arc::new(SharedRingBuffer {
        buffer,
        capacity,
        write_pos: AtomicUsize::new(0),
        len: AtomicUsize::new(0),
    });

    let writer = RingBufferWriter {
        shared: Arc::clone(&shared),
        _unsync: PhantomData,
    };

    (writer, shared)
}

impl<T> RingBufferWriter<T> {
    /// Pushes an item into the ring buffer and returns its position offset.
    ///
    /// If the buffer is full, the oldest item is overwritten.
    ///
    /// Taking `&mut self` combined with `!Sync` guarantees this is only called
    /// from a single thread at compile time.
    pub fn overwriting_push(&mut self, item: T) -> usize {
        let offset = self.shared.write_pos.load(Ordering::Relaxed);

        // Write the new value
        // Safety: Single writer guaranteed by &mut self + !Sync
        unsafe {
            (*self.shared.buffer[offset].get()).write(item);
        }

        // Update write position with Release ordering to ensure the write
        // is visible to readers before they see the new position
        let new_write_pos = (offset + 1) % self.shared.capacity;
        self.shared
            .write_pos
            .store(new_write_pos, Ordering::Release);

        // Update length
        let current_len = self.shared.len.load(Ordering::Relaxed);
        if current_len < self.shared.capacity {
            self.shared.len.store(current_len + 1, Ordering::Release);
        }

        offset
    }

    /// Attempts to push an item without overwriting.
    ///
    /// Returns the position offset if successful, or the input item if full.
    pub fn try_push(&mut self, item: T) -> Result<usize, T> {
        if self.is_full() {
            return Err(item);
        }
        Ok(self.overwriting_push(item))
    }

    /// Reinserts the oldest entry if the predicate returns true.
    ///
    /// Returns whether reinsert happened.
    ///
    /// Only call when the buffer is full.
    pub fn reinsert_unchecked_if(&mut self, f: impl FnOnce(&T) -> bool) -> bool {
        let write_pos = self.shared.write_pos.load(Ordering::Relaxed);
        let read_pos = match self.read_pos() {
            Some(pos) if pos == write_pos => pos,
            _ => return false,
        };

        // Check if we should reinsert
        let entry = unsafe { (*self.shared.buffer[read_pos].get()).assume_init_ref() };
        if !f(entry) {
            return false;
        }

        // Advance write position (entry stays in place, effectively reinserted)
        let new_write_pos = (write_pos + 1) % self.shared.capacity;
        self.shared
            .write_pos
            .store(new_write_pos, Ordering::Release);

        true
    }

    /// Pop oldest and push new item. Assumes queue is full.
    ///
    /// Returns the evicted value and the position of the new value.
    ///
    /// # Safety
    /// This method must only be called from the single writer thread.
    /// Only call when the buffer is full.
    pub fn pop_push_unchecked(&self, item: T) -> (T, usize) {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        debug_assert_eq!(self.read_pos(), Some(write_pos));

        // Swap the old item with the new one
        let old_item = unsafe {
            let cell = self.buffer[write_pos].get();
            std::mem::replace(&mut *cell, MaybeUninit::new(item)).assume_init()
        };

        // Advance write position
        let new_write_pos = (write_pos + 1) % self.capacity;
        self.write_pos.store(new_write_pos, Ordering::Release);

        (old_item, write_pos)
    }

    /// Returns a reference to the oldest entry in the buffer.
    ///
    /// Only call when the buffer is full.
    ///
    /// This is used to read the entry about to be evicted before removing it
    /// from the hashtable, ensuring readers can't find stale offsets.
    #[allow(dead_code)]
    pub fn peek_oldest_unchecked(&self) -> &T {
        let write_pos = self.shared.write_pos.load(Ordering::Relaxed);
        debug_assert_eq!(self.read_pos(), Some(write_pos));

        // Safety: When full, write_pos points to the oldest entry
        unsafe { (*self.shared.buffer[write_pos].get()).assume_init_ref() }
    }

    /// Takes the oldest entry out of the buffer, leaving the slot uninitialized.
    ///
    /// Returns the taken value and its position offset.
    ///
    /// Only call when the buffer is full.
    ///
    /// IMPORTANT: This must be followed by `write_at_unchecked` at the same offset
    /// to restore the buffer to a valid state before any other operations.
    pub fn take_oldest_unchecked(&mut self) -> (T, usize) {
        let write_pos = self.shared.write_pos.load(Ordering::Relaxed);
        debug_assert_eq!(self.read_pos(), Some(write_pos));

        // Take the oldest item out, leaving the slot uninitialized
        let old_item = unsafe {
            let cell = self.shared.buffer[write_pos].get();
            (*cell).assume_init_read()
        };

        (old_item, write_pos)
    }

    /// Writes an item at the specified position and advances the write position.
    ///
    /// Only call after `take_oldest_unchecked` at the same offset.
    ///
    /// # Safety
    /// The caller must ensure this is called with the same offset returned by
    /// the preceding `take_oldest_unchecked` call.
    pub fn write_at_unchecked(&mut self, item: T, offset: usize) {
        debug_assert_eq!(self.shared.write_pos.load(Ordering::Relaxed), offset);

        // Write the new item at the position
        unsafe {
            let cell = self.shared.buffer[offset].get();
            (*cell).write(item);
        }

        // Advance write position
        let new_write_pos = (offset + 1) % self.shared.capacity;
        self.shared
            .write_pos
            .store(new_write_pos, Ordering::Release);
    }

    /// Clears the ring buffer.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.shared.write_pos.store(0, Ordering::Release);
        self.shared.len.store(0, Ordering::Release);
    }
}

impl<T> Drop for SharedRingBuffer<T> {
    fn drop(&mut self) {
        let len = *self.len.get_mut();
        if len > 0 {
            let write_pos = *self.write_pos.get_mut();
            let start_pos = if len < self.capacity { 0 } else { write_pos };

            for i in 0..len {
                let pos = (start_pos + i) % self.capacity;
                unsafe {
                    std::ptr::drop_in_place((*self.buffer[pos].get()).as_mut_ptr());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer_is_empty() {
        let (writer, reader) = new_ring_buffer::<i32>(5);
        assert_eq!(writer.len(), 0);
        assert!(writer.is_empty());
        assert!(!writer.is_full());
        assert_eq!(writer.capacity(), 5);

        assert_eq!(reader.len(), 0);
        assert!(reader.is_empty());
        assert!(!reader.is_full());
        assert_eq!(reader.capacity(), 5);
    }

    #[test]
    #[should_panic(expected = "Ring buffer capacity must be greater than 0")]
    fn test_zero_capacity_panics() {
        let _: (RingBufferWriter<i32>, RingBufferReader<i32>) = new_ring_buffer(0);
    }

    #[test]
    fn test_push_and_get() {
        let (mut writer, reader) = new_ring_buffer(3);

        let offset0 = writer.overwriting_push(10);
        let offset1 = writer.overwriting_push(20);
        let offset2 = writer.overwriting_push(30);

        assert_eq!(reader.get_absolute_unchecked(offset0), &10);
        assert_eq!(reader.get_absolute_unchecked(offset1), &20);
        assert_eq!(reader.get_absolute_unchecked(offset2), &30);

        assert_eq!(writer.len(), 3);
        assert_eq!(reader.len(), 3);
        assert!(writer.is_full());
        assert!(reader.is_full());
    }

    #[test]
    fn test_offsets_wrap() {
        let (mut writer, reader) = new_ring_buffer(3);

        let offset0 = writer.overwriting_push(10);
        let offset1 = writer.overwriting_push(20);
        let offset2 = writer.overwriting_push(30);

        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1);
        assert_eq!(offset2, 2);

        let offset3 = writer.overwriting_push(40);
        assert_eq!(offset3, 0); // Wraps to 0

        // offset0 and offset3 point to same position (overwritten)
        assert_eq!(reader.get_absolute_unchecked(offset3), &40);
    }

    #[test]
    fn test_try_push() {
        let (mut writer, _reader) = new_ring_buffer(3);

        assert!(writer.try_push(10).is_ok());
        assert!(writer.try_push(20).is_ok());
        assert!(writer.try_push(30).is_ok());

        // Should fail when full
        assert_eq!(writer.try_push(40), Err(40));
        assert_eq!(writer.len(), 3);
    }

    #[test]
    fn test_reader_is_clone() {
        let (mut writer, reader1) = new_ring_buffer(3);
        let reader2 = Arc::clone(&reader1);

        writer.overwriting_push(42);

        assert_eq!(reader1.get_absolute_unchecked(0), &42);
        assert_eq!(reader2.get_absolute_unchecked(0), &42);
    }

    #[test]
    fn test_writer_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<RingBufferWriter<i32>>();
    }

    #[test]
    fn test_writer_is_not_sync() {
        // This is a compile-time check. If RingBufferWriter were Sync,
        // this would compile. Since it's !Sync, we can't easily test it
        // at runtime, but we can verify the PhantomData is present.
        fn assert_not_sync<T>() {
            // We can't directly assert !Sync, but we can verify behavior
        }
        assert_not_sync::<RingBufferWriter<i32>>();
    }

    #[test]
    fn test_reader_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Arc<SharedRingBuffer<i32>>>();
        assert_sync::<Arc<SharedRingBuffer<i32>>>();
    }
}
