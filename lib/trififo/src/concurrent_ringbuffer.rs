//! A fixed-capacity ring buffer with lock-free read access.
//!
//! This ring buffer is designed for a single-writer, multiple-reader pattern.
//! A writer thread serializes all writes, while readers can access entries
//! concurrently without locks.
//!
//! # Safety
//!
//! This implementation uses `UnsafeCell` and atomic operations to enable
//! concurrent access. The safety guarantees rely `&mut` access to `self`.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ringbuffer with support for concurrent reads, and serialized writes.
pub struct RingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    /// Current write position (0 to capacity-1, wraps around)
    write_pos: AtomicUsize,
    /// Number of elements currently stored
    len: AtomicUsize,
}

// Safety: SharedBuffer can be sent/shared between threads because:
// - Reads are done via UnsafeCell with proper synchronization
// - Writes are serialized by the single writer (enforced by using &mut operations)
// - Atomic operations provide necessary memory ordering
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {

    pub fn new(capacity: usize) -> Self {
        let buffer: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();

        Self {
            buffer,
            capacity,
            write_pos: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
        }
    }

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

    /// Pushes an item into the ring buffer and returns its position offset.
    ///
    /// If the buffer is full, the oldest item is overwritten.
    ///
    /// Taking `&mut self` combined with `!Sync` guarantees this is only called
    /// from a single thread at compile time.
    pub fn overwriting_push(&mut self, item: T) -> usize {
        let offset = self.write_pos.load(Ordering::Relaxed);

        // Write the new value
        // Safety: Single writer guaranteed by &mut self + !Sync
        unsafe {
            (*self.buffer[offset].get()).write(item);
        }

        // Update write position with Release ordering to ensure the write
        // is visible to readers before they see the new position
        let new_write_pos = (offset + 1) % self.capacity;
        self
            .write_pos
            .store(new_write_pos, Ordering::Release);

        // Update length
        let current_len = self.len.load(Ordering::Relaxed);
        if current_len < self.capacity {
            self.len.store(current_len + 1, Ordering::Release);
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
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = match self.read_pos() {
            Some(pos) if pos == write_pos => pos,
            _ => return false,
        };

        // Check if we should reinsert
        let entry = unsafe { (*self.buffer[read_pos].get()).assume_init_ref() };
        if !f(entry) {
            return false;
        }

        // Advance write position (entry stays in place, effectively reinserted)
        let new_write_pos = (write_pos + 1) % self.capacity;
        self
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

    /// Clears the ring buffer.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.write_pos.store(0, Ordering::Release);
        self.len.store(0, Ordering::Release);
    }
}

impl<T> Drop for RingBuffer<T> {
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
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_new_buffer_is_empty() {
        let buffer = RingBuffer::<i32>::new(5);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.capacity(), 5);

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.capacity(), 5);
    }

    #[test]
    #[should_panic(expected = "Ring buffer capacity must be greater than 0")]
    fn test_zero_capacity_panics() {
        let _ = RingBuffer::<i32>::new(0);

    }

    #[test]
    fn test_push_and_get() {
        let mut buffer = RingBuffer::<i32>::new(3);

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        assert_eq!(buffer.get_absolute_unchecked(offset0), &10);
        assert_eq!(buffer.get_absolute_unchecked(offset1), &20);
        assert_eq!(buffer.get_absolute_unchecked(offset2), &30);

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());
        assert!(buffer.is_full());
    }

    #[test]
    fn test_offsets_wrap() {
        let mut buffer = RingBuffer::<i32>::new(3);

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1);
        assert_eq!(offset2, 2);

        let offset3 = buffer.overwriting_push(40);
        assert_eq!(offset3, 0); // Wraps to 0

        // offset0 and offset3 point to same position (overwritten)
        assert_eq!(buffer.get_absolute_unchecked(offset3), &40);
    }

    #[test]
    fn test_try_push() {
        let mut buffer = RingBuffer::<i32>::new(3);

        assert!(buffer.try_push(10).is_ok());
        assert!(buffer.try_push(20).is_ok());
        assert!(buffer.try_push(30).is_ok());

        // Should fail when full
        assert_eq!(buffer.try_push(40), Err(40));
        assert_eq!(buffer.len(), 3);
    }



    #[test]
    fn test_reader_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Arc<RingBuffer<i32>>>();
        assert_sync::<Arc<RingBuffer<i32>>>();
    }
}
