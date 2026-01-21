//! A fixed-capacity ring buffer with absolute position access.
//!
//! By design, it can't evict items at the read position, it can only push, or
//! push while popping the last one.
//!
//! This module provides a simple ring buffer implementation using wrapped positions.
//! Positions wrap around at the capacity (0, 1, 2, ..., capacity-1, 0, 1, 2, ...).
//!
//! # Safety
//!
//! This ring buffer does NOT track whether a stored offset is still valid. It's the
//! caller's responsibility to track validity in an external structure and invalidate
//! offsets when items are overwritten.

use std::mem::MaybeUninit;

pub struct RingBuffer<T> {
    buffer: Vec<MaybeUninit<T>>,
    capacity: usize,
    /// Current write position (0 to capacity-1, wraps around)
    write_pos: usize,
    /// Number of elements currently stored
    len: usize,
}

impl<T> RingBuffer<T> {
    /// Creates a new ring buffer with the specified capacity.
    ///
    /// # Panics
    /// Panics if capacity is 0.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "Ring buffer capacity must be greater than 0");

        let mut buffer = Vec::with_capacity(capacity);
        // Reserve space without initializing
        for _ in 0..capacity {
            buffer.push(MaybeUninit::uninit());
        }

        Self {
            buffer,
            capacity,
            write_pos: 0,
            len: 0,
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
        self.len
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns true if the buffer is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    /// Pushes an item into the ring buffer and returns its position offset.
    ///
    /// If the buffer is full, the oldest item is overwritten.
    /// The returned offset is an absolute position that wraps around at capacity.
    ///
    /// # Note
    /// If an item is overwritten, any previously returned offset pointing to
    /// that position becomes invalid. It's the caller's responsibility to track
    /// this in an external structure.
    pub fn overwriting_push(&mut self, item: T) -> usize {
        let offset = self.write_pos;

        // Write the new value, potentially overwriting old data
        self.buffer[offset].write(item);

        // Move write position forward
        self.write_pos = (self.write_pos + 1) % self.capacity;

        // Update length
        if self.len < self.capacity {
            self.len += 1;
        }

        offset
    }

    /// Attempts to push an item into the ring buffer without overwriting.
    ///
    /// Returns the position offset if successful, or the intpu item if the buffer is full.
    pub fn try_push(&mut self, item: T) -> Result<usize, T> {
        if self.is_full() {
            return Err(item);
        }

        Ok(self.overwriting_push(item))
    }

    /// Gets a reference to the item at the given position offset.
    ///
    /// Returns None if the offset is out of bounds (>= capacity).
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `offset < capacity`
    /// - The offset points to valid (not overwritten) data
    pub fn get_absolute_unchecked(&self, offset: usize) -> Option<&T> {
        if offset >= self.capacity {
            return None;
        }

        // Safety: We only access positions that have been written to
        // The caller is responsible for ensuring the offset is still valid
        unsafe { Some(self.buffer[offset].assume_init_ref()) }
    }

    /// Returns the oldest valid position offset in the buffer.
    ///
    /// Returns None if the buffer is empty.
    /// This can be used to determine which offsets are still valid.
    pub fn read_pos(&self) -> Option<usize> {
        if self.is_empty() {
            None
        } else if self.len < self.capacity {
            // Buffer not full yet, oldest is at position 0
            Some(0)
        } else {
            // Buffer is full, oldest is at write_pos (about to be overwritten)
            Some(self.write_pos)
        }
    }

    /// Clears the ring buffer.
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.write_pos = 0;
        self.len = 0;
    }

    /// Returns the current write position (where the next item will be written).
    #[inline]
    pub fn write_position(&self) -> usize {
        self.write_pos
    }

    /// Reinserts the oldest entry into the buffer, if the closure returns true.
    ///
    /// Returns whether it was reinserted or not.
    ///
    /// # Safety
    /// This only reinserts if the buffer is full, otherwise it might panic.
    pub fn reinsert_if(&mut self, f: impl FnOnce(&T) -> bool) -> bool {
        match self.read_pos() {
            None => false,
            Some(pos) if pos == self.write_pos => {
                // The start position is the same as the position about to be overwritten
                let should_reinsert = f(unsafe { self.buffer[self.write_pos].assume_init_ref() });

                if !should_reinsert {
                    return false;
                }

                // Advance the write position
                self.write_pos = (self.write_pos + 1) % self.capacity;

                true
            }
            Some(_pos) => {
                debug_assert!(false, "Detected reinsertion when ringbuffer is not full");
                false
            }
        }
    }

    /// Pop oldest, and push new item. Returns the oldest value, and the absolute position of the new value
    ///
    /// Only use this if the queue was full, otherwise, use `overwriting_push`. Panics if queue wasn't full
    pub fn pop_push(&mut self, item: T) -> (T, usize) {
        debug_assert_eq!(self.read_pos().unwrap(), self.write_pos);

        let read_pos = self.write_pos;

        let old_item = std::mem::replace(&mut self.buffer[read_pos], MaybeUninit::new(item));
        let old_item = unsafe { old_item.assume_init() };

        self.write_pos = (self.write_pos + 1) % self.capacity;

        (old_item, read_pos)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Drop all initialized elements
        if self.len > 0 {
            let start_pos = if self.len < self.capacity {
                0
            } else {
                self.write_pos
            };

            for i in 0..self.len {
                let pos = (start_pos + i) % self.capacity;
                unsafe {
                    self.buffer[pos].assume_init_drop();
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
        let buffer: RingBuffer<i32> = RingBuffer::with_capacity(5);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.capacity(), 5);
    }

    #[test]
    #[should_panic(expected = "Ring buffer capacity must be greater than 0")]
    fn test_zero_capacity_panics() {
        let _buffer: RingBuffer<i32> = RingBuffer::with_capacity(0);
    }

    #[test]
    fn test_push_and_get() {
        let mut buffer = RingBuffer::with_capacity(3);

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        assert_eq!(buffer.get_absolute_unchecked(offset0), Some(&10));
        assert_eq!(buffer.get_absolute_unchecked(offset1), Some(&20));
        assert_eq!(buffer.get_absolute_unchecked(offset2), Some(&30));

        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());
    }

    #[test]
    fn test_offsets_wrap() {
        let mut buffer = RingBuffer::with_capacity(3);

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        // These offsets should wrap around
        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1);
        assert_eq!(offset2, 2);

        let offset3 = buffer.overwriting_push(40);
        assert_eq!(offset3, 0); // Wraps to 0

        // offset0 and offset3 point to same position
        assert_eq!(buffer.get_absolute_unchecked(offset0), Some(&40)); // Overwritten!
        assert_eq!(buffer.get_absolute_unchecked(offset3), Some(&40));
    }

    #[test]
    fn test_overwrites_oldest() {
        let mut buffer = RingBuffer::with_capacity(3);

        buffer.overwriting_push(10);
        buffer.overwriting_push(20);
        buffer.overwriting_push(30);
        buffer.overwriting_push(40); // Overwrites first item

        // Positions 1 and 2 still have their original values
        assert_eq!(buffer.get_absolute_unchecked(1), Some(&20));
        assert_eq!(buffer.get_absolute_unchecked(2), Some(&30));
        // Position 0 now has the new value
        assert_eq!(buffer.get_absolute_unchecked(0), Some(&40));

        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());
    }

    #[test]
    fn test_continuous_wrapping() {
        let mut buffer = RingBuffer::with_capacity(3);
        let mut offsets = Vec::new();

        for i in 0..10 {
            offsets.push(buffer.overwriting_push(i));
        }

        // Offsets should cycle: 0, 1, 2, 0, 1, 2, ...
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], 1);
        assert_eq!(offsets[2], 2);
        assert_eq!(offsets[3], 0);
        assert_eq!(offsets[6], 0);
        assert_eq!(offsets[9], 0);

        // Only last 3 items should be accessible
        assert_eq!(buffer.get_absolute_unchecked(0), Some(&9)); // Position 0
        assert_eq!(buffer.get_absolute_unchecked(1), Some(&7)); // Position 1
        assert_eq!(buffer.get_absolute_unchecked(2), Some(&8)); // Position 2

        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_oldest_valid_offset() {
        let mut buffer = RingBuffer::with_capacity(3);

        assert_eq!(buffer.read_pos(), None);

        buffer.overwriting_push(10);
        assert_eq!(buffer.read_pos(), Some(0));

        buffer.overwriting_push(20);
        buffer.overwriting_push(30);
        assert_eq!(buffer.read_pos(), Some(0)); // Not full yet

        buffer.overwriting_push(40); // Now full and wrapping
        assert_eq!(buffer.read_pos(), Some(1)); // Oldest is at position 1
    }

    #[test]
    fn test_clear() {
        let mut buffer = RingBuffer::with_capacity(3);

        buffer.overwriting_push(10);
        buffer.overwriting_push(20);
        buffer.overwriting_push(30);

        assert_eq!(buffer.len(), 3);

        buffer.clear();

        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(buffer.read_pos(), None);
        assert_eq!(buffer.write_position(), 0);
    }

    #[test]
    fn test_single_capacity() {
        let mut buffer = RingBuffer::with_capacity(1);

        let offset0 = buffer.overwriting_push(10);
        assert_eq!(buffer.get_absolute_unchecked(offset0), Some(&10));
        assert!(buffer.is_full());

        let offset1 = buffer.overwriting_push(20);
        assert_eq!(offset1, 0); // Same position
        assert_eq!(buffer.get_absolute_unchecked(offset1), Some(&20)); // Overwritten
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn test_try_push() {
        let mut buffer = RingBuffer::with_capacity(3);

        // Should succeed when not full
        assert!(buffer.try_push(10).is_ok());
        assert!(buffer.try_push(20).is_ok());
        assert!(buffer.try_push(30).is_ok());

        // Should fail when full
        assert_eq!(buffer.try_push(40), Err(40));
        assert_eq!(buffer.len(), 3);

        // Regular push should still work and overwrite
        let offset = buffer.overwriting_push(40);
        assert_eq!(buffer.get_absolute_unchecked(offset), Some(&40));

        // try_push should fail again since it's full
        assert_eq!(buffer.try_push(50), Err(50));
    }

    #[test]
    fn test_comparison_with_ringbuf_crate() {
        use ringbuf::HeapRb;
        use ringbuf::traits::{Consumer, Observer, RingBuffer as RingBufferTrait};

        const CAPACITY: usize = 5;
        let mut our_buffer = super::RingBuffer::with_capacity(CAPACITY);
        let mut their_buffer: HeapRb<i32> = HeapRb::new(CAPACITY);

        // Push some initial items
        for i in 0..3 {
            our_buffer.overwriting_push(i);
            their_buffer.push_overwrite(i);
        }

        // Verify both have same length
        assert_eq!(our_buffer.len(), their_buffer.occupied_len());

        // Push more items to cause wrapping
        for i in 3..10 {
            our_buffer.overwriting_push(i);
            their_buffer.push_overwrite(i);
        }

        // Both should have same length
        assert_eq!(our_buffer.len(), their_buffer.occupied_len());
        assert_eq!(our_buffer.len(), CAPACITY);

        // Verify their buffer has same items
        let their_items: Vec<_> = their_buffer.iter().copied().collect();
        let our_items: Vec<_> = (0..our_buffer.len()).map(|_| our_buffer.pop_push(0).0).collect();

        assert_eq!(our_items, their_items);
    }

    #[test]
    fn test_sequential_access_pattern() {
        // Simulate a use case where we track offsets in a separate structure
        let mut buffer = RingBuffer::with_capacity(4);
        let mut tracker = std::collections::HashMap::new();

        // Add items and track them
        for i in 0..3 {
            let offset = buffer.overwriting_push(i * 10);
            tracker.insert(format!("key{}", i), offset);
        }

        // Access via tracker
        assert_eq!(buffer.get_absolute_unchecked(tracker["key0"]), Some(&0));
        assert_eq!(buffer.get_absolute_unchecked(tracker["key1"]), Some(&10));
        assert_eq!(buffer.get_absolute_unchecked(tracker["key2"]), Some(&20));

        // Track which keys map to which offsets before wrapping
        let key0_offset = tracker["key0"];
        let key3_offset = 3 % buffer.capacity(); // Will be offset 3

        // Add more items, causing wrapping
        for i in 3..8 {
            let offset = buffer.overwriting_push(i * 10);
            tracker.insert(format!("key{}", i), offset);
        }

        // Note: After pushing 3,4,5,6,7 (5 more items into capacity-4 buffer),
        // positions 0,1,2,3 get reused. The valid range is now [0,1,2,3] containing [40,50,60,70]

        // Manually invalidate keys that were overwritten
        // key0 (offset 0) was overwritten by key4 (also offset 0)
        // key1 (offset 1) was overwritten by key5 (also offset 1)
        // key2 (offset 2) was overwritten by key6 (also offset 2)
        // key3 (offset 3) was overwritten by key7 (also offset 3)

        tracker.remove("key0");
        tracker.remove("key1");
        tracker.remove("key2");
        tracker.remove("key3");

        assert_eq!(tracker.len(), 4);

        // Recent keys should still work
        assert_eq!(buffer.get_absolute_unchecked(tracker["key4"]), Some(&40));
        assert_eq!(buffer.get_absolute_unchecked(tracker["key5"]), Some(&50));
        assert_eq!(buffer.get_absolute_unchecked(tracker["key6"]), Some(&60));
        assert_eq!(buffer.get_absolute_unchecked(tracker["key7"]), Some(&70));
    }

}
