use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Fixed-size ringbuffer with support for absolute indexing into its contents.
///
/// This needs to exist, and not use external crates because we need absolute indexing.
/// The only crate which allows that is `ringbuffer`, but it has a specific format which
/// includes an epoch number, and cannot be constructed out of thin air. We want to be able
/// to use u32/usize indexing directly.
///
/// There is explicitly no way to remove elements before they are overwritten by
/// filling the capacity. This can be changed, but other methods like `is_in_range` would
/// need to be reviewed carefully.
pub struct RingBuffer<T> {
    /// Container for elements
    buffer: Box<[MaybeUninit<T>]>,

    /// Maximum number of elements in the buffer
    capacity: NonZeroUsize,

    /// Current write position (0 to capacity-1, wraps around)
    write_pos: AtomicUsize,

    /// Number of elements currently stored
    len: AtomicUsize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let buffer: Box<[MaybeUninit<T>]> =
            (0..capacity.get()).map(|_| MaybeUninit::uninit()).collect();

        Self {
            buffer,
            capacity,
            write_pos: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
        }
    }

    /// Returns the number of elements currently stored in the buffer.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Returns true if the buffer is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len.load(Ordering::Acquire) == self.capacity.get()
    }

    /// Returns the current write position.
    #[inline]
    pub fn write_position(&self) -> usize {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Returns the oldest valid position offset in the buffer.
    ///
    /// Returns None if the buffer is empty.
    pub fn read_position(&self) -> Option<usize> {
        let len = self.len.load(Ordering::Acquire);
        if len == 0 {
            None
        } else if len < self.capacity.get() {
            Some(0)
        } else {
            Some(self.write_pos.load(Ordering::Acquire))
        }
    }

    /// Checks whether the position has an active value in the buffer.
    #[inline]
    fn is_in_range(&self, position: usize) -> bool {
        // Since we are never shrinking the number of active elements
        // in `self.buffer`, we can assume that, as long as position is
        // within 0..self.len, there is something initialized in that slot.
        position < self.len.load(Ordering::Acquire)
    }

    /// Gets a reference to the item at the given position offset.
    ///
    /// # SeqLock un/safety
    /// If there is a concurrent writer, this will still check bounds appropriately
    /// because it uses atomics for bounds.
    ///
    /// The value itself is not protected and can exhibit a torn read.
    #[inline]
    pub fn get_absolute(&self, position: usize) -> Option<&T> {
        if !self.is_in_range(position) {
            return None;
        }

        // Safety: We just checked the value is within bounds
        let value = &self.buffer[position];
        let value = unsafe { value.assume_init_ref() };
        Some(value)
    }

    /// Pushes an item into the ring buffer and returns its position offset.
    ///
    /// If the buffer is full, the oldest item is overwritten.
    pub fn overwriting_push(&mut self, item: T) -> usize {
        let offset = self.write_pos.load(Ordering::Relaxed);
        let current_len = self.len.load(Ordering::Relaxed);

        // Write the new value
        if offset >= current_len {
            // write into an uninitialized slot
            self.buffer[offset].write(item);
        } else {
            // SAFETY: It is within initialized bounds
            let slot = unsafe { self.buffer[offset].assume_init_mut() };
            *slot = item;
        }

        // Update write position with Release ordering to ensure the write
        // is visible to readers before they see the new position
        let new_write_pos = self.next_position(offset);
        self.write_pos.store(new_write_pos, Ordering::Release);

        // Update length
        if current_len < self.capacity.get() {
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
    /// Returns whether the reinsertion happened.
    ///
    /// Only works when the buffer is full.
    pub fn reinsert_if(&mut self, f: impl FnOnce(&T) -> bool) -> bool {
        let write_pos = self.write_pos.load(Ordering::Relaxed);
        let read_pos = match self.read_position() {
            Some(pos) if pos == write_pos => pos,
            // Buffer not full, won't reinsert
            _ => return false,
        };

        // Ask the callback if we should reinsert
        let entry = unsafe { (self.buffer[read_pos]).assume_init_ref() };
        if !f(entry) {
            return false;
        }

        // Advance write position (entry stays in place, effectively reinserted)
        let new_write_pos = self.next_position(write_pos);
        self.write_pos.store(new_write_pos, Ordering::Release);

        true
    }

    #[inline]
    fn next_position(&self, position: usize) -> usize {
        let next_position = position + 1;
        if next_position < self.capacity.get() {
            next_position
        } else {
            0
        }
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let len = *self.len.get_mut();

        for pos in 0..len {
            unsafe {
                std::ptr::drop_in_place(self.buffer[pos].as_mut_ptr());
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_push_and_get() {
        let mut buffer = RingBuffer::<i32>::new(NonZeroUsize::try_from(3).unwrap());

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        assert_eq!(buffer.get_absolute(offset0).unwrap(), &10);
        assert_eq!(buffer.get_absolute(offset1).unwrap(), &20);
        assert_eq!(buffer.get_absolute(offset2).unwrap(), &30);

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());
        assert!(buffer.is_full());
    }

    #[test]
    fn test_offsets_wrap() {
        let mut buffer = RingBuffer::<i32>::new(NonZeroUsize::try_from(3).unwrap());

        let offset0 = buffer.overwriting_push(10);
        let offset1 = buffer.overwriting_push(20);
        let offset2 = buffer.overwriting_push(30);

        assert_eq!(offset0, 0);
        assert_eq!(offset1, 1);
        assert_eq!(offset2, 2);

        let offset3 = buffer.overwriting_push(40);
        assert_eq!(offset3, 0); // Wraps to 0

        // offset0 and offset3 point to same position (overwritten)
        assert_eq!(buffer.get_absolute(offset3).unwrap(), &40);
        assert_eq!(buffer.get_absolute(offset0).unwrap(), &40);
    }

    #[test]
    fn test_try_push() {
        let mut buffer = RingBuffer::<i32>::new(NonZeroUsize::try_from(3).unwrap());

        assert!(buffer.try_push(10).is_ok());
        assert!(buffer.try_push(20).is_ok());
        assert!(buffer.try_push(30).is_ok());

        // Should fail when full
        assert_eq!(buffer.try_push(40), Err(40));
        assert_eq!(buffer.len(), 3);
    }
}
