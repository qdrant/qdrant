use std::mem::MaybeUninit;

use bitvec::array::BitArray;

/// A queue that accepts items in an arbitrary order (with limitations), and
/// reorders them based on their sequence numbers.
///
/// Has some sensible limitations on the order of accepted items to make it
/// usable within `io_uring` executor. Rules: start seqnums from 0, don't
/// spread them more than `DEPTH` elements apart, don't skip seqnums.
pub struct ReorderingQueue<T, const DEPTH: usize> {
    next_read: usize,
    occupied: OccupiedBitArray,
    buffer: [MaybeUninit<T>; DEPTH],
}

type OccupiedBitArray = BitArray<[u64; 1]>;
const MAX_DEPTH: usize = bitvec::mem::bits_of::<OccupiedBitArray>();

impl<T, const DEPTH: usize> ReorderingQueue<T, DEPTH> {
    /// Should be cheap as we don't zero-fill the buffer.
    pub fn new() -> Self {
        const { assert!(DEPTH <= MAX_DEPTH) };
        Self {
            next_read: 0,
            occupied: BitArray::ZERO,
            buffer: [const { MaybeUninit::uninit() }; _],
        }
    }

    /// Puts an item into the queue.
    ///
    /// Panics if rules mentioned in the [`ReorderingQueue`] doc are violated.
    pub fn put(&mut self, seqnum: usize, value: T) {
        let (min_seqnum, max_seqnum) = (self.next_read, self.next_read + DEPTH - 1);
        assert!(
            (min_seqnum..=max_seqnum).contains(&seqnum),
            "seqnums spread too far apart: {seqnum} is not in {min_seqnum}..={max_seqnum}",
        );
        assert!(!self.occupied[seqnum % DEPTH], "duplicate seqnum {seqnum}");

        let slot = seqnum % DEPTH;
        self.occupied.set(slot, true);
        self.buffer[slot].write(value);
    }

    /// Takes a single item out of this queue. You can assume that returned
    /// seqnums are strictly sequential across all calls to this method. A value
    /// of `None` means that the item with the next expected seqnum is not yet
    /// available (but items with higher seqnums might be already buffered).
    pub fn get(&mut self) -> Option<(usize, T)> {
        let slot = self.next_read % DEPTH;
        if !self.occupied[slot] {
            return None;
        }
        self.occupied.set(slot, false);
        self.next_read += 1;
        let value = unsafe { self.buffer[slot].assume_init_read() };
        Some((self.next_read - 1, value))
    }

    pub fn is_empty(&self) -> bool {
        self.occupied.not_any()
    }
}

impl<T, const DEPTH: usize> Drop for ReorderingQueue<T, DEPTH> {
    fn drop(&mut self) {
        for i in self.occupied.iter_ones() {
            unsafe { self.buffer[i].assume_init_drop() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() {
        let mut q = ReorderingQueue::<_, 6>::new();
        assert!(q.is_empty());
        q.put(0, "0");
        q.put(2, "2");
        q.put(3, "3");

        assert_eq!(q.get(), Some((0, "0")));
        assert_eq!(q.get(), None); // 1 is not yet available

        q.put(1, "1");
        assert_eq!(q.get(), Some((1, "1")));
        assert_eq!(q.get(), Some((2, "2")));
        assert_eq!(q.get(), Some((3, "3")));
        assert_eq!(q.get(), None);

        q.put(9, "9"); // This is max accepted ID (6 + 3)
        assert_eq!(q.get(), None);
        q.put(8, "8");
        assert_eq!(q.get(), None);
        q.put(7, "7");
        assert_eq!(q.get(), None);
        q.put(6, "6");
        assert_eq!(q.get(), None);
        q.put(5, "5");
        assert_eq!(q.get(), None);
        q.put(4, "4");
        assert_eq!(q.get(), Some((4, "4")));
        assert_eq!(q.get(), Some((5, "5")));
        assert_eq!(q.get(), Some((6, "6")));
        assert_eq!(q.get(), Some((7, "7")));
        assert_eq!(q.get(), Some((8, "8")));
        assert_eq!(q.get(), Some((9, "9")));
        assert_eq!(q.get(), None);
    }

    #[test]
    fn test_max() {
        let mut q = ReorderingQueue::<_, MAX_DEPTH>::new();
        q.put(0, 0.to_string());
        assert_eq!(q.get(), Some((0, 0.to_string())));

        for i in 1..MAX_DEPTH + 1 {
            q.put(i, i.to_string());
        }
        for i in 1..MAX_DEPTH + 1 {
            assert_eq!(q.get(), Some((i, i.to_string())));
        }
    }

    #[test]
    #[should_panic(expected = "seqnums spread too far apart")]
    fn test_max_plus_1() {
        let mut q = ReorderingQueue::<_, MAX_DEPTH>::new();
        q.put(0, 0.to_string());
        assert_eq!(q.get(), Some((0, 0.to_string())));

        for i in 1..MAX_DEPTH + 2 {
            q.put(i, i.to_string());
        }
    }
}
