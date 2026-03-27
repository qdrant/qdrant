//! Atomic bit-vector and bit-slice types backed by `AtomicU64` words.
//!
//! # Design
//! The module mirrors the `BitVec` / `BitSlice` split from the `bitvec` crate:
//!
//! * [`AtomicBitSlice`] — a *borrowed* view over a `[AtomicU64]` with a
//!   logical bit length. Provides all read, concurrent-replace, and iteration
//!   operations through shared (`&self`) references.
//! * [`AtomicBitVec`] — an *owned* bit vector. Provides constructors,
//!   `resize`, and a [`as_slice`][AtomicBitVec::as_slice] method. Delegates
//!   slice-level operations to the contained [`AtomicBitSlice`].
//!
//! Bit ordering is `Lsb0`: bit `i` lives in word `i / 64` at position
//! `i % 64` within that word.
//!
//! # Popcount
//! [`AtomicBitVec`] keeps a separate [`AtomicI64`] popcount cache. Because
//! the bit-word update and the popcount update are two distinct atomic
//! operations, the counter can transiently dip below zero under concurrent
//! use. [`AtomicBitVec::count_ones`] clamps negative values to `0`.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::bitvec::BitVec;

const BITS_PER_WORD: usize = u64::BITS as usize;

// ═══════════════════════════ AtomicBitSlice ═══════════════════════════════

/// A borrowed view over a sequence of [`AtomicU64`] words, interpreted as a
/// flat bit array with `Lsb0` ordering.
///
/// `AtomicBitSlice` is cheap to copy and provides all read, replace, and
/// iteration operations.
///
/// Acquire `AtomicBitSlice` via [`AtomicBitVec::as_slice`]. Every
/// `replace_concurrent` call automatically keeps the owning
/// [`AtomicBitVec`]'s popcount cache up-to-date via the borrowed reference.
#[derive(Debug, Copy, Clone)]
pub struct AtomicBitSlice<'a> {
    data: &'a [AtomicU64],
    /// Logical number of bits (≤ `data.len() * BITS_PER_WORD`).
    bit_len: usize,
    /// Popcount cache of the owning [`AtomicBitVec`]; updated on every replace.
    popcount: &'a AtomicI64,
}

// ── Service ───────────────────────────────────────────────────────────────

impl<'a> AtomicBitSlice<'a> {
    /// Number of logical bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.bit_len
    }

    /// Returns `true` when the slice contains no bits.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Reference to the underlying [`AtomicU64`] words.
    #[inline]
    pub fn as_raw_slice(&self) -> &'a [AtomicU64] {
        self.data
    }

    #[inline]
    pub fn to_owned(&self) -> AtomicBitVec {
        // Copy words and count ones in a single pass to avoid a TOCTOU race:
        // reading the atomic counter separately could observe mutations that
        // happened after the word snapshot was taken.
        let mut popcount: i64 = 0;
        let data: Vec<AtomicU64> = self
            .data
            .iter()
            .map(|w| {
                let val = w.load(Ordering::Acquire);
                popcount += i64::from(val.count_ones());
                AtomicU64::new(val)
            })
            .collect();
        AtomicBitVec {
            data,
            len: self.bit_len,
            popcount: AtomicI64::new(popcount),
        }
    }
}

// ── Read operations ───────────────────────────────────────────────────────

// TODO: implementing it similar to `&BitSlice` requires implementing own slice fat pointer.
impl AtomicBitSlice<'_> {
    /// Return the bit at `idx` using `Acquire` ordering.
    ///
    /// Panics `None` when `idx >= self.len()`.
    #[inline]
    pub fn get_checked(&self, idx: usize) -> Option<bool> {
        self.get_checked_with_ordering(idx, Ordering::Acquire)
    }

    /// Return the bit at `idx` using `Acquire` ordering.
    ///
    /// Panics `None` when `idx >= self.len()`.
    // TODO: implementing `Index` requires implementing own fat pointer to bit
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        self.get_checked(idx)
            .unwrap_or_else(|| panic!("the len is {} but the index is {}", self.bit_len, idx))
    }

    /// Return the bit at `idx` using `ordering`.
    ///
    /// Returns `None` when `idx >= self.len()`.
    #[inline]
    pub fn get_checked_with_ordering(&self, idx: usize, ordering: Ordering) -> Option<bool> {
        if idx >= self.bit_len {
            return None;
        }
        let word_idx = idx / BITS_PER_WORD;
        let bit_idx = idx % BITS_PER_WORD;
        let word = self.data[word_idx].load(ordering);
        Some((word >> bit_idx) & 1 == 1)
    }
}

// ── Concurrent replace (interior mutability) ──────────────────────────────

impl AtomicBitSlice<'_> {
    /// Atomically set the bit at `idx` to `value` using `AcqRel` ordering.
    ///
    /// Returns the *previous* value.
    ///
    /// # Panics
    /// Panics when `idx >= self.len()`.
    #[inline]
    pub fn replace_concurrent(&self, idx: usize, value: bool) -> bool {
        self.replace_concurrent_with_ordering(idx, value, Ordering::AcqRel)
    }

    /// Atomically set the bit at `idx` to `value` using `ordering`.
    ///
    /// Returns the *previous* value.
    ///
    /// # Panics
    /// Panics when `idx >= self.len()`.
    #[inline]
    pub fn replace_concurrent_with_ordering(
        &self,
        idx: usize,
        value: bool,
        ordering: Ordering,
    ) -> bool {
        assert!(
            idx < self.bit_len,
            "index {idx} out of bounds for AtomicBitSlice of length {}",
            self.bit_len
        );
        let word_idx = idx / BITS_PER_WORD;
        let bit_idx = idx % BITS_PER_WORD;
        let mask = 1u64 << bit_idx;

        let word = &self.data[word_idx];
        let old_word = if value {
            word.fetch_or(mask, ordering)
        } else {
            word.fetch_and(!mask, ordering)
        };

        let old = (old_word >> bit_idx) & 1 == 1;
        if old != value {
            let delta: i64 = if value { 1 } else { -1 };
            self.popcount.fetch_add(delta, Ordering::Release);
        }
        old
    }
}

// ── Iteration ─────────────────────────────────────────────────────────────

/// Iterator over bits of an [`AtomicBitSlice`].
#[derive(Copy, Clone)]
pub struct AtomicBitSliceIter<'a> {
    slice: AtomicBitSlice<'a>,
    idx: usize,
    ordering: Ordering,
}

impl Iterator for AtomicBitSliceIter<'_> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<bool> {
        let val = self
            .slice
            .get_checked_with_ordering(self.idx, self.ordering)?;
        self.idx += 1;
        Some(val)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice.bit_len.saturating_sub(self.idx);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for AtomicBitSliceIter<'_> {}

impl AtomicBitSlice<'_> {
    /// Iterate over all bits using `Acquire` ordering.
    pub fn iter(&self) -> AtomicBitSliceIter<'_> {
        self.iter_with_ordering(Ordering::Acquire)
    }

    /// Iterate over true bits positions using `Acquire` ordering.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter_with_ordering(Ordering::Acquire)
            .enumerate()
            .filter_map(|(idx, flag)| if flag { Some(idx) } else { None })
    }

    /// Iterate over false bits positions using `Acquire` ordering.
    pub fn iter_zeros(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter_with_ordering(Ordering::Acquire)
            .enumerate()
            .filter_map(|(idx, flag)| if flag { None } else { Some(idx) })
    }

    /// Iterate over all bits using the given `ordering`.
    pub fn iter_with_ordering(&self, ordering: Ordering) -> AtomicBitSliceIter<'_> {
        AtomicBitSliceIter {
            slice: *self,
            idx: 0,
            ordering,
        }
    }

    /// Iterate over all bits starting from `start` (inclusive) using the given `Acquire` ordering.
    pub fn iter_from(&self, start: usize) -> AtomicBitSliceIter<'_> {
        self.iter_with_ordering_from(start, Ordering::Acquire)
    }

    /// Iterate over all bits starting from `start` (inclusive) using the given `ordering`.
    pub fn iter_with_ordering_from(
        &self,
        start: usize,
        ordering: Ordering,
    ) -> AtomicBitSliceIter<'_> {
        AtomicBitSliceIter {
            slice: *self,
            idx: start,
            ordering,
        }
    }

    /// Count set bits in `[start, end)`.
    pub fn count_bits_in_range(&self, start: usize, end: usize) -> usize {
        let end = end.min(self.bit_len);
        if start >= end {
            return 0;
        }

        let first_word = start / BITS_PER_WORD;
        let last_word = (end - 1) / BITS_PER_WORD;

        if first_word == last_word {
            let word = self.data[first_word].load(Ordering::Relaxed);
            let sb = start % BITS_PER_WORD;
            let eb = end % BITS_PER_WORD;
            let eb = if eb == 0 { BITS_PER_WORD } else { eb };
            let mask = if eb == BITS_PER_WORD {
                u64::MAX << sb
            } else {
                ((1u64 << eb) - 1) & (u64::MAX << sb)
            };
            return (word & mask).count_ones() as usize;
        }

        let sb = start % BITS_PER_WORD;
        let first_mask = u64::MAX << sb;
        let mut count =
            (self.data[first_word].load(Ordering::Relaxed) & first_mask).count_ones() as usize;

        for w in (first_word + 1)..last_word {
            count += self.data[w].load(Ordering::Relaxed).count_ones() as usize;
        }

        let eb = end % BITS_PER_WORD;
        let last_mask = if eb == 0 { u64::MAX } else { (1u64 << eb) - 1 };
        count += (self.data[last_word].load(Ordering::Relaxed) & last_mask).count_ones() as usize;

        count
    }

    #[inline]
    pub fn count_ones(&self) -> usize {
        self.popcount
            .load(Ordering::Acquire)
            .try_into()
            .unwrap_or(0)
    }
}

// ═══════════════════════════ AtomicBitVec ═════════════════════════════════

/// An owned atomic bit vector.
///
/// Provides constructors and [`resize`][AtomicBitVec::resize], and delegates
/// all slice-level operations through [`as_slice`][AtomicBitVec::as_slice].
///
/// Keeps an [`AtomicI64`] popcount cache that may transiently be negative
/// under concurrent workloads; [`count_ones`][AtomicBitVec::count_ones] clamps
/// negative values to `0`.
// TODO bit ordering parameter like `BitVec` does.
pub struct AtomicBitVec {
    data: Vec<AtomicU64>,
    /// Logical number of bits (may be < `data.len() * BITS_PER_WORD`).
    len: usize,
    /// Approximate count of set bits; may transiently be negative.
    popcount: AtomicI64,
}

// ── Trait impls ──────────────────────────────────────────────────────────

impl Default for AtomicBitVec {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AtomicBitVec {
    fn clone(&self) -> Self {
        self.as_slice().to_owned()
    }
}

impl PartialEq for AtomicBitVec {
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        self.data
            .iter()
            .zip(other.data.iter())
            .all(|(a, b)| a.load(Ordering::Relaxed) == b.load(Ordering::Relaxed))
    }
}

impl std::fmt::Debug for AtomicBitVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AtomicBitVec {{ len: {}, popcount: {} }}",
            self.len,
            self.count_ones()
        )
    }
}

// ── Constructors ─────────────────────────────────────────────────────────

impl AtomicBitVec {
    /// Create an empty bit vector.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            len: 0,
            popcount: AtomicI64::new(0),
        }
    }

    /// Create a bit vector of `len` bits all set to `fill`.
    pub fn repeat(fill: bool, len: usize) -> Self {
        let word_count = len.div_ceil(BITS_PER_WORD);
        let fill_word: u64 = if fill { u64::MAX } else { 0 };
        let mut data: Vec<AtomicU64> = (0..word_count).map(|_| AtomicU64::new(fill_word)).collect();

        // Clear out-of-range trailing bits in the last word.
        if fill && !data.is_empty() && !len.is_multiple_of(BITS_PER_WORD) {
            let used = len % BITS_PER_WORD;
            let keep_mask = (1u64 << used) - 1;
            *data.last_mut().unwrap().get_mut() &= keep_mask;
        }

        let popcount: i64 = if fill { len as i64 } else { 0 };

        Self {
            data,
            len,
            popcount: AtomicI64::new(popcount),
        }
    }

    /// Create a bit vector from raw `u64` words.
    ///
    /// The resulting length is `data.len() * 64` bits.
    pub fn from_slice(data: &[u64]) -> Self {
        let popcount: i64 = data.iter().map(|w| i64::from(w.count_ones())).sum();
        let len = data.len() * BITS_PER_WORD;
        let atomic_data: Vec<AtomicU64> = data.iter().map(|&w| AtomicU64::new(w)).collect();
        Self {
            data: atomic_data,
            len,
            popcount: AtomicI64::new(popcount),
        }
    }

    /// Convert into a `BitVec<u64, Lsb0>`.
    pub fn into_bitvec(self) -> BitVec {
        let words: Vec<u64> = self
            .data
            .iter()
            .map(|w| w.load(Ordering::Relaxed))
            .collect();
        let mut bv = BitVec::from_vec(words);
        bv.truncate(self.len);
        bv
    }
}

// ── Slice access ─────────────────────────────────────────────────────────

impl AtomicBitVec {
    /// Borrow the bit vector as an [`AtomicBitSlice`].
    #[inline]
    pub fn as_slice(&self) -> AtomicBitSlice<'_> {
        AtomicBitSlice {
            data: &self.data,
            bit_len: self.len,
            popcount: &self.popcount,
        }
    }
}

// ── Convenience delegations to the slice ─────────────────────────────────

impl AtomicBitVec {
    /// Number of logical bits.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when the vector contains no bits.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reference to the underlying [`AtomicU64`] words.
    #[inline]
    pub fn as_raw_slice(&self) -> &[AtomicU64] {
        &self.data
    }

    /// Get the bit at `idx` using `Acquire` ordering.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        self.as_slice().get(idx)
    }

    /// Get the bit at `idx` using `Acquire` ordering.
    #[inline]
    pub fn get_checked(&self, idx: usize) -> Option<bool> {
        self.as_slice().get_checked(idx)
    }

    /// Get the bit at `idx` using `ordering`.
    #[inline]
    pub fn get_checked_with_ordering(&self, idx: usize, ordering: Ordering) -> Option<bool> {
        self.as_slice().get_checked_with_ordering(idx, ordering)
    }

    /// Atomically replace the bit at `idx` with `value` using `AcqRel`.
    ///
    /// Returns the previous value. Updates the popcount cache.
    ///
    /// # Panics
    /// Panics when `idx >= self.len()`.
    #[inline]
    pub fn replace_concurrent(&self, idx: usize, value: bool) -> bool {
        self.replace_concurrent_with_ordering(idx, value, Ordering::AcqRel)
    }

    /// Atomically replace the bit at `idx` with `value` using `ordering`.
    ///
    /// Returns the previous value. Updates the popcount cache.
    ///
    /// # Panics
    /// Panics when `idx >= self.len()`.
    #[inline]
    pub fn replace_concurrent_with_ordering(
        &self,
        idx: usize,
        value: bool,
        ordering: Ordering,
    ) -> bool {
        self.as_slice()
            .replace_concurrent_with_ordering(idx, value, ordering)
    }

    /// Iterate over all bits using `Acquire` ordering.
    pub fn iter(&self) -> AtomicBitSliceIter<'_> {
        self.iter_with_ordering(Ordering::Acquire)
    }

    /// Iterate over all bits using `ordering`.
    pub fn iter_with_ordering(&self, ordering: Ordering) -> AtomicBitSliceIter<'_> {
        AtomicBitSliceIter {
            slice: self.as_slice(),
            idx: 0,
            ordering,
        }
    }
}

// ── Popcount (owned-only, uses the cache) ─────────────────────────────────

impl AtomicBitVec {
    /// Approximate number of set bits.
    ///
    /// Transient negative values caused by concurrent races are clamped to `0`.
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.popcount
            .load(Ordering::Acquire)
            .try_into()
            .unwrap_or(0)
    }
}

// ── Resize ────────────────────────────────────────────────────────────────

impl AtomicBitVec {
    /// Resize the bit vector to `new_len` bits.
    ///
    /// If growing, new bits are set to `filler`. If shrinking, excess bits are
    /// discarded and the popcount is adjusted.
    pub fn resize(&mut self, new_len: usize, filler: bool) {
        let old_len = self.len;
        if new_len == old_len {
            return;
        }

        let new_word_count = new_len.div_ceil(BITS_PER_WORD);
        let old_word_count = self.data.len();

        if new_len > old_len {
            // ── Growing ──────────────────────────────────────────────────
            if filler {
                // Fill trailing bits of the current last word first.
                if !old_len.is_multiple_of(BITS_PER_WORD) && old_word_count > 0 {
                    let last = old_len / BITS_PER_WORD;
                    let used = old_len % BITS_PER_WORD;
                    let fill_mask = u64::MAX << used;
                    *self.data[last].get_mut() |= fill_mask;
                }
                // Append fully-set words.
                for _ in old_word_count..new_word_count {
                    self.data.push(AtomicU64::new(u64::MAX));
                }
                *self.popcount.get_mut() += (new_len - old_len) as i64;
            } else {
                // Append zero words.
                for _ in old_word_count..new_word_count {
                    self.data.push(AtomicU64::new(0));
                }
                // Popcount unchanged.
            }

            // Clear trailing bits in the new last word.
            if !new_len.is_multiple_of(BITS_PER_WORD) {
                let last = new_len / BITS_PER_WORD;
                let used = new_len % BITS_PER_WORD;
                let keep_mask = (1u64 << used) - 1;
                *self.data[last].get_mut() &= keep_mask;
            }
        } else {
            // ── Shrinking ────────────────────────────────────────────────
            let removed = self.as_slice().count_bits_in_range(new_len, old_len) as i64;

            self.data.truncate(new_word_count);

            // Clear trailing bits in the new last word.
            if !new_len.is_multiple_of(BITS_PER_WORD) && !self.data.is_empty() {
                let last = new_len / BITS_PER_WORD;
                let used = new_len % BITS_PER_WORD;
                let keep_mask = (1u64 << used) - 1;
                *self.data[last].get_mut() &= keep_mask;
            }

            *self.popcount.get_mut() -= removed;
        }

        self.len = new_len;
    }
}

// ─────────────────────────────── Tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;

    use super::*;

    // ── AtomicBitSlice ────────────────────────────────────────────────────

    #[test]
    fn test_slice_get_checked_and_replace() {
        let bv = AtomicBitVec::repeat(false, 10);
        let sl = bv.as_slice();

        assert_eq!(sl.len(), 10);
        assert!(!sl.is_empty());
        assert_eq!(sl.get_checked(0), Some(false));
        assert_eq!(sl.get_checked(10), None);

        let old = sl.replace_concurrent(3, true);
        assert!(!old);
        assert_eq!(sl.get_checked(3), Some(true));
    }

    #[test]
    fn test_slice_iter_matches_get_checked() {
        let bv = AtomicBitVec::repeat(false, 50);
        let sl = bv.as_slice();
        for i in (0..50).step_by(3) {
            sl.replace_concurrent(i, true);
        }
        let from_iter: Vec<bool> = sl.iter().collect();
        let from_get_checked: Vec<bool> = (0..50).map(|i| sl.get_checked(i).unwrap()).collect();
        let from_get: Vec<bool> = (0..50).map(|i| sl.get(i)).collect();
        assert_eq!(from_iter, from_get_checked);
        assert_eq!(from_iter, from_get);
    }

    #[test]
    fn test_slice_replace_panics_oob() {
        let bv = AtomicBitVec::repeat(false, 8);
        let result = std::panic::catch_unwind(|| bv.as_slice().replace_concurrent(8, true));
        assert!(result.is_err(), "must panic on out-of-bounds index");
    }

    // ── AtomicBitVec construction ─────────────────────────────────────────

    #[test]
    fn test_new_is_empty() {
        let bv = AtomicBitVec::new();
        assert_eq!(bv.len(), 0);
        assert!(bv.is_empty());
        assert_eq!(bv.count_ones(), 0);
        assert_eq!(bv.get_checked(0), None);
    }

    #[test]
    fn test_repeat_false() {
        let bv = AtomicBitVec::repeat(false, 130);
        assert_eq!(bv.len(), 130);
        assert_eq!(bv.count_ones(), 0);
        for i in 0..130 {
            assert_eq!(bv.get_checked(i), Some(false), "bit {i}");
        }
        assert_eq!(bv.get_checked(130), None);
    }

    #[test]
    fn test_repeat_true() {
        let bv = AtomicBitVec::repeat(true, 130);
        assert_eq!(bv.len(), 130);
        assert_eq!(bv.count_ones(), 130);
        for i in 0..130 {
            assert_eq!(bv.get_checked(i), Some(true), "bit {i}");
        }
        assert_eq!(bv.get_checked(130), None);
    }

    #[test]
    fn test_repeat_exact_word_boundaries() {
        for len in [64, 128, 192] {
            let bv_f = AtomicBitVec::repeat(false, len);
            let bv_t = AtomicBitVec::repeat(true, len);
            assert_eq!(bv_f.count_ones(), 0);
            assert_eq!(bv_t.count_ones(), len);
        }
    }

    // ── from_slice / from_bitvec / into_bitvec ───────────────────────────

    #[test]
    fn test_from_slice_length_and_data() {
        let words = vec![0xDEAD_BEEF_CAFE_1234_u64, 0x0000_0000_0000_0001_u64];
        let bv = AtomicBitVec::from_slice(&words);
        assert_eq!(bv.len(), 128);
        assert_eq!(bv.as_raw_slice()[0].load(Ordering::Relaxed), words[0]);
        assert_eq!(bv.as_raw_slice()[1].load(Ordering::Relaxed), words[1]);
    }

    // ── get / get_checked / get_checked_with_ordering ───────────────────────────────────────────

    #[test]
    #[should_panic]
    fn test_get_out_of_bounds_panics() {
        let bv = AtomicBitVec::repeat(true, 64);
        let _ = bv.get(64);
    }

    #[test]
    fn test_get_checked_out_of_bounds_returns_none() {
        let bv = AtomicBitVec::repeat(true, 64);
        assert_eq!(bv.get_checked(63), Some(true));
        assert_eq!(bv.get_checked(64), None);
    }

    // ── replace_concurrent ────────────────────────────────────────────────

    #[test]
    fn test_replace_concurrent_basic() {
        let bv = AtomicBitVec::repeat(false, 10);

        assert!(!bv.replace_concurrent(5, true));
        assert_eq!(bv.get_checked(5), Some(true));
        assert_eq!(bv.count_ones(), 1);

        assert!(bv.replace_concurrent(5, true)); // already true
        assert_eq!(bv.count_ones(), 1);

        assert!(bv.replace_concurrent(5, false));
        assert_eq!(bv.get_checked(5), Some(false));
        assert_eq!(bv.count_ones(), 0);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_replace_concurrent_panics_oob() {
        let bv = AtomicBitVec::repeat(false, 8);
        bv.replace_concurrent(8, true);
    }

    #[test]
    fn test_replace_concurrent_across_word_boundaries() {
        let bv = AtomicBitVec::repeat(false, 200);
        for i in [0, 63, 64, 65, 127, 128, 199] {
            assert!(!bv.replace_concurrent(i, true), "bit {i}");
        }
        assert_eq!(bv.count_ones(), 7);
        for i in [0, 63, 64, 65, 127, 128, 199] {
            assert!(bv.replace_concurrent(i, false), "bit {i}");
        }
        assert_eq!(bv.count_ones(), 0);
    }

    // ── resize ────────────────────────────────────────────────────────────

    #[test]
    fn test_resize_grow_false() {
        let mut bv = AtomicBitVec::repeat(true, 10);
        bv.resize(20, false);
        assert_eq!(bv.len(), 20);
        for i in 0..10 {
            assert_eq!(bv.get_checked(i), Some(true), "old bit {i}");
        }
        for i in 10..20 {
            assert_eq!(bv.get_checked(i), Some(false), "new bit {i}");
        }
        assert_eq!(bv.count_ones(), 10);
    }

    #[test]
    fn test_resize_grow_true() {
        let mut bv = AtomicBitVec::repeat(false, 10);
        bv.resize(20, true);
        assert_eq!(bv.len(), 20);
        for i in 0..10 {
            assert_eq!(bv.get_checked(i), Some(false), "old bit {i}");
        }
        for i in 10..20 {
            assert_eq!(bv.get_checked(i), Some(true), "new bit {i}");
        }
        assert_eq!(bv.count_ones(), 10);
    }

    #[test]
    fn test_resize_shrink() {
        let mut bv = AtomicBitVec::repeat(true, 100);
        bv.resize(50, false);
        assert_eq!(bv.len(), 50);
        assert_eq!(bv.count_ones(), 50);
        for i in 0..50 {
            assert_eq!(bv.get_checked(i), Some(true), "bit {i}");
        }
        assert_eq!(bv.get_checked(50), None);
    }

    #[test]
    fn test_resize_across_word_boundaries() {
        let mut bv = AtomicBitVec::repeat(true, 65);
        assert_eq!(bv.count_ones(), 65);

        bv.resize(130, true);
        assert_eq!(bv.len(), 130);
        assert_eq!(bv.count_ones(), 130);
        for i in 0..130 {
            assert_eq!(bv.get_checked(i), Some(true), "bit {i}");
        }

        bv.resize(65, false);
        assert_eq!(bv.len(), 65);
        assert_eq!(bv.count_ones(), 65);
        for i in 0..65 {
            assert_eq!(bv.get_checked(i), Some(true), "bit {i}");
        }
    }

    #[test]
    fn test_resize_no_trailing_bits_leak() {
        // After resize from 100 to 65 bits, trailing bits in last word = 0.
        let mut bv = AtomicBitVec::repeat(true, 100);
        bv.resize(65, false);
        // bit 64 = bit 0 of word 1; bits 1..63 of word 1 must be zero.
        let last_word = bv.as_raw_slice()[1].load(Ordering::Relaxed);
        assert_eq!(
            last_word & !1u64,
            0,
            "trailing bits in last word must be zero"
        );
    }
    // ── iter ─────────────────────────────────────────────────────────────

    #[test]
    fn test_iter_matches_get_checked() {
        let bv = AtomicBitVec::repeat(false, 50);
        for i in (0..50).step_by(3) {
            bv.replace_concurrent(i, true);
        }
        let from_iter: Vec<bool> = bv.iter().collect();
        let from_get: Vec<bool> = (0..50).map(|i| bv.get_checked(i).unwrap()).collect();
        assert_eq!(from_iter, from_get);
    }

    #[test]
    fn test_iter_exact_size() {
        let bv = AtomicBitVec::repeat(true, 37);
        let mut it = bv.iter();
        assert_eq!(it.len(), 37);
        it.next();
        assert_eq!(it.len(), 36);
    }

    // ── Clone / PartialEq ────────────────────────────────────────────────

    #[test]
    fn test_clone_is_independent() {
        let bv1 = AtomicBitVec::repeat(false, 10);
        let bv2 = bv1.clone();
        bv1.replace_concurrent(0, true);
        assert_eq!(bv2.get_checked(0), Some(false), "clone must be independent");
    }

    #[test]
    fn test_partial_eq() {
        let bv1 = AtomicBitVec::repeat(false, 10);
        let bv2 = bv1.clone();
        assert_eq!(bv1, bv2);
        bv1.replace_concurrent(0, true);
        assert_ne!(bv1, bv2);
    }

    // ── popcount ─────────────────────────────────────────────────────────

    #[test]
    fn test_count_ones_tracks_changes() {
        let bv = AtomicBitVec::repeat(false, 100);
        assert_eq!(bv.count_ones(), 0);
        bv.replace_concurrent(0, true);
        assert_eq!(bv.count_ones(), 1);
        bv.replace_concurrent(0, true); // no-op
        assert_eq!(bv.count_ones(), 1);
        bv.replace_concurrent(0, false);
        assert_eq!(bv.count_ones(), 0);
    }

    #[test]
    fn test_count_ones_after_resize() {
        let mut bv = AtomicBitVec::repeat(true, 64);
        assert_eq!(bv.count_ones(), 64);
        bv.resize(128, true);
        assert_eq!(bv.count_ones(), 128);
        bv.resize(32, false);
        assert_eq!(bv.count_ones(), 32);
    }

    // ── Concurrent replace correctness ────────────────────────────────────

    /// Each thread holds a unique random permutation of bit indices and sets
    /// them to `true`. The number of `replace_concurrent` calls that observe
    /// a previous value of `false` must equal the total number of bits.
    #[test]
    fn test_replace_concurrent_correctness() {
        const N_BITS: usize = 10_000;
        const N_THREADS: usize = 4;

        let bitmask = Arc::new(AtomicBitVec::repeat(false, N_BITS));
        let counter = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let bitmask = Arc::clone(&bitmask);
                let counter = Arc::clone(&counter);

                std::thread::spawn(move || {
                    let mut indices: Vec<usize> = (0..N_BITS).collect();
                    let mut rng = StdRng::seed_from_u64(t as u64 * 0xDEAD_BEEF + 1);
                    indices.shuffle(&mut rng);

                    for idx in indices {
                        let old = bitmask.replace_concurrent(idx, true);
                        if !old {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            counter.load(Ordering::Relaxed),
            N_BITS,
            "each bit must be set from false to true exactly once"
        );
        assert_eq!(bitmask.count_ones(), N_BITS);
    }

    /// Concurrent writes to *disjoint* bit ranges must not interfere.
    #[test]
    fn test_replace_concurrent_disjoint_ranges() {
        const N_BITS: usize = 10_000;
        const N_THREADS: usize = 4;
        const CHUNK: usize = N_BITS / N_THREADS;

        let bitmask = Arc::new(AtomicBitVec::repeat(false, N_BITS));

        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let bitmask = Arc::clone(&bitmask);
                std::thread::spawn(move || {
                    let start = t * CHUNK;
                    let end = if t + 1 == N_THREADS {
                        N_BITS
                    } else {
                        start + CHUNK
                    };
                    for idx in start..end {
                        bitmask.replace_concurrent(idx, true);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(bitmask.count_ones(), N_BITS);
        for i in 0..N_BITS {
            assert_eq!(bitmask.get_checked(i), Some(true), "bit {i}");
        }
    }

    // Bit ordering compatibility with BitVec
    #[test]
    #[allow(clippy::bool_assert_comparison)] // bools are values here
    fn test_bitvec_bit_ordering() {
        let mut bv = BitVec::repeat(false, 64);
        bv.replace(0, true);
        assert_eq!(bv[0], true);
        assert_eq!(bv[63], false);

        let av = AtomicBitVec::from_slice(bv.as_raw_slice());
        assert_eq!(av.len(), 64);
        assert_eq!(av.get(0), true);
        assert_eq!(av.get(63), false);
    }
}
