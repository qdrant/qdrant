use bitvec::field::BitField as _;

use crate::bitvec::BitSlice;
use crate::types::PointOffsetType;

/// Cursor over the ids in `0..point_count` whose bit is unset in all `N`
/// flagged bitmaps, yielded in ascending order in caller-buffer-sized chunks.
/// Bits past the end of a bitmap read as unflagged, matching the
/// `get_bit(id).unwrap_or(false)` convention of per-id bit checks. A mask
/// whose absent bits must count as flagged cannot rely on this — bound
/// `point_count` by that mask's length instead.
pub struct BatchedBitmapScan<'a, const N: usize> {
    masks: [&'a BitSlice; N],
    point_count: usize,
    /// Start of the next unread 64-id block; always a multiple of 64.
    start: usize,
    /// Candidate word that did not fit into the previous `next_chunk` buffer,
    /// carried over so the masks are read once per block; 0 = none pending.
    pending: u64,
    pending_base: PointOffsetType,
}

impl<'a, const N: usize> BatchedBitmapScan<'a, N> {
    pub fn new(point_count: usize, masks: [&'a BitSlice; N]) -> Self {
        Self {
            masks,
            point_count,
            start: 0,
            pending: 0,
            pending_base: 0,
        }
    }

    /// Fill `buf` with the next unflagged ids, in ascending order. Returns the
    /// count written; 0 means the scan is exhausted. `buf.len()` must be at
    /// least 64 so a whole 64-id block always fits into an empty buffer.
    ///
    /// Inlined so the cursor state stays in registers across a caller's
    /// chunk loop; with dense bitmaps this is called once per 64 ids.
    #[inline(always)]
    pub fn next_chunk(&mut self, buf: &mut [PointOffsetType]) -> usize {
        debug_assert!(buf.len() >= 64);
        let mut n = 0;

        // Word carried over because it did not fit into the previous buffer;
        // an empty buffer always has room for a whole word.
        if self.pending != 0 {
            n = expand_word(self.pending, self.pending_base, buf, 0);
            self.pending = 0;
        }

        while self.start < self.point_count {
            let start = self.start;
            self.start += 64;

            // Bit `i` set → id `start + i` is a candidate: unflagged in every mask.
            let mut flagged = 0u64;
            for mask in self.masks {
                flagged |= bitmap_word(mask, start);
            }
            let mut candidates = !flagged;

            // Zero the bits past the scan range in the final block.
            let block_len = self.point_count - start;
            if block_len < 64 {
                candidates &= (1u64 << block_len) - 1;
            }
            if candidates == 0 {
                continue;
            }

            // Return the chunk once this block's ids no longer fit; the word
            // is stashed for the next call rather than re-read from the masks.
            if n + candidates.count_ones() as usize > buf.len() {
                self.pending = candidates;
                self.pending_base = start as PointOffsetType;
                break;
            }

            n = expand_word(candidates, start as PointOffsetType, buf, n);
        }
        n
    }
}

/// Append the ids of the set bits of `candidates` (offset by `base`) to
/// `buf[n..]`; returns the new fill count. Caller guarantees the bits fit.
#[inline(always)]
fn expand_word(
    mut candidates: u64,
    base: PointOffsetType,
    buf: &mut [PointOffsetType],
    mut n: usize,
) -> usize {
    if candidates == u64::MAX {
        // Fully flag-free block — the common case.
        for (i, slot) in buf[n..n + 64].iter_mut().enumerate() {
            *slot = base + i as PointOffsetType;
        }
        n + 64
    } else {
        // One iteration per set bit, in ascending order: `trailing_zeros`
        // locates the lowest set bit (the next candidate's offset in the
        // block), `candidates - 1` &-ed into the mask clears exactly that bit.
        while candidates != 0 {
            buf[n] = base + candidates.trailing_zeros() as PointOffsetType;
            n += 1;
            candidates &= candidates - 1;
        }
        n
    }
}

/// The 64-bit word of `bitmap` covering positions `start..start + 64`.
/// Positions past the end of the slice read as 0 (not flagged).
#[inline]
fn bitmap_word(bitmap: &BitSlice, start: usize) -> u64 {
    let end = (start + 64).min(bitmap.len());
    if start >= end {
        return 0;
    }
    bitmap[start..end].load_le::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitvec::{BitSliceExt as _, BitVec};

    /// `BatchedBitmapScan::next_chunk` must reproduce a per-id `get_bit` scan of the
    /// same masks: ascending ids, no duplicates across calls, including a word
    /// split across two calls, a fully-flagged word, a fully-live word (fast
    /// path), masks shorter than the scan range, and a tail block.
    #[test]
    fn batched_bitmap_scan_chunks_match_per_id_reference() {
        const POINT_COUNT: usize = 300;

        // Word 0 (ids 0..64): every 3rd id flagged -> 42 candidates.
        // Word 1 (ids 64..128): fully live -> u64::MAX fast path.
        // Word 2 (ids 128..192): fully flagged by mask_b -> skipped block.
        // Word 3 (ids 192..256): fully live (both masks end before it).
        // Word 4 (ids 256..300): tail block of 44.
        let mut mask_a = BitVec::repeat(false, 64);
        for i in (0..64).step_by(3) {
            mask_a.set(i, true);
        }
        let mut mask_b = BitVec::repeat(false, 200);
        for i in 128..192 {
            mask_b.set(i, true);
        }

        let reference: Vec<PointOffsetType> = (0..POINT_COUNT as PointOffsetType)
            .filter(|&id| {
                !mask_a.get_bit(id as usize).unwrap_or(false)
                    && !mask_b.get_bit(id as usize).unwrap_or(false)
            })
            .collect();

        let mut scan =
            BatchedBitmapScan::new(POINT_COUNT, [mask_a.as_bitslice(), mask_b.as_bitslice()]);
        let mut buf = [0; 64];
        let mut harvested = Vec::new();
        let mut chunk_sizes = Vec::new();
        loop {
            let n = scan.next_chunk(&mut buf);
            if n == 0 {
                break;
            }
            chunk_sizes.push(n);
            harvested.extend_from_slice(&buf[..n]);
        }

        assert_eq!(harvested, reference);
        // Word 1 (64 candidates) must not fit behind word 0's 42 -> it is
        // deferred to the second call; every fully-live word fills one chunk.
        assert_eq!(chunk_sizes, vec![42, 64, 64, 44]);
    }
}
