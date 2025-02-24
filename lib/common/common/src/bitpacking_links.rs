use crate::bitpacking::{BitReader, BitWriter, packed_bits};

/// To simplify value counting, each value should be at least one byte.
/// Otherwise the count could would be ambiguous, e.g., a 2-byte slice of 5-bit
/// values could contain either 2 or 3 values.
pub const MIN_BITS_PER_VALUE: u8 = u8::BITS as u8;

/// How many bits required to store a value in range
/// `MIN_BITS_PER_VALUE..=u32::BITS`.
const HEADER_BITS: u8 = 5;

/// A specialized packer to pack HNSW graph links.
///
/// It assumes that the first `m` (or `m0`) values could be re-orderd for better
/// compression.
///
/// Parameters:
/// - `bits_per_unsorted` should be enough to store the maximum point ID
///   (it should be the same for all nodes/links within a segment).
/// - `sorted_count` is `m` (or `m0`) for this layer.
pub fn pack_links(
    links: &mut Vec<u8>,
    mut raw_links: Vec<u32>,
    bits_per_unsorted: u8,
    sorted_count: usize,
) {
    if raw_links.is_empty() {
        return;
    }

    // Sort and delta-encode the first `sorted_count` links.
    let sorted_count = raw_links.len().min(sorted_count);
    raw_links[..sorted_count].sort_unstable();
    for i in (1..sorted_count).rev() {
        raw_links[i] -= raw_links[i - 1];
    }

    let mut w = BitWriter::new(links);

    if sorted_count != 0 {
        // 1. Header.
        let bits_per_sorted =
            packed_bits(*raw_links[..sorted_count].iter().max().unwrap()).max(MIN_BITS_PER_VALUE);
        w.write(u32::from(bits_per_sorted - MIN_BITS_PER_VALUE), HEADER_BITS);

        // 2. First `sorted_count` values, sorted and delta-encoded.
        //    The bit width is determined by the header.
        for &value in &raw_links[..sorted_count] {
            w.write(value, bits_per_sorted);
        }
    }

    // 3. The rest of the values, unsorted.
    for &value in &raw_links[sorted_count..] {
        w.write(value, bits_per_unsorted);
    }

    w.finish();
}

/// Returns an iterator over packed links.
/// See [`pack_links`] for parameter descriptions.
#[inline]
pub fn iterate_packed_links(
    links: &[u8],
    bits_per_unsorted: u8,
    sorted_count: usize,
) -> PackedLinksIterator {
    let mut reader = BitReader::new(links);

    let mut remaining_bits = links.len() * u8::BITS as usize;
    let mut remaining_bits_target = remaining_bits;
    if sorted_count != 0 && !links.is_empty() {
        // 1. Header.
        reader.set_bits(HEADER_BITS);
        let bits_per_sorted = reader.read::<u8>() + MIN_BITS_PER_VALUE;
        remaining_bits -= HEADER_BITS as usize;

        // Prepare for reading sorted values.
        reader.set_bits(bits_per_sorted);
        let max_sorted = remaining_bits / bits_per_sorted as usize;
        remaining_bits_target -= sorted_count.min(max_sorted) * bits_per_sorted as usize;
    } else {
        // Prepare for reading unsorted values.
        reader.set_bits(bits_per_unsorted);
    }

    PackedLinksIterator {
        reader,
        bits_per_unsorted,
        remaining_bits,
        remaining_bits_target,
        current_delta: 0,
    }
}

/// Iterator over links packed with [`pack_links`].
/// Created by [`iterate_packed_links`].
pub struct PackedLinksIterator<'a> {
    reader: BitReader<'a>,
    bits_per_unsorted: u8,
    remaining_bits: usize,
    remaining_bits_target: usize,
    current_delta: u32,
}

impl PackedLinksIterator<'_> {
    #[inline]
    fn next_sorted(&mut self) -> u32 {
        self.current_delta = self.current_delta.wrapping_add(self.reader.read::<u32>());
        self.remaining_bits -= self.reader.bits() as usize;
        self.current_delta
    }

    #[inline]
    fn next_unsorted(&mut self) -> Option<u32> {
        if let Some(rb) = self.remaining_bits.checked_sub(self.reader.bits() as usize) {
            self.remaining_bits = rb;
            Some(self.reader.read::<u32>())
        } else {
            None
        }
    }
}

impl Iterator for PackedLinksIterator<'_> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.remaining_bits > self.remaining_bits_target {
            let value = self.next_sorted();
            if self.remaining_bits <= self.remaining_bits_target {
                // It was the last sorted value.
                self.reader.set_bits(self.bits_per_unsorted);
            }
            return Some(value);
        }

        self.next_unsorted()
    }

    /// Optimized [`Iterator::fold()`]. Should be faster than calling
    /// [`Iterator::next()`] in a loop.
    ///
    /// It is used in a hot loop during HNSW search, so performance is critical.
    #[inline]
    fn fold<Acc, F: FnMut(Acc, u32) -> Acc>(mut self, mut acc: Acc, mut f: F) -> Acc {
        while self.remaining_bits > self.remaining_bits_target {
            acc = f(acc, self.next_sorted());
        }

        self.reader.set_bits(self.bits_per_unsorted);
        while let Some(value) = self.next_unsorted() {
            acc = f(acc, value);
        }

        acc
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (sorted, unsorted);
        if let Some(sorted_bits) = self.remaining_bits.checked_sub(self.remaining_bits_target) {
            let sorted_bits = sorted_bits.next_multiple_of(self.reader.bits() as usize);
            sorted = sorted_bits / self.reader.bits() as usize;
            unsorted = (self.remaining_bits - sorted_bits) / self.bits_per_unsorted as usize;
        } else {
            sorted = 0;
            unsorted = self.remaining_bits / self.reader.bits() as usize;
        }
        (sorted + unsorted, Some(sorted + unsorted))
    }
}

impl ExactSizeIterator for PackedLinksIterator<'_> {}

#[cfg(test)]
mod tests {
    use itertools::Itertools as _;
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};
    use rstest::rstest;

    use super::*;
    use crate::iterator_ext::{check_exact_size_iterator_len, check_iterator_fold};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Cases {
        OnlyUnsorted = 0,
        OnlySorted = 1,
        OnlySortedExact = 2,
        Empty = 3,
        Both = 4,
    }

    #[rstest]
    #[case::only_unsorted(Cases::OnlyUnsorted)]
    #[case::only_sorted(Cases::OnlySorted)]
    #[case::only_sorted_exact(Cases::OnlySortedExact)]
    #[case::empty(Cases::Empty)]
    #[case::both(Cases::Both)]
    fn test_random(#[case] case: Cases) {
        let mut rng = StdRng::seed_from_u64(42u64.wrapping_add(case as u64));

        for _ in 0..1_000 {
            let (sorted_count, total_count);
            match case {
                Cases::OnlyUnsorted => {
                    sorted_count = 0;
                    total_count = rng.random_range(1..100);
                }
                Cases::OnlySorted => {
                    sorted_count = rng.random_range(2..100);
                    total_count = rng.random_range(1..sorted_count);
                }
                Cases::OnlySortedExact => {
                    sorted_count = rng.random_range(1..100);
                    total_count = sorted_count;
                }
                Cases::Empty => {
                    sorted_count = rng.random_range(0..100); // intentionally not 0
                    total_count = 0;
                }
                Cases::Both => {
                    sorted_count = rng.random_range(0..100);
                    total_count = rng.random_range(sorted_count..sorted_count + 100);
                }
            }

            let bits_per_unsorted = rng.random_range(MIN_BITS_PER_VALUE..=32);

            let mut raw_links = gen_unique_values(&mut rng, total_count, bits_per_unsorted);
            let mut links = Vec::new();
            pack_links(
                &mut links,
                raw_links.clone(),
                bits_per_unsorted,
                sorted_count,
            );

            let mut unpacked = Vec::new();
            let iter = iterate_packed_links(&links, bits_per_unsorted, sorted_count);
            iter.for_each(|value| unpacked.push(value));

            raw_links[..sorted_count.min(total_count)].sort_unstable();
            assert_eq!(raw_links, unpacked);

            check_iterator_fold(|| iterate_packed_links(&links, bits_per_unsorted, sorted_count));
            check_exact_size_iterator_len(iterate_packed_links(
                &links,
                bits_per_unsorted,
                sorted_count,
            ));
        }
    }

    /// Generate `count` unique values in range `[0, 2^bits)`.
    fn gen_unique_values(rng: &mut StdRng, count: usize, bits: u8) -> Vec<u32> {
        assert!(count <= 1 << bits);
        std::iter::repeat_with(|| rng.random_range(0..1u64 << bits) as u32)
            .unique()
            .take(count)
            .collect::<Vec<u32>>()
    }
}
