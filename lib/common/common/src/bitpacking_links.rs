use crate::bitpacking::{packed_bits, BitReader, BitWriter};

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

/// Iterate over packed links and apply a function to each value.
/// See [`pack_links`] for parameter descriptions.
#[inline]
pub fn for_each_packed_link(
    links: &[u8],
    bits_per_unsorted: u8,
    sorted_count: usize,
    mut f: impl FnMut(u32),
) {
    if links.is_empty() {
        return;
    }

    let mut r = BitReader::new(links);

    let mut remaining_bits = links.len() * u8::BITS as usize;
    if sorted_count != 0 {
        // 1. Header.
        r.set_bits(HEADER_BITS);
        let bits_per_sorted = r.read::<u8>() + MIN_BITS_PER_VALUE;
        remaining_bits -= HEADER_BITS as usize;

        // 2. First `sorted_count` values, sorted and delta-encoded.
        r.set_bits(bits_per_sorted);
        let remaining_bits_target = remaining_bits
            - sorted_count.min(remaining_bits / bits_per_sorted as usize)
                * bits_per_sorted as usize;
        let mut value = 0;
        while remaining_bits > remaining_bits_target {
            value += r.read::<u32>();
            f(value);
            remaining_bits -= bits_per_sorted as usize;
        }
    }

    // 3. The rest of the values, unsorted.
    r.set_bits(bits_per_unsorted);
    while remaining_bits >= bits_per_unsorted as usize {
        f(r.read());
        remaining_bits -= bits_per_unsorted as usize;
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools as _;
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::*;

    #[test]
    fn test_random() {
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let bits_per_unsorted = rng.random_range(7..=32);
            let sorted_count = rng.random_range(0..100);
            let unsorted_count = rng.random_range(0..100);
            if 1 << bits_per_unsorted < sorted_count + unsorted_count {
                continue;
            }

            let mut raw_links =
                std::iter::repeat_with(|| rng.random_range(0..1u64 << bits_per_unsorted) as u32)
                    .unique()
                    .take(sorted_count + unsorted_count)
                    .collect::<Vec<u32>>();

            let mut links = Vec::new();
            pack_links(
                &mut links,
                raw_links.clone(),
                bits_per_unsorted,
                sorted_count,
            );

            let mut unpacked = Vec::new();
            for_each_packed_link(&links, bits_per_unsorted, sorted_count, |value| {
                unpacked.push(value)
            });

            raw_links[..sorted_count].sort_unstable();
            assert_eq!(raw_links, unpacked);
        }
    }
}
