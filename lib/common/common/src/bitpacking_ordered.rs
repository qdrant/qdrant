//! A compression algorithm to store medium-to-large-sized sorted arrays of
//! `u64` values.
//!
//! Allows for fast random access within the compressed data.
//!
//! Assumptions:
//! - The input values are sorted.
//! - The distribution of the values is somewhat uniform, i.e. there are no
//!   large gaps between values. A single gap might bloat the overall size, but
//!   it shouldn't be worse than storing byte-aligned bases without deltas.
//!
//! # Format
//!
//! The compressed data consists of small, uniformely-sized chunks.
//! The size of each chunk is determined by compression parameters.
//! The compression parameters are determined automatically during compression.
//!
//! Each chunk contains `1 << chunk_len_log2` values: the first value (the base)
//! is stored as is, and the rest are stored as deltas from the base. Or, more
//! formally:
//! - `chunk_value[0] = base` (assume `delta[0]` is 0)
//! - `chunk_value[i] = base + delta[i]` for `i > 0`
//!
//! ```text
//! ┌───────┬───────┬───────┬   ┬───────┬────────┐
//! │chunk 0│chunk 1│chunk 2│ … │chunk X│7 × 0xFF│
//! └───────┤       ├───────┴   ┴───────┴────────┘
//! ╭───────╯       ╰────────────────╮
//! │        bitpacked chunk         │
//! ├────┬──┬──┬──┬──┬   ┬────┬──────┤
//! │base│Δ₁│Δ₂│Δ₃│Δ₄│ … │Δₙ₋₁│bitpad│
//! └────┴──┴──┴──┴──┴   ┴────┴──────┘
//! ```
//!
//! In the above diagram:
//! - `7 × 0xFF` is 8 bytes tail (see [`TAIL_SIZE`]).
//! - `base` is `parameters.base_bits` wide.
//! - `Δ₁`..`Δₙ₋₁` are delta values, each is `parameters.delta_bits` wide.
//! - `bitpad` is a bit padding (0..7 bits) so the chunk is byte-aligned.

use std::ops::RangeInclusive;

use thiserror::Error;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::bitpacking::{BitWriter, make_bitmask, packed_bits};

/// The size of the tail padding.
/// These extra 7 bytes after the last chunk allows the decompressor to safely
/// perform unchecked unaligned 8-byte reads.
const TAIL_SIZE: usize = size_of::<u64>() - 1;

/// The allowed range for the `delta_bits` parameter.
/// Limiting it up to 7*8 = 56 bits allows the decompressor to read a single
/// delta value in a single unaligned read.
/// Disallowing 0 removes unlikely edge cases.
const DELTA_BITS_RANGE: RangeInclusive<u8> = 1..=(u64::BITS - u8::BITS) as u8;

/// Larger values are unlikely to produce better compression.
const MAX_CHUNK_LEN_LOG2: u8 = 7;

/// Compress the provided data using the best parameters found.
///
/// # Panics
///
/// This function may panic if the input data is not sorted.
pub fn compress(values: &[u64]) -> (Vec<u8>, Parameters) {
    let parameters = Parameters::find_best(values);
    let compressed = compress_with_parameters(values, parameters);
    (compressed, parameters)
}

/// Compress the data with given parameters.
fn compress_with_parameters(values: &[u64], parameters: Parameters) -> Vec<u8> {
    let expected_size = parameters.total_chunks_size_bytes().unwrap() + TAIL_SIZE;
    let mut compressed = Vec::with_capacity(expected_size);

    for chunk in values.chunks(1 << parameters.chunk_len_log2) {
        let first = chunk[0];
        let mut w = BitWriter::new(&mut compressed);
        w.write(first, parameters.base_bits);
        for &value in chunk.iter().skip(1) {
            w.write(value - first, parameters.delta_bits);
        }
        // For the last (incomplete) chunk, pad it with 0b11...11, so all chunks
        // have the same size.
        for _ in 0..(1 << parameters.chunk_len_log2) - chunk.len() {
            w.write(
                make_bitmask::<u64>(parameters.delta_bits),
                parameters.delta_bits,
            );
        }
        w.finish(); // bit padding
    }

    compressed.extend_from_slice(&[0xFF; TAIL_SIZE]);
    assert_eq!(compressed.len(), expected_size);

    compressed
}

#[derive(Clone, Debug)]
pub struct Reader<'a> {
    base_bits: u8,
    base_mask: u64,
    delta_bits: u8,
    delta_mask: u64,
    chunk_len_log2: u8,
    chunk_len_mask: usize,
    chunk_size_bytes: usize,
    compressed: &'a [u8],
    len: usize,
}

#[derive(Error, Debug)]
#[error("decompression error: {0}")]
pub struct DecompressionError(String);

impl<'a> Reader<'a> {
    pub fn new(
        parameters: Parameters,
        bytes: &'a [u8],
    ) -> Result<(Self, &'a [u8]), DecompressionError> {
        // Safety checks: the `get()` method doesn't perform bounds checking,
        // so we need to be extra cautious here, including checking for
        // overflows.
        if !parameters.valid() {
            return Err(DecompressionError("invalid parameters".to_string()));
        }
        let total_size_bytes = parameters
            .total_chunks_size_bytes()
            .and_then(|size| size.checked_add(TAIL_SIZE))
            .ok_or_else(|| DecompressionError("invalid parameters".to_string()))?;

        let (compressed, bytes) = bytes.split_at_checked(total_size_bytes).ok_or_else(|| {
            DecompressionError(format!(
                "insufficient length (compressed data, expected {total_size_bytes} bytes, got {})",
                bytes.len(),
            ))
        })?;

        let result = Self {
            base_bits: parameters.base_bits,
            base_mask: make_bitmask(parameters.base_bits),
            delta_bits: parameters.delta_bits,
            delta_mask: make_bitmask(parameters.delta_bits),
            chunk_len_log2: parameters.chunk_len_log2,
            chunk_len_mask: make_bitmask(parameters.chunk_len_log2),
            chunk_size_bytes: parameters.chunk_size_bytes().unwrap(),
            compressed,
            len: parameters.length.get() as usize,
        };

        // Safety checks: the `get()` method doesn't perform bounds checking.
        // The assertions below ensure that the `compressed` slice holds enough
        // bytes for any index reachable by `get()`.
        if let Some(max_index) = result.len.checked_sub(1) {
            let chunk_offset = (max_index >> result.chunk_len_log2) * result.chunk_size_bytes;
            // *base*
            assert!(chunk_offset + size_of::<u64>() <= result.compressed.len());

            let max_value_index = result.chunk_len_mask;
            if max_value_index > 0 {
                let delta_offset_bits =
                    result.base_bits as usize + (max_value_index - 1) * result.delta_bits as usize;
                // *delta*
                assert!(
                    chunk_offset + delta_offset_bits / u8::BITS as usize + size_of::<u64>()
                        <= result.compressed.len()
                );
            }
        }

        Ok((result, bytes))
    }

    /// Parameters used to compress the data.
    #[cfg(feature = "testing")]
    pub fn parameters(&self) -> Parameters {
        Parameters {
            length: U64::new(self.len as u64),
            base_bits: self.base_bits,
            delta_bits: self.delta_bits,
            chunk_len_log2: self.chunk_len_log2,
        }
    }

    /// The number of values in the decompressed data.
    #[inline]
    #[expect(clippy::len_without_is_empty, reason = "len() is cheap")]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the value at the given index.
    #[inline]
    pub fn get(&self, index: usize) -> Option<u64> {
        if index >= self.len {
            return None;
        }

        let chunk_offset = (index >> self.chunk_len_log2) * self.chunk_size_bytes;
        let value_index = index & self.chunk_len_mask;
        let chunk_ptr = self.compressed.as_ptr().wrapping_add(chunk_offset);
        // SAFETY: see the *base* comment in `new()`.
        let base = unsafe { read_u64_le(chunk_ptr) } & self.base_mask;
        if value_index == 0 {
            return Some(base);
        }
        let delta_offset_bits =
            self.base_bits as usize + (value_index - 1) * self.delta_bits as usize;
        // SAFETY: see the *delta* comment in `new()`.
        let delta = (unsafe { read_u64_le(chunk_ptr.add(delta_offset_bits / u8::BITS as usize)) }
            >> (delta_offset_bits % u8::BITS as usize))
            & self.delta_mask;
        Some(base + delta)
    }
}

#[inline(always)]
unsafe fn read_u64_le(ptr: *const u8) -> u64 {
    unsafe { u64::from_le(ptr.cast::<u64>().read_unaligned()) }
}

/// Compression parameters. Required for decompression.
#[derive(Clone, Copy, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub struct Parameters {
    /// Amount of values in the decompressed data.
    pub length: U64,
    /// Amount of bits to store base values.
    pub base_bits: u8,
    /// Amount of bits to store delta values.
    pub delta_bits: u8,
    /// Log2 of the amount of values in a chunk.
    pub chunk_len_log2: u8,
}

impl Parameters {
    /// Check if the parameters are valid.
    fn valid(self) -> bool {
        u32::from(self.base_bits) <= u64::BITS
            && DELTA_BITS_RANGE.contains(&self.delta_bits)
            && self.chunk_len_log2 <= MAX_CHUNK_LEN_LOG2
    }

    /// Size of a single chunk in bytes.
    /// Returns `None` on overflow: see safety comments in [`Reader::new()`].
    #[deny(clippy::arithmetic_side_effects, reason = "extra cautious for safety")]
    fn chunk_size_bytes(self) -> Option<usize> {
        let bits = (self.base_bits as usize).checked_add(
            (self.delta_bits as usize).checked_mul(make_bitmask::<usize>(self.chunk_len_log2))?,
        )?;
        Some(bits.div_ceil(u8::BITS as usize))
    }

    /// Size of the compressed data, without the tail.
    /// Returns `None` on overflow: see safety comments in [`Reader::new()`].
    #[deny(clippy::arithmetic_side_effects, reason = "extra cautious for safety")]
    fn total_chunks_size_bytes(self) -> Option<usize> {
        let chunks_count = (self.length.get() as usize).div_ceil(1 << self.chunk_len_log2);
        chunks_count.checked_mul(self.chunk_size_bytes()?)
    }

    /// Find the best compression parameters for the given values.
    fn find_best(values: &[u64]) -> Self {
        Self::try_all(values)
            .min_by_key(|parameters| parameters.total_chunks_size_bytes())
            .unwrap()
    }

    /// Generate all possible compression parameters for the given values.
    fn try_all(values: &[u64]) -> impl Iterator<Item = Parameters> + use<'_> {
        let last_value = values.last().copied().unwrap_or(0);
        (0..=MAX_CHUNK_LEN_LOG2)
            .map(move |chunk_len_log2| {
                let mut delta_bits = *DELTA_BITS_RANGE.start();
                for chunk in values.chunks(1 << chunk_len_log2) {
                    delta_bits = delta_bits.max(packed_bits(chunk.last().unwrap() - chunk[0]));
                }
                Parameters {
                    length: U64::new(values.len() as u64),
                    base_bits: packed_bits(last_value).max(1),
                    delta_bits,
                    chunk_len_log2,
                }
            })
            .filter(|parameters| DELTA_BITS_RANGE.contains(&parameters.delta_bits))
    }
}

#[cfg(feature = "testing")]
pub fn gen_test_sequence(rng: &mut impl rand::Rng, max_delta: u64, len: usize) -> Vec<u64> {
    let mut last = 0u64;
    (0..len)
        .map(|_| {
            last = last.checked_add(rng.random_range(0..=max_delta)).unwrap();
            last
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::iter::{once, once_with};

    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng};

    use super::*;

    #[test]
    fn test_compress_decompress() {
        for values in test_sequences() {
            for parameters in Parameters::try_all(&values) {
                let compressed = compress_with_parameters(&values, parameters);
                let (decompressor, bytes) = Reader::new(parameters, &compressed).unwrap();
                assert!(bytes.is_empty());
                assert_eq!(decompressor.len(), values.len());
                for (i, &value) in values.iter().enumerate() {
                    assert_eq!(decompressor.get(i), Some(value));
                }
                assert_eq!(decompressor.get(values.len()), None);
            }
        }
    }

    fn test_sequences() -> impl Iterator<Item = Vec<u64>> {
        let params = [
            (10, 1_000),
            (20, 10_000),           // large `delta_count`
            (10_000_000, 10_000),   // large `base_bits`
            (0x123456789AB, 1_000), // both large `base_bits` and `delta_bits`
        ];

        itertools::chain!(
            once(vec![]),
            once(vec![0]),
            once(vec![1]),
            once(vec![u64::MAX]),
            once(vec![u64::MAX, u64::MAX]),
            once(vec![0, u64::MAX]), // Catches the "incomplete chunk" case.
            params.into_iter().map(|(max_delta, len)| {
                gen_test_sequence(&mut StdRng::seed_from_u64(42), max_delta, len)
            }),
            once_with(|| {
                let mut rng = StdRng::seed_from_u64(42);
                let mut values = (0..1000).map(|_| rng.random()).collect::<Vec<_>>();
                values.sort_unstable();
                values
            }),
        )
    }
}
