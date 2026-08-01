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
use crate::generic_consts::Random;
use crate::universal_io::{ReadBytesItem, UioResult, UniversalIoError, UniversalRead};

/// The size of the tail padding.
/// These extra 7 bytes after the last chunk let the decompressor read a whole
/// 8-byte word even when a delta ends in the last byte of the last chunk.
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
    let expected_size = parameters.compressed_size_bytes().unwrap();
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

#[derive(Error, Debug)]
#[error("decompression error: {0}")]
pub struct DecompressionError(String);

/// [`Reader`] bundled with in-memory compressed data.
#[derive(Clone, Debug)]
pub struct SliceReader<'a> {
    reader: Reader,
    data: &'a [u8],
}

impl<'a> SliceReader<'a> {
    /// Read `value[index]` and `value[index + 1]`. `index + 1` should be less
    /// than [`Reader::decompressed_len()`].
    #[inline]
    pub fn read_pair(&self, index: usize) -> Option<(u64, u64)> {
        // Each pair needs `index + 1`, so the last value is not a valid index.
        if index >= self.reader.decompressed_len().saturating_sub(1) {
            return None;
        }
        let end_index = index + 1;
        let chunk = &self.data[self.reader.chunk_offset(index)..];
        let start = self.reader.decode_chunk(index, chunk);
        let end = if end_index & self.reader.chunk_len_mask != 0 {
            self.reader.decode_chunk(end_index, chunk)
        } else {
            let chunk = &self.data[self.reader.chunk_offset(end_index)..];
            self.reader.decode_chunk(end_index, chunk)
        };
        Some((start, end))
    }
}

/// Validated [`Parameters`] with precomputed values, plus the decompression
/// logic.
#[derive(Clone, Copy, Debug)]
pub struct Reader {
    params: Parameters,
    base_mask: u64,
    delta_mask: u64,
    /// `chunk_len - 1`, i.e. the maximum in-chunk index.
    chunk_len_mask: usize,
    chunk_size_bytes: usize,
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
    pub fn validate(self) -> Result<Reader, DecompressionError> {
        let valid = (1..=u64::BITS as u8).contains(&self.base_bits)
            && DELTA_BITS_RANGE.contains(&self.delta_bits)
            && self.chunk_len_log2 <= MAX_CHUNK_LEN_LOG2
            && self.compressed_size_bytes().is_some();
        if !valid {
            return Err(DecompressionError("invalid parameters".to_string()));
        }
        Ok(Reader {
            params: self,
            base_mask: make_bitmask(self.base_bits),
            delta_mask: make_bitmask(self.delta_bits),
            chunk_len_mask: make_bitmask(self.chunk_len_log2),
            chunk_size_bytes: self.chunk_size_bytes().unwrap(),
        })
    }

    /// Size of the compressed data, including the tail.
    fn compressed_size_bytes(self) -> Option<usize> {
        let chunks_count = (self.length.get() as usize).div_ceil(1 << self.chunk_len_log2);
        chunks_count
            .checked_mul(self.chunk_size_bytes()?)?
            .checked_add(TAIL_SIZE)
    }

    /// Size of a single chunk in bytes.
    /// Returns `None` on overflow.
    fn chunk_size_bytes(self) -> Option<usize> {
        let bits = (self.base_bits as usize).checked_add(
            (self.delta_bits as usize).checked_mul(make_bitmask::<usize>(self.chunk_len_log2))?,
        )?;
        Some(bits.div_ceil(u8::BITS as usize))
    }

    /// Find the best compression parameters for the given values.
    fn find_best(values: &[u64]) -> Self {
        Self::try_all(values)
            .min_by_key(|parameters| parameters.compressed_size_bytes().unwrap())
            .unwrap()
    }

    /// Generate all possible compression parameters for the given values.
    fn try_all(values: &[u64]) -> impl Iterator<Item = Parameters> + use<'_> {
        let last_value = values.last().copied().unwrap_or(0);
        (0..=MAX_CHUNK_LEN_LOG2)
            .map(move |chunk_len_log2| {
                let mut delta_bits = *DELTA_BITS_RANGE.start();
                for chunk in values.chunks(1 << chunk_len_log2) {
                    let delta = chunk.last().unwrap().strict_sub(chunk[0]);
                    delta_bits = delta_bits.max(packed_bits(delta));
                }
                Parameters {
                    length: U64::new(values.len() as u64),
                    base_bits: packed_bits(last_value).max(1),
                    delta_bits,
                    chunk_len_log2,
                }
            })
            .filter(|params| DELTA_BITS_RANGE.contains(&params.delta_bits))
    }
}

impl Reader {
    /// Create a [`SliceReader`] from the compressed data slice.
    pub fn slice_reader(self, bytes: &[u8]) -> Result<SliceReader<'_>, DecompressionError> {
        let size = self.compressed_size_bytes();
        let Some(data) = bytes.get(..size) else {
            return Err(DecompressionError(format!(
                "insufficient length (compressed data, expected {size} bytes, got {})",
                bytes.len(),
            )));
        };
        Ok(SliceReader { reader: self, data })
    }

    /// Number of values in the decompressed data.
    #[inline]
    pub fn decompressed_len(self) -> usize {
        self.params.length.get() as usize
    }

    /// Size of the compressed data in bytes, including the tail.
    #[inline]
    pub fn compressed_size_bytes(self) -> usize {
        self.params.compressed_size_bytes().unwrap() // Checked by `Parameters::validate`.
    }

    /// For each `i` in `indices`, read a pair of values:
    /// `value[i]` and `value[i + 1]`.
    pub fn read_pairs_iter<'a, S: UniversalRead>(
        self,
        storage: &'a S,
        file_offset: u64,
        indices: &'a [usize],
    ) -> UioResult<impl Iterator<Item = UioResult<(usize, (u64, u64))>> + 'a> {
        // Each pair needs `index + 1`, so the last value is not a valid index.
        let max_index = self.decompressed_len().saturating_sub(1);
        if let Some(&index) = indices.iter().find(|&&index| index >= max_index) {
            return Err(UniversalIoError::OutOfBounds {
                start: index as u64,
                end: index as u64 + 2,
                elements: self.decompressed_len(),
            });
        }

        // One read per pair, from `index`'s chunk through the end of what
        // `index + 1` needs. Both values share a chunk unless `index` ends one.
        let chunk_read_len = (self.chunk_size_bytes + TAIL_SIZE) as u64;
        let items = indices.iter().enumerate().map(move |(position, &index)| {
            let first = file_offset + self.chunk_offset(index) as u64;
            let second = file_offset + self.chunk_offset(index + 1) as u64;
            ReadBytesItem {
                user_data: (position, index),
                range: first..second + chunk_read_len,
                align: 1,
            }
        });
        Ok(storage.read_bytes_iter(items, Random)?.map(move |result| {
            let ((position, index), chunk) = result?;
            // `chunk` starts at `index`'s chunk, so locate `index + 1` in it.
            let second = self.chunk_offset(index + 1) - self.chunk_offset(index);
            let start = self.decode_chunk(index, &chunk);
            let end = self.decode_chunk(index + 1, &chunk[second..]);
            Ok((position, (start, end)))
        }))
    }

    /// Byte offset of the chunk containing the value at `index`.
    #[inline]
    fn chunk_offset(self, index: usize) -> usize {
        (index >> self.params.chunk_len_log2) * self.chunk_size_bytes
    }

    /// Decode the value at `index` from `chunk`.
    ///
    /// The `chunk` must hold at least `chunk_size_bytes + TAIL_SIZE` bytes.
    /// The `index` must be less than [`Self::decompressed_len()`].
    #[inline]
    fn decode_chunk(self, index: usize, chunk: &[u8]) -> u64 {
        let word = |offset: usize| u64::from_le_bytes(*chunk[offset..].first_chunk().unwrap());
        let base = word(0) & self.base_mask;
        if let Some(delta_index) = (index & self.chunk_len_mask).checked_sub(1) {
            let bits =
                self.params.base_bits as usize + delta_index * self.params.delta_bits as usize;
            let delta =
                (word(bits / u8::BITS as usize) >> (bits % u8::BITS as usize)) & self.delta_mask;
            base + delta
        } else {
            base
        }
    }
}

#[cfg(feature = "testing")]
pub fn gen_test_sequence(rng: &mut impl rand::Rng, max_delta: u64, len: usize) -> Vec<u64> {
    let mut last = 0u64;
    (0..len)
        .map(|_| {
            use rand::RngExt;

            last = last.checked_add(rng.random_range(0..=max_delta)).unwrap();
            last
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::iter::{once, once_with};

    use itertools::Itertools;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::universal_io::{MmapFs, OpenOptions, UniversalReadFs as _};

    #[test]
    fn test_compress_decompress() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let file = file.path();

        for values in test_sequences() {
            for params in Parameters::try_all(&values) {
                let reader = params.validate().unwrap();
                let compressed = compress_with_parameters(&values, params);
                assert_eq!(reader.decompressed_len(), values.len());
                assert_eq!(params.compressed_size_bytes(), Some(compressed.len()));

                let expected = values.iter().copied().tuple_windows().collect::<Vec<_>>();
                let oob = values.len().saturating_sub(1); // the last value starts no pair

                // SliceReader::read_pair
                let slice_reader = reader.slice_reader(&compressed).unwrap();
                for (index, &expected) in expected.iter().enumerate() {
                    assert_eq!(slice_reader.read_pair(index), Some(expected));
                }
                assert_eq!(slice_reader.read_pair(oob), None);

                // Reader::read_pairs_iter
                let unrelated_data = [0xAA; 3]; // for `file_offset` testing
                fs_err::write(file, [&unrelated_data[..], &compressed].concat()).unwrap();
                let storage = MmapFs.open(file, OpenOptions::new_for_test(), ()).unwrap();
                let offset = unrelated_data.len() as u64;
                let indices = (0..expected.len()).collect::<Vec<_>>();
                let mut out = vec![(1234, 12345); expected.len()];
                for result in reader.read_pairs_iter(&storage, offset, &indices).unwrap() {
                    let (position, pair) = result.unwrap();
                    out[position] = pair;
                }
                assert_eq!(out, expected);
                assert!(reader.read_pairs_iter(&storage, offset, &[oob]).is_err());
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
