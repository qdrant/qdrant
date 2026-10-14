//! Term frequencies of a text index that scores without positions, kept apart
//! from the ids-only postings in `term_frequencies.dat`.
//!
//! Each posting list's frequencies are stored in posting order, in blocks of
//! [`BLOCK_LEN`] aligned with the posting list's chunks: block `k` holds the
//! frequencies of chunk `k`, and the entries past the last full chunk form the
//! remainder. A block is bit-packed at the width of its largest value, so every
//! frequency is exact.
//!
//! A frequency is stored minus one: every indexed term occurs at least once,
//! and most occur once, so a block of ones packs at width zero and takes no
//! bytes at all.
//!
//! ```text
//! TermFrequenciesHeader     term count
//! TermHeader[term count]    offset into the data, entry count; by token id
//! data, per term:
//!   block_offsets[blocks + 1]   U32, relative to the term's packed blocks
//!   blocks                      BitPacker4x, 16 x w bytes each
//!   remainder                   w: u8, then entries x w bits (BitWriter)
//! ```
//!
//! The width of block `k` is `(block_offsets[k + 1] - block_offsets[k]) / 16`,
//! the way the sparse index derives its chunk sizes, so it is not stored.

use std::borrow::Cow;
use std::io::Write as _;
use std::path::Path;

use bitpacking::{BitPacker as _, BitPacker4x};
use common::ambient::hw::HwMetric;
use common::bitpacking::{BitReader, BitWriter, packed_bits};
use common::generic_consts::{Random, Sequential};
use common::universal_io::{ReadRange, TypedStorage, UniversalRead};
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::TokenId;
use crate::common::operation_error::{OperationError, OperationResult};

/// Entries per block: one posting list chunk.
pub const BLOCK_LEN: usize = posting_list::CHUNK_LEN;
const _: () = assert!(BLOCK_LEN == BitPacker4x::BLOCK_LEN);

/// Bytes a full block takes per bit of width.
const BLOCK_BYTES_PER_BIT: usize = BLOCK_LEN / u8::BITS as usize;

#[derive(Debug, Clone, Copy, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct TermFrequenciesHeader {
    term_count: U64,
}

#[derive(Debug, Clone, Copy, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct TermHeader {
    /// Start of this term's data, relative to the end of the headers.
    offset: U64,
    /// Entries in the posting list.
    len: U32,
}

/// Bit-pack the frequencies of every posting list, `lists[token_id]` in
/// posting order, into the bytes of a `term_frequencies.dat` file.
pub fn encode<'a>(lists: impl ExactSizeIterator<Item = &'a [u32]>) -> Vec<u8> {
    let mut headers = Vec::with_capacity(lists.len());
    let mut data = Vec::new();
    for frequencies in lists {
        headers.push(TermHeader {
            offset: U64::new(data.len() as u64),
            len: U32::new(frequencies.len() as u32),
        });
        encode_term(frequencies, &mut data);
    }

    let header = TermFrequenciesHeader {
        term_count: U64::new(headers.len() as u64),
    };
    let mut bytes = Vec::with_capacity(
        size_of::<TermFrequenciesHeader>() + headers.len() * size_of::<TermHeader>() + data.len(),
    );
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(headers.as_bytes());
    bytes.extend_from_slice(&data);
    bytes
}

/// Write `bytes`, from [`encode`], to `path` atomically: a reader never sees
/// a partial file, as for the postings.
pub fn write(path: &Path, bytes: &[u8]) -> OperationResult<()> {
    let dir = path
        .parent()
        .ok_or_else(|| OperationError::service_error("term frequencies path has no parent"))?;
    let (file, temp_path) = tempfile::Builder::new()
        .prefix("term_frequencies")
        .tempfile_in(dir)?
        .into_parts();
    let mut file = fs_err::File::from_parts::<&Path>(file, temp_path.as_ref());
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    temp_path.persist(path).map_err(|err| err.error)?;
    Ok(())
}

fn encode_term(frequencies: &[u32], out: &mut Vec<u8>) {
    let packer = BitPacker4x::new();
    let (chunks, remainder) = frequencies.as_chunks::<BLOCK_LEN>();

    let mut packed = Vec::new();
    let mut block_offsets = Vec::with_capacity(chunks.len() + 1);
    let mut block = [0u32; BLOCK_LEN];
    for chunk in chunks {
        block_offsets.push(U32::new(packed.len() as u32));
        stored_values(chunk, &mut block);
        let width = packer.num_bits(&block);
        let start = packed.len();
        packed.resize(start + usize::from(width) * BLOCK_BYTES_PER_BIT, 0);
        packer.compress(&block, &mut packed[start..], width);
    }
    block_offsets.push(U32::new(packed.len() as u32));

    out.extend_from_slice(block_offsets.as_bytes());
    out.extend_from_slice(&packed);

    let width = remainder
        .iter()
        .map(|&frequency| packed_bits(stored(frequency)))
        .max()
        .unwrap_or(0);
    out.push(width);
    let mut writer = BitWriter::new(out);
    for &frequency in remainder {
        writer.write(stored(frequency), width);
    }
    writer.finish();
}

/// A frequency as stored: every indexed term occurs at least once.
fn stored(frequency: u32) -> u32 {
    debug_assert!(frequency > 0, "a posting entry occurs at least once");
    frequency - 1
}

fn stored_values(frequencies: &[u32], out: &mut [u32; BLOCK_LEN]) {
    for (stored_value, &frequency) in out.iter_mut().zip(frequencies) {
        *stored_value = stored(frequency);
    }
}

/// A `term_frequencies.dat` file, over its bytes.
#[derive(Debug, Clone, Copy)]
pub struct TermFrequencies<'a> {
    headers: &'a [TermHeader],
    data: &'a [u8],
}

impl<'a> TermFrequencies<'a> {
    pub fn new(bytes: &'a [u8]) -> OperationResult<Self> {
        let (header, rest) = TermFrequenciesHeader::read_from_prefix(bytes)
            .map_err(|_| corrupted("missing header"))?;
        let (headers, data) =
            <[TermHeader]>::ref_from_prefix_with_elems(rest, header.term_count.get() as usize)
                .map_err(|_| corrupted("truncated term headers"))?;
        Ok(Self { headers, data })
    }

    /// Number of terms, token ids `0..term_count()`.
    #[cfg(test)]
    pub fn term_count(&self) -> usize {
        self.headers.len()
    }

    /// The frequencies of `token_id`'s posting list, `None` past the vocabulary.
    pub fn term(&self, token_id: TokenId) -> OperationResult<Option<TermFrequencyList<'a>>> {
        let Some(header) = self.headers.get(token_id as usize) else {
            return Ok(None);
        };
        let data = self
            .data
            .get(header.offset.get() as usize..)
            .ok_or_else(|| corrupted("term offset past the data"))?;
        TermFrequencyList::parse(header.len.get() as usize, data).map(Some)
    }
}

/// A `term_frequencies.dat` file read through universal IO: the header at
/// open, then only the query terms' headers and data, two batched reads per
/// query.
pub struct OnDiskTermFrequencies<S: UniversalRead> {
    storage: TypedStorage<S, u8>,
    term_count: usize,
    file_len: u64,
}

impl<S: UniversalRead> OnDiskTermFrequencies<S> {
    pub fn open(storage: TypedStorage<S, u8>) -> OperationResult<Self> {
        let header = storage.read(
            ReadRange {
                byte_offset: 0,
                length: size_of::<TermFrequenciesHeader>() as u64,
            },
            Sequential,
        )?;
        let header = TermFrequenciesHeader::read_from_bytes(&header)
            .map_err(|_| corrupted("missing header"))?;
        let term_count = header.term_count.get() as usize;
        let file_len = storage.len()?;
        let data_start = size_of::<TermFrequenciesHeader>() as u64
            + (term_count as u64).saturating_mul(size_of::<TermHeader>() as u64);
        if file_len < data_start {
            return Err(corrupted("truncated term headers"));
        }
        Ok(Self {
            storage,
            term_count,
            file_len,
        })
    }

    /// Number of terms, token ids `0..term_count()`.
    pub fn term_count(&self) -> usize {
        self.term_count
    }

    pub fn storage(&self) -> &TypedStorage<S, u8> {
        &self.storage
    }

    /// The frequencies of each of `token_ids`, in that order, `None` past the
    /// vocabulary, handed to `f`. A term's data ends where the next term's
    /// starts, so its header is read with the next one.
    pub fn with_terms<R>(
        &self,
        token_ids: &[TokenId],
        f: impl FnOnce(Vec<Option<TermFrequencyList<'_>>>) -> OperationResult<R>,
    ) -> OperationResult<R> {
        let header_len = size_of::<TermHeader>() as u64;
        let headers_start = size_of::<TermFrequenciesHeader>() as u64;
        let data_start = headers_start + self.term_count as u64 * header_len;

        let header_ranges = token_ids
            .iter()
            .enumerate()
            .filter(|(_, token_id)| (**token_id as usize) < self.term_count)
            .map(|(at, &token_id)| {
                let with_next = token_id as usize + 1 < self.term_count;
                let range = ReadRange {
                    byte_offset: headers_start + u64::from(token_id) * header_len,
                    length: header_len * (1 + u64::from(with_next)),
                };
                (at, range)
            });
        let mut spans: Vec<Option<(usize, ReadRange)>> = vec![None; token_ids.len()];
        let mut bytes_read = 0;
        self.storage
            .read_batch(header_ranges, Random, |at, bytes: &[u8]| {
                bytes_read += bytes.len() as u64;
                let headers = <[TermHeader]>::ref_from_bytes(bytes)
                    .map_err(|_| corrupted("truncated term headers"))?;
                let start = headers[0].offset.get();
                let end = match headers.get(1) {
                    Some(next) => next.offset.get(),
                    None => self.file_len - data_start,
                };
                if end < start || data_start + end > self.file_len {
                    return Err(corrupted("term data out of order or past the file"));
                }
                spans[at] = Some((
                    headers[0].len.get() as usize,
                    ReadRange {
                        byte_offset: data_start + start,
                        length: end - start,
                    },
                ));
                OperationResult::Ok(())
            })?;

        // Billed as the length and header reads of the same index are.
        bytes_read += spans
            .iter()
            .flatten()
            .map(|(_, range)| range.length)
            .sum::<u64>();
        HwMetric::PayloadIndexIoRead.bump(bytes_read as usize);

        let mut data: Vec<Option<(usize, Cow<'_, [u8]>)>> = vec![None; token_ids.len()];
        let data_ranges = spans
            .iter()
            .enumerate()
            .filter_map(|(at, span)| span.map(|(len, range)| ((at, len), range)));
        for entry in self.storage.read_iter(data_ranges, Sequential)? {
            let ((at, len), bytes) = entry?;
            data[at] = Some((len, bytes));
        }

        let lists = data
            .iter()
            .map(|entry| {
                entry
                    .as_ref()
                    .map(|(len, bytes)| TermFrequencyList::parse(*len, bytes))
                    .transpose()
            })
            .collect::<OperationResult<_>>()?;
        f(lists)
    }
}

impl<'a> TermFrequencyList<'a> {
    /// The frequencies of a list of `len` entries, from its data: the block
    /// offsets, the packed blocks and the remainder. `data` may run past the
    /// remainder.
    fn parse(len: usize, data: &'a [u8]) -> OperationResult<Self> {
        let blocks = len / BLOCK_LEN;
        let (block_offsets, data) = <[U32]>::ref_from_prefix_with_elems(data, blocks + 1)
            .map_err(|_| corrupted("truncated block offsets"))?;
        // Every block is whole, at most 32 bits wide, and follows the last:
        // what `decode_block` relies on to slice and unpack it.
        let well_formed = block_offsets.windows(2).all(|pair| {
            let (start, end) = (pair[0].get(), pair[1].get());
            end >= start && {
                let bytes = (end - start) as usize;
                bytes.is_multiple_of(BLOCK_BYTES_PER_BIT) && bytes / BLOCK_BYTES_PER_BIT <= 32
            }
        });
        if !well_formed || block_offsets[0].get() != 0 {
            return Err(corrupted("malformed block offsets"));
        }
        let packed_len = block_offsets[blocks].get() as usize;
        let (packed, remainder) = data
            .split_at_checked(packed_len)
            .ok_or_else(|| corrupted("truncated blocks"))?;
        Ok(TermFrequencyList {
            len,
            block_offsets,
            packed,
            remainder,
        })
    }
}

/// The frequencies of one posting list, block by block.
#[derive(Debug, Clone, Copy)]
pub struct TermFrequencyList<'a> {
    len: usize,
    block_offsets: &'a [U32],
    packed: &'a [u8],
    /// Starts with the remainder's width; the term's data may run past it.
    remainder: &'a [u8],
}

impl TermFrequencyList<'_> {
    /// Entries in the posting list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Full blocks; the entries after them are the remainder.
    pub fn blocks(&self) -> usize {
        self.block_offsets.len() - 1
    }

    /// The frequencies of block `block`, the posting list's chunk of the same
    /// index.
    pub fn decode_block(&self, block: usize, out: &mut [u32; BLOCK_LEN]) {
        let start = self.block_offsets[block].get() as usize;
        let end = self.block_offsets[block + 1].get() as usize;
        let width = ((end - start) / BLOCK_BYTES_PER_BIT) as u8;
        BitPacker4x::new().decompress(&self.packed[start..end], out, width);
        out.iter_mut().for_each(|frequency| *frequency += 1);
    }

    /// The frequencies of the entries after the last full block, appended to
    /// `out`.
    pub fn decode_remainder(&self, out: &mut Vec<u32>) {
        let count = self.len - self.blocks() * BLOCK_LEN;
        let Some((&width, bits)) = self.remainder.split_first() else {
            return;
        };
        let mut reader = BitReader::new(bits);
        reader.set_bits(width);
        out.extend((0..count).map(|_| reader.read::<u32>() + 1));
    }
}

fn corrupted(what: &str) -> OperationError {
    OperationError::service_error(format!("corrupted term frequencies file: {what}"))
}

#[cfg(test)]
mod tests {
    use rand::rngs::SmallRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    fn decode_all(list: &TermFrequencyList) -> Vec<u32> {
        let mut out = Vec::with_capacity(list.len());
        let mut block = [0; BLOCK_LEN];
        for index in 0..list.blocks() {
            list.decode_block(index, &mut block);
            out.extend_from_slice(&block);
        }
        list.decode_remainder(&mut out);
        out
    }

    /// Every frequency reads back exactly, whatever the list length relative
    /// to a block and however wide the values: ones, small counts, and lone
    /// outliers up to `u32::MAX`.
    #[test]
    fn frequencies_round_trip_exactly() {
        let mut rng = SmallRng::seed_from_u64(42);
        let lengths = [0, 1, 2, 127, 128, 129, 255, 256, 257, 1000, 4096];
        let lists: Vec<Vec<u32>> = lengths
            .iter()
            .flat_map(|&len| {
                let ones = vec![1; len];
                let small: Vec<u32> = (0..len).map(|_| rng.random_range(1..=4)).collect();
                let mut outliers: Vec<u32> = (0..len).map(|_| rng.random_range(1..=3)).collect();
                if len > 0 {
                    outliers[rng.random_range(0..len)] = u32::MAX;
                    outliers[len / 2] = 900;
                }
                [ones, small, outliers]
            })
            // Last, so that its remainder ends the file.
            .chain([vec![1, 2, 3, 900, 1]])
            .collect();

        let bytes = encode(lists.iter().map(Vec::as_slice));
        let file = TermFrequencies::new(&bytes).unwrap();
        assert_eq!(file.term_count(), lists.len());
        for (token_id, expected) in lists.iter().enumerate() {
            let list = file.term(token_id as TokenId).unwrap().unwrap();
            assert_eq!(list.len(), expected.len());
            assert_eq!(&decode_all(&list), expected, "term {token_id}");
        }
        assert!(file.term(lists.len() as TokenId).unwrap().is_none());
    }

    /// The on-disk reader, which reads only the requested terms, answers
    /// what the in-RAM one does: every term including the last, whose data
    /// ends with the file, and ids out of order, repeated and past the
    /// vocabulary. Written through `write`, read through mmap.
    #[test]
    fn on_disk_terms_match_the_whole_file() {
        use common::mmap::AdviceSetting;
        use common::universal_io::{MmapFs, OpenOptions, Populate};

        let _scope = common::ambient::test_guard();
        let mut rng = SmallRng::seed_from_u64(7);
        let lists: Vec<Vec<u32>> = [0, 3, 128, 300, 1000, 5]
            .iter()
            .map(|&len| (0..len).map(|_| rng.random_range(1..=6)).collect())
            .collect();
        let bytes = encode(lists.iter().map(Vec::as_slice));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("term_frequencies.dat");
        write(&path, &bytes).unwrap();

        let storage = TypedStorage::open(
            &MmapFs,
            &path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            (),
        )
        .unwrap();
        let on_disk = OnDiskTermFrequencies::open(storage).unwrap();
        assert_eq!(on_disk.term_count(), lists.len());

        let token_ids: Vec<TokenId> = vec![5, 0, 2, 2, 4, 1, 3, 9];
        let decoded = on_disk
            .with_terms(&token_ids, |terms| {
                Ok(terms
                    .iter()
                    .map(|term| term.as_ref().map(decode_all))
                    .collect::<Vec<_>>())
            })
            .unwrap();
        for (&token_id, decoded) in token_ids.iter().zip(decoded) {
            assert_eq!(
                decoded.as_ref(),
                lists.get(token_id as usize),
                "term {token_id}"
            );
        }
    }

    /// Block offsets that cannot be sliced or unpacked are an error, not a
    /// panic on the scoring path.
    #[test]
    fn malformed_block_offsets_are_an_error() {
        let bytes = encode([vec![2; 2 * BLOCK_LEN]].iter().map(Vec::as_slice));
        let file = TermFrequencies::new(&bytes).unwrap();
        assert!(file.term(0).unwrap().is_some());

        // The second block offset, past the file and term headers.
        let at = size_of::<TermFrequenciesHeader>() + size_of::<TermHeader>() + size_of::<U32>();
        for corrupt in [u32::MAX, 3] {
            let mut bytes = bytes.clone();
            bytes[at..at + size_of::<U32>()].copy_from_slice(&corrupt.to_le_bytes());
            let file = TermFrequencies::new(&bytes).unwrap();
            assert!(file.term(0).is_err(), "offset {corrupt}");
        }
    }

    /// A list of ones packs at width zero: no block bytes, and a remainder
    /// that is its width byte alone.
    #[test]
    fn ones_take_no_block_bytes() {
        let ones = vec![1; 3 * BLOCK_LEN + 5];
        let mut data = Vec::new();
        encode_term(&ones, &mut data);
        // Four block offsets, and the remainder's width byte.
        assert_eq!(data.len(), 4 * size_of::<U32>() + 1);
    }
}
