//! Sorted key dictionary enabling prefix queries over the keyword map index.
//!
//! Stored in a single [`PREFIX_INDEX_PATH`] file next to the other on-disk map
//! index files. The file is an *ordered view over the keys* of
//! `values_to_points.bin`: it stores no postings — only the keys themselves
//! (front-coded, in byte-lexicographic order) and their postings counts.
//! Presence of the file is what signals "prefix matching supported" at load
//! time; absence means the index was built without the `prefix` option (or by
//! an older version) and prefix queries fall back to slower paths.
//!
//! Layout:
//!
//! | [`Header`] | block index | key blocks |
//!
//! The block index is small and read into RAM at open time; key blocks
//! (~4 KiB each) are fetched and decoded lazily per query. A prefix query
//! therefore costs zero storage reads to locate the candidate key range plus
//! one read per candidate block. Cumulative per-block counts allow estimating
//! the cardinality of a broad prefix by decoding only the two boundary blocks.
//!
//! Keys are opaque byte strings; ordering and prefix semantics are byte-wise.
//! For UTF-8 keys (the keyword index) byte-wise prefix coincides with
//! character-wise prefix.

use std::io::{BufWriter, Write as _};
use std::ops::Range;
use std::path::Path;

use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{
    MmapFile, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFileOps as _,
    UniversalReadFs as _,
};
use fs_err as fs;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::common::operation_error::{OperationError, OperationResult};

pub const PREFIX_INDEX_PATH: &str = "prefix_index.bin";

const MAGIC: [u8; 8] = *b"QdrPrfx\0";
const VERSION: u32 = 1;

/// Target size of one front-coded key block.
const BLOCK_SIZE_TARGET: usize = 4096;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: u32,
    _reserved: u32,
    key_count: u64,
    block_count: u64,
    /// Size in bytes of the block index section (which starts right after the
    /// header).
    block_index_size: u64,
}

const HEADER_SIZE: u64 = size_of::<Header>() as u64;

/// Per-block metadata, parsed from the block index section and kept resident.
struct BlockMeta {
    /// First key of the block, stored in full. Any valid separator (a string
    /// greater than the previous block's last key and not greater than this
    /// block's first key) would do for the binary search; the full first key
    /// is the simplest correct choice.
    first_key: Box<[u8]>,
    /// Byte range of the block within the file.
    bytes: Range<u64>,
    /// Number of keys in this block.
    key_count: u32,
    /// Sum of postings counts over all *preceding* blocks.
    postings_before: u64,
    /// Sum of postings counts within this block.
    postings_count: u64,
}

/// Aggregate statistics over the keys matching a prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PrefixIndexStats {
    /// Number of distinct keys with the prefix.
    pub keys: usize,
    /// Sum of postings counts over those keys. Counts are recorded at build
    /// time, so points deleted afterwards are still included.
    pub postings: usize,
}

pub struct PrefixIndex<S: UniversalRead = MmapFile> {
    storage: S,
    blocks: Vec<BlockMeta>,
    key_count: usize,
}

impl<S: UniversalRead> PrefixIndex<S> {
    /// Open the prefix index if its file exists; `Ok(None)` when the backing
    /// map index was built without prefix support.
    pub fn open(fs: &S::Fs, path: &Path, populate: Populate) -> OperationResult<Option<Self>> {
        let file_path = path.join(PREFIX_INDEX_PATH);
        if !fs.exists(&file_path)? {
            return Ok(None);
        }

        let storage = fs.open(
            &file_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;

        let header_bytes = storage.read_bytes::<Random>(0..HEADER_SIZE, align_of::<Header>())?;
        let header = Header::read_from_bytes(header_bytes.as_ref())
            .map_err(|_| OperationError::service_error("Failed to read prefix index header"))?;

        if header.magic != MAGIC {
            return Err(OperationError::service_error(
                "Prefix index file has invalid magic",
            ));
        }
        if header.version != VERSION {
            return Err(OperationError::service_error(format!(
                "Unsupported prefix index version {}",
                header.version,
            )));
        }

        let index_bytes =
            storage.read_bytes::<Random>(HEADER_SIZE..HEADER_SIZE + header.block_index_size, 1)?;
        let blocks = Self::parse_block_index(
            index_bytes.as_ref(),
            header.block_count,
            HEADER_SIZE + header.block_index_size,
        )?;

        Ok(Some(Self {
            storage,
            blocks,
            key_count: header.key_count as usize,
        }))
    }

    fn parse_block_index(
        mut bytes: &[u8],
        block_count: u64,
        blocks_section_offset: u64,
    ) -> OperationResult<Vec<BlockMeta>> {
        let corrupt = || OperationError::service_error("Prefix index block index is corrupt");

        let mut blocks = Vec::with_capacity(block_count as usize);
        let mut block_offset = blocks_section_offset;
        let mut postings_before = 0u64;
        for _ in 0..block_count {
            let (first_key_len, rest) = read_varint(bytes).ok_or_else(corrupt)?;
            let (block_size, rest) = read_varint(rest).ok_or_else(corrupt)?;
            let (key_count, rest) = read_varint(rest).ok_or_else(corrupt)?;
            let (postings_count, rest) = read_varint(rest).ok_or_else(corrupt)?;
            let first_key = rest.get(..first_key_len as usize).ok_or_else(corrupt)?;
            bytes = &rest[first_key_len as usize..];

            blocks.push(BlockMeta {
                first_key: first_key.into(),
                bytes: block_offset..block_offset + block_size,
                key_count: key_count as u32,
                postings_before,
                postings_count,
            });
            block_offset += block_size;
            postings_before += postings_count;
        }
        Ok(blocks)
    }

    /// Number of distinct keys in the dictionary.
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// RAM used by the resident block index.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            storage: _,
            blocks,
            key_count: _,
        } = self;
        blocks.capacity() * size_of::<BlockMeta>()
            + blocks
                .iter()
                .map(|block| block.first_key.len())
                .sum::<usize>()
    }

    /// Range of blocks that may contain keys starting with `prefix`.
    fn block_range_for_prefix(&self, prefix: &[u8]) -> Range<usize> {
        // The candidate range starts in the last block whose first key is not
        // greater than the prefix (an earlier block's keys are all smaller
        // than this block's first key, hence smaller than any `prefix*`).
        let lo = self
            .blocks
            .partition_point(|block| block.first_key.as_ref() <= prefix)
            .saturating_sub(1);
        // ...and ends before the first block whose first key already lies at
        // or beyond the exclusive upper bound of the prefix range.
        let hi = match prefix_successor(prefix) {
            Some(succ) => self
                .blocks
                .partition_point(|block| block.first_key.as_ref() < succ.as_slice()),
            None => self.blocks.len(),
        };
        lo..hi.max(lo)
    }

    /// Invoke `f(key, postings_count)` for every key starting with `prefix`,
    /// in ascending byte order.
    pub fn for_each_key_with_prefix(
        &self,
        prefix: &[u8],
        hw_counter: &HardwareCounterCell,
        f: &mut dyn FnMut(&[u8], usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        for block_index in self.block_range_for_prefix(prefix) {
            let mut past_range = false;
            self.decode_block(block_index, &hw_counter, &mut |key, count| {
                if key_vs_prefix_range(key, prefix).is_gt() {
                    past_range = true;
                    return Ok(());
                }
                if key.starts_with(prefix) {
                    f(key, count)?;
                }
                Ok(())
            })?;
            if past_range {
                break;
            }
        }
        Ok(())
    }

    /// Aggregate statistics over the keys matching `prefix`.
    ///
    /// Interior blocks of the candidate range are guaranteed to lie fully
    /// within the prefix range and contribute through the precomputed
    /// per-block counts; only the two boundary blocks are decoded.
    pub fn prefix_stats(
        &self,
        prefix: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<PrefixIndexStats> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        let range = self.block_range_for_prefix(prefix);
        let mut stats = PrefixIndexStats::default();

        // Interior blocks: first key > `prefix` (they follow the `lo` block)
        // and all keys smaller than the next block's first key, which is
        // still below the prefix range's upper bound. Byte-lexicographic
        // containment in `[prefix, successor(prefix))` is equivalent to
        // starting with `prefix`, so every key counts.
        let interior = (range.start + 1)..range.end.saturating_sub(1).max(range.start + 1);
        if interior.start < interior.end {
            let first = &self.blocks[interior.start];
            let last = &self.blocks[interior.end - 1];
            stats.postings +=
                (last.postings_before + last.postings_count - first.postings_before) as usize;
            stats.keys += self.blocks[interior.clone()]
                .iter()
                .map(|block| block.key_count as usize)
                .sum::<usize>();
        }

        // Boundary blocks are decoded and filtered per key.
        let mut boundary = |block_index: usize| -> OperationResult<()> {
            let mut keys = 0;
            let mut postings = 0;
            self.decode_block(block_index, &hw_counter, &mut |key, count| {
                if key.starts_with(prefix) {
                    keys += 1;
                    postings += count;
                }
                Ok(())
            })?;
            stats.keys += keys;
            stats.postings += postings;
            Ok(())
        };

        if !range.is_empty() {
            boundary(range.start)?;
            if range.end - range.start > 1 {
                boundary(range.end - 1)?;
            }
        }

        Ok(stats)
    }

    /// Invoke `f(key, postings_count)` for every key in the dictionary, in
    /// ascending byte order.
    pub fn for_each_key(
        &self,
        hw_counter: &HardwareCounterCell,
        f: &mut dyn FnMut(&[u8], usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_key_with_prefix(b"", hw_counter, f)
    }

    fn decode_block(
        &self,
        block_index: usize,
        hw_counter: &ConditionedCounter<'_>,
        f: &mut dyn FnMut(&[u8], usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let block = &self.blocks[block_index];

        hw_counter
            .payload_index_io_read_counter()
            .incr_delta((block.bytes.end - block.bytes.start) as usize);

        let bytes = self.storage.read::<Random, u8>(ReadRange::new(
            block.bytes.start,
            block.bytes.end - block.bytes.start,
        ))?;
        let mut bytes = bytes.as_ref();

        let corrupt = || OperationError::service_error("Prefix index key block is corrupt");

        let mut key = Vec::new();
        for _ in 0..block.key_count {
            let (shared_len, rest) = read_varint(bytes).ok_or_else(corrupt)?;
            let (suffix_len, rest) = read_varint(rest).ok_or_else(corrupt)?;
            let (count, rest) = read_varint(rest).ok_or_else(corrupt)?;
            let suffix = rest.get(..suffix_len as usize).ok_or_else(corrupt)?;
            bytes = &rest[suffix_len as usize..];

            if shared_len as usize > key.len() {
                return Err(corrupt());
            }
            key.truncate(shared_len as usize);
            key.extend_from_slice(suffix);

            f(&key, count as usize)?;
        }
        Ok(())
    }

    /// Populate all pages of the backing storage.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.populate()?;
        Ok(())
    }

    /// Hint that pages backing this index can be reclaimed.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_ram_cache()?;
        Ok(())
    }
}

/// Build the prefix index file from `(key, postings_count)` entries sorted in
/// ascending byte order without duplicates.
pub fn build_prefix_index<'a>(
    path: &Path,
    entries: impl Iterator<Item = (&'a [u8], usize)>,
) -> OperationResult<()> {
    let mut block_index = Vec::new();
    let mut blocks = Vec::new();

    let mut key_count = 0u64;
    let mut block_count = 0u64;

    // Current block state.
    let mut block_start = 0usize;
    let mut block_first_key: Vec<u8> = Vec::new();
    let mut block_key_count = 0u64;
    let mut block_postings = 0u64;
    let mut prev_key: Vec<u8> = Vec::new();

    let mut flush_block = |blocks: &mut Vec<u8>,
                           block_start: usize,
                           first_key: &[u8],
                           key_count: u64,
                           postings: u64| {
        write_varint(&mut block_index, first_key.len() as u64);
        write_varint(&mut block_index, (blocks.len() - block_start) as u64);
        write_varint(&mut block_index, key_count);
        write_varint(&mut block_index, postings);
        block_index.extend_from_slice(first_key);
    };

    for (key, count) in entries {
        debug_assert!(
            block_key_count == 0 && key_count == 0 || prev_key.as_slice() < key,
            "prefix index entries must be sorted and unique",
        );

        if block_key_count > 0 && blocks.len() - block_start >= BLOCK_SIZE_TARGET {
            flush_block(
                &mut blocks,
                block_start,
                &block_first_key,
                block_key_count,
                block_postings,
            );
            block_count += 1;
            block_start = blocks.len();
            block_key_count = 0;
            block_postings = 0;
        }

        let shared_len = if block_key_count == 0 {
            block_first_key.clear();
            block_first_key.extend_from_slice(key);
            0
        } else {
            common_prefix_len(&prev_key, key)
        };

        write_varint(&mut blocks, shared_len as u64);
        write_varint(&mut blocks, (key.len() - shared_len) as u64);
        write_varint(&mut blocks, count as u64);
        blocks.extend_from_slice(&key[shared_len..]);

        prev_key.clear();
        prev_key.extend_from_slice(key);
        block_key_count += 1;
        block_postings += count as u64;
        key_count += 1;
    }

    if block_key_count > 0 {
        flush_block(
            &mut blocks,
            block_start,
            &block_first_key,
            block_key_count,
            block_postings,
        );
        block_count += 1;
    }

    let header = Header {
        magic: MAGIC,
        version: VERSION,
        _reserved: 0,
        key_count,
        block_count,
        block_index_size: block_index.len() as u64,
    };

    let file = fs::File::create(path.join(PREFIX_INDEX_PATH))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(header.as_bytes())?;
    writer.write_all(&block_index)?;
    writer.write_all(&blocks)?;
    writer
        .into_inner()
        .map_err(|err| err.into_error())?
        .sync_all()?;
    Ok(())
}

/// The smallest byte string greater than every string starting with `prefix`,
/// or `None` if no such string exists (empty prefix or all `0xFF`).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|&byte| byte != u8::MAX)?;
    let mut successor = prefix[..=last_incrementable].to_vec();
    successor[last_incrementable] += 1;
    Some(successor)
}

/// Where `key` lies relative to the range of keys starting with `prefix`.
fn key_vs_prefix_range(key: &[u8], prefix: &[u8]) -> std::cmp::Ordering {
    if key.starts_with(prefix) {
        std::cmp::Ordering::Equal
    } else {
        key.cmp(prefix)
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let mut value = 0u64;
    for (i, &byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, &bytes[i + 1..]));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common::universal_io::MmapFs;
    use itertools::Itertools as _;
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};
    use tempfile::TempDir;

    use super::*;

    fn build_and_open(entries: &BTreeMap<Vec<u8>, usize>) -> (TempDir, PrefixIndex) {
        let dir = TempDir::with_prefix("prefix_index").unwrap();
        build_prefix_index(
            dir.path(),
            entries.iter().map(|(key, &count)| (key.as_slice(), count)),
        )
        .unwrap();
        let index = PrefixIndex::open(&MmapFs, dir.path(), Populate::Blocking)
            .unwrap()
            .unwrap();
        (dir, index)
    }

    fn collect_prefix(index: &PrefixIndex, prefix: &[u8]) -> Vec<(Vec<u8>, usize)> {
        let hw_counter = HardwareCounterCell::disposable();
        let mut result = Vec::new();
        index
            .for_each_key_with_prefix(prefix, &hw_counter, &mut |key, count| {
                result.push((key.to_vec(), count));
                Ok(())
            })
            .unwrap();
        result
    }

    fn naive_prefix(entries: &BTreeMap<Vec<u8>, usize>, prefix: &[u8]) -> Vec<(Vec<u8>, usize)> {
        entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, &count)| (key.clone(), count))
            .collect()
    }

    fn check_prefix(index: &PrefixIndex, entries: &BTreeMap<Vec<u8>, usize>, prefix: &[u8]) {
        let expected = naive_prefix(entries, prefix);
        assert_eq!(collect_prefix(index, prefix), expected, "prefix {prefix:?}",);

        let hw_counter = HardwareCounterCell::disposable();
        let stats = index.prefix_stats(prefix, &hw_counter).unwrap();
        assert_eq!(stats.keys, expected.len(), "prefix {prefix:?}");
        assert_eq!(
            stats.postings,
            expected.iter().map(|(_, count)| count).sum::<usize>(),
            "prefix {prefix:?}",
        );
    }

    #[test]
    fn missing_file_opens_as_none() {
        let dir = TempDir::with_prefix("prefix_index").unwrap();
        let index = PrefixIndex::<MmapFile>::open(&MmapFs, dir.path(), Populate::Blocking).unwrap();
        assert!(index.is_none());
    }

    #[test]
    fn empty_dictionary() {
        let entries = BTreeMap::new();
        let (_dir, index) = build_and_open(&entries);
        assert_eq!(index.key_count(), 0);
        check_prefix(&index, &entries, b"");
        check_prefix(&index, &entries, b"anything");
    }

    #[test]
    fn small_dictionary() {
        let entries: BTreeMap<Vec<u8>, usize> = [
            (&b"https://example.com"[..], 3),
            (b"https://qdrant.tech", 7),
            (b"https://qdrant.tech/docs", 2),
            (b"tag", 1),
            (b"tags", 5),
        ]
        .into_iter()
        .map(|(key, count)| (key.to_vec(), count))
        .collect();
        let (_dir, index) = build_and_open(&entries);

        assert_eq!(index.key_count(), 5);
        for prefix in [
            &b""[..],
            b"h",
            b"https://",
            b"https://qdrant.",
            b"https://qdrant.tech",
            b"https://qdrant.tech/docs/more",
            b"tag",
            b"tags",
            b"tagz",
            b"z",
            b"\xff",
        ] {
            check_prefix(&index, &entries, prefix);
        }
    }

    #[test]
    fn multibyte_and_edge_keys() {
        let entries: BTreeMap<Vec<u8>, usize> = [
            "".as_bytes().to_vec(),
            "α".as_bytes().to_vec(),
            "αβ".as_bytes().to_vec(),
            "яблоко".as_bytes().to_vec(),
            vec![0xFF],
            vec![0xFF, 0xFF],
            vec![0xFF, 0xFF, 0x01],
        ]
        .into_iter()
        .enumerate()
        .map(|(i, key)| (key, i + 1))
        .collect();
        let (_dir, index) = build_and_open(&entries);

        for prefix in [
            &b""[..],
            "α".as_bytes(),
            "я".as_bytes(),
            &[0xCE],
            &[0xFF],
            &[0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF],
        ] {
            check_prefix(&index, &entries, prefix);
        }
    }

    #[test]
    fn multi_block_random() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut entries = BTreeMap::new();
        // Enough long keys to span many blocks; skewed shared prefixes.
        for _ in 0..5_000 {
            let base = ["https://", "http://", "ftp://", ""][rng.random_range(0..4)];
            let len = rng.random_range(1..40);
            let tail: String = (0..len)
                .map(|_| char::from(rng.random_range(b'a'..=b'e')))
                .collect();
            entries.insert(
                format!("{base}{tail}").into_bytes(),
                rng.random_range(1..100),
            );
        }
        let (_dir, index) = build_and_open(&entries);
        assert!(index.blocks.len() > 3, "test should span multiple blocks");
        assert_eq!(index.key_count(), entries.len());

        // All keys, in order.
        assert_eq!(
            collect_prefix(&index, b""),
            entries
                .iter()
                .map(|(key, &count)| (key.clone(), count))
                .collect_vec(),
        );

        for prefix in [
            &b""[..],
            b"h",
            b"http",
            b"https://",
            b"https://a",
            b"https://ab",
            b"https://abc",
            b"ftp://e",
            b"a",
            b"ab",
            b"nonexistent",
        ] {
            check_prefix(&index, &entries, prefix);
        }

        // Random probes, including prefixes of existing keys.
        let keys = entries.keys().cloned().collect_vec();
        for _ in 0..200 {
            let key = &keys[rng.random_range(0..keys.len())];
            let len = rng.random_range(0..=key.len());
            check_prefix(&index, &entries, &key[..len]);
        }
    }

    #[test]
    fn prefix_successor_edge_cases() {
        assert_eq!(prefix_successor(b""), None);
        assert_eq!(prefix_successor(&[0xFF]), None);
        assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_successor(b"a"), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(&[b'a', 0xFF]), Some(b"b".to_vec()));
        assert_eq!(
            prefix_successor(&[b'a', 0xFF, b'c']),
            Some(vec![b'a', 0xFF, b'd']),
        );
    }
}
