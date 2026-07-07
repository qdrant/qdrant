//! Query surface over the prefix index file: [`PrefixIndex`].

use std::ops::Range;
use std::path::Path;

use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{
    MmapFile, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFileOps,
    UniversalReadFs,
};

use super::PREFIX_INDEX_PATH;
use super::format::{
    BlockEntry, Header, KeyEntry, MAGIC, VERSION, key_vs_prefix_range, prefix_successor,
    read_record,
};
use crate::common::operation_error::{OperationError, OperationResult};

/// Per-block metadata, parsed from the block index section and kept resident.
pub(super) struct BlockMeta {
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
    pub(super) blocks: Vec<BlockMeta>,
    key_count: usize,
}

impl<S: UniversalRead> PrefixIndex<S> {
    /// Open the prefix index if its file exists; `Ok(None)` when the backing
    /// map index was built without prefix support.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<Option<Self>> {
        let file_path = path.join(PREFIX_INDEX_PATH);
        if !UniversalReadFileOps::exists(fs, &file_path)? {
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

        let header_size = size_of::<Header>() as u64;
        let header_bytes = storage.read_bytes::<Random>(0..header_size, align_of::<Header>())?;
        let header: Header = bytemuck::try_pod_read_unaligned(header_bytes.as_ref())
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
            storage.read_bytes::<Random>(header_size..header_size + header.block_index_size, 1)?;
        let blocks = Self::parse_block_index(
            index_bytes.as_ref(),
            header.block_count,
            header_size + header.block_index_size,
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
            let (entry, rest) = read_record::<BlockEntry>(bytes).ok_or_else(corrupt)?;
            let first_key = rest
                .get(..entry.first_key_len as usize)
                .ok_or_else(corrupt)?;
            bytes = &rest[entry.first_key_len as usize..];

            blocks.push(BlockMeta {
                first_key: first_key.into(),
                bytes: block_offset..block_offset + u64::from(entry.block_size),
                key_count: entry.key_count,
                postings_before,
                postings_count: entry.postings_count,
            });
            block_offset += u64::from(entry.block_size);
            postings_before += entry.postings_count;
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
    ///
    /// Candidate blocks are contiguous in the file (by construction), so the
    /// whole candidate range is fetched with a single storage read; the
    /// over-read relative to the exact key range is bounded by the two
    /// partially-matching boundary blocks.
    pub fn for_each_key_with_prefix(
        &self,
        prefix: &[u8],
        hw_counter: &HardwareCounterCell,
        f: &mut dyn FnMut(&[u8], usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        let range = self.block_range_for_prefix(prefix);
        let Some((first_block, last_block)) = self.blocks[range.clone()]
            .first()
            .zip(self.blocks[range.clone()].last())
        else {
            return Ok(());
        };

        let bytes_start = first_block.bytes.start;
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta((last_block.bytes.end - bytes_start) as usize);

        let bytes = self.storage.read::<Random, u8>(ReadRange::new(
            bytes_start,
            last_block.bytes.end - bytes_start,
        ))?;

        for block in &self.blocks[range] {
            let block_bytes = bytes
                .as_ref()
                .get((block.bytes.start - bytes_start) as usize..)
                .ok_or_else(block_corrupt)?;
            let mut past_range = false;
            decode_block(block_bytes, block.key_count, &mut |key, count| {
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
            self.read_and_decode_block(block_index, &hw_counter, &mut |key, count| {
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

    /// Fetch a single block from storage and decode it.
    fn read_and_decode_block(
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
        decode_block(bytes.as_ref(), block.key_count, f)
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

fn block_corrupt() -> OperationError {
    OperationError::service_error("Prefix index key block is corrupt")
}

/// Decode the front-coded keys of one block, reconstructing each key and
/// invoking `f(key, postings_count)` in order.
///
/// `bytes` must start at the block's first [`KeyEntry`] and contain
/// `key_count` records of the following layout (trailing bytes beyond the
/// last record are ignored):
///
/// ```text
/// ┌──────────────────────────────────────┬─────────────────┐
/// │ KeyEntry (Pod, 12 bytes)             │ suffix          │
/// │   shared_prefix_len  u32             │ u8[suffix_len]  │  × key_count
/// │   suffix_len         u32             │                 │
/// │   postings_count     u32             │                 │
/// └──────────────────────────────────────┴─────────────────┘
/// ```
///
/// Each key is reconstructed from its predecessor: keep its first
/// `shared_prefix_len` bytes and append the suffix. The first record of a
/// block has `shared_prefix_len == 0`, so its suffix is the full key:
///
/// ```text
/// (0, 19, _) "https://qdrant.tech"       → https://qdrant.tech
/// (19, 5, _) "/docs"                     → https://qdrant.tech/docs
/// (13, 3, _) "com"                       → https://qdrant.com
/// ```
///
/// Records are read via [`read_record`] (`bytemuck::pod_read_unaligned`),
/// which *copies* the 12 record bytes into an aligned local instead of
/// casting a reference into the buffer. An aligned view (`cast_slice` /
/// `from_bytes`) is not an option here: the variable-length suffixes
/// interleaved between records put every record after the first at an
/// arbitrary, data-dependent offset, and the buffer itself is a slice of a
/// larger storage read starting at an arbitrary file offset — so no
/// alignment can be guaranteed by construction, and a reference cast would
/// be undefined behavior whenever the offset isn't a multiple of 4.
fn decode_block(
    bytes: &[u8],
    key_count: u32,
    f: &mut dyn FnMut(&[u8], usize) -> OperationResult<()>,
) -> OperationResult<()> {
    let mut rolling_bytes = bytes;
    let mut key = Vec::new();
    for _ in 0..key_count {
        let (entry, rest) = read_record::<KeyEntry>(rolling_bytes).ok_or_else(block_corrupt)?;
        let suffix = rest
            .get(..entry.suffix_len as usize)
            .ok_or_else(block_corrupt)?;
        rolling_bytes = &rest[entry.suffix_len as usize..];

        if entry.shared_prefix_len as usize > key.len() {
            return Err(block_corrupt());
        }
        key.truncate(entry.shared_prefix_len as usize);
        key.extend_from_slice(suffix);

        f(&key, entry.postings_count as usize)?;
    }
    Ok(())
}
