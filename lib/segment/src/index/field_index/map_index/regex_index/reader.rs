//! Query surface over the regex index file: [`RegexIndex`].

use std::borrow::Cow;
use std::ops::Range;
use std::path::Path;
use std::sync::OnceLock;

use common::ambient::hw::HwMetric;
use common::generic_consts::{Random, Sequential};
use common::mmap::AdviceSetting;
use common::universal_io::{
    CachedReadFs, MmapFile, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFs,
};
use ecow::EcoString;

use super::REGEX_INDEX_PATH;
use super::candidates::{Candidates, TrigramPostings, candidates};
use super::format::{
    BlockEntry, Header, KeyEntry, MAGIC, TrigramEntry, VERSION, decode_ids, read_record,
};
use super::map_read::RegexCandidateStats;
use super::matcher::RegexMatcher;
use super::query::TrigramQuery;
use super::trigram::Trigram;
use crate::common::operation_error::{OperationError, OperationResult};

/// Read granularity of a scan over every key.
const SCAN_CHUNK_BYTES: u64 = 1024 * 1024;

/// Opening reads the header and the block index; covers ~2k blocks.
const OPEN_READ_SIZE: u64 = size_of::<Header>() as u64 + 32 * 1024;

struct BlockMeta {
    /// Byte range of the block within the file.
    bytes: Range<u64>,
    /// Id of the block's first key.
    first_id: u32,
    key_count: u32,
}

/// Trigram index over the keys of an immutable map index, served from a
/// [`UniversalRead`] storage.
pub struct RegexIndex<S: UniversalRead = MmapFile> {
    storage: S,
    header: Header,
    blocks: Vec<BlockMeta>,
    /// Trigram directory, read on the first query: an index opened for other
    /// conditions never pays for it.
    directory: OnceLock<Vec<TrigramEntry>>,
}

impl<S: UniversalRead> RegexIndex<S> {
    fn open_options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: populate.or_partial(0..OPEN_READ_SIZE),
            advice: AdviceSetting::Global,
        }
    }

    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        let file_path = dir.join(REGEX_INDEX_PATH);
        if UniversalReadFs::exists(fs, &file_path)? {
            fs.schedule_open(&file_path, Some(Self::open_options(populate)), None);
        }
        Ok(())
    }

    /// Open the regex index if its file exists; `Ok(None)` when the backing
    /// map index was built without regex support.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        populate: Populate,
    ) -> OperationResult<Option<Self>> {
        let file_path = dir.join(REGEX_INDEX_PATH);
        if !UniversalReadFs::exists(fs, &file_path)? {
            return Ok(None);
        }

        let storage = fs.open(&file_path, Self::open_options(populate), Default::default())?;

        let header_size = size_of::<Header>() as u64;
        let header_bytes = storage.read_bytes(0..header_size, Random, 1)?;
        let (header, _) = read_record::<Header>(header_bytes.as_ref())
            .ok_or_else(|| corrupt("header is truncated"))?;
        if header.magic != MAGIC {
            return Err(corrupt("invalid magic"));
        }
        if header.version != VERSION {
            return Err(OperationError::service_error(format!(
                "Unsupported regex index version {}",
                header.version,
            )));
        }

        let index_bytes = storage.read_bytes(header_size..header.directory_offset, Random, 1)?;
        let mut rest = index_bytes.as_ref();
        let mut blocks = Vec::with_capacity(header.block_count as usize);
        let mut first_id = 0u32;
        for _ in 0..header.block_count {
            let (entry, tail) = read_record::<BlockEntry>(rest)
                .ok_or_else(|| corrupt("block index is truncated"))?;
            rest = tail;
            blocks.push(BlockMeta {
                bytes: entry.offset..entry.offset + u64::from(entry.size),
                first_id,
                key_count: entry.key_count,
            });
            first_id += entry.key_count;
        }
        if u64::from(first_id) != header.key_count {
            return Err(corrupt("block index doesn't add up to the key count"));
        }

        Ok(Some(Self {
            storage,
            header,
            blocks,
            directory: OnceLock::new(),
        }))
    }

    pub fn key_count(&self) -> usize {
        self.header.key_count as usize
    }

    fn directory(&self) -> OperationResult<&[TrigramEntry]> {
        if let Some(directory) = self.directory.get() {
            return Ok(directory);
        }
        let range = self.header.directory_offset..self.header.postings_offset;
        HwMetric::PayloadIndexIoRead.bump((range.end - range.start) as usize);
        let bytes = self.storage.read_bytes(range, Sequential, 1)?;
        let mut rest = bytes.as_ref();
        let mut directory = Vec::with_capacity(self.header.trigram_count as usize);
        for _ in 0..self.header.trigram_count {
            let (entry, tail) = read_record::<TrigramEntry>(rest)
                .ok_or_else(|| corrupt("trigram directory is truncated"))?;
            rest = tail;
            directory.push(entry);
        }
        // A concurrent first query may have set it already; both read the same.
        Ok(self.directory.get_or_init(|| directory))
    }

    /// Keys `matcher` matches, in ascending byte order.
    pub fn matching_keys(&self, matcher: &RegexMatcher) -> OperationResult<Vec<EcoString>> {
        let postings = Postings {
            index: self,
            directory: self.directory()?,
        };
        let mut keys = Vec::new();
        let mut check = |key: &str| {
            if matcher.is_match(key) {
                keys.push(EcoString::from(key));
            }
        };
        match candidates(matcher.query(), &postings)? {
            Candidates::All => self.for_each_key(&mut check)?,
            Candidates::Ids(ids) => self.for_each_key_of(&ids, &mut check)?,
        }
        Ok(keys)
    }

    /// Upper bounds over the keys `query` admits, from the trigram directory
    /// alone; `None` when the query admits every key. Point counts are taken
    /// at build time, so they include points deleted since.
    pub fn candidate_stats(
        &self,
        query: &TrigramQuery,
    ) -> OperationResult<Option<RegexCandidateStats>> {
        let directory = self.directory()?;
        Ok(candidate_bounds(query, directory))
    }

    /// Invoke `f` on every key, in ascending byte order.
    fn for_each_key(&self, f: &mut dyn FnMut(&str)) -> OperationResult<()> {
        let mut chunk_start = 0;
        while chunk_start < self.blocks.len() {
            let bytes_start = self.blocks[chunk_start].bytes.start;
            let mut chunk_end = chunk_start + 1;
            while chunk_end < self.blocks.len()
                && self.blocks[chunk_end].bytes.end - bytes_start <= SCAN_CHUNK_BYTES
            {
                chunk_end += 1;
            }
            let bytes_end = self.blocks[chunk_end - 1].bytes.end;

            HwMetric::PayloadIndexIoRead.bump((bytes_end - bytes_start) as usize);
            let bytes = self.storage.read::<_, u8>(
                ReadRange::new(bytes_start, bytes_end - bytes_start),
                Sequential,
            )?;

            for block in &self.blocks[chunk_start..chunk_end] {
                let block_bytes = bytes
                    .get((block.bytes.start - bytes_start) as usize..)
                    .ok_or_else(|| corrupt("key block is truncated"))?;
                decode_block(block_bytes, block.key_count, &mut |_, key| {
                    f(key);
                    true
                })?;
            }
            chunk_start = chunk_end;
        }
        Ok(())
    }

    /// Invoke `f` on the keys with the given ids (ascending), reading only
    /// the blocks that hold them.
    fn for_each_key_of(&self, ids: &[u32], f: &mut dyn FnMut(&str)) -> OperationResult<()> {
        // Group the ids by block: (block index, range of `ids`).
        let mut groups: Vec<(usize, Range<usize>)> = Vec::new();
        let mut start = 0;
        while start < ids.len() {
            let block_index = self
                .blocks
                .partition_point(|block| block.first_id <= ids[start])
                .checked_sub(1)
                .ok_or_else(|| corrupt("candidate id out of range"))?;
            let block = &self.blocks[block_index];
            let block_end = block.first_id + block.key_count;
            let end = start + ids[start..].partition_point(|&id| id < block_end);
            if end == start {
                return Err(corrupt("candidate id out of range"));
            }
            groups.push((block_index, start..end));
            start = end;
        }

        let reads = groups.into_iter().map(|(block_index, ids_range)| {
            let block = &self.blocks[block_index];
            HwMetric::PayloadIndexIoRead.bump((block.bytes.end - block.bytes.start) as usize);
            (
                (block_index, ids_range),
                ReadRange::new(block.bytes.start, block.bytes.end - block.bytes.start),
            )
        });

        self.storage.read_batch::<_, u8, _, OperationError>(
            reads,
            Random,
            |(block_index, ids_range), bytes| {
                let block = &self.blocks[block_index];
                let mut wanted = ids[ids_range].iter().copied().peekable();
                decode_block(bytes, block.key_count, &mut |offset, key| {
                    if wanted.peek() == Some(&(block.first_id + offset)) {
                        wanted.next();
                        f(key);
                    }
                    wanted.peek().is_some()
                })
            },
        )
    }

    /// RAM held by the resident block index and, once read, the directory.
    pub fn ram_usage_bytes(&self) -> usize {
        self.blocks.capacity() * size_of::<BlockMeta>()
            + self.directory.get().map_or(0, |directory| {
                directory.capacity() * size_of::<TrigramEntry>()
            })
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

struct Postings<'a, S: UniversalRead> {
    index: &'a RegexIndex<S>,
    directory: &'a [TrigramEntry],
}

impl<S: UniversalRead> Postings<'_, S> {
    fn entry(&self, trigram: Trigram) -> Option<&TrigramEntry> {
        let trigram = trigram.to_u32();
        self.directory
            .binary_search_by_key(&trigram, |entry| entry.trigram)
            .ok()
            .map(|pos| &self.directory[pos])
    }
}

impl<S: UniversalRead> TrigramPostings for Postings<'_, S> {
    fn postings_len(&self, trigram: Trigram) -> usize {
        self.entry(trigram)
            .map_or(0, |entry| entry.key_count as usize)
    }

    fn postings(&self, trigram: Trigram) -> OperationResult<Cow<'_, [u32]>> {
        let Some(entry) = self.entry(trigram) else {
            return Ok(Cow::Borrowed(&[]));
        };
        HwMetric::PayloadIndexIoRead.bump(entry.size as usize);
        let bytes = self.index.storage.read_bytes(
            entry.offset..entry.offset + u64::from(entry.size),
            Random,
            1,
        )?;
        let ids = decode_ids(bytes.as_ref(), entry.key_count as usize)
            .ok_or_else(|| corrupt("postings are malformed"))?;
        Ok(Cow::Owned(ids))
    }
}

/// `(keys, points)` upper bounds of what `query` admits; `None` if unbounded.
fn candidate_bounds(
    query: &TrigramQuery,
    directory: &[TrigramEntry],
) -> Option<RegexCandidateStats> {
    match query {
        TrigramQuery::All => None,
        TrigramQuery::None => Some(RegexCandidateStats::default()),
        TrigramQuery::Trigram(trigram) => {
            let trigram = trigram.to_u32();
            Some(
                directory
                    .binary_search_by_key(&trigram, |entry| entry.trigram)
                    .map_or_else(
                        |_| RegexCandidateStats::default(),
                        |pos| RegexCandidateStats {
                            keys: directory[pos].key_count as usize,
                            postings: directory[pos].points as usize,
                        },
                    ),
            )
        }
        TrigramQuery::And(children) => children
            .iter()
            .filter_map(|child| candidate_bounds(child, directory))
            .reduce(|a, b| RegexCandidateStats {
                keys: a.keys.min(b.keys),
                postings: a.postings.min(b.postings),
            }),
        TrigramQuery::Or(children) => {
            children
                .iter()
                .try_fold(RegexCandidateStats::default(), |acc, child| {
                    let bounds = candidate_bounds(child, directory)?;
                    Some(RegexCandidateStats {
                        keys: acc.keys + bounds.keys,
                        postings: acc.postings + bounds.postings,
                    })
                })
        }
    }
}

/// Decode the keys of one block, invoking `f(offset in block, key)` until it
/// returns `false`.
fn decode_block(
    mut bytes: &[u8],
    key_count: u32,
    f: &mut dyn FnMut(u32, &str) -> bool,
) -> OperationResult<()> {
    for offset in 0..key_count {
        let (entry, rest) =
            read_record::<KeyEntry>(bytes).ok_or_else(|| corrupt("key block is truncated"))?;
        let key = rest
            .get(..entry.len as usize)
            .ok_or_else(|| corrupt("key block is truncated"))?;
        bytes = &rest[entry.len as usize..];
        let key = std::str::from_utf8(key).map_err(|_| corrupt("key is not UTF-8"))?;
        if !f(offset, key) {
            break;
        }
    }
    Ok(())
}

fn corrupt(what: &str) -> OperationError {
    OperationError::service_error(format!("Regex index is corrupt: {what}"))
}
