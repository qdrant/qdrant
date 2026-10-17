//! One-pass construction of the regex index file.

use std::io::{BufWriter, Write as _};
use std::path::Path;

use ahash::AHashMap;
use fs_err as fs;

use super::REGEX_INDEX_PATH;
use super::format::{
    BLOCK_SIZE_TARGET, BlockEntry, Header, KeyEntry, MAGIC, TrigramEntry, VERSION, write_varint,
};
use super::trigram::{Trigram, key_trigrams};
use crate::common::operation_error::OperationResult;

/// Postings of one trigram, delta-encoded as they are collected: ids arrive
/// in ascending order, so the encoding never needs the full id list.
#[derive(Default)]
struct PostingsBuilder {
    bytes: Vec<u8>,
    key_count: u32,
    points: u64,
    last_id: u32,
}

impl PostingsBuilder {
    fn push(&mut self, id: u32, points: usize) {
        let delta = if self.key_count == 0 {
            id
        } else {
            id - self.last_id
        };
        write_varint(&mut self.bytes, delta);
        self.last_id = id;
        self.key_count += 1;
        self.points += points as u64;
    }
}

/// Build the regex index file in `dir` from `(key, postings_count)` entries
/// sorted in ascending byte order without duplicates.
pub fn build_regex_index<'a>(
    dir: &Path,
    entries: impl Iterator<Item = (&'a str, usize)>,
) -> OperationResult<()> {
    let mut block_index: Vec<BlockEntry> = Vec::new();
    let mut blocks: Vec<u8> = Vec::new();
    let mut postings: AHashMap<Trigram, PostingsBuilder> = AHashMap::new();

    let mut key_count = 0u32;
    let mut block_start = 0usize;
    let mut block_keys = 0u32;
    let mut prev_key: Option<&str> = None;

    for (key, count) in entries {
        debug_assert!(
            prev_key.is_none_or(|prev| prev.as_bytes() < key.as_bytes()),
            "regex index entries must be sorted and unique",
        );
        prev_key = Some(key);

        if block_keys > 0 && blocks.len() - block_start >= BLOCK_SIZE_TARGET {
            block_index.push(BlockEntry {
                offset: block_start as u64,
                size: (blocks.len() - block_start) as u32,
                key_count: block_keys,
            });
            block_start = blocks.len();
            block_keys = 0;
        }

        blocks.extend_from_slice(bytemuck::bytes_of(&KeyEntry {
            len: key.len() as u32,
            // Bounded by the number of points, which is a `u32` offset.
            postings_count: count as u32,
        }));
        blocks.extend_from_slice(key.as_bytes());

        for trigram in key_trigrams(key) {
            postings.entry(trigram).or_default().push(key_count, count);
        }

        block_keys += 1;
        key_count += 1;
    }
    if block_keys > 0 {
        block_index.push(BlockEntry {
            offset: block_start as u64,
            size: (blocks.len() - block_start) as u32,
            key_count: block_keys,
        });
    }

    let mut postings: Vec<(Trigram, PostingsBuilder)> = postings.into_iter().collect();
    postings.sort_unstable_by_key(|(trigram, _)| *trigram);

    // Section offsets, from the start of the file.
    let directory_offset =
        size_of::<Header>() as u64 + (block_index.len() * size_of::<BlockEntry>()) as u64;
    let postings_offset = directory_offset + (postings.len() * size_of::<TrigramEntry>()) as u64;
    let postings_size: u64 = postings
        .iter()
        .map(|(_, builder)| builder.bytes.len() as u64)
        .sum();
    let blocks_offset = postings_offset + postings_size;

    let header = Header {
        magic: MAGIC,
        version: VERSION,
        _reserved: 0,
        key_count: u64::from(key_count),
        block_count: block_index.len() as u64,
        trigram_count: postings.len() as u64,
        directory_offset,
        postings_offset,
        blocks_offset,
    };

    let file = fs::File::create(dir.join(REGEX_INDEX_PATH))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(bytemuck::bytes_of(&header))?;
    for block in &block_index {
        writer.write_all(bytemuck::bytes_of(&BlockEntry {
            offset: blocks_offset + block.offset,
            ..*block
        }))?;
    }
    let mut offset = postings_offset;
    for (trigram, builder) in &postings {
        writer.write_all(bytemuck::bytes_of(&TrigramEntry {
            trigram: trigram.to_u32(),
            key_count: builder.key_count,
            points: builder.points,
            offset,
            size: builder.bytes.len() as u32,
            _reserved: 0,
        }))?;
        offset += builder.bytes.len() as u64;
    }
    for (_, builder) in &postings {
        writer.write_all(&builder.bytes)?;
    }
    writer.write_all(&blocks)?;
    writer
        .into_inner()
        .map_err(|err| err.into_error())?
        .sync_all()?;
    Ok(())
}
