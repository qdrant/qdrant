//! One-pass construction of the prefix index file.

use std::io::{BufWriter, Write as _};
use std::path::Path;

use fs_err as fs;
use zerocopy::IntoBytes as _;

use super::PREFIX_INDEX_PATH;
use super::format::{BLOCK_SIZE_TARGET, Header, MAGIC, VERSION, common_prefix_len, write_varint};
use crate::common::operation_error::OperationResult;

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
