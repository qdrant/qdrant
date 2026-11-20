//! The compact vocabulary: every token, sorted and front-coded, zstd-compressed.
//!
//! The alternative to the `vocab.dat` hash map for serverless deployments. The
//! hash map is read in place, two dependent reads per looked-up token, which on
//! object storage are two round trips per query. This file is read whole, in one
//! request, when the index opens, and lookups are answered from RAM afterwards.
//! A vocabulary grows with the number of distinct tokens rather than with the
//! number of points, so keeping it resident stays cheap as a segment grows.
//!
//! A fixed 64-byte [`Header`](header::Header), followed by the tokens as its
//! `compression` says:
//!
//! | Field            | Type       | Description                                       |
//! | ---------------- | ---------- | ------------------------------------------------- |
//! | `magic`          | `[u8; 8]`  | `QDRANTVC`.                                       |
//! | `version`        | `u32`      | Format version, 1.                                |
//! | `compression`    | `u32`      | How the tokens follow, see [`Compression`][comp]. |
//! | `count`          | `u64`      | Number of tokens.                                 |
//! | `body_len`       | `u64`      | Bytes of the front-coded body, uncompressed.      |
//! | `keys_per_block` | `u32`      | Tokens per front-coded block.                     |
//! | `reserved`       | `[u8; 28]` | Zero when written, ignored when read.             |
//!
//! `Zstd`, the only compression so far: the front-coded body as one zstd frame
//! with its checksum.
//!
//! [comp]: compression::Compression
//!
//! The body holds the tokens in ascending byte order, in blocks of
//! `keys_per_block` tokens (the last block may hold fewer). Within a block each
//! token is front-coded against the previous one, as two LEB128 varints and the
//! suffix bytes:
//!
//! ```text
//! shared_prefix_len  varint   0 for the first token of a block
//! suffix_len         varint
//! suffix             u8[suffix_len]
//! ```
//!
//! A token's id is its rank, so the file stores no ids: the writer requires
//! the index's token ids to follow token order, which
//! [`ImmutableInvertedIndex`](crate::index::field_index::full_text_index::inverted_index::immutable_inverted_index::ImmutableInvertedIndex)
//! assigns them in.
//!
//! In RAM the body stays front-coded. A lookup binary-searches the blocks by
//! their first token, which is stored in full, then decodes one block.
//!
//! Room to change follows the document length sidecar: a new compression gets a
//! new id, data a reader may ignore goes in `reserved`, and anything an old
//! reader would misread bumps `version`.

mod compression;
mod format;
mod header;
#[cfg(test)]
mod tests;

use self::format::{next_entry, next_key};
pub(super) use self::format::{read, save};
use crate::index::field_index::full_text_index::inverted_index::TokenId;

/// A vocabulary held in RAM, front-coded. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct CompactVocab {
    /// The front-coded body, as the file holds it uncompressed.
    body: Vec<u8>,
    /// Byte offset in `body` of each block.
    block_offsets: Vec<u32>,
    /// The first 8 bytes of each block's first token, see [`anchor`]: the
    /// binary search runs over this array rather than probing the body.
    anchors: Vec<u64>,
    count: usize,
    keys_per_block: usize,
}

/// The first 8 bytes of `key` as a big-endian number, zero-padded. Ordered as
/// the keys are, though not strictly: keys sharing those 8 bytes tie.
fn anchor(key: &[u8]) -> u64 {
    let mut bytes = [0; 8];
    let len = key.len().min(bytes.len());
    bytes[..len].copy_from_slice(&key[..len]);
    u64::from_be_bytes(bytes)
}

impl CompactVocab {
    pub fn keys_count(&self) -> usize {
        self.count
    }

    pub fn ram_usage_bytes(&self) -> usize {
        self.body.capacity()
            + self.block_offsets.capacity() * size_of::<u32>()
            + self.anchors.capacity() * size_of::<u64>()
    }

    /// The id of `token`, `None` when it is not in the vocabulary.
    pub fn get(&self, token: &str) -> Option<TokenId> {
        let token = token.as_bytes();
        // Blocks whose first token is not past `token`; the last of them is
        // the only one that can hold it. A lower anchor is a first token
        // below `token` and a higher one is above it, so only the blocks
        // whose anchor ties need their first token compared.
        let target = anchor(token);
        let below = self.anchors.partition_point(|&a| a < target);
        let tied = self.anchors[below..].partition_point(|&a| a == target);
        let blocks_before = below
            + self.block_offsets[below..below + tied]
                .partition_point(|&offset| self.first_key(offset) <= token);
        let block = blocks_before.checked_sub(1)?;

        // Compared as decoded, without building each token. `matched` is how
        // many leading bytes the previous token, which is below `token`,
        // shares with it; the shared prefix length of the next one tells
        // where it stands without looking at it.
        let mut rest = &self.body[self.block_offsets[block] as usize..];
        let mut matched = 0;
        for rank in self.block_ranks(block) {
            let (shared, suffix) =
                next_entry(&mut rest).expect("compact vocabulary validated when loaded");
            // Same bytes as the previous token past `matched`, so below
            // `token` just as that one is.
            if shared > matched {
                continue;
            }
            // Differs from the previous token where that one still agreed
            // with `token`, and tokens ascend, so this one is above it.
            if shared < matched {
                return None;
            }
            let remaining = &token[matched..];
            let common = suffix
                .iter()
                .zip(remaining)
                .take_while(|(a, b)| a == b)
                .count();
            match (suffix.get(common), remaining.get(common)) {
                (None, None) => return Some(rank),
                // A proper prefix of `token`, or smaller at the first
                // difference: below it.
                (None, Some(_)) => matched += common,
                (Some(ours), Some(theirs)) if ours < theirs => matched += common,
                (Some(_), _) => return None,
            }
        }
        None
    }

    /// Call `f` on every token with its id, in ascending token order.
    pub fn for_each_entry<E>(
        &self,
        mut f: impl FnMut(&str, TokenId) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut rest = self.body.as_slice();
        let mut key = Vec::new();
        for rank in 0..self.count as TokenId {
            next_key(&mut rest, &mut key).expect("compact vocabulary validated when loaded");
            let token =
                std::str::from_utf8(&key).expect("compact vocabulary validated when loaded");
            f(token, rank)?;
        }
        Ok(())
    }

    /// The first token of the block at `offset`, stored in full.
    fn first_key(&self, offset: u32) -> &[u8] {
        let mut rest = &self.body[offset as usize..];
        let (shared, suffix) =
            next_entry(&mut rest).expect("compact vocabulary validated when loaded");
        debug_assert_eq!(shared, 0);
        suffix
    }

    /// The ids of the tokens in `block`.
    fn block_ranks(&self, block: usize) -> std::ops::Range<TokenId> {
        let start = block * self.keys_per_block;
        let end = (start + self.keys_per_block).min(self.count);
        start as TokenId..end as TokenId
    }

    /// Validate a decompressed `body` of `count` tokens in blocks of
    /// `keys_per_block`, and index its blocks.
    fn from_body(body: Vec<u8>, count: usize, keys_per_block: usize) -> Result<Self, String> {
        if keys_per_block == 0 {
            return Err("zero tokens per block".to_string());
        }
        // Walk every token once: lookups decode without checking, and rely on the
        // body being well-formed, valid UTF-8 and strictly ascending.
        let blocks = count.div_ceil(keys_per_block);
        let mut block_offsets = Vec::with_capacity(blocks);
        let mut anchors = Vec::with_capacity(blocks);
        let mut rest = body.as_slice();
        // The previous token, then the current one.
        let mut key = Vec::new();
        for index in 0..count {
            let first_of_block = index % keys_per_block == 0;
            if first_of_block {
                block_offsets.push((body.len() - rest.len()) as u32);
            }
            let (shared, suffix) = next_entry(&mut rest).ok_or("truncated or malformed token")?;
            if first_of_block && shared != 0 {
                return Err(format!("block of token {index} is not self-contained"));
            }
            // Above the previous token exactly when the suffix is above what
            // it replaces.
            if shared > key.len() || (index > 0 && suffix <= &key[shared..]) {
                return Err(format!("token {index} is out of order"));
            }
            // Lookups take the shared prefix to be the whole common prefix.
            if !first_of_block && suffix.first() == key.get(shared) {
                return Err(format!("token {index} understates its shared prefix"));
            }
            key.truncate(shared);
            key.extend_from_slice(suffix);
            if std::str::from_utf8(&key).is_err() {
                return Err(format!("token {index} is not UTF-8"));
            }
            if first_of_block {
                anchors.push(anchor(&key));
            }
        }
        if !rest.is_empty() {
            return Err(format!("{} bytes past the last token", rest.len()));
        }

        Ok(CompactVocab {
            body,
            block_offsets,
            anchors,
            count,
            keys_per_block,
        })
    }
}
