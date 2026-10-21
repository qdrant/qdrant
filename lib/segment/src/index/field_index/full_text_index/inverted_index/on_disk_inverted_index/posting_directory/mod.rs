//! The posting directory: the header of every posting list, and where every
//! term's frequencies lie, compressed, read whole when the index opens.
//!
//! `postings.dat` keeps one 24-byte [`PostingListHeader`] per token ahead of
//! the posting lists, and `term_frequencies.dat` one 12-byte header per token
//! ahead of the frequencies. Reading either in place takes two dependent
//! reads per term: its header, then the bytes the header points at. On object
//! storage those are round trips per query and segment. This file holds the
//! same information in a few bytes per token, so the index answers where a
//! posting list and its frequencies are, and how long, from RAM, and a query
//! reads their bytes alone. Like the vocabulary, it grows with the number of
//! distinct tokens rather than with the number of points.
//!
//! Nothing in it is new information: `postings.dat` and `term_frequencies.dat`
//! are unchanged, and an index without this file reads their headers in place
//! as before.
//!
//! A fixed 64-byte [`Header`](header::Header), followed by the entries as its
//! `compression` says:
//!
//! | Field               | Type      | Description                                        |
//! | ------------------- | --------- | -------------------------------------------------- |
//! | `magic`             | `[u8; 8]` | `QDRANTPD`.                                        |
//! | `version`           | `u32`     | Format version, 1.                                 |
//! | `compression`       | `u32`     | How the entries follow, see [`Compression`][comp]. |
//! | `count`             | `u64`     | Number of posting lists.                           |
//! | `body_len`          | `u64`     | Bytes of the entries, uncompressed.                |
//! | `data_start`        | `u64`     | Offset in `postings.dat` of the first list.        |
//! | `data_end`          | `u64`     | Offset in `postings.dat` past the last list.       |
//! | `entries_per_block` | `u32`     | Entries per block of the in-RAM index.             |
//! | `flags`             | `u32`     | Bit 0: entries carry term frequency sizes.         |
//! | `frequencies_len`   | `u64`     | Bytes of frequency data, when the flag is set.     |
//!
//! `Zstd`, the only compression so far: the entries as one zstd frame with its
//! checksum.
//!
//! [comp]: compression::Compression
//!
//! One entry per token id, in id order, as LEB128 varints: four fields of the
//! posting list header, `chunks_count`, `remainder_count`,
//! `ids_data_bytes_count` and `var_size_data_bytes_count`, then, with the
//! flag, the size of the term's frequency data. The rest is implied: the
//! alignment pads the variable-length data to [`ALIGNMENT`](super::types::ALIGNMENT), as the writer
//! does; posting lists are written back to back in id order, so a list's
//! offset is `data_start` plus the sizes of the lists before it; frequency
//! data is too, from the start of the frequency data; and a term has one
//! frequency per posting.
//!
//! In RAM the entries stay encoded, with the offsets of every block's first
//! list and first frequencies beside them; a lookup decodes at most one block.
//!
//! The file is checked against the files it describes when it is read: a
//! directory that disagrees with them would send every read to the wrong
//! bytes, so on any mismatch the index ignores the part that disagrees and
//! reads those headers in place.

mod compression;
mod entry;
mod format;
mod frequency_span;
mod header;
#[cfg(test)]
mod tests;

use self::entry::{Entry, next_entry};
pub(super) use self::format::{read, save};
pub use self::frequency_span::FrequencySpan;
use super::types::{PostingListHeader, ZerocopyPostingValue};
use crate::index::field_index::full_text_index::inverted_index::TokenId;

pub(in super::super) const POSTING_DIRECTORY_FILE: &str = "posting_directory.dat";

/// Every posting list header of an index, held in RAM, and the spans of its
/// term frequencies when the index scores. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct PostingDirectory {
    /// The entries, as the file holds them uncompressed.
    body: Vec<u8>,
    /// Offset in `body` of each block.
    block_body_offsets: Vec<u32>,
    /// Offset in `postings.dat` of the first posting list of each block.
    block_data_offsets: Vec<u64>,
    /// Offset of the first term's frequencies of each block, and the length of
    /// all frequency data, when the entries carry frequency sizes.
    frequencies: Option<(Vec<u64>, u64)>,
    count: usize,
    entries_per_block: usize,
}

impl PostingDirectory {
    pub fn ram_usage_bytes(&self) -> usize {
        self.body.capacity()
            + self.block_body_offsets.capacity() * size_of::<u32>()
            + self.block_data_offsets.capacity() * size_of::<u64>()
            + self
                .frequencies
                .as_ref()
                .map_or(0, |(offsets, _)| offsets.capacity() * size_of::<u64>())
    }

    /// Number of posting lists.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Bytes of frequency data the spans cover, `None` when the directory
    /// records no frequencies.
    pub fn frequencies_len(&self) -> Option<u64> {
        self.frequencies.as_ref().map(|&(_, len)| len)
    }

    /// The header of the posting list of `token_id`, `None` past the last one.
    pub fn header<V: ZerocopyPostingValue>(&self, token_id: TokenId) -> Option<PostingListHeader> {
        let (entry, _) = self.entry::<V>(token_id)?;
        Some(entry.header)
    }

    /// Where the frequencies of `token_id` lie, `None` past the last token or
    /// when the directory records no frequencies.
    pub fn frequency_span(&self, token_id: TokenId) -> Option<FrequencySpan> {
        self.frequencies.as_ref()?;
        // The frequency spans do not depend on the posting values' layout.
        let (entry, offset) = self.entry::<()>(token_id)?;
        Some(FrequencySpan {
            len: entry.header.posting_len(),
            offset,
            size: entry.frequencies_size,
        })
    }

    /// The entry of `token_id`, with its frequencies' offset.
    fn entry<V: ZerocopyPostingValue>(&self, token_id: TokenId) -> Option<(Entry, u64)> {
        let token_id = token_id as usize;
        if token_id >= self.count {
            return None;
        }
        let block = token_id / self.entries_per_block;
        let with_frequencies = self.frequencies.is_some();
        let mut rest = &self.body[self.block_body_offsets[block] as usize..];
        let mut offset = self.block_data_offsets[block];
        let mut frequencies_offset = self
            .frequencies
            .as_ref()
            .map_or(0, |(offsets, _)| offsets[block]);
        for _ in block * self.entries_per_block..token_id {
            let entry = next_entry(&mut rest, offset, with_frequencies)
                .expect("posting directory validated when loaded");
            offset += entry.header.posting_size::<V>() as u64;
            frequencies_offset += entry.frequencies_size;
        }
        let entry = next_entry(&mut rest, offset, with_frequencies)
            .expect("posting directory validated when loaded");
        Some((entry, frequencies_offset))
    }
}
