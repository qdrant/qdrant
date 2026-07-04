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
//! # File format
//!
//! Fixed-size little-endian [`bytemuck::Pod`] records interleaved with raw
//! key bytes. Records are written with [`bytemuck::bytes_of`] and read back
//! by copy ([`bytemuck::pod_read_unaligned`]), so they carry no alignment
//! requirement on their position in the file.
//!
//! ```text
//! prefix_index.bin
//! ┌──────────────────────────────────────────────────────────────────┐
//! │ Header (Pod, 40 bytes)                                           │
//! │   magic             [u8; 8]   = "QdrPrfx\0"                      │
//! │   version           u32       = 1                                │
//! │   _reserved         u32                                          │
//! │   key_count         u64       total keys in the dictionary       │
//! │   block_count       u64                                          │
//! │   block_index_size  u64       bytes in the block index section   │
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Block index — read into RAM at open time; per block:             │
//! │   BlockEntry (Pod, 24 bytes)                                     │
//! │     block_size      u32       bytes of the block in the next     │
//! │                               section                            │
//! │     key_count       u32       keys in the block                  │
//! │     first_key_len   u32                                          │
//! │     _reserved       u32                                          │
//! │     postings_count  u64       Σ postings counts over the block   │
//! │   first_key         u8[first_key_len]   stored in full           │
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Key blocks (~4 KiB each, ≥ 1 key) — fetched and decoded lazily;  │
//! │   per key, front-coded against the previous key in the block:    │
//! │   KeyEntry (Pod, 12 bytes)                                       │
//! │     shared_prefix_len  u32    0 for the block's first key        │
//! │     suffix_len         u32                                       │
//! │     postings_count     u32                                       │
//! │   suffix            u8[suffix_len]                               │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Reading a prefix
//!
//! Keys are sorted, so all keys starting with `prefix` form the contiguous
//! range `[prefix, successor(prefix))`, where `successor` increments the last
//! non-`0xFF` byte. A lookup:
//!
//! 1. binary-searches the resident block index for the candidate block range
//!    (0 storage reads: the first keys bound each block's contents);
//! 2. fetches candidate blocks and decodes them sequentially, reconstructing
//!    keys from shared-prefix/suffix pairs (1 read per block);
//! 3. for aggregate statistics only the two *boundary* blocks are decoded —
//!    interior blocks lie fully inside the range and contribute through the
//!    per-block counts of the resident block index.
//!
//! A block's first key doubles as its separator in the binary search: any
//! string greater than the previous block's last key and not greater than the
//! block's first key would do; the full first key is the simplest correct
//! choice.
//!
//! Keys are opaque byte strings; ordering and prefix semantics are byte-wise.
//! For UTF-8 keys (the keyword index) byte-wise prefix coincides with
//! character-wise prefix.
//!
//! # Module layout
//!
//! - [`format`](self::format) — on-disk layout primitives shared by reader
//!   and writer;
//! - [`writer`](self::writer) — one-pass file construction from sorted
//!   entries;
//! - [`reader`](self::reader) — [`PrefixIndex`], the query surface over a
//!   [`UniversalRead`](common::universal_io::UniversalRead) storage;
//! - [`map_read`](self::map_read) — [`StrMapIndexPrefixRead`], the prefix
//!   enumeration trait implemented by every map index variant.

mod format;
mod map_read;
mod reader;
mod writer;

#[cfg(test)]
mod tests;

pub use self::map_read::StrMapIndexPrefixRead;
pub use self::reader::{PrefixIndex, PrefixIndexStats};
pub use self::writer::build_prefix_index;

pub const PREFIX_INDEX_PATH: &str = "prefix_index.bin";
