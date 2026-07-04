//! On-disk layout primitives shared by the [`writer`](super::writer) and the
//! [`reader`](super::reader). See the [module docs](super) for the file
//! format diagram.
//!
//! All records are fixed-size, little-endian [`Pod`] structs, written with
//! [`bytemuck::bytes_of`] and read back with [`read_record`] (a copying
//! [`bytemuck::pod_read_unaligned`]), so they carry no alignment requirement
//! on their position in the file.

use bytemuck::{Pod, Zeroable};

pub(super) const MAGIC: [u8; 8] = *b"QdrPrfx\0";
pub(super) const VERSION: u32 = 1;

/// Target size of one front-coded key block.
pub(super) const BLOCK_SIZE_TARGET: usize = 4096;

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Header {
    pub(super) magic: [u8; 8],
    pub(super) version: u32,
    pub(super) _reserved: u32,
    pub(super) key_count: u64,
    pub(super) block_count: u64,
    /// Size in bytes of the block index section (which starts right after the
    /// header).
    pub(super) block_index_size: u64,
}

/// Per-block record of the block index section, followed in the file by the
/// block's first key (`first_key_len` bytes).
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct BlockEntry {
    /// Size in bytes of the block in the key blocks section.
    pub(super) block_size: u32,
    /// Number of keys in the block.
    pub(super) key_count: u32,
    pub(super) first_key_len: u32,
    pub(super) _reserved: u32,
    /// Sum of postings counts over the block.
    pub(super) postings_count: u64,
}

/// Per-key record within a key block, followed by the key's suffix
/// (`suffix_len` bytes). Keys are front-coded against their predecessor;
/// the first key of a block has `shared_prefix_len == 0`.
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct KeyEntry {
    pub(super) shared_prefix_len: u32,
    pub(super) suffix_len: u32,
    pub(super) postings_count: u32,
}

/// Read one fixed-size record from the front of `bytes`; returns the record
/// and the remaining bytes, or `None` if `bytes` is too short.
pub(super) fn read_record<T: Pod>(bytes: &[u8]) -> Option<(T, &[u8])> {
    let (record, rest) = bytes.split_at_checked(size_of::<T>())?;
    Some((bytemuck::pod_read_unaligned(record), rest))
}

/// The smallest byte string greater than every string starting with `prefix`,
/// or `None` if no such string exists (empty prefix or all `0xFF`).
pub(super) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last_incrementable = prefix.iter().rposition(|&byte| byte != u8::MAX)?;
    let mut successor = prefix[..=last_incrementable].to_vec();
    successor[last_incrementable] += 1;
    Some(successor)
}

/// Where `key` lies relative to the range of keys starting with `prefix`.
pub(super) fn key_vs_prefix_range(key: &[u8], prefix: &[u8]) -> std::cmp::Ordering {
    if key.starts_with(prefix) {
        std::cmp::Ordering::Equal
    } else {
        key.cmp(prefix)
    }
}

pub(super) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}
