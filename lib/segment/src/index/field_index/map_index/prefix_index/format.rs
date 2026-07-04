//! On-disk layout primitives shared by the [`writer`](super::writer) and the
//! [`reader`](super::reader). See the [module docs](super) for the file
//! format diagram.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub(super) const MAGIC: [u8; 8] = *b"QdrPrfx\0";
pub(super) const VERSION: u32 = 1;

/// Target size of one front-coded key block.
pub(super) const BLOCK_SIZE_TARGET: usize = 4096;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
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

pub(super) const HEADER_SIZE: u64 = size_of::<Header>() as u64;

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

pub(super) fn write_varint(out: &mut Vec<u8>, mut value: u64) {
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

pub(super) fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let mut value = 0u64;
    for (i, &byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, &bytes[i + 1..]));
        }
    }
    None
}
