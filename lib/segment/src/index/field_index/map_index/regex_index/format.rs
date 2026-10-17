//! On-disk layout of the regex index file, shared by the
//! [`writer`](super::writer) and the [`reader`](super::reader).
//!
//! ```text
//! regex_index.bin
//! ┌──────────────────────────────────────────────────────────────────┐
//! │ Header (Pod, 64 bytes)                                           │
//! │   magic             [u8; 8]   = "QdrRegx\0"                      │
//! │   version           u32       = 1                                │
//! │   _reserved         u32                                          │
//! │   key_count         u64                                          │
//! │   block_count       u64       key blocks                         │
//! │   trigram_count     u64                                          │
//! │   directory_offset  u64       trigram directory section          │
//! │   postings_offset   u64       postings section                   │
//! │   blocks_offset     u64       key blocks section                 │
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Block index — read into RAM at open time:                        │
//! │   BlockEntry (Pod, 16 bytes) × block_count                       │
//! │     offset          u64       of the block, from the file start  │
//! │     size            u32       bytes                              │
//! │     key_count       u32                                          │
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Trigram directory — read into RAM on the first query:            │
//! │   TrigramEntry (Pod, 32 bytes) × trigram_count, by trigram       │
//! │     trigram         u32       3 bytes, big-endian in low 24 bits │
//! │     key_count       u32       keys containing the trigram        │
//! │     points          u64       Σ postings counts of those keys    │
//! │     offset          u64       of its postings, from file start   │
//! │     size            u32       bytes of its postings              │
//! │     _reserved       u32                                          │
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Postings — per trigram, the ids of the keys containing it,      │
//! │   ascending, as LEB128 varints of the gaps (first: the id itself)│
//! ├──────────────────────────────────────────────────────────────────┤
//! │ Key blocks (~4 KiB each, ≥ 1 key), keys in ascending byte order; │
//! │   a key's id is its rank. Per key:                               │
//! │   KeyEntry (Pod, 8 bytes)                                        │
//! │     len             u32                                          │
//! │     postings_count  u32       points with this key at build time │
//! │   key               u8[len]                                      │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! The file is self-contained: it keeps its own copy of the keys, so
//! candidates are verified without touching the map index storage. All
//! records are fixed-size little-endian [`Pod`] structs read back by copy,
//! with no alignment requirement on their position.

use bytemuck::{Pod, Zeroable};

pub(super) const MAGIC: [u8; 8] = *b"QdrRegx\0";
pub(super) const VERSION: u32 = 1;

/// Target size of one key block.
pub(super) const BLOCK_SIZE_TARGET: usize = 4096;

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct Header {
    pub(super) magic: [u8; 8],
    pub(super) version: u32,
    pub(super) _reserved: u32,
    pub(super) key_count: u64,
    pub(super) block_count: u64,
    pub(super) trigram_count: u64,
    pub(super) directory_offset: u64,
    pub(super) postings_offset: u64,
    pub(super) blocks_offset: u64,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct BlockEntry {
    pub(super) offset: u64,
    pub(super) size: u32,
    pub(super) key_count: u32,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct TrigramEntry {
    pub(super) trigram: u32,
    pub(super) key_count: u32,
    pub(super) points: u64,
    pub(super) offset: u64,
    pub(super) size: u32,
    pub(super) _reserved: u32,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub(super) struct KeyEntry {
    pub(super) len: u32,
    pub(super) postings_count: u32,
}

/// Read one fixed-size record from the front of `bytes`; returns the record
/// and the remaining bytes, or `None` if `bytes` is too short.
pub(super) fn read_record<T: Pod>(bytes: &[u8]) -> Option<(T, &[u8])> {
    let (record, rest) = bytes.split_at_checked(size_of::<T>())?;
    Some((bytemuck::pod_read_unaligned(record), rest))
}

/// Append `value` as a LEB128 varint.
pub(super) fn write_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Decode delta-encoded ascending ids, see [`write_varint`]; `None` if the
/// bytes are malformed.
pub(super) fn decode_ids(bytes: &[u8], count: usize) -> Option<Vec<u32>> {
    let mut ids = Vec::with_capacity(count);
    let mut previous = 0u32;
    let mut value = 0u32;
    let mut shift = 0u32;
    for &byte in bytes {
        value |= u32::from(byte & 0x7F).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            previous = if ids.is_empty() {
                value
            } else {
                previous.checked_add(value)?
            };
            ids.push(previous);
            value = 0;
            shift = 0;
        } else {
            shift += 7;
            if shift > 28 {
                return None;
            }
        }
    }
    (shift == 0 && ids.len() == count).then_some(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_ids_round_trip() {
        let ids = [0, 1, 127, 128, 300, 16_384, 1 << 21, u32::MAX];
        let mut bytes = Vec::new();
        let mut previous = 0;
        for (i, &id) in ids.iter().enumerate() {
            write_varint(&mut bytes, if i == 0 { id } else { id - previous });
            previous = id;
        }
        assert_eq!(decode_ids(&bytes, ids.len()).unwrap(), ids);
        // Truncated or miscounted input is rejected.
        assert!(decode_ids(&bytes[..bytes.len() - 1], ids.len()).is_none());
        assert!(decode_ids(&bytes, ids.len() + 1).is_none());
    }
}
