//! On-disk format of the [`CompactedTracker`](super::CompactedTracker) file.
//!
//! ```text
//! ┌─────────┬─────────────┬───────────┬────────────────────────────────────────────┐
//! │ magic   │ version     │ count     │ LZ4 block with prepended size, holding the │
//! │ 8 bytes │ u32 LE      │ u32 LE    │ delta encoded mappings                     │
//! └─────────┴─────────────┴───────────┴────────────────────────────────────────────┘
//! ```
//!
//! Each mapping is a run of varints. A skipped point offset is the single varint `0`. A present
//! mapping is `length + 1`, followed by the page id delta and the offset delta against the
//! previous present mapping, both zigzag encoded. The offset delta is taken against the end of
//! the previous value on the same page, or against `0` on a new page. Values are packed back to
//! back in the append-only pages, so both deltas are almost always zero, and LZ4 squeezes the
//! runs of zeros. Arbitrary pointers still encode correctly, only less compactly.

use integer_encoding::VarInt;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::config::compress_lz4;
use crate::tracker::{PointOffset, ValuePointer};

const MAGIC: [u8; 8] = *b"QDRANTCT";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct Header {
    magic: [u8; 8],
    version: U32,
    count: U32,
}

/// Encode all mappings into the file format, see the module docs.
pub(super) fn encode(pointers: &[Option<ValuePointer>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    let mut prev = ValuePointer::new(0, 0, 0);
    for pointer in pointers {
        let Some(pointer) = pointer else {
            push_varint(&mut encoded, 0u64);
            continue;
        };

        push_varint(&mut encoded, u64::from(pointer.length) + 1);
        push_varint(
            &mut encoded,
            i64::from(pointer.page_id) - i64::from(prev.page_id),
        );
        push_varint(
            &mut encoded,
            i64::from(pointer.block_offset) - expected_offset(prev, pointer.page_id),
        );
        prev = *pointer;
    }

    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        count: U32::new(pointers.len() as PointOffset),
    };
    let mut bytes = header.as_bytes().to_vec();
    bytes.extend_from_slice(&compress_lz4(&encoded));
    bytes
}

/// Decode the file format, see the module docs.
///
/// Every byte is validated: a file that is truncated, has trailing bytes, or holds deltas
/// that do not add up to a valid pointer is rejected.
pub(super) fn decode(bytes: &[u8]) -> std::result::Result<Vec<Option<ValuePointer>>, String> {
    let (header, payload) =
        Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;
    if header.magic != MAGIC {
        return Err("unexpected magic".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }
    let count = header.count.get();

    let encoded = lz4_flex::decompress_size_prepended(payload)
        .map_err(|err| format!("invalid LZ4 block: {err}"))?;

    let mut pointers = Vec::with_capacity(count as usize);
    let mut pos = 0;
    let mut prev = ValuePointer::new(0, 0, 0);
    for _ in 0..count {
        let length_plus_one: u64 = read_varint(&encoded, &mut pos)?;
        let Some(length) = length_plus_one.checked_sub(1) else {
            pointers.push(None);
            continue;
        };
        let length = u32::try_from(length).map_err(|_| "value length out of range")?;

        let page_delta: i64 = read_varint(&encoded, &mut pos)?;
        let page_id = i64::from(prev.page_id)
            .checked_add(page_delta)
            .and_then(|page_id| u32::try_from(page_id).ok())
            .ok_or("page id out of range")?;

        let offset_delta: i64 = read_varint(&encoded, &mut pos)?;
        let block_offset = expected_offset(prev, page_id)
            .checked_add(offset_delta)
            .and_then(|offset| u32::try_from(offset).ok())
            .ok_or("block offset out of range")?;

        let pointer = ValuePointer::new(page_id, block_offset, length);
        pointers.push(Some(pointer));
        prev = pointer;
    }

    if pos != encoded.len() {
        return Err(format!(
            "{} trailing bytes after {count} mappings",
            encoded.len() - pos,
        ));
    }

    Ok(pointers)
}

/// Where the value after `prev` is expected to start: right behind `prev` on the same page, or at
/// the beginning of a new page.
fn expected_offset(prev: ValuePointer, page_id: u32) -> i64 {
    if page_id == prev.page_id {
        i64::from(prev.block_offset) + i64::from(prev.length)
    } else {
        0
    }
}

fn push_varint<T: VarInt>(buf: &mut Vec<u8>, value: T) {
    let mut encoded = [0u8; 10];
    let size = value.encode_var(&mut encoded);
    buf.extend_from_slice(&encoded[..size]);
}

fn read_varint<T: VarInt>(bytes: &[u8], pos: &mut usize) -> std::result::Result<T, String> {
    let (value, size) = T::decode_var(&bytes[*pos..]).ok_or("truncated varint")?;
    *pos += size;
    Ok(value)
}
