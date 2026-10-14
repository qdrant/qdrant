//! On-disk format of the [`CompactedTracker`](super::CompactedTracker) file.
//!
//! [Header], followed by zstd-compressed data.
//! The compressed data stores [`Option<ValuePointer>`] in the following layout:
//!
//! | Field/Section    | Type         | Description                                 |
//! | ---------------- | ------------ | ------------------------------------------- |
//! | `count`          | `u32`        | Determines the length of `bitmap`.          |
//! | `bitmap`         | `count` bits | 0 for None, 1 for Some. M = amount of Some. |
//! | `p.page_id`      | `u32` × M    | Byte-shuffled.                              |
//! | `p.block_offset` | `u32` × M    | Delta-encoded and byte-shuffled.            |
//! | `p.length`       | `u32` × M    | Byte-shuffled.                              |
//!
//! Byte-shuffling aka byte transpositioning aka columnar layout: the first
//! bytes of all values, then all second bytes, and so on.

use std::io::{BufWriter, Write as _};

use bitvec::prelude::{BitSlice, BitVec};
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::tracker::{PointOffset, ValuePointer};

const MAGIC: [u8; 8] = *b"QDRANTCT";
const FORMAT_VERSION: u32 = 1;
const PLANES_COUNT: usize = size_of::<[u32; 3]>();

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct Header {
    magic: [u8; 8],
    version: U32,
}

/// Encode all mappings into the file format, see the module docs.
pub(super) fn encode(pointers: &[Option<ValuePointer>]) -> std::io::Result<Vec<u8>> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    let mut writer = BufWriter::new(encoder);
    writer.write_all(U32::new(pointers.len() as PointOffset).as_bytes())?;

    let bitmap: BitVec<u8> = pointers.iter().map(Option::is_some).collect();
    writer.write_all(bitmap.as_raw_slice())?;

    for byte in 0..PLANES_COUNT {
        let mut end = 0u32;
        for pointer in pointers.iter().flatten() {
            let offset_delta = pointer.block_offset.wrapping_sub(end);
            end = pointer.block_offset.wrapping_add(pointer.length);
            let record = [pointer.page_id, offset_delta, pointer.length].map(U32::new);
            writer.write_all(&record.as_bytes()[byte..=byte])?;
        }
    }
    writer.into_inner()?.finish()
}

/// Decode the file format, see the module docs.
pub(super) fn decode(bytes: &[u8]) -> Result<Vec<Option<ValuePointer>>, String> {
    let (header, payload) =
        Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;
    if header.magic != MAGIC {
        return Err("unexpected magic".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }

    let raw = zstd::decode_all(payload).map_err(|err| err.to_string())?;
    let (count, raw) = U32::read_from_prefix(&raw).map_err(|_| "count is truncated")?;
    let count = count.get() as usize;
    let (bitmap, planes) = raw
        .split_at_checked(count.div_ceil(u8::BITS as usize))
        .ok_or("bitmap is truncated")?;
    let present = &BitSlice::<u8>::from_slice(bitmap)[..count];
    let present_count = present.count_ones();
    if planes.len() != present_count * PLANES_COUNT {
        return Err("record bytes do not match the bitmap".to_string());
    }

    let mut pointers = vec![None; count];
    let mut end = 0u32;
    for (index, point_offset) in present.iter_ones().enumerate() {
        let bytes: [u8; PLANES_COUNT] =
            std::array::from_fn(|byte| planes[byte * present_count + index]);
        let record: [U32; 3] = zerocopy::transmute!(bytes);
        let [page_id, offset_delta, length] = record.map(U32::get);
        let offset = end.wrapping_add(offset_delta);
        end = offset.wrapping_add(length);
        pointers[point_offset] = Some(ValuePointer::new(page_id, offset, length));
    }
    Ok(pointers)
}
