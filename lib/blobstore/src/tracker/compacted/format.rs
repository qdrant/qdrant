//! On-disk format of the [`CompactedTracker`](super::CompactedTracker) file.
//!
//! ```text
//! ┌─────────┬─────────┬─────────┬──────┬─────────┬────────────┐
//! │ magic   │ version │ count   │ gaps │ lengths │ exceptions │
//! │ 8 bytes │ u32 LE  │ u32 LE  │      │         │            │
//! └─────────┴─────────┴─────────┴──────┴─────────┴────────────┘
//! ```
//!
//! The mappings are stored as three columns, all numbers are varints unless noted otherwise:
//!
//! - **gaps**: the number of skipped point offsets, then each skipped point offset as the
//!   distance from one past the previous skipped one. The remaining point offsets are *present*.
//! - **lengths**: the value length of each present mapping, in blocks of [`BLOCK_LEN`] (the last
//!   block may be shorter). A block is its minimum length, a `u8` bit width, and each length minus
//!   the minimum bit-packed at that width, padded to a whole byte.
//! - **exceptions**: the number of present mappings that do not start where the previous present
//!   value ends on the same page (the first one is expected at page 0, offset 0), then for each
//!   of them: its index among the present mappings as the distance from one past the previous
//!   exception, its page id, and its offset.
//!
//! Values are packed back to back in the append-only pages, so exceptions only occur on page
//! rollover, and the lengths, which carry nearly all of the information, take a few bits each.
//! Arbitrary pointers still encode correctly, only less compactly.

use common::bitpacking::{BitReader, BitWriter, packed_bits};
use integer_encoding::VarInt;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::tracker::{PointOffset, ValuePointer};

const MAGIC: [u8; 8] = *b"QDRANTCT";
const FORMAT_VERSION: u32 = 1;

/// Number of lengths bit-packed at one width
pub(super) const BLOCK_LEN: usize = 128;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct Header {
    magic: [u8; 8],
    version: U32,
    count: U32,
}

/// Encode all mappings into the file format, see the module docs.
pub(super) fn encode(pointers: &[Option<ValuePointer>]) -> Vec<u8> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        count: U32::new(pointers.len() as PointOffset),
    };
    let mut bytes = header.as_bytes().to_vec();

    let gaps = pointers
        .iter()
        .enumerate()
        .filter(|(_, pointer)| pointer.is_none())
        .map(|(point_offset, _)| point_offset as u64)
        .collect::<Vec<_>>();
    push_varint(&mut bytes, gaps.len() as u64);
    push_distances(&mut bytes, &gaps);

    let present = pointers.iter().flatten().copied().collect::<Vec<_>>();
    for block in present.chunks(BLOCK_LEN) {
        let min = block
            .iter()
            .map(|pointer| pointer.length)
            .min()
            .unwrap_or(0);
        let max = block
            .iter()
            .map(|pointer| pointer.length)
            .max()
            .unwrap_or(0);
        let bits = packed_bits(max - min);
        push_varint(&mut bytes, min);
        bytes.push(bits);
        let mut writer = BitWriter::new(&mut bytes);
        for pointer in block {
            writer.write(pointer.length - min, bits);
        }
        writer.finish();
    }

    let mut prev_end = Some((0, 0));
    let exceptions = present
        .iter()
        .enumerate()
        .filter(|(_, pointer)| {
            let expected = prev_end;
            prev_end = end_of(pointer);
            expected != Some((pointer.page_id, pointer.block_offset))
        })
        .collect::<Vec<_>>();
    push_varint(&mut bytes, exceptions.len() as u64);
    let indices = exceptions
        .iter()
        .map(|(index, _)| *index as u64)
        .collect::<Vec<_>>();
    push_distances(&mut bytes, &indices);
    for (_, pointer) in &exceptions {
        push_varint(&mut bytes, pointer.page_id);
        push_varint(&mut bytes, pointer.block_offset);
    }

    bytes
}

/// Decode the file format, see the module docs.
///
/// Every byte is validated: a file that is truncated, has trailing bytes, or holds columns
/// that do not add up to valid pointers is rejected.
pub(super) fn decode(bytes: &[u8]) -> std::result::Result<Vec<Option<ValuePointer>>, String> {
    let (header, payload) =
        Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;
    if header.magic != MAGIC {
        return Err("unexpected magic".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }
    let count = header.count.get() as usize;
    let mut pos = 0;

    let gap_count = read_count(payload, &mut pos, count, "gaps")?;
    let gaps = read_distances(payload, &mut pos, gap_count, count, "gap")?;
    let present_count = count - gap_count;

    let mut lengths = Vec::with_capacity(present_count);
    while lengths.len() < present_count {
        let block_len = BLOCK_LEN.min(present_count - lengths.len());
        let min: u32 = read_varint(payload, &mut pos)?;
        let bits = *payload.get(pos).ok_or("truncated block width")?;
        pos += 1;
        if u32::from(bits) > u32::BITS {
            return Err(format!("block width {bits} out of range"));
        }
        let packed_len = (block_len * usize::from(bits)).div_ceil(u8::BITS as usize);
        let packed = payload
            .get(pos..pos + packed_len)
            .ok_or("truncated length block")?;
        pos += packed_len;
        let mut reader = BitReader::new(packed);
        reader.set_bits(bits);
        for _ in 0..block_len {
            let length = min
                .checked_add(reader.read::<u32>())
                .ok_or("value length out of range")?;
            lengths.push(length);
        }
    }

    let exception_count = read_count(payload, &mut pos, present_count, "exceptions")?;
    let exception_indices = read_distances(
        payload,
        &mut pos,
        exception_count,
        present_count,
        "exception",
    )?;
    let mut exceptions = Vec::with_capacity(exception_count);
    for index in exception_indices {
        let page_id: u32 = read_varint(payload, &mut pos)?;
        let block_offset: u32 = read_varint(payload, &mut pos)?;
        exceptions.push((index, (page_id, block_offset)));
    }

    if pos != payload.len() {
        return Err(format!(
            "{} trailing bytes after {count} mappings",
            payload.len() - pos,
        ));
    }

    let mut pointers = vec![None; count];
    let mut exceptions = exceptions.into_iter().peekable();
    let mut prev_end = Some((0, 0));
    let present_offsets =
        (0..count).filter(|point_offset| gaps.binary_search(point_offset).is_err());
    for (index, (point_offset, length)) in present_offsets.zip(lengths).enumerate() {
        let (page_id, block_offset) = match exceptions.next_if(|(at, _)| *at == index) {
            Some((_, start)) => start,
            None => prev_end.ok_or("block offset out of range")?,
        };
        let pointer = ValuePointer::new(page_id, block_offset, length);
        prev_end = end_of(&pointer);
        pointers[point_offset] = Some(pointer);
    }

    Ok(pointers)
}

/// Page and offset right behind `pointer`, where the next value is expected to start, or `None`
/// if that offset does not fit.
fn end_of(pointer: &ValuePointer) -> Option<(u32, u32)> {
    let end = pointer.block_offset.checked_add(pointer.length)?;
    Some((pointer.page_id, end))
}

/// Push strictly increasing `values`, each as the distance from one past the previous one.
fn push_distances(buf: &mut Vec<u8>, values: &[u64]) {
    let mut next = 0;
    for &value in values {
        push_varint(buf, value - next);
        next = value + 1;
    }
}

/// Read `n` values pushed by [`push_distances`], all of which must be below `bound`.
fn read_distances(
    bytes: &[u8],
    pos: &mut usize,
    n: usize,
    bound: usize,
    what: &str,
) -> std::result::Result<Vec<usize>, String> {
    let mut values = Vec::with_capacity(n);
    let mut next = 0usize;
    for _ in 0..n {
        let distance: u64 = read_varint(bytes, pos)?;
        let value = usize::try_from(distance)
            .ok()
            .and_then(|distance| next.checked_add(distance))
            .filter(|&value| value < bound)
            .ok_or_else(|| format!("{what} index out of range"))?;
        values.push(value);
        next = value + 1;
    }
    Ok(values)
}

/// Read a number of entries, which must not exceed `bound`.
fn read_count(
    bytes: &[u8],
    pos: &mut usize,
    bound: usize,
    what: &str,
) -> std::result::Result<usize, String> {
    let count: u64 = read_varint(bytes, pos)?;
    usize::try_from(count)
        .ok()
        .filter(|&count| count <= bound)
        .ok_or_else(|| format!("too many {what}: {count}"))
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
