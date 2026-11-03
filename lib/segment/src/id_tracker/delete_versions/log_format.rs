//! Layout of the delete log: a header, then one frame per append. A frame is a length and a
//! CRC32C of its payload, as in the Gridstore tracker journal, then the entries of that append.
//!
//! Frames are only appended. A crash on a local backend can leave an incomplete header or frame
//! at the end; readers stop at it, and the next writer cuts it off.

use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::DeleteLogEntry;
use crate::common::operation_error::{OperationError, OperationResult};

const MAGIC: [u8; 8] = *b"QDRANTDL";
const FORMAT_VERSION: u32 = 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    format: U32,
}

/// Length of the header, the offset of the first frame.
pub(super) const HEADER_LEN: usize = size_of::<Header>();

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FrameHeader {
    /// Length of the payload.
    length: U32,
    /// CRC32C of the payload.
    crc: U32,
}

/// One deleted slot, 12 bytes.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Entry {
    slot: U32,
    version: U64,
}

/// Append the header to `buffer`.
pub(super) fn encode_header(buffer: &mut Vec<u8>) {
    let header = Header {
        magic: MAGIC,
        format: U32::new(FORMAT_VERSION),
    };
    buffer.extend_from_slice(header.as_bytes());
}

/// Check the header at the start of `bytes`: `false` while it is incomplete, which only a write
/// in progress or a torn one leaves. A complete header of another file or format is an error.
pub(super) fn has_header(bytes: &[u8]) -> OperationResult<bool> {
    let Ok((header, _)) = Header::ref_from_prefix(bytes) else {
        return Ok(false);
    };
    if header.magic != MAGIC {
        return Err(OperationError::service_error(
            "delete log does not start with its magic bytes",
        ));
    }
    if header.format.get() != FORMAT_VERSION {
        return Err(OperationError::service_error(format!(
            "delete log has format version {}, only {FORMAT_VERSION} is supported",
            header.format.get(),
        )));
    }
    Ok(true)
}

/// Encode `entries` as one frame, appending it to `buffer`. Nothing is appended for no entries.
pub(super) fn encode_frame(entries: &[DeleteLogEntry], buffer: &mut Vec<u8>) {
    if entries.is_empty() {
        return;
    }

    let start = buffer.len();
    buffer.extend_from_slice(FrameHeader::new_zeroed().as_bytes());

    let payload = buffer.len();
    for &DeleteLogEntry { slot, version } in entries {
        let entry = Entry {
            slot: U32::new(slot),
            version: U64::new(version),
        };
        buffer.extend_from_slice(entry.as_bytes());
    }

    // The checksum covers the payload, filled in once it is written
    let header = FrameHeader {
        length: U32::new((buffer.len() - payload) as u32),
        crc: U32::new(crc32c::crc32c(&buffer[payload..])),
    };
    buffer[start..payload].copy_from_slice(header.as_bytes());
}

/// Decode the frame at the start of `bytes`, appending its entries to `entries`, and return the
/// number of bytes it spans. `None` if it is incomplete or invalid, leaving `entries` as it was.
fn decode_frame(bytes: &[u8], entries: &mut Vec<DeleteLogEntry>) -> Option<usize> {
    let (header, rest) = FrameHeader::ref_from_prefix(bytes).ok()?;
    let payload = rest.get(..header.length.get() as usize)?;
    if crc32c::crc32c(payload) != header.crc.get() {
        return None;
    }
    let frame = <[Entry]>::ref_from_bytes(payload).ok()?;
    // Frames always carry an entry, so a zeroed tail never reads as one
    if frame.is_empty() {
        return None;
    }

    entries.extend(frame.iter().map(|entry| DeleteLogEntry {
        slot: entry.slot.get(),
        version: entry.version.get(),
    }));
    Some(size_of::<FrameHeader>() + payload.len())
}

/// Decode frames from the start of `bytes`, which follow the header, stopping at the first
/// incomplete or invalid one. Returns their entries and the number of bytes the frames span.
pub(super) fn decode_frames(bytes: &[u8]) -> (Vec<DeleteLogEntry>, usize) {
    let mut entries = Vec::new();
    let mut consumed = 0;
    while let Some(length) = decode_frame(&bytes[consumed..], &mut entries) {
        consumed += length;
    }
    (entries, consumed)
}
