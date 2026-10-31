//! Framing of move log entries, the same as the Gridstore tracker journal's: a length and a CRC32C
//! of the payload in front of every entry.
//!
//! Entries are only appended. A crash on a local backend can leave an incomplete entry at the end;
//! readers stop at the first incomplete or invalid entry, and the next writer cuts it off.

use uuid::Uuid;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use super::{MoveEntry, MoveKind};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct EntryHeader {
    /// Length of the payload.
    length: U32,
    /// CRC32C of the payload.
    crc: U32,
}

#[derive(TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Payload {
    header: PayloadHeader,
    pairs: [Pair],
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct PayloadHeader {
    kind: MoveKind,
    peer: uuid::Bytes,
}

/// A slot in the segment that owns the log, and a slot in the peer segment.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Pair {
    local: U32,
    peer: U32,
}

/// Encode `entry`, appending it to `buffer`.
pub(super) fn encode_entry(entry: &MoveEntry, buffer: &mut Vec<u8>) {
    let start = buffer.len();
    buffer.extend_from_slice(EntryHeader::new_zeroed().as_bytes());

    let payload = buffer.len();
    let header = PayloadHeader {
        kind: entry.kind,
        peer: entry.peer.into_bytes(),
    };
    buffer.extend_from_slice(header.as_bytes());
    for &(local, peer) in &entry.pairs {
        let pair = Pair {
            local: U32::new(local),
            peer: U32::new(peer),
        };
        buffer.extend_from_slice(pair.as_bytes());
    }

    // The checksum covers the payload, filled in once it is written
    let header = EntryHeader {
        length: U32::new((buffer.len() - payload) as u32),
        crc: U32::new(crc32c::crc32c(&buffer[payload..])),
    };
    buffer[start..payload].copy_from_slice(header.as_bytes());
}

/// Decode the entry at the start of `bytes`, with the number of bytes it spans. `None` if it is
/// incomplete or invalid.
pub(super) fn decode_entry(bytes: &[u8]) -> Option<(MoveEntry, usize)> {
    let (header, rest) = EntryHeader::ref_from_prefix(bytes).ok()?;
    let payload = rest.get(..header.length.get() as usize)?;
    if crc32c::crc32c(payload) != header.crc.get() {
        return None;
    }
    let Payload { header, pairs } = Payload::try_ref_from_bytes(payload).ok()?;
    // Entries always carry a pair, so a zeroed tail never reads as one
    if pairs.is_empty() {
        return None;
    }

    let entry = MoveEntry {
        kind: header.kind,
        peer: Uuid::from_bytes(header.peer),
        pairs: pairs
            .iter()
            .map(|pair| (pair.local.get(), pair.peer.get()))
            .collect(),
    };
    Some((entry, size_of::<EntryHeader>() + payload.len()))
}

/// Decode entries from the start of `bytes`, stopping at the first incomplete or invalid one.
/// Returns the entries and the number of bytes they span.
pub(super) fn decode_entries(bytes: &[u8]) -> (Vec<MoveEntry>, usize) {
    let mut entries = Vec::new();
    let mut consumed = 0;
    while let Some((entry, length)) = decode_entry(&bytes[consumed..]) {
        entries.push(entry);
        consumed += length;
    }
    (entries, consumed)
}
