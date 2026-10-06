//! Framing of move log entries, the same as the Gridstore tracker journal's: a length and a CRC32C
//! of the payload in front of every entry.
//!
//! +-------------+-------------+-----------------------------------------------------------------+
//! | length: u32 | CRC32C: u32 | payload: kind u8, reserved 3 bytes, peer 16 bytes, pairs 8 * n  |
//! +-------------+-------------+-----------------------------------------------------------------+
//!
//! A pair is the slot in the segment that owns the log, then the slot in the peer segment, both
//! `u32`. Everything is little endian, like the other tracker files.
//!
//! Entries are only appended. A crash on a local backend can leave an incomplete entry at the end;
//! readers stop at the first incomplete or invalid entry, and the next writer cuts it off.

use common::types::PointOffsetType;
use uuid::Uuid;

use super::{MoveEntry, MoveKind};

/// Size of the length and checksum in front of each entry.
const ENTRY_HEADER_SIZE: usize = 2 * size_of::<u32>();

/// Size of the kind, reserved bytes and peer segment in front of the pairs.
const PAYLOAD_HEADER_SIZE: usize = size_of::<u8>() + 3 + size_of::<u128>();

/// Size of one `(local slot, peer slot)` pair.
const PAIR_SIZE: usize = 2 * size_of::<PointOffsetType>();

/// Encode `entry`, appending it to `buffer`.
pub(super) fn encode_entry(entry: &MoveEntry, buffer: &mut Vec<u8>) {
    let payload_len = PAYLOAD_HEADER_SIZE + entry.pairs.len() * PAIR_SIZE;
    buffer.reserve(ENTRY_HEADER_SIZE + payload_len);

    let header = buffer.len();
    buffer.extend_from_slice(&(payload_len as u32).to_le_bytes());
    // The checksum covers the payload, filled in once it is written
    buffer.extend_from_slice(&[0; size_of::<u32>()]);

    let payload = buffer.len();
    buffer.push(entry.kind as u8);
    buffer.extend_from_slice(&[0; 3]);
    buffer.extend_from_slice(&entry.peer.to_u128_le().to_le_bytes());
    for &(local, peer) in &entry.pairs {
        buffer.extend_from_slice(&local.to_le_bytes());
        buffer.extend_from_slice(&peer.to_le_bytes());
    }

    let crc = crc32c::crc32c(&buffer[payload..]);
    buffer[header + size_of::<u32>()..payload].copy_from_slice(&crc.to_le_bytes());
}

/// Decode the entry at the start of `bytes`, with the number of bytes it spans. `None` if it is
/// incomplete or invalid.
pub(super) fn decode_entry(bytes: &[u8]) -> Option<(MoveEntry, usize)> {
    let (header, rest) = bytes.split_at_checked(ENTRY_HEADER_SIZE)?;
    let (length, crc) = header.split_at(size_of::<u32>());
    let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(crc.try_into().unwrap());

    // Entries always carry a pair, so a zeroed tail never reads as one
    let is_valid_length =
        length > PAYLOAD_HEADER_SIZE && (length - PAYLOAD_HEADER_SIZE).is_multiple_of(PAIR_SIZE);
    if !is_valid_length {
        return None;
    }
    let payload = rest.get(..length)?;
    if crc32c::crc32c(payload) != crc {
        return None;
    }

    let (payload_header, pairs) = payload.split_at(PAYLOAD_HEADER_SIZE);
    let kind = MoveKind::from_byte(payload_header[0])?;
    let peer = u128::from_le_bytes(payload_header[4..].try_into().unwrap());
    let (pairs, _) = pairs.as_chunks::<PAIR_SIZE>();
    let pairs = pairs
        .iter()
        .map(|pair| {
            let (local, peer) = pair.split_at(size_of::<PointOffsetType>());
            (
                PointOffsetType::from_le_bytes(local.try_into().unwrap()),
                PointOffsetType::from_le_bytes(peer.try_into().unwrap()),
            )
        })
        .collect();

    let entry = MoveEntry {
        kind,
        peer: Uuid::from_u128_le(peer),
        pairs,
    };
    Some((entry, ENTRY_HEADER_SIZE + length))
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
