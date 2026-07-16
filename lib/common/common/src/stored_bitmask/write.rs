use std::io;
use std::path::Path;

use roaring::RoaringBitmap;

use super::format::{BitmaskHeader, Encoding, HEADER_SIZE};
use crate::universal_io::{Result, UniversalIoError, UniversalWriteFileOps};

/// Atomically persist a bitmask of `logical_len` bits whose set positions are
/// `ones`.
///
/// Picks the cheapest encoding: a roaring bitmap of the minority bit value,
/// or raw dense bits when the roaring payload would not be smaller.
pub fn save_bitmask(
    fs: &impl UniversalWriteFileOps,
    path: &Path,
    logical_len: u64,
    ones: RoaringBitmap,
) -> Result<()> {
    validate(logical_len, &ones)?;

    let (polarity, mut minority) = minority_polarity(logical_len, ones);
    minority.optimize();

    let dense_payload_len = logical_len.div_ceil(u64::from(u8::BITS));
    let bytes = if (minority.serialized_size() as u64) < dense_payload_len {
        roaring_file_bytes(logical_len, polarity, &minority)?
    } else {
        // Even the minority polarity does not compress below raw bits: store
        // them densely, so the compact format is never larger than the mask.
        dense_file_bytes(logical_len, polarity, &minority)
    };

    fs.atomic_save(path, &bytes)
}

fn validate(logical_len: u64, ones: &RoaringBitmap) -> Result<()> {
    let invalid = |message: String| {
        UniversalIoError::Io(io::Error::new(io::ErrorKind::InvalidInput, message))
    };

    if logical_len > u64::from(u32::MAX) + 1 {
        return Err(invalid(format!(
            "bitmask of {logical_len} bits exceeds the u32 position space"
        )));
    }
    // Hard check, not a debug_assert: `minority_polarity` relies on
    // `logical_len > 0` whenever `ones` is non-empty.
    if let Some(max) = ones.max()
        && u64::from(max) >= logical_len
    {
        return Err(invalid(format!(
            "set position {max} beyond bitmask of {logical_len} bits"
        )));
    }
    Ok(())
}

/// Reduce to whichever bit value is the minority: the set positions as-is, or
/// their complement within `0..logical_len`.
fn minority_polarity(logical_len: u64, ones: RoaringBitmap) -> (Encoding, RoaringBitmap) {
    if ones.len().saturating_mul(2) <= logical_len {
        (Encoding::RoaringOnes, ones)
    } else {
        let mut zeros = RoaringBitmap::new();
        zeros.insert_range(0..=(logical_len - 1) as u32);
        zeros -= ones;
        (Encoding::RoaringZeros, zeros)
    }
}

/// Header followed by the serialized roaring bitmap of the minority positions.
fn roaring_file_bytes(
    logical_len: u64,
    polarity: Encoding,
    minority: &RoaringBitmap,
) -> Result<Vec<u8>> {
    let payload_len = minority.serialized_size();
    let header = BitmaskHeader::new(logical_len, polarity, payload_len as u64);

    let mut bytes = Vec::with_capacity(HEADER_SIZE + payload_len);
    bytes.extend_from_slice(bytemuck::bytes_of(&header));
    minority.serialize_into(&mut bytes)?;
    Ok(bytes)
}

/// Header followed by raw dense bits, reconstructed from the minority
/// positions: start from all-majority bits and flip the minority in.
fn dense_file_bytes(logical_len: u64, polarity: Encoding, minority: &RoaringBitmap) -> Vec<u8> {
    let payload_len = (logical_len as usize).div_ceil(u64::BITS as usize) * size_of::<u64>();
    let header = BitmaskHeader::new(logical_len, Encoding::Dense, payload_len as u64);

    let mut bytes = vec![0u8; HEADER_SIZE + payload_len];
    bytes[..HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&header));
    let payload = &mut bytes[HEADER_SIZE..];

    let minority_is_ones = polarity == Encoding::RoaringOnes;
    if !minority_is_ones {
        // Majority is ones: prefill the first `logical_len` bits with ones,
        // leaving any trailing capacity bits clear.
        let full_bytes = (logical_len / u64::from(u8::BITS)) as usize;
        payload[..full_bytes].fill(u8::MAX);
        let trailing_bits = (logical_len % u64::from(u8::BITS)) as u8;
        if trailing_bits > 0 {
            payload[full_bytes] = (1u8 << trailing_bits) - 1;
        }
    }

    for idx in minority {
        let byte = &mut payload[(idx / u8::BITS) as usize];
        let bit = 1u8 << (idx % u8::BITS);
        if minority_is_ones {
            *byte |= bit;
        } else {
            *byte &= !bit;
        }
    }

    bytes
}
