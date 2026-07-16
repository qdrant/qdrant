use std::io;
use std::path::Path;

use roaring::RoaringBitmap;

use super::format::{BitmaskHeader, Encoding, HEADER_SIZE, MAGIC, VERSION};
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
    if logical_len > u64::from(u32::MAX) + 1 {
        return Err(UniversalIoError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bitmask of {logical_len} bits exceeds the u32 position space"),
        )));
    }
    // Hard check, not a debug_assert: the zeros-polarity branch below relies on
    // `logical_len > 0` whenever `ones` is non-empty.
    if let Some(max) = ones.max()
        && u64::from(max) >= logical_len
    {
        return Err(UniversalIoError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("set position {max} beyond bitmask of {logical_len} bits"),
        )));
    }

    // Store whichever polarity is the minority.
    let (encoding, mut bitmap) = if ones.len().saturating_mul(2) <= logical_len {
        (Encoding::RoaringOnes, ones)
    } else {
        let mut zeros = RoaringBitmap::new();
        zeros.insert_range(0..=(logical_len - 1) as u32);
        zeros -= ones;
        (Encoding::RoaringZeros, zeros)
    };
    bitmap.optimize();

    let dense_payload_len = logical_len.div_ceil(u64::from(u8::BITS));
    let roaring_payload_len = bitmap.serialized_size() as u64;

    let mut bytes;
    if roaring_payload_len < dense_payload_len {
        let header = BitmaskHeader {
            magic: MAGIC,
            version: VERSION,
            logical_len,
            encoding: encoding as u32,
            _reserved: 0,
            payload_len: roaring_payload_len,
        };
        bytes = Vec::with_capacity(HEADER_SIZE + roaring_payload_len as usize);
        bytes.extend_from_slice(bytemuck::bytes_of(&header));
        bitmap.serialize_into(&mut bytes)?;
    } else {
        // Even the minority polarity does not compress below raw bits: store
        // them densely, so the compact format is never larger than the mask.
        // Reconstruct the raw bits straight into the output buffer from the
        // minority bitmap and its polarity.
        let payload_len = (logical_len as usize).div_ceil(u64::BITS as usize) * size_of::<u64>();
        let header = BitmaskHeader {
            magic: MAGIC,
            version: VERSION,
            logical_len,
            encoding: Encoding::Dense as u32,
            _reserved: 0,
            payload_len: payload_len as u64,
        };
        bytes = vec![0u8; HEADER_SIZE + payload_len];
        bytes[..HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&header));
        let payload = &mut bytes[HEADER_SIZE..];

        let minority_is_ones = encoding == Encoding::RoaringOnes;
        if !minority_is_ones {
            // Majority is ones: prefill the first `logical_len` bits with
            // ones, leaving any trailing capacity bits clear.
            let full_bytes = (logical_len / u64::from(u8::BITS)) as usize;
            payload[..full_bytes].fill(u8::MAX);
            let trailing_bits = (logical_len % u64::from(u8::BITS)) as u8;
            if trailing_bits > 0 {
                payload[full_bytes] = (1u8 << trailing_bits) - 1;
            }
        }
        for idx in bitmap {
            let byte = &mut payload[(idx / u8::BITS) as usize];
            let bit = 1u8 << (idx % u8::BITS);
            if minority_is_ones {
                *byte |= bit;
            } else {
                *byte &= !bit;
            }
        }
    }

    fs.atomic_save(path, &bytes)
}
