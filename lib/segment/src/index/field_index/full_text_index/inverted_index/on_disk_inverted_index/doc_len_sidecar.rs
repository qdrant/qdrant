//! The document length sidecar: exact lengths, bit-packed in blocks of 128.
//!
//! ```text
//! offset 0
//! ┌──────────────────────────────────────────────┐
//! │ DocLenHeader                                 │  16 B
//! │   magic: b"qdln", version: u32, count: u64   │
//! ├──────────────────────────────────────────────┤
//! │ block 0: num_bits: u8                        │  1 B
//! │          lengths 0..128, BitPacker4x         │  16 * num_bits B
//! ├──────────────────────────────────────────────┤
//! │ block 1 …                                    │
//! ├──────────────────────────────────────────────┤
//! │ last block, zero-padded to 128 lengths       │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! The file keeps every length exactly, so the code held in RAM is chosen when
//! the file is read and can change without a rebuild. Each block is as wide as
//! its longest document, so one book-length document widens only its own block:
//! typical corpora take 8 to 14 bits a point, under the 16 of the codes.
//!
//! Nothing reads it in place. [`read_codes`] decodes it once, sequentially, into
//! the codes an index holds.

use std::io::Write;
use std::path::Path;

use bitpacking::BitPacker;
use common::fs::atomic_save;
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::super::doc_len::encode_doc_len;
use crate::common::operation_error::OperationResult;

type BitPackerImpl = bitpacking::BitPacker4x;

const BLOCK_LEN: usize = BitPackerImpl::BLOCK_LEN;

const MAGIC: [u8; 4] = *b"qdln";
const VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned)]
#[repr(C)]
struct DocLenHeader {
    magic: [u8; 4],
    version: U32,
    /// Points covered, live or not: one length per slot of the index.
    count: U64,
}

/// Write the exact `lens`, one per point, to `path`.
pub(super) fn write(path: &Path, lens: &[u32]) -> OperationResult<()> {
    atomic_save(path, |writer| -> OperationResult<()> {
        let header = DocLenHeader {
            magic: MAGIC,
            version: U32::new(VERSION),
            count: U64::new(lens.len() as u64),
        };
        writer.write_all(header.as_bytes())?;

        let packer = BitPackerImpl::new();
        let mut block = [0u32; BLOCK_LEN];
        let mut packed = [0u8; BLOCK_LEN * size_of::<u32>()];
        for chunk in lens.chunks(BLOCK_LEN) {
            block[..chunk.len()].copy_from_slice(chunk);
            block[chunk.len()..].fill(0);
            let num_bits = packer.num_bits(&block);
            let size = packer.compress(&block, &mut packed, num_bits);
            writer.write_all(&[num_bits])?;
            writer.write_all(&packed[..size])?;
        }
        Ok(())
    })
}

/// Decode a sidecar into the codes of [`encode_doc_len`], one per point.
///
/// `Err` carries why the bytes are not a sidecar this index can use: another
/// format, another version, another point count, or a truncated or padded
/// file. The caller treats every one of them as a missing sidecar.
pub(super) fn read_codes(bytes: &[u8], count: usize) -> Result<Vec<u16>, String> {
    let mut codes = Vec::with_capacity(count);
    decode(bytes, count, |lens| {
        codes.extend(lens.iter().copied().map(encode_doc_len));
    })?;
    Ok(codes)
}

/// Decode a sidecar into the exact lengths it holds.
#[cfg(test)]
pub(in super::super) fn read_exact(bytes: &[u8], count: usize) -> Result<Vec<u32>, String> {
    let mut lens = Vec::with_capacity(count);
    decode(bytes, count, |block| lens.extend_from_slice(block))?;
    Ok(lens)
}

/// Hand `f` the exact lengths of every block, in point order, without the
/// padding of the last one.
fn decode(bytes: &[u8], count: usize, mut f: impl FnMut(&[u32])) -> Result<(), String> {
    let (header, mut rest) = DocLenHeader::read_from_prefix(bytes)
        .map_err(|_| format!("{} bytes is shorter than the header", bytes.len()))?;
    if header.magic != MAGIC {
        return Err("not a packed document length file".to_string());
    }
    if header.version.get() != VERSION {
        return Err(format!("unknown version {}", header.version.get()));
    }
    if header.count.get() != count as u64 {
        return Err(format!(
            "it covers {} points while the index has {count}",
            header.count.get(),
        ));
    }

    let packer = BitPackerImpl::new();
    let mut block = [0u32; BLOCK_LEN];
    let mut remaining = count;
    while remaining > 0 {
        let Some((&num_bits, tail)) = rest.split_first() else {
            return Err(format!("truncated, {remaining} lengths missing"));
        };
        if u32::from(num_bits) > u32::BITS {
            return Err(format!("a block claims {num_bits} bits a length"));
        }
        let size = BitPackerImpl::compressed_block_size(num_bits);
        let Some((packed, tail)) = tail.split_at_checked(size) else {
            return Err(format!("truncated, {remaining} lengths missing"));
        };
        packer.decompress(packed, &mut block, num_bits);
        let take = remaining.min(BLOCK_LEN);
        f(&block[..take]);
        remaining -= take;
        rest = tail;
    }
    if !rest.is_empty() {
        return Err(format!("{} bytes past the last block", rest.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::super::super::doc_len::encode_doc_len;
    use super::*;

    fn round_trip(lens: &[u32]) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lens");
        write(&path, lens).unwrap();
        let bytes = fs_err::read(&path).unwrap();
        assert_eq!(
            read_exact(&bytes, lens.len()).unwrap(),
            lens,
            "exact lengths"
        );
        let codes: Vec<u16> = lens.iter().copied().map(encode_doc_len).collect();
        assert_eq!(read_codes(&bytes, lens.len()).unwrap(), codes, "codes");
        bytes
    }

    #[test]
    fn keeps_every_length_exactly() {
        let mut rng = StdRng::seed_from_u64(42);
        for count in [0, 1, 127, 128, 129, 1000] {
            let lens: Vec<u32> = (0..count).map(|_| rng.random_range(0..500)).collect();
            round_trip(&lens);
        }
        round_trip(&[0, 1, 8_192, 8_193, 100_003, 3_000_017, u32::MAX]);
        round_trip(&[0; 300]);
    }

    /// The point of the format: smaller than the codes for ordinary corpora,
    /// and a few long documents widen only their own blocks.
    #[test]
    fn packs_below_the_codes() {
        let mut rng = StdRng::seed_from_u64(7);
        let count = 100 * BLOCK_LEN;
        let mut lens: Vec<u32> = (0..count).map(|_| rng.random_range(20..400)).collect();
        let typical = round_trip(&lens).len();
        assert!(
            typical < count * size_of::<u16>() * 3 / 4,
            "{typical} bytes"
        );

        for at in [5, 2_000, 9_999] {
            lens[at] = 2_000_000;
        }
        let with_books = round_trip(&lens).len();
        assert!(
            with_books < count * size_of::<u16>() * 3 / 4,
            "{with_books} bytes"
        );
    }

    #[test]
    fn rejects_what_it_did_not_write() {
        let lens: Vec<u32> = (0..300).collect();
        let bytes = round_trip(&lens);

        // Another point count.
        assert!(read_codes(&bytes, 299).is_err());
        assert!(read_codes(&bytes, 301).is_err());
        // Truncated anywhere, header included.
        for len in [0, 8, size_of::<DocLenHeader>(), bytes.len() - 1] {
            assert!(read_codes(&bytes[..len], 300).is_err(), "{len} bytes");
        }
        // Padded.
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(read_codes(&padded, 300).is_err());
        // Another version.
        let mut versioned = bytes.clone();
        versioned[4] = 2;
        assert!(read_codes(&versioned, 300).is_err());
        // A raw `u32` array of the same lengths, as the unpacked sidecar was.
        let raw: Vec<u8> = lens.iter().flat_map(|len| len.to_le_bytes()).collect();
        assert!(read_codes(&raw, 300).is_err());
        assert!(read_codes(&raw, raw.len() / size_of::<u16>()).is_err());
        // A block wider than a length.
        let mut wide = bytes;
        wide[size_of::<DocLenHeader>()] = 33;
        assert!(read_codes(&wide, 300).is_err());
    }
}
