//! The document length sidecar: exact lengths, byte-shuffled, zstd-compressed.
//!
//! A fixed 64-byte [Header], followed by the lengths as its `compression` says:
//!
//! | Field         | Type       | Description                                          |
//! | ------------- | ---------- | ---------------------------------------------------- |
//! | `magic`       | `[u8; 8]`  | `QDRANTDL`.                                          |
//! | `version`     | `u32`      | Format version, 1.                                   |
//! | `compression` | `u32`      | How the lengths follow, see [`Compression`].         |
//! | `count`       | `u64`      | Number of lengths, one per point, live or not.       |
//! | `reserved`    | `[u8; 40]` | Zero when written, ignored when read.                |
//!
//! [`Compression::ShuffledZstd`], the only one so far: `count` exact `u32`
//! lengths, byte-shuffled, as one zstd frame with its checksum.
//!
//! Room to change: a new compression gets a new id and leaves the rest of the
//! format alone, and data a reader may ignore goes in `reserved` without a
//! version bump. Anything an old reader would misread bumps `version`. A reader
//! rejects a version or compression it does not know, and the index is then
//! rebuilt from payload.
//!
//! Byte-shuffling: the first bytes of all lengths, then all second bytes, and
//! so on, so the mostly-zero high bytes compress to almost nothing and zstd's
//! entropy coder sees the low bytes on their own. The same layout as the disk
//! id tracker's compact versions file, without the delta: neighbouring
//! documents' lengths are not correlated, so a delta only adds a bit.
//!
//! The file keeps every length exactly, so the code held in RAM is chosen when
//! the file is read and can change without a rebuild. Nothing reads it in
//! place: [`read_codes`] decodes it once into the codes an index holds.

use std::io::Write as _;
use std::path::Path;

use common::universal_io::UniversalWriteFs;
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::super::doc_len::encode_doc_len;
use crate::common::operation_error::OperationResult;

const MAGIC: [u8; 8] = *b"QDRANTDL";
const FORMAT_VERSION: u32 = 1;
const PLANES_COUNT: usize = size_of::<u32>();

/// How the lengths after the header are stored. Ids are never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum Compression {
    /// Exact `u32` lengths, byte-shuffled, in one zstd frame with a checksum.
    ShuffledZstd = 1,
}

impl Compression {
    fn from_id(id: u32) -> Option<Self> {
        match id {
            1 => Some(Self::ShuffledZstd),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
    compression: U32,
    count: U64,
    reserved: [u8; 40],
}

const _: () = assert!(size_of::<Header>() == 64);

/// Atomically replace the sidecar at `path` with the exact `lens`, one per point.
pub(super) fn save(fs: &impl UniversalWriteFs, path: &Path, lens: &[u32]) -> OperationResult<()> {
    fs.atomic_save(path, &encode(lens)?)?;
    Ok(())
}

fn encode(lens: &[u32]) -> std::io::Result<Vec<u8>> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        compression: U32::new(Compression::ShuffledZstd as u32),
        count: U64::new(lens.len() as u64),
        reserved: [0; 40],
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    let mut plane = Vec::with_capacity(lens.len());
    for byte in 0..PLANES_COUNT {
        plane.clear();
        plane.extend(lens.iter().map(|len| len.to_le_bytes()[byte]));
        encoder.write_all(&plane)?;
    }
    encoder.finish()
}

/// Decode a sidecar into the codes of [`encode_doc_len`], one per point.
///
/// `Err` carries why the bytes are not a sidecar this index can use: another
/// format, another version, another point count, or a truncated, padded or
/// corrupted file. The caller treats every one of them as a missing sidecar.
pub(super) fn read_codes(bytes: &[u8], count: usize) -> Result<Vec<u16>, String> {
    let planes = read_planes(bytes, count)?;
    Ok((0..count)
        .map(|index| encode_doc_len(length_at(&planes, count, index)))
        .collect())
}

/// Decode a sidecar into the exact lengths it holds.
#[cfg(test)]
pub(in super::super) fn read_exact(bytes: &[u8], count: usize) -> Result<Vec<u32>, String> {
    let planes = read_planes(bytes, count)?;
    Ok((0..count)
        .map(|index| length_at(&planes, count, index))
        .collect())
}

fn length_at(planes: &[u8], count: usize, index: usize) -> u32 {
    u32::from_le_bytes(std::array::from_fn(|byte| planes[byte * count + index]))
}

/// Check the header and decompress the planes of exactly `count` lengths.
fn read_planes(bytes: &[u8], count: usize) -> Result<Vec<u8>, String> {
    let (header, payload) = Header::ref_from_prefix(bytes)
        .map_err(|_| format!("{} bytes is shorter than the header", bytes.len()))?;
    if header.magic != MAGIC {
        return Err("not a document length file".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }
    let Some(compression) = Compression::from_id(header.compression.get()) else {
        return Err(format!("unknown compression {}", header.compression.get()));
    };
    if header.count.get() != count as u64 {
        return Err(format!(
            "it covers {} points while the index has {count}",
            header.count.get(),
        ));
    }

    match compression {
        Compression::ShuffledZstd => {
            // Decompressed into exactly the room `count` lengths need, so a
            // frame holding more fails before it can allocate for them.
            let expected = count * PLANES_COUNT;
            let raw = zstd::bulk::decompress(payload, expected).map_err(|err| err.to_string())?;
            if raw.len() != expected {
                return Err(format!(
                    "{} bytes of lengths where {count} points need {expected}",
                    raw.len(),
                ));
            }
            Ok(raw)
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::super::super::doc_len::encode_doc_len;
    use super::*;

    fn round_trip(lens: &[u32]) -> Vec<u8> {
        let bytes = encode(lens).unwrap();
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
    /// and a few book-length documents do not change that.
    #[test]
    fn compresses_below_the_codes() {
        let mut rng = StdRng::seed_from_u64(7);
        let count = 12_800;
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
        for len in [0, 8, size_of::<Header>(), bytes.len() - 1] {
            assert!(read_codes(&bytes[..len], 300).is_err(), "{len} bytes");
        }
        // Padded.
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(read_codes(&padded, 300).is_err());
        // Another version, another compression.
        let mut versioned = bytes.clone();
        versioned[std::mem::offset_of!(Header, version)] = 2;
        assert!(read_codes(&versioned, 300).is_err());
        let mut recompressed = bytes.clone();
        recompressed[std::mem::offset_of!(Header, compression)] = 2;
        assert!(read_codes(&recompressed, 300).is_err());
        // A header count that disagrees with the frame: 300 is 0x12C, so this
        // claims 299, and the frame still holds 300 lengths.
        let mut recounted = bytes.clone();
        recounted[std::mem::offset_of!(Header, count)] = 0x2B;
        assert!(read_codes(&recounted, 300).is_err());
        assert!(read_codes(&recounted, 299).is_err());
        // Reserved bytes are for later additions an old reader may ignore.
        let mut reserved = bytes.clone();
        reserved[std::mem::offset_of!(Header, reserved)] = 0xFF;
        assert_eq!(read_exact(&reserved, 300).unwrap(), lens);
        // A raw `u32` array of the same lengths, as the sidecar of earlier builds was.
        let raw: Vec<u8> = lens.iter().flat_map(|len| len.to_le_bytes()).collect();
        assert!(read_codes(&raw, 300).is_err());
        assert!(read_codes(&raw, raw.len() / size_of::<u16>()).is_err());
        // A flipped bit in the compressed data, caught by the checksum.
        let mut corrupted = bytes;
        let last = corrupted.len() - 5;
        corrupted[last] ^= 1;
        assert!(read_codes(&corrupted, 300).is_err());
    }
}
