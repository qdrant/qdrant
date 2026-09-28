//! Compact versions file of the disk-resident id tracker, written in place of
//! the flat `id_tracker.versions` in serverless-compatible deployments.
//!
//! The file is read into RAM whole on open and replaced whole when it changes:
//! it needs no in-place writes, and a reader fetches it in one request instead
//! of one per looked-up point.
//!
//! [Header], followed by zstd-compressed data:
//!
//! | Field    | Type             | Description                                         |
//! | -------- | ---------------- | --------------------------------------------------- |
//! | `count`  | `u64`            | Number of versions, one per internal offset.        |
//! | `deltas` | `u64` × `count`  | Zigzag delta from the previous version, byte-shuffled. |
//!
//! Byte-shuffling: the first bytes of all values, then all second bytes, and
//! so on, so the mostly-zero high bytes compress to almost nothing.

use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use common::universal_io::{
    OkNotFound, OpenOptions, UniversalRead, UniversalReadFs, UniversalWriteFs,
};
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::types::SeqNumberType;

pub const COMPACT_VERSIONS_FILE_NAME: &str = "id_tracker.compact_versions";

const MAGIC: [u8; 8] = *b"QDRANTCV";
const FORMAT_VERSION: u32 = 1;
const PLANES_COUNT: usize = size_of::<SeqNumberType>();

pub fn compact_versions_path(segment_path: &Path) -> PathBuf {
    segment_path.join(COMPACT_VERSIONS_FILE_NAME)
}

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
}

fn zigzag(delta: i64) -> u64 {
    ((delta << 1) ^ (delta >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    (value >> 1) as i64 ^ -((value & 1) as i64)
}

fn encode(versions: &CompressedVersions) -> std::io::Result<Vec<u8>> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    let mut writer = BufWriter::new(encoder);
    writer.write_all(U64::new(versions.len() as u64).as_bytes())?;

    let mut previous: SeqNumberType = 0;
    let deltas: Vec<u64> = versions
        .iter()
        .map(|(_offset, version)| {
            let delta = zigzag(version.wrapping_sub(previous) as i64);
            previous = version;
            delta
        })
        .collect();
    for byte in 0..PLANES_COUNT {
        for delta in &deltas {
            writer.write_all(&delta.to_le_bytes()[byte..=byte])?;
        }
    }
    writer.into_inner()?.finish()
}

fn decode(bytes: &[u8]) -> Result<Vec<SeqNumberType>, String> {
    let (header, payload) =
        Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;
    if header.magic != MAGIC {
        return Err("unexpected magic".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }

    let raw = zstd::decode_all(payload).map_err(|err| err.to_string())?;
    let (count, planes) = U64::read_from_prefix(&raw).map_err(|_| "count is truncated")?;
    let count = count.get() as usize;
    if Some(planes.len()) != count.checked_mul(PLANES_COUNT) {
        return Err("version bytes do not match the count".to_string());
    }

    let mut previous: SeqNumberType = 0;
    Ok((0..count)
        .map(|index| {
            let bytes: [u8; PLANES_COUNT] =
                std::array::from_fn(|byte| planes[byte * count + index]);
            previous = previous.wrapping_add(unzigzag(u64::from_le_bytes(bytes)) as u64);
            previous
        })
        .collect())
}

/// Atomically replace the compact versions file of the segment at `segment_path`.
pub fn save(
    fs: &impl UniversalWriteFs,
    segment_path: &Path,
    versions: &CompressedVersions,
) -> OperationResult<()> {
    fs.atomic_save(&compact_versions_path(segment_path), &encode(versions)?)?;
    Ok(())
}

/// Read the compact versions file of the segment at `segment_path` whole, or
/// `None` when the segment keeps its versions in the flat file.
pub fn load<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    segment_path: &Path,
    options: OpenOptions,
) -> OperationResult<Option<CompressedVersions>> {
    let path = compact_versions_path(segment_path);
    let Some(file) = fs.open(&path, options, Default::default()).ok_not_found()? else {
        return Ok(None);
    };
    let versions = decode(&file.read_whole::<u8>()?).map_err(|err| {
        OperationError::inconsistent_storage(format!("{}: {err}", path.display()))
    })?;
    Ok(Some(CompressedVersions::from_slice(&versions)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let versions = [0, 5, 3, u64::MAX, 0, 1 << 40, 7, 7, 7];
        let encoded = encode(&CompressedVersions::from_slice(&versions)).unwrap();
        assert_eq!(decode(&encoded).unwrap(), versions);

        let encoded = encode(&CompressedVersions::from_slice(&[])).unwrap();
        assert!(decode(&encoded).unwrap().is_empty());
    }

    #[test]
    fn test_rejects_corruption() {
        let encoded = encode(&CompressedVersions::from_slice(&[1, 2, 3])).unwrap();
        assert!(decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode(&encoded[..4]).is_err());
    }
}
