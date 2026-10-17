//! Element map of an immutable index: built once, in point order, so the start offsets
//! of the points are enough to map both ways.
//!
//! The start offsets are delta-encoded into element counts, byte-shuffled, and
//! compressed with Zstd. Opening decompresses the whole mapping into RAM.

use std::io::{BufWriter, Write as _};
use std::ops::Range;
use std::path::PathBuf;

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{ElementMapBuilder, ElementOffsetType, ElementUpdate};
use crate::common::operation_error::{OperationError, OperationResult};

/// Start offsets of the points, see [`pack_starts`].
const STARTS_FILE: &str = "starts.zstd";

const MAGIC: [u8; 8] = *b"QDRANTEM";
const FORMAT_VERSION: u32 = 2;
const PLANES_COUNT: usize = size_of::<ElementOffsetType>();

/// Header of [`STARTS_FILE`].
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
    /// Number of start offsets in the compressed data that follows
    len: U32,
    /// Build-time upper bound on the number of points with indexed values
    indexed_points: U32,
    /// Written as zeros and ignored when read, for fields added without a new version
    reserved: [u8; 12],
}

#[derive(Debug)]
pub struct ImmutableElementMap {
    /// Start offset of the elements of each point, followed by the number of elements:
    /// point `p` owns `starts[p]..starts[p + 1]`. Never empty.
    starts: Vec<ElementOffsetType>,
    /// Build-time upper bound on the number of points with indexed values
    built_indexed_points: usize,
    max_elements_per_point: usize,
    path: PathBuf,
}

impl ImmutableElementMap {
    /// Requires `builder` to have assigned the points in ascending order. `indexed_points`
    /// bounds the number of points with indexed values, stored so that opening the map
    /// doesn't need to read the index.
    pub fn create(
        path: PathBuf,
        builder: &ElementMapBuilder,
        indexed_points: usize,
    ) -> OperationResult<Self> {
        let ElementMapBuilder {
            ranges,
            elements_count,
        } = builder;

        let mut starts = Vec::new();
        for &(point, range) in ranges {
            if (point as usize) < starts.len() {
                return Err(OperationError::service_error(format!(
                    "elements of an immutable nested index must be assigned in point order, \
                     got point {point} out of order",
                )));
            }
            // Points without elements own an empty range at the start of the next one
            starts.resize(point as usize + 1, range.start);
        }
        starts.push(*elements_count);

        common::fs::atomic_save(&path.join(STARTS_FILE), |writer| {
            writer.write_all(&pack_starts(&starts, indexed_points)?)
        })?;
        Ok(Self::new(path, starts, indexed_points))
    }

    pub fn open(path: PathBuf) -> OperationResult<Option<Self>> {
        let file = path.join(STARTS_FILE);
        if !file.exists() {
            return Ok(None);
        }
        let (starts, indexed_points) = unpack_starts(&fs_err::read(&file)?).ok_or_else(|| {
            OperationError::service_error(format!("element map {} is invalid", file.display()))
        })?;
        Ok(Some(Self::new(path, starts, indexed_points)))
    }

    fn new(path: PathBuf, starts: Vec<ElementOffsetType>, built_indexed_points: usize) -> Self {
        let max_elements_per_point = starts
            .windows(2)
            .map(|window| (window[1] - window[0]) as usize)
            .max()
            .unwrap_or(0);
        Self {
            starts,
            built_indexed_points,
            max_elements_per_point,
            path,
        }
    }

    /// Build-time upper bound on the number of points with indexed values.
    ///
    /// Deletions are not subtracted: a point can have elements without any indexed
    /// values, so subtracting every deleted point with elements could undercount.
    pub fn indexed_points(&self) -> usize {
        self.built_indexed_points
    }

    pub fn point_elements(&self, point: PointOffsetType) -> Range<ElementOffsetType> {
        let point = point as usize;
        match (self.starts.get(point), self.starts.get(point + 1)) {
            (Some(&start), Some(&end)) => start..end,
            (Some(_) | None, None) | (None, Some(_)) => 0..0,
        }
    }

    pub fn element_point(&self, element: ElementOffsetType) -> Option<PointOffsetType> {
        if element >= self.elements_count_offset() {
            return None;
        }
        // The last point starting at or before `element`: points without elements
        // before it share its start
        let point = self.starts.partition_point(|&start| start <= element) - 1;
        Some(point as PointOffsetType)
    }

    pub fn points_upper_bound(&self) -> usize {
        self.starts.len() - 1
    }

    pub fn elements_count(&self) -> usize {
        self.elements_count_offset() as usize
    }

    fn elements_count_offset(&self) -> ElementOffsetType {
        self.starts.last().copied().unwrap_or(0)
    }

    pub fn max_elements_per_point(&self) -> usize {
        self.max_elements_per_point
    }

    pub fn assign(&self, point: PointOffsetType, count: usize) -> OperationResult<ElementUpdate> {
        let current: Vec<ElementOffsetType> = self.point_elements(point).collect();
        if count == 0 {
            return Ok(ElementUpdate {
                old: current,
                new: Vec::new(),
            });
        }
        if count != current.len() {
            return Err(OperationError::service_error(format!(
                "can't change the number of elements of point {point} in an immutable nested \
                 index from {} to {count}",
                current.len(),
            )));
        }
        Ok(ElementUpdate {
            old: current.clone(),
            new: current,
        })
    }

    pub fn deleted_elements(&self, deleted_points: &BitSlice) -> BitVec {
        let mut deleted = BitVec::repeat(false, self.elements_count());
        for point in deleted_points.iter_ones() {
            let elements = self.point_elements(point as PointOffsetType);
            deleted[elements.start as usize..elements.end as usize].fill(true);
        }
        deleted
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.join(STARTS_FILE)]
    }

    pub fn wipe(self) -> OperationResult<()> {
        if self.path.exists() {
            fs_err::remove_dir_all(&self.path)?;
        }
        Ok(())
    }

    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            starts,
            built_indexed_points: _,
            max_elements_per_point: _,
            path: _,
        } = self;
        starts.capacity() * size_of::<ElementOffsetType>()
    }
}

/// A [`Header`], followed by Zstd-compressed deltas in byte planes: all first
/// bytes, then all second bytes, and so on. Deltas are unsigned element counts.
fn pack_starts(starts: &[ElementOffsetType], indexed_points: usize) -> std::io::Result<Vec<u8>> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        len: U32::new(starts.len() as u32),
        indexed_points: U32::new(indexed_points as u32),
        reserved: [0; 12],
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    let mut writer = BufWriter::new(encoder);
    for byte in 0..PLANES_COUNT {
        let mut previous = 0;
        for &start in starts {
            let delta = start - previous;
            previous = start;
            writer.write_all(&delta.to_le_bytes()[byte..=byte])?;
        }
    }
    writer.into_inner()?.finish()
}

/// Unpack the starts and the indexed points [`pack_starts`] packed, `None` if malformed.
fn unpack_starts(packed: &[u8]) -> Option<(Vec<ElementOffsetType>, usize)> {
    let (header, packed) = Header::ref_from_prefix(packed).ok()?;
    let Header {
        magic,
        version,
        len,
        indexed_points,
        reserved: _,
    } = header;
    let indexed_points = indexed_points.get() as usize;
    if *magic != MAGIC || version.get() != FORMAT_VERSION {
        return None;
    }
    let len = len.get() as usize;
    if len == 0 {
        return None;
    }
    // Bound decompression by the declared size and reject incomplete byte planes.
    let size = len.checked_mul(PLANES_COUNT)?;
    let planes = zstd::bulk::decompress(packed, size).ok()?;
    if planes.len() != size {
        return None;
    }

    let mut starts = Vec::with_capacity(len);
    let mut previous: ElementOffsetType = 0;
    for index in 0..len {
        let bytes = std::array::from_fn(|byte| planes[byte * len + index]);
        previous = previous.checked_add(ElementOffsetType::from_le_bytes(bytes))?;
        starts.push(previous);
    }
    (starts[0] == 0).then_some((starts, indexed_points))
}

const _: () = assert!(size_of::<Header>() == 32);

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    fn starts_of(counts: impl IntoIterator<Item = u32>) -> Vec<ElementOffsetType> {
        let mut starts = vec![0];
        for count in counts {
            starts.push(starts.last().unwrap() + count);
        }
        starts
    }

    #[test]
    fn pack_roundtrip() {
        let mut rng = StdRng::seed_from_u64(42);
        for points in [0, 1, 127, 128, 129, 1000] {
            let starts = starts_of((0..points).map(|_| rng.random_range(0..10)));
            assert_eq!(
                unpack_starts(&pack_starts(&starts, points as usize).unwrap()),
                Some((starts, points as usize)),
            );
        }
    }

    #[test]
    fn pack_roundtrip_large_counts() {
        let mut counts: Vec<u32> = (0..1280).map(|point| point % 4).collect();
        counts[500] = 1_000_000;
        for counts in [counts, vec![0, 0, 0], vec![0, 1, u32::MAX - 1]] {
            let starts = starts_of(counts);
            assert_eq!(
                unpack_starts(&pack_starts(&starts, 0).unwrap()),
                Some((starts, 0)),
            );
        }
    }

    #[test]
    fn malformed_input_is_rejected() {
        let packed = pack_starts(&starts_of(0..300), 300).unwrap();
        assert_eq!(unpack_starts(&[]), None);
        assert_eq!(unpack_starts(&packed[..packed.len() - 1]), None);

        let mut trailing = packed.clone();
        trailing.push(0);
        assert_eq!(unpack_starts(&trailing), None);

        let mut corrupt_checksum = packed.clone();
        *corrupt_checksum.last_mut().unwrap() ^= 1;
        assert_eq!(unpack_starts(&corrupt_checksum), None);

        for len in [0u32, 300, 302] {
            let mut wrong_len = packed.clone();
            wrong_len[12..16].copy_from_slice(&len.to_le_bytes());
            assert_eq!(unpack_starts(&wrong_len), None);
        }

        assert_eq!(unpack_starts(&pack_starts(&[1, 2], 0).unwrap()), None);

        // A valid compressed frame can still contain deltas that overflow offsets.
        let mut overflowing = pack_starts(&[0, u32::MAX, u32::MAX], 0).unwrap();
        let mut planes = zstd::decode_all(&overflowing[size_of::<Header>()..]).unwrap();
        planes[2] = 1;
        overflowing.truncate(size_of::<Header>());
        overflowing.extend(zstd::encode_all(planes.as_slice(), 0).unwrap());
        assert_eq!(unpack_starts(&overflowing), None);

        let mut other_version = packed.clone();
        other_version[8] = 1;
        assert_eq!(unpack_starts(&other_version), None);

        let mut other_magic = packed.clone();
        other_magic[0] = b'X';
        assert_eq!(unpack_starts(&other_magic), None);

        // Reserved bytes are left to later fields
        let mut reserved = packed;
        reserved[size_of::<Header>() - 1] = 1;
        assert!(unpack_starts(&reserved).is_some());
    }
}
