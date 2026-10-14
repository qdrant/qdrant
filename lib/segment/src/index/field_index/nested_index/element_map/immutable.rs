//! Element map of an immutable index: built once, in point order, so the start offsets
//! of the points are enough to map both ways.
//!
//! The start offsets are stored bit-packed in blocks of [`BitPacker4x::BLOCK_LEN`]: each
//! block keeps the deltas between consecutive starts, which are the element counts of
//! the points, with as many bits as its largest count needs.

use std::io::Write as _;
use std::ops::Range;
use std::path::PathBuf;

use bitpacking::{BitPacker as _, BitPacker4x};
use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{ElementMapBuilder, ElementOffsetType, ElementUpdate};
use crate::common::operation_error::{OperationError, OperationResult};

/// Start offsets of the points, see [`pack_starts`].
const STARTS_FILE: &str = "starts.bitpacked";

const MAGIC: [u8; 8] = *b"QDRANTEM";
const FORMAT_VERSION: u32 = 1;

/// Header of [`STARTS_FILE`].
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
    /// Number of start offsets in the blocks that follow
    len: U32,
    /// Number of points with a value in the index when it was built
    indexed_points: U32,
    /// Written as zeros and ignored when read, for fields added without a new version
    reserved: [u8; 12],
}

#[derive(Debug)]
pub struct ImmutableElementMap {
    /// Start offset of the elements of each point, followed by the number of elements:
    /// point `p` owns `starts[p]..starts[p + 1]`. Never empty.
    starts: Vec<ElementOffsetType>,
    /// Number of points with a value in the index when it was built
    built_indexed_points: usize,
    max_elements_per_point: usize,
    path: PathBuf,
}

impl ImmutableElementMap {
    /// Requires `builder` to have assigned the points in ascending order. `indexed_points`
    /// is the number of points with a value in the index, stored so that opening the map
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
            writer.write_all(&pack_starts(&starts, indexed_points))
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

    /// Number of points with a value in the index, given the `deleted_points`.
    ///
    /// Approximate: every deleted point with elements is taken off the count stored at
    /// build, also one whose elements had no value. Points deleted before the build have
    /// no elements and were never counted.
    pub fn indexed_points(&self, deleted_points: &BitSlice) -> usize {
        let deleted_with_elements = deleted_points
            .iter_ones()
            .filter(|&point| !self.point_elements(point as PointOffsetType).is_empty())
            .count();
        self.built_indexed_points
            .saturating_sub(deleted_with_elements)
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

/// Pack the sorted `starts`: a [`Header`], then one block per
/// [`BitPacker4x::BLOCK_LEN`] values, each a byte with its bit width followed by its
/// deltas. The last block is padded by repeating the last value.
fn pack_starts(starts: &[ElementOffsetType], indexed_points: usize) -> Vec<u8> {
    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        len: U32::new(starts.len() as u32),
        indexed_points: U32::new(indexed_points as u32),
        reserved: [0; 12],
    };
    let bitpacker = BitPacker4x::new();
    let mut packed = header.as_bytes().to_vec();

    let mut block = [0; BitPacker4x::BLOCK_LEN];
    let mut initial = 0;
    for chunk in starts.chunks(BitPacker4x::BLOCK_LEN) {
        block[..chunk.len()].copy_from_slice(chunk);
        let last = chunk.last().copied().unwrap_or(initial);
        block[chunk.len()..].fill(last);

        let num_bits = bitpacker.num_bits_sorted(initial, &block);
        packed.push(num_bits);
        let offset = packed.len();
        packed.resize(offset + BitPacker4x::compressed_block_size(num_bits), 0);
        bitpacker.compress_sorted(initial, &block, &mut packed[offset..], num_bits);
        initial = last;
    }
    packed
}

/// Unpack the starts and the indexed points [`pack_starts`] packed, `None` if malformed.
fn unpack_starts(packed: &[u8]) -> Option<(Vec<ElementOffsetType>, usize)> {
    let (header, mut packed) = Header::ref_from_prefix(packed).ok()?;
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

    let bitpacker = BitPacker4x::new();
    let mut starts = Vec::with_capacity(len.next_multiple_of(BitPacker4x::BLOCK_LEN));
    let mut block = [0; BitPacker4x::BLOCK_LEN];
    let mut initial = 0;
    while starts.len() < len {
        let (&num_bits, rest) = packed.split_first()?;
        if u32::from(num_bits) > ElementOffsetType::BITS {
            return None;
        }
        let (compressed, rest) =
            rest.split_at_checked(BitPacker4x::compressed_block_size(num_bits))?;
        bitpacker.decompress_sorted(initial, compressed, &mut block, num_bits);
        starts.extend_from_slice(&block);
        initial = block[BitPacker4x::BLOCK_LEN - 1];
        packed = rest;
    }
    starts.truncate(len);

    (!starts.is_empty() && packed.is_empty() && starts.is_sorted())
        .then_some((starts, indexed_points))
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
                unpack_starts(&pack_starts(&starts, points as usize)),
                Some((starts, points as usize)),
            );
        }
    }

    /// A point with many elements widens its own block only.
    #[test]
    fn large_point_widens_its_block() {
        let small = starts_of((0..1280).map(|point| point % 4));
        let mut counts: Vec<u32> = (0..1280).map(|point| point % 4).collect();
        counts[500] = 1000;
        let with_large = starts_of(counts);

        let small_size = pack_starts(&small, 0).len();
        let large_size = pack_starts(&with_large, 0).len();
        // From 2 to 10 bits for one block of 128 values
        assert_eq!(large_size - small_size, 128 * 8 / 8);
        assert_eq!(
            unpack_starts(&pack_starts(&with_large, 1280)),
            Some((with_large, 1280)),
        );
    }

    #[test]
    fn malformed_input_is_rejected() {
        let packed = pack_starts(&starts_of(0..300), 300);
        assert_eq!(unpack_starts(&[]), None);
        assert_eq!(unpack_starts(&packed[..packed.len() - 1]), None);

        let mut trailing = packed.clone();
        trailing.push(0);
        assert_eq!(unpack_starts(&trailing), None);

        let mut too_wide = packed.clone();
        too_wide[size_of::<Header>()] = 33;
        assert_eq!(unpack_starts(&too_wide), None);

        let mut other_version = packed.clone();
        other_version[8] = 2;
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
