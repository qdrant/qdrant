//! Compact, RAM-resident copy of the append-only tracker mappings.
//!
//! The append-only tracker stores 16 bytes per point offset in a flat file, and every lookup
//! is a random read of one entry. On storage where each read is expensive (object stores,
//! remote disk caches), that read costs as much as fetching the value itself. For a storage
//! that is not appended to anymore, it is cheaper to load all mappings once, on open, than to
//! read them one by one at query time.
//!
//! This module defines a sidecar file holding the mappings of a prefix of the tracker in a
//! form that is small on disk and in RAM. The tracker file stays the source of truth and keeps
//! growing; the sidecar only accelerates lookups below the point offset it covers.
//!
//! # Encoding
//!
//! The append-only pages pack values back to back, so the value data of consecutive mappings
//! is contiguous: the value at point offset `i + 1` starts where the value at `i` ends, and a
//! new page starts where the previous one ends. Assigning each value a position in the
//! concatenation of all pages turns the mappings into a non-decreasing sequence of `N + 1`
//! positions, where entry `i` is the start of value `i` and entry `i + 1` its end. A skipped
//! point offset repeats the position of the previous end and is marked absent in a bitmask.
//!
//! The positions are stored with [`bitpacking_ordered`]: delta encoded in fixed-size chunks,
//! bit packed, and randomly accessible without decompressing. The page a position falls into
//! is found with a binary search over the persisted page start positions.
//!
//! ```text
//! ┌────────┬─────────────┬──────────┬───────────────────────┐
//! │ header │ page starts │ presence │  bitpacked positions  │
//! └────────┴─────────────┴──────────┴───────────────────────┘
//! ```
//!
//! The presence bitmask is omitted when every covered point offset has a value.
//!
//! The encoding relies on the packing invariant of the pages. [`CompactOffsets::build`]
//! verifies that every mapping decodes back to exactly the pointer it was built from, and
//! refuses to produce a sidecar otherwise, so a reader never has to trust the invariant.

use std::path::{Path, PathBuf};

use common::bitpacking_ordered;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, OkNotFound as _, OpenOptions, Populate, UniversalRead, UniversalReadFs,
    UniversalWriteFileOps,
};
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::Result;
use crate::tracker::{PageId, PointOffset, ValuePointer};

/// File name of the compact offsets sidecar, next to the append-only tracker file
const FILE_NAME: &str = "log_offsets.dat";

const MAGIC: [u8; 8] = *b"QdrntLCO";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
    /// Number of point offsets covered by the sidecar
    count: U32,
    /// Number of page start positions following the header
    page_count: U32,
    /// Size in bytes of the presence bitmask following the page starts, zero when all covered
    /// point offsets have a value
    presence_bytes: U32,
    /// Parameters of the bitpacked positions following the presence bitmask
    positions: bitpacking_ordered::Parameters,
}

/// Mappings of the first [`count`](Self::count) point offsets of an append-only tracker, held
/// in RAM in compressed form.
#[derive(Debug)]
pub(crate) struct CompactOffsets {
    count: PointOffset,
    /// Position of the first byte of each page in the concatenation of all pages
    page_starts: Vec<u64>,
    /// Bit `i` is set when point offset `i` has a value, `None` when all do
    presence: Option<Vec<u8>>,
    reader: bitpacking_ordered::Reader,
    positions: Vec<u8>,
}

impl CompactOffsets {
    pub fn file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    fn open_options() -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: true,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Sequential),
        }
    }

    /// Schedule a prefetch of the sidecar, so a subsequent [`load`](Self::load) is served from
    /// the prefetch pool. Harmless when there is no sidecar.
    pub fn preopen<Fs: CachedReadFs>(fs: &Fs, dir: &Path) {
        fs.schedule_open(&Self::file_name(dir), Some(Self::open_options()), None);
    }

    /// Encode the given mappings, where entry `i` is the mapping for point offset `i`.
    ///
    /// Returns `None` when there is nothing to cover, or when the mappings do not follow the
    /// packing invariant of the append-only pages and can therefore not be represented.
    pub fn build(pointers: &[Option<ValuePointer>]) -> Option<Self> {
        if pointers.is_empty() {
            return None;
        }
        let count = PointOffset::try_from(pointers.len()).ok()?;

        let mut page_starts = vec![0u64];
        let mut positions = Vec::with_capacity(pointers.len() + 1);
        let mut presence = vec![0u8; pointers.len().div_ceil(8)];
        let mut all_present = true;
        let mut end = 0u64;

        for (index, pointer) in pointers.iter().enumerate() {
            let Some(pointer) = pointer else {
                all_present = false;
                positions.push(end);
                continue;
            };

            presence[index / 8] |= 1 << (index % 8);

            let page = pointer.page_id as usize;
            if page + 1 < page_starts.len() {
                return None;
            }
            while page_starts.len() <= page {
                page_starts.push(end);
            }

            let start = page_starts[page] + u64::from(pointer.block_offset);
            if start != end {
                return None;
            }

            positions.push(start);
            end = start + u64::from(pointer.length);
        }
        positions.push(end);

        let (data, parameters) = bitpacking_ordered::compress(&positions);
        let reader = parameters.validate().ok()?;

        let compact = Self {
            count,
            page_starts,
            presence: (!all_present).then_some(presence),
            reader,
            positions: data,
        };

        let roundtrips = pointers
            .iter()
            .enumerate()
            .all(|(index, expected)| compact.get(index as PointOffset) == Some(*expected));
        roundtrips.then_some(compact)
    }

    /// Number of point offsets covered, the exclusive upper bound of what [`get`](Self::get)
    /// can answer.
    pub fn count(&self) -> PointOffset {
        self.count
    }

    /// Mapping at the given point offset.
    ///
    /// The outer `None` means the point offset is not covered and must be looked up in the
    /// tracker file; the inner `None` means it is covered and has no value.
    pub fn get(&self, point_offset: PointOffset) -> Option<Option<ValuePointer>> {
        if point_offset >= self.count {
            return None;
        }
        let index = point_offset as usize;

        if let Some(presence) = &self.presence
            && presence[index / 8] & (1 << (index % 8)) == 0
        {
            return Some(None);
        }

        let (start, end) = self
            .reader
            .slice_reader(&self.positions)
            .expect("positions are validated on construction")
            .read_pair(index)?;

        // The first page starts at position 0, so the search never yields 0
        let page = self
            .page_starts
            .partition_point(|&page_start| page_start <= start)
            - 1;
        let block_offset = u32::try_from(start - self.page_starts[page]).ok()?;
        let length = u32::try_from(end - start).ok()?;

        Some(Some(ValuePointer::new(
            page as PageId,
            block_offset,
            length,
        )))
    }

    fn to_bytes(&self) -> Vec<u8> {
        let presence = self.presence.as_deref().unwrap_or_default();

        let header = Header {
            magic: MAGIC,
            version: U32::new(FORMAT_VERSION),
            count: U32::new(self.count),
            page_count: U32::new(self.page_starts.len() as u32),
            presence_bytes: U32::new(presence.len() as u32),
            positions: self.reader.parameters(),
        };

        let mut bytes = Vec::with_capacity(
            size_of::<Header>()
                + self.page_starts.len() * size_of::<u64>()
                + presence.len()
                + self.positions.len(),
        );
        bytes.extend_from_slice(header.as_bytes());
        for page_start in &self.page_starts {
            bytes.extend_from_slice(&page_start.to_le_bytes());
        }
        bytes.extend_from_slice(presence);
        bytes.extend_from_slice(&self.positions);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, String> {
        let (header, rest) =
            Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;

        if header.magic != MAGIC {
            return Err("unexpected magic".to_string());
        }
        if header.version.get() != FORMAT_VERSION {
            return Err(format!("unsupported version {}", header.version.get()));
        }

        let count = header.count.get();
        let page_count = header.page_count.get() as usize;
        let presence_bytes = header.presence_bytes.get() as usize;

        if page_count == 0 {
            return Err("no page starts".to_string());
        }
        if presence_bytes != 0 && presence_bytes != (count as usize).div_ceil(8) {
            return Err(format!(
                "presence bitmask of {presence_bytes} bytes does not match {count} point offsets"
            ));
        }

        let (page_starts, rest) = <[U64]>::ref_from_prefix_with_elems(rest, page_count)
            .map_err(|_| "file is shorter than the page starts")?;
        let page_starts: Vec<u64> = page_starts.iter().map(|start| start.get()).collect();
        if page_starts[0] != 0 || !page_starts.is_sorted() {
            return Err("page starts must begin at 0 and be sorted".to_string());
        }

        let (presence, positions) = rest
            .split_at_checked(presence_bytes)
            .ok_or("file is shorter than the presence bitmask")?;

        let reader = header
            .positions
            .validate()
            .map_err(|err| format!("invalid positions parameters: {err}"))?;
        if reader.decompressed_len() != count as usize + 1 {
            return Err(format!(
                "{} positions do not match {count} point offsets",
                reader.decompressed_len(),
            ));
        }
        if reader.compressed_size_bytes() != positions.len() {
            return Err(format!(
                "positions of {} bytes do not match the expected {} bytes",
                positions.len(),
                reader.compressed_size_bytes(),
            ));
        }

        Ok(Self {
            count,
            page_starts,
            presence: (presence_bytes != 0).then(|| presence.to_vec()),
            reader,
            positions: positions.to_vec(),
        })
    }

    /// Write the sidecar into `dir`, replacing any previous one.
    pub fn write<Fs: UniversalWriteFileOps>(&self, fs: &Fs, dir: &Path) -> Result<()> {
        fs.atomic_save(&Self::file_name(dir), &self.to_bytes())?;
        Ok(())
    }

    /// Remove the sidecar from `dir`, if there is one.
    pub fn remove<Fs: UniversalWriteFileOps>(fs: &Fs, dir: &Path) -> Result<()> {
        let path = Self::file_name(dir);
        if fs.exists(&path)? {
            fs.remove(&path)?;
        }
        Ok(())
    }

    /// Load the sidecar from `dir`, if there is one, with a single sequential read of the
    /// whole file.
    ///
    /// `persisted_count` is the number of mappings in the tracker file. A sidecar covering
    /// more than that cannot belong to this tracker and is ignored, like one that fails
    /// validation: the tracker file remains the source of truth, and losing the sidecar only
    /// loses the acceleration.
    pub fn load<Fs: UniversalReadFs>(
        fs: &Fs,
        dir: &Path,
        persisted_count: PointOffset,
    ) -> Result<Option<Self>> {
        let path = Self::file_name(dir);
        let Some(file) = fs
            .open(&path, Self::open_options(), Default::default())
            .ok_not_found()?
        else {
            return Ok(None);
        };

        let bytes = file.read_whole::<u8>()?;
        let compact = Self::from_bytes(&bytes);
        drop(bytes);
        file.clear_ram_cache()?;

        let compact = match compact {
            Ok(compact) => compact,
            Err(err) => {
                log::warn!(
                    "Ignoring invalid compact offsets file {}: {err}",
                    path.display(),
                );
                return Ok(None);
            }
        };

        if compact.count > persisted_count {
            log::warn!(
                "Ignoring compact offsets file {}: it covers {} point offsets, but the tracker \
                 only holds {persisted_count}",
                path.display(),
                compact.count,
            );
            return Ok(None);
        }

        Ok(Some(compact))
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::MmapFs;
    use tempfile::TempDir;

    use super::*;

    /// Pointers packed back to back across pages of the given capacity, with `None` at the
    /// given point offsets.
    fn packed_pointers(
        lengths: &[u32],
        gaps: &[PointOffset],
        page_capacity: u32,
    ) -> Vec<Option<ValuePointer>> {
        let mut pointers = Vec::new();
        let mut page_id = 0;
        let mut offset = 0;
        for &length in lengths {
            while gaps.contains(&(pointers.len() as PointOffset)) {
                pointers.push(None);
            }
            if offset > 0 && offset + length > page_capacity {
                page_id += 1;
                offset = 0;
            }
            pointers.push(Some(ValuePointer::new(page_id, offset, length)));
            offset += length;
        }
        pointers
    }

    fn assert_roundtrip(pointers: &[Option<ValuePointer>]) -> CompactOffsets {
        let compact = CompactOffsets::build(pointers).expect("encodable");
        let reloaded = CompactOffsets::from_bytes(&compact.to_bytes()).unwrap();
        for c in [&compact, &reloaded] {
            assert_eq!(c.count() as usize, pointers.len());
            for (index, expected) in pointers.iter().enumerate() {
                assert_eq!(
                    c.get(index as PointOffset),
                    Some(*expected),
                    "index {index}"
                );
            }
            assert_eq!(c.get(pointers.len() as PointOffset), None);
        }
        reloaded
    }

    #[test]
    fn test_empty_is_not_encoded() {
        assert!(CompactOffsets::build(&[]).is_none());
    }

    #[test]
    fn test_single_page_all_present() {
        let pointers = packed_pointers(&[10, 20, 30, 0, 5], &[], 1000);
        let compact = assert_roundtrip(&pointers);
        assert!(compact.presence.is_none());
        assert_eq!(compact.page_starts, vec![0]);
    }

    #[test]
    fn test_gaps_and_page_rollover() {
        let pointers = packed_pointers(&[40, 40, 40, 100, 40, 40], &[0, 2, 5], 100);
        let compact = assert_roundtrip(&pointers);
        assert!(compact.presence.is_some());
        assert_eq!(compact.page_starts, vec![0, 80, 120, 220]);
    }

    #[test]
    fn test_trailing_gaps() {
        let mut pointers = packed_pointers(&[7, 7], &[], 100);
        pointers.extend([None, None, None]);
        assert_roundtrip(&pointers);
    }

    #[test]
    fn test_all_gaps() {
        assert_roundtrip(&[None, None]);
    }

    #[test]
    fn test_unpacked_pointers_are_rejected() {
        // A hole between two values
        let pointers = [
            Some(ValuePointer::new(0, 0, 10)),
            Some(ValuePointer::new(0, 11, 10)),
        ];
        assert!(CompactOffsets::build(&pointers).is_none());

        // Overlapping values
        let pointers = [
            Some(ValuePointer::new(0, 0, 10)),
            Some(ValuePointer::new(0, 5, 10)),
        ];
        assert!(CompactOffsets::build(&pointers).is_none());

        // A page that does not start at its beginning
        let pointers = [
            Some(ValuePointer::new(0, 0, 10)),
            Some(ValuePointer::new(1, 4, 10)),
        ];
        assert!(CompactOffsets::build(&pointers).is_none());

        // Pages out of order
        let pointers = [
            Some(ValuePointer::new(1, 0, 10)),
            Some(ValuePointer::new(0, 0, 10)),
        ];
        assert!(CompactOffsets::build(&pointers).is_none());
    }

    #[test]
    fn test_zero_length_value_at_page_end_is_rejected() {
        // The empty value at the end of page 0 shares its position with the start of page 1,
        // which the encoding cannot tell apart, so the roundtrip check must refuse it
        let pointers = [
            Some(ValuePointer::new(0, 0, 10)),
            Some(ValuePointer::new(0, 10, 0)),
            Some(ValuePointer::new(1, 0, 10)),
        ];
        assert!(CompactOffsets::build(&pointers).is_none());
    }

    #[test]
    fn test_large_sequence() {
        let rng = &mut rand::make_rng::<rand::rngs::SmallRng>();
        let lengths: Vec<u32> = (0..10_000)
            .map(|_| rand::RngExt::random_range(rng, 1..500))
            .collect();
        let gaps: Vec<PointOffset> = (0..10_000).step_by(97).collect();
        let pointers = packed_pointers(&lengths, &gaps, 64 * 1024);
        let compact = assert_roundtrip(&pointers);

        // Far below the 16 bytes per mapping of the tracker file
        let bytes_per_mapping = compact.to_bytes().len() as f64 / pointers.len() as f64;
        assert!(
            bytes_per_mapping < 4.0,
            "{bytes_per_mapping} bytes per mapping"
        );
    }

    #[test]
    fn test_invalid_bytes_are_rejected() {
        let pointers = packed_pointers(&[10, 20], &[1], 100);
        let compact = CompactOffsets::build(&pointers).unwrap();
        let bytes = compact.to_bytes();

        assert!(CompactOffsets::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        assert!(CompactOffsets::from_bytes(&bytes[..10]).is_err());
        assert!(CompactOffsets::from_bytes(&[]).is_err());

        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xFF;
        assert!(CompactOffsets::from_bytes(&bad_magic).is_err());

        let mut bad_version = bytes.clone();
        bad_version[8] = 0xFF;
        assert!(CompactOffsets::from_bytes(&bad_version).is_err());

        let mut bad_count = bytes.clone();
        bad_count[12] += 1;
        assert!(CompactOffsets::from_bytes(&bad_count).is_err());

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(CompactOffsets::from_bytes(&trailing).is_err());
    }

    #[test]
    fn test_write_load_remove() {
        let dir = TempDir::new().unwrap();
        let pointers = packed_pointers(&[10, 20, 30], &[], 100);
        let compact = CompactOffsets::build(&pointers).unwrap();

        assert!(
            CompactOffsets::load(&MmapFs, dir.path(), 3)
                .unwrap()
                .is_none()
        );

        compact.write(&MmapFs, dir.path()).unwrap();
        let loaded = CompactOffsets::load(&MmapFs, dir.path(), 3)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.count(), 3);
        assert_eq!(loaded.get(2), Some(pointers[2]));

        // A tracker holding fewer mappings than the sidecar covers cannot be the one it was
        // written for
        assert!(
            CompactOffsets::load(&MmapFs, dir.path(), 2)
                .unwrap()
                .is_none()
        );
        // A tracker that grew past the sidecar is still served for the covered prefix
        assert!(
            CompactOffsets::load(&MmapFs, dir.path(), 10)
                .unwrap()
                .is_some()
        );

        // Garbage is ignored, not an error
        fs_err::write(CompactOffsets::file_name(dir.path()), b"garbage").unwrap();
        assert!(
            CompactOffsets::load(&MmapFs, dir.path(), 3)
                .unwrap()
                .is_none()
        );

        CompactOffsets::remove(&MmapFs, dir.path()).unwrap();
        assert!(!CompactOffsets::file_name(dir.path()).exists());
        CompactOffsets::remove(&MmapFs, dir.path()).unwrap();
    }
}
