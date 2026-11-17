use std::cmp::Ordering;
use std::ops::Range;
use std::path::Path;

use super::traits::UniversalReadFs;
use super::types::read_whole_via;
use super::{OkNotFound, UioResult};

/// Target size of one logical block of the indexed array, in bytes.
///
/// Chosen to match the `DiskCache` block size and the auto-chunking read size
/// ([`ReadRange::iter_autochunks`]), so locating an element costs one cache
/// block / one remote range read.
///
/// [`ReadRange::iter_autochunks`]: super::ReadRange::iter_autochunks
pub const BLOCK_SIZE_BYTES: usize = 16 * 1024;

const MAGIC: [u8; 8] = *b"QdrntSBI";
const FORMAT_VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;

/// In-RAM sparse index over an on-disk sorted array: the first element of
/// every [`BLOCK_SIZE_BYTES`]-sized block of the array.
///
/// Binary search directly over an unpopulated storage costs `O(log n)` random
/// reads scattered across the whole file — each probe is a page fault (or a
/// remote range read) on high-latency storage. With this index, a lookup is an
/// in-RAM search over the block firsts plus a single contiguous read of one
/// block, which the caller then bisects in RAM.
///
/// The index is persisted as an *optional* sidecar file next to the main
/// array:
/// - old files without a sidecar keep working — [`Self::open`] returns
///   `Ok(None)` and callers fall back to plain binary search over the storage;
/// - old code ignores the sidecar file, so data written by new code stays
///   readable;
/// - any validation failure (unknown version, wrong element size, size
///   mismatch) is treated as "no sidecar", never as a hard error: the main
///   array remains the source of truth and losing the sidecar only loses the
///   acceleration.
///
/// File layout (header fields little-endian, elements native-endian, exactly
/// as they are encoded in the main array):
///
/// ```text
/// offset  size  field
///      0     8  magic b"QdrntSBI"
///      8     4  format version (u32)
///     12     4  element size in bytes (u32)
///     16     4  elements per block (u32)
///     20     4  reserved, zero
///     24     8  number of elements in the indexed array (u64)
///     32     -  first element of each block, `element size` bytes each
/// ```
pub struct SortedBlockIndex<T> {
    /// First element of every `block_entries`-sized block of the main array.
    firsts: Vec<T>,
    /// Number of array elements covered by one entry of `firsts`.
    block_entries: usize,
    /// Length of the indexed array.
    total_len: usize,
}

impl<T: bytemuck::Pod> SortedBlockIndex<T> {
    /// Number of `T` elements per block when building a new index.
    ///
    /// Readers must use the value recorded in the file header instead, so the
    /// constant can change without breaking existing files.
    fn default_block_entries() -> usize {
        (BLOCK_SIZE_BYTES / size_of::<T>()).max(1)
    }

    /// Write a block index for `sorted` to `path`.
    ///
    /// `sorted` must be exactly the array served by the main storage, in the
    /// same order.
    pub fn write(path: &Path, sorted: &[T]) -> std::io::Result<()> {
        let block_entries = Self::default_block_entries();
        let firsts_count = sorted.len().div_ceil(block_entries);

        let mut bytes = Vec::with_capacity(HEADER_SIZE + firsts_count * size_of::<T>());
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(size_of::<T>() as u32).to_le_bytes());
        bytes.extend_from_slice(&(block_entries as u32).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(sorted.len() as u64).to_le_bytes());
        debug_assert_eq!(bytes.len(), HEADER_SIZE);
        for first in sorted.iter().step_by(block_entries) {
            bytes.extend_from_slice(bytemuck::bytes_of(first));
        }

        fs_err::write(path, bytes)
    }

    /// Read the whole block index at `path` into RAM.
    ///
    /// `expected_len` is the element count of the main array the index must
    /// cover; a mismatch means a stale sidecar which must not be trusted to
    /// locate elements.
    ///
    /// Returns `Ok(None)` when the file is absent or fails validation; the
    /// caller is expected to fall back to plain binary search over the main
    /// storage.
    pub fn open<Fs: UniversalReadFs>(
        fs: &Fs,
        path: &Path,
        expected_len: usize,
    ) -> UioResult<Option<Self>> {
        let parsed =
            read_whole_via(fs, path, |bytes| Ok(Self::parse(&bytes, path))).ok_not_found()?;
        let Some(index) = parsed.flatten() else {
            return Ok(None);
        };
        if index.total_len != expected_len {
            log::warn!(
                "Ignoring block index {path}: it covers {} elements while the indexed array has \
                 {expected_len}, falling back to plain binary search",
                index.total_len,
                path = path.display(),
            );
            return Ok(None);
        }
        Ok(Some(index))
    }

    fn parse(bytes: &[u8], path: &Path) -> Option<Self> {
        let warn = |reason: &str| {
            log::warn!(
                "Ignoring block index {path}: {reason}, falling back to plain binary search",
                path = path.display(),
            );
        };

        let Some(header) = bytes.get(..HEADER_SIZE) else {
            warn("file is too short");
            return None;
        };
        if header[0..8] != MAGIC {
            warn("wrong magic");
            return None;
        }
        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            warn("unsupported format version");
            return None;
        }
        let entry_size = u32::from_le_bytes(header[12..16].try_into().unwrap());
        if entry_size as usize != size_of::<T>() {
            warn("element size mismatch");
            return None;
        }
        let block_entries = u32::from_le_bytes(header[16..20].try_into().unwrap());
        if block_entries == 0 {
            warn("zero elements per block");
            return None;
        }
        let total_len = u64::from_le_bytes(header[24..32].try_into().unwrap());
        let firsts_count = total_len.div_ceil(u64::from(block_entries));
        let firsts_bytes = &bytes[HEADER_SIZE..];
        if firsts_bytes.len() as u64 != firsts_count * u64::from(entry_size) {
            warn("element count does not match file size");
            return None;
        }

        Some(Self {
            firsts: bytemuck::pod_collect_to_vec(firsts_bytes),
            block_entries: block_entries as usize,
            total_len: total_len as usize,
        })
    }

    /// Element index range of the single block that may contain the target.
    ///
    /// `cmp` compares a stored element against the target (like the closure of
    /// [`slice::binary_search_by`]). Both an exact match and the insertion
    /// position of a missing target are guaranteed to lie within the returned
    /// range, so bisecting only this range gives the same result as bisecting
    /// the whole array. Requires a strict order: no two array elements may
    /// compare `Equal` under `cmp`, otherwise matches in earlier blocks are
    /// missed.
    pub fn find_block(&self, cmp: impl Fn(&T) -> Ordering) -> Range<usize> {
        let total_len = self.total_len;
        // First block whose first element is > target, minus one: the target —
        // or its insertion position — can only be in that block. Insertion
        // positions may also fall on the block-end boundary, which is covered
        // because `end` here is exclusive while `binary_search` may return it
        // as `Err(end)`.
        let block = self
            .firsts
            .partition_point(|first| cmp(first).is_le())
            .saturating_sub(1);
        let start = (block * self.block_entries).min(total_len);
        let end = (start + self.block_entries).min(total_len);
        start..end
    }

    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            firsts,
            block_entries: _,
            total_len: _,
        } = self;
        size_of::<Self>() + firsts.capacity() * size_of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::super::MmapFs;
    use super::*;

    /// 16-byte Pod element, 1024 per block.
    #[derive(Copy, Clone, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
    #[repr(C)]
    struct Pair {
        val: u64,
        idx: u64,
    }

    fn pairs(count: usize) -> Vec<Pair> {
        // Even values only, so odd targets test insertion positions.
        (0..count)
            .map(|i| Pair {
                val: 2 * i as u64,
                idx: i as u64,
            })
            .collect()
    }

    fn build_and_open(data: &[Pair]) -> SortedBlockIndex<Pair> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("block_index.bin");
        SortedBlockIndex::write(&path, data).unwrap();

        // A stale index (wrong array length) must be rejected.
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, data.len() + 1)
                .unwrap()
                .is_none()
        );

        SortedBlockIndex::open(&MmapFs, &path, data.len())
            .unwrap()
            .unwrap()
    }

    /// For every target, bisecting only `find_block`'s range must equal
    /// bisecting the whole array.
    fn assert_equivalent(index: &SortedBlockIndex<Pair>, data: &[Pair], target: u64) {
        let range = index.find_block(|pair| pair.val.cmp(&target));
        let expected = data.binary_search_by(|pair| pair.val.cmp(&target));
        let actual = data[range.clone()]
            .binary_search_by(|pair| pair.val.cmp(&target))
            .map(|i| i + range.start)
            .map_err(|i| i + range.start);
        assert_eq!(actual, expected, "target {target}, range {range:?}");
    }

    #[test]
    fn test_roundtrip_multiple_blocks() {
        // 2.5 blocks of 1024 entries.
        let data = pairs(2560);
        let index = build_and_open(&data);

        assert_eq!(index.firsts.len(), 3);

        for target in 0..=(2 * data.len() as u64 + 1) {
            assert_equivalent(&index, &data, target);
        }
    }

    #[test]
    fn test_single_partial_block() {
        let data = pairs(7);
        let index = build_and_open(&data);
        for target in 0..16 {
            assert_equivalent(&index, &data, target);
        }
    }

    #[test]
    fn test_empty() {
        let data = pairs(0);
        let index = build_and_open(&data);
        assert_eq!(index.find_block(|pair| pair.val.cmp(&42)), 0..0);
    }

    #[test]
    fn test_absent_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.bin");
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_invalid_files_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("block_index.bin");
        let data = pairs(100);
        SortedBlockIndex::write(&path, &data).unwrap();
        let valid = fs_err::read(&path).unwrap();

        // Too short
        fs_err::write(&path, &valid[..HEADER_SIZE - 1]).unwrap();
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );

        // Wrong magic
        let mut corrupted = valid.clone();
        corrupted[0] ^= 0xff;
        fs_err::write(&path, &corrupted).unwrap();
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );

        // Unknown (future) version
        let mut corrupted = valid.clone();
        corrupted[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        fs_err::write(&path, &corrupted).unwrap();
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );

        // Element size of a different type
        assert!(
            SortedBlockIndex::<u64>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );

        // Truncated firsts section
        fs_err::write(&path, &valid[..valid.len() - 1]).unwrap();
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_none()
        );

        // Intact file still opens
        fs_err::write(&path, &valid).unwrap();
        assert!(
            SortedBlockIndex::<Pair>::open(&MmapFs, &path, 100)
                .unwrap()
                .is_some()
        );
    }
}
