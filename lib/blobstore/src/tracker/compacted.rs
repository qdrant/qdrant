//! [`CompactedTracker`]: tracker for the append-only mode that keeps all mappings in RAM and
//! persists them as one compact file, rewritten whole on every flush.
//!
//! Intended for storages that are written once and then only read, such as the segments an
//! optimizer builds. A reader decodes the file once on open and serves every lookup from RAM,
//! instead of reading one 16 byte entry per lookup from the flat file of
//! [`AppendOnlyTracker`](super::append_only::AppendOnlyTracker), which is expensive on storage
//! where every read is a round trip.
//!
//! # File format
//!
//! ```text
//! ┌─────────┬─────────────┬───────────┬────────────────────────────────────────────┐
//! │ magic   │ version     │ count     │ LZ4 block with prepended size, holding the │
//! │ 8 bytes │ u32 LE      │ u32 LE    │ delta encoded mappings                     │
//! └─────────┴─────────────┴───────────┴────────────────────────────────────────────┘
//! ```
//!
//! Each mapping is a run of varints. A skipped point offset is the single varint `0`. A present
//! mapping is `length + 1`, followed by the page id delta and the offset delta against the
//! previous present mapping, both zigzag encoded. The offset delta is taken against the end of
//! the previous value on the same page, or against `0` on a new page. Values are packed back to
//! back in the append-only pages, so both deltas are almost always zero, and LZ4 squeezes the
//! runs of zeros. Arbitrary pointers still encode correctly, only less compactly.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::generic_consts::AccessPattern;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    IsNotFound, OpenOptions, Populate, UniversalRead, UniversalReadFs, UniversalWriteFileOps,
    UserData,
};
use integer_encoding::VarInt;
use parking_lot::Mutex;
use zerocopy::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::Result;
use crate::blobstore::Flusher;
use crate::config::compress_lz4;
use crate::error::BlobstoreError;
use crate::tracker::{PointOffset, PointerItem, TrackerRead, ValuePointer};

/// File name of the compacted tracker file
///
/// Deliberately different from the other tracker file names, so that one tracker never attempts
/// to load the incompatible file format of another.
const FILE_NAME: &str = "compacted_tracker.dat";

const MAGIC: [u8; 8] = *b"QDRANTCT";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Header {
    magic: [u8; 8],
    version: U32,
    count: U32,
}

/// Tracker of value pointers for the append-only storage mode, held entirely in RAM.
///
/// Mappings must be set in monotonically increasing point offset order, like in
/// [`AppendOnlyTracker`](super::append_only::AppendOnlyTracker). Skipped point offsets read as
/// `None`.
///
/// Flushing rewrites the whole file from a snapshot of the mappings, see [`Self::flusher`].
/// Opening decodes the whole file once; reads never touch the disk afterwards.
#[derive(Debug)]
pub struct CompactedTracker {
    /// Path to the tracker file
    path: PathBuf,
    /// Entry `i` is the mapping for point offset `i`
    pointers: Vec<Option<ValuePointer>>,
    /// Number of mappings the file holds, held while the file is being written.
    ///
    /// Shared with the flushers, so that a stale flush never replaces a newer file.
    persisted_count: Arc<Mutex<PointOffset>>,
}

impl CompactedTracker {
    fn tracker_file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// Create a new empty tracker in the given directory, replacing the file if it already
    /// exists.
    ///
    /// The directory must exist already.
    pub fn new<Fs: UniversalWriteFileOps>(fs: &Fs, dir: &Path) -> Result<Self> {
        let tracker = Self {
            path: Self::tracker_file_name(dir),
            pointers: Vec::new(),
            persisted_count: Arc::new(Mutex::new(0)),
        };
        fs.atomic_save(&tracker.path, &tracker.to_bytes())?;
        Ok(tracker)
    }

    /// Open an existing tracker in the given directory, decoding the whole file into RAM.
    ///
    /// If the file does not exist or does not decode, return an error.
    pub fn open<Fs: UniversalReadFs>(fs: &Fs, dir: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        let options = OpenOptions {
            writeable: false,
            need_sequential: true,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Sequential),
        };
        let file = fs.open(&path, options, Default::default()).map_err(|err| {
            if err.is_not_found() {
                // If config exists and this file doesn't, it should be treated as
                // inconsistent storage rather than a missing one
                BlobstoreError::service_error(format!(
                    "Compacted tracker file does not exist: {}",
                    path.display(),
                ))
            } else {
                BlobstoreError::from(err)
            }
        })?;

        let pointers = Self::from_bytes(&file.read_whole::<u8>()?).map_err(|err| {
            BlobstoreError::service_error(format!(
                "Invalid compacted tracker file {}: {err}",
                path.display(),
            ))
        })?;

        let count = pointers.len() as PointOffset;
        Ok(Self {
            path,
            pointers,
            persisted_count: Arc::new(Mutex::new(count)),
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Number of mappings.
    ///
    /// This is one past the highest point offset that was ever set, which makes it the next point
    /// offset that is allowed to be set.
    pub fn pointer_count(&self) -> PointOffset {
        self.pointers.len() as PointOffset
    }

    /// Set the mapping for the given point offset.
    ///
    /// Point offsets must be set in monotonically increasing order: each offset must be larger
    /// than every offset set before it. Skipped offsets are backfilled as `None` entries.
    pub fn set(&mut self, point_offset: PointOffset, pointer: ValuePointer) -> Result<()> {
        // Defensive re-check: the storage validates this before appending any value data, see
        // Logstore::put_value
        let next = self.pointer_count();
        if point_offset < next {
            return Err(BlobstoreError::unsupported_operation(format!(
                "cannot set mapping for point offset {point_offset}, the tracker requires \
                 monotonically increasing point offsets, the next allowed point offset is {next}",
            )));
        }

        self.pointers.resize(point_offset as usize, None);
        self.pointers.push(Some(pointer));

        Ok(())
    }

    /// Create a flusher that rewrites the whole file from the mappings set up to this point.
    ///
    /// The mappings are encoded right away, so that mappings set while a flush is in progress
    /// are left for the next flush. The file is replaced atomically, and a stale flush, whose
    /// snapshot holds no more mappings than the file already does, is a no-op: a flush must
    /// never make the file lose mappings a more recent flush persisted.
    ///
    /// Rewriting is linear in the number of mappings, which suits a storage that is flushed
    /// once after being built, not one that is flushed after every batch.
    pub fn flusher<Fs>(&self, fs: Fs) -> Flusher
    where
        Fs: UniversalWriteFileOps + Send + 'static,
    {
        let path = self.path.clone();
        let count = self.pointer_count();
        let bytes = self.to_bytes();
        let persisted_count = Arc::clone(&self.persisted_count);

        Box::new(move || {
            let mut persisted_count = persisted_count.lock();
            if count <= *persisted_count {
                return Ok(());
            }
            fs.atomic_save(&path, &bytes)?;
            *persisted_count = count;
            Ok(())
        })
    }

    fn item(&self, point_offset: PointOffset) -> PointerItem {
        match self.pointers.get(point_offset as usize) {
            Some(pointer) => PointerItem::from(*pointer),
            None => PointerItem::OutOfRange,
        }
    }

    /// Encode all mappings into the file format, see the module docs.
    fn to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut prev = ValuePointer::new(0, 0, 0);
        for pointer in &self.pointers {
            let Some(pointer) = pointer else {
                push_varint(&mut encoded, 0u64);
                continue;
            };

            push_varint(&mut encoded, u64::from(pointer.length) + 1);
            push_varint(
                &mut encoded,
                i64::from(pointer.page_id) - i64::from(prev.page_id),
            );
            push_varint(
                &mut encoded,
                i64::from(pointer.block_offset) - expected_offset(prev, pointer.page_id),
            );
            prev = *pointer;
        }

        let header = Header {
            magic: MAGIC,
            version: U32::new(FORMAT_VERSION),
            count: U32::new(self.pointer_count()),
        };
        let mut bytes = header.as_bytes().to_vec();
        bytes.extend_from_slice(&compress_lz4(&encoded));
        bytes
    }

    /// Decode the file format, see the module docs.
    ///
    /// Every byte is validated: a file that is truncated, has trailing bytes, or holds deltas
    /// that do not add up to a valid pointer is rejected.
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Vec<Option<ValuePointer>>, String> {
        let (header, payload) =
            Header::ref_from_prefix(bytes).map_err(|_| "file is shorter than the header")?;
        if header.magic != MAGIC {
            return Err("unexpected magic".to_string());
        }
        if header.version.get() != FORMAT_VERSION {
            return Err(format!("unsupported version {}", header.version.get()));
        }
        let count = header.count.get();

        let encoded = lz4_flex::decompress_size_prepended(payload)
            .map_err(|err| format!("invalid LZ4 block: {err}"))?;

        let mut pointers = Vec::with_capacity(count as usize);
        let mut pos = 0;
        let mut prev = ValuePointer::new(0, 0, 0);
        for _ in 0..count {
            let length_plus_one: u64 = read_varint(&encoded, &mut pos)?;
            let Some(length) = length_plus_one.checked_sub(1) else {
                pointers.push(None);
                continue;
            };
            let length = u32::try_from(length).map_err(|_| "value length out of range")?;

            let page_delta: i64 = read_varint(&encoded, &mut pos)?;
            let page_id = i64::from(prev.page_id)
                .checked_add(page_delta)
                .and_then(|page_id| u32::try_from(page_id).ok())
                .ok_or("page id out of range")?;

            let offset_delta: i64 = read_varint(&encoded, &mut pos)?;
            let block_offset = expected_offset(prev, page_id)
                .checked_add(offset_delta)
                .and_then(|offset| u32::try_from(offset).ok())
                .ok_or("block offset out of range")?;

            let pointer = ValuePointer::new(page_id, block_offset, length);
            pointers.push(Some(pointer));
            prev = pointer;
        }

        if pos != encoded.len() {
            return Err(format!(
                "{} trailing bytes after {count} mappings",
                encoded.len() - pos,
            ));
        }

        Ok(pointers)
    }
}

/// Where the value after `prev` is expected to start: right behind `prev` on the same page, or at
/// the beginning of a new page.
fn expected_offset(prev: ValuePointer, page_id: u32) -> i64 {
    if page_id == prev.page_id {
        i64::from(prev.block_offset) + i64::from(prev.length)
    } else {
        0
    }
}

fn push_varint<T: VarInt>(buf: &mut Vec<u8>, value: T) {
    let mut encoded = [0u8; 10];
    let size = value.encode_var(&mut encoded);
    buf.extend_from_slice(&encoded[..size]);
}

fn read_varint<T: VarInt>(bytes: &[u8], pos: &mut usize) -> std::result::Result<T, String> {
    let (value, size) = T::decode_var(&bytes[*pos..]).ok_or("truncated varint")?;
    *pos += size;
    Ok(value)
}

impl TrackerRead for CompactedTracker {
    fn max_point_offset(&self) -> Result<PointOffset> {
        Ok(self.pointer_count())
    }

    fn get<P: AccessPattern>(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        Ok(self.pointers.get(point_offset as usize).copied().flatten())
    }

    fn get_range<P: AccessPattern>(
        &self,
        point_offsets: Range<PointOffset>,
    ) -> Result<Vec<Option<ValuePointer>>> {
        let start = (point_offsets.start as usize).min(self.pointers.len());
        let end = (point_offsets.end as usize).min(self.pointers.len());
        let mut pointers = self.pointers[start..end].to_vec();
        pointers.resize(point_offsets.len(), None);
        Ok(pointers)
    }

    fn iter<U, I>(&self, point_offsets: I) -> Result<impl Iterator<Item = Result<(U, PointerItem)>>>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffset)>,
    {
        Ok(point_offsets.map(|(user_data, point_offset)| Ok((user_data, self.item(point_offset)))))
    }
}

#[cfg(test)]
mod tests {
    use common::generic_consts::Random;
    use common::universal_io::MmapFs;
    use tempfile::TempDir;

    use super::*;

    /// Pointers packed back to back across pages of the given capacity, with `None` at the
    /// given point offsets, as the append-only pages lay values out.
    fn packed_pointers(
        lengths: &[u32],
        gaps: &[usize],
        page_capacity: u32,
    ) -> Vec<Option<ValuePointer>> {
        let (mut page_id, mut offset) = (0, 0);
        lengths
            .iter()
            .enumerate()
            .map(|(index, &length)| {
                if gaps.contains(&index) {
                    return None;
                }
                if offset + length > page_capacity {
                    page_id += 1;
                    offset = 0;
                }
                let pointer = ValuePointer::new(page_id, offset, length);
                offset += length;
                Some(pointer)
            })
            .collect()
    }

    fn tracker_with(pointers: &[Option<ValuePointer>]) -> (TempDir, CompactedTracker) {
        let dir = TempDir::new().unwrap();
        let mut tracker = CompactedTracker::new(&MmapFs, dir.path()).unwrap();
        for (point_offset, pointer) in pointers.iter().enumerate() {
            if let Some(pointer) = pointer {
                tracker.set(point_offset as PointOffset, *pointer).unwrap();
            }
        }
        (dir, tracker)
    }

    fn assert_reads(tracker: &CompactedTracker, expected: &[Option<ValuePointer>]) {
        let count = expected.len() as PointOffset;
        assert_eq!(tracker.pointer_count(), count);
        assert_eq!(tracker.max_point_offset().unwrap(), count);

        for (point_offset, pointer) in expected.iter().enumerate() {
            assert_eq!(
                tracker.get::<Random>(point_offset as PointOffset).unwrap(),
                *pointer,
            );
        }
        assert_eq!(tracker.get::<Random>(count).unwrap(), None);
        assert_eq!(tracker.get::<Random>(count + 100).unwrap(), None);

        // Range crossing the end is padded, range past the end is all None
        let mut padded = expected.to_vec();
        padded.resize(expected.len() + 2, None);
        assert_eq!(tracker.get_range::<Random>(0..count + 2).unwrap(), padded);
        assert_eq!(
            tracker.get_range::<Random>(count + 1..count + 3).unwrap(),
            [None, None]
        );

        let items = tracker
            .iter((0..count + 1).map(|point_offset| (point_offset, point_offset)))
            .unwrap()
            .map(|item| item.unwrap())
            .collect::<Vec<_>>();
        let expected_items = expected
            .iter()
            .map(|pointer| PointerItem::from(*pointer))
            .chain([PointerItem::OutOfRange])
            .enumerate()
            .map(|(point_offset, item)| (point_offset as PointOffset, item))
            .collect::<Vec<_>>();
        assert_eq!(items, expected_items);
    }

    /// Flush, reopen, and check that the reopened tracker serves `expected`.
    fn assert_roundtrip(
        tracker: &CompactedTracker,
        dir: &TempDir,
        expected: &[Option<ValuePointer>],
    ) {
        tracker.flusher(MmapFs)().unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        assert_reads(&reopened, expected);
        assert_eq!(reopened.files(), vec![dir.path().join(FILE_NAME)]);
    }

    #[test]
    fn test_new_tracker_is_empty_and_reopens() {
        let (dir, tracker) = tracker_with(&[]);
        assert!(dir.path().join(FILE_NAME).exists());
        assert_reads(&tracker, &[]);

        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        assert_reads(&reopened, &[]);
    }

    #[test]
    fn test_open_missing_tracker_fails() {
        let dir = TempDir::new().unwrap();
        let err = CompactedTracker::open(&MmapFs, dir.path()).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn test_roundtrip_packed_with_gaps_and_page_rollover() {
        let lengths = [10, 0, 300, 25, 25, 1000, 7, 7, 7, 4000, 1];
        let expected = packed_pointers(&lengths, &[1, 4, 8], 1024);
        let (dir, tracker) = tracker_with(&expected);
        assert_reads(&tracker, &expected);
        assert_roundtrip(&tracker, &dir, &expected);
    }

    #[test]
    fn test_roundtrip_arbitrary_pointers() {
        // Not packed: page ids go down, offsets jump around, so every delta sign shows up
        let expected = vec![
            Some(ValuePointer::new(5, 100, 3)),
            Some(ValuePointer::new(2, 0, u32::MAX)),
            None,
            Some(ValuePointer::new(2, u32::MAX, 0)),
            Some(ValuePointer::new(u32::MAX, 1, 1)),
            Some(ValuePointer::new(0, 0, 0)),
        ];
        let (dir, tracker) = tracker_with(&expected);
        assert_roundtrip(&tracker, &dir, &expected);
    }

    #[test]
    fn test_set_rejects_non_monotonic_point_offsets() {
        let (_dir, mut tracker) = tracker_with(&packed_pointers(&[1, 1, 1], &[], 1024));
        for point_offset in [0, 2] {
            let err = tracker
                .set(point_offset, ValuePointer::new(0, 0, 1))
                .unwrap_err();
            assert!(err.to_string().contains("monotonically"), "{err}");
        }
        assert_eq!(tracker.pointer_count(), 3);
        // The next offset and any later one are fine
        tracker.set(3, ValuePointer::new(0, 3, 1)).unwrap();
        tracker.set(10, ValuePointer::new(0, 4, 1)).unwrap();
        assert_eq!(tracker.pointer_count(), 11);
    }

    #[test]
    fn test_unflushed_mappings_are_not_persisted() {
        let expected = packed_pointers(&[1, 2, 3], &[], 1024);
        let (dir, mut tracker) = tracker_with(&expected);
        tracker.flusher(MmapFs)().unwrap();

        tracker.set(3, ValuePointer::new(0, 6, 4)).unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        assert_reads(&reopened, &expected);
    }

    #[test]
    fn test_flusher_snapshots_and_rewrites_whole_file() {
        let first = packed_pointers(&[1, 2, 3], &[], 1024);
        let (dir, mut tracker) = tracker_with(&first);
        let flusher = tracker.flusher(MmapFs);

        // Set after the flusher was created, not part of its snapshot
        tracker.set(3, ValuePointer::new(0, 6, 4)).unwrap();
        flusher().unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        assert_reads(&reopened, &first);

        // The next flush rewrites the file with everything
        tracker.flusher(MmapFs)().unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        let mut second = first;
        second.push(Some(ValuePointer::new(0, 6, 4)));
        assert_reads(&reopened, &second);
    }

    #[test]
    fn test_stale_flusher_is_noop() {
        let first = packed_pointers(&[1, 2, 3], &[], 1024);
        let (dir, mut tracker) = tracker_with(&first);
        let stale = tracker.flusher(MmapFs);

        tracker.set(3, ValuePointer::new(0, 6, 4)).unwrap();
        tracker.flusher(MmapFs)().unwrap();

        // Running the older flusher afterwards must not roll the file back
        stale().unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        let mut second = first;
        second.push(Some(ValuePointer::new(0, 6, 4)));
        assert_reads(&reopened, &second);
    }

    #[test]
    fn test_invalid_files_are_rejected() {
        let expected = packed_pointers(&[1, 2, 3], &[], 1024);
        let (dir, tracker) = tracker_with(&expected);
        let valid = tracker.to_bytes();
        let header_size = size_of::<Header>();

        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 0xff;

        let mut bad_version = valid.clone();
        bad_version[8..12].copy_from_slice(&2u32.to_le_bytes());

        // Header claims one mapping more than the payload holds
        let mut count_too_large = valid.clone();
        count_too_large[12..16].copy_from_slice(&4u32.to_le_bytes());

        // Header claims one mapping less: the last mapping becomes trailing bytes
        let mut count_too_small = valid.clone();
        count_too_small[12..16].copy_from_slice(&2u32.to_le_bytes());

        let mut corrupt_payload = valid.clone();
        *corrupt_payload.last_mut().unwrap() ^= 0xff;

        let cases = [
            ("empty", Vec::new()),
            ("truncated header", valid[..header_size - 1].to_vec()),
            ("truncated payload", valid[..valid.len() - 1].to_vec()),
            ("bad magic", bad_magic),
            ("bad version", bad_version),
            ("count too large", count_too_large),
            ("count too small", count_too_small),
            ("corrupt payload", corrupt_payload),
        ];
        for (name, bytes) in cases {
            MmapFs
                .atomic_save(&dir.path().join(FILE_NAME), &bytes)
                .unwrap();
            let err = CompactedTracker::open(&MmapFs, dir.path()).unwrap_err();
            assert!(
                err.to_string().contains("Invalid compacted tracker"),
                "{name}: {err}"
            );
        }

        // And the untouched encoding still opens
        MmapFs
            .atomic_save(&dir.path().join(FILE_NAME), &valid)
            .unwrap();
        let reopened = CompactedTracker::open(&MmapFs, dir.path()).unwrap();
        assert_reads(&reopened, &expected);
    }

    #[test]
    fn test_packed_mappings_encode_small() {
        let lengths = (0..10_000).map(|i| 100 + (i % 37)).collect::<Vec<u32>>();
        let expected = packed_pointers(&lengths, &[], 1 << 20);
        let (_dir, tracker) = tracker_with(&expected);

        let bytes = tracker.to_bytes();
        // 16 bytes per entry in the flat tracker file; the deltas are zeros here, only the
        // lengths carry information, so the file should be a small fraction of that
        assert!(
            bytes.len() < expected.len(),
            "{} bytes for {} mappings",
            bytes.len(),
            expected.len()
        );
    }
}
