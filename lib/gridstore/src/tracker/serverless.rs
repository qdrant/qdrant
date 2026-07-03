use std::ops::Range;
use std::path::{Path, PathBuf};

use fs_err::File;

use crate::error::GridstoreError;
use crate::gridstore::Flusher;
use crate::tracker::{OptionalPointer, PointOffset, ValuePointer};
use crate::{Result, direct_io};

/// File name of the serverless tracker file
///
/// Deliberately different from the dynamic tracker file name, so that one mode never attempts to
/// load the incompatible file format of the other.
const FILE_NAME: &str = "serverless_tracker.dat";

/// Size in bytes of a single mapping entry in the tracker file
const ENTRY_SIZE: u64 = size_of::<OptionalPointer>() as u64;

/// Append-only tracker of value pointers for the serverless storage mode.
///
/// Stores a plain array of [`OptionalPointer`] entries in a single file, without any header. The
/// entry at index `i` is the mapping for point offset `i`, and the number of mappings is defined
/// by the exact file length. The file starts empty and only ever grows by appending; existing
/// bytes are never rewritten.
///
/// Mappings must be set in monotonically increasing point offset order. Skipped point offsets are
/// materialized as zeroed entries, which decode as `None`. New mappings are buffered in memory,
/// and appended to the file as a single write when flushing.
///
/// The file is read and written directly, it is never memory mapped.
///
/// A write may be torn. If the file length is not a multiple of the entry size, the trailing
/// partial entry is ignored when reading, and truncated away when opening writable.
#[derive(Debug)]
pub(crate) struct ServerlessTracker {
    /// Path to the tracker file
    path: PathBuf,
    /// Open handle to the tracker file
    file: File,
    /// Number of mappings persisted in the file
    persisted_count: PointOffset,
    /// Mappings that haven't been written to the file yet
    ///
    /// Entry `i` holds the mapping for point offset `persisted_count + i`, with gaps kept as
    /// `None` entries. This is byte for byte the data of the next append.
    pending: Vec<OptionalPointer>,
}

impl ServerlessTracker {
    fn tracker_file_name(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// Create a new empty tracker in the given directory.
    ///
    /// The directory must exist already.
    pub fn new(dir: &Path) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        let file = direct_io::create_new(&path)?;
        Ok(Self {
            path,
            file,
            persisted_count: 0,
            pending: Vec::new(),
        })
    }

    /// Open an existing tracker in the given directory.
    ///
    /// If the file does not exist, return an error.
    ///
    /// A trailing partial entry due to a torn write is ignored. When opening writable it is also
    /// truncated away, so that appends always start at a whole entry offset.
    pub fn open(dir: &Path, writeable: bool) -> Result<Self> {
        let path = Self::tracker_file_name(dir);
        let file = direct_io::open_existing(&path, writeable, "Serverless tracker")?;

        let len = file.metadata()?.len();
        let aligned_len = len - (len % ENTRY_SIZE);
        if writeable && aligned_len != len {
            // Recover from a torn write by truncating the partial trailing entry
            file.set_len(aligned_len)?;
        }

        Ok(Self {
            path,
            file,
            persisted_count: count_from_len(aligned_len)?,
            pending: Vec::new(),
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Number of mappings, including pending ones.
    ///
    /// This is one past the highest point offset that was ever set, which makes it the next point
    /// offset that is allowed to be set.
    pub fn pointer_count(&self) -> PointOffset {
        self.persisted_count + self.pending.len() as PointOffset
    }

    /// Get the mapping at the given point offset.
    ///
    /// Point offsets that were skipped or that are past the highest set mapping yield `None`.
    pub fn get(&self, point_offset: PointOffset) -> Result<Option<ValuePointer>> {
        if point_offset >= self.pointer_count() {
            return Ok(None);
        }

        if point_offset >= self.persisted_count {
            let pending_index = (point_offset - self.persisted_count) as usize;
            return Ok(self.pending[pending_index].to_option());
        }

        let mut buf = [0; size_of::<OptionalPointer>()];
        direct_io::read_exact_at(&self.file, &mut buf, u64::from(point_offset) * ENTRY_SIZE)?;
        let pointer: OptionalPointer = bytemuck::pod_read_unaligned(&buf);
        Ok(pointer.to_option())
    }

    /// Get the mappings for a contiguous range of point offsets.
    ///
    /// The persisted part of the range is read with a single read. Point offsets that were
    /// skipped or that are past the highest set mapping yield `None`.
    ///
    /// The result holds one entry per requested point offset, so callers should bound the range
    /// they ask for.
    pub fn get_range(
        &self,
        point_offsets: Range<PointOffset>,
    ) -> Result<Vec<Option<ValuePointer>>> {
        let mut pointers = Vec::with_capacity(point_offsets.len());
        let start = point_offsets.start;
        let end = point_offsets.end.min(self.pointer_count());

        // Persisted part of the range, read in a single read
        let persisted_end = end.min(self.persisted_count);
        if start < persisted_end {
            let count = (persisted_end - start) as usize;
            let mut buf = vec![0; count * size_of::<OptionalPointer>()];
            direct_io::read_exact_at(&self.file, &mut buf, u64::from(start) * ENTRY_SIZE)?;
            pointers.extend(
                buf.chunks_exact(size_of::<OptionalPointer>()).map(|entry| {
                    bytemuck::pod_read_unaligned::<OptionalPointer>(entry).to_option()
                }),
            );
        }

        // Pending part of the range
        for point_offset in start.max(self.persisted_count)..end {
            let pending_index = (point_offset - self.persisted_count) as usize;
            pointers.push(self.pending[pending_index].to_option());
        }

        // Point offsets past the highest set mapping
        pointers.resize(point_offsets.len(), None);

        Ok(pointers)
    }

    /// Set the mapping for the given point offset, buffering it in memory until flushed.
    ///
    /// Point offsets must be set in monotonically increasing order: each offset must be larger
    /// than every offset set before it. Skipped offsets are backfilled as `None` entries.
    pub fn set(&mut self, point_offset: PointOffset, pointer: ValuePointer) -> Result<()> {
        // Defensive re-check: the storage validates this before appending any value data, see
        // ServerlessGridstore::put_value
        let next = self.pointer_count();
        if point_offset < next {
            return Err(GridstoreError::unsupported_operation(format!(
                "cannot set mapping for point offset {point_offset}, the tracker is append-only \
                 and requires monotonically increasing point offsets, the next allowed point \
                 offset is {next}",
            )));
        }

        // Materialize skipped point offsets as None entries
        let pending_index = (point_offset - self.persisted_count) as usize;
        self.pending.resize(pending_index, OptionalPointer::none());
        self.pending.push(OptionalPointer::some(pointer));

        Ok(())
    }

    /// Append pending mappings for point offsets up to, but excluding, `target` to the file.
    ///
    /// All appended mappings are written with a single write at the end of the file. The `target`
    /// is captured through [`pointer_count`](Self::pointer_count) when a flusher is created, so
    /// that mappings set while a flush is in progress stay pending.
    ///
    /// A stale flush, with a `target` at or below what a more recent flush already persisted, is
    /// a no-op: bytes that were appended before must never be written again.
    ///
    /// This does not sync the file to disk, invoke a [`flusher`](Self::flusher) afterwards.
    pub fn write_pending(&mut self, target: PointOffset) -> Result<()> {
        if target <= self.persisted_count {
            return Ok(());
        }

        let count = (target - self.persisted_count) as usize;
        debug_assert!(
            count <= self.pending.len(),
            "flush target exceeds pending mappings",
        );
        let count = count.min(self.pending.len());

        let entries = &self.pending[..count];
        let offset = u64::from(self.persisted_count) * ENTRY_SIZE;
        if let Err(err) = direct_io::write_all_at(&self.file, bytemuck::cast_slice(entries), offset)
        {
            // Best effort: drop partially appended entries, so that the file stays consistent
            // with the persisted count and a retried flush never rewrites existing bytes
            let _ = self.file.set_len(offset);
            return Err(err.into());
        }

        self.pending.drain(..count);
        self.persisted_count += count as PointOffset;

        Ok(())
    }

    /// Create a closure that syncs all written mappings in the tracker file to disk.
    pub fn flusher(&self) -> Result<Flusher> {
        let file = self.file.try_clone()?;
        Ok(Box::new(move || {
            file.sync_data()?;
            Ok(())
        }))
    }

    /// Reload the number of mappings from the file, making newly appended mappings visible.
    ///
    /// Important assumptions:
    ///
    /// - Should only be called on read-only instances of the tracker.
    /// - Mappings are append-only, existing entries never change.
    /// - Partial writes are possible, but ignored: a trailing partial entry is not counted.
    ///
    /// Returns `true` if there are new mappings, `false` if the tracker is already up to date.
    pub fn live_reload(&mut self) -> Result<bool> {
        let len = self.file.metadata()?.len();
        let new_count = count_from_len(len)?;

        if new_count < self.persisted_count {
            Err(GridstoreError::service_error(format!(
                "live reload cannot decrease mapping count, possible data loss: old count {}, new count {new_count}",
                self.persisted_count,
            )))
        } else if new_count == self.persisted_count {
            // No new mappings, no need to reload
            Ok(false)
        } else {
            self.persisted_count = new_count;
            Ok(true)
        }
    }
}

/// Number of whole mapping entries in a tracker file of the given length.
///
/// A trailing partial entry due to a torn write is not counted.
fn count_from_len(len: u64) -> Result<PointOffset> {
    PointOffset::try_from(len / ENTRY_SIZE).map_err(|_| {
        GridstoreError::service_error(format!(
            "serverless tracker file of {len} bytes holds more mappings than supported",
        ))
    })
}

#[cfg(test)]
mod tests {
    use fs_err as fs;
    use tempfile::TempDir;

    use super::*;

    fn empty_tracker() -> (TempDir, ServerlessTracker) {
        let dir = TempDir::new().unwrap();
        let tracker = ServerlessTracker::new(dir.path()).unwrap();
        (dir, tracker)
    }

    fn pointer(n: u32) -> ValuePointer {
        ValuePointer::new(0, n * 2, n * 3 + 1)
    }

    fn file_len(tracker: &ServerlessTracker) -> u64 {
        fs::metadata(&tracker.path).unwrap().len()
    }

    #[test]
    fn test_new_tracker_is_empty() {
        let (_dir, tracker) = empty_tracker();
        assert_eq!(tracker.pointer_count(), 0);
        assert_eq!(tracker.get(0).unwrap(), None);
        assert_eq!(file_len(&tracker), 0);
    }

    #[test]
    fn test_open_missing_tracker_fails() {
        let dir = TempDir::new().unwrap();
        assert!(ServerlessTracker::open(dir.path(), true).is_err());
        assert!(ServerlessTracker::open(dir.path(), false).is_err());
    }

    #[test]
    fn test_set_and_get_pending() {
        let (_dir, mut tracker) = empty_tracker();

        for n in 0..5 {
            tracker.set(n, pointer(n)).unwrap();
        }

        assert_eq!(tracker.pointer_count(), 5);
        for n in 0..5 {
            assert_eq!(tracker.get(n).unwrap(), Some(pointer(n)));
        }
        assert_eq!(tracker.get(5).unwrap(), None);

        // Nothing is written to the file until flushed
        assert_eq!(file_len(&tracker), 0);
    }

    #[test]
    fn test_set_rejects_non_monotonic_point_offsets() {
        let (_dir, mut tracker) = empty_tracker();

        tracker.set(0, pointer(0)).unwrap();
        // Setting the same point offset twice is rejected
        assert!(tracker.set(0, pointer(0)).is_err());

        tracker.set(5, pointer(5)).unwrap();
        // Setting a lower point offset is rejected, even if it was never set
        assert!(tracker.set(3, pointer(3)).is_err());
        assert!(tracker.set(5, pointer(5)).is_err());

        tracker.set(6, pointer(6)).unwrap();
        assert_eq!(tracker.pointer_count(), 7);
    }

    #[test]
    fn test_skipped_point_offsets_read_as_none() {
        let (_dir, mut tracker) = empty_tracker();

        tracker.set(0, pointer(0)).unwrap();
        tracker.set(4, pointer(4)).unwrap();

        assert_eq!(tracker.pointer_count(), 5);
        assert_eq!(tracker.get(0).unwrap(), Some(pointer(0)));
        for n in 1..4 {
            assert_eq!(tracker.get(n).unwrap(), None);
        }
        assert_eq!(tracker.get(4).unwrap(), Some(pointer(4)));

        // Gaps are persisted as zeroed entries
        tracker.write_pending(tracker.pointer_count()).unwrap();
        assert_eq!(file_len(&tracker), 5 * ENTRY_SIZE);
        assert_eq!(tracker.get(2).unwrap(), None);
    }

    #[test]
    fn test_write_pending_and_reopen() {
        let dir = TempDir::new().unwrap();

        let mut tracker = ServerlessTracker::new(dir.path()).unwrap();
        for n in 0..5 {
            tracker.set(n, pointer(n)).unwrap();
        }
        tracker.write_pending(tracker.pointer_count()).unwrap();
        tracker.flusher().unwrap()().unwrap();

        // The file length matches the exact number of mappings
        assert_eq!(file_len(&tracker), 5 * ENTRY_SIZE);
        drop(tracker);

        let tracker = ServerlessTracker::open(dir.path(), true).unwrap();
        assert_eq!(tracker.pointer_count(), 5);
        for n in 0..5 {
            assert_eq!(tracker.get(n).unwrap(), Some(pointer(n)));
        }
        assert_eq!(
            tracker.get_range(0..7).unwrap(),
            (0..5)
                .map(|n| Some(pointer(n)))
                .chain([None, None])
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn test_partial_write_pending() {
        let (_dir, mut tracker) = empty_tracker();

        for n in 0..5 {
            tracker.set(n, pointer(n)).unwrap();
        }

        // Flush only the first three mappings
        tracker.write_pending(3).unwrap();
        assert_eq!(file_len(&tracker), 3 * ENTRY_SIZE);
        assert_eq!(tracker.pointer_count(), 5);
        for n in 0..5 {
            assert_eq!(tracker.get(n).unwrap(), Some(pointer(n)));
        }

        // Then the rest
        tracker.write_pending(5).unwrap();
        assert_eq!(file_len(&tracker), 5 * ENTRY_SIZE);
        for n in 0..5 {
            assert_eq!(tracker.get(n).unwrap(), Some(pointer(n)));
        }
    }

    #[test]
    fn test_stale_flush_is_noop() {
        let (_dir, mut tracker) = empty_tracker();

        for n in 0..3 {
            tracker.set(n, pointer(n)).unwrap();
        }
        tracker.write_pending(3).unwrap();
        assert_eq!(file_len(&tracker), 3 * ENTRY_SIZE);

        // A stale flush with an old target must not write anything again
        tracker.write_pending(2).unwrap();
        tracker.write_pending(3).unwrap();
        assert_eq!(file_len(&tracker), 3 * ENTRY_SIZE);
        assert_eq!(tracker.pointer_count(), 3);
        for n in 0..3 {
            assert_eq!(tracker.get(n).unwrap(), Some(pointer(n)));
        }
    }

    #[test]
    fn test_torn_write_is_ignored_and_truncated() {
        let dir = TempDir::new().unwrap();

        let mut tracker = ServerlessTracker::new(dir.path()).unwrap();
        for n in 0..5 {
            tracker.set(n, pointer(n)).unwrap();
        }
        tracker.write_pending(tracker.pointer_count()).unwrap();
        let path = tracker.path.clone();
        drop(tracker);

        // Simulate a torn write by appending a partial entry
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        direct_io::write_all_at(&file, &[0xAA; 7], 5 * ENTRY_SIZE).unwrap();
        drop(file);
        assert_eq!(fs::metadata(&path).unwrap().len(), 5 * ENTRY_SIZE + 7);

        // A read-only open ignores the partial entry, but leaves the file untouched
        let tracker = ServerlessTracker::open(dir.path(), false).unwrap();
        assert_eq!(tracker.pointer_count(), 5);
        assert_eq!(tracker.get(4).unwrap(), Some(pointer(4)));
        assert_eq!(file_len(&tracker), 5 * ENTRY_SIZE + 7);
        drop(tracker);

        // A writable open truncates the partial entry away
        let tracker = ServerlessTracker::open(dir.path(), true).unwrap();
        assert_eq!(tracker.pointer_count(), 5);
        assert_eq!(tracker.get(4).unwrap(), Some(pointer(4)));
        assert_eq!(file_len(&tracker), 5 * ENTRY_SIZE);
    }

    #[test]
    fn test_live_reload() {
        let dir = TempDir::new().unwrap();

        let mut writer = ServerlessTracker::new(dir.path()).unwrap();
        for n in 0..3 {
            writer.set(n, pointer(n)).unwrap();
        }
        writer.write_pending(writer.pointer_count()).unwrap();

        let mut reader = ServerlessTracker::open(dir.path(), false).unwrap();
        assert_eq!(reader.pointer_count(), 3);
        assert!(!reader.live_reload().unwrap());

        // The reader picks up newly appended mappings
        for n in 3..6 {
            writer.set(n, pointer(n)).unwrap();
        }
        writer.write_pending(writer.pointer_count()).unwrap();

        assert!(reader.live_reload().unwrap());
        assert_eq!(reader.pointer_count(), 6);
        for n in 0..6 {
            assert_eq!(reader.get(n).unwrap(), Some(pointer(n)));
        }
        assert!(!reader.live_reload().unwrap());
    }

    #[test]
    fn test_get_range_spans_persisted_and_pending() {
        let (_dir, mut tracker) = empty_tracker();

        for n in 0..3 {
            tracker.set(n, pointer(n)).unwrap();
        }
        tracker.write_pending(3).unwrap();
        tracker.set(3, pointer(3)).unwrap();
        tracker.set(6, pointer(6)).unwrap();

        assert_eq!(
            tracker.get_range(0..9).unwrap(),
            vec![
                Some(pointer(0)),
                Some(pointer(1)),
                Some(pointer(2)),
                Some(pointer(3)),
                None,
                None,
                Some(pointer(6)),
                None,
                None,
            ],
        );
        assert_eq!(
            tracker.get_range(2..4).unwrap(),
            vec![Some(pointer(2)), Some(pointer(3)),]
        );
        assert_eq!(tracker.get_range(7..9).unwrap(), vec![None, None]);
        #[allow(clippy::reversed_empty_ranges)]
        let empty = tracker.get_range(3..3).unwrap();
        assert!(empty.is_empty());
    }
}
