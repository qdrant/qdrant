use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound, OpenOptions, Populate, TypedStorage, UniversalRead};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Loads the persisted flags into an in-memory roaring bitmap on open and keeps
/// the backing [`StoredBitSlice`] handle so [`LiveReload::live_reload`] can
/// reopen it and fetch only the changed positions, rather than re-scanning the
/// whole file. There is no write path: no buffer, no [`BufferedDynamicFlags`][2],
/// no [`DynamicStoredFlags`][3]. The retained `S` is the one [`Self::open`] was
/// called with, mirroring the other read-only field indexes, which likewise
/// hold their backing storage across reloads.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct ReadOnlyRoaringFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on
    /// open and patched in place on [`LiveReload::live_reload`].
    bitmap: RoaringBitmap,
    /// Backing bitslice, retained so a reload can reopen it and read only the
    /// changed positions.
    storage: StoredBitSlice<S>,
    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,
    directory: PathBuf,
}

/// Read-only open options shared by every file this storage maps: never
/// writable, lazily paged under the global mmap advice, nothing populated up
/// front. `OpenOptions` has no general constructor by design (callers spell the
/// knobs out), so this one value keeps the read path's opens identical.
const READ_ONLY_OPTIONS: OpenOptions = OpenOptions {
    writeable: false,
    need_sequential: false,
    populate: Populate::No,
    advice: AdviceSetting::Global,
};

/// Read the logical flag length from the status struct, opened read-only.
///
/// `StoredStruct` is write-bound, so the read goes through the read-only
/// `TypedStorage`. Returns `Ok(None)` when the status file is absent — the flag
/// directory doesn't exist — matching the read path's never-create contract.
fn read_status_len<S: UniversalRead>(
    fs: &S::Fs,
    directory: &Path,
) -> OperationResult<Option<usize>> {
    let Some(status) = TypedStorage::<S, DynamicFlagsStatus>::open(
        fs,
        status_file(directory),
        READ_ONLY_OPTIONS,
        Default::default(),
    )
    .ok_not_found()?
    else {
        return Ok(None);
    };
    Ok(Some(status.read_whole()?[0].len()))
}

/// Open the flags bitslice read-only. Shared by `open` (full scan into a fresh
/// bitmap) and `live_reload` (delta reads of the changed positions).
fn open_flags_storage<S: UniversalRead>(
    fs: &S::Fs,
    directory: &Path,
) -> OperationResult<StoredBitSlice<S>> {
    Ok(StoredBitSlice::<S>::open(
        fs,
        directory.join(FLAGS_FILE),
        READ_ONLY_OPTIONS,
        Default::default(),
    )?)
}

impl<S: UniversalRead> ReadOnlyRoaringFlags<S> {
    /// Open persisted flags read-only and materialize them into an in-memory
    /// roaring bitmap, retaining the bitslice handle for [`LiveReload`].
    ///
    /// Read-only counterpart of [`RoaringFlags::new`][1]: every file is opened
    /// through `fs` non-writable, nothing is created and nothing is written.
    /// The logical length comes from the status file (the flags file is padded
    /// past it), and the set positions from the flags file — shared with the
    /// writable path via [`StoredBitSlice::iter_ones`].
    ///
    /// Returns [`Ok(None)`] when the flag directory doesn't exist (the status
    /// file is absent), matching the read path's never-create contract.
    ///
    /// [1]: super::roaring_flags::RoaringFlags::new
    pub fn open(fs: &S::Fs, directory: &Path) -> OperationResult<Option<Self>> {
        // A missing status file means the index isn't present on disk.
        let Some(len) = read_status_len::<S>(fs, directory)? else {
            return Ok(None);
        };

        // Build the bitmap from the set positions, then keep the bitslice so a
        // reload can reopen it and read only the changed positions.
        let storage = open_flags_storage::<S>(fs, directory)?;
        let bitmap =
            RoaringBitmap::from_sorted_iter(storage.iter_ones()?.map(|i| i as PointOffsetType))
                .expect("iter_ones iterates in sorted order");

        Ok(Some(Self {
            bitmap,
            storage,
            len,
            directory: directory.to_path_buf(),
        }))
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyRoaringFlags<S> {
    type Fs = S::Fs;

    /// Refresh the in-memory bitmap to the current on-disk state by fetching
    /// only the delta, instead of re-scanning every set position like
    /// [`Self::open`].
    ///
    /// `deleted_points` are dropped from the bitmap — a removed point belongs in
    /// no flag set, so this needs no I/O. `new_points` have their flag re-read
    /// from the reopened bitslice and the bitmap bit set or cleared to match;
    /// reading the current bit (rather than only inserting) also folds in value
    /// changes, e.g. a point flipping from `true` to `false`.
    ///
    /// `hw_counter` is unused: the per-position reads go through the retained
    /// [`StoredBitSlice`], which takes no hardware counter — mirroring
    /// [`Self::open`], which also doesn't account its scan.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &[PointOffsetType],
        new_points: &[PointOffsetType],
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for &point in deleted_points {
            self.bitmap.remove(point);
        }

        // Nothing appended → no on-disk delta to fetch, skip all I/O.
        if new_points.is_empty() {
            return Ok(());
        }

        // Reopen to pick up the writer's appended data, then read only the
        // changed positions.
        self.storage.reopen()?;
        for &point in new_points {
            // Possible optimization: If new_points is sorted, we should be able to use read_bit_range and iter_ones on top of it
            if self.storage.get_bit(u64::from(point))?.unwrap_or(false) {
                self.bitmap.insert(point);
            }
        }

        // The logical length grows as points are appended; refresh it so
        // length-driven readers (the null index's `iter_falses`) stay correct.
        if let Some(len) = read_status_len::<S>(fs, &self.directory)? {
            self.len = len;
        }

        Ok(())
    }
}

impl<S: UniversalRead> RoaringFlagsRead for ReadOnlyRoaringFlags<S> {
    fn len(&self) -> usize {
        self.len
    }

    fn get_bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }
}
