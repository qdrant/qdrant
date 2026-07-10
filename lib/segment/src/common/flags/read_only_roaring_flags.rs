use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::AdviceSetting;
use common::sorted_slice::SortedSlice;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, TypedStorage, UniversalRead, UniversalReadFs,
};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Materializes the persisted flags into an in-memory roaring bitmap on first
/// use — *not* on open, unlike the writable variant — and keeps the backing
/// [`StoredBitSlice`] handle so [`LiveReload::live_reload`] can reopen it to
/// read the points the writer appended, rather than re-scanning the whole file.
/// There is no write path: no buffer, no [`BufferedDynamicFlags`][2], no
/// [`DynamicStoredFlags`][3]. The retained `S` is the one [`Self::open`] was
/// called with, mirroring the other read-only field indexes, which likewise
/// hold their backing storage across reloads.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct ReadOnlyRoaringFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on
    /// first access and patched in place on [`LiveReload::live_reload`].
    ///
    /// Lazy so that opening the flags reads only the (tiny) status file: a
    /// segment open would otherwise scan every flags file end to end, which
    /// defeats prefetching only the bytes a query actually needs.
    ///
    /// [`OnceLock`] rather than a plain cell because the index is queried
    /// through `&self` from many threads. On a race both threads may build a
    /// bitmap; the first to finish wins and the loser's copy is dropped.
    bitmap: OnceLock<RoaringBitmap>,
    /// Backing bitslice. A reload reopens this handle so points the writer
    /// appended become readable, then reads only those new positions.
    storage: StoredBitSlice<S>,
    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,
    directory: PathBuf,
}

fn open_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate,
        advice: AdviceSetting::Global,
    }
}

/// Read the logical flag length from the status struct, opened read-only.
///
/// Returns `Ok(None)` when the status file is absent
fn read_status_len<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
) -> OperationResult<Option<usize>> {
    let Some(file) = fs
        .open(
            status_file(directory),
            open_options(Populate::No),
            Default::default(),
        )
        .ok_not_found()?
    else {
        return Ok(None);
    };
    let status = TypedStorage::<S, DynamicFlagsStatus>::new(file);
    Ok(Some(status.read_whole()?[0].len()))
}

impl<S: UniversalRead> ReadOnlyRoaringFlags<S> {
    /// Schedule background prefetch of the two files this storage reads.
    ///
    /// Returns whether the flag directory exists.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        // Status file.
        if fs
            .schedule_prefetch(
                &status_file(directory),
                Some(open_options(Populate::PreferBackground)),
                None,
            )
            .ok_not_found()?
            .is_none()
        {
            return Ok(false);
        }

        // Bitslice
        fs.schedule_prefetch(
            &directory.join(FLAGS_FILE),
            Some(open_options(populate)),
            None,
        )?;

        Ok(true)
    }

    /// Open persisted flags read-only, retaining the bitslice handle for
    /// [`Self::bitmap`] and [`LiveReload`].
    ///
    /// Returns [`Ok(None)`] when the flag directory doesn't exist (the status
    /// file is absent), matching the read path's never-create contract.
    ///
    /// [1]: super::roaring_flags::RoaringFlags::new
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
    ) -> OperationResult<Option<Self>> {
        // A missing status file means the index isn't present on disk.
        let Some(len) = read_status_len::<S>(fs, directory)? else {
            return Ok(None);
        };

        let storage = StoredBitSlice::<S>::open(
            fs,
            directory.join(FLAGS_FILE),
            open_options(Populate::No),
            Default::default(),
        )?;

        Ok(Some(Self {
            bitmap: OnceLock::new(),
            storage,
            len,
            directory: directory.to_path_buf(),
        }))
    }

    /// The in-memory bitmap of set positions, scanning the flags file to build
    /// it on the first call and returning the cached one afterwards.
    ///
    /// This is the whole-file read that [`Self::open`] avoids. It is deferred
    /// to the first query rather than paid per segment open — many segments
    /// hold flag indexes (every payload field carries a null index) that no
    /// query ever touches.
    fn bitmap(&self) -> OperationResult<&RoaringBitmap> {
        // `OnceLock::get_or_try_init` is still unstable, so build outside the
        // lock and let `get_or_init` arbitrate. A racing thread's bitmap is
        // simply dropped: both are built from the same bytes.
        if let Some(bitmap) = self.bitmap.get() {
            return Ok(bitmap);
        }

        let bitmap = RoaringBitmap::from_sorted_iter(
            self.storage.iter_ones()?.map(|i| i as PointOffsetType),
        )
        .expect("iter_ones iterates in sorted order");

        Ok(self.bitmap.get_or_init(|| bitmap))
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyRoaringFlags<S> {
    type Fs = S::Fs;

    /// Refresh the in-memory bitmap to the current on-disk state by fetching
    /// only the delta, instead of re-scanning every set position like
    /// [`Self::open`].
    ///
    /// `deleted_points` are dropped from the bitmap — a removed point belongs in
    /// no flag set, so this needs no I/O. `new_points` are freshly appended
    /// offsets (the producer is append-only — see the body): each has its flag
    /// read from the reopened bitslice and is inserted when set. A new offset was
    /// never in the bitmap, so an unset flag needs no action.
    ///
    /// `hw_counter` is unused: the per-position reads go through the reopened
    /// [`StoredBitSlice`], which takes no hardware counter — mirroring
    /// [`Self::bitmap`], which also doesn't account its scan.
    ///
    /// While the bitmap is still unmaterialized there is nothing to patch: the
    /// eventual scan reads the current on-disk flags, which the writer has
    /// already brought up to date (it clears a retired point's flag). The
    /// bitslice is still reopened, so that scan sees the grown file.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let Some(bitmap) = self.bitmap.get_mut() {
            for &point in deleted_points {
                bitmap.remove(point);
            }
        }

        // Nothing appended → no on-disk delta to fetch, skip all I/O.
        if new_points.is_empty() {
            return Ok(());
        }

        // Live reload is append-only, so we only need to process the new range of
        // the bitslice.
        self.storage.reopen()?;
        if let (Some(bitmap), Some(new_range)) = (self.bitmap.get_mut(), new_points.range_u64()) {
            let start = new_range.start as usize;
            let bitslice = self.storage.read_bit_range(new_range)?;
            for point in bitslice
                .iter_ones()
                .map(|idx| (idx + start) as PointOffsetType)
            {
                bitmap.insert(point);
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

    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap> {
        self.bitmap()
    }

    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap> {
        self.bitmap.get()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }
}
