use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, TypedStorage, UniversalRead};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Loads the persisted flags into an in-memory roaring bitmap on open. No
/// write path: there is no buffer, no [`BufferedDynamicFlags`][2], no
/// [`DynamicStoredFlags`][3] — the storage backend is bound to
/// [`UniversalRead`] only. Everything is in RAM after construction, so
/// `populate` / `clear_cache` from [`RoaringFlagsRead`] use their default
/// no-op behavior.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct ReadOnlyRoaringFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on open.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,

    directory: PathBuf,

    _marker: std::marker::PhantomData<fn() -> S>,
}

impl<S: UniversalRead> ReadOnlyRoaringFlags<S> {
    /// Open persisted flags read-only and materialize them into an in-memory
    /// roaring bitmap.
    ///
    /// Read-only counterpart of [`RoaringFlags::new`][1]: every file is opened
    /// through `fs` non-writable, nothing is created and nothing is written.
    /// The logical length comes from the status file (the flags file is padded
    /// past it), and the set positions from the flags file — shared with the
    /// writable path via [`StoredBitSlice::iter_ones`].
    ///
    /// [1]: super::roaring_flags::RoaringFlags::new
    pub fn open(fs: &S::Fs, directory: &Path) -> OperationResult<Self> {
        // Logical length: read the status struct directly. `StoredStruct` is
        // write-bound, so go through the read-only `TypedStorage`.
        let status_path = status_file(directory);
        let status = TypedStorage::<S, DynamicFlagsStatus>::open(
            fs,
            &status_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        let len = status.read_whole()?[0].len();

        // Set positions: open the flags file and build the bitmap. The
        // `StoredBitSlice` is dropped at the end of this function — the
        // bitmap is the only state we keep.
        let flags_path = directory.join(FLAGS_FILE);
        let flags_storage = StoredBitSlice::<S>::open(
            fs,
            &flags_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;

        let bitmap = RoaringBitmap::from_sorted_iter(
            flags_storage.iter_ones()?.map(|i| i as PointOffsetType),
        )
        .expect("iter_ones iterates in sorted order");

        Ok(Self {
            bitmap,
            len,
            directory: directory.to_path_buf(),
            _marker: std::marker::PhantomData,
        })
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
