use std::path::{Path, PathBuf};

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OkNotFound, OpenOptions, Populate, TypedStorage, UniversalRead};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Loads the persisted flags into an in-memory roaring bitmap on open. No
/// write path: there is no buffer, no [`BufferedDynamicFlags`][2], no
/// [`DynamicStoredFlags`][3]. After construction nothing on the struct
/// depends on the storage backend — `S` only appears on [`Self::open`],
/// so consumers don't have to thread the backend type through the index
/// stack just to hold an in-memory bitmap.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct ReadOnlyRoaringFlags {
    /// In-memory bitmap of true flags, materialized from the backing file on open.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,

    directory: PathBuf,
}

impl ReadOnlyRoaringFlags {
    /// Open persisted flags read-only and materialize them into an in-memory
    /// roaring bitmap.
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
    pub fn open<S: UniversalRead>(fs: &S::Fs, directory: &Path) -> OperationResult<Option<Self>> {
        // Logical length: read the status struct directly. `StoredStruct` is
        // write-bound, so go through the read-only `TypedStorage`. A missing
        // status file means the index isn't present, so map not-found to `None`.
        let status_path = status_file(directory);
        let Some(status) = TypedStorage::<S, DynamicFlagsStatus>::open(
            fs,
            &status_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )
        .ok_not_found()?
        else {
            return Ok(None);
        };
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

        Ok(Some(Self {
            bitmap,
            len,
            directory: directory.to_path_buf(),
        }))
    }
}

impl RoaringFlagsRead for ReadOnlyRoaringFlags {
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
