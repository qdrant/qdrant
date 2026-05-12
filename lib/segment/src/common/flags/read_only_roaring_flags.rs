use std::path::{Path, PathBuf};

use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, TypedStorage, UniversalRead};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Loads the persisted flags into an in-memory roaring bitmap on open and keeps
/// the underlying flags file around for [`populate`] / [`clear_cache`] /
/// [`files`]. No write path: there is no buffer, no [`BufferedDynamicFlags`][2],
/// no [`DynamicStoredFlags`][3] — the storage backend is bound to
/// [`UniversalRead`] only.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
/// [`populate`]: ReadOnlyRoaringFlags::populate
/// [`clear_cache`]: ReadOnlyRoaringFlags::clear_cache
/// [`files`]: ReadOnlyRoaringFlags::files
pub struct ReadOnlyRoaringFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on open.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,

    /// Backing flags file. Kept open so `populate` / `clear_cache` / `files`
    /// can drive the underlying storage; never read from again after the
    /// bitmap is built.
    flags_storage: StoredBitSlice<S>,

    directory: PathBuf,
}

impl<S: UniversalRead> ReadOnlyRoaringFlags<S> {
    pub fn open(directory: &Path, populate: bool) -> OperationResult<Self> {
        // Read status file once to learn the logical length.
        let status_path = status_file(directory);
        let status_storage: TypedStorage<S, DynamicFlagsStatus> = TypedStorage::open(
            &status_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                disk_parallel: None,
                populate: Some(false),
                advice: None,
                prevent_caching: Some(true),
            },
        )?;
        let len = status_storage.read_whole()?[0].len();
        drop(status_storage);

        // Open flags file read-only.
        let flags_path = directory.join(FLAGS_FILE);
        let flags_storage = StoredBitSlice::<S>::open(
            &flags_path,
            OpenOptions {
                writeable: false,
                need_sequential: false,
                disk_parallel: None,
                populate: Some(populate),
                advice: None,
                prevent_caching: None,
            },
        )?;

        // Materialize the bitmap from the persisted bitslice.
        let bitmap = {
            let bitslice = flags_storage.read_all()?;
            // The storage file may be larger than `len` (next_power_of_two capacity).
            let trimmed = bitslice.get(..len).unwrap_or(&bitslice);
            RoaringBitmap::from_sorted_iter(trimmed.iter_ones().map(|i| i as PointOffsetType))
                .expect("iter_ones iterates in sorted order")
        };

        // The bitmap is in RAM now — drop the disk cache.
        if let Err(err) = flags_storage.clear_ram_cache() {
            log::warn!("Failed to clear bitslice cache: {err}");
        }

        Ok(Self {
            bitmap,
            len,
            flags_storage,
            directory: directory.to_owned(),
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

    fn populate(&self) -> OperationResult<()> {
        self.flags_storage.populate()?;
        Ok(())
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.flags_storage.clear_ram_cache()?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }
}
