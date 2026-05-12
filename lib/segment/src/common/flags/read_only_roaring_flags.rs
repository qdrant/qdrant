use std::path::PathBuf;

use common::stored_bitslice::StoredBitSlice;
use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{FLAGS_FILE, status_file};
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
