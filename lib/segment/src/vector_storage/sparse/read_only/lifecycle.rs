use std::path::Path;

use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};
use gridstore::BlobstoreReader;

use super::ReadOnlySparseVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::sparse::mmap_sparse_vector_storage::{DELETED_DIRNAME, STORAGE_DIRNAME};
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;

impl<S: UniversalRead> ReadOnlySparseVectorStorage<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// `populate` decides whether the vector data is warmed: search doesn't
    /// read the sparse storage (`Populate::No` suffices), but a mutable-RAM
    /// sparse index is rebuilt from it at open, reading it in full — the
    /// caller warms it then.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        // Blobstore reader
        BlobstoreReader::<StoredSparseVector, S>::preopen(fs, path.join(STORAGE_DIRNAME), populate)
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to preopen read-only sparse vector storage: {err}"
                ))
            })?;

        // Deleted flags
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_DIRNAME))?;

        Ok(())
    }

    /// Open the read-only counterpart of the mmap sparse storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `populate` mirrors [`Self::preopen`].
    /// `next_point_offset` is reconstructed like the
    /// writable storage on reopen: the highest deleted id or the Blobstore
    /// pointer count, whichever is larger.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<Self> {
        let storage = BlobstoreReader::<StoredSparseVector, S>::open(
            fs,
            path.join(STORAGE_DIRNAME),
            populate,
        )
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to open read-only sparse vector storage: {err}"
            ))
        })?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIRNAME))?;

        let next_point_offset = deleted
            .as_bitslice()
            .last_one()
            .map(|i| i + 1)
            .max(Some(storage.max_point_offset()? as usize))
            .unwrap_or_default();

        Ok(Self {
            storage,
            deleted,
            next_point_offset,
        })
    }
}
