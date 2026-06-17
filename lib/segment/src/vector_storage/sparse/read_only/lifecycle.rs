use std::path::Path;

use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;

use super::ReadOnlySparseVectorStorage;
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::sparse::mmap_sparse_vector_storage::{DELETED_DIRNAME, STORAGE_DIRNAME};
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;

impl<S: UniversalRead> ReadOnlySparseVectorStorage<S> {
    /// Open the read-only counterpart of the mmap sparse storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `next_point_offset` is reconstructed like the
    /// writable storage on reopen: the highest deleted id or the Gridstore
    /// pointer count, whichever is larger.
    pub fn open(fs: &S::Fs, path: &Path) -> OperationResult<Self> {
        let storage =
            GridstoreReader::<StoredSparseVector, S>::open(fs, path.join(STORAGE_DIRNAME))
                .map_err(|err| {
                    OperationError::service_error(format!(
                        "Failed to open read-only sparse vector storage: {err}"
                    ))
                })?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_DIRNAME))?;

        let next_point_offset = deleted
            .as_bitslice()
            .last_one()
            .max(Some(storage.max_point_offset() as usize))
            .unwrap_or_default();

        Ok(Self {
            storage,
            deleted,
            next_point_offset,
        })
    }
}
