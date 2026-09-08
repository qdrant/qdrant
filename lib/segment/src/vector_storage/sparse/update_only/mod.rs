#[cfg(test)]
mod tests;

use std::path::Path;

use blobstore::config::{Compression, DEFAULT_PAGE_SIZE_BYTES, LogstoreConfig};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;
use sparse::common::sparse_vector::SparseVector;

use super::mmap_sparse_vector_storage::{DELETED_DIRNAME, STORAGE_DIRNAME};
use super::stored_sparse_vectors::StoredSparseVector;
use crate::common::flags::update_only_stored_flags::UpdateOnlyStoredFlags;
use crate::common::operation_error::OperationResult;
use crate::common::update_only_blobstore::UpdateOnlyBlobstore;
use crate::vector_storage::update_only::VectorToStore;

/// Storage layout for a sparse vector storage this writer creates.
///
/// Uncompressed, as the writable side is: the values are bitpacked already, and
/// compressing them again costs CPU for nothing.
const STORAGE_CONFIG: LogstoreConfig = LogstoreConfig {
    page_capacity_bytes: DEFAULT_PAGE_SIZE_BYTES,
    compression: Compression::None,
};

/// Writes what [`MmapSparseVectorStorage`] persists: the sparse vectors, and the
/// flags marking which slots hold none.
///
/// [`MmapSparseVectorStorage`]: super::mmap_sparse_vector_storage::MmapSparseVectorStorage
pub struct UpdateOnlySparseVectorStorage<S: UniversalAppend + 'static> {
    storage: UpdateOnlyBlobstore<StoredSparseVector, S>,
    deleted: UpdateOnlyStoredFlags,
}

impl<S: UniversalAppend + 'static> UpdateOnlySparseVectorStorage<S> {
    /// Open the storage at `path` for appending, creating it if it is not there
    /// yet.
    pub fn open(fs: &S::Fs, path: &Path) -> OperationResult<Self> {
        Ok(Self {
            storage: UpdateOnlyBlobstore::open(fs, &path.join(STORAGE_DIRNAME), STORAGE_CONFIG)?,
            deleted: UpdateOnlyStoredFlags::open(fs, &path.join(DELETED_DIRNAME))?,
        })
    }

    /// Append one sparse vector per point of a batch, starting at `start_slot`,
    /// and persist them.
    ///
    /// A point with no sparse vector here stores nothing — the storage is keyed
    /// by slot, so an unwritten slot is already "no vector" — and is flagged
    /// deleted, which is what the writable side records for it.
    pub fn append_many<'a>(
        &mut self,
        fs: &S::Fs,
        start_slot: PointOffsetType,
        vectors: impl IntoIterator<Item = VectorToStore<'a>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        for (offset, vector) in vectors.into_iter().enumerate() {
            let slot = start_slot + offset as PointOffsetType;
            let stored = match vector {
                VectorToStore::Decoded(vector) => {
                    let vector: &SparseVector = vector.try_into()?;
                    StoredSparseVector::from(vector)
                }
                // Validated, not trusted: these bytes may have come from a WAL
                // written by another version. Re-encoding from the decoded
                // vector is what the writable side does with them too.
                VectorToStore::Raw(bytes) => {
                    StoredSparseVector::from(&StoredSparseVector::decode_untrusted_bytes(bytes)?)
                }
                VectorToStore::Missing => {
                    self.deleted.set(slot, true);
                    continue;
                }
            };

            self.storage
                .put(fs, slot, &stored, hw_counter.ref_vector_io_write_counter())?;
        }

        self.storage.flush()?;
        self.deleted.flush(fs, hw_counter)
    }
}
