use std::path::Path;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::GridstoreReader;
use sparse::common::sparse_vector::SparseVector;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::sparse::SPARSE_VECTOR_DISTANCE;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::{DELETED_DIRNAME, STORAGE_DIRNAME};
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;

#[derive(Debug)]
pub struct ReadOnlySparseVectorStorage<S: UniversalRead> {
    storage: GridstoreReader<StoredSparseVector, S>,
    /// Flags marking deleted vectors.
    deleted: InMemoryBitvecFlags,
    next_point_offset: usize,
}

impl<S: UniversalRead> ReadOnlySparseVectorStorage<S> {
    /// Open the read-only counterpart of the mmap sparse storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `next_point_offset` is reconstructed like the
    /// writable storage on reopen: the highest deleted id or the Gridstore
    /// pointer count, whichever is larger.
    #[allow(dead_code)] // pending: read-only vector storage enum will use this
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

impl<S: UniversalRead> VectorStorageRead for ReadOnlySparseVectorStorage<S> {
    fn distance(&self) -> Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.next_point_offset
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key)
            .unwrap_or_else(CowVector::default_sparse)
    }

    fn read_vectors<P: AccessPattern, U: Copy>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let callback = |user_data, point_offset, sparse_vector| -> OperationResult<()> {
            let Some(sparse_vector) = sparse_vector else {
                return Ok(());
            };

            let sparse_vector = SparseVector::try_from(sparse_vector)?;
            callback(user_data, point_offset, CowVector::from(sparse_vector));
            Ok(())
        };

        self.storage
            .read_values::<P, _, _>(
                keys.into_iter(),
                callback,
                HardwareCounterCell::disposable().vector_io_read(),
            )
            .expect("sparse vectors read")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self
            .storage
            .get_value::<P>(key, &HardwareCounterCell::disposable())
        {
            Ok(Some(stored)) => SparseVector::try_from(stored).ok().map(CowVector::from),
            _ => None,
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted.count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}

#[cfg(test)]
mod tests {
    use common::generic_consts::Random;
    use common::universal_io::{MmapFile, MmapFs};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorRef;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;

    /// Write sparse vectors (deleting some) through the writable mmap storage,
    /// then reopen the same directory read-only and assert it mirrors the state,
    /// including per-point sparse contents and the reconstructed point count.
    #[test]
    fn read_only_sparse_round_trip() {
        const POINT_COUNT: PointOffsetType = 500;

        let dir = Builder::new().prefix("ro_sparse").tempdir().unwrap();
        let hw = HardwareCounterCell::disposable();

        let sparse_vectors: Vec<SparseVector> = (0..POINT_COUNT)
            .map(|id| {
                let value = id as f32;
                SparseVector {
                    indices: vec![1, 5, 9],
                    values: vec![value + 0.1, value + 0.2, value + 0.3],
                }
            })
            .collect();

        let mut deleted_ids = Vec::new();
        {
            let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();
            for (id, vector) in sparse_vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            for id in (0..POINT_COUNT).step_by(7) {
                storage.delete_vector(id).unwrap();
                deleted_ids.push(id);
            }
            storage.flusher()().unwrap();
        }

        let storage = ReadOnlySparseVectorStorage::<MmapFile>::open(&MmapFs, dir.path()).unwrap();

        assert_eq!(storage.total_vector_count(), POINT_COUNT as usize);
        assert_eq!(storage.distance(), SPARSE_VECTOR_DISTANCE);
        assert_eq!(storage.deleted_vector_count(), deleted_ids.len());

        for id in 0..POINT_COUNT {
            let deleted = deleted_ids.contains(&id);
            assert_eq!(storage.is_deleted_vector(id), deleted);

            // Deleting a sparse vector reclaims its Gridstore entry, so only
            // live points still carry their contents.
            if deleted {
                continue;
            }

            match storage.get_vector::<Random>(id) {
                CowVector::Sparse(got) => {
                    assert_eq!(got.indices, sparse_vectors[id as usize].indices);
                    assert_eq!(got.values, sparse_vectors[id as usize].values);
                }
                CowVector::Dense(_) | CowVector::MultiDense(_) => {
                    panic!("expected sparse vector for point {id}")
                }
            }
        }
    }
}
