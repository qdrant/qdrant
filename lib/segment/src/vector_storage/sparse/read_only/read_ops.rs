use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random};
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};
use gridstore::Blob;
use sparse::common::sparse_vector::SparseVector;

use super::ReadOnlySparseVectorStorage;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::sparse::SPARSE_VECTOR_DISTANCE;
use crate::vector_storage::{SparseVectorStorageRead, VectorStorageRead};

impl<S: UniversalRead> SparseVectorStorageRead for ReadOnlySparseVectorStorage<S> {
    fn get_sparse<P: AccessPattern>(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        self.get_sparse_opt::<P>(key)?
            .ok_or_else(|| OperationError::service_error(format!("Key {key} not found")))
    }

    fn get_sparse_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<SparseVector>> {
        self.storage
            .get_value::<P>(key, &HardwareCounterCell::disposable())? // Vector storage read IO not measured
            .map(SparseVector::try_from)
            .transpose()
    }

    fn for_each_in_sparse_batch<F>(
        &self,
        keys: &[PointOffsetType],
        mut callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, SparseVector),
    {
        let point_offsets = keys.iter().copied().enumerate();

        let callback = |value_idx, _, sparse_vector| {
            if let Some(sparse_vector) = sparse_vector {
                callback(value_idx, SparseVector::try_from(sparse_vector)?);
            }

            Ok(())
        };

        self.storage.read_values::<Random, _, _>(
            point_offsets,
            callback,
            HardwareCounterCell::disposable().vector_io_read(),
        )
    }
}

impl<S: UniversalRead> VectorStorageRead for ReadOnlySparseVectorStorage<S> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        unreachable!("Sparse storage does not know its total size, get from index instead")
    }

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

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
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

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        // Same batched pass as `read_vectors`, re-serializing the stored form
        // instead of decoding it into a `SparseVector`.
        self.storage.read_values::<P, _, _>(
            keys.into_iter(),
            move |user_data, point_offset, stored| {
                let Some(stored) = stored else {
                    return Ok(());
                };

                // TODO(uio): allow gridstore to return plain bytes.
                callback(user_data, point_offset, Blob::to_bytes(&stored));
                Ok(())
            },
            HardwareCounterCell::disposable().vector_io_read(),
        )
    }
}
