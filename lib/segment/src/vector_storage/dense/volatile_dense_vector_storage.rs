use std::borrow::Cow;
use std::ops::Range;
use std::sync::atomic::AtomicBool;

use common::bitvec::{BitSlice, BitSliceExt as _, BitVec, bitvec_set_deleted};
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use crate::common::Flusher;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::volatile_chunked_vectors::VolatileChunkedVectors;
use crate::vector_storage::{
    DenseVectorStorage, DenseVectorStorageRead, VectorOffsetType, VectorStorage, VectorStorageEnum,
    VectorStorageRead, default_read_vector_bytes_impl,
};

/// In-memory vector storage that is volatile
///
/// This storage is not persisted and intended for temporary use in tests.
#[derive(Debug)]
pub struct VolatileDenseVectorStorage<T: PrimitiveVectorElement> {
    dim: usize,
    distance: Distance,
    vectors: VolatileChunkedVectors<T>,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

pub fn new_volatile_dense_vector_storage(dim: usize, distance: Distance) -> VectorStorageEnum {
    VectorStorageEnum::DenseVolatile(VolatileDenseVectorStorage::new(dim, distance))
}

#[cfg(test)]
pub fn new_volatile_dense_byte_vector_storage(dim: usize, distance: Distance) -> VectorStorageEnum {
    VectorStorageEnum::DenseVolatileByte(VolatileDenseVectorStorage::new(dim, distance))
}

#[cfg(test)]
pub fn new_volatile_dense_half_vector_storage(dim: usize, distance: Distance) -> VectorStorageEnum {
    VectorStorageEnum::DenseVolatileHalf(VolatileDenseVectorStorage::new(dim, distance))
}

impl<T: PrimitiveVectorElement> VolatileDenseVectorStorage<T> {
    pub fn new(dim: usize, distance: Distance) -> Self {
        Self {
            dim,
            distance,
            vectors: VolatileChunkedVectors::new(dim),
            deleted: BitVec::new(),
            deleted_count: 0,
        }
    }

    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && key as usize >= self.vectors.len() {
            return false;
        }
        let was_deleted = bitvec_set_deleted(&mut self.deleted, key, deleted);
        if was_deleted != deleted {
            if !was_deleted {
                self.deleted_count += 1;
            } else {
                self.deleted_count = self.deleted_count.saturating_sub(1);
            }
        }
        was_deleted
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorageRead<T> for VolatileDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        Cow::Borrowed(self.vectors.get(key as VectorOffsetType))
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorage<T> for VolatileDenseVectorStorage<T> {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [T]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            let new_id = self.vectors.push(other_vector.as_ref())? as PointOffsetType;
            self.set_deleted(new_id, other_deleted);
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }
}

impl<T: PrimitiveVectorElement> VectorStorageRead for VolatileDenseVectorStorage<T> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<T>()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    /// Get vector by key, if it exists.
    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        // In memory so no optimization to be done for access pattern
        self.vectors
            .get_opt(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice.into())))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get_bit(key as usize).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        default_read_vector_bytes_impl::<Self, P, U>(self, keys, callback)
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for VolatileDenseVectorStorage<T> {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let vector: &[VectorElementType] = vector.try_into()?;
        let vector = T::slice_from_float_cow(Cow::from(vector));
        self.vectors
            .insert(key as VectorOffsetType, vector.as_ref())?;
        self.set_deleted(key, false);
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        let is_deleted = !self.set_deleted(key, true);
        Ok(is_deleted)
    }
}
