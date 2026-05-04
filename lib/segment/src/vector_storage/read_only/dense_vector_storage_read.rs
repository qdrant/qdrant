use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};

use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::dense::dense_vector_storage::DenseVectorStorageImpl;
use crate::vector_storage::{DenseVectorStorage, VectorStorageRead};

/// Read-only newtype wrapper around [`DenseVectorStorageImpl`].
///
/// Exposes only [`VectorStorageRead`] and [`DenseVectorStorage`].
pub struct ReadOnlyDenseVectorStorage<T, S = MmapFile>(DenseVectorStorageImpl<T, S>)
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>;

impl<T, S> DenseVectorStorage<T> for ReadOnlyDenseVectorStorage<T, S>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
    fn vector_dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.0.get_dense::<P>(key)
    }

    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(&self, keys: &[PointOffsetType], f: F) {
        self.0.for_each_in_dense_batch(keys, f);
    }
}

impl<T, S> VectorStorageRead for ReadOnlyDenseVectorStorage<T, S>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
    fn distance(&self) -> Distance {
        self.0.distance()
    }

    fn datatype(&self) -> VectorStorageDatatype {
        self.0.datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.0.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.0.total_vector_count()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.0.get_vector::<P>(key)
    }

    fn read_vectors<P: AccessPattern>(
        &self,
        keys: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, CowVector<'_>),
    ) {
        self.0.read_vectors::<P>(keys, callback);
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.0.get_vector_opt::<P>(key)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.0.is_deleted_vector(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.0.deleted_vector_count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.0.deleted_vector_bitslice()
    }
}
