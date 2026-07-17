use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::ReadOnlyChunkedDenseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::{DenseVectorStorageRead, VectorOffsetType, VectorStorageRead};

impl<T: PrimitiveVectorElement, S: UniversalRead> DenseVectorStorageRead<T>
    for ReadOnlyChunkedDenseVectorStorage<T, S>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .expect("vector not found")
    }

    fn read_dense_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> crate::common::operation_error::OperationResult<()> {
        let (user_datas, keys): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
        self.vectors.for_each_in_batch(&keys, |idx, dense| {
            let user_data = user_datas[idx];
            let key = keys[idx];
            callback(user_data, key, bytemuck::cast_slice(dense).to_vec());
        })
    }

    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        callback: F,
    ) -> OperationResult<()> {
        self.vectors.for_each_in_batch(keys, callback)
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorageRead
    for ReadOnlyChunkedDenseVectorStorage<T, S>
{
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
        self.vectors.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
            .expect("Vector not found")
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let keys = keys
            .into_iter()
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset, 1));

        self.vectors
            .for_each_vector::<P, _>(keys, |(user_data, point_offset), vector| {
                let vector = CowVector::from(T::slice_to_float_cow(vector));
                callback(user_data, point_offset, vector);
                Ok(())
            })
            .expect("read vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
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
