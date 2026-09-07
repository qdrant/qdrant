use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::ReadOnlyImmutableDenseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::dense::dense_vectors::DenseVectorBlob;
use crate::vector_storage::{DenseVectorStorageRead, VectorStorageRead};

impl<B: DenseVectorBlob> DenseVectorStorageRead<B::Element>
    for ReadOnlyImmutableDenseVectorStorage<B>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [B::Element]> {
        self.vectors
            .get_vector_opt::<P>(key)
            .expect("vector not found")
    }

    fn read_dense_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        let (user_data, keys): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
        self.vectors.for_each_in_batch(&keys, |idx, dense| {
            let user_data = user_data[idx];
            let key = keys[idx];
            callback(user_data, key, bytemuck::cast_slice(dense).to_vec());
        })
    }

    fn for_each_in_dense_batch<F: FnMut(usize, &[B::Element])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.vectors.for_each_in_batch(keys, f)
    }
}

impl<B: DenseVectorBlob> VectorStorageRead for ReadOnlyImmutableDenseVectorStorage<B> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<B::Element>()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        B::Element::datatype()
    }

    fn is_on_disk(&self) -> bool {
        !self.populate.to_bool::<B::File>()
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.num_vectors()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.vectors
            .get_vector_opt::<P>(key)
            .map(|vector| CowVector::from(B::Element::slice_to_float_cow(vector)))
            .expect("Vector not found")
    }

    fn read_vectors<P: AccessPattern, U: Copy>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        // Split into parallel arrays in one pass: `for_each_in_batch` needs an
        // offsets slice, but we still want `user_data[idx]` in the callback.
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.vectors
            .for_each_in_batch(&point_offsets, |idx, vector| {
                let vector = CowVector::from(B::Element::slice_to_float_cow(Cow::Borrowed(vector)));
                callback(user_data[idx], point_offsets[idx], vector);
            })
            .expect("read vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.vectors
            .get_vector_opt::<P>(key)
            .map(|vector| CowVector::from(B::Element::slice_to_float_cow(vector)))
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        self.read_dense_bytes::<P, U>(keys, callback)
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
