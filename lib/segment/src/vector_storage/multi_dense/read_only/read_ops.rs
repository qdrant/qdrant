use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::{ForkingHwCounter, HardwareCounterCell};
use common::generic_consts::{AccessPattern, Sequential};
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::TypedMultiDenseVectorRef;
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    flattened_to_multi_vector, read_multi_vector,
};
use crate::vector_storage::{MultiVectorStorageRead, VectorOffsetType, VectorStorageRead};

impl<T: PrimitiveVectorElement, S: UniversalRead> ReadOnlyChunkedMultiDenseVectorStorage<T, S> {
    fn for_each_flat_multi<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, &[T]),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let point_offsets = keys
            .into_iter()
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset));

        super::for_each_vector::<P, _, _, _>(
            &self.offsets,
            &self.vectors,
            point_offsets,
            |(user_data, point_offset), flattened| {
                callback(user_data, point_offset, flattened);
                Ok(())
            },
            hw_counter,
        )
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> MultiVectorStorageRead<T>
    for ReadOnlyChunkedMultiDenseVectorStorage<T, S>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_multi<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> CowMultiVector<'_, T> {
        self.get_multi_opt::<P>(key, hw_counter)
            .expect("vector not found")
    }

    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CowMultiVector<'_, T>> {
        read_multi_vector::<T, P, _>(&self.offsets, &self.vectors, key, hw_counter)
    }

    fn for_each_in_batch_multi<F>(
        &self,
        keys: &[PointOffsetType],
        mut callback: F,
        hw_counter: &HardwareCounterCell,
    ) where
        F: FnMut(usize, TypedMultiDenseVectorRef<'_, T>),
    {
        let point_offsets = keys.iter().copied().enumerate();

        super::for_each_vector::<Sequential, _, _, _>(
            &self.offsets,
            &self.vectors,
            point_offsets,
            |index, flattened| {
                let vector = TypedMultiDenseVectorRef::new(flattened, self.vector_dim());
                callback(index, vector);
                Ok(())
            },
            hw_counter,
        )
        .expect("read vectors");
    }

    fn iterate_inner_vectors(
        &self,
        hw_counter: ForkingHwCounter,
    ) -> impl Iterator<Item = Cow<'_, [T]>> + Clone + Send {
        let total_vector_count = self.total_vector_count(&hw_counter);
        (0..total_vector_count).flat_map(move |key| {
            let mmap_offset = self
                .offsets
                .get::<Sequential>(key as VectorOffsetType, &hw_counter)
                .unwrap()
                .first()
                .copied()
                .unwrap();
            let hw_counter = hw_counter.clone();
            (0..mmap_offset.count).map(move |i| {
                self.vectors
                    .get::<Sequential>((mmap_offset.offset + i) as VectorOffsetType, &hw_counter)
                    .unwrap()
            })
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }
}

impl<T: PrimitiveVectorElement, S: UniversalRead> VectorStorageRead
    for ReadOnlyChunkedMultiDenseVectorStorage<T, S>
{
    fn size_of_available_vectors_in_bytes(&self, _hw_counter: &HardwareCounterCell) -> usize {
        // Total flattened element bytes across all stored multi-vectors.
        self.vectors.len() * self.vectors.dim() * std::mem::size_of::<T>()
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

    fn total_vector_count(&self, _hw_counter: &HardwareCounterCell) -> usize {
        self.offsets.len()
    }

    fn get_vector<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> CowVector<'_> {
        self.get_vector_opt::<P>(key, hw_counter)
            .expect("vector not found")
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
        hw_counter: &HardwareCounterCell,
    ) {
        self.for_each_flat_multi::<P, U>(
            keys,
            |user_data, point_offset, flattened| {
                let vector = CowVector::MultiDense(T::into_float_multivector(
                    flattened_to_multi_vector(Cow::Borrowed(flattened), self.vectors.dim()),
                ));

                callback(user_data, point_offset, vector);
            },
            hw_counter,
        )
        .expect("read vectors");
    }

    fn get_vector_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CowVector<'_>> {
        read_multi_vector::<T, P, _>(&self.offsets, &self.vectors, key, hw_counter)
            .map(|multi| CowVector::MultiDense(T::into_float_multivector(multi)))
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.for_each_flat_multi::<P, U>(
            keys,
            |user_data, point_offset, flattened| {
                callback(
                    user_data,
                    point_offset,
                    bytemuck::cast_slice(flattened).to_vec(),
                );
            },
            hw_counter,
        )
    }
}
