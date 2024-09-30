use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;

use super::dense::memmap_dense_vector_storage::MemmapDenseVectorStorage;
use super::dense::simple_dense_vector_storage::SimpleDenseVectorStorage;
use super::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    AppendableMmapMultiDenseVectorStorage, MultivectorMmapOffset,
};
use super::multi_dense::simple_multi_dense_vector_storage::SimpleMultiDenseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    MultiDenseVectorInternal, TypedMultiDenseVectorRef, Vector, VectorElementType,
    VectorElementTypeByte, VectorElementTypeHalf, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::dense::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage;
use crate::vector_storage::in_ram_persisted_vectors::InRamPersistedVectors;
use crate::vector_storage::simple_sparse_vector_storage::SimpleSparseVectorStorage;

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn distance(&self) -> Distance;

    fn datatype(&self) -> VectorStorageDatatype;

    fn is_on_disk(&self) -> bool;

    /// Number of vectors
    ///
    /// - includes soft deleted vectors, as they are still stored
    fn total_vector_count(&self) -> usize;

    /// Get the number of available vectors, considering deleted points and vectors
    ///
    /// This uses [`VectorStorage::total_vector_count`] and [`VectorStorage::deleted_vector_count`] internally.
    ///
    /// # Warning
    ///
    /// This number may not always be accurate. See warning in [`VectorStorage::deleted_vector_count`] documentation.
    fn available_vector_count(&self) -> usize {
        self.total_vector_count()
            .saturating_sub(self.deleted_vector_count())
    }

    fn available_size_in_bytes(&self) -> usize;

    /// Get the vector by the given key
    fn get_vector(&self, key: PointOffsetType) -> CowVector;

    /// Get the vector by the given key if it exists
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector>;

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()>;

    fn update_from<'a>(
        &mut self,
        other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;

    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    /// Flag the vector by the given key as deleted
    ///
    /// Returns true if the vector was not deleted before and is now deleted
    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool>;

    /// Check whether the vector at the given key is flagged as deleted
    fn is_deleted_vector(&self, key: PointOffsetType) -> bool;

    /// Get the number of deleted vectors, considering deleted points and vectors
    ///
    /// Vectors may be deleted at two levels, as point or as vector. Deleted points should
    /// propagate to deleting the vectors. That means that the deleted vector count includes the
    /// number of deleted points as well.
    ///
    /// This includes any vectors that were deleted at creation.
    ///
    /// # Warning
    ///
    /// In some very exceptional cases it is possible for this count not to include some deleted
    /// points. That may happen when flushing a segment to disk fails. This should be recovered
    /// when loading/recovering the segment, but that isn't guaranteed. You should therefore use
    /// the deleted count with care.
    fn deleted_vector_count(&self) -> usize;

    /// Get [`BitSlice`] representation for deleted vectors with deletion flags
    ///
    /// The size of this slice is not guaranteed. It may be smaller/larger than the number of
    /// vectors in this segment.
    fn deleted_vector_bitslice(&self) -> &BitSlice;
}

pub trait DenseVectorStorage<T: PrimitiveVectorElement>: VectorStorage {
    fn vector_dim(&self) -> usize;
    fn get_dense(&self, key: PointOffsetType) -> &[T];
}

pub trait SparseVectorStorage: VectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector>;
}

pub trait MultiVectorStorage<T: PrimitiveVectorElement>: VectorStorage {
    fn vector_dim(&self) -> usize;
    fn get_multi(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<T>;
    fn get_multi_opt(&self, key: PointOffsetType) -> Option<TypedMultiDenseVectorRef<T>>;
    fn iterate_inner_vectors(&self) -> impl Iterator<Item = &[T]> + Clone + Send;
    fn multi_vector_config(&self) -> &MultiVectorConfig;
}

#[derive(Debug)]
pub enum VectorStorageEnum {
    DenseSimple(SimpleDenseVectorStorage<VectorElementType>),
    DenseSimpleByte(SimpleDenseVectorStorage<VectorElementTypeByte>),
    DenseSimpleHalf(SimpleDenseVectorStorage<VectorElementTypeHalf>),
    DenseMemmap(Box<MemmapDenseVectorStorage<VectorElementType>>),
    DenseMemmapByte(Box<MemmapDenseVectorStorage<VectorElementTypeByte>>),
    DenseMemmapHalf(Box<MemmapDenseVectorStorage<VectorElementTypeHalf>>),
    DenseAppendableMemmap(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementType,
                ChunkedMmapVectors<VectorElementType>,
            >,
        >,
    ),
    DenseAppendableMemmapByte(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementTypeByte,
                ChunkedMmapVectors<VectorElementTypeByte>,
            >,
        >,
    ),
    DenseAppendableMemmapHalf(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementTypeHalf,
                ChunkedMmapVectors<VectorElementTypeHalf>,
            >,
        >,
    ),
    DenseAppendableInRam(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementType,
                InRamPersistedVectors<VectorElementType>,
            >,
        >,
    ),
    DenseAppendableInRamByte(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementTypeByte,
                InRamPersistedVectors<VectorElementTypeByte>,
            >,
        >,
    ),
    DenseAppendableInRamHalf(
        Box<
            AppendableMmapDenseVectorStorage<
                VectorElementTypeHalf,
                InRamPersistedVectors<VectorElementTypeHalf>,
            >,
        >,
    ),
    SparseSimple(SimpleSparseVectorStorage),
    MultiDenseSimple(SimpleMultiDenseVectorStorage<VectorElementType>),
    MultiDenseSimpleByte(SimpleMultiDenseVectorStorage<VectorElementTypeByte>),
    MultiDenseSimpleHalf(SimpleMultiDenseVectorStorage<VectorElementTypeHalf>),
    MultiDenseAppendableMemmap(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementType,
                ChunkedMmapVectors<VectorElementType>,
                ChunkedMmapVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
    MultiDenseAppendableMemmapByte(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementTypeByte,
                ChunkedMmapVectors<VectorElementTypeByte>,
                ChunkedMmapVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
    MultiDenseAppendableMemmapHalf(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementTypeHalf,
                ChunkedMmapVectors<VectorElementTypeHalf>,
                ChunkedMmapVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
    MultiDenseAppendableInRam(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementType,
                InRamPersistedVectors<VectorElementType>,
                InRamPersistedVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
    MultiDenseAppendableInRamByte(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementTypeByte,
                InRamPersistedVectors<VectorElementTypeByte>,
                InRamPersistedVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
    MultiDenseAppendableInRamHalf(
        Box<
            AppendableMmapMultiDenseVectorStorage<
                VectorElementTypeHalf,
                InRamPersistedVectors<VectorElementTypeHalf>,
                InRamPersistedVectors<MultivectorMmapOffset>,
            >,
        >,
    ),
}

impl VectorStorageEnum {
    pub fn try_multi_vector_config(&self) -> Option<&MultiVectorConfig> {
        match self {
            VectorStorageEnum::DenseSimple(_) => None,
            VectorStorageEnum::DenseSimpleByte(_) => None,
            VectorStorageEnum::DenseSimpleHalf(_) => None,
            VectorStorageEnum::DenseMemmap(_) => None,
            VectorStorageEnum::DenseMemmapByte(_) => None,
            VectorStorageEnum::DenseMemmapHalf(_) => None,
            VectorStorageEnum::DenseAppendableMemmap(_) => None,
            VectorStorageEnum::DenseAppendableMemmapByte(_) => None,
            VectorStorageEnum::DenseAppendableMemmapHalf(_) => None,
            VectorStorageEnum::DenseAppendableInRam(_) => None,
            VectorStorageEnum::DenseAppendableInRamByte(_) => None,
            VectorStorageEnum::DenseAppendableInRamHalf(_) => None,
            VectorStorageEnum::SparseSimple(_) => None,
            VectorStorageEnum::MultiDenseSimple(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseSimpleByte(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseSimpleHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmap(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableInRam(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableInRamByte(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(s) => Some(s.multi_vector_config()),
        }
    }

    pub(crate) fn default_vector(&self) -> Vector {
        match self {
            VectorStorageEnum::DenseSimple(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseSimpleByte(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseSimpleHalf(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseMemmap(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseMemmapByte(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseMemmapHalf(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseAppendableMemmap(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                Vector::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                Vector::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableInRam(v) => Vector::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseAppendableInRamByte(v) => {
                Vector::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableInRamHalf(v) => {
                Vector::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::SparseSimple(_) => Vector::from(SparseVector::default()),
            VectorStorageEnum::MultiDenseSimple(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseSimpleByte(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseSimpleHalf(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRam(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => {
                Vector::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
        }
    }
}

macro_rules! impl_vector_storage_fn {
    ($mutable: ty, $name: ident$(<$($lifetime: tt),*>)?, $target: ty, [$($arg: ident: $ty: ty),* $(,)?]) => {
        fn $name$(<$($lifetime),*>)?(self: $mutable, $($arg: $ty),*) -> $target {
            match self {
                VectorStorageEnum::DenseSimple(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseSimpleByte(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseSimpleHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseMemmap(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseMemmapByte(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseMemmapHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableMemmap(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableMemmapByte(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableInRam(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableInRamByte(v) => v.$name($($arg),*),
                VectorStorageEnum::DenseAppendableInRamHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::SparseSimple(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseSimple(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseSimpleByte(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseSimpleHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableInRam(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.$name($($arg),*),
                VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.$name($($arg),*),
            }
        }
    };
}

macro_rules! impl_vector_storage_enum {
    () => {
        impl VectorStorage for VectorStorageEnum {
            impl_vector_storage_fn!(&Self, distance, Distance, []);
            impl_vector_storage_fn!(&Self, datatype, VectorStorageDatatype, []);
            impl_vector_storage_fn!(&Self, is_on_disk, bool, []);
            impl_vector_storage_fn!(&Self, total_vector_count, usize, []);
            impl_vector_storage_fn!(&Self, available_size_in_bytes, usize, []);
            impl_vector_storage_fn!(&Self, get_vector, CowVector, [key: PointOffsetType]);
            impl_vector_storage_fn!(&Self, get_vector_opt, Option<CowVector>, [key: PointOffsetType]);
            impl_vector_storage_fn!(&mut Self, insert_vector, OperationResult<()>, [key: PointOffsetType, vector: VectorRef]);
            impl_vector_storage_fn!(&mut Self, update_from<'a>, OperationResult<Range<PointOffsetType>>, [other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>, stopped: &AtomicBool]);
            impl_vector_storage_fn!(&Self, flusher, Flusher, []);
            impl_vector_storage_fn!(&Self, files, Vec<PathBuf>, []);
            impl_vector_storage_fn!(&mut Self, delete_vector, OperationResult<bool>, [key: PointOffsetType]);
            impl_vector_storage_fn!(&Self, is_deleted_vector, bool, [key: PointOffsetType]);
            impl_vector_storage_fn!(&Self, deleted_vector_count, usize, []);
            impl_vector_storage_fn!(&Self, deleted_vector_bitslice, &BitSlice, []);
        }
    };
}
impl_vector_storage_enum! {}
