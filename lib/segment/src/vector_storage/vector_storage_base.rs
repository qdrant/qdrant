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
use super::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf, VectorInternal, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::dense::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage;
use crate::vector_storage::in_ram_persisted_vectors::InRamPersistedVectors;
use crate::vector_storage::sparse::simple_sparse_vector_storage::SimpleSparseVectorStorage;

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

    fn size_of_available_vectors_in_bytes(&self) -> usize;

    /// Get the vector by the given key
    fn get_vector(&self, key: PointOffsetType) -> CowVector;

    /// Get the vector by the given key if it exists
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector>;

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()>;

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
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

    /// Get the dense vectors by the given keys
    ///
    /// Implementation can assume that the keys are consecutive
    fn get_dense_batch<'a>(&'a self, keys: &[PointOffsetType], vectors: &mut [&'a [T]]) {
        for (idx, key) in keys.iter().enumerate() {
            vectors[idx] = self.get_dense(*key);
        }
    }
}

pub trait SparseVectorStorage: VectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector>;
    fn get_sparse_opt(&self, key: PointOffsetType) -> OperationResult<Option<SparseVector>>;
}

pub trait MultiVectorStorage<T: PrimitiveVectorElement>: VectorStorage {
    fn vector_dim(&self) -> usize;
    fn get_multi(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<T>;
    fn get_multi_opt(&self, key: PointOffsetType) -> Option<TypedMultiDenseVectorRef<T>>;
    fn get_batch_multi<'a>(
        &'a self,
        keys: &[PointOffsetType],
        vectors: &mut [TypedMultiDenseVectorRef<'a, T>],
    );
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
    SparseMmap(MmapSparseVectorStorage),
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
            VectorStorageEnum::SparseMmap(_) => None,
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

    pub(crate) fn default_vector(&self) -> VectorInternal {
        match self {
            VectorStorageEnum::DenseSimple(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseSimpleByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseSimpleHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseMemmap(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseMemmapByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseMemmapHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmap(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableInRam(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableInRamByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableInRamHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::SparseSimple(_) => VectorInternal::from(SparseVector::default()),
            VectorStorageEnum::SparseMmap(_) => VectorInternal::from(SparseVector::default()),
            VectorStorageEnum::MultiDenseSimple(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseSimpleByte(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseSimpleHalf(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRam(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
        }
    }
}

impl VectorStorage for VectorStorageEnum {
    fn distance(&self) -> Distance {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.distance(),
            VectorStorageEnum::DenseSimpleByte(v) => v.distance(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.distance(),
            VectorStorageEnum::DenseMemmap(v) => v.distance(),
            VectorStorageEnum::DenseMemmapByte(v) => v.distance(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.distance(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.distance(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.distance(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.distance(),
            VectorStorageEnum::SparseSimple(v) => v.distance(),
            VectorStorageEnum::SparseMmap(v) => v.distance(),
            VectorStorageEnum::MultiDenseSimple(v) => v.distance(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.distance(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.distance(),
        }
    }

    fn datatype(&self) -> VectorStorageDatatype {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.datatype(),
            VectorStorageEnum::DenseSimpleByte(v) => v.datatype(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.datatype(),
            VectorStorageEnum::DenseMemmap(v) => v.datatype(),
            VectorStorageEnum::DenseMemmapByte(v) => v.datatype(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.datatype(),
            VectorStorageEnum::SparseSimple(v) => v.datatype(),
            VectorStorageEnum::SparseMmap(v) => v.datatype(),
            VectorStorageEnum::MultiDenseSimple(v) => v.datatype(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.datatype(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.datatype(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.is_on_disk(),
            VectorStorageEnum::DenseSimpleByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.is_on_disk(),
            VectorStorageEnum::SparseSimple(v) => v.is_on_disk(),
            VectorStorageEnum::SparseMmap(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseSimple(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.total_vector_count(),
            VectorStorageEnum::DenseSimpleByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.total_vector_count(),
            VectorStorageEnum::SparseSimple(v) => v.total_vector_count(),
            VectorStorageEnum::SparseMmap(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseSimple(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.total_vector_count(),
        }
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseSimpleByte(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmap(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmapByte(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::DenseAppendableInRam(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::DenseAppendableInRamHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::SparseSimple(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::SparseMmap(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::MultiDenseSimple(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableInRam(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
        }
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.get_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseSimpleHalf(v) => v.get_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableInRam(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.get_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.get_vector(key),
            VectorStorageEnum::SparseMmap(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.get_vector(key),
        }
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseSimpleHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableInRam(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::SparseSimple(v) => v.get_vector_opt(key),
            VectorStorageEnum::SparseMmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.get_vector_opt(key),
        }
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseSimpleByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseSimpleHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseMemmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseMemmapByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseMemmapHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableInRam(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::SparseSimple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::SparseMmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseSimple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.insert_vector(key, vector),
        }
    }

    fn update_from<'a>(
        &mut self,
        other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseSimpleByte(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseSimpleHalf(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseMemmap(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseMemmapByte(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseMemmapHalf(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableInRam(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::SparseSimple(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::SparseMmap(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseSimple(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.update_from(other_ids, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.update_from(other_ids, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.update_from(other_ids, stopped),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => {
                v.update_from(other_ids, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => {
                v.update_from(other_ids, stopped)
            }
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.flusher(),
            VectorStorageEnum::DenseSimpleByte(v) => v.flusher(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.flusher(),
            VectorStorageEnum::DenseMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseMemmapByte(v) => v.flusher(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.flusher(),
            VectorStorageEnum::SparseSimple(v) => v.flusher(),
            VectorStorageEnum::SparseMmap(v) => v.flusher(),
            VectorStorageEnum::MultiDenseSimple(v) => v.flusher(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.flusher(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.files(),
            VectorStorageEnum::DenseSimpleByte(v) => v.files(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.files(),
            VectorStorageEnum::DenseMemmap(v) => v.files(),
            VectorStorageEnum::DenseMemmapByte(v) => v.files(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.files(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.files(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.files(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.files(),
            VectorStorageEnum::SparseSimple(v) => v.files(),
            VectorStorageEnum::SparseMmap(v) => v.files(),
            VectorStorageEnum::MultiDenseSimple(v) => v.files(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.files(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.files(),
        }
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.delete_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseSimpleHalf(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableInRam(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.delete_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.delete_vector(key),
            VectorStorageEnum::SparseMmap(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.delete_vector(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseSimpleHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableInRam(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::SparseMmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseSimpleByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::SparseSimple(v) => v.deleted_vector_count(),
            VectorStorageEnum::SparseMmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseSimple(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseSimpleByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseSimpleHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableInRam(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableInRamByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableInRamHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::SparseSimple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::SparseMmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseSimple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseSimpleByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseSimpleHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableInRam(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableInRamByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableInRamHalf(v) => v.deleted_vector_bitslice(),
        }
    }
}
