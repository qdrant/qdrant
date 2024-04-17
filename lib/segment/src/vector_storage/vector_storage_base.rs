use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use sparse::common::sparse_vector::SparseVector;

use super::dense::memmap_dense_vector_storage::MemmapDenseVectorStorage;
use super::dense::simple_dense_vector_storage::SimpleDenseVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    MultiDenseVector, VectorElementType, VectorElementTypeByte, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::dense::appendable_mmap_dense_vector_storage::AppendableMmapDenseVectorStorage;
use crate::vector_storage::simple_multi_dense_vector_storage::SimpleMultiDenseVectorStorage;
use crate::vector_storage::simple_sparse_vector_storage::SimpleSparseVectorStorage;

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;

    fn distance(&self) -> Distance;

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

    /// Get the vector by the given key
    fn get_vector(&self, key: PointOffsetType) -> CowVector;

    /// Get the vector by the given key if it exists
    /// Blanket implementation - override if necessary
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        Some(self.get_vector(key))
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()>;

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
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
    fn get_dense(&self, key: PointOffsetType) -> &[T];
}

pub trait SparseVectorStorage: VectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector>;
}

pub trait MultiVectorStorage: VectorStorage {
    fn get_multi(&self, key: PointOffsetType) -> &MultiDenseVector;
    fn multi_vector_config(&self) -> &MultiVectorConfig;
}

pub enum VectorStorageEnum {
    DenseSimple(SimpleDenseVectorStorage<VectorElementType>),
    DenseSimpleByte(SimpleDenseVectorStorage<VectorElementTypeByte>),
    DenseMemmap(Box<MemmapDenseVectorStorage<VectorElementType>>),
    DenseMemmapByte(Box<MemmapDenseVectorStorage<VectorElementTypeByte>>),
    DenseAppendableMemmap(Box<AppendableMmapDenseVectorStorage<VectorElementType>>),
    DenseAppendableMemmapByte(Box<AppendableMmapDenseVectorStorage<VectorElementTypeByte>>),
    SparseSimple(SimpleSparseVectorStorage),
    MultiDenseSimple(SimpleMultiDenseVectorStorage),
}

impl VectorStorage for VectorStorageEnum {
    fn vector_dim(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.vector_dim(),
            VectorStorageEnum::DenseSimpleByte(v) => v.vector_dim(),
            VectorStorageEnum::DenseMemmap(v) => v.vector_dim(),
            VectorStorageEnum::DenseMemmapByte(v) => v.vector_dim(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.vector_dim(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.vector_dim(),
            VectorStorageEnum::SparseSimple(v) => v.vector_dim(),
            VectorStorageEnum::MultiDenseSimple(v) => v.vector_dim(),
        }
    }

    fn distance(&self) -> Distance {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.distance(),
            VectorStorageEnum::DenseSimpleByte(v) => v.distance(),
            VectorStorageEnum::DenseMemmap(v) => v.distance(),
            VectorStorageEnum::DenseMemmapByte(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.distance(),
            VectorStorageEnum::SparseSimple(v) => v.distance(),
            VectorStorageEnum::MultiDenseSimple(v) => v.distance(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.is_on_disk(),
            VectorStorageEnum::DenseSimpleByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::SparseSimple(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseSimple(v) => v.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.total_vector_count(),
            VectorStorageEnum::DenseSimpleByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::SparseSimple(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseSimple(v) => v.total_vector_count(),
        }
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.get_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.get_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.get_vector(key),
        }
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector_opt(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector_opt(key),
            VectorStorageEnum::SparseSimple(v) => v.get_vector_opt(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.get_vector_opt(key),
        }
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseSimpleByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseMemmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseMemmapByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.insert_vector(key, vector),
            VectorStorageEnum::SparseSimple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::MultiDenseSimple(v) => v.insert_vector(key, vector),
        }
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::DenseSimpleByte(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::DenseMemmap(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::DenseMemmapByte(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.update_from(other, other_ids, stopped)
            }
            VectorStorageEnum::SparseSimple(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::MultiDenseSimple(v) => v.update_from(other, other_ids, stopped),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.flusher(),
            VectorStorageEnum::DenseSimpleByte(v) => v.flusher(),
            VectorStorageEnum::DenseMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseMemmapByte(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.flusher(),
            VectorStorageEnum::SparseSimple(v) => v.flusher(),
            VectorStorageEnum::MultiDenseSimple(v) => v.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.files(),
            VectorStorageEnum::DenseSimpleByte(v) => v.files(),
            VectorStorageEnum::DenseMemmap(v) => v.files(),
            VectorStorageEnum::DenseMemmapByte(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.files(),
            VectorStorageEnum::SparseSimple(v) => v.files(),
            VectorStorageEnum::MultiDenseSimple(v) => v.files(),
        }
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.delete_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.delete_vector(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseSimpleByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::SparseSimple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseSimple(v) => v.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseSimpleByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::SparseSimple(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseSimple(v) => v.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageEnum::DenseSimple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseSimpleByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::SparseSimple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseSimple(v) => v.deleted_vector_bitslice(),
        }
    }
}
