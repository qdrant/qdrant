use std::alloc::Layout;
use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random};
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use sparse::common::sparse_vector::SparseVector;

use super::dense::dense_vector_storage::DenseVectorStorageImpl;
use super::dense::empty_dense_vector_storage::EmptyDenseVectorStorage;
use super::dense::volatile_dense_vector_storage::VolatileDenseVectorStorage;
use super::multi_dense::appendable_mmap_multi_dense_vector_storage::AppendableMmapMultiDenseVectorStorage;
use super::multi_dense::volatile_multi_dense_vector_storage::VolatileMultiDenseVectorStorage;
use super::sparse::empty_sparse_vector_storage::EmptySparseVectorStorage;
use super::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use super::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf, VectorInternal, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::dense::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage;

/// In case of simple vector storage, vector offset is the same as [`PointOffsetType`].
/// But in case of multivectors, it requires an additional lookup.
pub type VectorOffsetType = usize;

/// Generalized vector offset.
pub trait VectorOffset: Copy + fmt::Display + fmt::Debug {
    fn offset(self) -> VectorOffsetType;
}

impl VectorOffset for PointOffsetType {
    fn offset(self) -> VectorOffsetType {
        self as VectorOffsetType
    }
}

impl VectorOffset for VectorOffsetType {
    fn offset(self) -> VectorOffsetType {
        self
    }
}

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

    /// Get the vector by the given key
    /// Get the vector by the given key with potential optimizations for sequential reads.
    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_>;

    /// Get multiple vectors by the given keys
    /// Potentially optimized for internal parallel reads.
    fn read_vectors<P: AccessPattern>(
        &self,
        keys: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(PointOffsetType, CowVector<'_>),
    ) {
        for key in keys {
            callback(key, self.get_vector::<P>(key));
        }
    }

    /// Get the vector by the given key if it exists
    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>>;

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Add the given vectors to the storage.
    ///
    /// # Returns
    /// The range of point offsets that were added to the storage.
    ///
    /// If stopped, the operation returns a cancellation error.
    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;

    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

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

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]>;

    /// Call `f` with the raw bytes of the vector if it exists.
    ///
    /// Uses `bytemuck::cast_slice` on the borrowed data — zero copy, zero allocation.
    fn with_dense_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        ((key as usize) < self.total_vector_count()).then(|| {
            let dense = self.get_dense::<P>(key);
            f(bytemuck::cast_slice(&dense))
        })
    }

    /// Get layout for a single vector
    fn get_dense_vector_layout(&self) -> OperationResult<Layout> {
        Layout::array::<T>(self.vector_dim())
            .map_err(|_| OperationError::service_error("Layout is too big"))
    }

    /// Run given function for each vector in the dense batch.
    ///
    /// Implementation can assume that the keys are consecutive
    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(&self, keys: &[PointOffsetType], mut f: F) {
        for (idx, &key) in keys.iter().enumerate() {
            f(idx, &self.get_dense::<Random>(key));
        }
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<T>()
    }
}

pub trait SparseVectorStorage: VectorStorage {
    fn get_sparse<P: AccessPattern>(&self, key: PointOffsetType) -> OperationResult<SparseVector>;
    fn get_sparse_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<SparseVector>>;
}

pub trait MultiVectorStorage<T: PrimitiveVectorElement>: VectorStorage {
    fn vector_dim(&self) -> usize;

    fn get_multi<P: AccessPattern>(&self, key: PointOffsetType) -> CowMultiVector<'_, T>;
    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<CowMultiVector<'_, T>>;

    fn for_each_in_batch_multi<F>(&self, keys: &[PointOffsetType], callback: F)
    where
        F: FnMut(usize, TypedMultiDenseVectorRef<'_, T>);

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = Cow<'_, [T]>> + Clone + Send;
    fn multi_vector_config(&self) -> &MultiVectorConfig;

    fn size_of_available_vectors_in_bytes(&self) -> usize;
}

#[derive(Debug)]
pub enum VectorStorageEnum {
    DenseVolatile(VolatileDenseVectorStorage<VectorElementType>),
    #[cfg(test)]
    DenseVolatileByte(VolatileDenseVectorStorage<VectorElementTypeByte>),
    #[cfg(test)]
    DenseVolatileHalf(VolatileDenseVectorStorage<VectorElementTypeHalf>),

    DenseMemmap(Box<DenseVectorStorageImpl<VectorElementType>>),
    DenseMemmapByte(Box<DenseVectorStorageImpl<VectorElementTypeByte>>),
    DenseMemmapHalf(Box<DenseVectorStorageImpl<VectorElementTypeHalf>>),

    #[cfg(target_os = "linux")]
    DenseUring(Box<DenseVectorStorageImpl<VectorElementType, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringByte(Box<DenseVectorStorageImpl<VectorElementTypeByte, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringHalf(Box<DenseVectorStorageImpl<VectorElementTypeHalf, IoUringFile>>),

    DenseAppendableMemmap(Box<AppendableMmapDenseVectorStorage<VectorElementType>>),
    DenseAppendableMemmapByte(Box<AppendableMmapDenseVectorStorage<VectorElementTypeByte>>),
    DenseAppendableMemmapHalf(Box<AppendableMmapDenseVectorStorage<VectorElementTypeHalf>>),
    SparseVolatile(VolatileSparseVectorStorage),
    SparseMmap(MmapSparseVectorStorage),
    MultiDenseVolatile(VolatileMultiDenseVectorStorage<VectorElementType>),
    #[cfg(test)]
    MultiDenseVolatileByte(VolatileMultiDenseVectorStorage<VectorElementTypeByte>),
    #[cfg(test)]
    MultiDenseVolatileHalf(VolatileMultiDenseVectorStorage<VectorElementTypeHalf>),
    MultiDenseAppendableMemmap(Box<AppendableMmapMultiDenseVectorStorage<VectorElementType>>),
    MultiDenseAppendableMemmapByte(
        Box<AppendableMmapMultiDenseVectorStorage<VectorElementTypeByte>>,
    ),
    MultiDenseAppendableMemmapHalf(
        Box<AppendableMmapMultiDenseVectorStorage<VectorElementTypeHalf>>,
    ),
    EmptyDense(EmptyDenseVectorStorage),
    EmptySparse(EmptySparseVectorStorage),
}

impl VectorStorageEnum {
    pub fn try_multi_vector_config(&self) -> Option<&MultiVectorConfig> {
        match self {
            VectorStorageEnum::DenseVolatile(_) => None,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_) => None,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(_) => None,
            VectorStorageEnum::DenseMemmap(_) => None,
            VectorStorageEnum::DenseMemmapByte(_) => None,
            VectorStorageEnum::DenseMemmapHalf(_) => None,

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_) => None,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(_) => None,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(_) => None,

            VectorStorageEnum::DenseAppendableMemmap(_) => None,
            VectorStorageEnum::DenseAppendableMemmapByte(_) => None,
            VectorStorageEnum::DenseAppendableMemmapHalf(_) => None,
            VectorStorageEnum::SparseVolatile(_) => None,
            VectorStorageEnum::SparseMmap(_) => None,
            VectorStorageEnum::MultiDenseVolatile(s) => Some(s.multi_vector_config()),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(s) => Some(s.multi_vector_config()),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmap(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::EmptyDense(s) => s.multi_vector_config(),
            VectorStorageEnum::EmptySparse(_) => None,
        }
    }

    pub(crate) fn default_vector(&self) -> VectorInternal {
        match self {
            VectorStorageEnum::DenseVolatile(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseMemmap(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::DenseMemmapByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseMemmapHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),

            VectorStorageEnum::DenseAppendableMemmap(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::SparseVolatile(_) => VectorInternal::from(SparseVector::default()),
            VectorStorageEnum::SparseMmap(_) => VectorInternal::from(SparseVector::default()),
            VectorStorageEnum::MultiDenseVolatile(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
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
            VectorStorageEnum::EmptyDense(v) => VectorInternal::from(vec![1.0; v.vector_dim()]),
            VectorStorageEnum::EmptySparse(_) => VectorInternal::from(SparseVector::default()),
        }
    }

    pub fn size_of_available_vectors_in_bytes(&self) -> usize {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmap(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmapByte(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.size_of_available_vectors_in_bytes(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.size_of_available_vectors_in_bytes(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::SparseVolatile(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::SparseMmap(_v) => {
                unreachable!(
                    "Mmap sparse storage does not know its total size, get from index instead"
                )
            }
            VectorStorageEnum::MultiDenseVolatile(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.size_of_available_vectors_in_bytes(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.size_of_available_vectors_in_bytes(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.size_of_available_vectors_in_bytes()
            }
            VectorStorageEnum::EmptyDense(_) => 0,
            VectorStorageEnum::EmptySparse(_) => 0,
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseVolatile(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::DenseMemmap(vs) => vs.populate(),
            VectorStorageEnum::DenseMemmapByte(vs) => vs.populate(),
            VectorStorageEnum::DenseMemmapHalf(vs) => vs.populate(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(vs) => vs.populate(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(vs) => vs.populate(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(vs) => vs.populate(),

            VectorStorageEnum::DenseAppendableMemmap(vs) => vs.populate()?,
            VectorStorageEnum::DenseAppendableMemmapByte(vs) => vs.populate()?,
            VectorStorageEnum::DenseAppendableMemmapHalf(vs) => vs.populate()?,
            VectorStorageEnum::SparseVolatile(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::SparseMmap(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseVolatile(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::MultiDenseAppendableMemmap(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => vs.populate()?,
            VectorStorageEnum::EmptyDense(_) => {}
            VectorStorageEnum::EmptySparse(_) => {}
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseVolatile(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::DenseMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseMemmapHalf(vs) => vs.clear_cache()?,

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(vs) => vs.clear_cache()?,

            VectorStorageEnum::DenseAppendableMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseAppendableMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseAppendableMemmapHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::SparseVolatile(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::SparseMmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseVolatile(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_) => {} // Can't populate as it is not mmap
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(_) => {} // Can't populate as it is not mmap
            VectorStorageEnum::MultiDenseAppendableMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::EmptyDense(_) => {}
            VectorStorageEnum::EmptySparse(_) => {}
        }
        Ok(())
    }

    /// Call `f` with the raw bytes of the vector if it exists.
    pub fn with_vector_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::DenseMemmap(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::DenseMemmapByte(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::DenseMemmapHalf(v) => v.with_dense_bytes_opt::<P, R>(key, f),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.with_dense_bytes_opt::<P, R>(key, f),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.with_dense_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.with_dense_bytes_opt::<P, R>(key, f)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                v.with_dense_bytes_opt::<P, R>(key, f)
            }
            VectorStorageEnum::SparseVolatile(_) => None,
            VectorStorageEnum::SparseMmap(_) => None,
            VectorStorageEnum::MultiDenseVolatile(_) => None,
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_) => None,
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(_) => None,
            VectorStorageEnum::MultiDenseAppendableMemmap(_) => None,
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => None,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => None,
            VectorStorageEnum::EmptyDense(_) => None,
            VectorStorageEnum::EmptySparse(_) => None,
        }
    }

    /// Get layout for a single vector
    pub fn get_vector_layout(&self) -> OperationResult<Layout> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => return v.get_dense_vector_layout(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => return v.get_dense_vector_layout(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseMemmap(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseMemmapByte(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseMemmapHalf(v) => return v.get_dense_vector_layout(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => return v.get_dense_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => return v.get_dense_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => return v.get_dense_vector_layout(),

            VectorStorageEnum::DenseAppendableMemmap(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::SparseVolatile(_) => {}
            VectorStorageEnum::SparseMmap(_) => {}
            VectorStorageEnum::MultiDenseVolatile(_) => {}
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_) => {}
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(_) => {}
            VectorStorageEnum::MultiDenseAppendableMemmap(_) => {}
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => {}
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => {}
            VectorStorageEnum::EmptyDense(_) => {}
            VectorStorageEnum::EmptySparse(_) => {}
        }
        Err(OperationError::service_error(
            "Vector layout is not implemented for this storage",
        ))
    }
}

impl VectorStorage for VectorStorageEnum {
    fn distance(&self) -> Distance {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.distance(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.distance(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.distance(),
            VectorStorageEnum::DenseMemmap(v) => v.distance(),
            VectorStorageEnum::DenseMemmapByte(v) => v.distance(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.distance(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.distance(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.distance(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.distance(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.distance(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.distance(),
            VectorStorageEnum::SparseVolatile(v) => v.distance(),
            VectorStorageEnum::SparseMmap(v) => v.distance(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.distance(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.distance(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.distance(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.distance(),
            VectorStorageEnum::EmptyDense(v) => v.distance(),
            VectorStorageEnum::EmptySparse(v) => v.distance(),
        }
    }

    fn datatype(&self) -> VectorStorageDatatype {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.datatype(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.datatype(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.datatype(),
            VectorStorageEnum::DenseMemmap(v) => v.datatype(),
            VectorStorageEnum::DenseMemmapByte(v) => v.datatype(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.datatype(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.datatype(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.datatype(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.datatype(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.datatype(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.datatype(),
            VectorStorageEnum::SparseVolatile(v) => v.datatype(),
            VectorStorageEnum::SparseMmap(v) => v.datatype(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.datatype(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.datatype(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.datatype(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.datatype(),
            VectorStorageEnum::EmptyDense(v) => v.datatype(),
            VectorStorageEnum::EmptySparse(v) => v.datatype(),
        }
    }

    /// If false - data is stored in RAM (and persisted on disk)
    /// If true - data is stored on disk, and is not forced to be in RAM
    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.is_on_disk(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.is_on_disk(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.is_on_disk(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.is_on_disk(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.is_on_disk(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.is_on_disk(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.is_on_disk(),
            VectorStorageEnum::SparseVolatile(v) => v.is_on_disk(),
            VectorStorageEnum::SparseMmap(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.is_on_disk(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.is_on_disk(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.is_on_disk(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.is_on_disk(),
            VectorStorageEnum::EmptyDense(v) => v.is_on_disk(),
            VectorStorageEnum::EmptySparse(v) => v.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.total_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.total_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.total_vector_count(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.total_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.total_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.total_vector_count(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.total_vector_count(),
            VectorStorageEnum::SparseVolatile(v) => v.total_vector_count(),
            VectorStorageEnum::SparseMmap(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.total_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.total_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.total_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.total_vector_count(),
            VectorStorageEnum::EmptyDense(v) => v.total_vector_count(),
            VectorStorageEnum::EmptySparse(v) => v.total_vector_count(),
        }
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.get_vector::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.get_vector::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.get_vector::<P>(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector::<P>(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector::<P>(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.get_vector::<P>(key),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.get_vector::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.get_vector::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.get_vector::<P>(key),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector::<P>(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector::<P>(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.get_vector::<P>(key),
            VectorStorageEnum::SparseVolatile(v) => v.get_vector::<P>(key),
            VectorStorageEnum::SparseMmap(v) => v.get_vector::<P>(key),
            VectorStorageEnum::MultiDenseVolatile(v) => v.get_vector::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.get_vector::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.get_vector::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.get_vector::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.get_vector::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.get_vector::<P>(key),
            VectorStorageEnum::EmptyDense(v) => v.get_vector::<P>(key),
            VectorStorageEnum::EmptySparse(v) => v.get_vector::<P>(key),
        }
    }

    fn read_vectors<P: AccessPattern>(
        &self,
        keys: impl IntoIterator<Item = PointOffsetType>,
        callback: impl FnMut(PointOffsetType, CowVector<'_>),
    ) {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::DenseMemmap(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::DenseMemmapByte(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::DenseMemmapHalf(v) => v.read_vectors::<P>(keys, callback),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.read_vectors::<P>(keys, callback),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::SparseVolatile(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::SparseMmap(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::MultiDenseVolatile(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.read_vectors::<P>(keys, callback),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.read_vectors::<P>(keys, callback)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.read_vectors::<P>(keys, callback)
            }
            VectorStorageEnum::EmptyDense(v) => v.read_vectors::<P>(keys, callback),
            VectorStorageEnum::EmptySparse(v) => v.read_vectors::<P>(keys, callback),
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.get_vector_opt::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.get_vector_opt::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::DenseMemmap(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.get_vector_opt::<P>(key),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.get_vector_opt::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.get_vector_opt::<P>(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.get_vector_opt::<P>(key),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::SparseVolatile(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::SparseMmap(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::MultiDenseVolatile(v) => v.get_vector_opt::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.get_vector_opt::<P>(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::EmptyDense(v) => v.get_vector_opt::<P>(key),
            VectorStorageEnum::EmptySparse(v) => v.get_vector_opt::<P>(key),
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.insert_vector(key, vector, hw_counter),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.insert_vector(key, vector, hw_counter),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::DenseMemmap(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::DenseMemmapByte(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::DenseMemmapHalf(v) => v.insert_vector(key, vector, hw_counter),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.insert_vector(key, vector, hw_counter),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.insert_vector(key, vector, hw_counter),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.insert_vector(key, vector, hw_counter),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::SparseVolatile(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::SparseMmap(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::MultiDenseVolatile(v) => v.insert_vector(key, vector, hw_counter),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.insert_vector(key, vector, hw_counter)
            }
            VectorStorageEnum::EmptyDense(v) => v.insert_vector(key, vector, hw_counter),
            VectorStorageEnum::EmptySparse(v) => v.insert_vector(key, vector, hw_counter),
        }
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.update_from(other_vectors, stopped),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.update_from(other_vectors, stopped),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::DenseMemmap(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::DenseMemmapByte(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::DenseMemmapHalf(v) => v.update_from(other_vectors, stopped),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.update_from(other_vectors, stopped),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.update_from(other_vectors, stopped),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.update_from(other_vectors, stopped),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                v.update_from(other_vectors, stopped)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                v.update_from(other_vectors, stopped)
            }
            VectorStorageEnum::SparseVolatile(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::SparseMmap(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::MultiDenseVolatile(v) => v.update_from(other_vectors, stopped),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.update_from(other_vectors, stopped),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                v.update_from(other_vectors, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                v.update_from(other_vectors, stopped)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                v.update_from(other_vectors, stopped)
            }
            VectorStorageEnum::EmptyDense(v) => v.update_from(other_vectors, stopped),
            VectorStorageEnum::EmptySparse(v) => v.update_from(other_vectors, stopped),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.flusher(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.flusher(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.flusher(),
            VectorStorageEnum::DenseMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseMemmapByte(v) => v.flusher(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.flusher(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.flusher(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.flusher(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.flusher(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.flusher(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.flusher(),
            VectorStorageEnum::SparseVolatile(v) => v.flusher(),
            VectorStorageEnum::SparseMmap(v) => v.flusher(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.flusher(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.flusher(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.flusher(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.flusher(),
            VectorStorageEnum::EmptyDense(v) => v.flusher(),
            VectorStorageEnum::EmptySparse(v) => v.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.files(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.files(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.files(),
            VectorStorageEnum::DenseMemmap(v) => v.files(),
            VectorStorageEnum::DenseMemmapByte(v) => v.files(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.files(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.files(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.files(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.files(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.files(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.files(),
            VectorStorageEnum::SparseVolatile(v) => v.files(),
            VectorStorageEnum::SparseMmap(v) => v.files(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.files(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.files(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.files(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.files(),
            VectorStorageEnum::EmptyDense(v) => v.files(),
            VectorStorageEnum::EmptySparse(v) => v.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.immutable_files(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.immutable_files(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.immutable_files(),
            VectorStorageEnum::DenseMemmap(v) => v.immutable_files(),
            VectorStorageEnum::DenseMemmapByte(v) => v.immutable_files(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.immutable_files(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.immutable_files(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.immutable_files(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.immutable_files(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.immutable_files(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.immutable_files(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.immutable_files(),
            VectorStorageEnum::SparseVolatile(v) => v.immutable_files(),
            VectorStorageEnum::SparseMmap(v) => v.immutable_files(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.immutable_files(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.immutable_files(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.immutable_files(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.immutable_files(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.immutable_files(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.immutable_files(),
            VectorStorageEnum::EmptyDense(v) => v.immutable_files(),
            VectorStorageEnum::EmptySparse(v) => v.immutable_files(),
        }
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.delete_vector(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.delete_vector(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.delete_vector(key),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.delete_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.delete_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.delete_vector(key),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.delete_vector(key),
            VectorStorageEnum::SparseVolatile(v) => v.delete_vector(key),
            VectorStorageEnum::SparseMmap(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseVolatile(v) => v.delete_vector(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.delete_vector(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.delete_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.delete_vector(key),
            VectorStorageEnum::EmptyDense(v) => v.delete_vector(key),
            VectorStorageEnum::EmptySparse(v) => v.delete_vector(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.is_deleted_vector(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.is_deleted_vector(key),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseMemmapHalf(v) => v.is_deleted_vector(key),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.is_deleted_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.is_deleted_vector(key),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.is_deleted_vector(key),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::SparseVolatile(v) => v.is_deleted_vector(key),
            VectorStorageEnum::SparseMmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseVolatile(v) => v.is_deleted_vector(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.is_deleted_vector(key),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.is_deleted_vector(key),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.is_deleted_vector(key),
            VectorStorageEnum::EmptyDense(v) => v.is_deleted_vector(key),
            VectorStorageEnum::EmptySparse(v) => v.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.deleted_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.deleted_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.deleted_vector_count(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.deleted_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.deleted_vector_count(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.deleted_vector_count(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::SparseVolatile(v) => v.deleted_vector_count(),
            VectorStorageEnum::SparseMmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.deleted_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.deleted_vector_count(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.deleted_vector_count(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.deleted_vector_count(),
            VectorStorageEnum::EmptyDense(v) => v.deleted_vector_count(),
            VectorStorageEnum::EmptySparse(v) => v.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageEnum::DenseVolatile(v) => v.deleted_vector_bitslice(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => v.deleted_vector_bitslice(),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseMemmapHalf(v) => v.deleted_vector_bitslice(),

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => v.deleted_vector_bitslice(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => v.deleted_vector_bitslice(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => v.deleted_vector_bitslice(),

            VectorStorageEnum::DenseAppendableMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::SparseVolatile(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::SparseMmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseVolatile(v) => v.deleted_vector_bitslice(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => v.deleted_vector_bitslice(),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::EmptyDense(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::EmptySparse(v) => v.deleted_vector_bitslice(),
        }
    }
}
