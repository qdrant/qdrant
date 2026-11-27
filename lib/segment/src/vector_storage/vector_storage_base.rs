use std::alloc::Layout;
use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use blobstore::Blob;
use common::bitvec::BitSlice;
use common::generic_consts::{AccessPattern, Random};
use common::types::{PointOffsetType, ScoreType};
#[cfg(target_os = "linux")]
use common::universal_io::IoUringFile;
use common::universal_io::{MmapFile, UserData};
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;
use sparse::common::sparse_vector::SparseVector;

use super::dense::dense_vector_storage::DenseVectorStorageImpl;
use super::dense::empty_dense_vector_storage::EmptyDenseVectorStorage;
use super::dense::volatile_dense_vector_storage::VolatileDenseVectorStorage;
use super::graph_vectors::GraphVectors;
use super::multi_dense::appendable_mmap_multi_dense_vector_storage::AppendableMmapMultiDenseVectorStorage;
use super::multi_dense::volatile_multi_dense_vector_storage::VolatileMultiDenseVectorStorage;
use super::raw_scorer::NotDeletedChecker;
use super::sparse::StoredSparseVector;
use super::sparse::empty_sparse_vector_storage::EmptySparseVectorStorage;
use super::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use super::sparse::volatile_sparse_vector_storage::VolatileSparseVectorStorage;
use super::turbo::multi_turbo::AppendableMmapMultiTurboVectorStorage;
use super::turbo::{AppendableMmapTurboVectorStorage, TurboVectorStorageImpl};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, TypedMultiDenseVectorRef,
    VectorElementType, VectorElementTypeByte, VectorElementTypeHalf, VectorInternal, VectorRef,
};
use crate::index::hnsw_index::{HnswGraph, HnswLinksStorage};
use crate::types::{Distance, IoBackend, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::dense::appendable_dense_vector_storage::AppendableMmapDenseVectorStorage;
use crate::vector_storage::dense::graph_inline_dense_vector_storage::GraphInlineDenseVectorStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

/// In case of simple vector storage, vector offset is the same as [`PointOffsetType`].
/// But in case of multivectors, it requires an additional lookup.
pub type VectorOffsetType = usize;

/// Generalized vector offset.
pub trait VectorOffset: Copy + fmt::Debug {
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

/// Read-only trait for vector storage.
///
/// Defines all read operations on vector storage. Search and retrieval logic
/// only requires this trait, which makes it possible to implement read-only
/// segments without duplicating storage code.
///
/// Storage operates with internal IDs (`PointOffsetType`), which always starts
/// with zero and have no skips.
pub trait VectorStorageRead {
    fn distance(&self) -> Distance;

    fn datatype(&self) -> VectorStorageDatatype;

    /// Whether the data was opened cold: left on disk and paged in on demand, so
    /// reads may hit the disk. False for heap data and for mmaps populated on open.
    fn is_cold(&self) -> bool;

    /// Backend this storage reads through, `None` when it can only be opened on one. Surfaced
    /// as `vector_data[name].io_backend` in [`SegmentInfo`](crate::types::SegmentInfo).
    fn io_backend(&self) -> Option<IoBackend> {
        None
    }

    /// Number of vectors
    ///
    /// - includes soft deleted vectors, as they are still stored
    fn total_vector_count(&self) -> usize;

    /// Get the number of available vectors, considering deleted points and vectors
    ///
    /// This uses [`VectorStorageRead::total_vector_count`] and [`VectorStorageRead::deleted_vector_count`] internally.
    ///
    /// # Warning
    ///
    /// This number may not always be accurate. See warning in [`VectorStorageRead::deleted_vector_count`] documentation.
    fn available_vector_count(&self) -> usize {
        self.total_vector_count()
            .saturating_sub(self.deleted_vector_count())
    }

    /// Get the vector by the given key
    /// Get the vector by the given key with potential optimizations for sequential reads.
    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_>;

    /// Get multiple vectors by the given keys.
    /// Potentially optimized for internal parallel reads.
    ///
    /// Each input is tagged with caller-supplied user data `U` (e.g., an
    /// external point id, or the input position). The data is threaded
    /// straight back to the callback alongside its offset and vector, so
    /// callers can map results into a parallel input array without keeping a
    /// separate `offset → ...` lookup table.
    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) -> OperationResult<()> {
        for (user_data, key) in keys {
            callback(user_data, key, self.get_vector::<P>(key));
        }
        Ok(())
    }

    /// Get the vector by the given key if it exists
    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>>;

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

    /// [`NotDeletedChecker`] over this storage's deleted flags and the given point deleted flags.
    fn not_deleted_checker<'a>(&'a self, point_deleted: &'a BitSlice) -> NotDeletedChecker<'a> {
        NotDeletedChecker {
            point_deleted,
            vec_deleted: self.deleted_vector_bitslice(),
        }
    }

    /// Size of all available (non-deleted) vectors in bytes.
    fn size_of_available_vectors_in_bytes(&self) -> usize;

    /// Call `f` with the storage-native serialized bytes of the vector.
    /// `Ok(None)` means the storage has no value for this key.
    ///
    /// Only [`VectorStorageEnum`] implements this, dispatching to the concrete
    /// dense / sparse / multi byte readers; calling it on any other storage
    /// type is a bug and returns a service error. The format is storage-native
    /// (raw elements, TurboQuant bytes, or bincoded sparse) and carries no
    /// encoding/version tag, so it round-trips only into a matching storage.
    fn with_vector_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> OperationResult<Option<R>> {
        let _ = (key, f);
        debug_assert!(
            false,
            "with_vector_bytes_opt is only dispatched by VectorStorageEnum",
        );
        Err(OperationError::service_error(
            "Raw vector bytes are not implemented for this storage",
        ))
    }

    /// Owned counterpart of [`VectorStorageRead::with_vector_bytes_opt`], for
    /// callers that need a `Vec<u8>` (e.g. `retrieve_raw`).
    ///
    /// Default copies the borrowed bytes once; [`VectorStorageEnum`] returns the
    /// already-owned sparse buffer directly to skip a redundant copy.
    fn vector_bytes_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<Vec<u8>>> {
        self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
    }

    /// Bulk counterpart of [`Self::vector_bytes_opt`]: calls `callback` with
    /// the storage-native serialized bytes of each vector. Offsets without a
    /// stored value are skipped; storage read failures propagate as errors.
    ///
    /// On-disk storages batch the underlying reads, so bulk raw reads keep the
    /// same read pipelining as [`Self::read_vectors`] (io_uring submission
    /// batching / mmap prefetch). In-RAM storages with no batched reader opt
    /// into [`default_read_vector_bytes_impl`] explicitly.
    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()>;
}

/// Per-key fallback for [`VectorStorageRead::read_vector_bytes`], for storages
/// without a batched byte reader: one [`VectorStorageRead::vector_bytes_opt`]
/// read per offset, no pipelining.
pub fn default_read_vector_bytes_impl<VS, P, U>(
    vector_storage: &VS,
    keys: impl IntoIterator<Item = (U, PointOffsetType)>,
    mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
) -> OperationResult<()>
where
    VS: VectorStorageRead + ?Sized,
    P: AccessPattern,
    U: Copy + UserData,
{
    for (user_data, key) in keys {
        let Some(bytes) = vector_storage.vector_bytes_opt::<P>(key)? else {
            continue;
        };
        callback(user_data, key, bytes);
    }
    Ok(())
}

/// Trait for vector storage with mutating operations.
///
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage: VectorStorageRead {
    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()>;

    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    /// Flag the vector by the given key as deleted
    ///
    /// Returns true if the vector was not deleted before and is now deleted
    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool>;
}

/// Read-only access to a dense vector storage.
///
/// Everything needed to read and score dense vectors, without the ability to
/// mutate. Implemented by read-only storages too, which cannot provide
/// [`DenseVectorStorage::update_from`].
pub trait DenseVectorStorageRead<T: PrimitiveVectorElement>: VectorStorageRead {
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

    /// Batched byte counterpart of [`VectorStorageRead::read_vectors`]: calls
    /// `callback` with the raw bytes of each vector.
    ///
    /// The default reads one vector at a time; storages with batched readers
    /// override this so bulk byte reads keep the same read pipelining as
    /// `read_vectors` (io_uring submission batching for on-disk storages).
    fn read_dense_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        for (user_data, key) in keys {
            let dense = self.get_dense::<P>(key);
            callback(user_data, key, bytemuck::cast_slice(&dense).to_vec());
        }
        Ok(())
    }

    /// Get layout for a single vector
    fn get_dense_vector_layout(&self) -> OperationResult<Layout> {
        Layout::array::<T>(self.vector_dim())
            .map_err(|_| OperationError::service_error("Layout is too big"))
    }

    /// Run given function for each vector in the dense batch.
    ///
    /// Implementation can assume that the keys are consecutive
    fn for_each_in_dense_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()>;
}

pub fn default_for_each_in_dense_batch<T, F, D>(
    this: &D,
    keys: &[u32],
    mut callback: F,
) -> Result<(), OperationError>
where
    T: PrimitiveVectorElement,
    F: FnMut(usize, &[T]),
    D: DenseVectorStorageRead<T> + ?Sized,
{
    for (idx, &key) in keys.iter().enumerate() {
        callback(idx, &this.get_dense::<Random>(key));
    }
    Ok(())
}

pub trait DenseVectorStorage<T: PrimitiveVectorElement>: DenseVectorStorageRead<T> {
    /// Add the given dense vectors to the storage.
    ///
    /// # Returns
    /// The range of point offsets that were added to the storage.
    ///
    /// If stopped, the operation returns a cancellation error.
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [T]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
}

/// Read-only access to a sparse vector storage: everything needed to score
/// queries, without the ability to mutate. Implemented by read-only storages
/// too, which cannot provide [`SparseVectorStorage::update_from`].
pub trait SparseVectorStorageRead: VectorStorageRead {
    fn get_sparse<P: AccessPattern>(&self, key: PointOffsetType) -> OperationResult<SparseVector>;
    fn get_sparse_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<SparseVector>>;

    fn for_each_in_sparse_batch<F>(
        &self,
        keys: &[PointOffsetType],
        callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, SparseVector);

    /// Serialized sparse vector bytes, in the lossless on-disk
    /// [`StoredSparseVector`] form. Never zero-copy: re-encoding allocates the
    /// returned buffer. Prefer this over [`Self::with_sparse_bytes_opt`] when an
    /// owned `Vec<u8>` is needed, to skip an extra copy.
    fn sparse_bytes_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<Vec<u8>>> {
        let Some(sparse) = self.get_sparse_opt::<P>(key)? else {
            return Ok(None);
        };
        Ok(Some(StoredSparseVector::from(&sparse).to_bytes()))
    }

    /// Borrow-based form of [`Self::sparse_bytes_opt`].
    fn with_sparse_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> OperationResult<Option<R>> {
        Ok(self.sparse_bytes_opt::<P>(key)?.map(|bytes| f(&bytes)))
    }
}

pub trait SparseVectorStorage: SparseVectorStorageRead {
    /// Add the given sparse vectors to the storage.
    ///
    /// # Returns
    /// The range of point offsets that were added to the storage.
    ///
    /// If stopped, the operation returns a cancellation error.
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, SparseVector>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
}

/// Read-only access to a multi-dense vector storage: everything needed to score
/// queries, without the ability to mutate. Implemented by read-only storages
/// too, which cannot provide [`MultiVectorStorage::update_from`].
pub trait MultiVectorStorageRead<T: PrimitiveVectorElement>: VectorStorageRead {
    fn vector_dim(&self) -> usize;

    fn get_multi<P: AccessPattern>(&self, key: PointOffsetType) -> CowMultiVector<'_, T>;
    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<CowMultiVector<'_, T>>;

    fn for_each_in_batch_multi<F>(
        &self,
        keys: &[PointOffsetType],
        callback: F,
    ) -> OperationResult<()>
    where
        F: FnMut(usize, TypedMultiDenseVectorRef<'_, T>);

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = Cow<'_, [T]>> + Clone + Send;
    fn multi_vector_config(&self) -> &MultiVectorConfig;

    /// Call `f` with the raw bytes of the flattened inner vectors, if any.
    /// Inner-vector count = `len() / (vector_dim() * size_of::<T>())`.
    fn with_multi_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        let multi = self.get_multi_opt::<P>(key)?;
        Some(f(bytemuck::cast_slice(multi.as_ref().flattened_vectors)))
    }
}

pub trait MultiVectorStorage<T: PrimitiveVectorElement>: MultiVectorStorageRead<T> {
    /// Add the given multi-dense vectors to the storage.
    ///
    /// # Returns
    /// The range of point offsets that were added to the storage.
    ///
    /// If stopped, the operation returns a cancellation error.
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (CowMultiVector<'a, T>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
}

pub trait DenseTQVectorStorageRead: VectorStorageRead {
    /// Original dimension of the vector, without quantization applied.
    fn vector_dim(&self) -> usize;

    /// Size in bytes of the quantized vector.
    fn quantized_vector_size(&self) -> usize;

    /// Get the quantized vector by the given key
    fn get_dense_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]>;

    /// Call `f` with the raw encoded bytes of the vector if it exists.
    fn with_dense_tq_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        ((key as usize) < self.total_vector_count()).then(|| {
            let dense_tq = self.get_dense_tq::<P>(key);
            f(&dense_tq)
        })
    }

    /// Get layout for a single vector
    fn get_dense_tq_vector_layout(&self) -> OperationResult<Layout> {
        Layout::array::<u8>(self.quantized_vector_size())
            .map_err(|_| OperationError::service_error("Layout is too big"))
    }

    /// Run given function for each vector in the batch, batching the
    /// underlying reads where the storage supports it (io_uring submission
    /// batching / mmap prefetch for on-disk storages).
    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()>;

    /// Batched byte counterpart of [`VectorStorageRead::read_vectors`]: calls
    /// `callback` with the raw encoded bytes of each vector. TQ counterpart of
    /// [`DenseVectorStorageRead::read_dense_bytes`], with the same valid-keys
    /// precondition.
    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()>;

    /// Dequantize the stored vector at `key` back to a full-precision
    /// [`DenseVector`] for a requantization build (the quantized-vectors layer
    /// or the GPU index). When `keep_rotated` is set the inverse Hadamard
    /// rotation is skipped — valid only for rotation-invariant distances, where
    /// the caller compensates. Lets the requantization/GPU builders stay generic
    /// over the concrete single-file / appendable dense TQ storages.
    fn get_dense_for_requantization(&self, key: PointOffsetType, keep_rotated: bool)
    -> DenseVector;
}

pub trait DenseTQVectorStorage: DenseTQVectorStorageRead {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
}

pub trait MultiTQVectorStorageRead: VectorStorageRead {
    /// Original dimension of each inner vector, without quantization applied.
    fn vector_dim(&self) -> usize;

    /// Size in bytes of one encoded inner vector.
    fn quantized_vector_size(&self) -> usize;

    fn multi_vector_config(&self) -> &MultiVectorConfig;

    /// Concatenated encoded inner vectors of one point; the inner vector count
    /// is `len() / quantized_vector_size()`. Merge/retrieval API only — scoring
    /// must read inner records individually to avoid the concatenation.
    fn get_multi_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]>;

    /// Call `f` with the concatenated encoded inner vectors, if any.
    /// Inner-vector count = `len() / quantized_vector_size()`.
    fn with_multi_tq_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        ((key as usize) < self.total_vector_count()).then(|| {
            let multi_tq = self.get_multi_tq::<P>(key);
            f(&multi_tq)
        })
    }
}

pub trait MultiTQVectorStorage: MultiTQVectorStorageRead {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [u8]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
}

/// Scoring surface the Turbo query scorers need from a dense TurboQuant storage.
/// Implemented by both writable dense TQ storages and their read-only
/// counterpart, so the shared `raw_turbo_scorer_impl` works over either.
pub trait TurboScoring: DenseTQVectorStorageRead {
    fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ;

    fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType;

    /// Batch counterpart of [`Self::score_query_bytes`]: `scores[i]` ← score
    /// of `query` against point `ids[i]`.  Storages backed by contiguous
    /// encoded bytes score whole runs of consecutive ids with one batched
    /// quantizer call (see `turbo::shared::score_query_batch`).
    fn score_query_batch(
        &self,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()>;

    fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType;

    fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]>;

    /// The quantizer the vectors are encoded with.
    fn quantizer(&self) -> &TurboQuantizer;

    /// The stored vector of `key` as a query that scores the others as
    /// [`Self::score_internal_encoded`] does, so that an index is built from the codes alone.
    fn stored_query(&self, key: PointOffsetType) -> EncodedQueryTQ {
        self.quantizer()
            .stored_query(&self.get_quantized_vector(key))
    }
}

/// Scoring surface the Turbo multivector query scorers need from a multivector
/// TurboQuant storage. Multi counterpart of [`TurboScoring`]: implemented by the
/// writable multivector TQ storage and its read-only counterpart, so the shared
/// `raw_turbo_multi_scorer_impl` works over either.
pub trait TurboMultiScoring: MultiTQVectorStorageRead {
    /// Preprocess and precompute each inner query vector of a multi-query once.
    fn preprocess_query(&self, query: &MultiDenseVectorInternal) -> Vec<EncodedQueryTQ>;

    /// The stored inner vectors of `key` as a multi-query that scores the others as
    /// [`Self::score_internal_max_similarity`] does, so that an index is built from the codes
    /// alone.
    fn stored_query(&self, key: PointOffsetType) -> Vec<EncodedQueryTQ>;

    /// Symmetric MaxSim score between two stored points.
    fn score_internal_max_similarity(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType;

    /// Score a precomputed multi-query directly against a point's concatenated
    /// encoded records, without a separate storage fetch.
    fn score_records_max_similarity(&self, query: &[EncodedQueryTQ], records: &[u8]) -> ScoreType;

    /// Two-pass batched read for records (offsets, then vectors), invoking
    /// `callback` once per requested point. Lets the query scorers batch the
    /// underlying reads over either backend.
    fn for_each_record_range<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, &[u8]),
    ) -> OperationResult<()>;
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

    DenseGraphInline(Box<GraphInlineDenseVectorStorage<VectorElementType, HnswLinksStorage>>),
    DenseGraphInlineByte(
        Box<GraphInlineDenseVectorStorage<VectorElementTypeByte, HnswLinksStorage>>,
    ),
    DenseGraphInlineHalf(
        Box<GraphInlineDenseVectorStorage<VectorElementTypeHalf, HnswLinksStorage>>,
    ),

    #[cfg(target_os = "linux")]
    DenseUring(Box<DenseVectorStorageImpl<VectorElementType, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringByte(Box<DenseVectorStorageImpl<VectorElementTypeByte, IoUringFile>>),
    #[cfg(target_os = "linux")]
    DenseUringHalf(Box<DenseVectorStorageImpl<VectorElementTypeHalf, IoUringFile>>),

    DenseAppendableMemmap(Box<AppendableMmapDenseVectorStorage<VectorElementType>>),
    DenseAppendableMemmapByte(Box<AppendableMmapDenseVectorStorage<VectorElementTypeByte>>),
    DenseAppendableMemmapHalf(Box<AppendableMmapDenseVectorStorage<VectorElementTypeHalf>>),
    DenseTurboMemmap(Box<TurboVectorStorageImpl<QuantizedStorage<MmapFile>>>),
    DenseTurboGraphInline(Box<TurboVectorStorageImpl<GraphVectors<u8, HnswLinksStorage>>>),
    #[cfg(target_os = "linux")]
    DenseTurboUring(Box<TurboVectorStorageImpl<QuantizedStorage<IoUringFile>>>),
    DenseTurboAppendableMemmap(Box<AppendableMmapTurboVectorStorage>),
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
    MultiDenseTurbo(Box<AppendableMmapMultiTurboVectorStorage>),
    EmptyDense(EmptyDenseVectorStorage),
    EmptySparse(EmptySparseVectorStorage),
}

macro_rules! for_all_vector_storage {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            VectorStorageEnum::DenseVolatile($pattern) => $result,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte($pattern) => $result,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf($pattern) => $result,
            VectorStorageEnum::DenseMemmap($pattern) => $result,
            VectorStorageEnum::DenseMemmapByte($pattern) => $result,
            VectorStorageEnum::DenseMemmapHalf($pattern) => $result,
            VectorStorageEnum::DenseGraphInline($pattern) => $result,
            VectorStorageEnum::DenseGraphInlineByte($pattern) => $result,
            VectorStorageEnum::DenseGraphInlineHalf($pattern) => $result,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring($pattern) => $result,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte($pattern) => $result,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf($pattern) => $result,
            VectorStorageEnum::DenseAppendableMemmap($pattern) => $result,
            VectorStorageEnum::DenseAppendableMemmapByte($pattern) => $result,
            VectorStorageEnum::DenseAppendableMemmapHalf($pattern) => $result,
            VectorStorageEnum::DenseTurboMemmap($pattern) => $result,
            VectorStorageEnum::DenseTurboGraphInline($pattern) => $result,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring($pattern) => $result,
            VectorStorageEnum::DenseTurboAppendableMemmap($pattern) => $result,
            VectorStorageEnum::SparseVolatile($pattern) => $result,
            VectorStorageEnum::SparseMmap($pattern) => $result,
            VectorStorageEnum::MultiDenseVolatile($pattern) => $result,
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte($pattern) => $result,
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf($pattern) => $result,
            VectorStorageEnum::MultiDenseAppendableMemmap($pattern) => $result,
            VectorStorageEnum::MultiDenseAppendableMemmapByte($pattern) => $result,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf($pattern) => $result,
            VectorStorageEnum::MultiDenseTurbo($pattern) => $result,
            VectorStorageEnum::EmptyDense($pattern) => $result,
            VectorStorageEnum::EmptySparse($pattern) => $result,
        }
    };
}

impl VectorStorageEnum {
    pub fn hnsw_graph(&self) -> Option<&HnswGraph<HnswLinksStorage>> {
        match self {
            VectorStorageEnum::DenseGraphInline(v) => Some(v.hnsw_graph()),
            VectorStorageEnum::DenseGraphInlineByte(v) => Some(v.hnsw_graph()),
            VectorStorageEnum::DenseGraphInlineHalf(v) => Some(v.hnsw_graph()),
            VectorStorageEnum::DenseTurboGraphInline(v) => Some(v.hnsw_graph()),
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::DenseMemmap(_)
            | VectorStorageEnum::DenseMemmapByte(_)
            | VectorStorageEnum::DenseMemmapHalf(_)
            | VectorStorageEnum::DenseAppendableMemmap(_)
            | VectorStorageEnum::DenseAppendableMemmapByte(_)
            | VectorStorageEnum::DenseAppendableMemmapHalf(_)
            | VectorStorageEnum::DenseTurboMemmap(_)
            | VectorStorageEnum::DenseTurboAppendableMemmap(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseTurbo(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => None,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => None,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_)
            | VectorStorageEnum::DenseUringByte(_)
            | VectorStorageEnum::DenseUringHalf(_)
            | VectorStorageEnum::DenseTurboUring(_) => None,
        }
    }

    pub fn try_multi_vector_config(&self) -> Option<&MultiVectorConfig> {
        match self {
            VectorStorageEnum::MultiDenseVolatile(s) => Some(s.multi_vector_config()),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(s) => Some(s.multi_vector_config()),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmap(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::MultiDenseTurbo(s) => Some(s.multi_vector_config()),
            VectorStorageEnum::EmptyDense(s) => s.multi_vector_config(),
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::DenseMemmap(_)
            | VectorStorageEnum::DenseMemmapByte(_)
            | VectorStorageEnum::DenseMemmapHalf(_)
            | VectorStorageEnum::DenseGraphInline(_)
            | VectorStorageEnum::DenseGraphInlineByte(_)
            | VectorStorageEnum::DenseGraphInlineHalf(_)
            | VectorStorageEnum::DenseAppendableMemmap(_)
            | VectorStorageEnum::DenseAppendableMemmapByte(_)
            | VectorStorageEnum::DenseAppendableMemmapHalf(_)
            | VectorStorageEnum::DenseTurboMemmap(_)
            | VectorStorageEnum::DenseTurboGraphInline(_)
            | VectorStorageEnum::DenseTurboAppendableMemmap(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::EmptySparse(_) => None,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_) | VectorStorageEnum::DenseVolatileHalf(_) => {
                None
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_)
            | VectorStorageEnum::DenseUringByte(_)
            | VectorStorageEnum::DenseUringHalf(_)
            | VectorStorageEnum::DenseTurboUring(_) => None,
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
            VectorStorageEnum::DenseGraphInline(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseGraphInlineByte(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseGraphInlineHalf(v) => {
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
            VectorStorageEnum::DenseTurboMemmap(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseTurboGraphInline(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => {
                VectorInternal::from(vec![1.0; v.vector_dim()])
            }
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
            VectorStorageEnum::MultiDenseTurbo(v) => {
                VectorInternal::from(MultiDenseVectorInternal::placeholder(v.vector_dim()))
            }
            VectorStorageEnum::EmptyDense(v) => {
                VectorInternal::from(vec![1.0; DenseVectorStorageRead::vector_dim(v)])
            }
            VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::EmptySparse(_) => VectorInternal::from(SparseVector::default()),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseMemmap(vs) => vs.populate(),
            VectorStorageEnum::DenseMemmapByte(vs) => vs.populate(),
            VectorStorageEnum::DenseMemmapHalf(vs) => vs.populate(),
            VectorStorageEnum::DenseGraphInline(vs) => vs.populate(),
            VectorStorageEnum::DenseGraphInlineByte(vs) => vs.populate(),
            VectorStorageEnum::DenseGraphInlineHalf(vs) => vs.populate(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(vs) => vs.populate(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(vs) => vs.populate(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(vs) => vs.populate(),
            VectorStorageEnum::DenseAppendableMemmap(vs) => vs.populate()?,
            VectorStorageEnum::DenseAppendableMemmapByte(vs) => vs.populate()?,
            VectorStorageEnum::DenseAppendableMemmapHalf(vs) => vs.populate()?,
            VectorStorageEnum::DenseTurboMemmap(vs) => vs.populate()?,
            VectorStorageEnum::DenseTurboGraphInline(vs) => vs.populate()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(vs) => vs.populate()?,
            VectorStorageEnum::DenseTurboAppendableMemmap(vs) => vs.populate()?,
            VectorStorageEnum::SparseMmap(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseAppendableMemmap(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => vs.populate()?,
            VectorStorageEnum::MultiDenseTurbo(vs) => vs.populate()?,
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => {}
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => {}
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseMemmapHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseGraphInline(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseGraphInlineByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseGraphInlineHalf(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseAppendableMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseAppendableMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseAppendableMemmapHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseTurboMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseTurboGraphInline(vs) => vs.clear_cache()?,
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseTurboAppendableMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::SparseMmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseAppendableMemmap(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => vs.clear_cache()?,
            VectorStorageEnum::MultiDenseTurbo(vs) => vs.clear_cache()?,
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => {}
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => {}
        }
        Ok(())
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
            VectorStorageEnum::DenseGraphInline(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseGraphInlineByte(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseGraphInlineHalf(v) => return v.get_dense_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => return v.get_dense_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => return v.get_dense_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseAppendableMemmap(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseAppendableMemmapByte(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => return v.get_dense_vector_layout(),
            VectorStorageEnum::DenseTurboMemmap(v) => return v.get_dense_tq_vector_layout(),
            VectorStorageEnum::DenseTurboGraphInline(v) => return v.get_dense_tq_vector_layout(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => return v.get_dense_tq_vector_layout(),
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => {
                return v.get_dense_tq_vector_layout();
            }
            VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseTurbo(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => {}
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => {}
        }
        Err(OperationError::service_error(
            "Vector layout is not implemented for this storage",
        ))
    }

    /// Write-side counterpart of [`VectorStorageRead::with_vector_bytes_opt`]:
    /// insert a vector given in the storage-native serialized form.
    ///
    /// The bytes carry no encoding/version tag, so they must come from a
    /// storage with the same configuration (kind, datatype, dim) — in
    /// practice, from `retrieve_raw` on a segment with a matching config.
    /// TurboQuant storages ingest the encoded bytes verbatim, avoiding the
    /// lossy dequantize/requantize round-trip; all other storages decode
    /// losslessly and reuse their regular insert path.
    pub fn insert_vector_bytes(
        &mut self,
        key: PointOffsetType,
        bytes: &[u8],
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => insert_dense_bytes(v, key, bytes),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => insert_dense_bytes(v, key, bytes),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => insert_dense_bytes(v, key, bytes),
            VectorStorageEnum::DenseMemmap(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            VectorStorageEnum::DenseMemmapByte(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            VectorStorageEnum::DenseMemmapHalf(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            VectorStorageEnum::DenseGraphInline(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            VectorStorageEnum::DenseGraphInlineByte(v) => {
                insert_dense_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::DenseGraphInlineHalf(v) => {
                insert_dense_bytes(v.as_mut(), key, bytes)
            }

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => insert_dense_bytes(v.as_mut(), key, bytes),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => insert_dense_bytes(v.as_mut(), key, bytes),

            VectorStorageEnum::DenseAppendableMemmap(v) => {
                insert_dense_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                insert_dense_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                insert_dense_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::DenseTurboMemmap(v) => v.insert_tq_bytes(key, bytes),
            VectorStorageEnum::DenseTurboGraphInline(v) => v.insert_tq_bytes(key, bytes),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => v.insert_tq_bytes(key, bytes),
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => v.insert_tq_bytes(key, bytes),
            VectorStorageEnum::SparseVolatile(v) => insert_sparse_bytes(v, key, bytes),
            VectorStorageEnum::SparseMmap(v) => insert_sparse_bytes(v, key, bytes),
            VectorStorageEnum::MultiDenseVolatile(v) => insert_multi_bytes(v, key, bytes),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => insert_multi_bytes(v, key, bytes),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => insert_multi_bytes(v, key, bytes),
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                insert_multi_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                insert_multi_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                insert_multi_bytes(v.as_mut(), key, bytes)
            }
            VectorStorageEnum::MultiDenseTurbo(v) => v.insert_multi_tq_bytes(key, bytes),
            VectorStorageEnum::EmptyDense(_) | VectorStorageEnum::EmptySparse(_) => Err(
                OperationError::service_error("Cannot insert into empty vector storage"),
            ),
        }
    }
}

/// Insert a dense vector from its storage-native bytes (packed `[T]`, as
/// returned by [`DenseVectorStorageRead::with_dense_bytes_opt`]). The `T` →
/// `f32` → `T` round-trip through the regular insert path is lossless for
/// every supported element type.
fn insert_dense_bytes<T: PrimitiveVectorElement, S: DenseVectorStorageRead<T> + VectorStorage>(
    storage: &mut S,
    key: PointOffsetType,
    bytes: &[u8],
) -> OperationResult<()> {
    let expected_size = storage.vector_dim() * size_of::<T>();
    if bytes.len() != expected_size {
        // `MalformedVectorBlob` (not `service_error`) so a malformed blob that
        // reached the WAL is skipped on replay instead of crash-looping recovery.
        return Err(OperationError::malformed_vector_blob(format!(
            "Malformed dense vector blob of {} bytes, expected {expected_size}",
            bytes.len(),
        )));
    }

    // Zero-copy cast when the byte buffer is aligned for `T` (heap
    // allocations virtually always are); the length already matches, so
    // misalignment is the only way the cast can fail, and
    // `pod_collect_to_vec` then copies into an aligned buffer.
    let elements: Cow<'_, [T]> = match bytemuck::try_cast_slice(bytes) {
        Ok(slice) => Cow::Borrowed(slice),
        Err(_) => Cow::Owned(bytemuck::allocation::pod_collect_to_vec(bytes)),
    };
    let vector = T::slice_to_float_cow(elements);
    storage.insert_vector(key, VectorRef::from(vector.as_ref()))
}

/// Insert a multi-dense vector from its storage-native bytes (flattened inner
/// vectors, as returned by [`MultiVectorStorageRead::with_multi_bytes_opt`]).
fn insert_multi_bytes<T: PrimitiveVectorElement, S: MultiVectorStorageRead<T> + VectorStorage>(
    storage: &mut S,
    key: PointOffsetType,
    bytes: &[u8],
) -> OperationResult<()> {
    let inner_size = storage.vector_dim() * size_of::<T>();
    if bytes.is_empty() || !bytes.len().is_multiple_of(inner_size) {
        return Err(OperationError::malformed_vector_blob(format!(
            "Malformed multi vector blob of {} bytes, expected a positive multiple of {inner_size}",
            bytes.len(),
        )));
    }
    let dim = storage.vector_dim();
    // Zero-copy cast when the byte buffer is aligned for `T` (heap
    // allocations virtually always are); copy to an aligned buffer otherwise.
    let multi = match bytemuck::try_cast_slice(bytes) {
        Ok(slice) => T::into_float_multivector(CowMultiVector::Borrowed(
            TypedMultiDenseVectorRef::new(slice, dim),
        )),
        Err(_) => T::into_float_multivector(CowMultiVector::Owned(TypedMultiDenseVector::new(
            bytemuck::allocation::pod_collect_to_vec(bytes),
            dim,
        ))),
    };
    storage.insert_vector(key, VectorRef::MultiDense(multi.as_vec_ref()))
}

/// Insert a sparse vector from its storage-native bytes (the lossless
/// [`StoredSparseVector`] bincode form returned by
/// [`SparseVectorStorageRead::sparse_bytes_opt`]).
fn insert_sparse_bytes<S: VectorStorage>(
    storage: &mut S,
    key: PointOffsetType,
    bytes: &[u8],
) -> OperationResult<()> {
    let sparse = StoredSparseVector::decode_untrusted_bytes(bytes)?;
    storage.insert_vector(key, VectorRef::from(&sparse))
}

impl VectorStorageRead for VectorStorageEnum {
    fn io_backend(&self) -> Option<IoBackend> {
        match self {
            // The single-file immutable storages are the ones opened on either backend
            VectorStorageEnum::DenseMemmap(_) => Some(IoBackend::Mmap),
            VectorStorageEnum::DenseMemmapByte(_) => Some(IoBackend::Mmap),
            VectorStorageEnum::DenseMemmapHalf(_) => Some(IoBackend::Mmap),
            VectorStorageEnum::DenseGraphInline(v) => v.io_backend(),
            VectorStorageEnum::DenseGraphInlineByte(v) => v.io_backend(),
            VectorStorageEnum::DenseGraphInlineHalf(v) => v.io_backend(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_) => Some(IoBackend::IoUring),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(_) => Some(IoBackend::IoUring),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(_) => Some(IoBackend::IoUring),
            VectorStorageEnum::DenseTurboMemmap(_) => Some(IoBackend::Mmap),
            VectorStorageEnum::DenseTurboGraphInline(v) => v.io_backend(),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(_) => Some(IoBackend::IoUring),
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::DenseAppendableMemmap(_)
            | VectorStorageEnum::DenseAppendableMemmapByte(_)
            | VectorStorageEnum::DenseAppendableMemmapHalf(_)
            | VectorStorageEnum::DenseTurboAppendableMemmap(_)
            | VectorStorageEnum::SparseVolatile(_)
            | VectorStorageEnum::SparseMmap(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseTurbo(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => None,
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => None,
        }
    }

    fn with_vector_bytes_opt<P: AccessPattern, R>(
        &self,
        key: PointOffsetType,
        f: impl FnOnce(&[u8]) -> R,
    ) -> OperationResult<Option<R>> {
        match self {
            VectorStorageEnum::DenseVolatile(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileHalf(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseMemmap(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseMemmapByte(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseMemmapHalf(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseGraphInline(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseGraphInlineByte(v) => {
                Ok(v.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::DenseGraphInlineHalf(v) => {
                Ok(v.with_dense_bytes_opt::<P, R>(key, f))
            }

            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringByte(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUringHalf(v) => Ok(v.with_dense_bytes_opt::<P, R>(key, f)),

            VectorStorageEnum::DenseAppendableMemmap(v) => {
                Ok(v.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::DenseAppendableMemmapByte(v) => {
                Ok(v.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(v) => {
                Ok(v.with_dense_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::DenseTurboMemmap(v) => Ok(v.with_dense_tq_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseTurboGraphInline(v) => {
                Ok(v.with_dense_tq_bytes_opt::<P, R>(key, f))
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseTurboUring(v) => Ok(v.with_dense_tq_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::DenseTurboAppendableMemmap(v) => {
                Ok(v.with_dense_tq_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::SparseVolatile(v) => v.with_sparse_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::SparseMmap(v) => v.with_sparse_bytes_opt::<P, R>(key, f),
            VectorStorageEnum::MultiDenseVolatile(v) => Ok(v.with_multi_bytes_opt::<P, R>(key, f)),
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileByte(v) => {
                Ok(v.with_multi_bytes_opt::<P, R>(key, f))
            }
            #[cfg(test)]
            VectorStorageEnum::MultiDenseVolatileHalf(v) => {
                Ok(v.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::MultiDenseAppendableMemmap(v) => {
                Ok(v.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapByte(v) => {
                Ok(v.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(v) => {
                Ok(v.with_multi_bytes_opt::<P, R>(key, f))
            }
            VectorStorageEnum::MultiDenseTurbo(v) => Ok(v.with_multi_tq_bytes_opt::<P, R>(key, f)),
            VectorStorageEnum::EmptyDense(_) | VectorStorageEnum::EmptySparse(_) => Ok(None),
        }
    }

    fn vector_bytes_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> OperationResult<Option<Vec<u8>>> {
        match self {
            // Sparse already allocates on serialize — return it without a copy.
            VectorStorageEnum::SparseVolatile(v) => v.sparse_bytes_opt::<P>(key),
            VectorStorageEnum::SparseMmap(v) => v.sparse_bytes_opt::<P>(key),
            // Others expose borrowed bytes; one copy out is unavoidable.
            VectorStorageEnum::DenseVolatile(_)
            | VectorStorageEnum::DenseMemmap(_)
            | VectorStorageEnum::DenseMemmapByte(_)
            | VectorStorageEnum::DenseMemmapHalf(_)
            | VectorStorageEnum::DenseGraphInline(_)
            | VectorStorageEnum::DenseGraphInlineByte(_)
            | VectorStorageEnum::DenseGraphInlineHalf(_)
            | VectorStorageEnum::DenseAppendableMemmap(_)
            | VectorStorageEnum::DenseAppendableMemmapByte(_)
            | VectorStorageEnum::DenseAppendableMemmapHalf(_)
            | VectorStorageEnum::DenseTurboMemmap(_)
            | VectorStorageEnum::DenseTurboGraphInline(_)
            | VectorStorageEnum::DenseTurboAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseVolatile(_)
            | VectorStorageEnum::MultiDenseAppendableMemmap(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
            | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
            | VectorStorageEnum::MultiDenseTurbo(_)
            | VectorStorageEnum::EmptyDense(_)
            | VectorStorageEnum::EmptySparse(_) => {
                self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
            }
            #[cfg(test)]
            VectorStorageEnum::DenseVolatileByte(_)
            | VectorStorageEnum::DenseVolatileHalf(_)
            | VectorStorageEnum::MultiDenseVolatileByte(_)
            | VectorStorageEnum::MultiDenseVolatileHalf(_) => {
                self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
            }
            #[cfg(target_os = "linux")]
            VectorStorageEnum::DenseUring(_)
            | VectorStorageEnum::DenseUringByte(_)
            | VectorStorageEnum::DenseUringHalf(_)
            | VectorStorageEnum::DenseTurboUring(_) => {
                self.with_vector_bytes_opt::<P, _>(key, <[u8]>::to_vec)
            }
        }
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        for_all_vector_storage!(self, v => v.read_vector_bytes::<P, U>(keys, callback))
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        for_all_vector_storage!(self, v => v.size_of_available_vectors_in_bytes())
    }

    fn distance(&self) -> Distance {
        for_all_vector_storage!(self, v => v.distance())
    }

    fn datatype(&self) -> VectorStorageDatatype {
        for_all_vector_storage!(self, v => v.datatype())
    }

    fn is_cold(&self) -> bool {
        for_all_vector_storage!(self, v => v.is_cold())
    }

    fn total_vector_count(&self) -> usize {
        for_all_vector_storage!(self, v => v.total_vector_count())
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        for_all_vector_storage!(self, v => v.get_vector::<P>(key))
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) -> OperationResult<()> {
        for_all_vector_storage!(self, v => v.read_vectors::<P, U>(keys, callback))
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        for_all_vector_storage!(self, v => v.get_vector_opt::<P>(key))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        for_all_vector_storage!(self, v => v.is_deleted_vector(key))
    }

    fn deleted_vector_count(&self) -> usize {
        for_all_vector_storage!(self, v => v.deleted_vector_count())
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        for_all_vector_storage!(self, v => v.deleted_vector_bitslice())
    }

    fn available_vector_count(&self) -> usize {
        for_all_vector_storage!(self, v => v.available_vector_count())
    }
}

impl VectorStorage for VectorStorageEnum {
    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        for_all_vector_storage!(self, v => v.insert_vector(key, vector))
    }

    fn flusher(&self) -> Flusher {
        for_all_vector_storage!(self, v => v.flusher())
    }

    fn files(&self) -> Vec<PathBuf> {
        for_all_vector_storage!(self, v => v.files())
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        for_all_vector_storage!(self, v => v.immutable_files())
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        for_all_vector_storage!(self, v => v.delete_vector(key))
    }
}
