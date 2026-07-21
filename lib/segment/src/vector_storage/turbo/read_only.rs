//! Read-only counterpart of [`TurboVectorStorage`](super::TurboVectorStorage).
//!
//! Opens the TurboQuant-encoded blob and deletion flags over an arbitrary
//! [`UniversalRead`] backend (mmap / cache / remote), so a read-only segment can
//! retrieve and score a `Turbo4`-typed vector storage. The quantizer is fully
//! determined by `(dim, distance)` (fixed TQDT constants), so nothing beyond the
//! encoded bytes and flags is read from disk.
//!
//! Scoring is reused verbatim from the writable storage via the shared
//! [`TurboScoring`](super::TurboScoring) trait — this storage only provides the
//! read surface it needs.

use std::borrow::Cow;
use std::path::Path;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::{AccessPattern, Random};
use common::mmap::AdviceSetting;
use common::sorted_slice::SortedSlice;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs, UserData};
use quantization::EncodedStorage;
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;
use smallvec::{SmallVec, smallvec};

use super::multi::{OFFSETS_PATH, TurboMultiScoring};
use super::{DELETED_PATH, TQDT_BITS, TQDT_MODE, TQDT_ROTATION, TurboScoring, VECTORS_PATH};
use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, VectorElementType,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::vector_storage_base::VectorStorageRead;
use crate::vector_storage::{DenseTQVectorStorageRead, MultiTQVectorStorageRead, VectorOffsetType};

/// Read-only TurboQuant encoded backend over a [`UniversalRead`] `S`.
///
/// Read-only counterpart of the writable
/// [`TurboEncodedVectorStorage`](super::turbo_encoded_vectors::TurboEncodedVectorStorage):
/// the storage layout is picked at open time from the segment's storage type.
enum ReadOnlyTurboEncoded<S: UniversalRead> {
    /// Single-file layout, written by the non-appendable `Mmap` / `InRamMmap`.
    Single(QuantizedStorage<S>),
    /// Chunked layout, written by the appendable `ChunkedMmap` / `InRamChunkedMmap`.
    Chunked(QuantizedChunkedStorageRead<S>),
}

impl<S: UniversalRead> ReadOnlyTurboEncoded<S> {
    fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            Self::Single(s) => s.get_vector_data(key),
            Self::Chunked(s) => s.get_vector_data(key),
        }
    }

    fn get_quantized_vector_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Single(s) => s.get_vector_data_opt(key),
            Self::Chunked(s) => s.get_vector_data_opt(key),
        }
    }

    /// Run `f` for each vector in the batch, batching the underlying reads where
    /// the backend supports it (the single-file backend pipelines io_uring /
    /// prefetches mmap; the chunked backend reads one record at a time).
    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        mut f: F,
    ) -> OperationResult<()> {
        match self {
            Self::Single(s) => s.for_each_in_batch(keys, f),
            Self::Chunked(s) => {
                for (idx, &key) in keys.iter().enumerate() {
                    f(idx, &s.get_vector_data(key));
                }
                Ok(())
            }
        }
    }

    fn vectors_count(&self) -> usize {
        match self {
            Self::Single(s) => s.vectors_count(),
            Self::Chunked(s) => s.vectors_count(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            Self::Single(s) => s.is_on_disk(),
            Self::Chunked(s) => s.is_on_disk(),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Single(s) => {
                s.populate();
                Ok(())
            }
            Self::Chunked(s) => s.populate(),
        }
    }
}

/// Read-only TurboQuant dense vector storage over a [`UniversalRead`] backend.
pub struct ReadOnlyTurboVectorStorage<S: UniversalRead> {
    /// Read-only encoded vector blob over one of the two backends.
    storage: ReadOnlyTurboEncoded<S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyTurboVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}

impl<S: UniversalRead> ReadOnlyTurboVectorStorage<S> {
    /// Build the quantizer for a `Turbo4` storage; fully determined by
    /// `(dim, distance)` and the fixed TQDT constants (matches the writable
    /// `open_turbo_vector_storage_impl`).
    fn build_quantizer(dim: usize, distance: Distance) -> TurboQuantizer {
        TurboQuantizer::new(
            dim,
            TQDT_BITS,
            TQDT_MODE,
            distance.into(),
            TQDT_ROTATION,
            None,
        )
    }

    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        chunked: bool,
        populate: Populate,
    ) -> OperationResult<()> {
        let vectors_path = path.join(VECTORS_PATH);
        if chunked {
            QuantizedChunkedStorageRead::<S>::preopen(fs, &vectors_path, populate)?;
        } else {
            QuantizedStorage::<S>::preopen(fs, &vectors_path, populate)?;
        }
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_PATH))?;
        Ok(())
    }

    /// Open the read-only counterpart of a `Turbo4` dense storage at `path`,
    /// threading every file open through `fs`; reads the existing layout but
    /// creates and writes nothing. `chunked` selects the on-disk layout the
    /// writable storage produced for the segment's storage type.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        chunked: bool,
        populate: Populate,
    ) -> OperationResult<Self> {
        let quantizer = Self::build_quantizer(dim, distance);
        let quantized_vector_size = quantizer.quantized_size();
        let vectors_path = path.join(VECTORS_PATH);

        let storage = if chunked {
            ReadOnlyTurboEncoded::Chunked(QuantizedChunkedStorageRead::open(
                fs,
                &vectors_path,
                quantized_vector_size,
            )?)
        } else {
            ReadOnlyTurboEncoded::Single(QuantizedStorage::from_file(
                fs,
                &vectors_path,
                quantized_vector_size,
            )?)
        };

        // The read-only backends map lazily; warm them when the load profile asks.
        if !matches!(populate, Populate::No) {
            storage.populate()?;
        }

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_PATH))?;

        Ok(Self {
            storage,
            quantizer,
            deleted,
            distance,
            dim,
        })
    }

    /// Whether scores must be negated to follow qdrant's "higher = better"
    /// convention (mirrors `TurboVectorStorage::invert_score`).
    fn invert_score(&self) -> bool {
        matches!(self.distance, Distance::Euclid | Distance::Manhattan)
    }

    /// Dequantize + inverse-rotate a stored encoded vector back to `f32`,
    /// dropping the padding tail (mirrors `TurboVectorStorage::dequantize_vector`).
    fn dequantize_vector(&self, quantized: Cow<[u8]>) -> CowVector<'_> {
        let mut dequantized = self.quantizer.dequantize::<f64>(&quantized);
        self.quantizer.apply_inverse_rotation(&mut dequantized);
        CowVector::Dense(Cow::Owned(
            dequantized[..self.dim]
                .iter()
                .map(|i| *i as f32)
                .collect::<Vec<_>>(),
        ))
    }
}

impl<S: UniversalRead> VectorStorageRead for ReadOnlyTurboVectorStorage<S> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.quantized_vector_size()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Turbo4
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.storage.vectors_count()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.dequantize_vector(self.storage.get_quantized_vector(key))
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                let vector = self.dequantize_vector(Cow::Borrowed(bytes));
                callback(user_data[idx], point_offsets[idx], vector);
            })
            .expect("read TQ vectors");
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        Some(self.dequantize_vector(self.storage.get_quantized_vector_opt(key)?))
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
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        self.read_dense_tq_bytes::<P, U>(keys, callback)
    }
}

impl<S: UniversalRead> DenseTQVectorStorageRead for ReadOnlyTurboVectorStorage<S> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn get_dense_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }

    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.storage.for_each_in_batch(keys, f)
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        // Same parallel-arrays split as `read_vectors`, minus the dequantization.
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(&point_offsets, |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            })
    }
}

impl<S: UniversalRead> TurboScoring for ReadOnlyTurboVectorStorage<S> {
    fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        let preprocessed = match self.distance {
            Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Dot => <DotProductMetric as Metric<VectorElementType>>::preprocess(query),
            Distance::Manhattan => {
                <ManhattanMetric as Metric<VectorElementType>>::preprocess(query)
            }
        };
        self.quantizer.precompute_query(&preprocessed)
    }

    fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        let score = self.quantizer.score_precomputed(query, bytes);
        if self.invert_score() { -score } else { score }
    }

    fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
    ) -> ScoreType {
        let v1 = self.storage.get_quantized_vector(point_a);
        let v2 = self.storage.get_quantized_vector(point_b);
        let score = self.quantizer.score_symmetric(&v1, &v2);
        if self.invert_score() { -score } else { score }
    }

    fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.storage.get_quantized_vector(key)
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyTurboVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up vectors a writer appended (chunked backend only; the single-file
    /// layout is immutable) and patch the in-memory deletion flags.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let ReadOnlyTurboEncoded::Chunked(storage) = &mut self.storage {
            storage.live_reload(fs, deleted_points, new_points, hw_counter)?;
        }
        self.deleted.insert_all(deleted_points);
        Ok(())
    }
}

/// Read-only TurboQuant multivector vector storage over a [`UniversalRead`]
/// backend. Read-only counterpart of
/// [`TurboMultiVectorStorage`](super::multi::TurboMultiVectorStorage).
///
/// Multivectors are always stored in the appendable chunked layout, so — unlike
/// the dense storage — there is a single backend.
pub struct ReadOnlyTurboMultiVectorStorage<S: UniversalRead> {
    /// Flat inner-vector space: one fixed-size encoded record per inner vector.
    storage: QuantizedChunkedStorageRead<S>,
    /// Maps each point to its record range in the inner space.
    offsets: ChunkedVectorsRead<MultivectorMmapOffset, S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) inner vector dimensionality.
    dim: usize,
    multi_vector_config: MultiVectorConfig,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyTurboMultiVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyTurboMultiVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.offsets.len())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}

impl<S: UniversalRead> ReadOnlyTurboMultiVectorStorage<S> {
    /// Schedule background prefetch of the files [`Self::open`] will read.
    ///
    /// Absent files are skipped rather than reported: the subsequent open is
    /// the one to produce the error.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<()> {
        QuantizedChunkedStorageRead::<S>::preopen(fs, &path.join(VECTORS_PATH), populate)?;
        ChunkedVectorsRead::<MultivectorMmapOffset, S>::preopen(
            fs,
            &path.join(OFFSETS_PATH),
            advice,
            populate,
        )?;
        InMemoryBitvecFlags::preopen(fs, &path.join(DELETED_PATH))?;
        Ok(())
    }

    /// Open the read-only counterpart of a `Turbo4` multivector storage at
    /// `path`, threading every file open through `fs`; reads the existing layout
    /// but creates and writes nothing.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        dim: usize,
        distance: Distance,
        multi_vector_config: MultiVectorConfig,
        advice: AdviceSetting,
        populate: Populate,
    ) -> OperationResult<Self> {
        let quantizer = ReadOnlyTurboVectorStorage::<S>::build_quantizer(dim, distance);

        let storage = QuantizedChunkedStorageRead::open(
            fs,
            &path.join(VECTORS_PATH),
            quantizer.quantized_size(),
        )?;
        // The chunked read backend maps lazily; warm it when the profile asks.
        if !matches!(populate, Populate::No) {
            storage.populate()?;
        }

        let offsets = ChunkedVectorsRead::open(fs, &path.join(OFFSETS_PATH), 1, advice, populate)?;

        let deleted = InMemoryBitvecFlags::open::<S>(fs, &path.join(DELETED_PATH))?;

        Ok(Self {
            storage,
            offsets,
            quantizer,
            deleted,
            distance,
            dim,
            multi_vector_config,
        })
    }

    /// Offset record for `key`, if the point exists.
    fn get_offset<P: AccessPattern>(&self, key: PointOffsetType) -> Option<MultivectorMmapOffset> {
        let record = self.offsets.get::<P>(key as VectorOffsetType)?;
        let &[offset] = record.as_ref() else {
            debug_assert!(
                false,
                "multi-vector offsets are stored as vectors of length 1"
            );
            return None;
        };
        Some(offset)
    }

    fn invert_score(&self) -> bool {
        matches!(self.distance, Distance::Euclid | Distance::Manhattan)
    }

    fn signed(&self, score: f32) -> ScoreType {
        if self.invert_score() { -score } else { score }
    }

    /// Decode one inner record into `out`: dequantize, rotate back, drop padding.
    fn dequantize_inner_into(&self, encoded: &[u8], out: &mut Vec<VectorElementType>) {
        let mut dequantized = self.quantizer.dequantize::<f64>(encoded);
        self.quantizer.apply_inverse_rotation(&mut dequantized);
        out.extend(
            dequantized[..self.dim]
                .iter()
                .map(|&x| x as VectorElementType),
        );
    }

    /// Decode the full multivector behind an offset record.
    fn dequantize_multi(&self, offset: MultivectorMmapOffset) -> CowVector<'_> {
        let records = self
            .storage
            .get_many::<Random>(offset.offset, offset.count as usize)
            .expect("Multivector not found");
        self.dequantize_records(&records)
    }

    /// Decode concatenated encoded records into a multivector.
    fn dequantize_records(&self, records: &[u8]) -> CowVector<'_> {
        let record_size = self.quantizer.quantized_size();
        let mut flattened = Vec::with_capacity(records.len() / record_size * self.dim);
        for encoded in records.chunks_exact(record_size) {
            self.dequantize_inner_into(encoded, &mut flattened);
        }
        CowVector::MultiDense(CowMultiVector::Owned(TypedMultiDenseVector {
            flattened_vectors: flattened,
            dim: self.dim,
        }))
    }

    /// Two-pass batched read for records: first offsets, then vectors.
    fn for_each_record_range<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, &[u8]),
    ) -> OperationResult<()> {
        let offset_keys = keys
            .into_iter()
            .map(|(user_data, key)| ((user_data, key), key, 1));

        let mut ranges = Vec::with_capacity(offset_keys.size_hint().0);
        self.offsets
            .for_each_vector::<P, _>(offset_keys, |(user_data, key), record| {
                let &[offset] = record.as_ref() else {
                    unreachable!("multi-vector offsets are stored as vectors of length 1");
                };
                ranges.push(((user_data, key), offset.offset, offset.count));
                Ok(())
            })?;

        self.storage
            .for_each_many::<P, _>(ranges.into_iter(), |(user_data, key), records| {
                callback(user_data, key, records.as_ref());
                Ok(())
            })
    }
}

impl<S: UniversalRead> VectorStorageRead for ReadOnlyTurboMultiVectorStorage<S> {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Turbo4
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.offsets.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        Some(self.dequantize_multi(self.get_offset::<P>(key)?))
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

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.total_vector_count() > 0 {
            let total_size = self.storage.vectors_count() * self.quantizer.quantized_size();
            (total_size as u128 * self.available_vector_count() as u128
                / self.total_vector_count() as u128) as usize
        } else {
            0
        }
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        self.for_each_record_range::<P, _>(keys, |user_data, key, records| {
            callback(user_data, key, self.dequantize_records(records));
        })
        .expect("read TQ multivectors");
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
    ) -> OperationResult<()> {
        self.for_each_record_range::<P, _>(keys, |user_data, key, records| {
            callback(user_data, key, records.to_vec());
        })
    }
}

impl<S: UniversalRead> MultiTQVectorStorageRead for ReadOnlyTurboMultiVectorStorage<S> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }

    fn get_multi_tq<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_offset::<P>(key)
            .and_then(|offset| {
                self.storage
                    .get_many::<P>(offset.offset, offset.count as usize)
            })
            .expect("Multivector not found")
    }
}

impl<S: UniversalRead> TurboMultiScoring for ReadOnlyTurboMultiVectorStorage<S> {
    fn preprocess_query(&self, query: &MultiDenseVectorInternal) -> Vec<EncodedQueryTQ> {
        query
            .multi_vectors()
            .map(|inner| {
                let preprocessed = match self.distance {
                    Distance::Cosine => {
                        <CosineMetric as Metric<VectorElementType>>::preprocess(inner.to_vec())
                    }
                    Distance::Euclid => {
                        <EuclidMetric as Metric<VectorElementType>>::preprocess(inner.to_vec())
                    }
                    Distance::Dot => {
                        <DotProductMetric as Metric<VectorElementType>>::preprocess(inner.to_vec())
                    }
                    Distance::Manhattan => {
                        <ManhattanMetric as Metric<VectorElementType>>::preprocess(inner.to_vec())
                    }
                };
                self.quantizer.precompute_query(&preprocessed)
            })
            .collect()
    }

    fn score_point_max_similarity(
        &self,
        query: &[EncodedQueryTQ],
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let Some(offset) = self.get_offset::<Random>(key) else {
            log::error!("Multivector not found");
            return ScoreType::NEG_INFINITY;
        };

        let records = self
            .storage
            .get_many::<Random>(offset.offset, offset.count as usize)
            .expect("Multivector not found");

        hw_counter
            .cpu_counter()
            .incr_delta(records.len() * query.len());
        hw_counter.vector_io_read().incr_delta(records.len());

        let mut max_sim: SmallVec<[_; 8]> = smallvec![ScoreType::NEG_INFINITY; query.len()];

        for bytes in records.chunks_exact(self.quantizer.quantized_size()) {
            for (qi, inner_query) in query.iter().enumerate() {
                let sim = self.signed(self.quantizer.score_precomputed(inner_query, bytes));
                if max_sim[qi] < sim {
                    max_sim[qi] = sim;
                }
            }
        }
        max_sim.into_iter().sum()
    }

    fn score_internal_max_similarity(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let (Some(offset_a), Some(offset_b)) = (
            self.get_offset::<Random>(point_a),
            self.get_offset::<Random>(point_b),
        ) else {
            log::error!("Multivector not found");
            return ScoreType::NEG_INFINITY;
        };

        let records_a = self
            .storage
            .get_many::<Random>(offset_a.offset, offset_a.count as usize)
            .expect("Multivector not found");

        let records_b = self
            .storage
            .get_many::<Random>(offset_b.offset, offset_b.count as usize)
            .expect("Multivector not found");

        hw_counter
            .cpu_counter()
            .incr_delta(records_a.len() * offset_b.count as usize);
        hw_counter
            .vector_io_read()
            .incr_delta(records_a.len() + records_b.len());

        let quantized_size = self.quantizer.quantized_size();
        let mut sum = 0.0;
        for bytes_a in records_a.chunks_exact(quantized_size) {
            let mut max_sim = ScoreType::NEG_INFINITY;
            for bytes_b in records_b.chunks_exact(quantized_size) {
                let sim = self.signed(self.quantizer.score_symmetric(bytes_a, bytes_b));
                if sim > max_sim {
                    max_sim = sim;
                }
            }
            sum += max_sim;
        }
        sum
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyTurboMultiVectorStorage<S> {
    type Fs = S::Fs;

    /// Pick up multivectors a writer appended (records + offsets) and patch the
    /// in-memory deletion flags.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.offsets
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.deleted.insert_all(deleted_points);
        Ok(())
    }
}
