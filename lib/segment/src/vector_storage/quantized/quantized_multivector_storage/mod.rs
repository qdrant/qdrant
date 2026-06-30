use std::borrow::Cow;
use std::path::PathBuf;
use std::{iter, slice};

use ahash::HashMapExt as _;
use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use quantization::EncodedVectors;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType};
use crate::types::{MultiVectorComparator, MultiVectorConfig};

mod live_reload;
mod offsets;

pub use offsets::{
    MultivectorOffsetsStorageChunked, MultivectorOffsetsStorageChunkedRead,
    MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam,
};

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Deserialize,
    Serialize,
    bytemuck::Pod,
    bytemuck::Zeroable,
)]
#[repr(C)]
pub struct MultivectorOffset {
    pub start: PointOffsetType,
    pub count: PointOffsetType,
}

pub trait MultivectorOffsets {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset;
}

#[allow(clippy::len_without_is_empty)]
pub trait MultivectorOffsetsStorage: Sized {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset;

    fn iter_offsets(
        &self,
        ids: &[PointOffsetType],
    ) -> impl Iterator<Item = (usize, MultivectorOffset)>;

    fn len(&self) -> usize;

    fn upsert_offset(
        &mut self,
        id: PointOffsetType,
        offset: MultivectorOffset,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn flusher(&self) -> MmapFlusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    fn heap_size_bytes(&self) -> usize;
}

pub struct QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    quantized_storage: QuantizedStorage,
    offsets: TMultivectorOffsetsStorage,
    dim: usize,
    multi_vector_config: MultiVectorConfig,
}

pub type DynScore<'a, Query> = dyn Fn(&Vec<Query>) -> ScoreType + 'a;

impl<QuantizedStorage, TMultivectorOffsetsStorage>
    QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    pub fn storage(&self) -> &QuantizedStorage {
        &self.quantized_storage
    }

    pub fn storage_mut(&mut self) -> &mut QuantizedStorage {
        &mut self.quantized_storage
    }

    pub fn offsets_storage(&self) -> &TMultivectorOffsetsStorage {
        &self.offsets
    }

    pub fn offsets_storage_mut(&mut self) -> &mut TMultivectorOffsetsStorage {
        &mut self.offsets
    }

    pub fn new(
        dim: usize,
        quantized_storage: QuantizedStorage,
        offsets: TMultivectorOffsetsStorage,
        multi_vector_config: MultiVectorConfig,
    ) -> Self {
        Self {
            quantized_storage,
            offsets,
            dim,
            multi_vector_config,
        }
    }

    #[inline]
    pub fn score_points_batch<F>(
        &self,
        point_ids: &[PointOffsetType],
        scorer: F,
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) where
        F: Fn(&DynScore<QuantizedStorage::EncodedQuery>) -> ScoreType,
    {
        debug_assert_eq!(point_ids.len(), scores.len());

        if QuantizedStorage::is_in_ram_or_mmap() {
            // This function is optimized of in-ram access
            // it doesn't do extra mem copies and assume reference access
            self.score_points_batch_in_mem_like(point_ids, scorer, scores, hw_counter);
        } else {
            // Optimized for remote access from external storage,
            // does memory copy, but minimizes assessed to external storage
            self.score_points_batch_uring_like(point_ids, scorer, scores, hw_counter);
        }
    }

    #[inline]
    fn score_points_batch_in_mem_like<F>(
        &self,
        point_ids: &[PointOffsetType],
        scorer: F,
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) where
        F: Fn(&DynScore<QuantizedStorage::EncodedQuery>) -> ScoreType,
    {
        match self.multi_vector_config.comparator {
            MultiVectorComparator::MaxSim => (),
        }

        for (index, &point_id) in point_ids.iter().enumerate() {
            scores[index] = scorer(&|query| {
                self.score_point_max_similarity(query, point_id, hw_counter) // inhibit rustfmt
            });
        }
    }

    #[inline]
    fn score_points_batch_uring_like<F>(
        &self,
        point_ids: &[PointOffsetType],
        scorer: F,
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) where
        F: Fn(&DynScore<QuantizedStorage::EncodedQuery>) -> ScoreType,
    {
        match self.multi_vector_config.comparator {
            MultiVectorComparator::MaxSim => (),
        }

        self.for_each_in_multi_batch(
            point_ids,
            |index, multi_vector| {
                scores[index] = scorer(&|query| {
                    self.score_vector_max_similarity(query, multi_vector, hw_counter)
                });
            },
            hw_counter,
        );
    }

    fn for_each_in_multi_batch(
        &self,
        point_ids: &[PointOffsetType],
        mut callback: impl FnMut(usize, &[Cow<'_, [u8]>]),
        hw_counter: &HardwareCounterCell,
    ) {
        debug_assert!(point_ids.len() <= u32::MAX as usize);

        #[derive(Copy, Clone)]
        struct State {
            index: u32,
            count: u32,
        }

        let mut state = Vec::with_capacity(point_ids.len());
        let mut chunks = Vec::with_capacity(point_ids.len());

        for (index, offset) in self.offsets.iter_offsets(point_ids) {
            for _ in 0..offset.count {
                state.push(State {
                    index: index as u32,
                    count: offset.count,
                });
            }

            chunks.extend(offset.start..offset.start + offset.count);
        }

        hw_counter
            .vector_io_read()
            .incr_delta(chunks.len() * self.quantized_vector_size());

        let mut multi_vectors = ahash::HashMap::new();

        for (chunk_index, vector) in self.quantized_storage.iter_batch(&chunks) {
            let State { index, count } = state[chunk_index];

            if count == 1 {
                callback(index as _, slice::from_ref(&vector));
                continue;
            }

            let multi_vector = multi_vectors
                .entry(index)
                .or_insert_with(SmallVec::<[_; 4]>::new);

            multi_vector.push(vector);

            if multi_vector.len() == count as usize {
                let multi_vector = multi_vectors.remove(&index).expect("multi-vector exists");
                callback(index as _, &multi_vector);
            }
        }

        debug_assert!(multi_vectors.is_empty());
    }

    /// Custom `score_max_similarity` implementation for quantized vectors.
    /// Efficient for io_uring storage implementation.
    fn score_vector_max_similarity(
        &self,
        query: &[QuantizedStorage::EncodedQuery],
        multi_vector: &[Cow<'_, [u8]>],
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let mut sum = 0.0;

        for inner_query in query {
            let mut max_sim = ScoreType::NEG_INFINITY;

            // manual `max_by` for performance
            for vector in multi_vector {
                let sim = self
                    .quantized_storage
                    .score(inner_query, vector, hw_counter);

                if max_sim < sim {
                    max_sim = sim;
                }
            }

            // sum of max similarity
            sum += max_sim;
        }

        sum
    }

    /// Custom `score_max_similarity` implementation for quantized vectors.
    /// Efficient for in-RAM and mmap storage implementations.
    fn score_point_max_similarity(
        &self,
        query: &[QuantizedStorage::EncodedQuery],
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let offset = self.offsets.get_offset(point_id);
        let offsets: SmallVec<[_; 8]> = (offset.start..offset.start + offset.count).collect();

        let mut max_sim: SmallVec<[_; 8]> = SmallVec::new();
        max_sim.resize(query.len(), ScoreType::NEG_INFINITY);

        for (_, vector) in self.quantized_storage.iter_batch(&offsets) {
            for (query_idx, query) in query.iter().enumerate() {
                let sim = self.quantized_storage.score(query, &vector, hw_counter);

                if max_sim[query_idx] < sim {
                    max_sim[query_idx] = sim;
                }
            }
        }

        max_sim.into_iter().sum()
    }

    /// Custom `score_max_similarity` implementation for quantized vectors
    fn score_internal_max_similarity(
        &self,
        vector_a_index: PointOffsetType,
        vector_b_index: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let offset_a = self.offsets.get_offset(vector_a_index);
        let offset_b = self.offsets.get_offset(vector_b_index);
        let mut sum = 0.0;
        for a in 0..offset_a.count {
            let mut max_sim = ScoreType::NEG_INFINITY;
            // manual `max_by` for performance
            for b in 0..offset_b.count {
                let sim = self.quantized_storage.score_internal(
                    offset_a.start + a,
                    offset_b.start + b,
                    hw_counter,
                );
                if sim > max_sim {
                    max_sim = sim;
                }
            }
            // sum of max similarity
            sum += max_sim;
        }
        sum
    }

    pub fn inner_storage(&self) -> &QuantizedStorage {
        &self.quantized_storage
    }

    pub fn inner_vector_offset(&self, id: PointOffsetType) -> MultivectorOffset {
        self.offsets.get_offset(id)
    }

    pub fn vectors_count(&self) -> usize {
        self.offsets.len()
    }
}

impl<QuantizedStorage, TMultivectorOffsetsStorage> EncodedVectors
    for QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    // TODO(colbert): refactor `EncodedVectors` to support multi vector storage after quantization migration
    type EncodedQuery = Vec<QuantizedStorage::EncodedQuery>;

    fn is_in_ram_or_mmap() -> bool {
        QuantizedStorage::is_in_ram_or_mmap()
    }

    fn is_on_disk(&self) -> bool {
        self.quantized_storage.is_on_disk()
    }

    fn encode_query(&self, query: &[VectorElementType]) -> Vec<QuantizedStorage::EncodedQuery> {
        let multi_vector = TypedMultiDenseVectorRef {
            dim: self.dim,
            flattened_vectors: query,
        };
        multi_vector
            .multi_vectors()
            .map(|inner_vector| self.quantized_storage.encode_query(inner_vector))
            .collect()
    }

    fn iter_batch(&self, _: &[PointOffsetType]) -> impl Iterator<Item = (usize, Cow<'_, [u8]>)> {
        unimplemented!("quantized multi-vector storage does not support `iter_batch`");

        #[allow(unreachable_code)]
        iter::empty()
    }

    fn score(&self, _: &Self::EncodedQuery, _: &[u8], _: &HardwareCounterCell) -> f32 {
        unimplemented!("quantized multi-vector storage does not support `score`");
    }

    fn score_point(
        &self,
        query: &Vec<QuantizedStorage::EncodedQuery>,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        match self.multi_vector_config.comparator {
            MultiVectorComparator::MaxSim => self.score_point_max_similarity(query, i, hw_counter),
        }
    }

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        match self.multi_vector_config.comparator {
            MultiVectorComparator::MaxSim => self.score_internal_max_similarity(i, j, hw_counter),
        }
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantized_storage.quantized_vector_size()
    }

    fn encode_internal_vector(
        &self,
        id: PointOffsetType,
    ) -> Option<Vec<QuantizedStorage::EncodedQuery>> {
        let offset = self.offsets.get_offset(id);
        let mut query = Vec::with_capacity(offset.count as usize);
        for i in 0..offset.count {
            let internal_id = offset.start + i;
            query.push(self.quantized_storage.encode_internal_vector(internal_id)?)
        }
        Some(query)
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        let multi_vector = TypedMultiDenseVectorRef {
            dim: self.dim,
            flattened_vectors: vector,
        };

        let inner_vectors_count = self.quantized_storage.vectors_count() as PointOffsetType;
        let offset = if (id as usize) < self.offsets.len() {
            let old_offset = self.offsets.get_offset(id);
            if multi_vector.vectors_count() <= old_offset.count as usize {
                // If the new vector has less or equal number of inner vectors, we can reuse the old offset
                MultivectorOffset {
                    start: old_offset.start,
                    count: multi_vector.vectors_count() as PointOffsetType,
                }
            } else {
                // Otherwise, we need allocate a new offset
                MultivectorOffset {
                    start: inner_vectors_count,
                    count: multi_vector.vectors_count() as PointOffsetType,
                }
            }
        } else {
            MultivectorOffset {
                start: inner_vectors_count,
                count: multi_vector.vectors_count() as PointOffsetType,
            }
        };

        for (i, inner_vector) in multi_vector.multi_vectors().enumerate() {
            self.quantized_storage.upsert_vector(
                offset.start + i as PointOffsetType,
                inner_vector,
                hw_counter,
            )?;
        }
        self.offsets.upsert_offset(id, offset, hw_counter)?;
        Ok(())
    }

    fn vectors_count(&self) -> usize {
        self.offsets.len()
    }

    fn flusher(&self) -> MmapFlusher {
        let quantized_storage_flusher = self.quantized_storage.flusher();
        let offsets_flusher = self.offsets.flusher();
        Box::new(move || {
            quantized_storage_flusher()?;
            offsets_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.quantized_storage.files();
        files.extend(self.offsets.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.quantized_storage.immutable_files();
        files.extend(self.offsets.immutable_files());
        files
    }

    fn heap_size_bytes(&self) -> usize {
        let Self {
            quantized_storage,
            offsets,
            dim: _,
            multi_vector_config: _,
        } = self;

        quantized_storage.heap_size_bytes() + offsets.heap_size_bytes()
    }

    type SupportsBytes = False;
    fn score_bytes(
        &self,
        enabled: Self::SupportsBytes,
        _: &Self::EncodedQuery,
        _: &[u8],
        _: &HardwareCounterCell,
    ) -> f32 {
        match enabled {}
    }
}

impl<QuantizedStorage, TMultivectorOffsetsStorage> MultivectorOffsets
    for QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets.get_offset(idx)
    }
}
