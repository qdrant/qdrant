use std::fs::canonicalize;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use memmap2::MmapMut;
use memory::mmap_type::MmapSlice;
use quantization::{EncodedVectors, VectorParameters};
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType};
use crate::types::{MultiVectorComparator, MultiVectorConfig};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MultivectorOffset {
    pub start: PointOffsetType,
    pub count: PointOffsetType,
}

pub trait MultivectorOffsetsStorage: Sized {
    fn load(path: &Path) -> OperationResult<Self>;

    fn save(&self, path: &Path) -> OperationResult<()>;

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl MultivectorOffsetsStorage for Vec<MultivectorOffset> {
    fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let mut offsets_mmap_type =
            unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(offsets_mmap_type.deref_mut().iter().copied().collect())
    }

    fn save(&self, path: &Path) -> OperationResult<()> {
        create_offsets_file_from_iter(path, self.len(), self.iter().copied())
    }

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self[idx as usize]
    }

    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Debug)]
pub struct MultivectorOffsetsStorageMmap {
    path: PathBuf,
    offsets: MmapSlice<MultivectorOffset>,
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageMmap {
    fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let offsets = unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(Self {
            path: path.to_path_buf(),
            offsets,
        })
    }

    fn save(&self, path: &Path) -> OperationResult<()> {
        if canonicalize(path)? != canonicalize(&self.path)? {
            create_offsets_file_from_iter(path, self.offsets.len(), self.offsets.iter().copied())
        } else {
            // no need to flush, as the mmap is immutable
            Ok(())
        }
    }

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }
}

#[derive(Debug)]
pub struct QuantizedMultivectorStorage<TEncodedQuery, QuantizedStorage, TMultivectorOffsetsStorage>
where
    TEncodedQuery: Sized,
    QuantizedStorage: EncodedVectors<TEncodedQuery>,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    quantized_storage: QuantizedStorage,
    offsets: TMultivectorOffsetsStorage,
    dim: usize,
    multi_vector_config: MultiVectorConfig,
    encoded_query: PhantomData<TEncodedQuery>,
}

impl<TEncodedQuery, QuantizedStorage, TMultivectorOffsetsStorage>
    QuantizedMultivectorStorage<TEncodedQuery, QuantizedStorage, TMultivectorOffsetsStorage>
where
    TEncodedQuery: Sized,
    QuantizedStorage: EncodedVectors<TEncodedQuery>,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    pub fn new(
        dim: usize,
        quantized_storage: QuantizedStorage,
        offsets: TMultivectorOffsetsStorage,
        multi_vector_config: MultiVectorConfig,
    ) -> Self {
        Self {
            dim,
            quantized_storage,
            offsets,
            multi_vector_config,
            encoded_query: PhantomData,
        }
    }

    pub fn save_multi(
        &self,
        data_path: &Path,
        meta_path: &Path,
        offsets_path: &Path,
    ) -> OperationResult<()> {
        self.offsets.save(offsets_path)?;
        Ok(self.quantized_storage.save(data_path, meta_path)?)
    }

    pub fn load_multi(
        data_path: &Path,
        meta_path: &Path,
        offsets_path: &Path,
        vector_parameters: &VectorParameters,
        multi_vector_config: &MultiVectorConfig,
    ) -> OperationResult<Self> {
        Ok(Self {
            dim: vector_parameters.dim,
            quantized_storage: QuantizedStorage::load(data_path, meta_path, vector_parameters)?,
            offsets: TMultivectorOffsetsStorage::load(offsets_path)?,
            multi_vector_config: *multi_vector_config,
            encoded_query: PhantomData,
        })
    }

    /// Custom `score_max_similarity` implementation for quantized vectors
    fn score_point_max_similarity(
        &self,
        query: &Vec<TEncodedQuery>,
        vector_index: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let offset = self.offsets.get_offset(vector_index);
        let mut sum = 0.0;
        for inner_query in query {
            let mut max_sim = ScoreType::NEG_INFINITY;
            // manual `max_by` for performance
            for i in 0..offset.count {
                let sim =
                    self.quantized_storage
                        .score_point(inner_query, offset.start + i, hw_counter);
                if sim > max_sim {
                    max_sim = sim;
                }
            }
            // sum of max similarity
            sum += max_sim;
        }
        sum
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

impl<TEncodedQuery, QuantizedStorage, TMultivectorOffsetsStorage> EncodedVectors<Vec<TEncodedQuery>>
    for QuantizedMultivectorStorage<TEncodedQuery, QuantizedStorage, TMultivectorOffsetsStorage>
where
    TEncodedQuery: Sized,
    QuantizedStorage: EncodedVectors<TEncodedQuery>,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    // TODO(colbert): refactor `EncodedVectors` to support multi vector storage after quantization migration
    fn save(&self, _data_path: &Path, _meta_path: &Path) -> std::io::Result<()> {
        unreachable!("multivector quantized storage should be saved using `self.save_multi` method")
    }

    // TODO(colbert): refactor `EncodedVectors` to support multi vector storage after quantization migration
    fn load(
        _data_path: &Path,
        _meta_path: &Path,
        _vector_parameters: &quantization::VectorParameters,
    ) -> std::io::Result<Self> {
        unreachable!(
            "multivector quantized storage should be loaded using `self.load_multi` method"
        )
    }

    fn encode_query(&self, query: &[VectorElementType]) -> Vec<TEncodedQuery> {
        let multi_vector = TypedMultiDenseVectorRef {
            dim: self.dim,
            flattened_vectors: query,
        };
        multi_vector
            .multi_vectors()
            .map(|inner_vector| self.quantized_storage.encode_query(inner_vector))
            .collect()
    }

    fn score_point(
        &self,
        query: &Vec<TEncodedQuery>,
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
}

pub(super) fn create_offsets_file_from_iter(
    path: &Path,
    count: usize,
    iter: impl Iterator<Item = MultivectorOffset>,
) -> OperationResult<()> {
    path.parent().map(std::fs::create_dir_all);

    let offsets_file_size = count * std::mem::size_of::<MultivectorOffset>();
    let offsets_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        // Don't truncate because we explicitly set the length later
        .truncate(false)
        .open(path)?;
    offsets_file.set_len(offsets_file_size as u64)?;

    let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
    let mut offsets_mmap_type = unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
    let offsets_mut: &mut [MultivectorOffset] = offsets_mmap_type.deref_mut();
    for (dst, src) in offsets_mut.iter_mut().zip(iter) {
        *dst = src;
    }
    offsets_mmap_type.flusher()()?;
    Ok(())
}
