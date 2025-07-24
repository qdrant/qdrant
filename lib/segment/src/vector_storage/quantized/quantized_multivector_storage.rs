use std::ops::DerefMut;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use memmap2::MmapMut;
use memory::mmap_type::{MmapFlusher, MmapSlice};
use quantization::EncodedVectors;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType};
use crate::types::{MultiVectorComparator, MultiVectorConfig};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
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

    fn len(&self) -> usize;

    fn push_offset(
        &mut self,
        offset: MultivectorOffset,
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn flusher(&self) -> MmapFlusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;
}

pub struct MultivectorOffsetsStorageRam {
    path: PathBuf,
    offsets: Vec<MultivectorOffset>,
}

impl MultivectorOffsetsStorageRam {
    pub fn create(
        path: &Path,
        offsets: impl Iterator<Item = MultivectorOffset>,
    ) -> OperationResult<Self> {
        let offsets: Vec<_> = offsets.collect();
        create_offsets_file_from_iter(path, offsets.len(), offsets.iter().cloned())?;
        Ok(MultivectorOffsetsStorageRam {
            path: path.to_path_buf(),
            offsets,
        })
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let mut offsets_mmap_type =
            unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(MultivectorOffsetsStorageRam {
            offsets: offsets_mmap_type.deref_mut().iter().copied().collect(),
            path: path.to_path_buf(),
        })
    }
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageRam {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn push_offset(
        &mut self,
        offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.offsets.push(offset);
        Ok(())
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
}

#[derive(Debug)]
pub struct MultivectorOffsetsStorageMmap {
    path: PathBuf,
    offsets: MmapSlice<MultivectorOffset>,
}

impl MultivectorOffsetsStorageMmap {
    pub fn create(
        path: &Path,
        offsets: impl Iterator<Item = MultivectorOffset>,
        count: usize,
    ) -> OperationResult<Self> {
        create_offsets_file_from_iter(path, count, offsets)?;
        MultivectorOffsetsStorageMmap::load(path)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let offsets = unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(Self {
            path: path.to_path_buf(),
            offsets,
        })
    }
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageMmap {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn push_offset(
        &mut self,
        _offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        unreachable!("Cannot push offset to mmap storage");
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
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

impl<QuantizedStorage, TMultivectorOffsetsStorage>
    QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    pub fn storage(&self) -> &QuantizedStorage {
        &self.quantized_storage
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

    /// Custom `score_max_similarity` implementation for quantized vectors
    fn score_point_max_similarity(
        &self,
        query: &Vec<QuantizedStorage::EncodedQuery>,
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

impl<QuantizedStorage, TMultivectorOffsetsStorage> EncodedVectors
    for QuantizedMultivectorStorage<QuantizedStorage, TMultivectorOffsetsStorage>
where
    QuantizedStorage: EncodedVectors,
    TMultivectorOffsetsStorage: MultivectorOffsetsStorage,
{
    // TODO(colbert): refactor `EncodedVectors` to store flattened vector data
    type EncodedQuery = Vec<QuantizedStorage::EncodedQuery>;

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

    fn encode_internal_vector(&self, id: u32) -> Option<Vec<QuantizedStorage::EncodedQuery>> {
        let offset = self.offsets.get_offset(id);
        let mut query = Vec::with_capacity(offset.count as usize);
        for i in 0..offset.count {
            let internal_id = offset.start + i;
            query.push(self.quantized_storage.encode_internal_vector(internal_id)?)
        }
        Some(query)
    }

    fn push_vector(
        &mut self,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        let multi_vector = TypedMultiDenseVectorRef {
            dim: self.dim,
            flattened_vectors: vector,
        };

        let old_inner_vectors_count = self.quantized_storage.vectors_count();
        for inner_vector in multi_vector.multi_vectors() {
            self.quantized_storage
                .push_vector(inner_vector, hw_counter)?;
        }

        let offset: MultivectorOffset = MultivectorOffset {
            start: old_inner_vectors_count as PointOffsetType,
            count: multi_vector.vectors_count() as PointOffsetType,
        };
        self.offsets.push_offset(offset, hw_counter)?;
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
