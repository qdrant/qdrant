use std::ops::DerefMut;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};
use memmap2::MmapMut;
use memory::mmap_type::{MmapFlusher, MmapSlice};
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

pub trait MultivectorOffsets {
    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset;
}

#[allow(clippy::len_without_is_empty)]
pub trait MultivectorOffsetsStorage: Sized {
    fn load(path: &Path) -> OperationResult<Self>;

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset;

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
}

pub struct MultivectorOffsetsStorageRam {
    offsets: Vec<MultivectorOffset>,
    path: PathBuf,
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
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageRam {
    fn load(path: &Path) -> OperationResult<Self> {
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

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn upsert_offset(
        &mut self,
        id: PointOffsetType,
        offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        if id as usize >= self.len() {
            self.offsets
                .resize(id as usize + 1, MultivectorOffset::default());
        }
        self.offsets[id as usize] = offset;
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
    offsets: MmapSlice<MultivectorOffset>,
    path: PathBuf,
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
}

impl MultivectorOffsetsStorage for MultivectorOffsetsStorageMmap {
    fn load(path: &Path) -> OperationResult<Self> {
        let offsets_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&offsets_file) }?;
        let offsets = unsafe { MmapSlice::<MultivectorOffset>::try_from(offsets_mmap)? };
        Ok(Self {
            offsets,
            path: path.to_path_buf(),
        })
    }

    fn get_offset(&self, idx: PointOffsetType) -> MultivectorOffset {
        self.offsets[idx as usize]
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn upsert_offset(
        &mut self,
        _id: PointOffsetType,
        _offset: MultivectorOffset,
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert offset in mmap storage",
        ))
    }

    fn flusher(&self) -> MmapFlusher {
        // Mmap storage does not need a flusher, as it is non-appendable and already backed by a file.
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

    pub fn load_multi(
        data_path: &Path,
        meta_path: &Path,
        offsets_path: &Path,
        vector_parameters: &VectorParameters,
        multi_vector_config: &MultiVectorConfig,
    ) -> OperationResult<Self> {
        let offsets = TMultivectorOffsetsStorage::load(offsets_path)?;
        let quantized_storage = QuantizedStorage::load(data_path, meta_path, vector_parameters)?;
        Ok(Self {
            dim: vector_parameters.dim,
            quantized_storage,
            offsets,
            multi_vector_config: *multi_vector_config,
        })
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
    // TODO(colbert): refactor `EncodedVectors` to support multi vector storage after quantization migration
    type EncodedQuery = Vec<QuantizedStorage::EncodedQuery>;

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

fn create_offsets_file_from_iter(
    path: &Path,
    count: usize,
    iter: impl Iterator<Item = MultivectorOffset>,
) -> OperationResult<()> {
    path.parent()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Path must have a parent directory",
            )
        })
        .and_then(std::fs::create_dir_all)?;

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
