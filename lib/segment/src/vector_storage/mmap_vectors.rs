use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{size_of, transmute};
use std::path::Path;
use std::sync::Arc;

use bitvec::vec::BitVec;
use memmap2::{Mmap, MmapMut, MmapOptions};
use parking_lot::{RwLock, RwLockReadGuard};
use quantization::encoder::EncodingParameters;

use super::quantized_mmap_storage::{QuantizedMmapStorage, QuantizedMmapStorageBuilder};
use super::quantized_vector_storage::QuantizedVectorStorage;
use crate::common::error_logging::LogError;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::madvise;
use crate::types::{Distance, PointOffsetType, QuantizationConfig};

const HEADER_SIZE: usize = 4;
const DELETED_HEADER: &[u8; 4] = b"drop";
const VECTORS_HEADER: &[u8; 4] = b"data";

/// Mem-mapped file with vectors and soft-delete flags
pub struct MmapVectors {
    pub dim: usize,
    pub num_vectors: usize,
    mmap: Mmap,
    deleted_mmap: Arc<RwLock<MmapMut>>,
    pub deleted_count: usize,
    pub deleted_ram: Option<BitVec>,
    pub quantized_vectors: Option<Box<dyn QuantizedVectorStorage>>,
}

fn open_read(path: &Path) -> OperationResult<Mmap> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(true)
        .create(true)
        .open(path)?;

    let mmap = unsafe { MmapOptions::new().map(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;
    Ok(mmap)
}

fn open_write(path: &Path) -> OperationResult<MmapMut> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };
    madvise::madvise(&mmap, madvise::get_global())?;
    Ok(mmap)
}

fn ensure_mmap_file_exists(path: &Path, header: &[u8]) -> OperationResult<()> {
    if path.exists() {
        return Ok(());
    }
    let mut file = File::create(path)?;
    file.write_all(header)?;
    Ok(())
}

impl MmapVectors {
    pub fn open(vectors_path: &Path, deleted_path: &Path, dim: usize) -> OperationResult<Self> {
        ensure_mmap_file_exists(vectors_path, VECTORS_HEADER).describe("Create mmap data file")?;
        ensure_mmap_file_exists(deleted_path, DELETED_HEADER)
            .describe("Create mmap deleted flags file")?;

        let mmap = open_read(vectors_path).describe("Open mmap for reading")?;
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        let deleted_mmap = open_write(deleted_path).describe("Open mmap for writing")?;

        let deleted_count = (HEADER_SIZE..deleted_mmap.len())
            .map(|idx| *deleted_mmap.get(idx).unwrap() as usize)
            .sum();

        Ok(MmapVectors {
            dim,
            num_vectors,
            mmap,
            deleted_mmap: Arc::new(RwLock::new(deleted_mmap)),
            deleted_count,
            deleted_ram: None,
            quantized_vectors: None,
        })
    }

    fn enable_deleted_ram(&mut self) {
        let mut deleted = BitVec::new();
        deleted.resize(self.num_vectors, false);
        for i in 0..self.num_vectors {
            deleted.set(i, self.deleted(i as PointOffsetType).unwrap_or_default());
        }
        self.deleted_ram = Some(deleted);
    }

    fn create_quantized_storage<TStorage, TStorageBuilder>(
        &self,
        meta_path: &Path,
        data_path: &Path,
        encoding_parameters: EncodingParameters,
    ) -> OperationResult<Box<dyn QuantizedVectorStorage>>
    where
        TStorage: quantization::encoder::Storage,
        TStorageBuilder: quantization::encoder::StorageBuilder<TStorage>,
    {
        let vector_data_iterator = (0..self.num_vectors as u32).map(|i| {
            let offset = self.data_offset(i as PointOffsetType).unwrap_or_default();
            self.raw_vector_offset(offset)
        });
        let quantized_vectors = quantization::encoder::EncodedVectors::<Vec<u8>>::encode(
            vector_data_iterator,
            Vec::new(),
            encoding_parameters,
        )
        .map_err(|e| OperationError::service_error(format!("Cannot quantize vector data: {e}")))?;
        quantized_vectors.save(data_path, meta_path)?;
        Ok(Box::new(quantized_vectors))
    }

    pub fn quantize(
        &mut self,
        distance: Distance,
        meta_path: &Path,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        self.enable_deleted_ram();
        let encoding_parameters = EncodingParameters {
            distance_type: match distance {
                Distance::Cosine => quantization::encoder::SimilarityType::Dot,
                Distance::Euclid => quantization::encoder::SimilarityType::L2,
                Distance::Dot => quantization::encoder::SimilarityType::Dot,
            },
            invert: distance == Distance::Euclid,
            quantile: quantization_config.quantile,
        };
        let quantized_vectors = if quantization_config.always_ram == Some(true) {
            self.create_quantized_storage::<Vec<u8>, Vec<u8>>(
                meta_path,
                data_path,
                encoding_parameters,
            )?
        } else {
            self.create_quantized_storage::<QuantizedMmapStorage, QuantizedMmapStorageBuilder>(
                meta_path,
                data_path,
                encoding_parameters,
            )?
        };
        self.quantized_vectors = Some(quantized_vectors);
        Ok(())
    }

    pub fn load_quantization(
        &mut self,
        meta_path: &Path,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        self.enable_deleted_ram();
        self.quantized_vectors = if quantization_config.always_ram == Some(true) {
            Some(Box::new(
                quantization::encoder::EncodedVectors::<Vec<u8>>::load(data_path, meta_path)?,
            ))
        } else {
            Some(Box::new(quantization::encoder::EncodedVectors::<
                QuantizedMmapStorage,
            >::load(data_path, meta_path)?))
        };
        Ok(())
    }

    pub fn data_offset(&self, key: PointOffsetType) -> Option<usize> {
        let vector_data_length = self.dim * size_of::<VectorElementType>();
        let offset = (key as usize) * vector_data_length + HEADER_SIZE;
        if key >= (self.num_vectors as PointOffsetType) {
            return None;
        }
        Some(offset)
    }

    pub fn raw_size(&self) -> usize {
        self.dim * size_of::<VectorElementType>()
    }

    pub fn raw_vector_offset(&self, offset: usize) -> &[VectorElementType] {
        let byte_slice = &self.mmap[offset..(offset + self.raw_size())];
        let arr: &[VectorElementType] = unsafe { transmute(byte_slice) };
        &arr[0..self.dim]
    }

    pub fn raw_vector(&self, key: PointOffsetType) -> Option<&[VectorElementType]> {
        self.data_offset(key)
            .map(|offset| self.raw_vector_offset(offset))
    }

    pub fn check_deleted(mmap: &MmapMut, key: PointOffsetType) -> Option<bool> {
        mmap.get(HEADER_SIZE + (key as usize)).map(|x| *x > 0)
    }

    pub fn read_deleted_map(&self) -> RwLockReadGuard<MmapMut> {
        self.deleted_mmap.read()
    }

    pub fn deleted(&self, key: PointOffsetType) -> Option<bool> {
        if let Some(deleted_ram) = &self.deleted_ram {
            deleted_ram.get(key as usize).map(|x| *x)
        } else {
            Self::check_deleted(&self.deleted_mmap.read(), key)
        }
    }

    /// Creates returns owned vector (copy of internal vector)
    pub fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        match self.deleted(key) {
            None | Some(true) => None,
            Some(false) => self
                .data_offset(key)
                .map(|offset| self.raw_vector_offset(offset).to_vec()),
        }
    }

    pub fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        if key < (self.num_vectors as PointOffsetType) {
            let mut deleted_mmap = self.deleted_mmap.write();
            let flag = deleted_mmap.get_mut((key as usize) + HEADER_SIZE).unwrap();

            if let Some(deleted_ram) = &mut self.deleted_ram {
                deleted_ram.set(key as usize, true);
            }

            if *flag == 0 {
                *flag = 1;
                self.deleted_count += 1;
            }
        }
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let deleted_mmap = self.deleted_mmap.clone();
        Box::new(move || {
            deleted_mmap.read().flush()?;
            Ok(())
        })
    }
}
