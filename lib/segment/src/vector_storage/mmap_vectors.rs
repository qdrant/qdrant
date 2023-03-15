use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{size_of, transmute};
use std::path::Path;

use memmap2::{Mmap, MmapOptions};

use crate::common::error_logging::LogError;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::madvise;
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectorsStorage;

const HEADER_SIZE: usize = 4;
const VECTORS_HEADER: &[u8; 4] = b"data";

/// Mem-mapped file
pub struct MmapVectors {
    pub dim: usize,
    pub num_vectors: usize,
    mmap: Mmap,
    pub quantized_vectors: Option<QuantizedVectorsStorage>,
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

fn ensure_mmap_file_exists(path: &Path, header: &[u8]) -> OperationResult<()> {
    if path.exists() {
        return Ok(());
    }
    let mut file = File::create(path)?;
    file.write_all(header)?;
    Ok(())
}

impl MmapVectors {
    pub fn open(vectors_path: &Path, dim: usize) -> OperationResult<Self> {
        ensure_mmap_file_exists(vectors_path, VECTORS_HEADER).describe("Create mmap data file")?;

        let mmap = open_read(vectors_path).describe("Open mmap for reading")?;
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        Ok(MmapVectors {
            dim,
            num_vectors,
            mmap,
            quantized_vectors: None,
        })
    }

    pub fn quantize(
        &mut self,
        distance: Distance,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        let vector_data_iterator = (0..self.num_vectors as u32).map(|i| {
            let offset = self.data_offset(i as PointOffsetType).unwrap_or_default();
            self.raw_vector_offset(offset)
        });
        self.quantized_vectors = Some(QuantizedVectorsStorage::create(
            vector_data_iterator,
            quantization_config,
            distance,
            self.dim,
            self.num_vectors,
            data_path,
            true,
        )?);
        Ok(())
    }

    pub fn load_quantization(
        &mut self,
        data_path: &Path,
        distance: Distance,
    ) -> OperationResult<()> {
        if QuantizedVectorsStorage::check_exists(data_path) {
            self.quantized_vectors =
                Some(QuantizedVectorsStorage::load(data_path, true, distance)?);
        }
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

    /// Creates returns owned vector (copy of internal vector)
    pub fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        let offset = self.data_offset(key).unwrap();
        self.raw_vector_offset(offset)
    }
}
