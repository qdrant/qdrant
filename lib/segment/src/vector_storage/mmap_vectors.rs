use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{size_of, transmute};
use std::path::Path;
use std::sync::Arc;

use memmap2::{Mmap, MmapMut, MmapOptions};
use parking_lot::{RwLock, RwLockReadGuard};

use crate::common::error_logging::LogError;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::madvise;
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::quantized::quantized_vectors_base::QuantizedVectorsStorage;

const HEADER_SIZE: usize = 4;
const VECTORS_HEADER: &[u8; HEADER_SIZE] = b"data";
const DELETED_HEADER: &[u8; HEADER_SIZE] = b"drop";

/// Mem-mapped file
pub struct MmapVectors {
    pub dim: usize,
    pub num_vectors: usize,
    /// Has an exact size to fit `num_vectors` of vectors.
    mmap: Mmap,
    pub quantized_vectors: Option<QuantizedVectorsStorage>,
    /// Has an exact size to fit deleted flags for `num_vectors`.
    deleted_mmap: Arc<RwLock<MmapMut>>,
    pub deleted_count: usize,
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

/// Ensure the given mmap file exists
///
/// # Arguments
/// * `path`: path of the file.
/// * `header`: header to set when the file is newly created.
/// * `size_bytes`: grow the file to this number of bytes filled with zeroes>
fn ensure_mmap_file_exists(
    path: &Path,
    header: &[u8],
    size_bytes: Option<usize>,
) -> OperationResult<()> {
    if path.exists() {
        return Ok(());
    }
    let mut file = File::create(path)?;
    file.write_all(header)?;
    if let Some(size) = size_bytes {
        file.write_all(&vec![0; size - HEADER_SIZE])?;
    }
    Ok(())
}

impl MmapVectors {
    pub fn open(vectors_path: &Path, deleted_path: &Path, dim: usize) -> OperationResult<Self> {
        // Allocate/open vectors mmap
        ensure_mmap_file_exists(vectors_path, VECTORS_HEADER, None)
            .describe("Create mmap data file")?;
        let mmap = open_read(vectors_path).describe("Open mmap for reading")?;
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        // Allocate/open deleted mmap
        ensure_mmap_file_exists(
            deleted_path,
            DELETED_HEADER,
            Some(num_vectors + HEADER_SIZE),
        )
        .describe("Create mmap data file")?;
        let deleted_mmap = open_write(deleted_path).describe("Open mmap deleted for writing")?;
        let deleted_count = deleted_mmap.as_ref().iter().filter(|b| b != &&0).count();
        debug_assert_eq!(
            num_vectors + HEADER_SIZE,
            deleted_mmap.len(),
            "deleted mmap file size must match number of mmap vectors",
        );

        Ok(MmapVectors {
            dim,
            num_vectors,
            mmap,
            quantized_vectors: None,
            deleted_mmap: Arc::new(RwLock::new(deleted_mmap)),
            deleted_count,
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

    pub fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        let i = key as usize;
        debug_assert!(
            i < self.num_vectors,
            "key must always be in-bound of deleted mmap"
        );

        let mut deleted_mmap = self.deleted_mmap.write();
        let flag = &mut deleted_mmap[HEADER_SIZE + i];
        if *flag == 0 {
            *flag = 1;
            self.deleted_count += 1;
        }
        Ok(())
    }

    pub fn is_deleted(&self, key: PointOffsetType) -> bool {
        Self::check_deleted(&self.deleted_mmap.read(), key)
    }

    #[inline]
    pub fn check_deleted(mmap: &MmapMut, key: PointOffsetType) -> bool {
        mmap[HEADER_SIZE + (key as usize)] != 0
    }

    pub fn read_deleted_map(&self) -> RwLockReadGuard<MmapMut> {
        self.deleted_mmap.read()
    }
}
