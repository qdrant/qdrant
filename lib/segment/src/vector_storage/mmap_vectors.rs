use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{self, size_of, transmute};
use std::ops::DerefMut;
use std::path::Path;
use std::slice;

use bitvec::prelude::BitSlice;
use memmap2::{Mmap, MmapMut, MmapOptions};

use super::div_ceil;
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
    /// Memory mapped file for vector data
    ///
    /// Has an exact size to fit a header and `num_vectors` of vectors.
    mmap: Mmap,
    /// Memory mapped file for deletion flags
    ///
    /// Has an exact size to fit a header and an aligned `BitSlice` for `num_vectors` of vectors.
    ///
    /// This should never be accessed directly, because it shares a mutable reference with
    /// [`deleted_bitslice`]. Use that instead. The sole purpouse of this is to keep ownership of
    /// the mmap, and to properly clean it up when this struct is dropped.
    _deleted_mmap: MmapMut,
    /// A convenient [`BitSlice`] view into the deleted memory map file.
    ///
    /// This has the same lifetime as this struct.
    deleted: &'static mut BitSlice,
    pub deleted_count: usize,
    pub quantized_vectors: Option<QuantizedVectorsStorage>,
}

impl MmapVectors {
    pub fn open(vectors_path: &Path, deleted_path: &Path, dim: usize) -> OperationResult<Self> {
        // Allocate/open vectors mmap
        ensure_mmap_file_size(vectors_path, VECTORS_HEADER, None)
            .describe("Create mmap data file")?;
        let mmap = open_read(vectors_path).describe("Open mmap for reading")?;
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        // Allocate/open deleted mmap
        let deleted_mmap_size = deleted_mmap_size(num_vectors);
        ensure_mmap_file_size(deleted_path, DELETED_HEADER, Some(deleted_mmap_size as u64))
            .describe("Create mmap deleted file")?;
        let mut deleted_mmap =
            open_write(deleted_path).describe("Open mmap deleted for writing")?;

        // Advice kernel that we'll need this page soon so the kernel can prepare
        #[cfg(unix)]
        if let Err(err) = deleted_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advice MADV_WILLNEED for deleted flags: {}", err,);
        }

        // Create convenient BitSlice view over it
        let deleted = unsafe { mmap_to_bitslice(&mut deleted_mmap) };
        let deleted_count = deleted.count_ones();

        Ok(MmapVectors {
            dim,
            num_vectors,
            mmap,
            _deleted_mmap: deleted_mmap,
            deleted,
            deleted_count,
            quantized_vectors: None,
        })
    }

    pub fn quantize(
        &mut self,
        distance: Distance,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        self.lock_deleted_flags();
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
        self.lock_deleted_flags();
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

    pub fn delete(&mut self, key: PointOffsetType) {
        if self.num_vectors <= key as usize {
            return;
        }
        if !self.deleted.replace(key as usize, true) {
            self.deleted_count += 1;
        }
    }

    pub fn is_deleted(&self, key: PointOffsetType) -> bool {
        self.deleted[key as usize]
    }

    pub fn deleted_bitslice(&self) -> &BitSlice {
        self.deleted
    }

    /// Lock memory map of deleted flags into RAM for optimal access performance
    ///
    /// Because the deleted flags are backed by a memory mapped file, its pages may be swapped out
    /// to disk. This will hurt performance in case of quantization because the access times may be
    /// huge. Calling this will lock the deleted flags in memory to prevent this.
    ///
    /// This is only supported on Unix.
    fn lock_deleted_flags(&mut self) {
        #[cfg(unix)]
        if let Err(err) = self._deleted_mmap.lock() {
            log::error!(
                "Failed to lock deleted flags for quantized mmap segment in memory: {}",
                err,
            );
        }
    }
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

/// Ensure the given mmap file exists and is the given size
///
/// # Arguments
/// * `path`: path of the file.
/// * `header`: header to set when the file is newly created.
/// * `size`: set the file size in bytes, filled with zeroes.
fn ensure_mmap_file_size(path: &Path, header: &[u8], size: Option<u64>) -> OperationResult<()> {
    // If it exists, only set the length
    if path.exists() {
        if let Some(size) = size {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(size)?;
        }
        return Ok(());
    }

    // Create file, and make it the correct size
    let mut file = File::create(path)?;
    file.write_all(header)?;
    if let Some(size) = size {
        if size > header.len() as u64 {
            file.set_len(size)?;
        }
    }
    Ok(())
}

/// Get start position of flags `BitSlice` in deleted mmap.
#[inline]
const fn deleted_mmap_data_start() -> usize {
    let align = mem::align_of::<usize>();
    div_ceil(HEADER_SIZE, align) * align
}

/// Calculate size for deleted mmap to hold the given number of vectors.
///
/// The mmap will hold a file header and an aligned `BitSlice`.
fn deleted_mmap_size(num: usize) -> usize {
    let unit_size = mem::size_of::<usize>();
    let num_bytes = div_ceil(num, 8);
    let num_usizes = div_ceil(num_bytes, unit_size);
    let data_size = num_usizes * unit_size;
    deleted_mmap_data_start() + data_size
}

/// Create a convenient [`BitSlice`] view over a [`MmapMut`].
///
/// This works because the internal memory mapped slice is pinned and doesn't move in memory.
///
/// This is unsafe because we create a shared mutable refrence with lifetime `'a`.
///
/// - The mmap and BitSlice should never be mutated together.
/// - The bitslice reference should never outlive the mmap.
/// - The caller is responsible for handling this with care.
unsafe fn mmap_to_bitslice<'a>(mmap: &mut MmapMut) -> &'a mut BitSlice {
    // Obtain static slice into mmap
    let slice: &'static mut [u8] = {
        let slice = mmap.deref_mut();
        slice::from_raw_parts_mut(slice.as_mut_ptr(), slice.len())
    };

    // Reslice to aligned data portion
    let data_start = deleted_mmap_data_start();
    let slice: &mut [u8] = &mut slice[data_start..];

    // Create BitSlice view over data slice
    // Transmute: &mut [u8] -> &mut [usize] -> &mut BitSlice
    debug_assert_eq!(slice.as_ptr() as usize % mem::align_of::<usize>(), 0);
    debug_assert_eq!(slice.len() % mem::size_of::<usize>(), 0);
    BitSlice::from_slice_unchecked_mut(slice::from_raw_parts_mut(
        slice.as_ptr() as *mut usize,
        slice.len() / mem::size_of::<usize>(),
    ))
}
