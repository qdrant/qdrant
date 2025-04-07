use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::{self, MaybeUninit, size_of, transmute};
use std::path::Path;
use std::sync::Arc;

use bitvec::prelude::BitSlice;
use common::ext::BitSliceExt as _;
use common::maybe_uninit::maybe_uninit_fill_from;
use common::types::PointOffsetType;
use memmap2::Mmap;
use memory::madvise::{Advice, AdviceSetting, Madviseable};
use memory::mmap_ops;
use memory::mmap_type::{MmapBitSlice, MmapFlusher};
use parking_lot::Mutex;

use crate::common::error_logging::LogError;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
#[cfg(target_os = "linux")]
use crate::vector_storage::async_io::UringReader;
#[cfg(not(target_os = "linux"))]
use crate::vector_storage::async_io_mock::UringReader;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient_points;

const HEADER_SIZE: usize = 4;
const VECTORS_HEADER: &[u8; HEADER_SIZE] = b"data";
const DELETED_HEADER: &[u8; HEADER_SIZE] = b"drop";

/// Mem-mapped file for dense vectors
#[derive(Debug)]
pub struct MmapDenseVectors<T: PrimitiveVectorElement> {
    pub dim: usize,
    pub num_vectors: usize,
    /// Memory mapped file for vector data
    ///
    /// Has an exact size to fit a header and `num_vectors` of vectors.
    mmap: Arc<Mmap>,
    /// Same as `mmap`, but with `Advice::Sequential` set
    /// for better performance when reading vectors sequentially.
    mmap_sequential: Arc<Mmap>,
    /// Context for io_uring-base async IO
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    uring_reader: Mutex<Option<UringReader<T>>>,
    /// Memory mapped deletion flags
    deleted: MmapBitSlice,
    /// Current number of deleted vectors.
    pub deleted_count: usize,
}

impl<T: PrimitiveVectorElement> MmapDenseVectors<T> {
    pub fn open(
        vectors_path: &Path,
        deleted_path: &Path,
        dim: usize,
        with_async_io: bool,
    ) -> OperationResult<Self> {
        // Allocate/open vectors mmap
        ensure_mmap_file_size(vectors_path, VECTORS_HEADER, None)
            .describe("Create mmap data file")?;
        let mmap = mmap_ops::open_read_mmap(vectors_path, AdviceSetting::Global, false)
            .describe("Open mmap for reading")?;

        let seq_mmap = mmap_ops::open_read_mmap(
            vectors_path,
            AdviceSetting::Advice(Advice::Sequential),
            false,
        )
        .describe("Open mmap for sequential reading")?;

        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<T>();

        // Allocate/open deleted mmap
        let deleted_mmap_size = deleted_mmap_size(num_vectors);
        ensure_mmap_file_size(deleted_path, DELETED_HEADER, Some(deleted_mmap_size as u64))
            .describe("Create mmap deleted file")?;
        let deleted_mmap = mmap_ops::open_write_mmap(deleted_path, AdviceSetting::Global, false)
            .describe("Open mmap deleted for writing")?;

        // Advise kernel that we'll need this page soon so the kernel can prepare
        #[cfg(unix)]
        if let Err(err) = deleted_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advise MADV_WILLNEED for deleted flags: {err}");
        }

        // Transform into mmap BitSlice
        let deleted = MmapBitSlice::try_from(deleted_mmap, deleted_mmap_data_start())?;
        let deleted_count = deleted.count_ones();

        let uring_reader = if with_async_io {
            // Keep file handle open for async IO
            let vectors_file = File::open(vectors_path)?;
            let raw_size = dim * size_of::<T>();
            Some(UringReader::new(vectors_file, raw_size, HEADER_SIZE)?)
        } else {
            None
        };

        Ok(MmapDenseVectors {
            dim,
            num_vectors,
            mmap: mmap.into(),
            mmap_sequential: seq_mmap.into(),
            uring_reader: Mutex::new(uring_reader),
            deleted,
            deleted_count,
        })
    }

    pub fn has_async_reader(&self) -> bool {
        self.uring_reader.lock().is_some()
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.deleted.flusher()
    }

    pub fn data_offset(&self, key: PointOffsetType) -> Option<usize> {
        let vector_data_length = self.dim * size_of::<T>();
        let offset = (key as usize) * vector_data_length + HEADER_SIZE;
        if key >= (self.num_vectors as PointOffsetType) {
            return None;
        }
        Some(offset)
    }

    pub fn raw_size(&self) -> usize {
        self.dim * size_of::<T>()
    }

    fn raw_vector_offset(&self, offset: usize) -> &[T] {
        let byte_slice = &self.mmap[offset..(offset + self.raw_size())];
        let arr: &[T] = unsafe { transmute(byte_slice) };
        &arr[0..self.dim]
    }

    fn raw_vector_offset_sequential(&self, offset: usize) -> &[T] {
        let byte_slice = &self.mmap_sequential[offset..(offset + self.raw_size())];
        let arr: &[T] = unsafe { transmute(byte_slice) };
        &arr[0..self.dim]
    }

    /// Returns reference to vector data by key
    fn get_vector(&self, key: PointOffsetType) -> &[T] {
        self.get_vector_opt(key).expect("vector not found")
    }

    /// Returns an optional reference to vector data by key
    pub fn get_vector_opt(&self, key: PointOffsetType) -> Option<&[T]> {
        self.data_offset(key)
            .map(|offset| self.raw_vector_offset(offset))
    }

    pub fn get_vector_opt_sequential(&self, key: PointOffsetType) -> Option<&[T]> {
        self.data_offset(key)
            .map(|offset| self.raw_vector_offset_sequential(offset))
    }

    pub fn get_vectors<'a>(
        &'a self,
        keys: &[PointOffsetType],
        vectors: &'a mut [MaybeUninit<&'a [T]>],
    ) -> &'a [&'a [T]] {
        debug_assert_eq!(keys.len(), vectors.len());
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);
        if is_read_with_prefetch_efficient_points(keys) {
            maybe_uninit_fill_from(
                vectors,
                keys.iter()
                    .map(|key| self.get_vector_opt_sequential(*key).unwrap_or(&[])),
            )
            .0
        } else {
            maybe_uninit_fill_from(vectors, keys.iter().map(|key| self.get_vector(*key))).0
        }
    }

    /// Marks the key as deleted.
    ///
    /// Returns true if the key was not deleted before, and it is now deleted.
    pub fn delete(&mut self, key: PointOffsetType) -> bool {
        let is_deleted = !self.deleted.replace(key as usize, true);
        if is_deleted {
            self.deleted_count += 1;
        }
        is_deleted
    }

    pub fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get_bit(key as usize).unwrap_or(false)
    }

    /// Get [`BitSlice`] representation for deleted vectors with deletion flags
    ///
    /// The size of this slice is not guaranteed. It may be smaller/larger than the number of
    /// vectors in this segment.
    pub fn deleted_vector_bitslice(&self) -> &BitSlice {
        &self.deleted
    }

    #[cfg(target_os = "linux")]
    fn process_points_uring(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        callback: impl FnMut(usize, PointOffsetType, &[T]),
    ) -> OperationResult<()> {
        self.uring_reader
            .lock()
            .as_mut()
            .expect("io_uring reader should be initialized")
            .read_stream(points, callback)
    }

    #[cfg(not(target_os = "linux"))]
    fn process_points_simple(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        mut callback: impl FnMut(usize, PointOffsetType, &[T]),
    ) {
        for (idx, point) in points.enumerate() {
            let vector = self.get_vector(point);
            callback(idx, point, vector);
        }
    }

    /// Reads vectors for the given ids and calls the callback for each vector.
    /// Tries to utilize asynchronous IO if possible.
    /// In particular, uses io_uring on Linux and simple synchronous IO otherwise.
    pub fn read_vectors_async(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        callback: impl FnMut(usize, PointOffsetType, &[T]),
    ) -> OperationResult<()> {
        #[cfg(target_os = "linux")]
        {
            self.process_points_uring(points, callback)
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.process_points_simple(points, callback);
            Ok(())
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.mmap.populate();
        Ok(())
    }
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
    HEADER_SIZE.div_ceil(align) * align
}

/// Calculate size for deleted mmap to hold the given number of vectors.
///
/// The mmap will hold a file header and an aligned `BitSlice`.
fn deleted_mmap_size(num: usize) -> usize {
    let unit_size = mem::size_of::<usize>();
    let num_bytes = num.div_ceil(8);
    let num_usizes = num_bytes.div_ceil(unit_size);
    let data_size = num_usizes * unit_size;
    deleted_mmap_data_start() + data_size
}
