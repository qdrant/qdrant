use std::borrow::Cow;
use std::io::Write;
use std::mem::{self, MaybeUninit, size_of};
use std::path::Path;

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::mmap;
use common::mmap::{AdviceSetting, MmapBitSlice, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{
    MmapFile, OpenOptions as UniversalOpenOptions, ReadOnly, ReadRange, TypedStorage, UniversalRead,
};
use fs_err::{File, OpenOptions};

use crate::common::error_logging::LogError;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;

const HEADER_SIZE: usize = 4;
const VECTORS_HEADER: &[u8; HEADER_SIZE] = b"data";
const DELETED_HEADER: &[u8; HEADER_SIZE] = b"drop";

/// Immutable storage for dense vectors.
#[derive(Debug)]
pub struct ImmutableDenseVectors<T, S = MmapFile>
where
    T: PrimitiveVectorElement,
    S: UniversalRead<T>,
{
    pub dim: usize,
    pub num_vectors: usize,
    /// Vector data storage, providing read access via [`UniversalRead<T>`].
    storage: TypedStorage<ReadOnly<S>, T>,
    /// Memory mapped deletion flags
    deleted: MmapBitSlice,
    /// Current number of deleted vectors.
    pub deleted_count: usize,
}

impl<T: PrimitiveVectorElement, S: UniversalRead<T>> ImmutableDenseVectors<T, S> {
    pub fn open(
        vectors_path: &Path,
        deleted_path: &Path,
        dim: usize,
        populate: bool,
    ) -> OperationResult<Self> {
        // Allocate/open vectors file
        ensure_mmap_file_size(vectors_path, VECTORS_HEADER, None)
            .describe("Create mmap data file")?;

        let file_len = fs_err::metadata(vectors_path)?.len() as usize;
        let num_vectors = file_len.saturating_sub(HEADER_SIZE) / dim / size_of::<T>();

        let options = UniversalOpenOptions {
            writeable: false,
            need_sequential: true,
            disk_parallel: None,
            populate: Some(populate),
            advice: None,
            prevent_caching: None,
        };
        let storage = TypedStorage::open(vectors_path, options).map_err(|e| {
            crate::common::operation_error::OperationError::service_error(format!(
                "Failed to open vector mmap at {}: {e}",
                vectors_path.display()
            ))
        })?;

        // Allocate/open deleted mmap
        let deleted_mmap_size = deleted_mmap_size(num_vectors);
        ensure_mmap_file_size(deleted_path, DELETED_HEADER, Some(deleted_mmap_size as u64))
            .describe("Create mmap deleted file")?;
        let deleted_mmap = mmap::open_write_mmap(deleted_path, AdviceSetting::Global, false)
            .describe("Open mmap deleted for writing")?;

        // Advise kernel that we'll need this page soon so the kernel can prepare
        #[cfg(unix)]
        if let Err(err) = deleted_mmap.advise(memmap2::Advice::WillNeed) {
            log::error!("Failed to advise MADV_WILLNEED for deleted flags: {err}");
        }

        // Transform into mmap BitSlice
        let deleted = MmapBitSlice::try_from(deleted_mmap, deleted_mmap_data_start())?;
        let deleted_count = deleted.count_ones();

        Ok(Self {
            dim,
            num_vectors,
            storage,
            deleted,
            deleted_count,
        })
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.deleted.flusher()
    }

    /// Returns the byte offset within the file at which the vector for `key` begins.
    ///
    /// File layout:
    /// ```text
    /// [HEADER_SIZE] [vector_0] [vector_1] ... [vector_N-1]
    /// ```
    /// Each vector occupies `dim * size_of::<T>()` bytes.
    pub fn data_offset(&self, key: PointOffsetType) -> Option<usize> {
        let vector_data_length = self.dim * size_of::<T>();
        let offset = (key as usize) * vector_data_length + HEADER_SIZE;
        if key >= (self.num_vectors as PointOffsetType) {
            return None;
        }
        Some(offset)
    }

    /// Read one vector from storage at the given byte offset.
    fn raw_vector_offset<P: AccessPattern>(&self, byte_offset: usize) -> Cow<'_, [T]> {
        let range = ReadRange {
            byte_offset: byte_offset as u64,
            length: self.dim as u64,
        };

        self.storage
            .read::<P>(range)
            .expect("vector read from storage failed")
    }

    /// Returns vector data by key
    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    /// Returns an optional vector data by key
    pub fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<Cow<'_, [T]>> {
        self.data_offset(key)
            .map(|offset| self.raw_vector_offset::<P>(offset))
    }

    pub fn for_each_in_batch<F: FnMut(usize, &[T])>(&self, keys: &[PointOffsetType], mut f: F) {
        #[cfg(target_os = "linux")]
        if S::kind() == common::universal_io::UniversalKind::IoUring {
            return self.for_each_in_batch_async(keys, f);
        }

        // The `f` is most likely a scorer function. Fetching all vectors first, and then scoring
        // them is more cache friendly, than fetching and scoring in a single loop.

        let mut vectors_buffer = [const { MaybeUninit::uninit() }; VECTOR_READ_BATCH_SIZE];

        for (batch_idx, keys) in keys.chunks(VECTOR_READ_BATCH_SIZE).enumerate() {
            let vectors = if is_read_with_prefetch_efficient(keys) {
                let iter = keys
                    .iter()
                    .map(|&point_offset| self.get_vector::<Sequential>(point_offset));

                maybe_uninit_fill_from(&mut vectors_buffer, iter).0
            } else {
                let iter = keys
                    .iter()
                    .map(|&point_offset| self.get_vector::<Random>(point_offset));

                maybe_uninit_fill_from(&mut vectors_buffer, iter).0
            };

            let batch_offset = VECTOR_READ_BATCH_SIZE * batch_idx;

            for (vector_idx, vec) in vectors.iter().enumerate() {
                f(batch_offset + vector_idx, vec);
            }
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn for_each_in_batch_async<F>(&self, keys: &[PointOffsetType], mut callback: F)
    where
        F: FnMut(usize, &[T]),
    {
        let ranges = keys.iter().enumerate().map(|(idx, &point_offset)| {
            let range = ReadRange {
                byte_offset: self.data_offset(point_offset).expect("point exists") as _,
                length: self.dim as _,
            };

            (idx, range)
        });

        let callback = move |idx, vector: &[T]| {
            callback(idx, vector);
            Ok(())
        };

        // access pattern does not matter for io_uring
        self.storage
            .read_batch::<Random, _>(ranges, callback)
            .expect("vectors read");
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

    pub fn populate(&self) {
        if let Err(err) = self.storage.populate() {
            log::error!("Failed to populate vector storage: {err}");
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            dim: _,
            num_vectors: _,
            storage,
            deleted,
            deleted_count: _,
        } = self;
        storage.clear_ram_cache()?;
        deleted.clear_cache()?;
        Ok(())
    }
}

/// Ensure the given mmap file exists and is the given size.
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
    if let Some(size) = size
        && size > header.len() as u64
    {
        file.set_len(size)?;
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
