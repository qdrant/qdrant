use std::borrow::Cow;
use std::mem::{MaybeUninit, size_of};
use std::path::Path;

use bytemuck::TransparentWrapper;
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::mmap::{AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFile, OpenOptions as UniversalOpenOptions, Populate, ReadOnly, ReadRange,
    TypedStorage, UioResult, UniversalRead, UniversalReadFs,
};

use crate::common::error_logging::LogError;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::vector_storage::common::{VECTOR_READ_BATCH_SIZE, ensure_mmap_file_size};
use crate::vector_storage::deleted_flags::DeletedFlags;
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;

const HEADER_SIZE: usize = 4;
const VECTORS_HEADER: &[u8; HEADER_SIZE] = b"data";

/// Immutable dense vector blob, shared by the writable [`ImmutableDenseVectors`]
/// and the read-only dense storage. Provides typed read access for `T` through
/// the [`UniversalRead`] backend `S`; holds no deletion flags.
#[derive(Debug)]
pub struct ImmutableDenseVectorData<T, S = MmapFile>
where
    T: PrimitiveVectorElement,
    S: UniversalRead,
{
    pub dim: usize,
    pub num_vectors: usize,
    /// Vector data storage, providing typed read access for `T`.
    storage: TypedStorage<ReadOnly<S>, T>,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> ImmutableDenseVectorData<T, S> {
    fn open_options(populate: Populate) -> UniversalOpenOptions {
        UniversalOpenOptions {
            writeable: false,
            need_sequential: true,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of the vector blob [`Self::open`] reads.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        vectors_path: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        // Vector data
        fs.schedule_open(vectors_path, Some(Self::open_options(populate)), None);

        Ok(())
    }

    /// Open the immutable vector blob read-only through `fs`. The file must
    /// already exist (the writer creates it); nothing is created here.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        vectors_path: &Path,
        dim: usize,
        populate: Populate,
    ) -> OperationResult<Self> {
        let options = Self::open_options(populate);
        let read_only =
            ReadOnly::open(fs, vectors_path, options, Default::default()).map_err(|e| {
                crate::common::operation_error::OperationError::service_error(format!(
                    "Failed to open vector mmap at {}: {e}",
                    vectors_path.display()
                ))
            })?;
        // Vector count from the length read through `fs` — the backing store may
        // be remote, so never stat the path locally.
        let file_len = read_only.len::<u8>().map_err(|e| {
            crate::common::operation_error::OperationError::service_error(format!(
                "Failed to read length of vector file {}: {e}",
                vectors_path.display()
            ))
        })? as usize;
        let num_vectors = file_len.saturating_sub(HEADER_SIZE) / dim / size_of::<T>();

        let storage = TypedStorage::<ReadOnly<S>, T>::wrap(read_only);

        Ok(Self {
            dim,
            num_vectors,
            storage,
        })
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
            .read(range, P::default())
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

    pub fn for_each_in_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        mut f: F,
    ) -> OperationResult<()> {
        if TypedStorage::<ReadOnly<S>, T>::kind().can_be_async() {
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

        Ok(())
    }

    fn for_each_in_batch_async<F>(
        &self,
        keys: &[PointOffsetType],
        mut callback: F,
    ) -> OperationResult<()>
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
            UioResult::Ok(())
        };

        // access pattern does not matter for io_uring
        self.storage.read_batch(ranges, Random, callback)?;
        Ok(())
    }

    pub fn populate(&self) {
        if let Err(err) = self.storage.populate() {
            log::error!("Failed to populate vector storage: {err}");
        }
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_ram_cache()?;
        Ok(())
    }
}

/// Immutable storage for dense vectors.
///
/// Wraps the shared [`ImmutableDenseVectorData`] blob with a writable deletion
/// bitmap, so it can mark vectors as removed even though the vector data itself
/// is append-only and can only be constructed from another storage.
#[derive(Debug)]
pub struct ImmutableDenseVectors<T, S = MmapFile>
where
    T: PrimitiveVectorElement,
    S: UniversalRead,
{
    /// Vector data blob, read-only through `S`.
    data: ImmutableDenseVectorData<T, S>,
    deleted: DeletedFlags,
}

impl<T: PrimitiveVectorElement, S: UniversalRead> ImmutableDenseVectors<T, S> {
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        vectors_path: &Path,
        deleted_path: &Path,
        dim: usize,
        populate: Populate,
    ) -> OperationResult<Self> {
        // Allocate/open vectors file
        ensure_mmap_file_size(vectors_path, VECTORS_HEADER, None)
            .describe("Create mmap data file")?;

        let data = ImmutableDenseVectorData::open(fs, vectors_path, dim, populate)?;
        let deleted = DeletedFlags::open_or_create(deleted_path, data.num_vectors)?;
        Ok(Self { data, deleted })
    }

    pub fn dim(&self) -> usize {
        self.data.dim
    }

    pub fn num_vectors(&self) -> usize {
        self.data.num_vectors
    }

    pub fn flusher(&self) -> MmapFlusher {
        self.deleted.flusher()
    }

    /// Returns an optional vector data by key
    pub fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<Cow<'_, [T]>> {
        self.data.get_vector_opt::<P>(key)
    }

    pub fn for_each_in_batch<F: FnMut(usize, &[T])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        self.data.for_each_in_batch(keys, f)
    }

    pub fn deleted(&self) -> &DeletedFlags {
        &self.deleted
    }

    pub fn deleted_mut(&mut self) -> &mut DeletedFlags {
        &mut self.deleted
    }

    pub fn populate(&self) {
        self.data.populate();
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.data.clear_cache()?;
        self.deleted.clear_cache()?;
        Ok(())
    }
}
