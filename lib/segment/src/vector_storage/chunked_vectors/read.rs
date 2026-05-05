use std::borrow::Cow;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::mmap::AdviceSetting;
use common::universal_io::{
    MmapFile, ReadRange, TypedStorage, UniversalIoError, UniversalRead, read_json_via,
};
use fs_err as fs;
use num_traits::AsPrimitive;

use super::chunks::{chunk_name, read_chunks};
use super::config::{CONFIG_FILE_NAME, ChunkedVectorsConfig, STATUS_FILE_NAME};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::{PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::{VectorOffset, VectorOffsetType};

/// Read-only view over a chunked-vectors directory.
///
/// Holds the indexing logic (chunks, config, vector count) and exposes only the
/// read-side API. Status length is loaded from disk once at open time and is
/// not refreshed afterwards. Mutating storage uses [`super::ChunkedVectors`]
/// which wraps this and adds a writable status mmap.
#[derive(Debug)]
pub struct ChunkedVectorsRead<T: Copy + Sized + 'static, S: UniversalRead<T>> {
    pub(super) config: ChunkedVectorsConfig,
    /// Number of vectors currently stored. Snapshot for read-only mode; for
    /// [`super::ChunkedVectors`] this is kept in sync with the writable status
    /// mmap.
    pub(super) len: usize,
    pub(super) chunks: Vec<TypedStorage<S, T>>,
    pub(super) directory: PathBuf,
}

impl<T: Sized + Copy + 'static, S: UniversalRead<T>> ChunkedVectorsRead<T, S> {
    pub(super) fn config_file(directory: &Path) -> PathBuf {
        directory.join(CONFIG_FILE_NAME)
    }

    pub fn status_file(directory: &Path) -> PathBuf {
        directory.join(STATUS_FILE_NAME)
    }

    pub(super) fn load_config(config_file: &Path) -> OperationResult<Option<ChunkedVectorsConfig>> {
        match read_json_via::<MmapFile, ChunkedVectorsConfig>(config_file) {
            Ok(config) => Ok(Some(config)),
            Err(UniversalIoError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Open an existing chunked-vectors directory in read-only mode.
    ///
    /// Both `config.json` and `status.dat` must already exist; this function
    /// will not create them.
    #[allow(dead_code)] // pending: read-only vector storage enum will use this
    pub fn open(
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Option<bool>,
    ) -> OperationResult<Self> {
        let config_file = Self::config_file(directory);
        let config = Self::load_config(&config_file)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Config file {} is missing",
                config_file.display(),
            ))
        })?;
        if config.dim != dim {
            return Err(OperationError::service_error(format!(
                "Wrong configuration in {}: expected {}, found {dim}",
                config_file.display(),
                config.dim,
            )));
        }

        let len = read_status_len(&Self::status_file(directory))?;
        let chunks = read_chunks(directory, advice, populate.unwrap_or_default(), false)?;

        Ok(Self {
            config,
            len,
            chunks,
            directory: directory.to_owned(),
        })
    }

    #[inline]
    pub(super) fn get_chunk_index(&self, key: usize) -> usize {
        key / self.config.chunk_size_vectors
    }

    /// Returns the byte offset of the vector in the chunk
    #[inline]
    pub(super) fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.config.chunk_size_vectors;
        chunk_vector_idx * self.config.dim
    }

    #[inline]
    pub fn max_vector_size_bytes(&self) -> usize {
        self.config.chunk_size_bytes
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.config.dim
    }

    // returns how many vectors can be inserted starting from key
    pub fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        let start_key = start_key.as_();
        let chunk_vector_idx = self.get_chunk_offset(start_key) / self.config.dim;
        self.config.chunk_size_vectors - chunk_vector_idx
    }

    #[inline]
    fn read_range(&self, offset: VectorOffsetType, count: usize) -> Option<(usize, ReadRange)> {
        if offset.checked_add(count)? > self.len {
            return None;
        }

        let chunk_idx = self.get_chunk_index(offset);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let element_offset = self.get_chunk_offset(offset);
        let elements_length = count * self.config.dim;
        if element_offset + elements_length > self.config.chunk_size_vectors * self.config.dim {
            return None;
        }

        let range = ReadRange {
            byte_offset: (element_offset * size_of::<T>()) as u64,
            length: elements_length as u64,
        };

        Some((chunk_idx, range))
    }

    /// Returns `count` flattened vectors starting from `starting_key`.
    ///
    /// Returns `None` when:
    /// - chunk boundary is crossed
    /// - any section of `start_key..start_key + count` is out of bounds
    #[inline]
    fn get_many_impl(
        &self,
        start_key: VectorOffsetType,
        count: usize,
        force_sequential: bool,
    ) -> Option<Cow<'_, [T]>> {
        let (chunk_idx, range) = self.read_range(start_key, count)?;

        let chunk = &self.chunks[chunk_idx];

        let use_sequential =
            force_sequential || range.length as usize * size_of::<T>() > PAGE_SIZE_BYTES * 4;

        if use_sequential {
            chunk.read::<Sequential>(range).ok()
        } else {
            chunk.read::<Random>(range).ok()
        }
    }

    pub fn for_each_in_batch<F: FnMut(usize, &[T]), O: VectorOffset>(&self, keys: &[O], mut f: F) {
        #[cfg(target_os = "linux")]
        if S::kind() == common::universal_io::UniversalKind::IoUring {
            for (idx, vectors) in self.iter(keys) {
                f(idx, &vectors);
            }

            return;
        }

        // The `f` is most likely a scorer function. Fetching all vectors first, and then scoring
        // them is more cache friendly, than fetching and scoring in a single loop.

        let mut vectors_buffer = [const { MaybeUninit::uninit() }; VECTOR_READ_BATCH_SIZE];

        for (batch_idx, keys) in keys.chunks(VECTOR_READ_BATCH_SIZE).enumerate() {
            let force_sequential = is_read_with_prefetch_efficient(keys);

            let (vectors, _) = maybe_uninit_fill_from(
                &mut vectors_buffer,
                keys.iter().map(|&key| {
                    self.get_many_impl(key.offset(), key.multi_vector_count(), force_sequential)
                        .expect("vectors read")
                }),
            );

            let batch_offset = VECTOR_READ_BATCH_SIZE * batch_idx;

            for (vector_idx, vec) in vectors.iter().enumerate() {
                f(batch_offset + vector_idx, vec.as_ref());
            }
        }
    }

    pub fn iter<O>(&self, offsets: &[O]) -> impl Iterator<Item = (usize, Cow<'_, [T]>)>
    where
        O: VectorOffset,
    {
        let reads = offsets.iter().enumerate().map(|(idx, offset)| {
            let (chunk_idx, range) = self
                .read_range(offset.offset(), offset.multi_vector_count())
                .expect("vectors exist");

            let chunk = &self.chunks[chunk_idx];
            (idx, chunk, range)
        });

        // access pattern does not matter for io_uring
        UniversalRead::read_multi_iter::<Random, _>(reads)
            .expect("iterator initialized")
            .map(|result| result.expect("vector read"))
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        files.push(Self::config_file(&self.directory));
        files.push(Self::status_file(&self.directory));
        for chunk_idx in 0..self.chunks.len() {
            files.push(chunk_name(&self.directory, chunk_idx));
        }
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        vec![Self::config_file(&self.directory)] // TODO: Is config immutable?
    }

    #[inline]
    pub fn get<P: AccessPattern>(&self, key: VectorOffsetType) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, 1, P::IS_SEQUENTIAL)
    }

    #[inline]
    pub fn get_many<P: AccessPattern>(
        &self,
        key: VectorOffsetType,
        count: usize,
    ) -> Option<Cow<'_, [T]>> {
        self.get_many_impl(key, count, P::IS_SEQUENTIAL)
    }

    pub fn is_on_disk(&self) -> bool {
        !self.config.populate.unwrap_or(false)
    }

    pub fn populate(&self) -> OperationResult<()> {
        for chunk in &self.chunks {
            chunk.populate()?;
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            config: _,
            len: _,
            chunks,
            directory: _,
        } = self;
        for chunk in chunks {
            chunk.clear_ram_cache()?;
        }
        Ok(())
    }

    pub fn heap_size_bytes(&self) -> usize {
        let Self {
            config: _,
            len: _,
            chunks: _,
            directory: _,
        } = self;

        0
    }
}

#[allow(dead_code)] // pending: read-only vector storage enum will use this
fn read_status_len(status_file: &Path) -> OperationResult<usize> {
    let bytes = fs::read(status_file)?;
    let needed = std::mem::size_of::<usize>();
    if bytes.len() < needed {
        return Err(OperationError::service_error(format!(
            "Status file {} is too short: {} < {needed}",
            status_file.display(),
            bytes.len(),
        )));
    }
    Ok(usize::from_ne_bytes(
        bytes[..needed].try_into().expect("size matches"),
    ))
}
