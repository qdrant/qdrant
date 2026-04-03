use std::borrow::Cow;
use std::cmp::max;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::{atomic_save_json, clear_disk_cache};
use common::generic_consts::{AccessPattern, Random, Sequential};
use common::maybe_uninit::maybe_uninit_fill_from;
use common::mmap::{
    Advice, AdviceSetting, MULTI_MMAP_IS_SUPPORTED, MmapType, create_and_ensure_length,
    open_write_mmap,
};
use common::universal_io::{
    MmapFile, OpenOptions, ReadRange, TypedStorage, UniversalIoError, UniversalRead,
    UniversalWrite, read_json_via,
};
use fs_err as fs;
use memmap2::MmapMut;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::common::{CHUNK_SIZE, PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient;
use crate::vector_storage::{VectorOffset, VectorOffsetType};

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.dat";

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap"; // TODO: rename for other storages?

#[repr(C)]
pub struct Status {
    pub len: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkedVectorsConfig {
    chunk_size_bytes: usize,
    chunk_size_vectors: usize,
    dim: usize,
    #[serde(default)]
    populate: Option<bool>,
}

#[derive(Debug)]
pub struct ChunkedVectors<T: Copy + Sized + 'static, S: UniversalWrite<T>> {
    config: ChunkedVectorsConfig,
    status: MmapType<Status>,
    chunks: Vec<TypedStorage<S, T>>,
    directory: PathBuf,
}

impl<T: Sized + Copy + 'static, S: UniversalWrite<T>> ChunkedVectors<T, S> {
    fn config_file(directory: &Path) -> PathBuf {
        directory.join(CONFIG_FILE_NAME)
    }

    pub fn status_file(directory: &Path) -> PathBuf {
        directory.join(STATUS_FILE_NAME)
    }

    pub fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
        let status_file = Self::status_file(directory);
        if !status_file.exists() {
            {
                let length = std::mem::size_of::<usize>() as u64;
                create_and_ensure_length(&status_file, length as usize)?;
            }
        }
        Ok(open_write_mmap(
            &status_file,
            AdviceSetting::from(Advice::Normal),
            false, // Status file is write-only
        )?)
    }

    fn ensure_config(
        directory: &Path,
        dim: usize,
        populate: Option<bool>,
    ) -> OperationResult<ChunkedVectorsConfig> {
        let config_file = Self::config_file(directory);
        match Self::load_config(&config_file) {
            Ok(Some(config)) => {
                if config.dim == dim {
                    Ok(config)
                } else {
                    Err(OperationError::service_error(format!(
                        "Wrong configuration in {}: expected {}, found {dim}",
                        config_file.display(),
                        config.dim,
                    )))
                }
            }
            Ok(None) => Self::create_config(&config_file, dim, populate),
            Err(e) => {
                log::error!("Failed to deserialize config file {:?}: {e}", &config_file);
                Self::create_config(&config_file, dim, populate)
            }
        }
    }

    fn load_config(config_file: &Path) -> OperationResult<Option<ChunkedVectorsConfig>> {
        match read_json_via::<MmapFile, ChunkedVectorsConfig>(config_file) {
            Ok(config) => Ok(Some(config)),
            Err(UniversalIoError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn create_config(
        config_file: &Path,
        dim: usize,
        populate: Option<bool>,
    ) -> OperationResult<ChunkedVectorsConfig> {
        if dim == 0 {
            return Err(OperationError::service_error(
                "The vector's dimension cannot be 0",
            ));
        }

        let chunk_size_bytes = CHUNK_SIZE;
        let vector_size_bytes = dim * std::mem::size_of::<T>();
        let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
        let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

        let config = ChunkedVectorsConfig {
            chunk_size_bytes: corrected_chunk_size_bytes,
            chunk_size_vectors,
            dim,
            populate,
        };
        atomic_save_json(config_file, &config)?;
        Ok(config)
    }

    pub fn open(
        directory: &Path,
        dim: usize,
        advice: AdviceSetting,
        populate: Option<bool>,
    ) -> OperationResult<Self> {
        fs::create_dir_all(directory)?;
        let status_mmap = Self::ensure_status_file(directory)?;
        let status = unsafe { MmapType::from(status_mmap) };

        let config = Self::ensure_config(directory, dim, populate)?;
        let chunks = read_chunks(directory, advice, populate.unwrap_or_default())?;
        let vectors = Self {
            status,
            config,
            chunks,
            directory: directory.to_owned(),
        };
        Ok(vectors)
    }

    #[inline]
    fn get_chunk_index(&self, key: usize) -> usize {
        key / self.config.chunk_size_vectors
    }

    /// Returns the byte offset of the vector in the chunk
    #[inline]
    fn get_chunk_offset(&self, key: usize) -> usize {
        let chunk_vector_idx = key % self.config.chunk_size_vectors;
        chunk_vector_idx * self.config.dim
    }

    #[inline]
    pub fn max_vector_size_bytes(&self) -> usize {
        self.config.chunk_size_bytes
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.status.len
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.config.dim
    }

    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk = create_chunk(
            &self.directory,
            self.chunks.len(),
            self.config.chunk_size_bytes,
        )?;

        self.chunks.push(chunk);
        Ok(())
    }

    pub fn insert(
        &mut self,
        key: VectorOffsetType,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.insert_many(key, vector, 1, hw_counter)
    }

    #[inline]
    pub fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        assert_eq!(
            vectors.len(),
            count * self.config.dim,
            "Vector size mismatch"
        );

        let start_key = start_key.as_();
        let chunk_idx = self.get_chunk_index(start_key);
        let chunk_offset = self.get_chunk_offset(start_key);

        // check if the vectors fit in the chunk
        if chunk_offset + vectors.len() > self.config.dim * self.config.chunk_size_vectors {
            return Err(OperationError::service_error(format!(
                "Vectors do not fit in the chunk. Chunk idx {chunk_idx}, chunk offset {chunk_offset}, vectors count {count}",
            )));
        }

        // Ensure capacity
        while chunk_idx >= self.chunks.len() {
            self.add_chunk()?;
        }

        let chunk = &mut self.chunks[chunk_idx];

        chunk.write((chunk_offset * size_of::<T>()) as u64, vectors)?;

        hw_counter
            .vector_io_write_counter()
            .incr_delta(size_of_val(vectors));

        let new_len = max(self.status.len, start_key + count);

        if new_len > self.status.len {
            self.status.len = new_len;
        }
        Ok(())
    }

    // returns how many vectors can be inserted starting from key
    pub fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        let start_key = start_key.as_();
        let chunk_vector_idx = self.get_chunk_offset(start_key) / self.config.dim;
        self.config.chunk_size_vectors - chunk_vector_idx
    }

    pub fn push(
        &mut self,
        vector: &[T],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<VectorOffsetType> {
        let new_id = self.status.len;
        self.insert(new_id, vector, hw_counter)?;
        Ok(new_id)
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
        if start_key.checked_add(count)? > self.status.len {
            return None;
        }

        let chunk_idx = self.get_chunk_index(start_key);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let elements_length = count * self.config.dim;
        let element_offset = self.get_chunk_offset(start_key);
        let element_end = element_offset + elements_length;
        let chunk = &self.chunks[chunk_idx];

        if element_end > self.config.chunk_size_vectors * self.config.dim {
            return None;
        }

        let range = ReadRange {
            byte_offset: (element_offset * size_of::<T>()) as u64,
            length: elements_length as u64,
        };

        let use_sequential =
            force_sequential || elements_length * size_of::<T>() > PAGE_SIZE_BYTES * 4;

        if use_sequential {
            chunk.read::<Sequential>(range).ok()
        } else {
            chunk.read::<Random>(range).ok()
        }
    }

    pub fn for_each_in_batch<F: FnMut(usize, &[T]), O: VectorOffset>(&self, keys: &[O], mut f: F) {
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);
        let do_sequential_read = is_read_with_prefetch_efficient(keys);

        // The `f` is most likely a scorer function.
        // Fetching all vectors first then scoring them is more cache friendly
        // then fetching and scoring in a single loop.
        let mut vectors_buffer = [const { MaybeUninit::uninit() }; VECTOR_READ_BATCH_SIZE];
        let vectors = maybe_uninit_fill_from(
            &mut vectors_buffer,
            keys.iter().map(|&key| {
                self.get_many_impl(key.offset(), 1, do_sequential_read)
                    .unwrap_or_else(|| panic!("Vector {key} not found"))
            }),
        )
        .0;

        for (i, vec) in vectors.iter().enumerate() {
            f(i, vec.as_ref());
        }
    }

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let status_flusher = self.status.flusher();
            let chunks_flushers: Vec<_> = self.chunks.iter().map(|chunk| chunk.flusher()).collect();
            move || {
                for flusher in chunks_flushers {
                    flusher()?;
                }
                status_flusher()?;
                Ok(())
            }
        })
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
        for chunk_idx in 0..self.chunks.len() {
            let file_path = chunk_name(&self.directory, chunk_idx);
            clear_disk_cache(&file_path)?;
        }
        Ok(())
    }
}

/// Checks if the file name matches the pattern for mmap chunks
/// Return ID from the file name if it matches, None otherwise
fn check_mmap_file_name_pattern(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(MMAP_CHUNKS_PATTERN_START)
        .and_then(|file_name| file_name.strip_suffix(MMAP_CHUNKS_PATTERN_END))
        .and_then(|file_name| file_name.parse::<usize>().ok())
}

pub fn read_chunks<T: Sized + Copy + 'static, S: UniversalWrite<T>>(
    directory: &Path,
    advice: AdviceSetting,
    populate: bool,
) -> Result<Vec<TypedStorage<S, T>>, common::universal_io::UniversalIoError> {
    let mut chunks_files: AHashMap<usize, _> = AHashMap::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let chunk_id = path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .and_then(check_mmap_file_name_pattern);

            if let Some(chunk_id) = chunk_id {
                chunks_files.insert(chunk_id, path);
            }
        }
    }

    let num_chunks = chunks_files.len();
    let mut result = Vec::with_capacity(num_chunks);
    for chunk_id in 0..num_chunks {
        let chunk_path = chunks_files.remove(&chunk_id).ok_or_else(|| {
            UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Missing chunk {chunk_id} in {}", directory.display(),),
            ))
        })?;

        let chunk = TypedStorage::open(
            &chunk_path,
            OpenOptions {
                writeable: true,
                need_sequential: *MULTI_MMAP_IS_SUPPORTED,
                disk_parallel: None,
                populate: Some(populate),
                advice: Some(advice),
                prevent_caching: None,
            },
        )?;

        result.push(chunk);
    }
    Ok(result)
}

pub fn chunk_name(directory: &Path, chunk_id: usize) -> PathBuf {
    directory.join(format!(
        "{MMAP_CHUNKS_PATTERN_START}{chunk_id}{MMAP_CHUNKS_PATTERN_END}",
    ))
}

pub fn create_chunk<T: Sized + Copy + 'static, S: UniversalWrite<T>>(
    directory: &Path,
    chunk_id: usize,
    chunk_length_bytes: usize,
) -> Result<TypedStorage<S, T>, UniversalIoError> {
    let chunk_file_path = chunk_name(directory, chunk_id);
    create_and_ensure_length(&chunk_file_path, chunk_length_bytes)?;

    TypedStorage::open(
        &chunk_file_path,
        OpenOptions {
            writeable: true,
            need_sequential: *MULTI_MMAP_IS_SUPPORTED,
            disk_parallel: None,
            populate: Some(false), // don't populate newly created chunk, as it's empty and will be filled later
            advice: None,
            prevent_caching: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use common::universal_io::MmapFile;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::VectorElementType;
    use crate::fixtures::index_fixtures::random_vector;

    #[test]
    fn test_chunked_mmap() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let dim = 500;
        let num_vectors = 1000;
        let mut rng = StdRng::seed_from_u64(42);

        let hw_counter = HardwareCounterCell::new();

        let mut vectors: Vec<_> = (0..num_vectors)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        {
            let mut chunked_mmap: ChunkedVectors<VectorElementType, MmapFile> =
                ChunkedVectors::open(dir.path(), dim, AdviceSetting::Global, Some(true)).unwrap();

            for vec in &vectors {
                chunked_mmap.push(vec, &hw_counter).unwrap();
            }

            let random_offset = 666;
            let batch_size = 10;

            let batch_ids = (random_offset..random_offset + batch_size).collect::<Vec<_>>();
            let mut vectors_buffer = Vec::with_capacity(batch_size);
            chunked_mmap.for_each_in_batch(&batch_ids, |i, vec| {
                assert_eq!(i, vectors_buffer.len());
                vectors_buffer.push(vec.to_vec());
            });

            for (i, (vec, loaded_vec)) in zip(
                &vectors[random_offset..random_offset + batch_size],
                &vectors_buffer[..batch_size],
            )
            .enumerate()
            {
                assert_eq!(
                    vec, loaded_vec,
                    "Vectors at index {i} in chunked_mmap are not equal to vectors",
                );
            }

            vectors[0] = random_vector(&mut rng, dim);
            vectors[150] = random_vector(&mut rng, dim);
            vectors[44] = random_vector(&mut rng, dim);
            vectors[999] = random_vector(&mut rng, dim);

            chunked_mmap.insert(0, &vectors[0], &hw_counter).unwrap();
            chunked_mmap
                .insert(150, &vectors[150], &hw_counter)
                .unwrap();
            chunked_mmap.insert(44, &vectors[44], &hw_counter).unwrap();
            chunked_mmap
                .insert(999, &vectors[999], &hw_counter)
                .unwrap();

            assert!(
                chunked_mmap.chunks.len() > 1,
                "must have multiple chunks to test",
            );

            chunked_mmap.flusher()().unwrap();
        }
    }
}
