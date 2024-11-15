use std::cmp::max;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use memory::chunked_utils::{chunk_name, create_chunk, read_mmaps, UniversalMmapChunk};
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::MmapType;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};
use crate::vector_storage::common::{CHUNK_SIZE, PAGE_SIZE_BYTES, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::query_scorer::is_read_with_prefetch_efficient_vectors;

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.dat";

#[repr(C)]
pub struct Status {
    pub len: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkedMmapConfig {
    chunk_size_bytes: usize,
    chunk_size_vectors: usize,
    dim: usize,
    #[serde(default)]
    mlock: Option<bool>,
    #[serde(default)]
    populate: Option<bool>,
}

#[derive(Debug)]
pub struct ChunkedMmapVectors<T: Sized + 'static> {
    config: ChunkedMmapConfig,
    status: MmapType<Status>,
    chunks: Vec<UniversalMmapChunk<T>>,
    directory: PathBuf,
}

impl<T: Sized + Copy + 'static> ChunkedMmapVectors<T> {
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
        mlock: Option<bool>,
        populate: Option<bool>,
    ) -> OperationResult<ChunkedMmapConfig> {
        let config_file = Self::config_file(directory);
        if !config_file.exists() {
            let chunk_size_bytes = CHUNK_SIZE;
            let vector_size_bytes = dim * std::mem::size_of::<T>();
            let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
            let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

            let config = ChunkedMmapConfig {
                chunk_size_bytes: corrected_chunk_size_bytes,
                chunk_size_vectors,
                dim,
                mlock,
                populate,
            };
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&config_file)?;
            serde_json::to_writer(&mut file, &config)?;
            file.flush()?;
            Ok(config)
        } else {
            let file = std::fs::File::open(&config_file)?;
            let config: ChunkedMmapConfig = serde_json::from_reader(file)?;

            if config.dim != dim {
                return Err(OperationError::service_error(format!(
                    "Wrong configuration in {}: expected {}, found {dim}",
                    config_file.display(),
                    config.dim,
                )));
            }

            Ok(config)
        }
    }

    pub fn open(
        directory: &Path,
        dim: usize,
        mlock: Option<bool>,
        advice: AdviceSetting,
        populate: Option<bool>,
    ) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = Self::ensure_status_file(directory)?;
        let status = unsafe { MmapType::from(status_mmap) };

        let config = Self::ensure_config(directory, dim, mlock, populate)?;
        let chunks = read_mmaps(
            directory,
            config.mlock.unwrap_or_default(),
            populate.unwrap_or_default(),
            advice,
        )?;
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

    pub fn max_vector_size_bytes(&self) -> usize {
        self.config.chunk_size_bytes
    }

    pub fn len(&self) -> usize {
        self.status.len
    }

    pub fn dim(&self) -> usize {
        self.config.dim
    }

    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk = create_chunk(
            &self.directory,
            self.chunks.len(),
            self.config.chunk_size_bytes,
            self.config.mlock.unwrap_or_default(),
        )?;

        self.chunks.push(chunk);
        Ok(())
    }

    pub fn insert(&mut self, key: VectorOffsetType, vector: &[T]) -> OperationResult<()> {
        self.insert_many(key, vector, 1)
    }

    #[inline]
    pub fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
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

        chunk.as_mut_slice()[chunk_offset..chunk_offset + vectors.len()].copy_from_slice(vectors);

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

    pub fn push(&mut self, vector: &[T]) -> OperationResult<VectorOffsetType> {
        let new_id = self.status.len;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    fn get(&self, key: VectorOffsetType, force_sequential: bool) -> Option<&[T]> {
        self.get_many(key, 1, force_sequential)
    }

    // returns count flattened vectors starting from key. if chunk boundary is crossed, returns None
    #[inline]
    fn get_many(
        &self,
        start_key: VectorOffsetType,
        count: usize,
        force_sequential: bool,
    ) -> Option<&[T]> {
        let start_key: usize = start_key.as_();
        let chunk_idx = self.get_chunk_index(start_key);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let block_size_elements = count * self.config.dim;
        let chunk_offset = self.get_chunk_offset(start_key);
        let chunk_end = chunk_offset + block_size_elements;
        let chunk = &self.chunks[chunk_idx];
        if chunk_end > chunk.len() {
            None
        } else if force_sequential || block_size_elements * size_of::<T>() > PAGE_SIZE_BYTES * 4 {
            Some(&chunk.as_seq_slice()[chunk_offset..chunk_end])
        } else {
            Some(&chunk.as_slice()[chunk_offset..chunk_end])
        }
    }

    pub fn get_batch<'a>(&'a self, keys: &[VectorOffsetType], vectors: &mut [&'a [T]]) {
        debug_assert!(keys.len() == vectors.len());
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);
        let do_sequential_read = is_read_with_prefetch_efficient_vectors(keys);

        for (i, key) in keys.iter().enumerate() {
            vectors[i] = self
                .get(*key, do_sequential_read)
                .unwrap_or_else(|| panic!("Vector {key} not found"));
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
}

impl<T: Sized + Copy + 'static> ChunkedVectorStorage<T> for ChunkedMmapVectors<T> {
    #[inline]
    fn len(&self) -> usize {
        ChunkedMmapVectors::len(self)
    }

    #[inline]
    fn dim(&self) -> usize {
        ChunkedMmapVectors::dim(self)
    }

    #[inline]
    fn get(&self, key: VectorOffsetType) -> Option<&[T]> {
        ChunkedMmapVectors::get(self, key, false)
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        ChunkedMmapVectors::files(self)
    }

    #[inline]
    fn flusher(&self) -> Flusher {
        ChunkedMmapVectors::flusher(self)
    }

    #[inline]
    fn push(&mut self, vector: &[T]) -> OperationResult<VectorOffsetType> {
        ChunkedMmapVectors::push(self, vector)
    }

    #[inline]
    fn insert(&mut self, key: VectorOffsetType, vector: &[T]) -> OperationResult<()> {
        ChunkedMmapVectors::insert(self, key, vector)
    }

    #[inline]
    fn insert_many(
        &mut self,
        start_key: VectorOffsetType,
        vectors: &[T],
        count: usize,
    ) -> OperationResult<()> {
        ChunkedMmapVectors::insert_many(self, start_key, vectors, count)
    }

    #[inline]
    fn get_many(&self, key: VectorOffsetType, count: usize) -> Option<&[T]> {
        ChunkedMmapVectors::get_many(self, key, count, false)
    }

    #[inline]
    fn get_batch<'a>(&'a self, keys: &[VectorOffsetType], vectors: &mut [&'a [T]]) {
        ChunkedMmapVectors::get_batch(self, keys, vectors)
    }

    #[inline]
    fn get_remaining_chunk_keys(&self, start_key: VectorOffsetType) -> usize {
        ChunkedMmapVectors::get_remaining_chunk_keys(self, start_key)
    }

    #[inline]
    fn max_vector_size_bytes(&self) -> usize {
        ChunkedMmapVectors::max_vector_size_bytes(self)
    }

    fn is_on_disk(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

    use rand::prelude::StdRng;
    use rand::SeedableRng;
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

        let mut vectors: Vec<_> = (0..num_vectors)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        {
            let mut chunked_mmap: ChunkedMmapVectors<VectorElementType> = ChunkedMmapVectors::open(
                dir.path(),
                dim,
                Some(false),
                AdviceSetting::Global,
                Some(true),
            )
            .unwrap();

            for vec in &vectors {
                chunked_mmap.push(vec).unwrap();
            }

            let mut vectors_buffer = Vec::with_capacity(VECTOR_READ_BATCH_SIZE);

            vectors_buffer.resize_with(VECTOR_READ_BATCH_SIZE, Default::default);

            let random_offset = 666;
            let batch_size = 10;

            assert!(random_offset + batch_size < num_vectors);
            assert!(batch_size <= VECTOR_READ_BATCH_SIZE);

            let batch_ids = (random_offset..random_offset + batch_size).collect::<Vec<_>>();
            chunked_mmap.get_batch(&batch_ids, &mut vectors_buffer[..batch_size]);

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

            chunked_mmap.insert(0, &vectors[0]).unwrap();
            chunked_mmap.insert(150, &vectors[150]).unwrap();
            chunked_mmap.insert(44, &vectors[44]).unwrap();
            chunked_mmap.insert(999, &vectors[999]).unwrap();

            assert!(
                chunked_mmap.chunks.len() > 1,
                "must have multiple chunks to test",
            );

            chunked_mmap.flusher()().unwrap();
        }
    }
}
