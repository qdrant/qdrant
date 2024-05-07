use std::cmp::max;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use memmap2::MmapMut;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use serde::{Deserialize, Serialize};

use crate::common::mmap_type::MmapType;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::vector_storage::chunked_utils::{chunk_name, create_chunk, read_mmaps, MmapChunk};

#[cfg(debug_assertions)]
const DEFAULT_CHUNK_SIZE: usize = 512 * 1024; // 512Kb
#[cfg(not(debug_assertions))]
const DEFAULT_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32Mb

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.dat";

#[repr(C)]
pub struct Status {
    pub len: usize,
}

#[derive(Serialize, Deserialize)]
struct ChunkedMmapConfig {
    chunk_size_bytes: usize,
    chunk_size_vectors: usize,
    dim: usize,
}

pub struct ChunkedMmapVectors<T: Sized + 'static> {
    config: ChunkedMmapConfig,
    status: MmapType<Status>,
    chunks: Vec<MmapChunk<T>>,
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
            let mmap = open_write_mmap(&status_file)?;
            Ok(mmap)
        } else {
            let mmap = open_write_mmap(&status_file)?;
            Ok(mmap)
        }
    }

    fn ensure_config(directory: &Path, dim: usize) -> OperationResult<ChunkedMmapConfig> {
        let config_file = Self::config_file(directory);
        if !config_file.exists() {
            let chunk_size_bytes = DEFAULT_CHUNK_SIZE;
            let vector_size_bytes = dim * std::mem::size_of::<T>();
            let chunk_size_vectors = chunk_size_bytes / vector_size_bytes;
            let corrected_chunk_size_bytes = chunk_size_vectors * vector_size_bytes;

            let config = ChunkedMmapConfig {
                chunk_size_bytes: corrected_chunk_size_bytes,
                chunk_size_vectors,
                dim,
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

    pub fn open(directory: &Path, dim: usize) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = Self::ensure_status_file(directory)?;
        let status = unsafe { MmapType::from(status_mmap) };

        let config = Self::ensure_config(directory, dim)?;
        let chunks = read_mmaps(directory)?;

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
        )?;

        self.chunks.push(chunk);
        Ok(())
    }

    pub fn insert<TKey>(&mut self, key: TKey, vector: &[T]) -> OperationResult<()>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        self.insert_many(key, vector, 1)
    }

    #[inline]
    pub fn insert_many<TKey>(
        &mut self,
        start_key: TKey,
        vectors: &[T],
        count: usize,
    ) -> OperationResult<()>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
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

        chunk[chunk_offset..chunk_offset + vectors.len()].copy_from_slice(vectors);

        let new_len = max(self.status.len, start_key + count);

        if new_len > self.status.len {
            self.status.len = new_len;
        }
        Ok(())
    }

    // returns how many vectors can be inserted starting from key
    pub fn get_remaining_chunk_keys<TKey>(&mut self, start_key: TKey) -> usize
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let start_key = start_key.as_();
        let chunk_vector_idx = self.get_chunk_offset(start_key) / self.config.dim;
        self.config.chunk_size_vectors - chunk_vector_idx
    }

    pub fn push(&mut self, vector: &[T]) -> OperationResult<PointOffsetType> {
        let new_id = self.status.len as PointOffsetType;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    pub fn get<TKey>(&self, key: TKey) -> Option<&[T]>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        self.get_many(key, 1)
    }

    // returns count flattened vectors starting from key. if chunk boundary is crossed, returns None
    #[inline]
    pub fn get_many<TKey>(&self, start_key: TKey, count: usize) -> Option<&[T]>
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let start_key: usize = start_key.as_();
        let chunk_idx = self.get_chunk_index(start_key);
        if chunk_idx >= self.chunks.len() {
            return None;
        }

        let chunk_offset = self.get_chunk_offset(start_key);
        let chunk_end = chunk_offset + count * self.config.dim;
        let chunk = &self.chunks[chunk_idx];
        if chunk_end > chunk.len() {
            None
        } else {
            Some(&chunk[chunk_offset..chunk_end])
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

#[cfg(test)]
mod tests {
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
            let mut chunked_mmap: ChunkedMmapVectors<VectorElementType> =
                ChunkedMmapVectors::open(dir.path(), dim).unwrap();

            for vec in &vectors {
                chunked_mmap.push(vec).unwrap();
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

        {
            let chunked_mmap: ChunkedMmapVectors<VectorElementType> =
                ChunkedMmapVectors::open(dir.path(), dim).unwrap();

            assert!(
                chunked_mmap.chunks.len() > 1,
                "must have multiple chunks to test",
            );
            assert_eq!(chunked_mmap.len(), vectors.len());

            for (i, vec) in vectors.iter().enumerate() {
                assert_eq!(
                    chunked_mmap.get(i).unwrap(),
                    vec,
                    "Vectors at index {} are not equal",
                    i
                );
            }
        }
    }
}
