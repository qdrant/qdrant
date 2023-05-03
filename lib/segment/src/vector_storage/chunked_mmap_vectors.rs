use std::cmp::max;
use std::collections::HashMap;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use serde::{Deserialize, Serialize};

use crate::common::mmap_ops::{
    create_and_ensure_length, open_write_mmap, read_from_mmap, read_slice_from_mmap,
    write_slice_to_mmap, write_to_mmap,
};
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::PointOffsetType;

#[cfg(test)]
const DEFAULT_CHUNK_SIZE: usize = 512 * 1024; // 512Kb

#[cfg(not(test))]
const DEFAULT_CHUNK_SIZE: usize = 128 * 1024 * 1024; // 128Mb

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.json";

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

struct ChunkedMmapStatus {
    len: usize,
}

#[derive(Serialize, Deserialize)]
struct ChunkedMmapConfig {
    chunk_size_bytes: usize,
    chunk_size_vectors: usize,
    dim: usize,
}

pub struct ChunkedMmapVectors<T> {
    config: ChunkedMmapConfig,
    status: ChunkedMmapStatus,
    _phantom: PhantomData<T>,
    chunks: Vec<MmapMut>,
    status_mmap: MmapMut,
    directory: PathBuf,
}

impl<T: Copy + Clone + Default> ChunkedMmapVectors<T> {
    fn status_file(directory: &Path) -> PathBuf {
        directory.join(STATUS_FILE_NAME)
    }

    fn config_file(directory: &Path) -> PathBuf {
        directory.join(CONFIG_FILE_NAME)
    }

    fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
        let status_file = Self::status_file(directory);
        if !status_file.exists() {
            {
                let length = std::mem::size_of::<usize>() as u64;
                create_and_ensure_length(&status_file, length as usize)?;
            }
            let mut mmap = open_write_mmap(&status_file)?;
            write_to_mmap(&mut mmap, 0, 0usize);
            mmap.flush()?;
            Ok(mmap)
        } else {
            open_write_mmap(&status_file)
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
                .create(true)
                .write(true)
                .open(&config_file)?;
            serde_json::to_writer(&mut file, &config)?;
            file.flush()?;
            Ok(config)
        } else {
            let file = std::fs::File::open(&config_file)?;
            let config: ChunkedMmapConfig = serde_json::from_reader(file)?;

            if config.dim != dim {
                return Err(OperationError::service_error(format!(
                    "Wrong configuration in {}: expected {}, found {}",
                    config_file.display(),
                    config.dim,
                    dim
                )));
            }

            Ok(config)
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

    fn read_mmaps(directory: &Path) -> OperationResult<Vec<MmapMut>> {
        let mut result = Vec::new();
        let mut mmap_files: HashMap<usize, _> = HashMap::new();
        for entry in directory.read_dir()? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let chunk_id = path
                    .file_name()
                    .and_then(|file_name| file_name.to_str())
                    .and_then(Self::check_mmap_file_name_pattern);

                if let Some(chunk_id) = chunk_id {
                    mmap_files.insert(chunk_id, path);
                }
            }
        }

        let num_chunks = mmap_files.len();
        for chunk_id in 0..num_chunks {
            let mmap_file = mmap_files.remove(&chunk_id).ok_or_else(|| {
                OperationError::service_error(format!(
                    "Missing mmap chunk {} in {}",
                    chunk_id,
                    directory.display()
                ))
            })?;
            let mmap = open_write_mmap(&mmap_file)?;
            result.push(mmap);
        }
        Ok(result)
    }

    pub fn open(directory: &Path, dim: usize) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = Self::ensure_status_file(directory)?;
        let len = *read_from_mmap(&status_mmap, 0);
        let status = ChunkedMmapStatus { len };
        let config = Self::ensure_config(directory, dim)?;
        let chunks = Self::read_mmaps(directory)?;

        let vectors = Self {
            config,
            status,
            _phantom: Default::default(),
            chunks,
            status_mmap,
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
        chunk_vector_idx * self.config.dim * std::mem::size_of::<T>()
    }

    pub fn len(&self) -> usize {
        self.status.len
    }

    pub fn is_empty(&self) -> bool {
        self.status.len == 0
    }

    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk_file_name = format!(
            "{}{}{}",
            MMAP_CHUNKS_PATTERN_START,
            self.chunks.len(),
            MMAP_CHUNKS_PATTERN_END
        );
        let chunk_file_path = self.directory.join(chunk_file_name);
        create_and_ensure_length(&chunk_file_path, self.config.chunk_size_bytes)?;
        self.chunks.push(open_write_mmap(&chunk_file_path)?);
        Ok(())
    }

    pub fn insert(&mut self, key: PointOffsetType, vector: &[T]) -> OperationResult<()> {
        let key = key as usize;
        let chunk_idx = self.get_chunk_index(key);
        let chunk_offset = self.get_chunk_offset(key);

        // Ensure capacity
        while chunk_idx >= self.chunks.len() {
            self.add_chunk()?;
        }

        let chunk = &mut self.chunks[chunk_idx];

        write_slice_to_mmap(chunk, chunk_offset, vector);

        let new_len = max(self.status.len, key + 1);

        if new_len > self.status.len {
            write_to_mmap(&mut self.status_mmap, 0, new_len);
            self.status.len = new_len;
        }
        Ok(())
    }

    pub fn push(&mut self, vector: &[T]) -> OperationResult<PointOffsetType> {
        let new_id = self.status.len as PointOffsetType;
        self.insert(new_id, vector)?;
        Ok(new_id)
    }

    pub fn get<TKey>(&self, key: TKey) -> &[T]
    where
        TKey: num_traits::cast::AsPrimitive<usize>,
    {
        let key: usize = key.as_();
        let chunk_idx = self.get_chunk_index(key);
        let chunk_offset = self.get_chunk_offset(key);
        let chunk = &self.chunks[chunk_idx];
        read_slice_from_mmap(chunk, chunk_offset, self.config.dim)
    }

    pub fn flush(&mut self) -> OperationResult<()> {
        for chunk in &mut self.chunks {
            chunk.flush()?;
        }
        self.status_mmap.flush()?;
        Ok(())
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

            chunked_mmap.flush().unwrap();
        }

        {
            let chunked_mmap: ChunkedMmapVectors<VectorElementType> =
                ChunkedMmapVectors::open(dir.path(), dim).unwrap();

            assert_eq!(chunked_mmap.len(), vectors.len());

            for (i, vec) in vectors.iter().enumerate() {
                assert_eq!(
                    chunked_mmap.get(i),
                    vec,
                    "Vectors at index {} are not equal",
                    i
                );
            }
        }
    }
}
