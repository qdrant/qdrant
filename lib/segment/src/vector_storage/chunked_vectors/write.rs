use std::cmp::max;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::atomic_save_json;
use common::mmap::AdviceSetting;
use common::universal_io::{OpenOptions, StoredStruct, UniversalWrite};
use fs_err as fs;
use num_traits::AsPrimitive;

use super::chunks::{create_chunk, read_chunks};
use super::config::{ChunkedVectorsConfig, Status};
use super::read::ChunkedVectorsRead;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::common::CHUNK_SIZE;

#[derive(Debug)]
pub struct ChunkedVectors<T, S>
where
    T: Copy + 'static,
    S: UniversalWrite<T> + UniversalWrite<Status> + Send + 'static,
{
    inner: ChunkedVectorsRead<T, S>,
    status: StoredStruct<S, Status>,
}

impl<T, S> Deref for ChunkedVectors<T, S>
where
    T: Copy + 'static,
    S: UniversalWrite<T> + UniversalWrite<Status> + Send + 'static,
{
    type Target = ChunkedVectorsRead<T, S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T, S> ChunkedVectors<T, S>
where
    T: Copy + 'static,
    S: UniversalWrite<T> + UniversalWrite<Status> + Send + 'static,
{
    pub fn ensure_status_file(directory: &Path) -> OperationResult<PathBuf> {
        let status_file = ChunkedVectorsRead::<T, S>::status_file(directory);
        if !S::exists(&status_file)? {
            {
                let length = std::mem::size_of::<Status>();
                // TODO(uio): migrate when UniversalWriteFileOps is available
                common::mmap::create_and_ensure_length(&status_file, length)?;
            }
        }
        Ok(status_file)
    }

    fn ensure_config(
        directory: &Path,
        dim: usize,
        populate: Option<bool>,
    ) -> OperationResult<ChunkedVectorsConfig> {
        let config_file = ChunkedVectorsRead::<T, S>::config_file(directory);
        match ChunkedVectorsRead::<T, S>::load_config(&config_file) {
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
        let status_path = Self::ensure_status_file(directory)?;

        let status: StoredStruct<S, Status> = StoredStruct::open(
            status_path,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                disk_parallel: None,
                populate,
                advice: None,
                prevent_caching: None,
            },
        )?;

        let config = Self::ensure_config(directory, dim, populate)?;
        let chunks = read_chunks(directory, advice, populate.unwrap_or_default(), true)?;
        let inner = ChunkedVectorsRead {
            config,
            len: status.len,
            chunks,
            directory: directory.to_owned(),
        };
        Ok(Self { inner, status })
    }

    fn add_chunk(&mut self) -> OperationResult<()> {
        let chunk = create_chunk(
            &self.inner.directory,
            self.inner.chunks.len(),
            self.inner.config.chunk_size_bytes,
        )?;

        self.inner.chunks.push(chunk);
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
            count * self.inner.config.dim,
            "Vector size mismatch"
        );

        let start_key = start_key.as_();
        let chunk_idx = self.inner.get_chunk_index(start_key);
        let chunk_offset = self.inner.get_chunk_offset(start_key);

        // check if the vectors fit in the chunk
        if chunk_offset + vectors.len()
            > self.inner.config.dim * self.inner.config.chunk_size_vectors
        {
            return Err(OperationError::service_error(format!(
                "Vectors do not fit in the chunk. Chunk idx {chunk_idx}, chunk offset {chunk_offset}, vectors count {count}",
            )));
        }

        // Ensure capacity
        while chunk_idx >= self.inner.chunks.len() {
            self.add_chunk()?;
        }

        let chunk = &mut self.inner.chunks[chunk_idx];

        chunk.write((chunk_offset * size_of::<T>()) as u64, vectors)?;

        hw_counter
            .vector_io_write_counter()
            .incr_delta(size_of_val(vectors));

        let new_len = max(self.status.len, start_key + count);

        if new_len > self.status.len {
            self.status.len = new_len;
            self.inner.len = new_len;
        }
        Ok(())
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

    pub fn flusher(&self) -> Flusher {
        Box::new({
            let status_flusher = self.status.flusher();
            let chunks_flushers: Vec<_> = self
                .inner
                .chunks
                .iter()
                .map(|chunk| chunk.flusher())
                .collect();
            move || {
                for flusher in chunks_flushers {
                    flusher()?;
                }
                status_flusher()?;
                Ok(())
            }
        })
    }
}
