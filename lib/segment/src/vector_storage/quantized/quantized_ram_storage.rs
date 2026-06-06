use std::borrow::Cow;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::OneshotFile;
use common::mmap::{AdviceSetting, MmapFlusher};
use common::types::PointOffsetType;
use common::universal_io::{OpenOptions, Populate, ReadOnly, UniversalRead};
use fs_err as fs;
use fs_err::File;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::vector_utils::TrySetCapacityExact;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::volatile_chunked_vectors::VolatileChunkedVectors;

#[derive(Debug)]
pub struct QuantizedRamStorage {
    vectors: VolatileChunkedVectors<u8>,
    path: PathBuf,
}

impl QuantizedRamStorage {
    pub fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self> {
        let mut vectors = VolatileChunkedVectors::<u8>::new(quantized_vector_size);
        let file = OneshotFile::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; quantized_vector_size];
        while reader.read_exact(&mut buffer).is_ok() {
            vectors.push(&buffer).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("Failed to load quantized vectors from file: {err}"),
                )
            })?;
        }
        reader.into_inner().drop_cache()?;
        vectors.shrink_last_chunk();
        Ok(QuantizedRamStorage {
            vectors,
            path: path.to_path_buf(),
        })
    }

    /// Load all quantized vectors into RAM through the provided [`UniversalRead`]
    /// filesystem, performing no writes.
    ///
    /// This is the read-only counterpart of [`Self::from_file`]: it sources the
    /// bytes through the pluggable `S` backend (mmap, io_uring, object storage, …)
    /// instead of opening the local path directly, so it works wherever a
    /// [`UniversalRead`] handle is available.
    pub fn from_universal_read<S: UniversalRead>(
        fs: &S::Fs,
        path: &Path,
        quantized_vector_size: usize,
    ) -> OperationResult<Self> {
        if quantized_vector_size == 0 {
            return Err(OperationError::service_error(
                "`quantized_vector_size` must be non-zero",
            ));
        }

        let storage = ReadOnly::<S>::open(
            fs,
            path,
            OpenOptions {
                writeable: false,
                need_sequential: true,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;

        let len = storage.len::<u8>()? as usize;
        if !len.is_multiple_of(quantized_vector_size) {
            return Err(OperationError::inconsistent_storage(format!(
                "Encoded file size ({len}) is not a multiple of quantized_vector_size ({quantized_vector_size})",
            )));
        }

        let data = storage.read_whole::<u8>()?;
        let mut vectors = VolatileChunkedVectors::<u8>::new(quantized_vector_size);
        for chunk in data.chunks_exact(quantized_vector_size) {
            vectors.push(chunk).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load quantized vectors into RAM: {err}"
                ))
            })?;
        }
        vectors.shrink_last_chunk();

        Ok(QuantizedRamStorage {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorage for QuantizedRamStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.vectors.get(index as VectorOffsetType))
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.vectors
            .insert(id as usize, vector)
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        Ok(())
    }

    fn is_in_ram_or_mmap() -> bool {
        true
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self) -> usize {
        self.vectors.len()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn heap_size_bytes(&self) -> usize {
        let Self { vectors, path: _ } = self;
        vectors.heap_size_bytes()
    }
}

pub struct QuantizedRamStorageBuilder {
    pub vectors: VolatileChunkedVectors<u8>,
    pub path: PathBuf,
}

impl QuantizedRamStorageBuilder {
    pub fn new(path: &Path, count: usize, dim: usize) -> OperationResult<Self> {
        let mut vectors = VolatileChunkedVectors::new(dim);
        vectors.try_set_capacity_exact(count)?;
        Ok(Self {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorageBuilder for QuantizedRamStorageBuilder {
    type Storage = QuantizedRamStorage;
    type Error = std::io::Error;

    fn build(self) -> std::io::Result<QuantizedRamStorage> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let mut buffer = BufWriter::new(File::create(&self.path)?);
        for i in 0..self.vectors.len() {
            buffer.write_all(self.vectors.get(i))?;
        }

        // Explicitly flush write buffer so we can catch IO errors
        buffer.flush()?;
        buffer.into_inner()?.sync_all()?;

        Ok(QuantizedRamStorage {
            vectors: self.vectors,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        self.vectors
            .push(other)
            .map(|_| ())
            .map_err(|e| std::io::Error::other(format!("Failed to push vector data: {e}")))
    }
}
