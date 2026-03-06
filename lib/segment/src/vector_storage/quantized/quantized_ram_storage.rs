use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::OneshotFile;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use fs_err as fs;
use fs_err::File;

use crate::common::operation_error::OperationResult;
use crate::common::vector_utils::TrySetCapacityExact;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;

#[derive(Debug)]
pub struct QuantizedRamStorage {
    vectors: ChunkedVectors<u8>,
    path: PathBuf,
}

impl QuantizedRamStorage {
    pub fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self> {
        let mut vectors = ChunkedVectors::<u8>::new(quantized_vector_size);
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
        Ok(QuantizedRamStorage {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorage for QuantizedRamStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        self.vectors.get(index as VectorOffsetType)
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
}

pub struct QuantizedRamStorageBuilder {
    pub vectors: ChunkedVectors<u8>,
    pub path: PathBuf,
}

impl QuantizedRamStorageBuilder {
    pub fn new(path: &Path, count: usize, dim: usize) -> OperationResult<Self> {
        let mut vectors = ChunkedVectors::new(dim);
        vectors.try_set_capacity_exact(count)?;
        Ok(Self {
            vectors,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorageBuilder for QuantizedRamStorageBuilder {
    type Storage = QuantizedRamStorage;

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
