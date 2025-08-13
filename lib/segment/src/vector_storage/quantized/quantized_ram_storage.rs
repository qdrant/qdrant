use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use memory::fadvise::OneshotFile;
use memory::mmap_type::MmapFlusher;

use crate::common::operation_error::OperationResult;
use crate::common::vector_utils::TrySetCapacityExact;
use crate::vector_storage::chunked_vectors::ChunkedVectors;

#[derive(Debug)]
pub struct QuantizedRamStorage {
    vectors: ChunkedVectors<u8>,
}

impl quantization::EncodedStorage for QuantizedRamStorage {
    fn get_vector_data(&self, index: usize, _vector_size: usize) -> &[u8] {
        self.vectors.get(index)
    }

    fn push_vector(
        &mut self,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.vectors
            .push(vector)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::OutOfMemory, err.to_string()))?;
        Ok(())
    }

    fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self> {
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
        Ok(QuantizedRamStorage { vectors })
    }

    fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut buffer = BufWriter::new(File::create(path)?);
        for i in 0..self.vectors.len() {
            buffer.write_all(self.vectors.get(i))?;
        }

        // Explicitly flush write buffer so we can catch IO errors
        buffer.flush()?;
        buffer.into_inner()?.sync_all()?;
        Ok(())
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self, _quantized_vector_size: usize) -> usize {
        self.vectors.len()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }
}

pub struct QuantizedRamStorageBuilder {
    pub vectors: ChunkedVectors<u8>,
}

impl QuantizedRamStorageBuilder {
    pub fn new(count: usize, dim: usize) -> OperationResult<Self> {
        let mut vectors = ChunkedVectors::new(dim);
        vectors.try_set_capacity_exact(count)?;
        Ok(Self { vectors })
    }
}

impl quantization::EncodedStorageBuilder for QuantizedRamStorageBuilder {
    type Storage = QuantizedRamStorage;

    fn build(self) -> std::io::Result<QuantizedRamStorage> {
        Ok(QuantizedRamStorage {
            vectors: self.vectors,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        // Memory for ChunkedVectors are already pre-allocated,
        // so we do not expect any errors here.
        self.vectors.push(other).unwrap();
    }
}
