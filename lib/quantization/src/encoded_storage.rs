use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use memory::fadvise::OneshotFile;

pub trait EncodedStorage {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8];

    fn from_file(
        path: &Path,
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> std::io::Result<Self>
    where
        Self: Sized;

    fn save_to_file(&self, path: &Path) -> std::io::Result<()>;

    fn is_on_disk(&self) -> bool;

    fn push_vector(
        &mut self,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn vectors_count(&self, quantized_vector_size: usize) -> usize;
}

pub trait EncodedStorageBuilder {
    type Storage: EncodedStorage;

    fn build(self) -> std::io::Result<Self::Storage>;

    fn push_vector_data(&mut self, other: &[u8]);
}

impl EncodedStorage for Vec<u8> {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self[vector_size * index..vector_size * (index + 1)]
    }

    fn push_vector(
        &mut self,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.try_reserve(vector.len())?;
        self.extend_from_slice(vector);
        Ok(())
    }

    fn from_file(
        path: &Path,
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> std::io::Result<Self> {
        let mut file = OneshotFile::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        file.drop_cache()?;
        let expected_size = quantized_vector_size * vectors_count;
        if buffer.len() == expected_size {
            Ok(buffer)
        } else {
            Err(std::io::Error::other(format!(
                "Loaded storage size {} is not equal to expected size {expected_size}",
                buffer.len()
            )))
        }
    }

    fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut buffer = File::create(path)?;
        buffer.write_all(self.as_slice())?;
        buffer.sync_all()?;
        Ok(())
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self, quantized_vector_size: usize) -> usize {
        self.len() / quantized_vector_size
    }
}

impl EncodedStorageBuilder for Vec<u8> {
    type Storage = Vec<u8>;

    fn build(self) -> std::io::Result<Vec<u8>> {
        Ok(self)
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        self.extend_from_slice(other);
    }
}
