use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

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
}

pub trait EncodedStorageBuilder<TStorage: EncodedStorage> {
    fn build(self) -> TStorage;

    fn push_vector_data(&mut self, other: &[u8]);
}

impl EncodedStorage for Vec<u8> {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self[vector_size * index..vector_size * (index + 1)]
    }

    fn from_file(
        path: &Path,
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let expected_size = quantized_vector_size * vectors_count;
        if buffer.len() == expected_size {
            Ok(buffer)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Loaded storage size {} is not equal to expected size {expected_size}",
                    buffer.len()
                ),
            ))
        }
    }

    fn save_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut buffer = File::create(path)?;
        buffer.write_all(self.as_slice())?;
        buffer.flush()?;
        Ok(())
    }
}

impl EncodedStorageBuilder<Vec<u8>> for Vec<u8> {
    fn build(self) -> Vec<u8> {
        self
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        self.extend_from_slice(other);
    }
}
