#[cfg(feature = "testing")]
use std::fs::File;
#[cfg(feature = "testing")]
use std::io::{Read, Write};
use std::path::Path;
#[cfg(feature = "testing")]
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
#[cfg(feature = "testing")]
use memory::fadvise::OneshotFile;
use memory::mmap_type::MmapFlusher;

pub trait EncodedStorage {
    fn get_vector_data(&self, index: PointOffsetType, vector_size: usize) -> &[u8];

    fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self>
    where
        Self: Sized;

    fn is_on_disk(&self) -> bool;

    fn push_vector(
        &mut self,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn vectors_count(&self, quantized_vector_size: usize) -> usize;

    fn flusher(&self) -> MmapFlusher;
}

pub trait EncodedStorageBuilder {
    type Storage: EncodedStorage;

    fn build(self) -> std::io::Result<Self::Storage>;

    fn push_vector_data(&mut self, other: &[u8]);
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorage {
    data: Vec<u8>,
}

#[cfg(feature = "testing")]
impl EncodedStorage for TestEncodedStorage {
    fn get_vector_data(&self, index: PointOffsetType, vector_size: usize) -> &[u8] {
        &self.data[vector_size * index as usize..vector_size * (index as usize + 1)]
    }

    fn push_vector(
        &mut self,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        self.data.try_reserve(vector.len())?;
        self.data.extend_from_slice(vector);
        Ok(())
    }

    fn from_file(path: &Path, _quantized_vector_size: usize) -> std::io::Result<Self> {
        let mut file = OneshotFile::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        file.drop_cache()?;
        Ok(Self { data: buffer })
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self, quantized_vector_size: usize) -> usize {
        self.data
            .len()
            .checked_div(quantized_vector_size)
            .unwrap_or_default()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorageBuilder {
    data: Vec<u8>,
    path: Option<PathBuf>,
}

#[cfg(feature = "testing")]
impl TestEncodedStorageBuilder {
    pub fn new(path: Option<&std::path::Path>) -> Self {
        Self {
            data: Vec::new(),
            path: path.map(PathBuf::from),
        }
    }
}

#[cfg(feature = "testing")]
impl EncodedStorageBuilder for TestEncodedStorageBuilder {
    type Storage = TestEncodedStorage;

    fn build(self) -> std::io::Result<Self::Storage> {
        if let Some(path) = &self.path {
            path.parent()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Path must have a parent directory",
                    )
                })
                .and_then(std::fs::create_dir_all)?;
            let mut file = File::create(path)?;
            file.write_all(&self.data)?;
            file.sync_all()?;
        }
        Ok(TestEncodedStorage { data: self.data })
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        self.data.extend_from_slice(other);
    }
}
