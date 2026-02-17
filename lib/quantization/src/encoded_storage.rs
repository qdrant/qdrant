#[cfg(feature = "testing")]
use std::io::{Read, Write};
#[cfg(feature = "testing")]
use std::num::NonZeroUsize;
#[cfg(feature = "testing")]
use std::path::Path;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(feature = "testing")]
use common::fs::OneshotFile;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
#[cfg(feature = "testing")]
use fs_err as fs;
#[cfg(feature = "testing")]
use fs_err::File;

pub trait EncodedStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8];

    fn is_on_disk(&self) -> bool;

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn vectors_count(&self) -> usize;

    fn flusher(&self) -> MmapFlusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;
}

pub trait EncodedStorageBuilder {
    type Storage: EncodedStorage;

    fn build(self) -> std::io::Result<Self::Storage>;

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()>;
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorage {
    data: Vec<u8>,
    quantized_vector_size: NonZeroUsize,
    path: Option<PathBuf>,
}

#[cfg(feature = "testing")]
impl TestEncodedStorage {
    pub fn from_file(path: &Path, quantized_vector_size: usize) -> std::io::Result<Self> {
        let mut file = OneshotFile::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        file.drop_cache()?;
        if !buffer.len().is_multiple_of(quantized_vector_size) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "TestEncodedStorage: buffer size ({}) not divisible by quantized_vector_size ({})",
                    buffer.len(),
                    quantized_vector_size,
                ),
            ));
        }
        let quantized_vector_size = NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "`quantized_vector_size` must be non-zero",
            )
        })?;
        Ok(Self {
            data: buffer,
            quantized_vector_size,
            path: Some(path.to_path_buf()),
        })
    }
}

#[cfg(feature = "testing")]
impl EncodedStorage for TestEncodedStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        let start = self
            .quantized_vector_size
            .get()
            .saturating_mul(index as usize);
        let end = self
            .quantized_vector_size
            .get()
            .saturating_mul(index as usize + 1);
        self.data.get(start..end).unwrap_or(&[])
    }

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        if vector.len() != self.quantized_vector_size.get() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "upsert_vector: payload length {} != quantized_vector_size {}",
                    vector.len(),
                    self.quantized_vector_size
                ),
            ));
        }
        // Skip hardware counter increment because it's a RAM storage.
        let offset = id as usize * self.quantized_vector_size.get();
        if id as usize >= self.vectors_count() {
            self.data
                .resize(offset + self.quantized_vector_size.get(), 0);
        }
        self.data[offset..offset + self.quantized_vector_size.get()].copy_from_slice(vector);
        Ok(())
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self) -> usize {
        self.data.len() / self.quantized_vector_size.get()
    }

    fn flusher(&self) -> MmapFlusher {
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        if let Some(ref path) = self.path {
            vec![path.clone()]
        } else {
            vec![]
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files()
    }
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorageBuilder {
    data: Vec<u8>,
    path: Option<PathBuf>,
    quantized_vector_size: NonZeroUsize,
}

#[cfg(feature = "testing")]
impl TestEncodedStorageBuilder {
    pub fn new(path: Option<&std::path::Path>, quantized_vector_size: usize) -> Self {
        Self {
            data: Vec::new(),
            path: path.map(PathBuf::from),
            quantized_vector_size: NonZeroUsize::new(quantized_vector_size).unwrap_or_else(|| {
                panic!("quantized_vector_size must be non-zero");
            }),
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
                .and_then(fs::create_dir_all)?;
            let mut file = File::create(path)?;
            file.write_all(&self.data)?;
            file.sync_all()?;
        }
        Ok(TestEncodedStorage {
            data: self.data,
            quantized_vector_size: self.quantized_vector_size,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        debug_assert_eq!(other.len(), self.quantized_vector_size.get());
        self.data.extend_from_slice(other);
        Ok(())
    }
}
