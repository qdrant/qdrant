#[cfg(feature = "testing")]
use std::fs::File;
#[cfg(feature = "testing")]
use std::io::{Read, Write};
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
#[cfg(feature = "testing")]
use memory::fadvise::OneshotFile;
use memory::mmap_type::MmapFlusher;

pub trait EncodedStorage {
    fn get_vector_data(&self, index: usize) -> &[u8];

    fn is_on_disk(&self) -> bool;

    fn update_vector(
        &mut self,
        id: u32,
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

    fn push_vector_data(&mut self, other: &[u8]);
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorage {
    data: Vec<u8>,
    quantized_vector_size: usize,
    path: Option<PathBuf>,
}

#[cfg(feature = "testing")]
impl EncodedStorage for TestEncodedStorage {
    fn get_vector_data(&self, index: usize) -> &[u8] {
        &self.data[self.quantized_vector_size * index..self.quantized_vector_size * (index + 1)]
    }

    fn update_vector(
        &mut self,
        id: u32,
        vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        // Skip hardware counter increment because it's a RAM storage.
        if id as usize >= self.data.len() / self.quantized_vector_size {
            self.data
                .resize((id as usize + 1) * self.quantized_vector_size, 0);
        }
        self.data[id as usize * self.quantized_vector_size
            ..(id as usize + 1) * self.quantized_vector_size]
            .copy_from_slice(vector);
        Ok(())
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn vectors_count(&self) -> usize {
        self.data
            .len()
            .checked_div(self.quantized_vector_size)
            .unwrap_or_default()
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
impl TestEncodedStorage {
    pub fn load(
        path: &std::path::Path,
        quantized_vector_size: usize,
    ) -> std::io::Result<TestEncodedStorage> {
        let mut file = OneshotFile::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        file.drop_cache()?;
        Ok(TestEncodedStorage {
            data: buffer,
            quantized_vector_size,
            path: Some(path.to_path_buf()),
        })
    }
}

#[cfg(feature = "testing")]
pub struct TestEncodedStorageBuilder {
    data: Vec<u8>,
    path: Option<PathBuf>,
    quantized_vector_size: Option<usize>,
}

#[cfg(feature = "testing")]
impl TestEncodedStorageBuilder {
    pub fn new(path: Option<&std::path::Path>) -> Self {
        Self {
            data: Vec::new(),
            path: path.map(PathBuf::from),
            quantized_vector_size: None,
        }
    }
}

#[cfg(feature = "testing")]
impl EncodedStorageBuilder for TestEncodedStorageBuilder {
    type Storage = TestEncodedStorage;

    fn build(self) -> std::io::Result<TestEncodedStorage> {
        if let Some(path) = &self.path {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut file = File::create(path)?;
            file.write_all(&self.data)?;
            file.sync_all()?;
        }
        Ok(TestEncodedStorage {
            data: self.data,
            quantized_vector_size: self.quantized_vector_size.unwrap_or_default(),
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        debug_assert_ne!(other.len(), 0, "Cannot push empty vector data");
        match self.quantized_vector_size {
            Some(size) => {
                if other.len() % size != 0 {
                    panic!("Data length is not a multiple of quantized vector size");
                }
            }
            None => {
                self.quantized_vector_size = Some(other.len());
            }
        }
        self.data.extend_from_slice(other);
    }
}
