use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use memmap2::{Mmap, MmapMut};
use memory::madvise;
use memory::madvise::Madviseable;
use memory::mmap_type::MmapFlusher;

#[derive(Debug)]
pub struct QuantizedMmapStorage {
    mmap: Mmap,
    quantized_vector_size: usize,
    path: PathBuf,
}

impl QuantizedMmapStorage {
    pub fn populate(&self) {
        self.mmap.populate();
    }

    pub fn from_file(
        path: &Path,
        quantized_vector_size: usize,
    ) -> std::io::Result<QuantizedMmapStorage> {
        if quantized_vector_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "quantized_vector_size must be > 0",
            ));
        }
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;
        Ok(Self {
            mmap,
            quantized_vector_size,
            path: path.to_path_buf(),
        })
    }
}

pub struct QuantizedMmapStorageBuilder {
    mmap: MmapMut,
    cursor_pos: usize,
    path: PathBuf,
    quantized_vector_size: usize,
}

impl quantization::EncodedStorage for QuantizedMmapStorage {
    fn get_vector_data(&self, index: usize) -> &[u8] {
        &self.mmap[self.quantized_vector_size * index..self.quantized_vector_size * (index + 1)]
    }

    fn push_vector(
        &mut self,
        _vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot push vector to mmap storage",
        ))
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self) -> usize {
        self.mmap
            .len()
            .checked_div(self.quantized_vector_size)
            .unwrap_or_default()
    }

    fn flusher(&self) -> MmapFlusher {
        // Mmap storage does not need a flusher, as it is non-appendable and already backed by a file.
        Box::new(|| Ok(()))
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }
}

impl quantization::EncodedStorageBuilder for QuantizedMmapStorageBuilder {
    type Storage = QuantizedMmapStorage;

    fn build(self) -> std::io::Result<QuantizedMmapStorage> {
        self.mmap.flush()?;
        let mmap = self.mmap.make_read_only()?;
        Ok(QuantizedMmapStorage {
            mmap,
            quantized_vector_size: self.quantized_vector_size,
            path: self.path,
        })
    }

    fn push_vector_data(&mut self, other: &[u8]) {
        debug_assert_eq!(
            self.quantized_vector_size,
            other.len(),
            "Pushed vector size does not match expected quantized vector size"
        );
        debug_assert!(
            self.cursor_pos + other.len() <= self.mmap.len(),
            "Overflow allocated quantization storage mmap file (cursor_pos {} + len {} > total {})",
            self.cursor_pos,
            other.len(),
            self.mmap.len()
        );
        self.mmap[self.cursor_pos..self.cursor_pos + other.len()].copy_from_slice(other);
        self.cursor_pos += other.len();
    }
}

impl QuantizedMmapStorageBuilder {
    pub fn new(
        path: &Path,
        vectors_count: usize,
        quantized_vector_size: usize,
    ) -> std::io::Result<Self> {
        if quantized_vector_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "quantized_vector_size must be > 0",
            ));
        }
        let encoded_storage_size = quantized_vector_size
            .checked_mul(vectors_count)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "encoded storage size overflow",
                )
            })?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Don't truncate because we explicitly set the length later
            .truncate(false)
            .open(path)?;
        file.set_len(encoded_storage_size as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file) }?;
        madvise::madvise(&mmap, madvise::get_global())?;
        Ok(Self {
            mmap,
            cursor_pos: 0,
            quantized_vector_size,
            path: path.to_path_buf(),
        })
    }
}
