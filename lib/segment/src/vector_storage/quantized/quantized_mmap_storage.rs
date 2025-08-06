use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use memmap2::{Mmap, MmapMut};
use memory::madvise;
use memory::madvise::Madviseable;

#[derive(Debug)]
pub struct QuantizedMmapStorage {
    mmap: Mmap,
}

impl QuantizedMmapStorage {
    pub fn populate(&self) {
        self.mmap.populate();
    }
}

pub struct QuantizedMmapStorageBuilder {
    mmap: MmapMut,
    cursor_pos: usize,
}

impl quantization::EncodedStorage for QuantizedMmapStorage {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self.mmap[vector_size * index..vector_size * (index + 1)]
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

    fn from_file(
        path: &Path,
        _quantized_vector_size: usize,
    ) -> std::io::Result<QuantizedMmapStorage> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;
        Ok(Self { mmap })
    }

    fn save_to_file(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because mmap is already saved
        Ok(())
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self, quantized_vector_size: usize) -> usize {
        self.mmap.len() / quantized_vector_size
    }
}

impl quantization::EncodedStorageBuilder for QuantizedMmapStorageBuilder {
    type Storage = QuantizedMmapStorage;

    fn build(self) -> std::io::Result<QuantizedMmapStorage> {
        self.mmap.flush()?;
        let mmap = self.mmap.make_read_only()?;
        Ok(QuantizedMmapStorage { mmap })
    }

    fn push_vector_data(&mut self, other: &[u8]) {
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
        let encoded_storage_size = quantized_vector_size * vectors_count;
        path.parent().map(std::fs::create_dir_all);

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
        })
    }
}
