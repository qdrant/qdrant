use std::path::Path;

use memmap2::{Mmap, MmapMut};
use quantization::encoder::{EncodingParameters, Storage, StorageBuilder};

use crate::madvise;

pub struct QuantizedMmapStorage {
    mmap: Mmap,
}

pub struct QuantizedMmapStorageBuilder {
    mmap: MmapMut,
    cursor_pos: usize,
}

impl Storage for QuantizedMmapStorage {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self.mmap[vector_size * index..vector_size * (index + 1)]
    }

    fn from_file(
        path: &Path,
        _encoding_parameters: &EncodingParameters,
    ) -> std::io::Result<QuantizedMmapStorage> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;
        Ok(Self { mmap })
    }

    fn save_to_file(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because mmap is already saved
        Ok(())
    }
}

impl StorageBuilder<QuantizedMmapStorage> for QuantizedMmapStorageBuilder {
    fn build(self) -> QuantizedMmapStorage {
        self.mmap.flush().unwrap();
        let mmap = self.mmap.make_read_only().unwrap(); // TODO: remove unwrap
        QuantizedMmapStorage { mmap }
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
        encoding_parameters: &EncodingParameters,
    ) -> std::io::Result<Self> {
        let encoded_storage_size = encoding_parameters.get_vector_data_size() * vectors_count;
        path.parent().map(std::fs::create_dir_all);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
