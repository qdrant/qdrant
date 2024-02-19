use std::path::Path;

use memmap2::{Mmap, MmapMut};
use memory::madvise;

pub struct QuantizedMmapStorage {
    mmap: Mmap,
}

pub struct QuantizedMmapStorageBuilder {
    mmap: MmapMut,
    cursor_pos: usize,
}

impl quantization::EncodedStorage for QuantizedMmapStorage {
    fn get_vector_data(&self, index: usize, vector_size: usize) -> &[u8] {
        &self.mmap[vector_size * index..vector_size * (index + 1)]
    }

    fn from_file(
        path: &Path,
        quantized_vector_size: usize,
        vectors_count: usize,
    ) -> std::io::Result<QuantizedMmapStorage> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;

        let expected_size = quantized_vector_size * vectors_count;
        if mmap.len() == expected_size {
            Ok(Self { mmap })
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Loaded storage size {} is not equal to expected size {expected_size}",
                    mmap.len()
                ),
            ))
        }
    }

    fn save_to_file(&self, _path: &Path) -> std::io::Result<()> {
        // do nothing because mmap is already saved
        Ok(())
    }
}

impl quantization::EncodedStorageBuilder<QuantizedMmapStorage> for QuantizedMmapStorageBuilder {
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
