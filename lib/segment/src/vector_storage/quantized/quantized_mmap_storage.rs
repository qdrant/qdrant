use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::{Madviseable, MmapFlusher, advice};
use common::types::PointOffsetType;
use fs_err as fs;
use fs_err::OpenOptions;
use memmap2::{Mmap, MmapMut};

#[derive(Debug)]
pub struct QuantizedMmapStorage {
    mmap: Mmap,
    quantized_vector_size: NonZeroUsize,
    path: PathBuf,
}

impl QuantizedMmapStorage {
    pub fn populate(&self) {
        self.mmap.populate();
    }
}

pub struct QuantizedMmapStorageBuilder {
    mmap: MmapMut,
    cursor_pos: usize,
    quantized_vector_size: NonZeroUsize,
    path: PathBuf,
}

impl QuantizedMmapStorage {
    pub fn from_file(
        path: &Path,
        quantized_vector_size: usize,
    ) -> std::io::Result<QuantizedMmapStorage> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        advice::madvise(&mmap, advice::get_global())?;

        let quantized_vector_size = NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "`quantized_vector_size` must be non-zero",
            )
        })?;
        if !mmap.len().is_multiple_of(quantized_vector_size.get()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Encoded file size ({}) is not a multiple of quantized_vector_size ({})",
                    mmap.len(),
                    quantized_vector_size
                ),
            ));
        }
        Ok(Self {
            mmap,
            quantized_vector_size,
            path: path.to_path_buf(),
        })
    }
}

impl quantization::EncodedStorage for QuantizedMmapStorage {
    fn get_vector_data(&self, index: PointOffsetType) -> &[u8] {
        let start = self.quantized_vector_size.get() * index as usize;
        let end = self.quantized_vector_size.get() * (index + 1) as usize;
        self.mmap.get(start..end).unwrap_or(&[])
    }

    fn upsert_vector(
        &mut self,
        _id: PointOffsetType,
        _vector: &[u8],
        _hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Cannot upsert vector in mmap storage",
        ))
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn vectors_count(&self) -> usize {
        self.mmap.len() / self.quantized_vector_size.get()
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

    fn push_vector_data(&mut self, other: &[u8]) -> std::io::Result<()> {
        debug_assert_eq!(
            self.quantized_vector_size.get(),
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
        Ok(())
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
        let encoded_storage_size = quantized_vector_size * vectors_count;
        path.parent()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Path must have a parent directory",
                )
            })
            .and_then(fs::create_dir_all)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Don't truncate because we explicitly set the length later
            .truncate(false)
            .open(path)?;
        file.set_len(encoded_storage_size as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file) }?;
        advice::madvise(&mmap, advice::get_global())?;
        Ok(Self {
            mmap,
            cursor_pos: 0,
            quantized_vector_size: NonZeroUsize::new(quantized_vector_size).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "`quantized_vector_size` must be non-zero",
                )
            })?,
            path: path.to_path_buf(),
        })
    }
}
