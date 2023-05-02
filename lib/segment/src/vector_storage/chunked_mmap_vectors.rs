use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use serde::{Deserialize, Serialize};

use crate::common::mmap_ops::{
    open_write_mmap, transmute_from_u8_to_array, transmute_to_u8, transmute_to_u8_array,
};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};

#[cfg(test)]
const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4Mb

#[cfg(not(test))]
const DEFAULT_CHUNK_SIZE: usize = 128 * 1024 * 1024; // 128Mb

const CONFIG_FILE_NAME: &str = "config.json";
const STATUS_FILE_NAME: &str = "status.json";

const MMAP_CHUNKS_PATTERN_START: &str = "chunk_";
const MMAP_CHUNKS_PATTERN_END: &str = ".mmap";

struct ChunkedMMapStatus {
    len: usize,
}

#[derive(Serialize, Deserialize)]
struct ChunkedMMapConfig {
    chunk_size: usize,
    dim: usize,
}

pub struct ChunkedMMapVectors<T> {
    config: ChunkedMMapConfig,
    status: ChunkedMMapStatus,
    _phantom: PhantomData<T>,
    chunks: Vec<MmapMut>,
    status_mmap: MmapMut,
    directory: PathBuf,
}

impl ChunkedMMapVectors<VectorElementType> {
    fn status_file(directory: &Path) -> PathBuf {
        directory.join(STATUS_FILE_NAME)
    }

    fn write_to_mmap<T>(mmap: &mut MmapMut, offset: usize, data: T) {
        debug_assert!(offset + size_of::<T>() <= mmap.len());
        let ptr = mmap.as_mut_ptr();
        unsafe {
            let byte_data = transmute_to_u8(&data);
            std::ptr::copy_nonoverlapping(byte_data.as_ptr(), ptr.add(offset), byte_data.len());
        }
    }

    fn write_array_to_mmap<T>(mmap: &mut MmapMut, offset: usize, data: &[T]) {
        debug_assert!(offset + data.len() * size_of::<T>() <= mmap.len());
        let ptr = mmap.as_mut_ptr();
        unsafe {
            let byte_data = transmute_to_u8_array(data);
            std::ptr::copy_nonoverlapping(byte_data.as_ptr(), ptr.add(offset), byte_data.len());
        }
    }

    fn read_from_mmap<T>(mmap: &MmapMut, offset: usize) -> &T {
        debug_assert!(offset + size_of::<T>() <= mmap.len());
        let ptr = mmap.as_ptr();
        unsafe { &*(ptr.add(offset) as *const T) }
    }

    fn read_array_from_mmap<T>(mmap: &MmapMut, offset: usize, len: usize) -> &[T] {
        debug_assert!(offset + len * size_of::<T>() <= mmap.len());
        let byte_len = len * size_of::<T>();
        let slice = &mmap[offset..offset + byte_len];
        transmute_from_u8_to_array(slice)
    }

    fn ensure_status_file(directory: &Path) -> OperationResult<MmapMut> {
        let status_file = Self::status_file(directory);
        if !status_file.exists() {
            {
                let length = std::mem::size_of::<usize>() as u64;
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&status_file)?;
                file.set_len(length)?;
            }
            let mut mmap = open_write_mmap(&status_file)?;
            Self::write_to_mmap(&mut mmap, 0, 0usize);
            mmap.flush()?;
            Ok(mmap)
        } else {
            open_write_mmap(&status_file)
        }
    }

    fn ensure_config(directory: &Path, dim: usize) -> OperationResult<ChunkedMMapConfig> {
        let config_file = directory.join(CONFIG_FILE_NAME);
        if !config_file.exists() {
            let config = ChunkedMMapConfig {
                chunk_size: DEFAULT_CHUNK_SIZE,
                dim,
            };
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&config_file)?;
            serde_json::to_writer_pretty(&mut file, &config)?;
            file.flush()?;
            Ok(config)
        } else {
            let file = std::fs::File::open(&config_file)?;
            let config: ChunkedMMapConfig = serde_json::from_reader(file)?;

            if config.dim != dim {
                return Err(OperationError::service_error(format!(
                    "Wrong configuration in {}: expected {}, found {}",
                    config_file.display(),
                    config.dim,
                    dim
                )));
            }

            Ok(config)
        }
    }

    fn check_mmap_file_name_pattern(file_name: &str) -> bool {
        file_name.starts_with(MMAP_CHUNKS_PATTERN_START)
            && file_name.ends_with(MMAP_CHUNKS_PATTERN_END)
    }

    fn read_mmaps(directory: &Path) -> OperationResult<Vec<MmapMut>> {
        let mut result = Vec::new();
        for entry in directory.read_dir()? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file()
                && path
                    .file_name()
                    .and_then(|file_name| file_name.to_str())
                    .map(Self::check_mmap_file_name_pattern)
                    .unwrap_or(false)
            {
                result.push(open_write_mmap(&path)?);
            }
        }
        Ok(result)
    }

    pub fn open(directory: &Path, dim: usize) -> OperationResult<Self> {
        create_dir_all(directory)?;
        let status_mmap = Self::ensure_status_file(directory)?;
        let len = *Self::read_from_mmap(&status_mmap, 0);
        let status = ChunkedMMapStatus { len };
        let config = Self::ensure_config(directory, dim)?;
        let chunks = Self::read_mmaps(directory)?;

        let vectors = Self {
            config,
            status,
            _phantom: Default::default(),
            chunks,
            status_mmap,
            directory: directory.to_owned(),
        };
        Ok(vectors)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_chunked_mmap() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let dim = 512;
        {
            let chunked_mmap = ChunkedMMapVectors::open(dir.path(), dim).unwrap();
        }

        {
            let chunked_mmap = ChunkedMMapVectors::open(dir.path(), dim).unwrap();
        }
    }
}
