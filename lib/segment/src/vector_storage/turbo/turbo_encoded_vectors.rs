use std::borrow::Cow;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
#[cfg(target_os = "linux")]
use common::universal_io::{IoUringFile, IoUringFs};
use common::universal_io::{MmapFile, MmapFs, UniversalRead};
use quantization::EncodedStorage;

use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

/// Raw quantized storage backend for the TurboQuant-encoded bytes.
pub(super) enum TurboEncodedVectorStorage {
    /// Single mem-mapped file of encoded vectors.
    Mmap(QuantizedStorage<MmapFile>),

    /// Single file of encoded vectors, read through io_uring (non-appendable).
    #[cfg(target_os = "linux")]
    Uring(QuantizedStorage<IoUringFile>),

    /// Chunked mem-mapped encoded vectors (appendable).
    ChunkedMmap(QuantizedChunkedStorage<MmapFile>),
}

impl TurboEncodedVectorStorage {
    /// Open (create-or-load) the single mem-mapped file backend (non-appendable).
    pub(super) fn open_mmap(
        path: &Path,
        quantized_vector_size: usize,
        populate: bool,
    ) -> OperationResult<Self> {
        Ok(Self::Mmap(QuantizedStorage::<MmapFile>::open(
            &MmapFs,
            path,
            quantized_vector_size,
            populate,
        )?))
    }

    /// Open (create-or-load) the single-file io_uring backend (non-appendable).
    #[cfg(target_os = "linux")]
    pub(super) fn open_uring(
        path: &Path,
        quantized_vector_size: usize,
        populate: bool,
    ) -> OperationResult<Self> {
        Ok(Self::Uring(QuantizedStorage::<IoUringFile>::open(
            &IoUringFs,
            path,
            quantized_vector_size,
            populate,
        )?))
    }

    /// Open (create-or-load) the appendable chunked mem-mapped backend.
    pub(super) fn open_chunked_mmap(
        path: &Path,
        quantized_vector_size: usize,
        in_ram: bool,
    ) -> OperationResult<Self> {
        Ok(Self::ChunkedMmap(QuantizedChunkedStorage::new(
            MmapFs,
            path,
            quantized_vector_size,
            in_ram,
        )?))
    }

    /// Raw encoded blob for one vector (no dequantization).
    pub(super) fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            Self::Mmap(s) => s.get_vector_data(key),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.get_vector_data(key),
            Self::ChunkedMmap(s) => s.get_vector_data(key),
        }
    }

    /// Raw encoded blob for one vector (no dequantization).
    pub(super) fn get_quantized_vector_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Mmap(s) => s.get_vector_data_opt(key),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.get_vector_data_opt(key),
            Self::ChunkedMmap(s) => s.get_vector_data_opt(key),
        }
    }

    /// Run `f` for each vector in the batch, batching the underlying reads
    /// (io_uring submission batching / mmap prefetch on the single-file
    /// backends; the appendable chunked backend reads one record at a time).
    pub(super) fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        mut f: F,
    ) -> OperationResult<()> {
        match self {
            Self::Mmap(s) => s.for_each_in_batch(keys, f),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.for_each_in_batch(keys, f),
            Self::ChunkedMmap(s) => {
                for (idx, &key) in keys.iter().enumerate() {
                    f(idx, &s.get_vector_data(key));
                }
                Ok(())
            }
        }
    }

    /// Number of encoded vectors (including soft-deleted).
    pub(super) fn vectors_count(&self) -> usize {
        match self {
            Self::Mmap(s) => s.vectors_count(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.vectors_count(),
            Self::ChunkedMmap(s) => s.vectors_count(),
        }
    }

    pub(super) fn is_on_disk(&self) -> bool {
        match self {
            Self::Mmap(s) => s.is_on_disk(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.is_on_disk(),
            Self::ChunkedMmap(s) => s.is_on_disk(),
        }
    }

    /// All on-disk files backing the encoded vectors.
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mmap(s) => s.files(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.files(),
            Self::ChunkedMmap(s) => s.files(),
        }
    }

    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mmap(s) => s.immutable_files(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.immutable_files(),
            Self::ChunkedMmap(s) => s.immutable_files(),
        }
    }

    pub(super) fn flusher(&self) -> MmapFlusher {
        match self {
            Self::Mmap(s) => s.flusher(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.flusher(),
            Self::ChunkedMmap(s) => s.flusher(),
        }
    }

    /// Populate all pages of the encoded vectors into the page cache.
    pub(super) fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Mmap(s) => s.populate(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.populate(),
            Self::ChunkedMmap(s) => s.populate()?,
        }
        Ok(())
    }

    /// Drop the disk cache for the encoded vectors.
    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Mmap(s) => s.clear_cache(),
            #[cfg(target_os = "linux")]
            Self::Uring(s) => s.clear_cache(),
            Self::ChunkedMmap(s) => s.clear_cache()?,
        }
        Ok(())
    }

    pub(super) fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        match self {
            TurboEncodedVectorStorage::Mmap(storage) => {
                // We let the underlying storage decide the error instead of doing it here.
                // Therefore, we don't assume it's read-only here and pretend to write.
                storage.upsert_vector(id, vector, hw_counter)
            }
            #[cfg(target_os = "linux")]
            TurboEncodedVectorStorage::Uring(storage) => {
                storage.upsert_vector(id, vector, hw_counter)
            }
            TurboEncodedVectorStorage::ChunkedMmap(storage) => {
                storage.upsert_vector(id, vector, hw_counter)
            }
        }
    }

    /// Bulk-ingest already-encoded vectors, dispatching to the backend implementation.
    pub(super) fn update_from<'a>(
        &mut self,
        vectors: impl Iterator<Item = Cow<'a, [u8]>>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            Self::Mmap(storage) => {
                Self::update_from_single_file(storage, &MmapFs, vectors, stopped)
            }
            #[cfg(target_os = "linux")]
            Self::Uring(storage) => {
                Self::update_from_single_file(storage, &IoUringFs, vectors, stopped)
            }
            Self::ChunkedMmap(storage) => Self::update_from_chunked(storage, vectors, stopped),
        }
    }

    /// Single-file backends: bulk-append encoded bytes to the file, then reopen once
    /// through `fs` so reads observe the appended vectors (mirrors
    /// `DenseVectorStorageImpl::update_from`).
    fn update_from_single_file<'a, S: UniversalRead>(
        storage: &mut QuantizedStorage<S>,
        fs: &S::Fs,
        vectors: impl Iterator<Item = Cow<'a, [u8]>>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = storage.vectors_count() as PointOffsetType;
        let mut end_index = start_index;

        let mut writer = storage.open_appender()?;
        for vector in vectors {
            check_process_stopped(stopped)?;
            writer.write_all(&vector)?;
            end_index += 1;
        }

        // Persist + reopen so reads observe the appended vectors.
        writer.flush()?;
        let file = writer
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_data()?;
        storage.reload(fs)?;

        Ok(start_index..end_index)
    }

    /// Bulk-append already-encoded vectors, returning the appended key range.
    fn update_from_chunked<'a>(
        storage: &mut QuantizedChunkedStorage<MmapFile>,
        vectors: impl Iterator<Item = Cow<'a, [u8]>>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let disposed_hw = HardwareCounterCell::disposable();
        let start_index = storage.vectors_count() as PointOffsetType;
        let mut key = start_index;

        for vector in vectors {
            check_process_stopped(stopped)?;
            storage.upsert_vector(key, &vector, &disposed_hw)?;
            key += 1;
        }

        Ok(start_index..key)
    }
}
