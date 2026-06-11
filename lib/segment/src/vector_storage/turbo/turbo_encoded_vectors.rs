use std::borrow::Cow;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use quantization::EncodedStorage;

use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

/// Raw quantized storage backend for the TurboQuant-encoded bytes.
pub(super) enum TurboEncodedVectorStorage {
    /// Single mem-mapped file of encoded vectors.
    Mmap(QuantizedStorage<MmapFile>),

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
            Self::ChunkedMmap(s) => s.get_vector_data(key),
        }
    }

    /// Raw encoded blob for one vector (no dequantization).
    pub(super) fn get_quantized_vector_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Mmap(s) => s.get_vector_data_opt(key),
            Self::ChunkedMmap(s) => s.get_vector_data_opt(key),
        }
    }

    /// Number of encoded vectors (including soft-deleted).
    pub(super) fn vectors_count(&self) -> usize {
        match self {
            Self::Mmap(s) => s.vectors_count(),
            Self::ChunkedMmap(s) => s.vectors_count(),
        }
    }

    pub(super) fn is_on_disk(&self) -> bool {
        match self {
            Self::Mmap(s) => s.is_on_disk(),
            Self::ChunkedMmap(s) => s.is_on_disk(),
        }
    }

    /// All on-disk files backing the encoded vectors.
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mmap(s) => s.files(),
            Self::ChunkedMmap(s) => s.files(),
        }
    }

    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Mmap(s) => s.immutable_files(),
            Self::ChunkedMmap(s) => s.immutable_files(),
        }
    }

    pub(super) fn flusher(&self) -> MmapFlusher {
        match self {
            Self::Mmap(s) => s.flusher(),
            Self::ChunkedMmap(s) => s.flusher(),
        }
    }

    /// Populate all pages of the encoded vectors into the page cache.
    pub(super) fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Mmap(s) => s.populate(),
            Self::ChunkedMmap(s) => s.populate()?,
        }
        Ok(())
    }

    /// Drop the disk cache for the encoded vectors.
    pub(super) fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Mmap(s) => s.clear_cache(),
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
            Self::Mmap(storage) => Self::update_from_mmap(storage, vectors, stopped),
            Self::ChunkedMmap(storage) => storage.update_from(vectors, stopped),
        }
    }

    /// Single-file backend: bulk-append encoded bytes to the file, then re-mmap once
    /// (mirrors `DenseVectorStorageImpl::update_from`).
    fn update_from_mmap<'a>(
        storage: &mut QuantizedStorage<MmapFile>,
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

        // Persist + re-mmap so reads observe the appended vectors.
        writer.flush()?;
        let file = writer
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_data()?;
        storage.reload()?;

        Ok(start_index..end_index)
    }
}
