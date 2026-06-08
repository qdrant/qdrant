use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use quantization::EncodedStorage;

use crate::common::operation_error::OperationResult;
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

    pub(super) fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()> {
        match self {
            TurboEncodedVectorStorage::Mmap(_) => {
                panic!("Can't directly update vector in mmap storage")
            }
            TurboEncodedVectorStorage::ChunkedMmap(chunked_mmap) => {
                chunked_mmap.upsert_vector(id, vector, hw_counter)
            }
        }
    }
}
