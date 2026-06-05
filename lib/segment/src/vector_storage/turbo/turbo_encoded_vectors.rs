use std::borrow::Cow;
use std::path::PathBuf;

use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use quantization::EncodedStorage;

use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

/// Raw quantized storage backend for the TurboQuant-encoded bytes.
pub(super) enum TurboEncodedVectorStorage {
    /// In-memory encoded vectors.
    Ram(QuantizedRamStorage),
    /// Single mem-mapped file of encoded vectors.
    Mmap(QuantizedStorage<MmapFile>),
    /// Chunked mem-mapped encoded vectors (appendable).
    ChunkedMmap(QuantizedChunkedStorage<MmapFile>),
}

impl TurboEncodedVectorStorage {
    /// Raw encoded blob for one vector (no dequantization).
    pub(super) fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            Self::Ram(s) => s.get_vector_data(key),
            Self::Mmap(s) => s.get_vector_data(key),
            Self::ChunkedMmap(s) => s.get_vector_data(key),
        }
    }

    /// Raw encoded blob for one vector (no dequantization).
    pub(super) fn get_quantized_vector_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Ram(s) => s.get_vector_data_opt(key),
            Self::Mmap(s) => s.get_vector_data_opt(key),
            Self::ChunkedMmap(s) => s.get_vector_data_opt(key),
        }
    }

    /// Number of encoded vectors (including soft-deleted).
    pub(super) fn vectors_count(&self) -> usize {
        match self {
            Self::Ram(s) => s.vectors_count(),
            Self::Mmap(s) => s.vectors_count(),
            Self::ChunkedMmap(s) => s.vectors_count(),
        }
    }

    pub(super) fn is_on_disk(&self) -> bool {
        unimplemented!("TurboEncodedVectorStorage::is_on_disk")
    }

    /// All on-disk files backing the encoded vectors.
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Ram(s) => s.files(),
            Self::Mmap(s) => s.files(),
            Self::ChunkedMmap(s) => s.files(),
        }
    }

    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Ram(s) => s.immutable_files(),
            Self::Mmap(s) => s.immutable_files(),
            Self::ChunkedMmap(s) => s.immutable_files(),
        }
    }

    pub(super) fn flusher(&self) -> MmapFlusher {
        match self {
            Self::Ram(s) => s.flusher(),
            Self::Mmap(s) => s.flusher(),
            Self::ChunkedMmap(s) => s.flusher(),
        }
    }
}
