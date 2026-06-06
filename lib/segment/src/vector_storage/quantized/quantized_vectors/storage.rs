use std::fmt;

use common::universal_io::MmapFile;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;
use quantization::{EncodedVectors, EncodedVectorsPQ};

use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedMmapStorage;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedMmap, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

type ScalarRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type ScalarMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedStorage<MmapFile>>,
    MultivectorOffsetsStorageMmap,
>;

type ScalarChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

type PQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type PQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedStorage<MmapFile>>,
    MultivectorOffsetsStorageMmap,
>;

type PQChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

type BinaryRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type BinaryMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedStorage<MmapFile>>,
    MultivectorOffsetsStorageMmap,
>;

type BinaryChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

type TQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type TQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedStorage<MmapFile>>,
    MultivectorOffsetsStorageMmap,
>;

type TQChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedChunkedMmapStorage>,
    MultivectorOffsetsStorageChunkedMmap,
>;

pub enum QuantizedVectorStorage {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedStorage<MmapFile>>),
    ScalarChunkedMmap(EncodedVectorsU8<QuantizedChunkedMmapStorage>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedStorage<MmapFile>>),
    PQChunkedMmap(EncodedVectorsPQ<QuantizedChunkedMmapStorage>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedStorage<MmapFile>>),
    BinaryChunkedMmap(EncodedVectorsBin<u128, QuantizedChunkedMmapStorage>),
    TQRam(EncodedVectorsTQ<QuantizedRamStorage>),
    TQMmap(EncodedVectorsTQ<QuantizedStorage<MmapFile>>),
    TQChunkedMmap(EncodedVectorsTQ<QuantizedChunkedMmapStorage>),
    ScalarRamMulti(ScalarRamMulti),
    ScalarMmapMulti(ScalarMmapMulti),
    ScalarChunkedMmapMulti(ScalarChunkedMmapMulti),
    PQRamMulti(PQRamMulti),
    PQMmapMulti(PQMmapMulti),
    PQChunkedMmapMulti(PQChunkedMmapMulti),
    BinaryRamMulti(BinaryRamMulti),
    BinaryMmapMulti(BinaryMmapMulti),
    BinaryChunkedMmapMulti(BinaryChunkedMmapMulti),
    TQRamMulti(TQRamMulti),
    TQMmapMulti(TQMmapMulti),
    TQChunkedMmapMulti(TQChunkedMmapMulti),
}

impl QuantizedVectorStorage {
    pub fn is_on_disk(&self) -> bool {
        match self {
            QuantizedVectorStorage::ScalarRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQRam(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.is_on_disk(),
        }
    }
}

impl QuantizedVectorStorage {
    /// Heap memory used by this storage that is not tracked in files.
    pub fn heap_size_bytes(&self) -> usize {
        match &self {
            QuantizedVectorStorage::ScalarRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::ScalarMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.heap_size_bytes(),
        }
    }
}

impl fmt::Debug for QuantizedVectorStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("QuantizedVectorStorage").finish()
    }
}
