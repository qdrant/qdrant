use std::alloc::Layout;
use std::borrow::Cow;
use std::fmt;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, UniversalRead};
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;
use quantization::{EncodedVectors, EncodedVectorsPQ};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::spaces::metric::Metric;
use crate::vector_storage::RawScorer;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedRead, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_scorer_builder::{
    QuantizedScorerBuilder, QuantizedScorerDispatch, internal_raw_multi_scorer, internal_raw_scorer,
};
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

type ScalarMmapMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedStorage<S>>,
    MultivectorOffsetsStorageMmap<S>,
>;
type ScalarRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;

type PQMmapMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedStorage<S>>,
    MultivectorOffsetsStorageMmap<S>,
>;
type PQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;

type BinaryMmapMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedStorage<S>>,
    MultivectorOffsetsStorageMmap<S>,
>;
type BinaryRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;

type TQMmapMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedStorage<S>>,
    MultivectorOffsetsStorageMmap<S>,
>;
type TQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;

// Chunked (appendable) on-disk format, opened read-only. Only Binary and TurboQuant
// quantization produce this layout; Scalar/PQ are always immutable.
type BinaryChunkedMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedChunkedStorageRead<S>>,
    MultivectorOffsetsStorageChunkedRead<S>,
>;
type TQChunkedMulti<S> = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedChunkedStorageRead<S>>,
    MultivectorOffsetsStorageChunkedRead<S>,
>;

/// Read-only counterpart of [`super::super::QuantizedVectorStorage`].
///
/// Generic over the [`UniversalRead`] backend `S`. It exposes no write path, but
/// can read every on-disk layout: in-RAM (`*Ram`), read-only mmap (`*Mmap`) and the
/// appendable chunked layout opened read-only (`*Chunked`, only produced by Binary
/// and TurboQuant quantization).
pub enum ReadOnlyQuantizedVectorStorage<S: UniversalRead = MmapFile> {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedStorage<S>>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedStorage<S>>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedStorage<S>>),
    BinaryChunked(EncodedVectorsBin<u128, QuantizedChunkedStorageRead<S>>),
    TQRam(EncodedVectorsTQ<QuantizedRamStorage>),
    TQMmap(EncodedVectorsTQ<QuantizedStorage<S>>),
    TQChunked(EncodedVectorsTQ<QuantizedChunkedStorageRead<S>>),
    ScalarRamMulti(ScalarRamMulti),
    ScalarMmapMulti(ScalarMmapMulti<S>),
    PQRamMulti(PQRamMulti),
    PQMmapMulti(PQMmapMulti<S>),
    BinaryRamMulti(BinaryRamMulti),
    BinaryMmapMulti(BinaryMmapMulti<S>),
    BinaryChunkedMulti(BinaryChunkedMulti<S>),
    TQRamMulti(TQRamMulti),
    TQMmapMulti(TQMmapMulti<S>),
    TQChunkedMulti(TQChunkedMulti<S>),
}

impl<S: UniversalRead> fmt::Debug for ReadOnlyQuantizedVectorStorage<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadOnlyQuantizedVectorStorage")
            .finish_non_exhaustive()
    }
}

impl<S: UniversalRead> ReadOnlyQuantizedVectorStorage<S> {
    pub fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => q.is_on_disk(),
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => q.is_on_disk(),
        }
    }

    /// Heap memory used by this storage that is not tracked in files.
    pub fn heap_size_bytes(&self) -> usize {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => q.heap_size_bytes(),
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => q.heap_size_bytes(),
        }
    }

    pub fn default_rescoring(&self) -> bool {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmap(_)
            | ReadOnlyQuantizedVectorStorage::PQRam(_)
            | ReadOnlyQuantizedVectorStorage::PQMmap(_)
            | ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQMmapMulti(_) => false,
            ReadOnlyQuantizedVectorStorage::BinaryRam(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmap(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryChunked(_)
            | ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(_) => true,
            ReadOnlyQuantizedVectorStorage::TQRam(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
        }
    }

    /// Get layout for a single quantized vector (size in bytes and required alignment).
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => Ok(q.layout()),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQChunkedMulti(_) => {
                Err(OperationError::service_error(
                    "Cannot get quantized vector layout from multivector storage",
                ))
            }
        }
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.get_quantized_vector(id),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQMmapMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQChunkedMulti(_) => {
                panic!("Cannot get quantized vector from multivector storage");
            }
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => q.files(),
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => q.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => q.immutable_files(),
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => q.immutable_files(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(_)
            | ReadOnlyQuantizedVectorStorage::PQRam(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRam(_)
            | ReadOnlyQuantizedVectorStorage::TQRam(_)
            | ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQRamMulti(_) => {} // already in RAM
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.storage().populate(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.storage().populate(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.storage().populate(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.storage().populate(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.storage().populate()?,
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.storage().populate()?,
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                q.storage().storage().populate()?;
                q.offsets_storage().populate()?;
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                q.storage().storage().populate()?;
                q.offsets_storage().populate()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(_)
            | ReadOnlyQuantizedVectorStorage::PQRam(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRam(_)
            | ReadOnlyQuantizedVectorStorage::TQRam(_)
            | ReadOnlyQuantizedVectorStorage::ScalarRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::PQRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::BinaryRamMulti(_)
            | ReadOnlyQuantizedVectorStorage::TQRamMulti(_) => {} // nothing to clear
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.storage().clear_cache(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.storage().clear_cache(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.storage().clear_cache(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.storage().clear_cache(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.storage().clear_cache()?,
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.storage().clear_cache()?,
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                q.storage().storage().clear_cache()?;
                q.offsets_storage().clear_cache()?;
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                q.storage().storage().clear_cache()?;
                q.offsets_storage().clear_cache()?;
            }
        }
        Ok(())
    }

    pub fn flusher(&self) -> MmapFlusher {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::PQRam(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQRam(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => q.flusher(),
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => q.flusher(),
        }
    }
}

impl<S: UniversalRead> QuantizedScorerDispatch for ReadOnlyQuantizedVectorStorage<S> {
    fn build_metric_scorer<'a, TElement, TMetric>(
        &'a self,
        builder: QuantizedScorerBuilder<'a>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::PQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
        }
    }

    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        match self {
            ReadOnlyQuantizedVectorStorage::ScalarRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::ScalarMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::PQRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::PQMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::ScalarRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::ScalarMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::PQRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::PQMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunked(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQChunked(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::BinaryChunkedMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            ReadOnlyQuantizedVectorStorage::TQChunkedMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
        }
    }
}
