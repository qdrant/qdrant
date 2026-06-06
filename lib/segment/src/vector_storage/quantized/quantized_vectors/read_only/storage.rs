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
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
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

/// Read-only counterpart of [`super::super::QuantizedVectorStorage`].
///
/// Generic over the [`UniversalRead`] backend `S`, it keeps only the immutable
/// variants: in-RAM (`*Ram`) and read-only mmap (`*Mmap`). The appendable
/// `*ChunkedMmap` variants — the only ones that can be mutated — are
/// intentionally absent, so this enum exposes no write path.
pub enum QuantizedVectorStorageRead<S: UniversalRead = MmapFile> {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedStorage<S>>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedStorage<S>>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedStorage<S>>),
    TQRam(EncodedVectorsTQ<QuantizedRamStorage>),
    TQMmap(EncodedVectorsTQ<QuantizedStorage<S>>),
    ScalarRamMulti(ScalarRamMulti),
    ScalarMmapMulti(ScalarMmapMulti<S>),
    PQRamMulti(PQRamMulti),
    PQMmapMulti(PQMmapMulti<S>),
    BinaryRamMulti(BinaryRamMulti),
    BinaryMmapMulti(BinaryMmapMulti<S>),
    TQRamMulti(TQRamMulti),
    TQMmapMulti(TQMmapMulti<S>),
}

impl<S: UniversalRead> fmt::Debug for QuantizedVectorStorageRead<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("QuantizedVectorStorageRead").finish()
    }
}

impl<S: UniversalRead> QuantizedVectorStorageRead<S> {
    pub fn is_on_disk(&self) -> bool {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::PQRam(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::PQMmap(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::BinaryRam(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::TQRam(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::TQMmap(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::ScalarRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::PQRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::PQMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::BinaryRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::TQRamMulti(q) => q.is_on_disk(),
            QuantizedVectorStorageRead::TQMmapMulti(q) => q.is_on_disk(),
        }
    }

    /// Heap memory used by this storage that is not tracked in files.
    pub fn heap_size_bytes(&self) -> usize {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::PQRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::PQMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::BinaryRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::TQRam(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::TQMmap(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::ScalarRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::PQRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::PQMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::BinaryRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::TQRamMulti(q) => q.heap_size_bytes(),
            QuantizedVectorStorageRead::TQMmapMulti(q) => q.heap_size_bytes(),
        }
    }

    pub fn default_rescoring(&self) -> bool {
        match self {
            QuantizedVectorStorageRead::ScalarRam(_)
            | QuantizedVectorStorageRead::ScalarMmap(_)
            | QuantizedVectorStorageRead::PQRam(_)
            | QuantizedVectorStorageRead::PQMmap(_)
            | QuantizedVectorStorageRead::ScalarRamMulti(_)
            | QuantizedVectorStorageRead::ScalarMmapMulti(_)
            | QuantizedVectorStorageRead::PQRamMulti(_)
            | QuantizedVectorStorageRead::PQMmapMulti(_) => false,
            QuantizedVectorStorageRead::BinaryRam(_)
            | QuantizedVectorStorageRead::BinaryMmap(_)
            | QuantizedVectorStorageRead::BinaryRamMulti(_)
            | QuantizedVectorStorageRead::BinaryMmapMulti(_) => true,
            QuantizedVectorStorageRead::TQRam(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            QuantizedVectorStorageRead::TQMmap(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            QuantizedVectorStorageRead::TQRamMulti(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
            QuantizedVectorStorageRead::TQMmapMulti(q) => {
                QuantizedVectors::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
        }
    }

    /// Get layout for a single quantized vector (size in bytes and required alignment).
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::ScalarMmap(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::PQRam(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::PQMmap(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::BinaryRam(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::BinaryMmap(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::TQRam(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::TQMmap(q) => Ok(q.layout()),
            QuantizedVectorStorageRead::ScalarRamMulti(_)
            | QuantizedVectorStorageRead::ScalarMmapMulti(_)
            | QuantizedVectorStorageRead::PQRamMulti(_)
            | QuantizedVectorStorageRead::PQMmapMulti(_)
            | QuantizedVectorStorageRead::BinaryRamMulti(_)
            | QuantizedVectorStorageRead::BinaryMmapMulti(_)
            | QuantizedVectorStorageRead::TQRamMulti(_)
            | QuantizedVectorStorageRead::TQMmapMulti(_) => Err(OperationError::service_error(
                "Cannot get quantized vector layout from multivector storage",
            )),
        }
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::PQRam(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::PQMmap(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::BinaryRam(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::TQRam(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::TQMmap(q) => q.get_quantized_vector(id),
            QuantizedVectorStorageRead::ScalarRamMulti(_)
            | QuantizedVectorStorageRead::ScalarMmapMulti(_)
            | QuantizedVectorStorageRead::PQRamMulti(_)
            | QuantizedVectorStorageRead::PQMmapMulti(_)
            | QuantizedVectorStorageRead::BinaryRamMulti(_)
            | QuantizedVectorStorageRead::BinaryMmapMulti(_)
            | QuantizedVectorStorageRead::TQRamMulti(_)
            | QuantizedVectorStorageRead::TQMmapMulti(_) => {
                panic!("Cannot get quantized vector from multivector storage");
            }
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.files(),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.files(),
            QuantizedVectorStorageRead::PQRam(q) => q.files(),
            QuantizedVectorStorageRead::PQMmap(q) => q.files(),
            QuantizedVectorStorageRead::BinaryRam(q) => q.files(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.files(),
            QuantizedVectorStorageRead::TQRam(q) => q.files(),
            QuantizedVectorStorageRead::TQMmap(q) => q.files(),
            QuantizedVectorStorageRead::ScalarRamMulti(q) => q.files(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => q.files(),
            QuantizedVectorStorageRead::PQRamMulti(q) => q.files(),
            QuantizedVectorStorageRead::PQMmapMulti(q) => q.files(),
            QuantizedVectorStorageRead::BinaryRamMulti(q) => q.files(),
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => q.files(),
            QuantizedVectorStorageRead::TQRamMulti(q) => q.files(),
            QuantizedVectorStorageRead::TQMmapMulti(q) => q.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.immutable_files(),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.immutable_files(),
            QuantizedVectorStorageRead::PQRam(q) => q.immutable_files(),
            QuantizedVectorStorageRead::PQMmap(q) => q.immutable_files(),
            QuantizedVectorStorageRead::BinaryRam(q) => q.immutable_files(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.immutable_files(),
            QuantizedVectorStorageRead::TQRam(q) => q.immutable_files(),
            QuantizedVectorStorageRead::TQMmap(q) => q.immutable_files(),
            QuantizedVectorStorageRead::ScalarRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::PQRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::PQMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::BinaryRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::TQRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorageRead::TQMmapMulti(q) => q.immutable_files(),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(_)
            | QuantizedVectorStorageRead::PQRam(_)
            | QuantizedVectorStorageRead::BinaryRam(_)
            | QuantizedVectorStorageRead::TQRam(_)
            | QuantizedVectorStorageRead::ScalarRamMulti(_)
            | QuantizedVectorStorageRead::PQRamMulti(_)
            | QuantizedVectorStorageRead::BinaryRamMulti(_)
            | QuantizedVectorStorageRead::TQRamMulti(_) => {} // already in RAM
            QuantizedVectorStorageRead::ScalarMmap(q) => q.storage().populate(),
            QuantizedVectorStorageRead::PQMmap(q) => q.storage().populate(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.storage().populate(),
            QuantizedVectorStorageRead::TQMmap(q) => q.storage().populate(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            QuantizedVectorStorageRead::PQMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
            QuantizedVectorStorageRead::TQMmapMulti(q) => {
                q.storage().storage().populate();
                q.offsets_storage().populate()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            QuantizedVectorStorageRead::ScalarRam(_)
            | QuantizedVectorStorageRead::PQRam(_)
            | QuantizedVectorStorageRead::BinaryRam(_)
            | QuantizedVectorStorageRead::TQRam(_)
            | QuantizedVectorStorageRead::ScalarRamMulti(_)
            | QuantizedVectorStorageRead::PQRamMulti(_)
            | QuantizedVectorStorageRead::BinaryRamMulti(_)
            | QuantizedVectorStorageRead::TQRamMulti(_) => {} // nothing to clear
            QuantizedVectorStorageRead::ScalarMmap(q) => q.storage().clear_cache(),
            QuantizedVectorStorageRead::PQMmap(q) => q.storage().clear_cache(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.storage().clear_cache(),
            QuantizedVectorStorageRead::TQMmap(q) => q.storage().clear_cache(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorageRead::PQMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorageRead::TQMmapMulti(q) => {
                q.storage().storage().clear_cache();
                q.offsets_storage().clear_cache()?;
            }
        }
        Ok(())
    }

    pub fn flusher(&self) -> MmapFlusher {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => q.flusher(),
            QuantizedVectorStorageRead::ScalarMmap(q) => q.flusher(),
            QuantizedVectorStorageRead::PQRam(q) => q.flusher(),
            QuantizedVectorStorageRead::PQMmap(q) => q.flusher(),
            QuantizedVectorStorageRead::BinaryRam(q) => q.flusher(),
            QuantizedVectorStorageRead::BinaryMmap(q) => q.flusher(),
            QuantizedVectorStorageRead::TQRam(q) => q.flusher(),
            QuantizedVectorStorageRead::TQMmap(q) => q.flusher(),
            QuantizedVectorStorageRead::ScalarRamMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::PQRamMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::PQMmapMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::BinaryRamMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::TQRamMulti(q) => q.flusher(),
            QuantizedVectorStorageRead::TQMmapMulti(q) => q.flusher(),
        }
    }
}

impl<S: UniversalRead> QuantizedScorerDispatch for QuantizedVectorStorageRead<S> {
    fn build_metric_scorer<'a, TElement, TMetric>(
        &'a self,
        builder: QuantizedScorerBuilder<'a>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        match self {
            QuantizedVectorStorageRead::ScalarRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::ScalarMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::PQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::PQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::BinaryRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::BinaryMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::TQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::TQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorageRead::ScalarRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::PQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::PQMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::BinaryRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::TQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorageRead::TQMmapMulti(q) => {
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
            QuantizedVectorStorageRead::ScalarRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::ScalarMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::PQRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::PQMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::BinaryRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::BinaryMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::TQRam(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::TQMmap(q) => {
                internal_raw_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::ScalarRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::ScalarMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::PQRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::PQMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::BinaryRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::BinaryMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::TQRamMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
            QuantizedVectorStorageRead::TQMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q, hardware_counter)
            }
        }
    }
}
