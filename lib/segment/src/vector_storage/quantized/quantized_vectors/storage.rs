use std::fmt;

use common::types::PointOffsetType;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;
use quantization::{EncodedVectors, EncodedVectorsPQ};

use super::ReadFile;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::spaces::metric::Metric;
use crate::vector_storage::RawScorer;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorage;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunked, MultivectorOffsetsStorageMmap, MultivectorOffsetsStorageRam,
    QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_query_scorer::InternalScorerUnsupported;
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_scorer_builder::{
    QuantizedScorerBuilder, QuantizedScorerDispatch, internal_raw_multi_scorer, internal_raw_scorer,
};
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

type ScalarRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type ScalarMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedStorage<ReadFile>>,
    MultivectorOffsetsStorageMmap,
>;

type ScalarChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsU8<QuantizedChunkedStorage>,
    MultivectorOffsetsStorageChunked,
>;

type PQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type PQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedStorage<ReadFile>>,
    MultivectorOffsetsStorageMmap,
>;

type PQChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsPQ<QuantizedChunkedStorage>,
    MultivectorOffsetsStorageChunked,
>;

type BinaryRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type BinaryMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedStorage<ReadFile>>,
    MultivectorOffsetsStorageMmap,
>;

type BinaryChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsBin<u8, QuantizedChunkedStorage>,
    MultivectorOffsetsStorageChunked,
>;

type TQRamMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedRamStorage>,
    MultivectorOffsetsStorageRam,
>;
type TQMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedStorage<ReadFile>>,
    MultivectorOffsetsStorageMmap,
>;

type TQChunkedMmapMulti = QuantizedMultivectorStorage<
    EncodedVectorsTQ<QuantizedChunkedStorage>,
    MultivectorOffsetsStorageChunked,
>;

pub enum QuantizedVectorStorage {
    ScalarRam(EncodedVectorsU8<QuantizedRamStorage>),
    ScalarMmap(EncodedVectorsU8<QuantizedStorage<ReadFile>>),
    ScalarChunkedMmap(EncodedVectorsU8<QuantizedChunkedStorage>),
    PQRam(EncodedVectorsPQ<QuantizedRamStorage>),
    PQMmap(EncodedVectorsPQ<QuantizedStorage<ReadFile>>),
    PQChunkedMmap(EncodedVectorsPQ<QuantizedChunkedStorage>),
    BinaryRam(EncodedVectorsBin<u128, QuantizedRamStorage>),
    BinaryMmap(EncodedVectorsBin<u128, QuantizedStorage<ReadFile>>),
    BinaryChunkedMmap(EncodedVectorsBin<u128, QuantizedChunkedStorage>),
    TQRam(EncodedVectorsTQ<QuantizedRamStorage>),
    TQMmap(EncodedVectorsTQ<QuantizedStorage<ReadFile>>),
    TQChunkedMmap(EncodedVectorsTQ<QuantizedChunkedStorage>),
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
    pub fn is_cold(&self) -> bool {
        match self {
            QuantizedVectorStorage::ScalarRam(q) => q.is_cold(),
            QuantizedVectorStorage::ScalarMmap(q) => q.is_cold(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.is_cold(),
            QuantizedVectorStorage::PQRam(q) => q.is_cold(),
            QuantizedVectorStorage::PQMmap(q) => q.is_cold(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryRam(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryMmap(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.is_cold(),
            QuantizedVectorStorage::TQRam(q) => q.is_cold(),
            QuantizedVectorStorage::TQMmap(q) => q.is_cold(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.is_cold(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.is_cold(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::PQRamMulti(q) => q.is_cold(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::TQRamMulti(q) => q.is_cold(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.is_cold(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.is_cold(),
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
        f.debug_tuple("QuantizedVectorStorage")
            .finish_non_exhaustive()
    }
}

impl QuantizedScorerDispatch for QuantizedVectorStorage {
    fn build_metric_scorer<'a, TElement, TMetric>(
        &'a self,
        builder: QuantizedScorerBuilder<'a>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        match self {
            QuantizedVectorStorage::ScalarRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::ScalarMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::ScalarChunkedMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::PQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::PQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::PQChunkedMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::BinaryRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::BinaryMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::BinaryChunkedMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::TQRam(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::TQMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::TQChunkedMmap(q) => {
                builder.new_quantized_scorer::<TElement, TMetric>(q)
            }
            QuantizedVectorStorage::ScalarRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::ScalarMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::PQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::PQMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::BinaryRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::BinaryMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::TQRamMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::TQMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => {
                builder.new_multi_quantized_scorer::<TElement, TMetric, _, _>(q)
            }
        }
    }

    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported> {
        match self {
            QuantizedVectorStorage::ScalarRam(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::ScalarMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::PQRam(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::PQMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::PQChunkedMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::BinaryRam(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::BinaryMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::TQRam(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::TQMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::TQChunkedMmap(q) => internal_raw_scorer(point_id, q),
            QuantizedVectorStorage::ScalarRamMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::ScalarMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q)
            }
            QuantizedVectorStorage::PQRamMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::PQMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::BinaryRamMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::BinaryMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => {
                internal_raw_multi_scorer(point_id, q)
            }
            QuantizedVectorStorage::TQRamMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::TQMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => internal_raw_multi_scorer(point_id, q),
        }
    }
}
