use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use quantization::EncodedVectors;

use super::quantized_custom_query_scorer::QuantizedCustomQueryScorer;
use super::quantized_query_scorer::QuantizedQueryScorer;
use super::quantized_vectors::QuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVector, QueryVector, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};
use crate::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery, TransformInto};
use crate::vector_storage::{raw_scorer_from_query_scorer, RawScorer};

pub(super) struct QuantizedScorerBuilder<'a> {
    quantized_storage: &'a QuantizedVectorStorage,
    quantization_config: &'a QuantizationConfig,
    query: QueryVector,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
    distance: &'a Distance,
    datatype: VectorStorageDatatype,
}

impl<'a> QuantizedScorerBuilder<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        quantized_storage: &'a QuantizedVectorStorage,
        quantization_config: &'a QuantizationConfig,
        query: QueryVector,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
        distance: &'a Distance,
        datatype: VectorStorageDatatype,
    ) -> Self {
        Self {
            quantized_storage,
            quantization_config,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance,
            datatype,
        }
    }

    pub fn build(self) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self.datatype {
            VectorStorageDatatype::Float32 => match self.distance {
                Distance::Cosine => self.build_with_metric::<VectorElementType, CosineMetric>(),
                Distance::Euclid => self.build_with_metric::<VectorElementType, EuclidMetric>(),
                Distance::Dot => self.build_with_metric::<VectorElementType, DotProductMetric>(),
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementType, ManhattanMetric>()
                }
            },
            VectorStorageDatatype::Uint8 => match self.distance {
                Distance::Cosine => self.build_with_metric::<VectorElementTypeByte, CosineMetric>(),
                Distance::Euclid => self.build_with_metric::<VectorElementTypeByte, EuclidMetric>(),
                Distance::Dot => {
                    self.build_with_metric::<VectorElementTypeByte, DotProductMetric>()
                }
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementTypeByte, ManhattanMetric>()
                }
            },
            VectorStorageDatatype::Float16 => match self.distance {
                Distance::Cosine => self.build_with_metric::<VectorElementTypeHalf, CosineMetric>(),
                Distance::Euclid => self.build_with_metric::<VectorElementTypeHalf, EuclidMetric>(),
                Distance::Dot => {
                    self.build_with_metric::<VectorElementTypeHalf, DotProductMetric>()
                }
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementTypeHalf, ManhattanMetric>()
                }
            },
        }
    }

    pub fn build_with_metric<TElement, TMetric>(self) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        match self.quantized_storage {
            QuantizedVectorStorage::ScalarRam(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::ScalarMmap(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQRam(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQMmap(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryRam(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryMmap(storage) => {
                self.new_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::ScalarRamMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQRamMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryRamMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                self.new_multi_quantized_scorer::<TElement, TMetric, _>(storage)
            }
        }
    }

    fn new_quantized_scorer<TElement, TMetric, TEncodedQuery>(
        self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
        TEncodedQuery: 'a,
    {
        let Self {
            quantized_storage: _same_as_quantized_storage_in_args,
            quantization_config,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance: _,
            datatype: _,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = QuantizedQueryScorer::<TElement, TMetric, _, _>::new(
                    vector.try_into()?,
                    quantized_storage,
                    quantization_config,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Recommend(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new(
                    reco_query,
                    quantized_storage,
                    quantization_config,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Discovery(discovery_query) => {
                let discovery_query: DiscoveryQuery<DenseVector> =
                    discovery_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new(
                    discovery_query,
                    quantized_storage,
                    quantization_config,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new(
                    context_query,
                    quantized_storage,
                    quantization_config,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
        }
    }

    fn new_multi_quantized_scorer<TElement, TMetric, TEncodedQuery>(
        self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
        TEncodedQuery: 'a,
    {
        let Self {
            quantized_storage: _same_as_quantized_storage_in_args,
            quantization_config,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance: _,
            datatype: _,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = QuantizedQueryScorer::<TElement, TMetric, _, _>::new_multi(
                    vector.try_into()?,
                    quantized_storage,
                    quantization_config,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Recommend(reco_query) => {
                let reco_query: RecoQuery<MultiDenseVector> = reco_query.transform_into()?;
                let query_scorer =
                    QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new_multi(
                        reco_query,
                        quantized_storage,
                        quantization_config,
                    );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Discovery(discovery_query) => {
                let discovery_query: DiscoveryQuery<MultiDenseVector> =
                    discovery_query.transform_into()?;
                let query_scorer =
                    QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new_multi(
                        discovery_query,
                        quantized_storage,
                        quantization_config,
                    );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<MultiDenseVector> =
                    context_query.transform_into()?;
                let query_scorer =
                    QuantizedCustomQueryScorer::<TElement, TMetric, _, _, _>::new_multi(
                        context_query,
                        quantized_storage,
                        quantization_config,
                    );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
        }
    }
}
