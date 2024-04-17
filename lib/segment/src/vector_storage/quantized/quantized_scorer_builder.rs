use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use quantization::EncodedVectors;

use super::quantized_custom_query_scorer::QuantizedCustomQueryScorer;
use super::quantized_query_scorer::QuantizedQueryScorer;
use super::quantized_vectors::QuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{DenseVector, QueryVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;
use crate::vector_storage::query::context_query::ContextQuery;
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query::reco_query::RecoQuery;
use crate::vector_storage::query::TransformInto;
use crate::vector_storage::{raw_scorer_from_query_scorer, RawScorer};

pub(super) struct QuantizedScorerBuilder<'a> {
    quantized_storage: &'a QuantizedVectorStorage,
    query: QueryVector,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
    distance: &'a Distance,
}

impl<'a> QuantizedScorerBuilder<'a> {
    pub fn new(
        quantized_storage: &'a QuantizedVectorStorage,
        query: QueryVector,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
        distance: &'a Distance,
    ) -> Self {
        Self {
            quantized_storage,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance,
        }
    }

    pub fn build(self) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self.distance {
            Distance::Cosine => self.build_with_metric::<CosineMetric>(),
            Distance::Euclid => self.build_with_metric::<EuclidMetric>(),
            Distance::Dot => self.build_with_metric::<DotProductMetric>(),
            Distance::Manhattan => self.build_with_metric::<ManhattanMetric>(),
        }
    }

    pub fn build_with_metric<TMetric: Metric<VectorElementType> + 'a>(
        self,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self.quantized_storage {
            QuantizedVectorStorage::ScalarRam(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
            QuantizedVectorStorage::ScalarMmap(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQRam(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
            QuantizedVectorStorage::PQMmap(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryRam(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
            QuantizedVectorStorage::BinaryMmap(storage) => {
                self.new_quantized_scorer::<TMetric, _>(storage)
            }
        }
    }

    #[inline]
    fn new_quantized_scorer<TMetric: Metric<VectorElementType> + 'a, TEncodedQuery: 'a>(
        self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let Self {
            quantized_storage: _same_as_quantized_storage_in_args,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance: _,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = QuantizedQueryScorer::<TMetric, _, _>::new(
                    vector.try_into()?,
                    quantized_storage,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Recommend(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TMetric, _, _, _, _>::new(
                    reco_query,
                    quantized_storage,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Discovery(discovery_query) => {
                let discovery_query: DiscoveryQuery<DenseVector> =
                    discovery_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TMetric, _, _, _, _>::new(
                    discovery_query,
                    quantized_storage,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TMetric, _, _, _, _>::new(
                    context_query,
                    quantized_storage,
                );
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
        }
    }
}
