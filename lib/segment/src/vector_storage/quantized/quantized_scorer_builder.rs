use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use quantization::EncodedVectors;

use super::quantized_custom_query_scorer::QuantizedCustomQueryScorer;
use super::quantized_query_scorer::QuantizedQueryScorer;
use super::quantized_vectors::QuantizedVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{QueryVector, VectorType};
use crate::types::Distance;
use crate::vector_storage::query::context_query::ContextQuery;
use crate::vector_storage::query::discovery_query::DiscoveryQuery;
use crate::vector_storage::query::reco_query::RecoQuery;
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
        match self.quantized_storage {
            QuantizedVectorStorage::ScalarRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::ScalarMmap(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::PQRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::PQMmap(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::BinaryRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::BinaryMmap(storage) => self.new_quantized_scorer(storage),
        }
    }

    #[inline]
    fn new_quantized_scorer<TEncodedQuery: 'a>(
        self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let Self {
            quantized_storage: _same_as_quantized_storage_in_args,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            distance,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer =
                    QuantizedQueryScorer::new(vector.into(), quantized_storage, *distance);
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Recommend(reco_query) => {
                let reco_query: RecoQuery<VectorType> = reco_query.into();
                let query_scorer =
                    QuantizedCustomQueryScorer::new(reco_query, quantized_storage, *distance);
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Discovery(discovery_query) => {
                let discovery_query: DiscoveryQuery<VectorType> = discovery_query.into();
                let query_scorer =
                    QuantizedCustomQueryScorer::new(discovery_query, quantized_storage, *distance);
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<VectorType> = context_query.into();
                let query_scorer =
                    QuantizedCustomQueryScorer::new(context_query, quantized_storage, *distance);
                raw_scorer_from_query_scorer(query_scorer, point_deleted, vec_deleted, is_stopped)
            }
        }
    }
}
