use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use quantization::EncodedVectors;

use super::quantized_query_scorer::QuantizedQueryScorer;
use super::quantized_reco_query_scorer::QuantizedRecoQueryScorer;
use super::quantized_vectors::QuantizedVectorStorage;
use crate::data_types::vectors::{QueryVector, VectorType};
use crate::types::Distance;
use crate::vector_storage::query::RecoQuery;
use crate::vector_storage::{raw_scorer_from_query_scorer, RawScorer};

pub(super) struct QuantizedRawScorerBuilder<'a> {
    quantized_storage: &'a QuantizedVectorStorage,
    query: Option<QueryVector>,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
    distance: &'a Distance,
}

impl<'a> QuantizedRawScorerBuilder<'a> {
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
            query: Some(query),
            point_deleted,
            vec_deleted,
            is_stopped,
            distance,
        }
    }

    pub fn build(self) -> Box<dyn RawScorer + 'a> {
        match self.quantized_storage {
            QuantizedVectorStorage::ScalarRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::ScalarMmap(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::PQRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::PQMmap(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::BinaryRam(storage) => self.new_quantized_scorer(storage),
            QuantizedVectorStorage::BinaryMmap(storage) => self.new_quantized_scorer(storage),
        }
    }

    fn new_quantized_scorer<TEncodedQuery: 'a>(
        mut self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> Box<dyn RawScorer + 'a> {
        match self.query.take().unwrap() {
            QueryVector::Nearest(vector) => self.quantized_metric_scorer(vector, quantized_storage),
            QueryVector::Recommend(reco_query) => {
                self.quantized_reco_scorer(reco_query, quantized_storage)
            }
        }
    }

    fn quantized_reco_scorer<TEncodedQuery: 'a>(
        &self,
        query: RecoQuery<Vec<f32>>,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> Box<dyn RawScorer + 'a> {
        let query_scorer = QuantizedRecoQueryScorer::new(query, quantized_storage, *self.distance);
        raw_scorer_from_query_scorer(
            query_scorer,
            self.point_deleted,
            self.vec_deleted,
            self.is_stopped,
        )
    }

    fn quantized_metric_scorer<TEncodedQuery: 'a>(
        &self,
        query: VectorType,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> Box<dyn RawScorer + 'a> {
        let query_scorer = QuantizedQueryScorer::new(query, quantized_storage, *self.distance);
        raw_scorer_from_query_scorer(
            query_scorer,
            self.point_deleted,
            self.vec_deleted,
            self.is_stopped,
        )
    }
}
