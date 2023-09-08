use std::sync::atomic::AtomicBool;

use bitvec::slice::BitSlice;
use quantization::EncodedVectors;

use super::quantized_query_scorer::QuantizedQueryScorer;
use super::quantized_reco_query_scorer::QuantizedRecoQueryScorer;
use super::quantized_vectors::QuantizedVectorStorage;
use crate::data_types::vectors::QueryVector;
use crate::types::Distance;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::{raw_scorer_from_query_scorer, RawScorer};

pub(super) struct QuantizedScorerBuilder<'a> {
    quantized_storage: &'a QuantizedVectorStorage,
    query: Option<QueryVector>,
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

    #[inline]
    fn new_quantized_scorer<TEncodedQuery: 'a>(
        mut self,
        quantized_storage: &'a impl EncodedVectors<TEncodedQuery>,
    ) -> Box<dyn RawScorer + 'a> {
        match self
            .query
            .take()
            .expect("new() ensures to always have a value")
        {
            QueryVector::Nearest(vector) => {
                let query_scorer =
                    QuantizedQueryScorer::new(vector, quantized_storage, *self.distance);
                self.into_raw_scorer(query_scorer)
            }
            QueryVector::Recommend(reco_query) => {
                let query_scorer =
                    QuantizedRecoQueryScorer::new(reco_query, quantized_storage, *self.distance);
                self.into_raw_scorer(query_scorer)
            }
        }
    }

    #[inline]
    fn into_raw_scorer(self, query_scorer: impl QueryScorer + 'a) -> Box<dyn RawScorer + 'a> {
        raw_scorer_from_query_scorer(
            query_scorer,
            self.point_deleted,
            self.vec_deleted,
            self.is_stopped,
        )
    }
}
