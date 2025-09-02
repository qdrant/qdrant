use crate::operations::generalizer::Generalizer;
use crate::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryRequest, NearestWithMmr, Query, VectorInputInternal,
    VectorQuery,
};
use segment::data_types::vectors::{MultiDenseVectorInternal, VectorInternal};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;

impl Generalizer for CollectionQueryRequest {
    fn remove_vectors_and_payloads(&self) -> Self {
        let CollectionQueryRequest {
            prefetch,
            query,
            using,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
            lookup_from,
        } = self;

        Self {
            prefetch: prefetch
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
            query: query.as_ref().map(|q| q.remove_vectors_and_payloads()),
            using: using.clone(),
            filter: filter.clone(),
            score_threshold: *score_threshold,
            limit: *limit,
            offset: *offset,
            params: params.clone(),
            with_vector: with_vector.clone(),
            with_payload: with_payload.clone(),
            lookup_from: lookup_from.clone(),
        }
    }
}

impl Generalizer for CollectionPrefetch {
    fn remove_vectors_and_payloads(&self) -> Self {
        let CollectionPrefetch {
            prefetch,
            query,
            using,
            filter,
            score_threshold,
            limit,
            params,
            lookup_from,
        } = self;

        Self {
            prefetch: prefetch
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
            query: query.as_ref().map(|q| q.remove_vectors_and_payloads()),
            using: using.clone(),
            filter: filter.clone(),
            score_threshold: *score_threshold,
            limit: *limit,
            params: params.clone(),
            lookup_from: lookup_from.clone(),
        }
    }
}

impl Generalizer for Query {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            Query::Vector(vector) => Query::Vector(vector.remove_vectors_and_payloads()),
            Query::Fusion(_) => self.clone(),
            Query::OrderBy(_) => self.clone(),
            Query::Formula(_) => self.clone(),
            Query::Sample(_) => self.clone(),
        }
    }
}

impl Generalizer for VectorInputInternal {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorInputInternal::Vector(vector) => {
                VectorInputInternal::Vector(vector.remove_vectors_and_payloads())
            }
            VectorInputInternal::Id(id) => VectorInputInternal::Id(*id),
        }
    }
}

impl Generalizer for VectorInternal {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorInternal::Dense(dense) => VectorInternal::Dense(vec![dense.len() as f32]),
            VectorInternal::Sparse(sparse) => VectorInternal::Sparse(
                SparseVector::new(vec![sparse.len() as DimId], vec![0.0]).unwrap(),
            ),
            VectorInternal::MultiDense(multi) => {
                VectorInternal::MultiDense(MultiDenseVectorInternal::new(
                    vec![multi.num_vectors() as f32, multi.dim as f32],
                    2,
                ))
            }
        }
    }
}

impl Generalizer for VectorQuery<VectorInputInternal> {
    fn remove_vectors_and_payloads(&self) -> Self {
        match self {
            VectorQuery::Nearest(nearest) => {
                VectorQuery::Nearest(nearest.remove_vectors_and_payloads())
            }
            VectorQuery::NearestWithMmr(nearest_with_mmr) => {
                let NearestWithMmr { nearest, mmr } = nearest_with_mmr;

                VectorQuery::NearestWithMmr(NearestWithMmr {
                    nearest: nearest.remove_vectors_and_payloads(),
                    mmr: mmr.clone(),
                })
            }
            VectorQuery::RecommendAverageVector(recommend) => {
                VectorQuery::RecommendAverageVector(recommend.remove_vectors_and_payloads())
            }
            VectorQuery::RecommendBestScore(recommend) => {
                VectorQuery::RecommendBestScore(recommend.remove_vectors_and_payloads())
            }
            VectorQuery::RecommendSumScores(recommend) => {
                VectorQuery::RecommendSumScores(recommend.remove_vectors_and_payloads())
            }
            VectorQuery::Discover(discovery) => {
                VectorQuery::Discover(discovery.remove_vectors_and_payloads())
            }
            VectorQuery::Context(context) => {
                VectorQuery::Context(context.remove_vectors_and_payloads())
            }
        }
    }
}

impl Generalizer for ContextQuery<VectorInputInternal> {
    fn remove_vectors_and_payloads(&self) -> Self {
        let ContextQuery { pairs } = self;
        Self {
            pairs: pairs
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
        }
    }
}

impl Generalizer for ContextPair<VectorInputInternal> {
    fn remove_vectors_and_payloads(&self) -> Self {
        let ContextPair { positive, negative } = self;
        Self {
            positive: positive.remove_vectors_and_payloads(),
            negative: negative.remove_vectors_and_payloads(),
        }
    }
}

impl Generalizer for DiscoveryQuery<VectorInputInternal> {
    fn remove_vectors_and_payloads(&self) -> Self {
        let DiscoveryQuery { target, pairs } = self;
        let target = target.remove_vectors_and_payloads();

        Self {
            target,
            pairs: pairs
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
        }
    }
}

impl Generalizer for RecoQuery<VectorInputInternal> {
    fn remove_vectors_and_payloads(&self) -> Self {
        let RecoQuery {
            positives,
            negatives,
        } = self;
        Self {
            positives: positives
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
            negatives: negatives
                .iter()
                .map(|p| p.remove_vectors_and_payloads())
                .collect(),
        }
    }
}
