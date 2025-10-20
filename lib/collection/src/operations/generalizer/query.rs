use segment::data_types::vectors::{MultiDenseVectorInternal, NamedQuery, VectorInternal};
use segment::vector_storage::query::{
    ContextPair, ContextQuery, DiscoveryQuery, FeedbackItem, FeedbackQueryInternal, RecoQuery,
    SimpleFeedbackStrategy,
};
use shard::query::query_enum::QueryEnum;
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::DimId;

use crate::operations::generalizer::Generalizer;
use crate::operations::universal_query::collection_query::VectorInputInternal;
use crate::operations::universal_query::shard_query::{
    MmrInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest,
};

impl Generalizer for Vec<ShardQueryRequest> {
    fn remove_details(&self) -> Self {
        self.iter().map(|req| req.remove_details()).collect()
    }
}

impl Generalizer for ShardQueryRequest {
    fn remove_details(&self) -> Self {
        let ShardQueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = self;

        ShardQueryRequest {
            prefetches: prefetches.iter().map(|p| p.remove_details()).collect(),
            query: query.as_ref().map(|q| q.remove_details()),
            filter: filter.clone(),
            score_threshold: *score_threshold,
            limit: *limit,
            offset: *offset,
            params: *params,
            with_vector: with_vector.clone(),
            with_payload: with_payload.clone(),
        }
    }
}

impl Generalizer for ShardPrefetch {
    fn remove_details(&self) -> Self {
        let ShardPrefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = self;

        Self {
            prefetches: prefetches.iter().map(|p| p.remove_details()).collect(),
            query: query.as_ref().map(|q| q.remove_details()),
            filter: filter.clone(),
            score_threshold: *score_threshold,
            limit: *limit,
            params: *params,
        }
    }
}

impl Generalizer for ScoringQuery {
    fn remove_details(&self) -> Self {
        match self {
            ScoringQuery::Vector(vector) => ScoringQuery::Vector(vector.remove_details()),
            ScoringQuery::Fusion(_) => self.clone(),
            ScoringQuery::OrderBy(_) => self.clone(),
            ScoringQuery::Formula(_) => self.clone(),
            ScoringQuery::Sample(_) => self.clone(),
            ScoringQuery::Mmr(mmr) => ScoringQuery::Mmr(mmr.remove_details()),
        }
    }
}

impl Generalizer for MmrInternal {
    fn remove_details(&self) -> Self {
        let Self {
            vector,
            using,
            lambda,
            candidates_limit,
        } = self;

        Self {
            vector: vector.remove_details(),
            using: using.clone(),
            lambda: *lambda,
            candidates_limit: *candidates_limit,
        }
    }
}

impl Generalizer for QueryEnum {
    fn remove_details(&self) -> Self {
        match self {
            QueryEnum::Nearest(nearest) => QueryEnum::Nearest(nearest.remove_details()),
            QueryEnum::RecommendBestScore(recommend) => {
                QueryEnum::RecommendBestScore(recommend.remove_details())
            }
            QueryEnum::RecommendSumScores(recommend) => {
                QueryEnum::RecommendSumScores(recommend.remove_details())
            }
            QueryEnum::Discover(disocover) => QueryEnum::Discover(disocover.remove_details()),
            QueryEnum::Context(context) => QueryEnum::Context(context.remove_details()),
            QueryEnum::FeedbackSimple(feedback) => {
                QueryEnum::FeedbackSimple(feedback.remove_details())
            }
        }
    }
}

impl<T: Generalizer> Generalizer for NamedQuery<T> {
    fn remove_details(&self) -> Self {
        let NamedQuery { query, using } = self;
        Self {
            using: using.clone(),
            query: query.remove_details(),
        }
    }
}

impl Generalizer for VectorInputInternal {
    fn remove_details(&self) -> Self {
        match self {
            VectorInputInternal::Vector(vector) => {
                VectorInputInternal::Vector(vector.remove_details())
            }
            VectorInputInternal::Id(id) => VectorInputInternal::Id(*id),
        }
    }
}

impl Generalizer for VectorInternal {
    fn remove_details(&self) -> Self {
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

impl<T: Generalizer> Generalizer for DiscoveryQuery<T> {
    fn remove_details(&self) -> Self {
        let DiscoveryQuery { target, pairs } = self;
        Self {
            target: target.remove_details(),
            pairs: pairs.iter().map(|p| p.remove_details()).collect(),
        }
    }
}

impl<T: Generalizer> Generalizer for ContextQuery<T> {
    fn remove_details(&self) -> Self {
        let ContextQuery { pairs } = self;
        Self {
            pairs: pairs.iter().map(|p| p.remove_details()).collect(),
        }
    }
}

impl<T: Generalizer> Generalizer for ContextPair<T> {
    fn remove_details(&self) -> Self {
        let ContextPair { positive, negative } = self;
        Self {
            positive: positive.remove_details(),
            negative: negative.remove_details(),
        }
    }
}

impl<T: Generalizer> Generalizer for RecoQuery<T> {
    fn remove_details(&self) -> Self {
        let RecoQuery {
            positives,
            negatives,
        } = self;
        Self {
            positives: positives.iter().map(|p| p.remove_details()).collect(),
            negatives: negatives.iter().map(|p| p.remove_details()).collect(),
        }
    }
}

impl<T: Generalizer, TStrategy: Generalizer> Generalizer for FeedbackQueryInternal<T, TStrategy> {
    fn remove_details(&self) -> Self {
        let Self {
            target,
            feedback,
            strategy,
        } = self;
        Self {
            target: target.remove_details(),
            feedback: feedback.iter().map(|p| p.remove_details()).collect(),
            strategy: strategy.remove_details(),
        }
    }
}

impl<T: Generalizer> Generalizer for FeedbackItem<T> {
    fn remove_details(&self) -> Self {
        let FeedbackItem { vector, score: _ } = self;
        Self {
            vector: vector.remove_details(),
            score: 0.0.into(),
        }
    }
}

impl Generalizer for SimpleFeedbackStrategy {
    fn remove_details(&self) -> Self {
        let SimpleFeedbackStrategy { a: _, b: _, c: _ } = self;
        Self {
            a: 0.0.into(),
            b: 0.0.into(),
            c: 0.0.into(),
        }
    }
}
