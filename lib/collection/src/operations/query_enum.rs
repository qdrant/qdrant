use std::fmt::Debug;

use segment::data_types::vectors::{DenseVector, Named, NamedQuery, VectorInternal};
use segment::types::VectorName;
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::SparseVector;

impl QueryEnum {
    pub fn get_vector_name(&self) -> &VectorName {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
            QueryEnum::RecommendSumScores(reco_query) => reco_query.get_name(),
            QueryEnum::Discover(discovery_query) => discovery_query.get_name(),
            QueryEnum::Context(context_query) => context_query.get_name(),
        }
    }

    /// Only when the distance is the scoring, this will return true.
    pub fn is_distance_scored(&self) -> bool {
        match self {
            QueryEnum::Nearest(_) => true,
            QueryEnum::RecommendBestScore(_)
            | QueryEnum::RecommendSumScores(_)
            | QueryEnum::Discover(_)
            | QueryEnum::Context(_) => false,
        }
    }

    pub fn iterate_sparse(&self, mut f: impl FnMut(&VectorName, &SparseVector)) {
        match self {
            QueryEnum::Nearest(named) => match &named.query {
                VectorInternal::Sparse(sparse_vector) => f(named.get_name(), sparse_vector),
                VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
            },
            QueryEnum::RecommendBestScore(reco_query)
            | QueryEnum::RecommendSumScores(reco_query) => {
                let name = reco_query.get_name();
                for vector in reco_query.query.flat_iter() {
                    match vector {
                        VectorInternal::Sparse(sparse_vector) => f(name, sparse_vector),
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::Discover(discovery_query) => {
                let name = discovery_query.get_name();
                for pair in discovery_query.query.flat_iter() {
                    match pair {
                        VectorInternal::Sparse(sparse_vector) => f(name, sparse_vector),
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::Context(context_query) => {
                let name = context_query.get_name();
                for pair in context_query.query.flat_iter() {
                    match pair {
                        VectorInternal::Sparse(sparse_vector) => f(name, sparse_vector),
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryEnum {
    Nearest(NamedQuery<VectorInternal>),
    RecommendBestScore(NamedQuery<RecoQuery<VectorInternal>>),
    RecommendSumScores(NamedQuery<RecoQuery<VectorInternal>>),
    Discover(NamedQuery<DiscoveryQuery<VectorInternal>>),
    Context(NamedQuery<ContextQuery<VectorInternal>>),
}

impl From<DenseVector> for QueryEnum {
    fn from(vector: DenseVector) -> Self {
        QueryEnum::Nearest(NamedQuery {
            query: VectorInternal::Dense(vector),
            using: None,
        })
    }
}

impl From<NamedQuery<DiscoveryQuery<VectorInternal>>> for QueryEnum {
    fn from(query: NamedQuery<DiscoveryQuery<VectorInternal>>) -> Self {
        QueryEnum::Discover(query)
    }
}

impl AsRef<QueryEnum> for QueryEnum {
    fn as_ref(&self) -> &QueryEnum {
        self
    }
}
