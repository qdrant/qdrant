use segment::data_types::vectors::{DenseVector, Named, NamedQuery, QueryVector, VectorInternal};
use segment::types::VectorName;
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use serde::Serialize;
use sparse::common::sparse_vector::SparseVector;

/// Every kind of vector query that can be performed on segment level.
#[derive(Clone, Debug, PartialEq, Hash, Serialize)]
pub enum QueryEnum {
    Nearest(NamedQuery<VectorInternal>),
    RecommendBestScore(NamedQuery<RecoQuery<VectorInternal>>),
    RecommendSumScores(NamedQuery<RecoQuery<VectorInternal>>),
    Discover(NamedQuery<DiscoveryQuery<VectorInternal>>),
    Context(NamedQuery<ContextQuery<VectorInternal>>),
}

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

    /// Returns the estimated cost of using this query in terms of number of vectors.
    /// The cost approximates how many similarity comparisons this query will make against one point.
    pub fn search_cost(&self) -> usize {
        match self {
            QueryEnum::Nearest(named_query) => search_cost([&named_query.query]),
            QueryEnum::RecommendBestScore(named_query) => {
                search_cost(named_query.query.flat_iter())
            }
            QueryEnum::RecommendSumScores(named_query) => {
                search_cost(named_query.query.flat_iter())
            }
            QueryEnum::Discover(named_query) => search_cost(named_query.query.flat_iter()),
            QueryEnum::Context(named_query) => search_cost(named_query.query.flat_iter()),
        }
    }
}

impl AsRef<QueryEnum> for QueryEnum {
    fn as_ref(&self) -> &QueryEnum {
        self
    }
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

impl From<QueryEnum> for QueryVector {
    fn from(query: QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(named) => QueryVector::Nearest(named.query),
            QueryEnum::RecommendBestScore(named) => QueryVector::RecommendBestScore(named.query),
            QueryEnum::RecommendSumScores(named) => QueryVector::RecommendSumScores(named.query),
            QueryEnum::Discover(named) => QueryVector::Discovery(named.query),
            QueryEnum::Context(named) => QueryVector::Context(named.query),
        }
    }
}

impl From<QueryEnum> for api::grpc::qdrant::QueryEnum {
    fn from(value: QueryEnum) -> Self {
        match value {
            QueryEnum::Nearest(vector) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::NearestNeighbors(
                    api::grpc::qdrant::Vector::from(vector.query),
                )),
            },
            QueryEnum::RecommendBestScore(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::RecommendBestScore(
                    named.query.into(),
                )),
            },
            QueryEnum::RecommendSumScores(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::RecommendSumScores(
                    named.query.into(),
                )),
            },
            QueryEnum::Discover(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::Discover(
                    api::grpc::qdrant::DiscoveryQuery {
                        target: Some(named.query.target.into()),
                        context: named
                            .query
                            .pairs
                            .into_iter()
                            .map(|pair| api::grpc::qdrant::ContextPair {
                                positive: { Some(pair.positive.into()) },
                                negative: { Some(pair.negative.into()) },
                            })
                            .collect(),
                    },
                )),
            },
            QueryEnum::Context(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::Context(
                    api::grpc::qdrant::ContextQuery {
                        context: named
                            .query
                            .pairs
                            .into_iter()
                            .map(|pair| api::grpc::qdrant::ContextPair {
                                positive: { Some(pair.positive.into()) },
                                negative: { Some(pair.negative.into()) },
                            })
                            .collect(),
                    },
                )),
            },
        }
    }
}

fn search_cost<'a>(vectors: impl IntoIterator<Item = &'a VectorInternal>) -> usize {
    vectors
        .into_iter()
        .map(VectorInternal::similarity_cost)
        .sum()
}
