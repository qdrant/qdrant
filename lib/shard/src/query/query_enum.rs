use segment::data_types::vectors::*;
use segment::types::VectorName;
use segment::vector_storage::query::*;
use serde::Serialize;
use sparse::common::sparse_vector::SparseVector;

/// Every kind of vector query that can be performed on segment level.
#[derive(Clone, Debug, PartialEq, Hash, Serialize)]
pub enum QueryEnum {
    Nearest(NamedQuery<VectorInternal>),
    RecommendBestScore(NamedQuery<RecoQuery<VectorInternal>>),
    RecommendSumScores(NamedQuery<RecoQuery<VectorInternal>>),
    Discover(NamedQuery<DiscoverQuery<VectorInternal>>),
    Context(NamedQuery<ContextQuery<VectorInternal>>),
    FeedbackNaive(NamedQuery<NaiveFeedbackQuery<VectorInternal>>),
}

impl QueryEnum {
    pub fn get_vector_name(&self) -> &VectorName {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
            QueryEnum::RecommendSumScores(reco_query) => reco_query.get_name(),
            QueryEnum::Discover(discover_query) => discover_query.get_name(),
            QueryEnum::Context(context_query) => context_query.get_name(),
            QueryEnum::FeedbackNaive(feedback_query) => feedback_query.get_name(),
        }
    }

    /// Only when the distance is the scoring, this will return true.
    pub fn is_distance_scored(&self) -> bool {
        match self {
            QueryEnum::Nearest(_) => true,
            QueryEnum::RecommendBestScore(_)
            | QueryEnum::RecommendSumScores(_)
            | QueryEnum::Discover(_)
            | QueryEnum::Context(_)
            | QueryEnum::FeedbackNaive(_) => false,
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
            QueryEnum::Discover(discover_query) => {
                let name = discover_query.get_name();
                for vector in discover_query.query.flat_iter() {
                    match vector {
                        VectorInternal::Sparse(sparse_vector) => f(name, sparse_vector),
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::Context(context_query) => {
                let name = context_query.get_name();
                for vector in context_query.query.flat_iter() {
                    match vector {
                        VectorInternal::Sparse(sparse_vector) => f(name, sparse_vector),
                        VectorInternal::Dense(_) | VectorInternal::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::FeedbackNaive(feedback_query) => {
                let name = feedback_query.get_name();
                for vector in feedback_query.query.flat_iter() {
                    match vector {
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
            QueryEnum::FeedbackNaive(named_query) => search_cost(named_query.query.flat_iter()),
        }
    }
}

fn search_cost<'a>(vectors: impl IntoIterator<Item = &'a VectorInternal>) -> usize {
    vectors
        .into_iter()
        .map(VectorInternal::similarity_cost)
        .sum()
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

impl From<NamedQuery<DiscoverQuery<VectorInternal>>> for QueryEnum {
    fn from(query: NamedQuery<DiscoverQuery<VectorInternal>>) -> Self {
        QueryEnum::Discover(query)
    }
}

impl From<QueryEnum> for QueryVector {
    fn from(query: QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(named) => QueryVector::Nearest(named.query),
            QueryEnum::RecommendBestScore(named) => QueryVector::RecommendBestScore(named.query),
            QueryEnum::RecommendSumScores(named) => QueryVector::RecommendSumScores(named.query),
            QueryEnum::Discover(named) => QueryVector::Discover(named.query),
            QueryEnum::Context(named) => QueryVector::Context(named.query),
            QueryEnum::FeedbackNaive(named) => QueryVector::FeedbackNaive(named.query),
        }
    }
}
