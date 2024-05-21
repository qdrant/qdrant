use segment::data_types::vectors::{DenseVector, Named, NamedQuery, NamedVectorStruct, Vector};
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::SparseVector;

impl QueryEnum {
    pub fn get_vector_name(&self) -> &str {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
            QueryEnum::Discover(discovery_query) => discovery_query.get_name(),
            QueryEnum::Context(context_query) => context_query.get_name(),
        }
    }

    /// Only when the distance is the scoring, this will return false.
    pub fn has_custom_scoring(&self) -> bool {
        match self {
            QueryEnum::Nearest(_) => false,
            QueryEnum::RecommendBestScore(_) | QueryEnum::Discover(_) | QueryEnum::Context(_) => {
                true
            }
        }
    }

    pub fn iterate_sparse(&self, mut f: impl FnMut(&str, &SparseVector)) {
        match self {
            QueryEnum::Nearest(vector) => match vector {
                NamedVectorStruct::Sparse(named_sparse_vector) => {
                    f(&named_sparse_vector.name, &named_sparse_vector.vector)
                }
                NamedVectorStruct::Default(_)
                | NamedVectorStruct::Dense(_)
                | NamedVectorStruct::MultiDense(_) => {}
            },
            QueryEnum::RecommendBestScore(reco_query) => {
                let name = reco_query.get_name();
                for vector in reco_query.query.flat_iter() {
                    match vector {
                        Vector::Sparse(sparse_vector) => f(name, sparse_vector),
                        Vector::Dense(_) | Vector::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::Discover(discovery_query) => {
                let name = discovery_query.get_name();
                for pair in discovery_query.query.flat_iter() {
                    match pair {
                        Vector::Sparse(sparse_vector) => f(name, sparse_vector),
                        Vector::Dense(_) | Vector::MultiDense(_) => {}
                    }
                }
            }
            QueryEnum::Context(context_query) => {
                let name = context_query.get_name();
                for pair in context_query.query.flat_iter() {
                    match pair {
                        Vector::Sparse(sparse_vector) => f(name, sparse_vector),
                        Vector::Dense(_) | Vector::MultiDense(_) => {}
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryEnum {
    Nearest(NamedVectorStruct),
    RecommendBestScore(NamedQuery<RecoQuery<Vector>>),
    Discover(NamedQuery<DiscoveryQuery<Vector>>),
    Context(NamedQuery<ContextQuery<Vector>>),
}

impl From<DenseVector> for QueryEnum {
    fn from(vector: DenseVector) -> Self {
        QueryEnum::Nearest(NamedVectorStruct::Default(vector))
    }
}

impl From<NamedQuery<DiscoveryQuery<Vector>>> for QueryEnum {
    fn from(query: NamedQuery<DiscoveryQuery<Vector>>) -> Self {
        QueryEnum::Discover(query)
    }
}

impl AsRef<QueryEnum> for QueryEnum {
    fn as_ref(&self) -> &QueryEnum {
        self
    }
}
