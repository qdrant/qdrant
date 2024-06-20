use segment::data_types::vectors::{DenseVector, Named, NamedQuery, NamedVectorStruct, Vector};
use segment::types::Order;
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::SparseVector;

use super::types::CollectionResult;
use crate::config::CollectionParams;

impl QueryEnum {
    pub fn get_vector_name(&self) -> &str {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
            QueryEnum::Discover(discovery_query) => discovery_query.get_name(),
            QueryEnum::Context(context_query) => context_query.get_name(),
        }
    }

    /// Only when the distance is the scoring, this will return true.
    pub fn is_distance_scored(&self) -> bool {
        match self {
            QueryEnum::Nearest(_) => true,
            QueryEnum::RecommendBestScore(_) | QueryEnum::Discover(_) | QueryEnum::Context(_) => {
                false
            }
        }
    }

    pub fn query_order(&self, collection_params: &CollectionParams) -> CollectionResult<Order> {
        let order = if self.is_distance_scored() {
            collection_params
                .get_distance(self.get_vector_name())?
                .distance_order()
        } else {
            // Score comes from special handling of the distances in a way that it doesn't
            // directly represent distance anymore, so the order is always `LargeBetter`
            Order::LargeBetter
        };

        Ok(order)
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

impl QueryEnum {}

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
