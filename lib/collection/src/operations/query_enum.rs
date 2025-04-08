use std::fmt::Debug;
use std::iter;

use segment::data_types::vectors::{DenseVector, Named, NamedQuery, VectorInternal};
use segment::types::{StrictModeConfig, VectorName};
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::SparseVector;

use super::types::{CollectionError, CollectionResult};

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

impl QueryEnum {
    /// Iterate over all vectors in the query.
    fn vectors(&self) -> Box<dyn Iterator<Item = &VectorInternal> + '_> {
        match self {
            QueryEnum::Nearest(named_query) => Box::new(iter::once(&named_query.query)),
            QueryEnum::RecommendBestScore(named_query) => Box::new(named_query.query.flat_iter()),
            QueryEnum::RecommendSumScores(named_query) => Box::new(named_query.query.flat_iter()),
            QueryEnum::Discover(named_query) => Box::new(named_query.query.flat_iter()),
            QueryEnum::Context(named_query) => Box::new(named_query.query.flat_iter()),
        }
    }

    pub fn check_strict_mode(&self, strict_mode_config: &StrictModeConfig) -> CollectionResult<()> {
        if !strict_mode_config.is_enabled() {
            return Ok(());
        }

        let max_sparse_len = strict_mode_config
            .sparse_config
            .as_ref()
            .and_then(|config| config.config.get(self.get_vector_name()))
            .and_then(|sparse_config| sparse_config.max_length);
        let max_multivec_size = strict_mode_config
            .multivector_config
            .as_ref()
            .and_then(|config| config.config.get(self.get_vector_name()))
            .and_then(|multivec_config| multivec_config.max_vectors);

        let mut num_query_vectors = 0;
        for vector in self.vectors() {
            // check individual size
            vector.check_strict_mode(max_sparse_len, max_multivec_size)?;

            // aggregate num vectors
            num_query_vectors += vector.num_vectors();
        }

        // Check total number of vectors
        if let Some(max_query_vectors) = strict_mode_config.max_query_vectors {
            if num_query_vectors > max_query_vectors {
                return Err(CollectionError::strict_mode(
                    format!("Query exceeds maximum number of vectors ({max_query_vectors})"),
                    "Use less vectors in the query and/or make multivector(s) shorter",
                ));
            }
        }
        Ok(())
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

impl AsRef<QueryEnum> for QueryEnum {
    fn as_ref(&self) -> &QueryEnum {
        self
    }
}
