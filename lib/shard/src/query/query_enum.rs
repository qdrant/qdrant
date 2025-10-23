use api::grpc;
use segment::data_types::vectors::*;
use segment::types::{VectorName, VectorNameBuf};
use segment::vector_storage::query::*;
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
    FeedbackSimple(NamedQuery<FeedbackQueryInternal<VectorInternal, SimpleFeedbackStrategy>>),
}

impl QueryEnum {
    pub fn get_vector_name(&self) -> &VectorName {
        match self {
            QueryEnum::Nearest(vector) => vector.get_name(),
            QueryEnum::RecommendBestScore(reco_query) => reco_query.get_name(),
            QueryEnum::RecommendSumScores(reco_query) => reco_query.get_name(),
            QueryEnum::Discover(discovery_query) => discovery_query.get_name(),
            QueryEnum::Context(context_query) => context_query.get_name(),
            QueryEnum::FeedbackSimple(feedback_query) => feedback_query.get_name(),
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
            | QueryEnum::FeedbackSimple(_) => false,
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
                for vector in discovery_query.query.flat_iter() {
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
            QueryEnum::FeedbackSimple(feedback_query) => {
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
            QueryEnum::FeedbackSimple(named_query) => search_cost(named_query.query.flat_iter()),
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
            QueryEnum::FeedbackSimple(named) => QueryVector::FeedbackSimple(named.query),
        }
    }
}

impl From<QueryEnum> for grpc::QueryEnum {
    fn from(value: QueryEnum) -> Self {
        match value {
            QueryEnum::Nearest(vector) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::NearestNeighbors(
                    grpc::Vector::from(vector.query),
                )),
            },
            QueryEnum::RecommendBestScore(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::RecommendBestScore(
                    named.query.into(),
                )),
            },
            QueryEnum::RecommendSumScores(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::RecommendSumScores(
                    named.query.into(),
                )),
            },
            QueryEnum::Discover(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::Discover(grpc::DiscoveryQuery {
                    target: Some(named.query.target.into()),
                    context: named
                        .query
                        .pairs
                        .into_iter()
                        .map(|pair| grpc::ContextPair {
                            positive: Some(pair.positive.into()),
                            negative: Some(pair.negative.into()),
                        })
                        .collect(),
                })),
            },
            QueryEnum::Context(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::Context(grpc::ContextQuery {
                    context: named
                        .query
                        .pairs
                        .into_iter()
                        .map(|pair| grpc::ContextPair {
                            positive: Some(pair.positive.into()),
                            negative: Some(pair.negative.into()),
                        })
                        .collect(),
                })),
            },
            QueryEnum::FeedbackSimple(_named) => {
                // This conversion only happens for search/recommend/discover dedicated endpoints. Feedback does not have one.
                unimplemented!("there is no specialized feedback endpoint")
            }
        }
    }
}

impl QueryEnum {
    pub fn from_grpc_raw_query(
        raw_query: grpc::RawQuery,
        using: Option<VectorNameBuf>,
    ) -> Result<QueryEnum, tonic::Status> {
        use grpc::raw_query::Variant;

        let variant = raw_query
            .variant
            .ok_or_else(|| tonic::Status::invalid_argument("missing field: variant"))?;

        let query_enum = match variant {
            Variant::Nearest(nearest) => {
                let vector = VectorInternal::try_from(nearest)?;
                let name = match (using, &vector) {
                    (None, VectorInternal::Sparse(_)) => {
                        return Err(tonic::Status::invalid_argument(
                            "Sparse vector must have a name",
                        ));
                    }

                    (
                        Some(name),
                        VectorInternal::MultiDense(_)
                        | VectorInternal::Sparse(_)
                        | VectorInternal::Dense(_),
                    ) => name,

                    (None, VectorInternal::MultiDense(_) | VectorInternal::Dense(_)) => {
                        DEFAULT_VECTOR_NAME.to_owned()
                    }
                };

                let named_vector = NamedQuery::new(vector, name);
                QueryEnum::Nearest(named_vector)
            }
            Variant::RecommendBestScore(recommend) => QueryEnum::RecommendBestScore(NamedQuery {
                query: RecoQuery::try_from(recommend)?,
                using,
            }),
            Variant::RecommendSumScores(recommend) => QueryEnum::RecommendSumScores(NamedQuery {
                query: RecoQuery::try_from(recommend)?,
                using,
            }),
            Variant::Discover(discovery) => QueryEnum::Discover(NamedQuery {
                query: DiscoveryQuery::try_from(discovery)?,
                using,
            }),
            Variant::Context(context) => QueryEnum::Context(NamedQuery {
                query: ContextQuery::try_from(context)?,
                using,
            }),
            Variant::Feedback(grpc::raw_query::Feedback {
                target,
                feedback,
                strategy,
            }) => {
                let strategy = strategy
                    .and_then(|strategy| strategy.variant)
                    .ok_or_else(|| {
                        tonic::Status::invalid_argument("feedback strategy is required")
                    })?;

                let target = VectorInternal::try_from(
                    target.ok_or_else(|| tonic::Status::invalid_argument("No target provided"))?,
                )?;
                let feedback = feedback
                    .into_iter()
                    .map(<FeedbackItem<VectorInternal>>::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                match strategy {
                    grpc::feedback_strategy::Variant::Simple(strategy) => {
                        let feedback_query = FeedbackQueryInternal {
                            target,
                            feedback,
                            strategy: SimpleFeedbackStrategy::from(strategy),
                        };
                        let named = NamedQuery {
                            query: feedback_query,
                            using,
                        };
                        QueryEnum::FeedbackSimple(named)
                    }
                }
            }
        };

        Ok(query_enum)
    }
}

impl From<QueryEnum> for grpc::RawQuery {
    fn from(query: QueryEnum) -> Self {
        use grpc::raw_query::Variant;

        let variant = match query {
            QueryEnum::Nearest(named) => Variant::Nearest(grpc::RawVector::from(named.query)),
            QueryEnum::RecommendBestScore(named) => {
                Variant::RecommendBestScore(grpc::raw_query::Recommend::from(named.query))
            }
            QueryEnum::RecommendSumScores(named) => {
                Variant::RecommendSumScores(grpc::raw_query::Recommend::from(named.query))
            }
            QueryEnum::Discover(named) => {
                Variant::Discover(grpc::raw_query::Discovery::from(named.query))
            }
            QueryEnum::Context(named) => {
                Variant::Context(grpc::raw_query::Context::from(named.query))
            }
            QueryEnum::FeedbackSimple(named) => {
                Variant::Feedback(grpc::raw_query::Feedback::from(named.query))
            }
        };

        grpc::RawQuery {
            variant: Some(variant),
        }
    }
}
