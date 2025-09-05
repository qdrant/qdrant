use api::rest::SearchRequestInternal;
use common::types::ScoreType;
use itertools::Itertools as _;
use segment::data_types::vectors::{
    DenseVector, Named as _, NamedQuery, NamedVectorStruct, QueryVector, VectorInternal,
};
use segment::types::{Filter, SearchParams, VectorName, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::{SparseVector, validate_sparse_vector_impl};

#[derive(Clone, Debug, PartialEq)]
pub struct CoreSearchRequest {
    /// Every kind of query that can be performed on segment level
    pub query: QueryEnum,
    /// Look only for points which satisfies this conditions
    pub filter: Option<Filter>,
    /// Additional search params
    pub params: Option<SearchParams>,
    /// Max number of result to return
    pub limit: usize,
    /// Offset of the first result to return.
    /// May be used to paginate results.
    /// Note: large offset values may cause performance issues.
    pub offset: usize,
    /// Select which payload to return with the response. Default is false.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into response. Default is false.
    pub with_vector: Option<WithVector>,
    pub score_threshold: Option<ScoreType>,
}

impl CoreSearchRequest {
    pub fn search_rate_cost(&self) -> usize {
        let mut cost = self.query.search_cost();

        if let Some(filter) = &self.filter {
            cost += filter.total_conditions_count();
        }

        cost
    }
}

impl From<SearchRequestInternal> for CoreSearchRequest {
    fn from(request: SearchRequestInternal) -> Self {
        let SearchRequestInternal {
            vector,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = request;
        Self {
            query: QueryEnum::Nearest(NamedQuery::from(NamedVectorStruct::from(vector))),
            filter,
            params,
            limit,
            offset: offset.unwrap_or_default(),
            with_payload,
            with_vector,
            score_threshold,
        }
    }
}

impl TryFrom<api::grpc::qdrant::CoreSearchPoints> for CoreSearchRequest {
    type Error = tonic::Status;

    fn try_from(value: api::grpc::qdrant::CoreSearchPoints) -> Result<Self, Self::Error> {
        let query = value
            .query
            .and_then(|query| query.query)
            .map(|query| {
                Ok(match query {
                    api::grpc::qdrant::query_enum::Query::NearestNeighbors(vector) => {
                        let vector_internal = VectorInternal::try_from(vector)?;
                        QueryEnum::Nearest(NamedQuery::from(
                            api::grpc::conversions::into_named_vector_struct(
                                value.vector_name,
                                vector_internal,
                            )?,
                        ))
                    }
                    api::grpc::qdrant::query_enum::Query::RecommendBestScore(query) => {
                        QueryEnum::RecommendBestScore(NamedQuery {
                            query: RecoQuery::try_from(query)?,
                            using: value.vector_name,
                        })
                    }
                    api::grpc::qdrant::query_enum::Query::RecommendSumScores(query) => {
                        QueryEnum::RecommendSumScores(NamedQuery {
                            query: RecoQuery::try_from(query)?,
                            using: value.vector_name,
                        })
                    }
                    api::grpc::qdrant::query_enum::Query::Discover(query) => {
                        let Some(target) = query.target else {
                            return Err(tonic::Status::invalid_argument("Target is not specified"));
                        };

                        let pairs = query
                            .context
                            .into_iter()
                            .map(try_context_pair_from_grpc)
                            .try_collect()?;

                        QueryEnum::Discover(NamedQuery {
                            query: DiscoveryQuery::new(target.try_into()?, pairs),
                            using: value.vector_name,
                        })
                    }
                    api::grpc::qdrant::query_enum::Query::Context(query) => {
                        let pairs = query
                            .context
                            .into_iter()
                            .map(try_context_pair_from_grpc)
                            .try_collect()?;

                        QueryEnum::Context(NamedQuery {
                            query: ContextQuery::new(pairs),
                            using: value.vector_name,
                        })
                    }
                })
            })
            .transpose()?
            .ok_or_else(|| tonic::Status::invalid_argument("Query is not specified"))?;

        Ok(Self {
            query,
            filter: value.filter.map(|f| f.try_into()).transpose()?,
            params: value.params.map(|p| p.into()),
            limit: value.limit as usize,
            offset: value.offset.unwrap_or_default() as usize,
            with_payload: value.with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(
                value
                    .with_vectors
                    .map(|with_vectors| with_vectors.into())
                    .unwrap_or_default(),
            ),
            score_threshold: value.score_threshold,
        })
    }
}

fn try_context_pair_from_grpc(
    pair: api::grpc::qdrant::ContextPair,
) -> Result<ContextPair<VectorInternal>, tonic::Status> {
    let api::grpc::qdrant::ContextPair { positive, negative } = pair;
    match (positive, negative) {
        (Some(positive), Some(negative)) => Ok(ContextPair {
            positive: positive.try_into()?,
            negative: negative.try_into()?,
        }),
        _ => Err(tonic::Status::invalid_argument(
            "All context pairs must have both positive and negative parts",
        )),
    }
}

impl TryFrom<api::grpc::qdrant::SearchPoints> for CoreSearchRequest {
    type Error = tonic::Status;

    fn try_from(value: api::grpc::qdrant::SearchPoints) -> Result<Self, Self::Error> {
        let api::grpc::qdrant::SearchPoints {
            collection_name: _,
            vector,
            filter,
            limit,
            with_payload,
            params,
            score_threshold,
            offset,
            vector_name,
            with_vectors,
            read_consistency: _,
            timeout: _,
            shard_key_selector: _,
            sparse_indices,
        } = value;

        if let Some(sparse_indices) = &sparse_indices {
            let api::grpc::qdrant::SparseIndices { data } = sparse_indices;
            validate_sparse_vector_impl(data, &vector).map_err(|e| {
                tonic::Status::invalid_argument(format!(
                    "Sparse indices does not match sparse vector conditions: {e}"
                ))
            })?;
        }

        let vector_internal =
            VectorInternal::from_vector_and_indices(vector, sparse_indices.map(|v| v.data));

        let vector_struct =
            api::grpc::conversions::into_named_vector_struct(vector_name, vector_internal)?;

        Ok(Self {
            query: QueryEnum::Nearest(NamedQuery::from(vector_struct)),
            filter: filter.map(Filter::try_from).transpose()?,
            params: params.map(SearchParams::from),
            limit: limit as usize,
            offset: offset.map(|v| v as usize).unwrap_or_default(),
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?,
            with_vector: with_vectors.map(WithVector::from),
            score_threshold: score_threshold.map(|s| s as ScoreType),
        })
    }
}

/// Every kind of vector query that can be performed on segment level.
#[derive(Clone, Debug, PartialEq)]
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
    fn search_cost(&self) -> usize {
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
