use api::rest::SearchRequestInternal;
use common::types::ScoreType;
use itertools::Itertools as _;
use segment::data_types::vectors::{NamedQuery, NamedVectorStruct, VectorInternal};
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::validate_sparse_vector_impl;

use crate::query::query_enum::QueryEnum;

/// DEPRECATED: Search method should be removed and replaced with `ShardQueryRequest`
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

#[derive(Debug, Clone)]
pub struct CoreSearchRequestBatch {
    pub searches: Vec<CoreSearchRequest>,
}
