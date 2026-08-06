use common::types::ScoreType;
#[cfg(feature = "api")]
use itertools::Itertools as _;
use segment::data_types::load_profile::LoadProfile;
#[cfg(feature = "api")]
use segment::data_types::vectors::NamedQuery;
use segment::data_types::vectors::QueryVector;
use segment::types::{
    Filter, SearchParams, VectorName, WithPayload, WithPayloadInterface, WithVector,
};
#[cfg(feature = "api")]
use segment::{data_types::vectors::VectorInternal, vector_storage::query::ContextPair};

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
    /// Request-specific [`LoadProfile`] for opening a read-only shard to serve exactly
    /// this search: only the queried vector's components and the filter's field indexes
    /// keep their configured placement.
    pub fn load_profile(&self) -> LoadProfile {
        let Self {
            query,
            filter,
            params: _,
            limit: _,
            offset: _,
            with_payload,
            with_vector: _,
            score_threshold: _,
        } = self;

        // The `with_payload` default of a search is `false`.
        let with_payload = with_payload.as_ref().is_some_and(|wp| wp.is_required());

        LoadProfile::for_search(query.get_vector_name(), filter.as_ref(), with_payload)
    }

    pub fn search_rate_cost(&self) -> usize {
        let mut cost = self.query.search_cost();

        if let Some(filter) = &self.filter {
            cost += filter.total_conditions_count();
        }

        cost
    }
}

#[cfg(feature = "api")]
impl From<api::rest::SearchRequestInternal> for CoreSearchRequest {
    fn from(request: api::rest::SearchRequestInternal) -> Self {
        #[cfg(feature = "api")]
        use segment::data_types::vectors::NamedVectorStruct;

        let api::rest::SearchRequestInternal {
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

#[cfg(feature = "api")]
impl TryFrom<api::grpc::qdrant::CoreSearchPoints> for CoreSearchRequest {
    type Error = tonic::Status;

    fn try_from(value: api::grpc::qdrant::CoreSearchPoints) -> Result<Self, Self::Error> {
        use segment::data_types::vectors::VectorInternal;
        use segment::vector_storage::query::{ContextQuery, DiscoverQuery, RecoQuery};

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
                            query: DiscoverQuery::new(target.try_into()?, pairs),
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
            params: value.params.map(TryInto::try_into).transpose()?,
            limit: value.limit as usize,
            offset: value.offset.unwrap_or_default() as usize,
            with_payload: value.with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(value.with_vectors.map(Into::into).unwrap_or_default()),
            score_threshold: value.score_threshold,
        })
    }
}

#[cfg(feature = "api")]
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

#[cfg(feature = "api")]
impl TryFrom<api::grpc::qdrant::SearchPoints> for CoreSearchRequest {
    type Error = tonic::Status;

    fn try_from(value: api::grpc::qdrant::SearchPoints) -> Result<Self, Self::Error> {
        use sparse::common::sparse_vector::validate_sparse_vector_impl;

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
            params: params.map(SearchParams::try_from).transpose()?,
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

/// Which scoring query a search runs. Only searches of the same type can share one batched
/// segment call, because a segment scores a whole batch with a single query implementation.
#[derive(PartialEq, Debug)]
pub enum SearchType {
    Nearest,
    RecommendBestScore,
    RecommendSumScores,
    Discover,
    Context,
    FeedbackNaive,
}

impl From<&QueryEnum> for SearchType {
    fn from(query: &QueryEnum) -> Self {
        match query {
            QueryEnum::Nearest(_) => Self::Nearest,
            QueryEnum::RecommendBestScore(_) => Self::RecommendBestScore,
            QueryEnum::RecommendSumScores(_) => Self::RecommendSumScores,
            QueryEnum::Discover(_) => Self::Discover,
            QueryEnum::Context(_) => Self::Context,
            QueryEnum::FeedbackNaive(_) => Self::FeedbackNaive,
        }
    }
}

/// Everything a segment search takes apart from the query vector itself, i.e. exactly the
/// arguments of [`ReadSegmentEntry::search_batch`] that are shared by a batch.
///
/// [`ReadSegmentEntry::search_batch`]: segment::entry::ReadSegmentEntry::search_batch
#[derive(PartialEq, Debug)]
pub struct BatchSearchParams<'a> {
    pub search_type: SearchType,
    pub vector_name: &'a VectorName,
    pub filter: Option<&'a Filter>,
    pub with_payload: WithPayload,
    pub with_vector: WithVector,
    pub top: usize,
    pub params: Option<&'a SearchParams>,
}

impl<'a> From<&'a CoreSearchRequest> for BatchSearchParams<'a> {
    fn from(request: &'a CoreSearchRequest) -> Self {
        let CoreSearchRequest {
            query,
            filter,
            params,
            limit,
            offset,
            with_payload,
            with_vector,
            score_threshold: _, // applied to the merged result, not by the segment
        } = request;

        Self {
            search_type: SearchType::from(query),
            vector_name: query.get_vector_name(),
            filter: filter.as_ref(),
            with_payload: WithPayload::from(
                with_payload
                    .as_ref()
                    .unwrap_or(&WithPayloadInterface::Bool(false)),
            ),
            with_vector: with_vector.clone().unwrap_or_default(),
            top: limit + offset,
            params: params.as_ref(),
        }
    }
}

/// A run of search requests that a segment can serve with one
/// [`search_batch`](segment::entry::ReadSegmentEntry::search_batch) call: they agree on every
/// parameter and differ only in their query vector.
#[derive(Debug)]
pub struct SearchBatchGroup<'a> {
    pub params: BatchSearchParams<'a>,
    pub query_vectors: Vec<QueryVector>,
}

/// Split a batch of search requests into groups that can each be pushed down to a segment as a
/// single batched search, so per-query work that does not depend on the query vector — resolving
/// the vector index, building the filtered id context — is paid once per group instead of once
/// per request.
///
/// Only *consecutive* requests are grouped, so the requests keep their input order: concatenating
/// the per-group results in group order yields one result list per input request, in input order.
///
/// The grouping depends on the requests alone, so a caller searching several segments computes it
/// once and reuses it for every segment.
pub fn group_search_batches(searches: &[CoreSearchRequest]) -> Vec<SearchBatchGroup<'_>> {
    let mut groups: Vec<SearchBatchGroup> = Vec::with_capacity(searches.len());

    for search in searches {
        let params = BatchSearchParams::from(search);
        let query_vector = QueryVector::from(search.query.clone());

        // Comparing params is expensive on large filters, but far cheaper than re-running a
        // segment search that could have shared one.
        match groups.last_mut() {
            Some(last) if last.params == params => last.query_vectors.push(query_vector),
            Some(_) | None => groups.push(SearchBatchGroup {
                params,
                query_vectors: vec![query_vector],
            }),
        }
    }

    groups
}

#[cfg(test)]
mod tests {
    use ahash::AHashSet;
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::types::{Condition, HasIdCondition};

    use super::*;

    fn nearest(vector: Vec<f32>, limit: usize) -> CoreSearchRequest {
        CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(vector),
                "vector".to_string(),
            )),
            filter: None,
            params: None,
            limit,
            offset: 0,
            with_payload: None,
            with_vector: None,
            score_threshold: None,
        }
    }

    fn group_sizes(searches: &[CoreSearchRequest]) -> Vec<usize> {
        group_search_batches(searches)
            .iter()
            .map(|group| group.query_vectors.len())
            .collect()
    }

    #[test]
    fn requests_differing_only_by_vector_form_one_group() {
        let searches = vec![
            nearest(vec![1.0], 3),
            nearest(vec![2.0], 3),
            nearest(vec![3.0], 3),
        ];

        assert_eq!(group_sizes(&searches), vec![3]);
    }

    #[test]
    fn differing_params_split_groups() {
        let with_filter = |mut search: CoreSearchRequest| {
            search.filter = Some(Filter::new_must(Condition::HasId(HasIdCondition::from(
                AHashSet::from_iter([1.into()]),
            ))));
            search
        };
        let with_offset = |mut search: CoreSearchRequest| {
            // Offsets are served by raising the segment-level top, so they split too.
            search.offset = 1;
            search
        };
        let with_payload = |mut search: CoreSearchRequest| {
            search.with_payload = Some(WithPayloadInterface::Bool(true));
            search
        };

        let searches = vec![
            nearest(vec![1.0], 3),
            nearest(vec![1.0], 4),
            with_filter(nearest(vec![1.0], 4)),
            with_offset(nearest(vec![1.0], 4)),
            with_payload(nearest(vec![1.0], 4)),
        ];

        assert_eq!(group_sizes(&searches), vec![1, 1, 1, 1, 1]);
    }

    /// Only consecutive requests are grouped, so results can be concatenated back in input order.
    #[test]
    fn identical_params_are_not_grouped_across_a_different_request() {
        let searches = vec![
            nearest(vec![1.0], 3),
            nearest(vec![2.0], 5),
            nearest(vec![3.0], 3),
        ];

        assert_eq!(group_sizes(&searches), vec![1, 1, 1]);
    }

    #[test]
    fn empty_batch_has_no_groups() {
        assert!(group_search_batches(&[]).is_empty());
    }
}
