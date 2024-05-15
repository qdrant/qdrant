//! ## Universal query request types
//!
//! Provides a common interface for querying points in a collection
//!
//! Pipeline of type conversion:
//!
//! 1. `rest::QueryRequest`, `grpc::QueryPoints`: rest or grpc request. Used in API
//! 2. `CollectionQueryRequest`: Direct representation of the API request, but to be used as a single type. Created at API to enter ToC.
//! 3. `ShardQueryRequest`: same as the common request, but all point ids have been substituted with vectors. Created at Collection
//! 4. `QueryShardPoints`: to be used in the internal service. Created for RemoteShard, converts to and from ShardQueryRequest
//! 5. `PlannedQuery`: an easier-to-execute representation. Created in LocalShard

// TODO(universal-query): Create `CollectionQueryRequest` struct

pub mod shard_query {
    use api::grpc::qdrant as grpc;
    use common::types::ScoreType;
    use itertools::Itertools;
    use segment::data_types::vectors::{
        NamedQuery, NamedVectorStruct, Vector, DEFAULT_VECTOR_NAME,
    };
    use segment::types::{Filter, ScoredPoint, SearchParams, WithPayloadInterface, WithVector};
    use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
    use tonic::Status;

    use crate::operations::query_enum::QueryEnum;

    /// Internal response type for a universal query request.
    ///
    /// Capable of returning multiple intermediate results if needed, like the case of RRF (Reciprocal Rank Fusion)
    pub type ShardQueryResponse = Vec<Vec<ScoredPoint>>;

    #[derive(Debug, Clone)]
    pub enum ScoringQuery {
        /// Score points against some vector(s)
        Vector(QueryEnum),
        // TODO(universal-query): Add fusion and order-by
    }

    impl ScoringQuery {
        /// Get the vector name if it is scored against a vector
        pub fn get_vector_name(&self) -> Option<&str> {
            match self {
                ScoringQuery::Vector(query) => Some(query.get_vector_name()),
            }
        }
    }

    #[derive(Clone)]
    pub struct ShardPrefetch {
        pub prefetches: Vec<ShardPrefetch>,
        pub query: ScoringQuery,
        pub limit: usize,
        pub params: Option<SearchParams>,
        pub filter: Option<Filter>,
        pub score_threshold: Option<ScoreType>,
    }

    /// Internal representation of a universal query request.
    ///
    /// Direct translation of the user-facing request, but with all point ids substituted with their corresponding vectors.
    #[derive(Clone)]
    pub struct ShardQueryRequest {
        pub prefetches: Vec<ShardPrefetch>,
        pub query: ScoringQuery,
        pub filter: Option<Filter>,
        pub score_threshold: Option<ScoreType>,
        pub limit: usize,
        pub offset: usize,
        pub params: Option<SearchParams>,
        pub with_vector: WithVector,
        pub with_payload: WithPayloadInterface,
    }

    impl TryFrom<grpc::query_shard_points::Prefetch> for ShardPrefetch {
        type Error = Status;

        fn try_from(value: grpc::query_shard_points::Prefetch) -> Result<Self, Self::Error> {
            let grpc::query_shard_points::Prefetch {
                prefetch,
                query,
                limit,
                params,
                filter,
                score_threshold,
                using,
            } = value;

            let shard_prefetch = Self {
                prefetches: prefetch
                    .into_iter()
                    .map(ShardPrefetch::try_from)
                    .try_collect()?,
                query: query
                    .map(|query| ScoringQuery::try_from_grpc_query(query, using))
                    .transpose()?
                    .ok_or_else(|| Status::invalid_argument("missing field: query"))?,
                limit: limit as usize,
                params: params.map(SearchParams::from),
                filter: filter.map(Filter::try_from).transpose()?,
                score_threshold,
            };

            Ok(shard_prefetch)
        }
    }

    impl QueryEnum {
        fn try_from_grpc_raw_query(
            raw_query: grpc::RawQuery,
            using: Option<String>,
        ) -> Result<Self, Status> {
            use grpc::raw_query::Variant;

            let variant = raw_query
                .variant
                .ok_or_else(|| Status::invalid_argument("missing field: variant"))?;

            let query_enum = match variant {
                Variant::Nearest(nearest) => {
                    let vector = Vector::try_from(nearest)?;
                    let name = match (using, &vector) {
                        (None, Vector::Sparse(_)) => {
                            return Err(Status::invalid_argument("Sparse vector must have a name"))
                        }
                        (
                            Some(name),
                            Vector::MultiDense(_) | Vector::Sparse(_) | Vector::Dense(_),
                        ) => name,
                        (None, Vector::MultiDense(_) | Vector::Dense(_)) => {
                            DEFAULT_VECTOR_NAME.to_string()
                        }
                    };
                    let named_vector = NamedVectorStruct::new_from_vector(vector, name);
                    QueryEnum::Nearest(named_vector)
                }
                Variant::RecommendBestScore(recommend) => QueryEnum::RecommendBestScore(
                    NamedQuery::new(RecoQuery::try_from(recommend)?, using),
                ),
                Variant::Discover(discovery) => QueryEnum::Discover(NamedQuery {
                    query: DiscoveryQuery::try_from(discovery)?,
                    using,
                }),
                Variant::Context(context) => QueryEnum::Context(NamedQuery {
                    query: ContextQuery::try_from(context)?,
                    using,
                }),
            };

            Ok(query_enum)
        }
    }

    impl ScoringQuery {
        fn try_from_grpc_query(
            query: grpc::query_shard_points::Query,
            using: Option<String>,
        ) -> Result<Self, Status> {
            let score = query
                .score
                .ok_or_else(|| Status::invalid_argument("missing field: score"))?;
            let scoring_query = match score {
                grpc::query_shard_points::query::Score::Vector(query) => {
                    ScoringQuery::Vector(QueryEnum::try_from_grpc_raw_query(query, using)?)
                }
            };

            Ok(scoring_query)
        }
    }

    impl TryFrom<grpc::QueryShardPoints> for ShardQueryRequest {
        type Error = Status;

        fn try_from(value: grpc::QueryShardPoints) -> Result<Self, Self::Error> {
            let grpc::QueryShardPoints {
                prefetch,
                query,
                using,
                filter,
                limit,
                params,
                score_threshold,
                offset,
                with_payload,
                with_vectors,
            } = value;

            let request = Self {
                prefetches: prefetch
                    .into_iter()
                    .map(ShardPrefetch::try_from)
                    .try_collect()?,
                query: query
                    .map(|query| ScoringQuery::try_from_grpc_query(query, using))
                    .transpose()?
                    .ok_or_else(|| Status::invalid_argument("missing field: query"))?,
                filter: filter.map(Filter::try_from).transpose()?,
                score_threshold,
                limit: limit as usize,
                offset: offset as usize,
                params: params.map(SearchParams::from),
                with_vector: with_vectors
                    .map(WithVector::from)
                    .unwrap_or(WithVector::Bool(false)),
                with_payload: with_payload
                    .map(WithPayloadInterface::try_from)
                    .transpose()?
                    .unwrap_or(WithPayloadInterface::Bool(true)),
            };

            Ok(request)
        }
    }

    impl From<QueryEnum> for grpc::RawQuery {
        fn from(value: QueryEnum) -> Self {
            use api::grpc::qdrant::raw_query::Variant;

            let variant = match value {
                QueryEnum::Nearest(named) => {
                    Variant::Nearest(grpc::RawVector::from(named.to_vector()))
                }
                QueryEnum::RecommendBestScore(named) => {
                    Variant::RecommendBestScore(grpc::raw_query::Recommend::from(named.query))
                }
                QueryEnum::Discover(named) => {
                    Variant::Discover(grpc::raw_query::Discovery::from(named.query))
                }
                QueryEnum::Context(named) => {
                    Variant::Context(grpc::raw_query::Context::from(named.query))
                }
            };

            Self {
                variant: Some(variant),
            }
        }
    }

    impl From<ScoringQuery> for grpc::query_shard_points::Query {
        fn from(value: ScoringQuery) -> Self {
            use grpc::query_shard_points::query::Score;

            match value {
                ScoringQuery::Vector(query) => Self {
                    score: Some(Score::Vector(grpc::RawQuery::from(query))),
                },
            }
        }
    }

    impl From<ShardPrefetch> for grpc::query_shard_points::Prefetch {
        fn from(value: ShardPrefetch) -> Self {
            let ShardPrefetch {
                prefetches,
                query,
                limit,
                params,
                filter,
                score_threshold,
            } = value;
            Self {
                prefetch: prefetches.into_iter().map(Self::from).collect(),
                using: query.get_vector_name().map(ToOwned::to_owned),
                query: Some(grpc::query_shard_points::Query::from(query)),
                filter: filter.map(grpc::Filter::from),
                params: params.map(grpc::SearchParams::from),
                score_threshold,
                limit: limit as u64,
            }
        }
    }

    impl From<ShardQueryRequest> for grpc::QueryShardPoints {
        fn from(value: ShardQueryRequest) -> Self {
            let ShardQueryRequest {
                prefetches,
                query,
                filter,
                score_threshold,
                limit,
                offset,
                params,
                with_vector,
                with_payload,
            } = value;

            Self {
                prefetch: prefetches
                    .into_iter()
                    .map(grpc::query_shard_points::Prefetch::from)
                    .collect(),
                using: query.get_vector_name().map(ToOwned::to_owned),
                query: Some(grpc::query_shard_points::Query::from(query)),
                filter: filter.map(grpc::Filter::from),
                params: params.map(grpc::SearchParams::from),
                score_threshold,
                limit: limit as u64,
                offset: offset as u64,
                with_payload: Some(grpc::WithPayloadSelector::from(with_payload)),
                with_vectors: Some(grpc::WithVectorsSelector::from(with_vector)),
            }
        }
    }
}

pub mod planned_query {
    //! Types used within `LocalShard` to represent a planned `ShardQueryRequest`

    use std::sync::Arc;

    use segment::types::{WithPayloadInterface, WithVector};

    use super::shard_query::{ScoringQuery, ShardPrefetch, ShardQueryRequest};
    use crate::operations::types::{
        CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    };

    pub struct PrefetchMerge {
        /// Alter the scores before selecting the best limit
        pub rescore: Option<ScoringQuery>,

        /// Keep this much points from the top
        pub limit: usize,
    }

    pub enum PrefetchSource {
        /// A reference offset into the main search batch
        BatchIdx(usize),

        /// A nested prefetch
        Prefetch(PrefetchPlan),
    }

    pub struct PrefetchPlan {
        /// Gather all these sources
        pub sources: Vec<PrefetchSource>,

        /// How to merge the sources
        pub merge: PrefetchMerge,
    }

    pub struct PlannedQuery {
        pub merge_plan: PrefetchPlan,
        pub batch: Arc<CoreSearchRequestBatch>,
        pub offset: usize,
        pub with_vector: WithVector,
        pub with_payload: WithPayloadInterface,
    }

    // TODO: Maybe just return a CoreSearchRequest if there is no prefetch?
    impl TryFrom<ShardQueryRequest> for PlannedQuery {
        type Error = CollectionError;

        fn try_from(request: ShardQueryRequest) -> CollectionResult<Self> {
            let ShardQueryRequest {
                query,
                filter,
                score_threshold,
                limit,
                offset: req_offset,
                with_vector,
                with_payload,
                prefetches: prefetch,
                params,
            } = request;

            let mut core_searches = Vec::new();
            let sources;
            let rescore;
            let offset;

            if !prefetch.is_empty() {
                sources = recurse_prefetches(&mut core_searches, prefetch);
                rescore = Some(query);
                offset = req_offset;
            } else {
                #[allow(clippy::infallible_destructuring_match)]
                // TODO: remove once there are more variants
                let query = match query {
                    ScoringQuery::Vector(query) => query,
                    // TODO: return error for fusion queries without prefetch
                };
                let core_search = CoreSearchRequest {
                    query,
                    filter,
                    score_threshold,
                    with_vector: None,
                    with_payload: None,
                    offset: req_offset,
                    params,
                    limit,
                };
                core_searches.push(core_search);

                sources = vec![PrefetchSource::BatchIdx(0)];
                rescore = None;
                offset = 0;
            }

            Ok(Self {
                merge_plan: PrefetchPlan {
                    sources,
                    merge: PrefetchMerge { rescore, limit },
                },
                batch: Arc::new(CoreSearchRequestBatch {
                    searches: core_searches,
                }),
                offset,
                with_vector,
                with_payload,
            })
        }
    }

    fn recurse_prefetches(
        core_searches: &mut Vec<CoreSearchRequest>,
        prefetches: Vec<ShardPrefetch>,
    ) -> Vec<PrefetchSource> {
        let mut sources = Vec::with_capacity(prefetches.len());

        for prefetch in prefetches {
            let ShardPrefetch {
                prefetches,
                query,
                limit,
                params,
                filter,
                score_threshold,
            } = prefetch;

            let source = if prefetches.is_empty() {
                match query {
                    ScoringQuery::Vector(query_enum) => {
                        let core_search = CoreSearchRequest {
                            query: query_enum,
                            filter,
                            params,
                            limit,
                            offset: 0,
                            with_payload: None,
                            with_vector: None,
                            score_threshold,
                        };

                        let idx = core_searches.len();
                        core_searches.push(core_search);

                        PrefetchSource::BatchIdx(idx)
                    }
                }
            } else {
                let sources = recurse_prefetches(core_searches, prefetches);

                let prefetch_plan = PrefetchPlan {
                    sources,
                    merge: PrefetchMerge {
                        rescore: Some(query),
                        limit,
                    },
                };
                PrefetchSource::Prefetch(prefetch_plan)
            };
            sources.push(source);
        }

        sources
    }
}
