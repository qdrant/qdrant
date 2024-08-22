use std::collections::HashSet;

use api::rest::{LookupLocation, RecommendStrategy};
use common::types::ScoreType;
use itertools::Itertools;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{
    MultiDenseVectorInternal, NamedQuery, NamedVectorStruct, Vector, VectorRef, DEFAULT_VECTOR_NAME,
};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, SearchParams,
    WithPayloadInterface, WithVector,
};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};

use super::shard_query::{Fusion, Sample, ScoringQuery, ShardPrefetch, ShardQueryRequest};
use crate::common::fetch_vectors::ReferencedVectors;
use crate::lookup::WithLookup;
use crate::operations::query_enum::QueryEnum;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::recommendations::avg_vector_for_recommendation;

/// Internal representation of a query request, used to converge from REST and gRPC. This can have IDs referencing vectors.
#[derive(Clone, Debug, PartialEq)]
pub struct CollectionQueryRequest {
    pub prefetch: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    pub offset: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
    pub lookup_from: Option<LookupLocation>,
}

impl CollectionQueryRequest {
    pub const DEFAULT_LIMIT: usize = 10;

    pub const DEFAULT_GROUP_SIZE: usize = 3;

    pub const DEFAULT_OFFSET: usize = 0;

    pub const DEFAULT_WITH_VECTOR: WithVector = WithVector::Bool(false);

    pub const DEFAULT_WITH_PAYLOAD: WithPayloadInterface = WithPayloadInterface::Bool(false);
}

/// Lightweight representation of a query request to implement the [RetrieveRequest] trait.
#[derive(Debug)]
pub struct CollectionQueryResolveRequest<'a> {
    pub vector_query: &'a VectorQuery<VectorInput>,
    pub lookup_from: Option<LookupLocation>,
    pub using: String,
}

/// Internal representation of a group query request, used to converge from REST and gRPC.
#[derive(Debug)]
pub struct CollectionQueryGroupsRequest {
    pub prefetch: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub params: Option<SearchParams>,
    pub score_threshold: Option<ScoreType>,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
    pub lookup_from: Option<LookupLocation>,
    pub group_by: JsonPath,
    pub group_size: usize,
    pub limit: usize,
    pub with_lookup: Option<WithLookup>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Query {
    /// Score points against some vector(s)
    Vector(VectorQuery<VectorInput>),

    /// Reciprocal rank fusion
    Fusion(Fusion),

    /// Order by a payload field
    OrderBy(OrderBy),

    /// Sample points
    Sample(Sample),
}

impl Query {
    pub fn try_into_scoring_query(
        self,
        ids_to_vectors: &ReferencedVectors,
        lookup_vector_name: &str,
        lookup_collection: Option<&String>,
        using: String,
    ) -> CollectionResult<ScoringQuery> {
        let scoring_query = match self {
            Query::Vector(vector_query) => {
                let query_enum = vector_query
                    // Homogenize the input into raw vectors
                    .ids_into_vectors(ids_to_vectors, lookup_vector_name, lookup_collection)
                    // Turn into QueryEnum
                    .into_query_enum(using)?;
                ScoringQuery::Vector(query_enum)
            }
            Query::Fusion(fusion) => ScoringQuery::Fusion(fusion),
            Query::OrderBy(order_by) => ScoringQuery::OrderBy(order_by),
            Query::Sample(sample) => ScoringQuery::Sample(sample),
        };

        Ok(scoring_query)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum VectorInput {
    Id(PointIdType),
    Vector(Vector),
}

impl VectorInput {
    pub fn as_id(&self) -> Option<&PointIdType> {
        match self {
            VectorInput::Id(id) => Some(id),
            VectorInput::Vector(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum VectorQuery<T> {
    Nearest(T),
    RecommendAverageVector(RecoQuery<T>),
    RecommendBestScore(RecoQuery<T>),
    Discover(DiscoveryQuery<T>),
    Context(ContextQuery<T>),
}

impl<T> VectorQuery<T> {
    /// Iterate through all items, without any kind of structure
    pub fn flat_iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
        match self {
            VectorQuery::Nearest(input) => Box::new(std::iter::once(input)),
            VectorQuery::RecommendAverageVector(query) => Box::new(query.flat_iter()),
            VectorQuery::RecommendBestScore(query) => Box::new(query.flat_iter()),
            VectorQuery::Discover(query) => Box::new(query.flat_iter()),
            VectorQuery::Context(query) => Box::new(query.flat_iter()),
        }
    }
}

impl VectorQuery<VectorInput> {
    /// Turns all [VectorInput]s into [Vector]s, using the provided [ReferencedVectors] to look up the vectors.
    ///
    /// Will panic if the ids are not found in the [ReferencedVectors].
    fn ids_into_vectors(
        self,
        ids_to_vectors: &ReferencedVectors,
        lookup_vector_name: &str,
        lookup_collection: Option<&String>,
    ) -> VectorQuery<Vector> {
        match self {
            VectorQuery::Nearest(vector_input) => {
                let vector = ids_to_vectors
                    .resolve_reference(lookup_collection, lookup_vector_name, vector_input)
                    .unwrap();

                VectorQuery::Nearest(vector)
            }
            VectorQuery::RecommendAverageVector(reco) => {
                let (positives, negatives) = Self::resolve_reco_reference(
                    reco,
                    ids_to_vectors,
                    lookup_vector_name,
                    lookup_collection,
                );
                VectorQuery::RecommendAverageVector(RecoQuery::new(positives, negatives))
            }
            VectorQuery::RecommendBestScore(reco) => {
                let (positives, negatives) = Self::resolve_reco_reference(
                    reco,
                    ids_to_vectors,
                    lookup_vector_name,
                    lookup_collection,
                );
                VectorQuery::RecommendBestScore(RecoQuery::new(positives, negatives))
            }
            VectorQuery::Discover(discover) => {
                let target = ids_to_vectors
                    .resolve_reference(lookup_collection, lookup_vector_name, discover.target)
                    .unwrap();
                let pairs = discover
                    .pairs
                    .into_iter()
                    .map(|pair| ContextPair {
                        positive: ids_to_vectors
                            .resolve_reference(lookup_collection, lookup_vector_name, pair.positive)
                            .unwrap(),
                        negative: ids_to_vectors
                            .resolve_reference(lookup_collection, lookup_vector_name, pair.negative)
                            .unwrap(),
                    })
                    .collect();

                VectorQuery::Discover(DiscoveryQuery { target, pairs })
            }
            VectorQuery::Context(context) => {
                let pairs = context
                    .pairs
                    .into_iter()
                    .map(|pair| ContextPair {
                        positive: ids_to_vectors
                            .resolve_reference(lookup_collection, lookup_vector_name, pair.positive)
                            .unwrap(),
                        negative: ids_to_vectors
                            .resolve_reference(lookup_collection, lookup_vector_name, pair.negative)
                            .unwrap(),
                    })
                    .collect();

                VectorQuery::Context(ContextQuery { pairs })
            }
        }
    }

    /// Resolves the references in the RecoQuery into actual vectors.
    fn resolve_reco_reference(
        reco_query: RecoQuery<VectorInput>,
        ids_to_vectors: &ReferencedVectors,
        lookup_vector_name: &str,
        lookup_collection: Option<&String>,
    ) -> (Vec<Vector>, Vec<Vector>) {
        let positives = reco_query
            .positives
            .into_iter()
            .filter_map(|vector_input| {
                ids_to_vectors.resolve_reference(
                    lookup_collection,
                    lookup_vector_name,
                    vector_input,
                )
            })
            .collect();
        let negatives = reco_query
            .negatives
            .into_iter()
            .filter_map(|vector_input| {
                ids_to_vectors.resolve_reference(
                    lookup_collection,
                    lookup_vector_name,
                    vector_input,
                )
            })
            .collect();
        (positives, negatives)
    }
}

impl VectorQuery<Vector> {
    fn into_query_enum(self, using: String) -> CollectionResult<QueryEnum> {
        let query_enum = match self {
            VectorQuery::Nearest(vector) => {
                QueryEnum::Nearest(NamedVectorStruct::new_from_vector(vector, using))
            }
            VectorQuery::RecommendAverageVector(reco) => {
                // Get average vector
                let search_vector = avg_vector_for_recommendation(
                    reco.positives.iter().map(VectorRef::from),
                    reco.negatives.iter().map(VectorRef::from).peekable(),
                )?;
                QueryEnum::Nearest(NamedVectorStruct::new_from_vector(search_vector, using))
            }
            VectorQuery::RecommendBestScore(reco) => QueryEnum::RecommendBestScore(NamedQuery {
                query: reco,
                using: Some(using),
            }),
            VectorQuery::Discover(discover) => QueryEnum::Discover(NamedQuery {
                query: discover,
                using: Some(using),
            }),
            VectorQuery::Context(context) => QueryEnum::Context(NamedQuery {
                query: context,
                using: Some(using),
            }),
        };

        Ok(query_enum)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CollectionPrefetch {
    pub prefetch: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
    pub lookup_from: Option<LookupLocation>,
}

/// Exclude the referenced ids by editing the filter.
fn exclude_referenced_ids(ids: Vec<ExtendedPointId>, filter: Option<Filter>) -> Option<Filter> {
    let ids: HashSet<_> = ids.into_iter().collect();

    if ids.is_empty() {
        return filter;
    }

    let id_filter = Filter::new_must_not(Condition::HasId(HasIdCondition::from(ids)));
    Some(id_filter.merge_owned(filter.unwrap_or_default()))
}

impl CollectionPrefetch {
    fn get_lookup_collection(&self) -> Option<&String> {
        self.lookup_from.as_ref().map(|x| &x.collection)
    }

    fn get_lookup_vector_name(&self) -> String {
        self.lookup_from
            .as_ref()
            .and_then(|lookup_from| lookup_from.vector.as_ref())
            .unwrap_or(&self.using)
            .to_owned()
    }

    pub fn get_referenced_point_ids_on_collection(&self, collection: &str) -> Vec<PointIdType> {
        let mut refs = Vec::new();

        let mut lookup_other_collection = false;
        if let Some(lookup_collection) = self.get_lookup_collection() {
            lookup_other_collection = lookup_collection != collection
        };

        if !lookup_other_collection {
            if let Some(Query::Vector(vector_query)) = &self.query {
                if let VectorQuery::Nearest(VectorInput::Id(id)) = vector_query {
                    refs.push(*id);
                }
                refs.extend(vector_query.get_referenced_ids())
            };
        }

        for prefetch in &self.prefetch {
            refs.extend(prefetch.get_referenced_point_ids_on_collection(collection))
        }

        refs
    }

    fn try_into_shard_prefetch(
        self,
        ids_to_vectors: &ReferencedVectors,
    ) -> CollectionResult<ShardPrefetch> {
        CollectionQueryRequest::validation(
            &self.query,
            &self.using,
            &self.prefetch,
            self.score_threshold,
        )?;

        let lookup_vector_name = self.get_lookup_vector_name();
        let lookup_collection = self.get_lookup_collection().cloned();
        let using = self.using.clone();

        let query = self
            .query
            .map(|query| {
                query.try_into_scoring_query(
                    ids_to_vectors,
                    &lookup_vector_name,
                    lookup_collection.as_ref(),
                    using,
                )
            })
            .transpose()?;

        let prefetches = self
            .prefetch
            .into_iter()
            .map(|prefetch| prefetch.try_into_shard_prefetch(ids_to_vectors))
            .try_collect()?;

        Ok(ShardPrefetch {
            prefetches,
            query,
            filter: self.filter,
            score_threshold: self.score_threshold,
            limit: self.limit,
            params: self.params,
        })
    }

    pub fn flatten_resolver_requests(&self) -> Vec<CollectionQueryResolveRequest> {
        let mut inner_queries = vec![];
        // resolve query for root query
        if let Some(Query::Vector(vector_query)) = &self.query {
            let resolve_root = CollectionQueryResolveRequest {
                vector_query,
                lookup_from: self.lookup_from.clone(),
                using: self.using.clone(),
            };
            inner_queries.push(resolve_root);
        }

        // recurse on prefetches
        for prefetch in &self.prefetch {
            for flatten in prefetch.flatten_resolver_requests() {
                inner_queries.push(flatten);
            }
        }
        inner_queries
    }
}

impl CollectionQueryRequest {
    fn get_lookup_collection(&self) -> Option<&String> {
        self.lookup_from.as_ref().map(|x| &x.collection)
    }

    fn get_lookup_vector_name(&self) -> String {
        self.lookup_from
            .as_ref()
            .and_then(|lookup_from| lookup_from.vector.as_ref())
            .unwrap_or(&self.using)
            .to_owned()
    }

    fn get_referenced_point_ids_on_collection(&self, collection: &str) -> Vec<PointIdType> {
        let mut refs = Vec::new();

        let mut lookup_other_collection = false;
        if let Some(lookup_collection) = self.get_lookup_collection() {
            lookup_other_collection = lookup_collection != collection
        };

        if !lookup_other_collection {
            if let Some(Query::Vector(vector_query)) = &self.query {
                if let VectorQuery::Nearest(VectorInput::Id(id)) = vector_query {
                    refs.push(*id);
                }
                refs.extend(vector_query.get_referenced_ids())
            };
        }

        for prefetch in &self.prefetch {
            refs.extend(prefetch.get_referenced_point_ids_on_collection(collection))
        }

        refs
    }

    /// Substitutes all the point ids in the request with the actual vectors, as well as editing filters so that ids are not included in the response.
    pub fn try_into_shard_request(
        self,
        collection_name: &str,
        ids_to_vectors: &ReferencedVectors,
    ) -> CollectionResult<ShardQueryRequest> {
        Self::validation(
            &self.query,
            &self.using,
            &self.prefetch,
            self.score_threshold,
        )?;

        let mut offset = self.offset;
        if matches!(self.query, Some(Query::Sample(Sample::Random))) && self.prefetch.is_empty() {
            // Shortcut: Ignore offset with random query, since output is not stable.
            offset = 0;
        }

        let query_lookup_collection = self.get_lookup_collection().cloned();
        let query_lookup_vector_name = self.get_lookup_vector_name();
        let using = self.using.clone();

        // Edit filter to exclude all referenced point ids (root and nested) on the searched collection
        // We do not want to exclude vector ids from different collection via lookup_from.
        let referenced_point_ids = self.get_referenced_point_ids_on_collection(collection_name);

        let filter = exclude_referenced_ids(referenced_point_ids, self.filter);

        let query = self
            .query
            .map(|query| {
                query.try_into_scoring_query(
                    ids_to_vectors,
                    &query_lookup_vector_name,
                    query_lookup_collection.as_ref(),
                    using,
                )
            })
            .transpose()?;

        let prefetches = self
            .prefetch
            .into_iter()
            .map(|prefetch| prefetch.try_into_shard_prefetch(ids_to_vectors))
            .try_collect()?;

        Ok(ShardQueryRequest {
            prefetches,
            query,
            filter,
            score_threshold: self.score_threshold,
            limit: self.limit,
            offset,
            params: self.params,
            with_vector: self.with_vector,
            with_payload: self.with_payload,
        })
    }

    pub fn validation(
        query: &Option<Query>,
        using: &String,
        prefetch: &[CollectionPrefetch],
        score_threshold: Option<ScoreType>,
    ) -> CollectionResult<()> {
        // Check no prefetches without a query
        if !prefetch.is_empty() && query.is_none() {
            return Err(CollectionError::bad_request(
                "A query is needed to merge the prefetches. Can't have prefetches without defining a query.",
            ));
        }

        // Check no score_threshold without a vector query
        if score_threshold.is_some() {
            match query {
                Some(Query::OrderBy(_)) => {
                    return Err(CollectionError::bad_request(
                        "Can't use score_threshold with an order_by query.",
                    ));
                }
                None => {
                    return Err(CollectionError::bad_request(
                        "A query is needed to use the score_threshold. Can't have score_threshold without defining a query.",
                    ));
                }
                _ => {}
            }
        }

        // Check that fusion queries are not combined with a using vector name
        if let Some(Query::Fusion(_)) = query {
            if using != DEFAULT_VECTOR_NAME {
                return Err(CollectionError::bad_request(
                    "Fusion queries cannot be combined with the 'using' field.",
                ));
            }
        }

        Ok(())
    }
}

mod from_rest {
    use api::rest::schema as rest;

    use super::*;

    impl From<rest::QueryGroupsRequestInternal> for CollectionQueryGroupsRequest {
        fn from(value: rest::QueryGroupsRequestInternal) -> Self {
            let rest::QueryGroupsRequestInternal {
                prefetch,
                query,
                using,
                filter,
                score_threshold,
                params,
                with_vector,
                with_payload,
                lookup_from,
                group_request,
            } = value;

            Self {
                prefetch: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                params,
                with_vector: with_vector.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
                with_payload: with_payload.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
                lookup_from,
                limit: group_request
                    .limit
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                group_by: group_request.group_by,
                group_size: group_request
                    .group_size
                    .unwrap_or(CollectionQueryRequest::DEFAULT_GROUP_SIZE),
                with_lookup: group_request.with_lookup.map(WithLookup::from),
            }
        }
    }

    impl From<rest::QueryRequestInternal> for CollectionQueryRequest {
        fn from(value: rest::QueryRequestInternal) -> Self {
            let rest::QueryRequestInternal {
                prefetch,
                query,
                using,
                filter,
                score_threshold,
                params,
                limit,
                offset,
                with_vector,
                with_payload,
                lookup_from,
            } = value;

            Self {
                prefetch: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                limit: limit.unwrap_or(Self::DEFAULT_LIMIT),
                offset: offset.unwrap_or(Self::DEFAULT_OFFSET),
                params,
                with_vector: with_vector.unwrap_or(Self::DEFAULT_WITH_VECTOR),
                with_payload: with_payload.unwrap_or(Self::DEFAULT_WITH_PAYLOAD),
                lookup_from: lookup_from.map(LookupLocation::from),
            }
        }
    }

    impl From<rest::Prefetch> for CollectionPrefetch {
        fn from(value: rest::Prefetch) -> Self {
            let rest::Prefetch {
                prefetch,
                query,
                using,
                filter,
                score_threshold,
                params,
                limit,
                lookup_from,
            } = value;

            Self {
                prefetch: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                params,
                lookup_from,
            }
        }
    }

    impl From<rest::QueryInterface> for Query {
        fn from(value: rest::QueryInterface) -> Self {
            Query::from(rest::Query::from(value))
        }
    }

    impl From<rest::Query> for Query {
        fn from(value: rest::Query) -> Self {
            match value {
                rest::Query::Nearest(nearest) => {
                    Query::Vector(VectorQuery::Nearest(From::from(nearest.nearest)))
                }
                rest::Query::Recommend(recommend) => Query::Vector(From::from(recommend.recommend)),
                rest::Query::Discover(discover) => Query::Vector(From::from(discover.discover)),
                rest::Query::Context(context) => Query::Vector(From::from(context.context)),
                rest::Query::OrderBy(order_by) => Query::OrderBy(OrderBy::from(order_by.order_by)),
                rest::Query::Fusion(fusion) => Query::Fusion(Fusion::from(fusion.fusion)),
                rest::Query::Sample(sample) => Query::Sample(Sample::from(sample.sample)),
            }
        }
    }

    impl From<rest::RecommendInput> for VectorQuery<VectorInput> {
        fn from(value: rest::RecommendInput) -> Self {
            let rest::RecommendInput {
                positive,
                negative,
                strategy,
            } = value;

            let positives = positive.into_iter().flatten().map(From::from).collect();
            let negatives = negative.into_iter().flatten().map(From::from).collect();
            let reco_query = RecoQuery::new(positives, negatives);

            match strategy.unwrap_or_default() {
                RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
                RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
            }
        }
    }

    impl From<rest::DiscoverInput> for VectorQuery<VectorInput> {
        fn from(value: rest::DiscoverInput) -> Self {
            let rest::DiscoverInput { target, context } = value;

            let target = From::from(target);
            let context = context
                .into_iter()
                .flatten()
                .map(context_pair_from_rest)
                .collect();

            VectorQuery::Discover(DiscoveryQuery::new(target, context))
        }
    }

    impl From<rest::ContextInput> for VectorQuery<VectorInput> {
        fn from(value: rest::ContextInput) -> Self {
            let rest::ContextInput(pairs) = value;

            let context = pairs
                .into_iter()
                .flatten()
                .map(context_pair_from_rest)
                .collect();

            VectorQuery::Context(ContextQuery::new(context))
        }
    }

    impl From<rest::VectorInput> for VectorInput {
        fn from(value: rest::VectorInput) -> Self {
            match value {
                rest::VectorInput::Id(id) => VectorInput::Id(id),
                rest::VectorInput::DenseVector(dense) => VectorInput::Vector(Vector::Dense(dense)),
                rest::VectorInput::SparseVector(sparse) => {
                    VectorInput::Vector(Vector::Sparse(sparse))
                }
                rest::VectorInput::MultiDenseVector(multi_dense) => VectorInput::Vector(
                    // TODO(universal-query): Validate at API level
                    Vector::MultiDense(MultiDenseVectorInternal::new_unchecked(multi_dense)),
                ),
                rest::VectorInput::Document(_) => {
                    // If this is reached, it means validation failed
                    unimplemented!("Document inference is not implemented")
                }
            }
        }
    }

    /// Circular dependencies prevents us from implementing `From` directly
    fn context_pair_from_rest(value: rest::ContextPair) -> ContextPair<VectorInput> {
        let rest::ContextPair { positive, negative } = value;

        ContextPair {
            positive: VectorInput::from(positive),
            negative: VectorInput::from(negative),
        }
    }

    impl From<rest::Fusion> for Fusion {
        fn from(value: rest::Fusion) -> Self {
            match value {
                rest::Fusion::Rrf => Fusion::Rrf,
                rest::Fusion::Dbsf => Fusion::Dbsf,
            }
        }
    }

    impl From<rest::Sample> for Sample {
        fn from(value: rest::Sample) -> Self {
            match value {
                rest::Sample::Random => Sample::Random,
            }
        }
    }
}

pub mod from_grpc {
    use api::grpc::conversions::json_path_from_proto;
    use api::grpc::qdrant::{self as grpc};
    use tonic::Status;

    use super::*;

    impl TryFrom<api::grpc::qdrant::QueryPointGroups> for CollectionQueryGroupsRequest {
        type Error = Status;

        fn try_from(value: api::grpc::qdrant::QueryPointGroups) -> Result<Self, Self::Error> {
            let grpc::QueryPointGroups {
                collection_name: _,
                prefetch,
                query,
                using,
                filter,
                params,
                score_threshold,
                with_payload,
                with_vectors,
                lookup_from,
                limit,
                group_size,
                group_by,
                with_lookup,
                read_consistency: _,
                timeout: _,
                shard_key_selector: _,
            } = value;

            let request = CollectionQueryGroupsRequest {
                prefetch: prefetch
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                query: query.map(TryFrom::try_from).transpose()?,
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter: filter.map(TryFrom::try_from).transpose()?,
                score_threshold,
                with_vector: with_vectors
                    .map(From::from)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
                with_payload: with_payload
                    .map(TryFrom::try_from)
                    .transpose()?
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
                lookup_from: lookup_from.map(From::from),
                group_by: json_path_from_proto(&group_by)?,
                group_size: group_size
                    .map(|s| s as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_GROUP_SIZE),
                limit: limit
                    .map(|l| l as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                params: params.map(From::from),
                with_lookup: with_lookup.map(TryFrom::try_from).transpose()?,
            };
            Ok(request)
        }
    }

    impl TryFrom<api::grpc::qdrant::QueryPoints> for CollectionQueryRequest {
        type Error = Status;

        fn try_from(value: api::grpc::qdrant::QueryPoints) -> Result<Self, Self::Error> {
            let grpc::QueryPoints {
                collection_name: _,
                prefetch,
                query,
                using,
                filter,
                params,
                score_threshold,
                limit,
                offset,
                with_payload,
                with_vectors,
                read_consistency: _,
                shard_key_selector: _,
                lookup_from,
                timeout: _,
            } = value;

            let request = CollectionQueryRequest {
                prefetch: prefetch
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                query: query.map(TryFrom::try_from).transpose()?,
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter: filter.map(TryFrom::try_from).transpose()?,
                score_threshold,
                limit: limit
                    .map(|l| l as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                offset: offset
                    .map(|o| o as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_OFFSET),
                params: params.map(From::from),
                with_vector: with_vectors
                    .map(From::from)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
                with_payload: with_payload
                    .map(TryFrom::try_from)
                    .transpose()?
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
                lookup_from: lookup_from.map(From::from),
            };
            Ok(request)
        }
    }

    impl TryFrom<grpc::PrefetchQuery> for CollectionPrefetch {
        type Error = Status;

        fn try_from(value: grpc::PrefetchQuery) -> Result<Self, Self::Error> {
            let grpc::PrefetchQuery {
                prefetch,
                query,
                using,
                filter,
                params,
                score_threshold,
                limit,
                lookup_from,
            } = value;

            let collection_query = Self {
                prefetch: prefetch
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                query: query.map(TryFrom::try_from).transpose()?,
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter: filter.map(TryFrom::try_from).transpose()?,
                score_threshold,
                limit: limit
                    .map(|l| l as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                params: params.map(From::from),
                lookup_from: lookup_from.map(From::from),
            };

            Ok(collection_query)
        }
    }

    impl TryFrom<grpc::Query> for Query {
        type Error = Status;

        fn try_from(value: grpc::Query) -> Result<Self, Self::Error> {
            use api::grpc::qdrant::query::Variant;

            let variant = value
                .variant
                .ok_or_else(|| Status::invalid_argument("Query variant is missing"))?;

            let query = match variant {
                Variant::Nearest(nearest) => {
                    Query::Vector(VectorQuery::Nearest(TryFrom::try_from(nearest)?))
                }
                Variant::Recommend(recommend) => Query::Vector(TryFrom::try_from(recommend)?),
                Variant::Discover(discover) => Query::Vector(TryFrom::try_from(discover)?),
                Variant::Context(context) => Query::Vector(TryFrom::try_from(context)?),
                Variant::OrderBy(order_by) => Query::OrderBy(OrderBy::try_from(order_by)?),
                Variant::Fusion(fusion) => Query::Fusion(Fusion::try_from(fusion)?),
                Variant::Sample(sample) => Query::Sample(Sample::try_from(sample)?),
            };

            Ok(query)
        }
    }

    impl TryFrom<grpc::RecommendInput> for VectorQuery<VectorInput> {
        type Error = Status;

        fn try_from(value: grpc::RecommendInput) -> Result<Self, Self::Error> {
            let grpc::RecommendInput {
                positive,
                negative,
                strategy,
            } = value;

            let positives = positive
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            let negatives = negative
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            let reco_query = RecoQuery::new(positives, negatives);

            let strategy = strategy
                .and_then(|x|
                    // XXX: Invalid values silently converted to None
                    grpc::RecommendStrategy::try_from(x).ok())
                .map(RecommendStrategy::from)
                .unwrap_or_default();

            let query = match strategy {
                RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
                RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
            };

            Ok(query)
        }
    }

    impl TryFrom<grpc::DiscoverInput> for VectorQuery<VectorInput> {
        type Error = Status;

        fn try_from(value: grpc::DiscoverInput) -> Result<Self, Self::Error> {
            let grpc::DiscoverInput { target, context } = value;

            let target = VectorInput::try_from(
                target
                    .ok_or_else(|| Status::invalid_argument("DiscoverInput target is missing"))?,
            )?;

            let grpc::ContextInput { pairs } = context
                .ok_or_else(|| Status::invalid_argument("DiscoverInput context is missing"))?;

            let context = pairs
                .into_iter()
                .map(context_pair_from_grpc)
                .collect::<Result<_, _>>()?;

            Ok(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
        }
    }

    impl TryFrom<grpc::ContextInput> for VectorQuery<VectorInput> {
        type Error = Status;

        fn try_from(value: grpc::ContextInput) -> Result<Self, Self::Error> {
            let context_query = context_query_from_grpc(value)?;

            Ok(VectorQuery::Context(context_query))
        }
    }

    impl TryFrom<grpc::VectorInput> for VectorInput {
        type Error = Status;

        fn try_from(value: grpc::VectorInput) -> Result<Self, Self::Error> {
            use api::grpc::qdrant::vector_input::Variant;

            let variant = value
                .variant
                .ok_or_else(|| Status::invalid_argument("VectorInput variant is missing"))?;

            let vector_input = match variant {
                Variant::Id(id) => VectorInput::Id(TryFrom::try_from(id)?),
                Variant::Dense(dense) => VectorInput::Vector(Vector::Dense(From::from(dense))),
                Variant::Sparse(sparse) => VectorInput::Vector(Vector::Sparse(From::from(sparse))),
                Variant::MultiDense(multi_dense) => VectorInput::Vector(
                    // TODO(universal-query): Validate at API level
                    Vector::MultiDense(From::from(multi_dense)),
                ),
            };

            Ok(vector_input)
        }
    }

    /// Circular dependencies prevents us from implementing `TryFrom` directly
    fn context_query_from_grpc(
        value: grpc::ContextInput,
    ) -> Result<ContextQuery<VectorInput>, Status> {
        let grpc::ContextInput { pairs } = value;

        Ok(ContextQuery {
            pairs: pairs
                .into_iter()
                .map(context_pair_from_grpc)
                .collect::<Result<_, _>>()?,
        })
    }

    /// Circular dependencies prevents us from implementing `TryFrom` directly
    fn context_pair_from_grpc(
        value: grpc::ContextInputPair,
    ) -> Result<ContextPair<VectorInput>, Status> {
        let grpc::ContextInputPair { positive, negative } = value;

        let positive =
            positive.ok_or_else(|| Status::invalid_argument("ContextPair positive is missing"))?;
        let negative =
            negative.ok_or_else(|| Status::invalid_argument("ContextPair negative is missing"))?;

        Ok(ContextPair {
            positive: VectorInput::try_from(positive)?,
            negative: VectorInput::try_from(negative)?,
        })
    }
}
