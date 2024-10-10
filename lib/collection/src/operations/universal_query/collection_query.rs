use std::collections::HashSet;

use api::rest::LookupLocation;
use common::types::ScoreType;
use itertools::Itertools;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{
    NamedQuery, NamedVectorStruct, VectorInternal, VectorRef, DEFAULT_VECTOR_NAME,
};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, SearchParams,
    WithPayloadInterface, WithVector,
};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};

use super::shard_query::{
    FusionInternal, SampleInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest,
};
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
    pub vector_query: &'a VectorQuery<VectorInputInternal>,
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
    Vector(VectorQuery<VectorInputInternal>),

    /// Reciprocal rank fusion
    Fusion(FusionInternal),

    /// Order by a payload field
    OrderBy(OrderBy),

    /// Sample points
    Sample(SampleInternal),
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
                    .ids_into_vectors(ids_to_vectors, lookup_vector_name, lookup_collection)?
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
pub enum VectorInputInternal {
    Id(PointIdType),
    Vector(VectorInternal),
}

impl VectorInputInternal {
    pub fn as_id(&self) -> Option<&PointIdType> {
        match self {
            VectorInputInternal::Id(id) => Some(id),
            VectorInputInternal::Vector(_) => None,
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

impl VectorQuery<VectorInputInternal> {
    /// Turns all [VectorInputInternal]s into [VectorInternal]s, using the provided [ReferencedVectors] to look up the vectors.
    ///
    /// Will panic if the ids are not found in the [ReferencedVectors].
    fn ids_into_vectors(
        self,
        ids_to_vectors: &ReferencedVectors,
        lookup_vector_name: &str,
        lookup_collection: Option<&String>,
    ) -> CollectionResult<VectorQuery<VectorInternal>> {
        match self {
            VectorQuery::Nearest(vector_input) => {
                let vector = ids_to_vectors
                    .resolve_reference(lookup_collection, lookup_vector_name, vector_input)
                    .ok_or_else(|| vector_not_found_error(lookup_vector_name))?;

                Ok(VectorQuery::Nearest(vector))
            }
            VectorQuery::RecommendAverageVector(reco) => {
                let (positives, negatives) = Self::resolve_reco_reference(
                    reco,
                    ids_to_vectors,
                    lookup_vector_name,
                    lookup_collection,
                );
                Ok(VectorQuery::RecommendAverageVector(RecoQuery::new(
                    positives, negatives,
                )))
            }
            VectorQuery::RecommendBestScore(reco) => {
                let (positives, negatives) = Self::resolve_reco_reference(
                    reco,
                    ids_to_vectors,
                    lookup_vector_name,
                    lookup_collection,
                );
                Ok(VectorQuery::RecommendBestScore(RecoQuery::new(
                    positives, negatives,
                )))
            }
            VectorQuery::Discover(discover) => {
                let target = ids_to_vectors
                    .resolve_reference(lookup_collection, lookup_vector_name, discover.target)
                    .ok_or_else(|| vector_not_found_error(lookup_vector_name))?;
                let pairs = discover
                    .pairs
                    .into_iter()
                    .map(|pair| {
                        Ok(ContextPair {
                            positive: ids_to_vectors
                                .resolve_reference(
                                    lookup_collection,
                                    lookup_vector_name,
                                    pair.positive,
                                )
                                .ok_or_else(|| vector_not_found_error(lookup_vector_name))?,
                            negative: ids_to_vectors
                                .resolve_reference(
                                    lookup_collection,
                                    lookup_vector_name,
                                    pair.negative,
                                )
                                .ok_or_else(|| vector_not_found_error(lookup_vector_name))?,
                        })
                    })
                    .collect::<CollectionResult<_>>()?;

                Ok(VectorQuery::Discover(DiscoveryQuery { target, pairs }))
            }
            VectorQuery::Context(context) => {
                let pairs = context
                    .pairs
                    .into_iter()
                    .map(|pair| {
                        Ok(ContextPair {
                            positive: ids_to_vectors
                                .resolve_reference(
                                    lookup_collection,
                                    lookup_vector_name,
                                    pair.positive,
                                )
                                .ok_or_else(|| vector_not_found_error(lookup_vector_name))?,
                            negative: ids_to_vectors
                                .resolve_reference(
                                    lookup_collection,
                                    lookup_vector_name,
                                    pair.negative,
                                )
                                .ok_or_else(|| vector_not_found_error(lookup_vector_name))?,
                        })
                    })
                    .collect::<CollectionResult<_>>()?;

                Ok(VectorQuery::Context(ContextQuery { pairs }))
            }
        }
    }

    /// Resolves the references in the RecoQuery into actual vectors.
    fn resolve_reco_reference(
        reco_query: RecoQuery<VectorInputInternal>,
        ids_to_vectors: &ReferencedVectors,
        lookup_vector_name: &str,
        lookup_collection: Option<&String>,
    ) -> (Vec<VectorInternal>, Vec<VectorInternal>) {
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

fn vector_not_found_error(vector_name: &str) -> CollectionError {
    CollectionError::not_found(format!("Vector with name {vector_name:?} for point"))
}

impl VectorQuery<VectorInternal> {
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
                if let VectorQuery::Nearest(VectorInputInternal::Id(id)) = vector_query {
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
                if let VectorQuery::Nearest(VectorInputInternal::Id(id)) = vector_query {
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
        if matches!(self.query, Some(Query::Sample(SampleInternal::Random)))
            && self.prefetch.is_empty()
        {
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

    impl From<rest::Fusion> for FusionInternal {
        fn from(value: rest::Fusion) -> Self {
            match value {
                rest::Fusion::Rrf => FusionInternal::Rrf,
                rest::Fusion::Dbsf => FusionInternal::Dbsf,
            }
        }
    }

    impl From<rest::Sample> for SampleInternal {
        fn from(value: rest::Sample) -> Self {
            match value {
                rest::Sample::Random => SampleInternal::Random,
            }
        }
    }
}
