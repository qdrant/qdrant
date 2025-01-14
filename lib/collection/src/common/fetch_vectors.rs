use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::time::Duration;

use api::rest::ShardKeySelector;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::future::try_join_all;
use futures::Future;
use segment::data_types::vectors::{VectorInternal, VectorRef};
use segment::types::{PointIdType, WithPayloadInterface, WithVector};
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::batching::batch_requests;
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionError, CollectionResult, PointRequestInternal, RecommendExample, RecordInternal,
};
use crate::operations::universal_query::collection_query;
use crate::operations::universal_query::collection_query::{
    CollectionQueryRequest, CollectionQueryResolveRequest, VectorInputInternal,
};

pub async fn retrieve_points(
    collection: &Collection,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    read_consistency: Option<ReadConsistency>,
    shard_selector: &ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<RecordInternal>> {
    collection
        .retrieve(
            PointRequestInternal {
                ids,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Selector(vector_names),
            },
            read_consistency,
            shard_selector,
            timeout,
            hw_measurement_acc,
        )
        .await
}

pub enum CollectionRefHolder<'a> {
    Ref(&'a Collection),
    Guard(RwLockReadGuard<'a, Collection>),
}

pub async fn retrieve_points_with_locked_collection(
    collection_holder: CollectionRefHolder<'_>,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    read_consistency: Option<ReadConsistency>,
    shard_selector: &ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<RecordInternal>> {
    match collection_holder {
        CollectionRefHolder::Ref(collection) => {
            retrieve_points(
                collection,
                ids,
                vector_names,
                read_consistency,
                shard_selector,
                timeout,
                hw_measurement_acc,
            )
            .await
        }
        CollectionRefHolder::Guard(guard) => {
            retrieve_points(
                &guard,
                ids,
                vector_names,
                read_consistency,
                shard_selector,
                timeout,
                hw_measurement_acc,
            )
            .await
        }
    }
}

pub type CollectionName = String;

/// This is a temporary structure, which holds resolved references to vectors,
/// mentioned in the query.
///
///  ┌──────────────┐
///  │              │  -> Batch request
///  │ request(+ids)├───────┐   to storage
///  │              │       │
///  └──────────────┘       │
///                         │
///                         │
///    Reference Vectors    ▼
///  ┌──────────────────────────────┐
///  │                              │
///  │  ┌───────┐      ┌──────────┐ │
///  │  │       │      │          │ │
///  │  │  IDs  ├─────►│ Vectors  │ │
///  │  │       │      │          │ │
///  │  └───────┘      └──────────┘ │
///  │                              │
///  └──────────────────────────────┘
///
#[derive(Default, Debug)]
pub struct ReferencedVectors {
    collection_mapping: HashMap<CollectionName, HashMap<PointIdType, RecordInternal>>,
    default_mapping: HashMap<PointIdType, RecordInternal>,
}

impl ReferencedVectors {
    pub fn extend(
        &mut self,
        collection_name: Option<CollectionName>,
        mapping: impl IntoIterator<Item = (PointIdType, RecordInternal)>,
    ) {
        match collection_name {
            None => self.default_mapping.extend(mapping),
            Some(collection) => {
                let entry = self.collection_mapping.entry(collection);
                let entry_internal: &mut HashMap<_, _> = entry.or_default();
                entry_internal.extend(mapping);
            }
        }
    }

    pub fn extend_from_other(&mut self, other: Self) {
        self.default_mapping.extend(other.default_mapping);
        for (collection_name, points) in other.collection_mapping {
            let entry = self.collection_mapping.entry(collection_name);
            let entry_internal: &mut HashMap<_, _> = entry.or_default();
            entry_internal.extend(points);
        }
    }

    pub fn get(
        &self,
        lookup_collection_name: Option<&CollectionName>,
        point_id: PointIdType,
    ) -> Option<&RecordInternal> {
        match lookup_collection_name {
            None => self.default_mapping.get(&point_id),
            Some(collection) => {
                let collection_mapping = self.collection_mapping.get(collection)?;
                collection_mapping.get(&point_id)
            }
        }
    }

    /// Convert potential reference to a vector (vector id) into actual vector,
    /// which was resolved by the request to the storage.
    pub fn resolve_reference<'a>(
        &'a self,
        collection_name: Option<&'a String>,
        vector_name: &str,
        vector_input: VectorInputInternal,
    ) -> Option<VectorInternal> {
        match vector_input {
            VectorInputInternal::Vector(vector) => Some(vector),
            VectorInputInternal::Id(vid) => {
                let rec = self.get(collection_name, vid)?;
                rec.get_vector_by_name(vector_name).map(|v| v.to_owned())
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct ReferencedPoints<'coll_name> {
    ids_per_collection: HashMap<Option<&'coll_name String>, HashSet<PointIdType>>,
    vector_names_per_collection: HashMap<Option<&'coll_name String>, HashSet<String>>,
}

impl<'coll_name> ReferencedPoints<'coll_name> {
    pub fn is_empty(&self) -> bool {
        self.ids_per_collection.is_empty() && self.vector_names_per_collection.is_empty()
    }

    pub fn add_from_iter(
        &mut self,
        point_ids: impl Iterator<Item = PointIdType>,
        vector_name: String,
        collection_name: Option<&'coll_name String>,
    ) {
        let reference_vectors_ids = self.ids_per_collection.entry(collection_name).or_default();

        let vector_names = self
            .vector_names_per_collection
            .entry(collection_name)
            .or_default();

        vector_names.insert(vector_name);

        point_ids.for_each(|point_id| {
            reference_vectors_ids.insert(point_id);
        });
    }

    pub async fn fetch_vectors<'a, F, Fut>(
        mut self,
        collection: &Collection,
        read_consistency: Option<ReadConsistency>,
        collection_by_name: &F,
        shard_selector: ShardSelectorInternal,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<ReferencedVectors>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        debug_assert!(self.ids_per_collection.len() == self.vector_names_per_collection.len());

        let mut collections_names = Vec::new();
        let mut vector_retrieves = Vec::new();
        for (collection_name, reference_vectors_ids) in self.ids_per_collection {
            collections_names.push(collection_name);
            let points: Vec<_> = reference_vectors_ids.into_iter().collect();
            let vector_names: Vec<_> = self
                .vector_names_per_collection
                .remove(&collection_name)
                .unwrap()
                .into_iter()
                .collect();
            match collection_name {
                None => vector_retrieves.push(retrieve_points_with_locked_collection(
                    CollectionRefHolder::Ref(collection),
                    points,
                    vector_names,
                    read_consistency,
                    &shard_selector,
                    timeout,
                    hw_measurement_acc.clone(),
                )),
                Some(name) => {
                    let other_collection = collection_by_name(name.to_string()).await;
                    match other_collection {
                        Some(other_collection) => {
                            vector_retrieves.push(retrieve_points_with_locked_collection(
                                CollectionRefHolder::Guard(other_collection),
                                points,
                                vector_names,
                                read_consistency,
                                &shard_selector,
                                timeout,
                                hw_measurement_acc.clone(),
                            ))
                        }
                        None => {
                            return Err(CollectionError::NotFound {
                                what: format!("Collection {name}"),
                            })
                        }
                    }
                }
            }
        }
        let all_reference_vectors: Vec<Vec<RecordInternal>> =
            try_join_all(vector_retrieves).await?;
        let mut all_vectors_records_map: ReferencedVectors = Default::default();

        for (collection_name, reference_vectors) in
            collections_names.into_iter().zip(all_reference_vectors)
        {
            all_vectors_records_map.extend(
                collection_name.cloned(),
                reference_vectors
                    .into_iter()
                    .map(|record| (record.id, record)),
            );
        }

        Ok(all_vectors_records_map)
    }
}

pub fn convert_to_vectors_owned(
    examples: Vec<RecommendExample>,
    all_vectors_records_map: &ReferencedVectors,
    vector_name: &str,
    collection_name: Option<&String>,
) -> Vec<VectorInternal> {
    examples
        .into_iter()
        .filter_map(|example| match example {
            RecommendExample::Dense(vector) => Some(vector.into()),
            RecommendExample::Sparse(vector) => Some(vector.into()),
            RecommendExample::PointId(vid) => {
                let rec = all_vectors_records_map.get(collection_name, vid).unwrap();
                rec.get_vector_by_name(vector_name).map(|v| v.to_owned())
            }
        })
        .collect()
}

pub fn convert_to_vectors<'a>(
    examples: impl Iterator<Item = &'a RecommendExample> + 'a,
    all_vectors_records_map: &'a ReferencedVectors,
    vector_name: &'a str,
    collection_name: Option<&'a String>,
) -> impl Iterator<Item = VectorRef<'a>> + 'a {
    examples.filter_map(move |example| match example {
        RecommendExample::Dense(vector) => Some(vector.into()),
        RecommendExample::Sparse(vector) => Some(vector.into()),
        RecommendExample::PointId(vid) => {
            let rec = all_vectors_records_map.get(collection_name, *vid).unwrap();
            rec.get_vector_by_name(vector_name)
        }
    })
}

pub async fn resolve_referenced_vectors_batch<'a, 'b, F, Fut, Req: RetrieveRequest>(
    requests: &'b [(Req, ShardSelectorInternal)],
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<ReferencedVectors>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let fetch_requests = batch_requests::<
        &(Req, ShardSelectorInternal),
        Option<ShardKeySelector>,
        ReferencedPoints,
        Vec<_>,
    >(
        requests,
        |(request, _)| request.get_lookup_shard_key(),
        |(request, _), referenced_points| {
            let collection_name = request.get_lookup_collection();
            let vector_name = request.get_lookup_vector_name();
            let point_ids_iter = request.get_referenced_point_ids();
            referenced_points.add_from_iter(
                point_ids_iter.into_iter(),
                vector_name,
                collection_name,
            );
            Ok(())
        },
        |shard_selector, referenced_points, requests| {
            let shard_selector = match shard_selector {
                None => ShardSelectorInternal::All,
                Some(shard_key_selector) => ShardSelectorInternal::from(shard_key_selector),
            };

            if referenced_points.is_empty() {
                return Ok(());
            }
            let fetch = referenced_points.fetch_vectors(
                collection,
                read_consistency,
                &collection_by_name,
                shard_selector,
                timeout,
                hw_measurement_acc.clone(),
            );
            requests.push(fetch);
            Ok(())
        },
    )?;

    let batch_reference_vectors: Vec<_> = try_join_all(fetch_requests).await?;

    if batch_reference_vectors.len() == 1 {
        return Ok(batch_reference_vectors.into_iter().next().unwrap());
    }

    let mut all_vectors_records_map: ReferencedVectors = Default::default();

    for reference_vectors in batch_reference_vectors {
        all_vectors_records_map.extend_from_other(reference_vectors);
    }

    Ok(all_vectors_records_map)
}

/// This function is used to build a list of queries to resolve vectors for the given batch of query requests.
///
/// For each request, one query is issue for the root request and one query for each nested prefetch.
/// The resolver queries have no prefetches.
pub fn build_vector_resolver_queries(
    requests_batch: &Vec<(CollectionQueryRequest, ShardSelectorInternal)>,
) -> Vec<(CollectionQueryResolveRequest, ShardSelectorInternal)> {
    let mut resolve_prefetches = vec![];
    for (request, shard_selector) in requests_batch {
        build_vector_resolver_query(request, shard_selector)
            .into_iter()
            .for_each(|(resolve_request, shard_selector)| {
                resolve_prefetches.push((resolve_request, shard_selector))
            });
    }
    resolve_prefetches
}

pub fn build_vector_resolver_query<'a>(
    request: &'a CollectionQueryRequest,
    shard_selector: &'a ShardSelectorInternal,
) -> Vec<(CollectionQueryResolveRequest<'a>, ShardSelectorInternal)> {
    let mut resolve_prefetches = vec![];
    // resolve query for root query
    if let Some(collection_query::Query::Vector(vector_query)) = &request.query {
        let resolve_root = CollectionQueryResolveRequest {
            vector_query,
            lookup_from: request.lookup_from.clone(),
            using: request.using.clone(),
        };
        resolve_prefetches.push((resolve_root, shard_selector.clone()));
    }

    // flatten prefetches
    for prefetch in &request.prefetch {
        for flatten in prefetch.flatten_resolver_requests() {
            resolve_prefetches.push((flatten, shard_selector.clone()));
        }
    }

    resolve_prefetches
}
