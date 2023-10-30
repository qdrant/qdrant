use std::collections::{HashMap, HashSet};

use futures::future::try_join_all;
use futures::Future;
use segment::data_types::vectors::VectorType;
use segment::types::{PointIdType, WithPayloadInterface, WithVector};
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    CollectionError, CollectionResult, PointRequest, RecommendExample, Record,
};

pub async fn retrieve_points(
    collection: &Collection,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<Record>> {
    collection
        .retrieve(
            PointRequest {
                ids,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Selector(vector_names),
            },
            read_consistency,
            None,
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
) -> CollectionResult<Vec<Record>> {
    match collection_holder {
        CollectionRefHolder::Ref(collection) => {
            retrieve_points(collection, ids, vector_names, read_consistency).await
        }
        CollectionRefHolder::Guard(guard) => {
            retrieve_points(&guard, ids, vector_names, read_consistency).await
        }
    }
}
#[derive(Eq, PartialEq, Hash)]
pub struct PointRef<'a> {
    pub collection_name: Option<&'a String>,
    pub point_id: PointIdType,
}

pub type ReferencedVectors<'coll_name> = HashMap<PointRef<'coll_name>, Record>;

#[derive(Default)]
pub struct ReferencedPoints<'coll_name> {
    ids_per_collection: HashMap<Option<&'coll_name String>, HashSet<PointIdType>>,
    vector_names_per_collection: HashMap<Option<&'coll_name String>, HashSet<String>>,
}

impl<'coll_name> ReferencedPoints<'coll_name> {
    pub fn new() -> Self {
        Self::default()
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
        collection_by_name: F,
    ) -> CollectionResult<ReferencedVectors<'coll_name>>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        debug_assert!(self.ids_per_collection.len() == self.vector_names_per_collection.len());

        let mut collections_names = Vec::new();
        let mut vector_retrieves = Vec::new();
        for (collection_name, reference_vectors_ids) in self.ids_per_collection.into_iter() {
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
        let all_reference_vectors: Vec<Vec<Record>> = try_join_all(vector_retrieves).await?;
        let mut all_vectors_records_map: HashMap<_, _> = Default::default();
        for (collection_name, reference_vectors) in
            collections_names.into_iter().zip(all_reference_vectors)
        {
            for rec in reference_vectors {
                all_vectors_records_map.insert(
                    PointRef {
                        collection_name,
                        point_id: rec.id,
                    },
                    rec,
                );
            }
        }

        Ok(all_vectors_records_map)
    }
}

pub fn convert_to_vectors<'a>(
    examples: impl Iterator<Item = &'a RecommendExample> + 'a,
    all_vectors_records_map: &'a HashMap<PointRef, Record>,
    vector_name: &'a str,
    collection_name: Option<&'a String>,
) -> impl Iterator<Item = &'a VectorType> + 'a {
    examples.filter_map(move |example| match example {
        RecommendExample::Vector(vector) => Some(vector),
        RecommendExample::PointId(vid) => {
            let rec = all_vectors_records_map
                .get(&PointRef {
                    collection_name,
                    point_id: *vid,
                })
                .unwrap();
            rec.get_vector_by_name(vector_name)
        }
    })
}
