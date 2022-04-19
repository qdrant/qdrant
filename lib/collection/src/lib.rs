//! Crate, which implements all functions required for operations with a single collection

use std::{
    cmp::max,
    collections::HashMap,
    fs::{create_dir_all, rename},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::operations::types::PointRequest;
use crate::operations::OperationToShard;
use crate::shard::ShardOperation;
use collection_manager::collection_managers::CollectionSearcher;
use config::CollectionConfig;
use futures::future::{join_all, try_join_all};
use futures::{stream::futures_unordered::FuturesUnordered, StreamExt};
use hashring::HashRing;
use itertools::Itertools;
use operations::{
    config_diff::OptimizersConfigDiff,
    types::{
        CollectionError, CollectionInfo, CollectionResult, RecommendRequest, Record, ScrollRequest,
        ScrollResult, SearchRequest, UpdateResult,
    },
    CollectionUpdateOperations, SplitByShard, Validate,
};
use optimizers_builder::OptimizersConfig;
use segment::{
    spaces::tools::peek_top_scores_iterable,
    types::{
        Condition, ExtendedPointId, Filter, HasIdCondition, ScoredPoint, VectorElementType,
        WithPayload, WithPayloadInterface,
    },
};
use serde::{Deserialize, Serialize};
use shard::{local_shard::LocalShard, Shard, ShardId};
use tokio::runtime::Handle;

pub mod collection_manager;
mod common;
pub mod config;
pub mod operations;
pub mod optimizers_builder;
pub mod shard;
mod update_handler;
mod wal;

#[cfg(test)]
mod tests;

pub type CollectionId = String;

pub type PeerId = u32;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct State {
    pub config: CollectionConfig,
    pub shard_to_peer: HashMap<ShardId, PeerId>,
}

impl State {
    pub async fn apply(
        self,
        this_peer_id: PeerId,
        collection: &mut Collection,
    ) -> CollectionResult<()> {
        Self::apply_config(self.config, collection).await?;
        Self::apply_shard_to_peer(self.shard_to_peer, this_peer_id, collection);
        Ok(())
    }

    async fn apply_config(
        config: CollectionConfig,
        collection: &mut Collection,
    ) -> CollectionResult<()> {
        log::warn!("Applying only optimizers config snapshot. Other config updates are not yet implemented.");
        collection
            .update_optimizer_params(config.optimizer_config)
            .await
    }

    fn apply_shard_to_peer(
        shard_to_peer: HashMap<ShardId, PeerId>,
        this_peer_id: PeerId,
        collection: &mut Collection,
    ) {
        for (shard_id, peer_id) in shard_to_peer {
            match collection.shards.get(&shard_id) {
                Some(shard) => {
                    if shard.peer_id(this_peer_id) != peer_id {
                        log::warn!("Shard movement between peers is not yet implemented. Failed to move shard {shard_id} to peer {peer_id}")
                    }
                }
                None => log::warn!(
                    "Shard addition is not yet implemented. Failed to add shard {shard_id}"
                ),
            }
        }
    }
}

/// Collection's data is split into several shards.
pub struct Collection {
    shards: HashMap<ShardId, Shard>,
    ring: HashRing<ShardId>,
    config: CollectionConfig,
    /// Tracks whether `before_drop` fn has been called.
    before_drop_called: bool,
}

impl Collection {
    pub async fn new(
        id: CollectionId,
        path: &Path,
        config: &CollectionConfig,
    ) -> Result<Self, CollectionError> {
        config.save(path)?;
        let mut ring = HashRing::new();
        let mut shards: HashMap<ShardId, Shard> = HashMap::new();
        for shard_id in 0..config.params.shard_number.get() {
            let shard_path = shard_path(path, shard_id);
            let shard = create_dir_all(&shard_path)
                .map_err(|err| CollectionError::ServiceError {
                    error: format!("Can't create shard {shard_id} directory. Error: {}", err),
                })
                .and_then(|()| LocalShard::build(shard_id, id.clone(), &shard_path, config));
            let shard = match shard {
                Ok(shard) => shard,
                Err(err) => {
                    let futures: FuturesUnordered<_> = shards
                        .iter_mut()
                        .map(|(_, shard)| shard.before_drop())
                        .collect();
                    futures.collect::<Vec<()>>().await;
                    return Err(err);
                }
            };
            shards.insert(shard_id, Shard::Local(shard));
            ring.add(shard_id);
        }
        Ok(Self {
            shards,
            ring,
            config: config.clone(),
            before_drop_called: false,
        })
    }

    pub async fn load(id: CollectionId, path: &Path) -> Self {
        let config = CollectionConfig::load(path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                path.to_str().unwrap()
            )
        });
        let mut ring = HashRing::new();
        let mut shards = HashMap::new();

        Self::try_migrate_legacy_one_shard(path)
            .expect("Failed to migrate legacy collection format.");

        for shard_id in 0..config.params.shard_number.get() {
            let shard_path = shard_path(path, shard_id);
            shards.insert(
                shard_id,
                Shard::Local(LocalShard::load(shard_id, id.clone(), &shard_path, &config).await),
            );
            ring.add(shard_id);
        }
        Self {
            shards,
            ring,
            config,
            before_drop_called: false,
        }
    }

    fn try_migrate_legacy_one_shard(collection_path: &Path) -> io::Result<()> {
        if LocalShard::segments_path(collection_path).is_dir() {
            log::warn!("Migrating legacy collection storage to 1 shard.");
            let shard_path = shard_path(collection_path, 0);
            let new_segmnents_path = LocalShard::segments_path(&shard_path);
            let new_wal_path = LocalShard::wal_path(&shard_path);
            create_dir_all(&new_segmnents_path)?;
            create_dir_all(&new_wal_path)?;
            rename(
                LocalShard::segments_path(collection_path),
                &new_segmnents_path,
            )?;
            rename(LocalShard::wal_path(collection_path), &new_wal_path)?;
            log::info!("Migration finished.");
        }
        Ok(())
    }

    fn shard_by_id(&self, id: ShardId) -> &Shard {
        self.shards
            .get(&id)
            .expect("Shard is guaranteed to be added when id is added to the ring.")
    }

    fn local_shard_by_id(&self, id: ShardId) -> CollectionResult<&Shard> {
        match self.shards.get(&id) {
            None => Err(CollectionError::bad_shard_selection(format!(
                "Shard {} does not exist",
                id
            ))),
            Some(Shard::Remote(_)) => Err(CollectionError::bad_shard_selection(format!(
                "Shard {} is not local on peer",
                id
            ))),
            Some(shard @ Shard::Local(_)) => Ok(shard),
        }
    }

    fn target_shards(
        &self,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<Arc<dyn ShardOperation + Sync + Send + '_>>> {
        match shard_selection {
            None => Ok(self.all_shards().map(|shard| shard.get()).collect()),
            Some(shard_selection) => {
                let local_shard = self.local_shard_by_id(shard_selection)?;
                Ok(vec![local_shard.get()])
            }
        }
    }

    fn all_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.values()
    }

    pub async fn update_from_peer(
        &self,
        operation: CollectionUpdateOperations,
        shard_selection: ShardId,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let local_shard = self.local_shard_by_id(shard_selection)?;
        local_shard.get().update(operation.clone(), wait).await
    }

    pub async fn update_from_client(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;
        let shard_ops: Vec<_> = match operation.split_by_shard(&self.ring) {
            OperationToShard::ByShard(by_shard) => by_shard
                .into_iter()
                .map(|(shard_id, operation)| (self.shard_by_id(shard_id).get(), operation))
                .collect(),
            OperationToShard::ToAll(operation) => self
                .all_shards()
                .map(|shard| (shard.get(), operation.clone()))
                .collect(),
        };
        let shard_requests = shard_ops
            .iter()
            .map(|(shard, operation)| shard.update(operation.clone(), wait));
        let mut results = join_all(shard_requests).await;
        let with_error = results
            .iter()
            .filter(|result| matches!(result, Err(_)))
            .count();

        if with_error > 0 {
            let err = results
                .into_iter()
                .find(|result| matches!(result, Err(_)))
                .unwrap();
            if with_error < self.shards.len() {
                err.map_err(|err| CollectionError::InconsistentFailure {
                    shards_total: self.shards.len() as u32,
                    shards_failed: with_error as u32,
                    first_err: format!("{err}"),
                })
            } else {
                err
            }
        } else {
            // At least one result is always present.
            results.pop().unwrap()
        }
    }

    pub async fn recommend_by(
        &self,
        request: RecommendRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        if request.positive.is_empty() {
            return Err(CollectionError::BadRequest {
                description: "At least one positive vector ID required".to_owned(),
            });
        }

        let reference_vectors_ids = request
            .positive
            .iter()
            .chain(&request.negative)
            .cloned()
            .collect_vec();

        let vectors = self
            .retrieve(
                PointRequest {
                    ids: reference_vectors_ids.clone(),
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: true,
                },
                segment_searcher,
                shard_selection,
            )
            .await?;
        let vectors_map: HashMap<ExtendedPointId, Vec<VectorElementType>> = vectors
            .into_iter()
            .map(|rec| (rec.id, rec.vector.unwrap()))
            .collect();

        for &point_id in &reference_vectors_ids {
            if !vectors_map.contains_key(&point_id) {
                return Err(CollectionError::NotFound {
                    missed_point_id: point_id,
                });
            }
        }

        let avg_positive = avg_vectors(
            request
                .positive
                .iter()
                .map(|vid| vectors_map.get(vid).unwrap()),
        );

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = avg_vectors(
                request
                    .negative
                    .iter()
                    .map(|vid| vectors_map.get(vid).unwrap()),
            );

            avg_positive
                .iter()
                .cloned()
                .zip(avg_negative.iter().cloned())
                .map(|(pos, neg)| pos + pos - neg)
                .collect()
        };

        let search_request = SearchRequest {
            vector: search_vector,
            filter: Some(Filter {
                should: None,
                must: request
                    .filter
                    .clone()
                    .map(|filter| vec![Condition::Filter(filter)]),
                must_not: Some(vec![Condition::HasId(HasIdCondition {
                    has_id: reference_vectors_ids.iter().cloned().collect(),
                })]),
            }),
            with_payload: request.with_payload.clone(),
            with_vector: request.with_vector,
            params: request.params,
            top: request.top,
        };

        self.search(
            search_request,
            segment_searcher,
            search_runtime_handle,
            shard_selection,
        )
        .await
    }

    pub async fn search(
        &self,
        request: SearchRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let request = Arc::new(request);
        let target_shards = self.target_shards(shard_selection)?;
        let all_searches = target_shards
            .iter()
            .map(|shard| shard.search(request.clone(), segment_searcher, search_runtime_handle));

        let all_search_results = try_join_all(all_searches).await?;
        Ok(peek_top_scores_iterable(
            all_search_results.into_iter().flatten(),
            request.top,
        ))
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<ScrollResult> {
        let default_request = ScrollRequest::default();

        let offset = request.offset;
        let limit = request
            .limit
            .unwrap_or_else(|| default_request.limit.unwrap());
        let with_payload_interface = request
            .with_payload
            .clone()
            .unwrap_or_else(|| default_request.with_payload.clone().unwrap());
        let with_vector = request.with_vector;

        if limit == 0 {
            return Err(CollectionError::BadRequest {
                description: "Limit cannot be 0".to_string(),
            });
        }

        // Needed to return next page offset.
        let limit = limit + 1;

        let target_shards = self.target_shards(shard_selection)?;
        let scroll_futures = target_shards.iter().map(|shard| {
            shard.scroll_by(
                segment_searcher,
                offset,
                limit,
                &with_payload_interface,
                with_vector,
                request.filter.as_ref(),
            )
        });

        let mut points: Vec<_> = try_join_all(scroll_futures)
            .await?
            .into_iter()
            .flatten()
            .sorted_by_key(|point| point.id)
            .take(limit)
            .collect();

        let next_page_offset = if points.len() < limit {
            // This was the last page
            None
        } else {
            // remove extra point, it would be a first point of the next page
            Some(points.pop().unwrap().id)
        };
        Ok(ScrollResult {
            points,
            next_page_offset,
        })
    }

    pub async fn retrieve(
        &self,
        request: PointRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<Record>> {
        let with_payload_interface = request
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));
        let with_payload = WithPayload::from(with_payload_interface);
        let with_vector = request.with_vector;
        let request = Arc::new(request);
        let target_shards = self.target_shards(shard_selection)?;
        let retrieve_futures = target_shards.iter().map(|shard| {
            shard.retrieve(
                request.clone(),
                segment_searcher,
                &with_payload,
                with_vector,
            )
        });

        let all_shard_collection_results = try_join_all(retrieve_futures).await?;
        let points = all_shard_collection_results.into_iter().flatten().collect();
        Ok(points)
    }

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params_from_diff(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        for shard in self.all_shards() {
            if let Shard::Local(shard) = shard {
                shard
                    .update_optimizer_with_diff(optimizer_config_diff.clone())
                    .await?;
            }
        }
        Ok(())
    }

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params(
        &self,
        optimizer_config: OptimizersConfig,
    ) -> CollectionResult<()> {
        for shard in self.all_shards() {
            if let Shard::Local(shard) = shard {
                shard
                    .update_optimizer_with_config(optimizer_config.clone())
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn info(&self, shard_selection: Option<ShardId>) -> CollectionResult<CollectionInfo> {
        let target_shards = self.target_shards(shard_selection)?;
        let first_shard = target_shards
            .first()
            .ok_or_else(|| CollectionError::ServiceError {
                error: "There are no shards for selected collection".to_string(),
            })?;

        let mut info = first_shard.info().await?;
        let info_futures = target_shards.iter().skip(1).map(|shard| shard.info());

        let all_shard_collection_results = try_join_all(info_futures).await?;
        all_shard_collection_results
            .into_iter()
            .for_each(|mut shard_info| {
                info.status = max(info.status, shard_info.status);
                info.optimizer_status =
                    max(info.optimizer_status.clone(), shard_info.optimizer_status);
                info.vectors_count += shard_info.vectors_count;
                info.segments_count += shard_info.segments_count;
                info.disk_data_size += shard_info.disk_data_size;
                info.ram_data_size += shard_info.ram_data_size;
                info.payload_schema
                    .extend(shard_info.payload_schema.drain());
            });
        Ok(info)
    }

    pub async fn before_drop(&mut self) {
        let futures: FuturesUnordered<_> = self
            .shards
            .iter_mut()
            .map(|(_, shard)| shard.before_drop())
            .collect();
        futures.collect::<Vec<()>>().await;
        self.before_drop_called = true
    }

    pub fn state(&self, this_peer_id: PeerId) -> State {
        State {
            config: self.config.clone(),
            shard_to_peer: self
                .shards
                .iter()
                .map(|(shard_id, shard)| (*shard_id, shard.peer_id(this_peer_id)))
                .collect(),
        }
    }

    pub async fn apply_state(
        &mut self,
        state: State,
        this_peer_id: PeerId,
    ) -> CollectionResult<()> {
        state.apply(this_peer_id, self).await
    }
}

pub fn shard_path(collection_path: &Path, shard_id: ShardId) -> PathBuf {
    collection_path.join(format!("{shard_id}"))
}

pub fn avg_vectors<'a>(
    vectors: impl Iterator<Item = &'a Vec<VectorElementType>>,
) -> Vec<VectorElementType> {
    let mut count: usize = 0;
    let mut avg_vector: Vec<VectorElementType> = vec![];
    for vector in vectors {
        count += 1;
        for i in 0..vector.len() {
            if i >= avg_vector.len() {
                avg_vector.push(vector[i])
            } else {
                avg_vector[i] += vector[i];
            }
        }
    }

    for item in &mut avg_vector {
        *item /= count as VectorElementType;
    }

    avg_vector
}

impl Drop for Collection {
    fn drop(&mut self) {
        if !self.before_drop_called {
            // Panic is used to get fast feedback in unit and integration tests
            // in cases where `before_drop` was not added.
            if cfg!(test) {
                panic!("Collection `before_drop` was not called.")
            } else {
                log::error!("Collection `before_drop` was not called.")
            }
        }
    }
}
