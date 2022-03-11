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
use collection_manager::collection_managers::CollectionSearcher;
use config::CollectionConfig;
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
use segment::{
    spaces::tools::peek_top_scores_iterable,
    types::{
        Condition, ExtendedPointId, Filter, HasIdCondition, ScoredPoint, VectorElementType,
        WithPayload, WithPayloadInterface,
    },
};
use shard::{Shard, ShardId};
use tokio::runtime::Handle;

use crate::operations::OperationToShard;

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

type CollectionId = String;

/// Collection's data is split into several shards.
pub struct Collection {
    shards: HashMap<ShardId, Shard>,
    ring: HashRing<ShardId>,
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
        for shard_id in 0..config.params.shard_number {
            let shard_path = shard_path(path, shard_id);
            let shard = create_dir_all(&shard_path)
                .map_err(|err| CollectionError::ServiceError {
                    error: format!("Can't create shard {shard_id} directory. Error: {}", err),
                })
                .and_then(|()| Shard::build(shard_id, id.clone(), &shard_path, config));
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
            shards.insert(shard_id, shard);
            ring.add(shard_id);
        }
        Ok(Self {
            shards,
            ring,
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

        for shard_id in 0..config.params.shard_number {
            let shard_path = shard_path(path, shard_id);
            shards.insert(
                shard_id,
                Shard::load(shard_id, id.clone(), &shard_path, &config).await,
            );
            ring.add(shard_id);
        }
        Self {
            shards,
            ring,
            before_drop_called: false,
        }
    }

    fn try_migrate_legacy_one_shard(collection_path: &Path) -> io::Result<()> {
        if Shard::segments_path(collection_path).is_dir() {
            log::warn!("Migrating legacy collection storage to 1 shard.");
            let shard_path = shard_path(collection_path, 0);
            let new_segmnents_path = Shard::segments_path(&shard_path);
            let new_wal_path = Shard::wal_path(&shard_path);
            create_dir_all(&new_segmnents_path)?;
            create_dir_all(&new_wal_path)?;
            rename(Shard::segments_path(collection_path), &new_segmnents_path)?;
            rename(Shard::wal_path(collection_path), &new_wal_path)?;
            log::info!("Migration finished.");
        }
        Ok(())
    }

    fn shard_by_id(&self, id: ShardId) -> &Shard {
        self.shards
            .get(&id)
            .expect("Shard is guaranteed to be added when id is added to the ring.")
    }

    fn all_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.values()
    }

    pub async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;
        let by_shard = operation.split_by_shard(&self.ring);
        let mut results = Vec::new();
        match by_shard {
            OperationToShard::ByShard(by_shard) => {
                for (shard_id, operation) in by_shard {
                    results.push(self.shard_by_id(shard_id).update(operation, wait).await)
                }
            }
            OperationToShard::ToAll(operation) => {
                for shard in self.all_shards() {
                    results.push(shard.update(operation.clone(), wait).await)
                }
            }
        }
        // At least one result is always present.
        // This is a stub to keep the current API as we have only 1 shard for now.
        results.pop().unwrap()
    }

    pub async fn recommend_by(
        &self,
        request: RecommendRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
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

        self.search(search_request, segment_searcher, search_runtime_handle)
            .await
    }

    pub async fn search(
        &self,
        request: SearchRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let mut points = Vec::new();
        let request = Arc::new(request);
        for shard in self.all_shards() {
            let mut shard_points = segment_searcher
                .search(shard.segments(), request.clone(), search_runtime_handle)
                .await?;
            points.append(&mut shard_points);
        }
        Ok(peek_top_scores_iterable(points, request.top))
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
        segment_searcher: &(dyn CollectionSearcher + Sync),
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

        let mut points = Vec::new();
        for shard in self.all_shards() {
            let mut shard_points = shard
                .scroll_by(
                    segment_searcher,
                    offset,
                    limit,
                    &with_payload_interface,
                    with_vector,
                    request.filter.as_ref(),
                )
                .await?;
            points.append(&mut shard_points);
        }
        points.sort_by_key(|point| point.id);
        let mut points: Vec<_> = points.into_iter().take(limit).collect();
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
    ) -> CollectionResult<Vec<Record>> {
        let with_payload_interface = request
            .with_payload
            .as_ref()
            .unwrap_or(&WithPayloadInterface::Bool(false));
        let with_payload = WithPayload::from(with_payload_interface);
        let with_vector = request.with_vector;

        let mut points = Vec::new();
        for shard in self.all_shards() {
            let mut shard_points = segment_searcher
                .retrieve(shard.segments(), &request.ids, &with_payload, with_vector)
                .await?;
            points.append(&mut shard_points);
        }
        Ok(points)
    }

    /// Updates shard optimization params:
    /// - Saves new params on disk
    /// - Stops existing optimization loop
    /// - Runs new optimizers with new params
    pub async fn update_optimizer_params(
        &self,
        optimizer_config_diff: OptimizersConfigDiff,
    ) -> CollectionResult<()> {
        for shard in self.all_shards() {
            shard
                .update_optimizer_params(optimizer_config_diff.clone())
                .await?;
        }
        Ok(())
    }

    pub async fn info(&self) -> CollectionResult<CollectionInfo> {
        let mut shards = self.all_shards();
        let mut info = shards
            .next()
            .expect("At least 1 shard expected")
            .info()
            .await?;
        for shard in shards {
            let mut shard_info = shard.info().await?;
            info.status = max(info.status, shard_info.status);
            info.optimizer_status = max(info.optimizer_status, shard_info.optimizer_status);
            info.vectors_count += shard_info.vectors_count;
            info.segments_count += shard_info.segments_count;
            info.disk_data_size += shard_info.disk_data_size;
            info.ram_data_size += shard_info.ram_data_size;
            info.payload_schema
                .extend(shard_info.payload_schema.drain());
        }
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
