use crate::collection_state::State;
use crate::config::CollectionConfig;
use crate::hash_ring::HashRing;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::snapshot_ops::{
    get_snapshot_description, list_snapshots_in_directory, SnapshotDescription,
};
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountRequest, CountResult, PointRequest,
    RecommendRequest, Record, ScrollRequest, ScrollResult, SearchRequest, UpdateResult,
};
use crate::operations::{CollectionUpdateOperations, OperationToShard, SplitByShard, Validate};
use crate::optimizers_builder::OptimizersConfig;
use crate::shard::collection_shard_distribution::CollectionShardDistribution;
use crate::shard::local_shard::LocalShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_config::{ShardConfig, ShardType};
use crate::shard::{
    create_shard_dir, shard_path, ChannelService, CollectionId, PeerId, Shard, ShardId,
    ShardOperation, ShardTransfer, HASH_RING_SHARD_SCALE,
};
use crate::telemetry::CollectionTelemetry;
use futures::future::{join_all, try_join_all};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use segment::common::version::StorageVersion;
use segment::spaces::tools::{peek_top_largest_scores_iterable, peek_top_smallest_scores_iterable};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, Order, ScoredPoint, VectorElementType,
    WithPayload, WithPayloadInterface,
};
use semver::{Version, VersionReq};
use std::cmp::max;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tar::Builder as TarBuilder;
use tokio::fs::{copy, create_dir_all, remove_dir_all, remove_file, rename};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

struct CollectionVersion;

impl StorageVersion for CollectionVersion {
    fn current() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Collection's data is split into several shards.
pub struct Collection {
    pub(crate) id: CollectionId,
    pub(crate) shards: HashMap<ShardId, Shard>,
    pub(crate) ring: HashRing<ShardId>,
    config: Arc<RwLock<CollectionConfig>>,
    /// Tracks whether `before_drop` fn has been called.
    before_drop_called: bool,
    path: PathBuf,
    snapshots_path: PathBuf,
    telemetry: CollectionTelemetry,
    shard_transfers: HashMap<ShardId, ShardTransfer>,
}

impl Collection {
    pub fn name(&self) -> String {
        self.id.clone()
    }

    pub async fn new(
        id: CollectionId,
        path: &Path,
        snapshots_path: &Path,
        config: &CollectionConfig,
        shard_distribution: CollectionShardDistribution,
        channel_service: ChannelService,
    ) -> Result<Self, CollectionError> {
        let start_time = std::time::Instant::now();

        CollectionVersion::save(path)?;
        config.save(path)?;
        let mut ring = HashRing::fair(HASH_RING_SHARD_SCALE);
        let mut shards: HashMap<ShardId, Shard> = HashMap::new();

        let shared_config = Arc::new(RwLock::new(config.clone()));
        for shard_id in shard_distribution.local {
            let shard_path = create_shard_dir(path, shard_id).await?;
            let shard =
                LocalShard::build(shard_id, id.clone(), &shard_path, shared_config.clone()).await;
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

        for (shard_id, peer_id) in shard_distribution.remote {
            let shard_path = create_shard_dir(path, shard_id).await?;
            let shard = RemoteShard::init(
                shard_id,
                id.clone(),
                peer_id,
                shard_path,
                channel_service.clone(),
            )?;
            shards.insert(shard_id, Shard::Remote(shard));
            ring.add(shard_id);
        }

        Ok(Self {
            id: id.clone(),
            shards,
            ring,
            config: shared_config,
            before_drop_called: false,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            shard_transfers: Default::default(),
            telemetry: CollectionTelemetry::new(id, config.clone(), start_time.elapsed()),
        })
    }

    pub fn can_upgrade_storage(stored: &Version, app: &Version) -> bool {
        VersionReq {
            comparators: vec![semver::Comparator {
                op: semver::Op::Caret,
                major: stored.major,
                minor: Some(stored.minor),
                patch: Some(stored.patch),
                pre: stored.pre.clone(),
            }],
        }
        .matches(app)
    }

    pub async fn load(
        id: CollectionId,
        path: &Path,
        snapshots_path: &Path,
        channel_service: ChannelService,
    ) -> Self {
        let start_time = std::time::Instant::now();
        let mut stored_version = CollectionVersion::load(path)
            .expect("Can't read collection version")
            .parse()
            .expect("Failed to parse stored collection version as semver");
        let app_version: Version = CollectionVersion::current()
            .parse()
            .expect("Failed to parse current collection version as semver");
        if stored_version != app_version {
            if Self::can_upgrade_storage(&stored_version, &app_version) {
                log::info!("Migrating collection {stored_version} -> {app_version}",);
                CollectionVersion::save(path)
                    .unwrap_or_else(|err| panic!("Can't save collection version {}", err));
                stored_version = app_version;
            } else {
                log::info!("Cannot upgrade version {stored_version} to {app_version}. Attempting to use {stored_version}")
            }
        }

        let config = CollectionConfig::load(path).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                path.to_str().unwrap()
            )
        });

        // 0.4.0 is the collection version at which we introduced fair hash ring
        let mut ring = if stored_version >= Version::new(0, 4, 0) {
            log::debug!("Using fair hash ring with scale: {HASH_RING_SHARD_SCALE}");
            HashRing::fair(HASH_RING_SHARD_SCALE)
        } else {
            log::debug!("Using raw hash ring");
            HashRing::raw()
        };
        let mut shards = HashMap::new();

        let shard_distribution = CollectionShardDistribution::from_local_state(path)
            .expect("Can't infer shard distribution from local shard configurations");

        let shared_config = Arc::new(RwLock::new(config.clone()));
        for shard_id in shard_distribution.local {
            let shard_path = shard_path(path, shard_id);
            shards.insert(
                shard_id,
                Shard::Local(
                    LocalShard::load(shard_id, id.clone(), &shard_path, shared_config.clone())
                        .await,
                ),
            );
            ring.add(shard_id);
        }

        for (shard_id, peer_id) in shard_distribution.remote {
            let shard = RemoteShard::new(shard_id, id.clone(), peer_id, channel_service.clone());
            shards.insert(shard_id, Shard::Remote(shard));
            ring.add(shard_id);
        }

        Self {
            id: id.clone(),
            shards,
            ring,
            config: shared_config,
            before_drop_called: false,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            // TODO: Need to be persisted on disk in case of restart?
            shard_transfers: Default::default(),
            telemetry: CollectionTelemetry::new(id, config, start_time.elapsed()),
        }
    }

    pub fn shard_by_id(&self, id: ShardId) -> Option<&Shard> {
        self.shards.get(&id)
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
            Some(shard @ Shard::Proxy(_)) => Ok(shard),
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

    pub fn all_shards(&self) -> impl Iterator<Item = &Shard> {
        self.shards.values()
    }

    fn send_shard(&mut self, _shard_id: ShardId) {
        todo!()
    }

    pub fn start_shard_transfer(
        &mut self,
        shard_id: ShardId,
        to_peer: PeerId,
        this_peer: PeerId,
    ) -> CollectionResult<()> {
        let shard = self.shard_by_id(shard_id).ok_or_else(|| {
            CollectionError::service_error("Shard {shard_id} is abscent".to_owned())
        })?;
        let from_peer = shard.peer_id(this_peer);
        self.shard_transfers.insert(
            shard_id,
            ShardTransfer {
                from: from_peer,
                to: to_peer,
            },
        );
        if from_peer == this_peer {
            self.send_shard(shard_id);
        }
        Ok(())
    }

    pub fn finish_shard_transfer(&mut self, shard_id: ShardId) -> CollectionResult<()> {
        let transfer = self.shard_transfers.remove(&shard_id).ok_or_else(|| {
            CollectionError::service_error(format!(
                "Shard transfer data for {shard_id} is abscent at the end of the transfer."
            ))
        })?;
        if let Shard::Remote(shard) = self.shards.get_mut(&shard_id).ok_or_else(|| {
            CollectionError::service_error("Shard {shard_id} is absent".to_owned())
        })? {
            shard.peer_id = transfer.to;
        }
        Ok(())
    }

    pub fn abort_shard_transfer(&mut self, shard_id: ShardId) -> CollectionResult<()> {
        self.shard_transfers.remove(&shard_id);
        todo!("Handle for sender and receiver")
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
                .map(|(shard_id, operation)| (self.shard_by_id(shard_id).unwrap().get(), operation))
                .collect(),
            OperationToShard::ToAll(operation) => self
                .all_shards()
                .map(|shard| (shard.get(), operation.clone()))
                .collect(),
        };
        let shard_requests = shard_ops
            .iter()
            .map(move |(shard, operation)| shard.update(operation.clone(), wait));
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
                shard_selection,
            )
            .await?;
        let vectors_map: HashMap<ExtendedPointId, Vec<VectorElementType>> = vectors
            .into_iter()
            .map(|rec| (rec.id, rec.vector.unwrap()))
            .collect();

        for &point_id in &reference_vectors_ids {
            if !vectors_map.contains_key(&point_id) {
                return Err(CollectionError::PointNotFound {
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
            limit: request.limit,
            score_threshold: request.score_threshold,
            offset: request.offset,
        };

        self.search(search_request, search_runtime_handle, shard_selection)
            .await
    }

    async fn _search(
        &self,
        request: SearchRequest,
        search_runtime_handle: &Handle,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let request = Arc::new(request);
        let target_shards = self.target_shards(shard_selection)?;
        let all_searches = target_shards
            .iter()
            .map(|shard| shard.search(request.clone(), search_runtime_handle));

        let all_searches_res = try_join_all(all_searches).await?.into_iter().flatten();
        let distance = self.config.read().await.params.distance;

        let mut top_result = match distance.distance_order() {
            Order::LargeBetter => {
                peek_top_largest_scores_iterable(all_searches_res, request.limit + request.offset)
            }
            Order::SmallBetter => {
                peek_top_smallest_scores_iterable(all_searches_res, request.limit + request.offset)
            }
        };

        if request.offset > 0 {
            // Remove offset from top result.
            top_result.drain(..request.offset);
        }
        Ok(top_result)
    }

    async fn fill_search_result_with_payload(
        &self,
        search_result: Vec<ScoredPoint>,
        with_payload: Option<WithPayloadInterface>,
        with_vector: bool,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let retrieve_request = PointRequest {
            ids: search_result.iter().map(|x| x.id).collect(),
            with_payload,
            with_vector,
        };
        let retrieved_records = self.retrieve(retrieve_request, shard_selection).await?;
        let mut records_map: HashMap<ExtendedPointId, Record> = retrieved_records
            .into_iter()
            .map(|rec| (rec.id, rec))
            .collect();
        let enriched_result = search_result
            .into_iter()
            .filter_map(|mut scored_point| {
                // Points might get deleted between search and retrieve.
                // But it's not a problem, because we don't want to return deleted points.
                // So we just filter out them.
                records_map.remove(&scored_point.id).map(|record| {
                    scored_point.payload = record.payload;
                    scored_point.vector = record.vector;
                    scored_point
                })
            })
            .collect();
        Ok(enriched_result)
    }

    pub async fn search(
        &self,
        request: SearchRequest,
        search_runtime_handle: &Handle,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        // A factor which determines if we need to use the 2-step search or not
        // Should be adjusted based on usage statistics.
        const PAYLOAD_TRANSFERS_FACTOR_THRESHOLD: usize = 10;

        let is_payload_required = if let Some(with_payload) = &request.with_payload {
            with_payload.is_required()
        } else {
            false
        };

        let metadata_required = is_payload_required || request.with_vector;

        // Number of records we need to retrieve to fill the search result.
        let require_transfers = self.shards.len() * (request.limit + request.offset);
        // Actually used number of records.
        let used_transfers = request.limit;

        let is_required_transfer_large_enough =
            require_transfers > used_transfers * PAYLOAD_TRANSFERS_FACTOR_THRESHOLD;

        if metadata_required && is_required_transfer_large_enough {
            // If there is an significant offset, we need to retrieve the whole result
            // set without payload first and then retrieve the payload.
            // It is required to do this because the payload might be too large to send over the
            // network.
            let mut without_payload_request = request.clone();
            without_payload_request.with_payload = None;
            without_payload_request.with_vector = false;
            let without_payload_result = self
                ._search(
                    without_payload_request,
                    search_runtime_handle,
                    shard_selection,
                )
                .await?;
            let filled_result = self
                .fill_search_result_with_payload(
                    without_payload_result,
                    request.with_payload.clone(),
                    request.with_vector,
                    shard_selection,
                )
                .await?;
            Ok(filled_result)
        } else {
            let result = self
                ._search(request, search_runtime_handle, shard_selection)
                .await?;
            Ok(result)
        }
    }

    pub async fn scroll_by(
        &self,
        request: ScrollRequest,
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

    pub async fn count(
        &self,
        request: CountRequest,
        shard_selection: Option<ShardId>,
    ) -> CollectionResult<CountResult> {
        let request = Arc::new(request);
        let target_shards = self.target_shards(shard_selection)?;
        let count_futures = target_shards
            .iter()
            .map(|shard| shard.count(request.clone()));
        let counts: Vec<_> = try_join_all(count_futures).await?.into_iter().collect();

        let total_count = counts.iter().map(|x| x.count).sum::<usize>();
        let aggregated_count = CountResult { count: total_count };
        Ok(aggregated_count)
    }

    pub async fn retrieve(
        &self,
        request: PointRequest,
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
        let retrieve_futures = target_shards
            .iter()
            .map(|shard| shard.retrieve(request.clone(), &with_payload, with_vector));

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
        {
            let mut config = self.config.write().await;
            config.optimizer_config =
                DiffConfig::update(optimizer_config_diff, &config.optimizer_config)?;
        }
        for shard in self.all_shards() {
            match shard {
                Shard::Local(shard) => shard.on_optimizer_config_update().await?,
                Shard::Proxy(shard) => shard.on_optimizer_config_update().await?,
                Shard::Remote(_) => {} // Do nothing for remote shards
            }
        }
        self.config.read().await.save(&self.path)?;
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
        {
            let mut config = self.config.write().await;
            config.optimizer_config = optimizer_config;
        }
        for shard in self.all_shards() {
            match shard {
                Shard::Local(shard) => shard.on_optimizer_config_update().await?,
                Shard::Remote(_) => {} // Do nothing for remote shards
                Shard::Proxy(proxy) => proxy.on_optimizer_config_update().await?,
            }
        }
        self.config.read().await.save(&self.path)?;
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
                info.points_count += shard_info.points_count;
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

    pub async fn state(&self, this_peer_id: PeerId) -> State {
        State {
            config: self.config.read().await.clone(),
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
        collection_path: &Path,
        channel_service: ChannelService,
    ) -> CollectionResult<()> {
        state
            .apply(this_peer_id, self, collection_path, channel_service)
            .await
    }

    pub async fn get_telemetry_data(&self) -> Option<CollectionTelemetry> {
        let info = self.info(None).await.ok()?;
        let mut telemetry = self.telemetry.clone();
        telemetry.config = info.config;
        telemetry.status = info.status;
        telemetry.optimizer_status = info.optimizer_status;
        telemetry.vectors_count = info.vectors_count;
        telemetry.segments_count = info.segments_count;
        telemetry.disk_data_size = info.disk_data_size;
        telemetry.ram_data_size = info.ram_data_size;
        Some(telemetry)
    }

    pub async fn list_snapshots(&self) -> CollectionResult<Vec<SnapshotDescription>> {
        list_snapshots_in_directory(&self.snapshots_path).await
    }

    pub async fn get_snapshot_path(&self, snapshot_name: &str) -> CollectionResult<PathBuf> {
        let snapshot_path = self.snapshots_path.join(snapshot_name);
        if !snapshot_path.exists() {
            return Err(CollectionError::NotFound {
                what: format!("Snapshot {}", snapshot_name),
            });
        }
        Ok(snapshot_path)
    }

    pub async fn create_snapshot(&self, temp_dir: &Path) -> CollectionResult<SnapshotDescription> {
        let snapshot_name = format!(
            "{}-{}.snapshot",
            self.name(),
            chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S")
        );
        let snapshot_path = self.snapshots_path.join(&snapshot_name);

        let snapshot_path_tmp = snapshot_path.with_extension("tmp");

        let snapshot_path_with_tmp_extension = temp_dir.join(&snapshot_name).with_extension("tmp");
        let snapshot_path_with_arc_extension = temp_dir.join(snapshot_name).with_extension("arc");

        create_dir_all(&snapshot_path_with_tmp_extension).await?;

        // Create snapshot of each shard
        for (shard_id, shard) in self.shards.iter() {
            let shard_snapshot_path =
                snapshot_path_with_tmp_extension.join(format!("{}", shard_id));
            create_dir_all(&shard_snapshot_path).await?;
            match shard {
                Shard::Local(local_shard) => {
                    local_shard.create_snapshot(&shard_snapshot_path).await?;
                }
                Shard::Proxy(proxy_shard) => {
                    proxy_shard.create_snapshot(&shard_snapshot_path).await?;
                }
                Shard::Remote(remote_shard) => {
                    // copy shard directory to snapshot directory
                    remote_shard.create_snapshot(&shard_snapshot_path).await?;
                }
            }
        }

        CollectionVersion::save(&snapshot_path_with_tmp_extension)?;
        self.config
            .read()
            .await
            .save(&snapshot_path_with_tmp_extension)?;

        // have to use std here, cause TarBuilder is not async
        let file = std::fs::File::create(&snapshot_path_with_arc_extension)?;
        let mut builder = TarBuilder::new(file);
        // archive recursively collection directory `snapshot_path_with_arc_extension` into `snapshot_path`
        builder.append_dir_all(".", &snapshot_path_with_tmp_extension)?;
        builder.finish()?;

        // remove temporary snapshot directory
        remove_dir_all(&snapshot_path_with_tmp_extension).await?;

        // move snapshot to permanent location
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, cause copy is not atomic.
        copy(&snapshot_path_with_arc_extension, &snapshot_path_tmp).await?;
        rename(&snapshot_path_tmp, &snapshot_path).await?;
        remove_file(snapshot_path_with_arc_extension).await?;

        get_snapshot_description(&snapshot_path).await
    }

    pub fn restore_snapshot(snapshot_path: &Path, target_dir: &Path) -> CollectionResult<()> {
        // decompress archive
        let archive_file = std::fs::File::open(snapshot_path).unwrap();
        let mut ar = tar::Archive::new(archive_file);
        ar.unpack(target_dir)?;

        let config = CollectionConfig::load(target_dir).unwrap_or_else(|err| {
            panic!(
                "Can't read collection config due to {}\nat {}",
                err,
                target_dir.to_str().unwrap()
            )
        });
        let configured_shards = config.params.shard_number.get();

        for shard_id in 0..configured_shards {
            let shard_path = shard_path(target_dir, shard_id);
            let shard_config = ShardConfig::load(&shard_path)?;
            match shard_config.r#type {
                ShardType::Local => LocalShard::restore_snapshot(&shard_path)?,
                ShardType::Remote { .. } => RemoteShard::restore_snapshot(&shard_path),
            }
        }

        Ok(())
    }
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

fn avg_vectors<'a>(
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
