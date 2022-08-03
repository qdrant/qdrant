use std::cmp::max;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use segment::common::version::StorageVersion;
use segment::spaces::tools::{peek_top_largest_scores_iterable, peek_top_smallest_scores_iterable};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, Order, ScoredPoint, VectorElementType,
    WithPayload, WithPayloadInterface,
};
use semver::{Version, VersionReq};
use tar::Builder as TarBuilder;
use tokio::fs::{copy, create_dir_all, remove_dir_all, remove_file, rename};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use crate::collection_state::State;
use crate::config::CollectionConfig;
use crate::hash_ring::HashRing;
use crate::operations::config_diff::{DiffConfig, OptimizersConfigDiff};
use crate::operations::snapshot_ops::{
    get_snapshot_description, list_snapshots_in_directory, SnapshotDescription,
};
use crate::operations::types::{
    CollectionClusterInfo, CollectionError, CollectionInfo, CollectionResult, CountRequest,
    CountResult, LocalShardInfo, PointRequest, RecommendRequest, Record, RemoteShardInfo,
    ScrollRequest, ScrollResult, SearchRequest, ShardTransferInfo, UpdateResult,
};
use crate::operations::{CollectionUpdateOperations, Validate};
use crate::optimizers_builder::OptimizersConfig;
use crate::shard::collection_shard_distribution::CollectionShardDistribution;
use crate::shard::local_shard::LocalShard;
use crate::shard::remote_shard::RemoteShard;
use crate::shard::shard_config::{ShardConfig, ShardType};
use crate::shard::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shard::shard_versioning::versioned_shard_path;
use crate::shard::transfer::shard_transfer::{
    change_remote_shard_route, drop_temporary_shard, promote_proxy_to_remote_shard,
    promote_temporary_shard_to_local, revert_proxy_shard_to_local, spawn_transfer_task,
};
use crate::shard::transfer::transfer_tasks_pool::{TaskResult, TransferTasksPool};
use crate::shard::{
    create_shard_dir, ChannelService, CollectionId, PeerId, Shard, ShardId, ShardOperation,
    ShardTransfer, HASH_RING_SHARD_SCALE,
};
use crate::telemetry::CollectionTelemetry;

struct CollectionVersion;

impl StorageVersion for CollectionVersion {
    fn current() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Collection's data is split into several shards.
pub struct Collection {
    pub(crate) id: CollectionId,
    pub(crate) shards_holder: Arc<LockedShardHolder>,
    config: Arc<RwLock<CollectionConfig>>,
    /// Tracks whether `before_drop` fn has been called.
    before_drop_called: bool,
    path: PathBuf,
    snapshots_path: PathBuf,
    telemetry: CollectionTelemetry,
    channel_service: ChannelService,
    transfer_tasks: Mutex<TransferTasksPool>,
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

        let mut shard_holder = ShardHolder::new(path, HashRing::fair(HASH_RING_SHARD_SCALE))?;

        let shared_config = Arc::new(RwLock::new(config.clone()));
        for shard_id in shard_distribution.local {
            let shard_path = create_shard_dir(path, shard_id).await;
            let shard = match shard_path {
                Ok(shard_path) => {
                    LocalShard::build(shard_id, id.clone(), &shard_path, shared_config.clone())
                        .await
                }
                Err(e) => Err(e),
            };

            let shard = match shard {
                Ok(shard) => shard,
                Err(err) => {
                    shard_holder.before_drop().await;
                    return Err(err);
                }
            };
            shard_holder.add_shard(shard_id, Shard::Local(shard));
        }

        for (shard_id, peer_id) in shard_distribution.remote {
            let shard = create_shard_dir(path, shard_id)
                .await
                .and_then(|shard_path| {
                    RemoteShard::init(
                        shard_id,
                        id.clone(),
                        peer_id,
                        shard_path,
                        channel_service.clone(),
                    )
                });
            let shard = match shard {
                Ok(shard) => shard,
                Err(err) => {
                    shard_holder.before_drop().await;
                    return Err(err);
                }
            };
            shard_holder.add_shard(shard_id, Shard::Remote(shard));
        }

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        Ok(Self {
            id: id.clone(),
            shards_holder: locked_shard_holder,
            config: shared_config,
            before_drop_called: false,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            telemetry: CollectionTelemetry::new(id, config.clone(), start_time.elapsed()),
            channel_service,
            transfer_tasks: Default::default(),
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
        collection_id: CollectionId,
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
        let ring = if stored_version >= Version::new(0, 4, 0) {
            log::debug!("Using fair hash ring with scale: {HASH_RING_SHARD_SCALE}");
            HashRing::fair(HASH_RING_SHARD_SCALE)
        } else {
            log::debug!("Using raw hash ring");
            HashRing::raw()
        };
        let mut shard_holder = ShardHolder::new(path, ring).expect("Can not create shard holder");

        let shared_config = Arc::new(RwLock::new(config.clone()));

        shard_holder
            .load_shards(
                path,
                &collection_id,
                shared_config.clone(),
                channel_service.clone(),
            )
            .await;

        let locked_shard_holder = Arc::new(LockedShardHolder::new(shard_holder));

        Self {
            id: collection_id.clone(),
            shards_holder: locked_shard_holder,
            config: shared_config,
            before_drop_called: false,
            path: path.to_owned(),
            snapshots_path: snapshots_path.to_owned(),
            telemetry: CollectionTelemetry::new(collection_id, config, start_time.elapsed()),
            channel_service,
            transfer_tasks: Mutex::new(TransferTasksPool::default()),
        }
    }

    pub async fn contains_shard(&self, shard_id: &ShardId) -> bool {
        let shard_holder_read = self.shards_holder.read().await;
        shard_holder_read.contains_shard(shard_id)
    }

    /// Returns true if shard it explicitly local, false otherwise.
    pub async fn is_shard_local(&self, shard_id: &ShardId) -> Option<bool> {
        let shard_holder_read = self.shards_holder.read().await;
        shard_holder_read
            .get_shard(shard_id)
            .map(|shard| matches!(shard, Shard::Local(_)))
    }

    async fn send_shard<OF, OE>(&self, transfer: ShardTransfer, on_finish: OF, on_error: OE)
    where
        OF: Future<Output = ()> + Send + 'static,
        OE: Future<Output = ()> + Send + 'static,
    {
        let mut active_transfer_tasks = self.transfer_tasks.lock().await;
        let task_result = active_transfer_tasks.stop_if_exists(&transfer).await;

        debug_assert_eq!(task_result, TaskResult::NotFound);

        let shard_holder = self.shards_holder.clone();
        let collection_id = self.id.clone();
        let channel_service = self.channel_service.clone();

        let transfer_task = spawn_transfer_task(
            shard_holder,
            transfer.clone(),
            collection_id,
            channel_service,
            on_finish,
            on_error,
        );

        active_transfer_tasks.add_task(&transfer, transfer_task);
    }

    pub async fn start_shard_transfer<T, F>(
        &self,
        shard_transfer: ShardTransfer,
        on_finish: T,
        on_error: F,
    ) -> CollectionResult<bool>
    where
        T: Future<Output = ()> + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        let shard_id = shard_transfer.shard_id;
        let do_transfer = {
            let mut shards_holder = self.shards_holder.write().await;
            let was_not_transferred =
                shards_holder.register_start_shard_transfer(shard_transfer.clone())?;
            let shard = shards_holder.get_shard(&shard_id);

            // Check if current node owns the shard which should be transferred
            match shard {
                None => {
                    return Err(CollectionError::service_error(format!(
                        "Shard {shard_id} doesn't exist"
                    )))
                }
                Some(shard) => match shard {
                    Shard::Local(_) => {
                        debug_assert!(was_not_transferred);
                        true
                    }
                    Shard::Remote(_) => false, // Shard is on some other peer
                    Shard::Proxy(_) => {
                        debug_assert!(!was_not_transferred);
                        false // Shard if already in transferring state
                    }
                    Shard::ForwardProxy(_) => {
                        debug_assert!(!was_not_transferred);
                        false // Shard if already in transferring state
                    }
                },
            }
        };
        if do_transfer {
            self.send_shard(shard_transfer, on_finish, on_error).await;
        }
        Ok(do_transfer)
    }

    /// Handles finishing of the shard transfer.
    ///
    /// 1. Removes transfer state from list of active transfers.
    /// 2. Awaits for related transfer task to finish.
    /// 3. Converts proxy shard -> remote shard
    /// 4. Promotes temporary shard to local shard.
    /// 5. Point remote shard to new location
    ///
    /// Returns true if state was changed, false otherwise.
    pub async fn finish_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<bool> {
        let finish_was_registered = self
            .shards_holder
            .write()
            .await
            .register_finish_transfer(&transfer)?;
        let transfer_finished = self
            .transfer_tasks
            .lock()
            .await
            .stop_if_exists(&transfer)
            .await
            .is_finished();

        // Should happen on transfer side
        let proxy_promoted = promote_proxy_to_remote_shard(
            &self.path,
            self.shards_holder.clone(),
            transfer.shard_id,
            transfer.to,
        )
        .await?;
        // Should happen on receiving side
        let shard_promoted = promote_temporary_shard_to_local(
            self.id.clone(),
            &self.path,
            self.shards_holder.clone(),
            transfer.shard_id,
        )
        .await?;
        // Should happen on a third-party side
        let remote_shard_rerouted = change_remote_shard_route(
            &self.path,
            self.shards_holder.clone(),
            transfer.shard_id,
            transfer.to,
        )
        .await?;

        // Transfer task and proxy should exist on the same node
        debug_assert_eq!(transfer_finished, proxy_promoted);
        log::debug!("finish_was_registered: {}", finish_was_registered);
        log::debug!("transfer_finished: {}", transfer_finished);
        log::debug!("proxy_promoted: {}", proxy_promoted);
        log::debug!("shard_promoted: {}", shard_promoted);
        log::debug!("remote_shard_rerouted: {}", remote_shard_rerouted);

        let something_changed =
            finish_was_registered && (proxy_promoted || shard_promoted || remote_shard_rerouted);

        Ok(something_changed)
    }

    /// Handles abort of the transfer
    ///
    /// 1. Unregister the transfer
    /// 2. Stop transfer task
    /// 3. Unwrap the proxy
    /// 4. Remove temp shard
    pub async fn abort_shard_transfer(&self, transfer: ShardTransfer) -> CollectionResult<bool> {
        let finish_was_registered = self
            .shards_holder
            .write()
            .await
            .register_finish_transfer(&transfer)?;
        let transfer_finished = self
            .transfer_tasks
            .lock()
            .await
            .stop_if_exists(&transfer)
            .await
            .is_finished();

        let proxy_unwrapped =
            revert_proxy_shard_to_local(self.shards_holder.clone(), transfer.shard_id).await?;

        let temp_shard_removed =
            drop_temporary_shard(self.shards_holder.clone(), transfer.shard_id).await?;

        let changed_something =
            finish_was_registered && (transfer_finished || proxy_unwrapped || temp_shard_removed);

        Ok(changed_something)
    }

    /// Initiate temporary shard
    ///
    /// Drops existing temporary shards for `shard_id`.
    pub async fn initiate_temporary_shard(&self, shard_id: ShardId) -> CollectionResult<()> {
        let mut shard_holder_write = self.shards_holder.write().await;
        let existing_shard = shard_holder_write.remove_temporary_shard(shard_id);
        // do not lock shards while creating temporary shard  disk
        drop(shard_holder_write);

        if let Some(mut existing_temporary_shard) = existing_shard {
            log::info!(
                "A temporary shard is already present for {}:{} - its content will be deleted",
                self.id,
                shard_id
            );
            // Finish update tasks
            existing_temporary_shard.before_drop().await;

            match existing_temporary_shard {
                Shard::Local(local_shard) => {
                    let old_shard_path = local_shard.shard_path();
                    // Delete existing folder
                    drop(local_shard);
                    log::debug!("Deleting old shard {}", old_shard_path.display());
                    tokio::fs::remove_dir_all(&old_shard_path).await?;
                }
                Shard::Remote(_) => {
                    debug_assert!(false, "Remote shard should not be temporary");
                }
                Shard::Proxy(_) => {
                    debug_assert!(false, "Proxy shard should not be temporary");
                }
                Shard::ForwardProxy(_) => {
                    debug_assert!(false, "Proxy shard should not be temporary");
                }
            }
        }

        let temporary_shard_path = create_shard_dir(&self.path, shard_id).await?;

        // create directory to hold the temporary data for shard with `shard_id`
        tokio::fs::create_dir_all(&temporary_shard_path).await?;
        let temporary_shard = LocalShard::build_temp(
            shard_id,
            self.id.clone(),
            &temporary_shard_path,
            self.config.clone(),
        )
        .await?;

        // register temporary shard
        let mut shards_holder_write = self.shards_holder.write().await;
        shards_holder_write.add_temporary_shard(shard_id, temporary_shard);
        Ok(())
    }

    /// Handle collection updates from peers.
    ///
    /// Shard transfer aware.
    pub async fn update_from_peer(
        &self,
        operation: CollectionUpdateOperations,
        shard_selection: ShardId,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let shard_holder_guard = self.shards_holder.read().await;

        let target_shards = shard_holder_guard.target_shards(Some(shard_selection))?;
        let mut res = None;
        for target_shard in target_shards {
            res = Some(target_shard.get().update(operation.clone(), wait).await?);
        }
        if let Some(res) = res {
            Ok(res)
        } else {
            Err(CollectionError::service_error(format!(
                "No target shard {} found for update",
                shard_selection
            )))
        }
    }

    pub async fn update_from_client(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        operation.validate()?;

        let mut results = {
            let shards_holder = self.shards_holder.read().await;
            let shard_to_op = shards_holder.split_by_shard(operation);

            let shard_requests = shard_to_op
                .into_iter()
                .map(move |(shard, operation)| shard.get().update(operation, wait));
            join_all(shard_requests).await
        };

        let with_error = results
            .iter()
            .filter(|result| matches!(result, Err(_)))
            .count();

        if with_error > 0 {
            let err = results
                .into_iter()
                .find(|result| matches!(result, Err(_)))
                .unwrap();
            let num_shards = self.shards_holder.read().await.len();
            if with_error < num_shards {
                err.map_err(|err| CollectionError::InconsistentFailure {
                    shards_total: num_shards as u32,
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

        let all_searches_res = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shards(shard_selection)?;
            let all_searches = target_shards
                .iter()
                .map(|shard| shard.get().search(request.clone(), search_runtime_handle));
            try_join_all(all_searches).await?.into_iter().flatten()
        };

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
        let require_transfers =
            self.shards_holder.read().await.len() * (request.limit + request.offset);
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
        let retrieved_points: Vec<_> = {
            let shards_holder = self.shards_holder.read().await;
            let target_shards = shards_holder.target_shards(shard_selection)?;
            let scroll_futures = target_shards.into_iter().map(|shard| {
                shard.get().scroll_by(
                    offset,
                    limit,
                    &with_payload_interface,
                    with_vector,
                    request.filter.as_ref(),
                )
            });

            try_join_all(scroll_futures).await?
        };
        let mut points: Vec<_> = retrieved_points
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

        let counts: Vec<_> = {
            let shards_holder = self.shards_holder.read().await;
            let target_shards = shards_holder.target_shards(shard_selection)?;
            let count_futures = target_shards
                .into_iter()
                .map(|shard| shard.get().count(request.clone()));
            try_join_all(count_futures).await?.into_iter().collect()
        };

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
        let all_shard_collection_results = {
            let shard_holder = self.shards_holder.read().await;
            let target_shards = shard_holder.target_shards(shard_selection)?;
            let retrieve_futures = target_shards.into_iter().map(|shard| {
                shard
                    .get()
                    .retrieve(request.clone(), &with_payload, with_vector)
            });
            try_join_all(retrieve_futures).await?
        };
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
        {
            let shard_holder = self.shards_holder.read().await;
            for shard in shard_holder
                .all_shards()
                .chain(shard_holder.all_temporary_shards())
            {
                match shard {
                    Shard::Local(shard) => shard.on_optimizer_config_update().await?,
                    Shard::Proxy(shard) => shard.on_optimizer_config_update().await?,
                    Shard::ForwardProxy(shard) => shard.on_optimizer_config_update().await?,
                    Shard::Remote(_) => {} // Do nothing for remote shards
                }
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
        {
            let shard_holder = self.shards_holder.read().await;
            for shard in shard_holder
                .all_shards()
                .chain(shard_holder.all_temporary_shards())
            {
                match shard {
                    Shard::Local(shard) => shard.on_optimizer_config_update().await?,
                    Shard::Remote(_) => {} // Do nothing for remote shards
                    Shard::Proxy(proxy) => proxy.on_optimizer_config_update().await?,
                    Shard::ForwardProxy(proxy) => proxy.on_optimizer_config_update().await?,
                }
            }
        }
        self.config.read().await.save(&self.path)?;
        Ok(())
    }

    pub async fn info(&self, shard_selection: Option<ShardId>) -> CollectionResult<CollectionInfo> {
        let (all_shard_collection_results, mut info) = {
            let shards_holder = self.shards_holder.read().await;

            let target_shards = shards_holder.target_shards(shard_selection)?;

            let first_shard =
                *target_shards
                    .first()
                    .ok_or_else(|| CollectionError::ServiceError {
                        error: "There are no shards for selected collection".to_string(),
                    })?;

            let info = first_shard.get().info().await?;
            let info_futures = target_shards
                .into_iter()
                .skip(1)
                .map(|shard| shard.get().info());

            (try_join_all(info_futures).await?, info)
        };

        all_shard_collection_results
            .into_iter()
            .for_each(|mut shard_info| {
                info.status = max(info.status, shard_info.status);
                info.optimizer_status =
                    max(info.optimizer_status.clone(), shard_info.optimizer_status);
                info.vectors_count += shard_info.vectors_count;
                info.indexed_vectors_count += shard_info.indexed_vectors_count;
                info.points_count += shard_info.points_count;
                info.segments_count += shard_info.segments_count;
                info.disk_data_size += shard_info.disk_data_size;
                info.ram_data_size += shard_info.ram_data_size;
                info.payload_schema
                    .extend(shard_info.payload_schema.drain());
            });
        Ok(info)
    }

    pub async fn cluster_info(&self, peer_id: PeerId) -> CollectionResult<CollectionClusterInfo> {
        let shards_holder = self.shards_holder.read().await;
        let shard_count = shards_holder.len();
        let mut local_shards = Vec::new();
        let mut remote_shards = Vec::new();
        let mut shard_transfers = Vec::new();
        let count_request = Arc::new(CountRequest {
            filter: None,
            exact: true,
        });
        // extract shards info
        for (shard_id, shard) in shards_holder.get_shards() {
            let shard_id = *shard_id;
            match shard {
                Shard::Local(ls) => {
                    let count_result = ls.count(count_request.clone()).await?;
                    let points_count = count_result.count;
                    local_shards.push(LocalShardInfo {
                        shard_id,
                        points_count,
                    })
                }
                Shard::Remote(rs) => remote_shards.push(RemoteShardInfo {
                    shard_id,
                    peer_id: rs.peer_id,
                }),
                Shard::Proxy(ls) => {
                    let count_result = ls.count(count_request.clone()).await?;
                    let points_count = count_result.count;
                    local_shards.push(LocalShardInfo {
                        shard_id,
                        points_count,
                    })
                }
                Shard::ForwardProxy(ls) => {
                    let count_result = ls.count(count_request.clone()).await?;
                    let points_count = count_result.count;
                    local_shards.push(LocalShardInfo {
                        shard_id,
                        points_count,
                    })
                }
            }
        }
        // extract shard transfers info
        for shard_transfer in shards_holder.get_shard_transfers() {
            let shard_id = shard_transfer.shard_id;
            let to = shard_transfer.to;
            let from = shard_transfer.from;
            shard_transfers.push(ShardTransferInfo { shard_id, from, to })
        }

        // sort by shard_id
        local_shards.sort_by_key(|k| k.shard_id);
        remote_shards.sort_by_key(|k| k.shard_id);
        shard_transfers.sort_by_key(|k| k.shard_id);

        let info = CollectionClusterInfo {
            peer_id,
            shard_count,
            local_shards,
            remote_shards,
            shard_transfers,
        };
        Ok(info)
    }

    pub async fn before_drop(&mut self) {
        self.shards_holder.write().await.before_drop().await;
        self.before_drop_called = true
    }

    pub async fn state(&self, this_peer_id: PeerId) -> State {
        let shards_holder = self.shards_holder.read().await;
        State {
            config: self.config.read().await.clone(),
            shard_to_peer: shards_holder
                .get_shards()
                .map(|(shard_id, shard)| (*shard_id, shard.peer_id(this_peer_id)))
                .collect(),
            transfers: (*shards_holder.shard_transfers).clone(),
        }
    }

    pub async fn apply_state(
        &self,
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
        let mut telemetry = self.telemetry.clone();
        telemetry.shards.clear();
        let shard_holder = self.shards_holder.read().await;
        for shard in shard_holder.all_shards() {
            telemetry.shards.push(shard.get_telemetry_data());
        }
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

        {
            let shards_holder = self.shards_holder.read().await;
            // Create snapshot of each shard
            for (shard_id, shard) in shards_holder.get_shards() {
                let shard_snapshot_path =
                    versioned_shard_path(&snapshot_path_with_tmp_extension, *shard_id, 0);
                create_dir_all(&shard_snapshot_path).await?;
                match shard {
                    Shard::Local(local_shard) => {
                        local_shard.create_snapshot(&shard_snapshot_path).await?;
                    }
                    Shard::Proxy(proxy_shard) => {
                        proxy_shard.create_snapshot(&shard_snapshot_path).await?;
                    }
                    Shard::ForwardProxy(proxy_shard) => {
                        proxy_shard.create_snapshot(&shard_snapshot_path).await?;
                    }
                    Shard::Remote(remote_shard) => {
                        // copy shard directory to snapshot directory
                        remote_shard.create_snapshot(&shard_snapshot_path).await?;
                    }
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

        let config = CollectionConfig::load(target_dir)?;
        let configured_shards = config.params.shard_number.get();

        for shard_id in 0..configured_shards {
            let shard_path = versioned_shard_path(target_dir, shard_id, 0);
            let shard_config_opt = ShardConfig::load(&shard_path)?;
            if let Some(shard_config) = shard_config_opt {
                match shard_config.r#type {
                    ShardType::Local => LocalShard::restore_snapshot(&shard_path)?,
                    ShardType::Remote { .. } => RemoteShard::restore_snapshot(&shard_path),
                    ShardType::Temporary => {}
                }
            } else {
                return Err(CollectionError::service_error(format!(
                    "Can't read shard config at {}",
                    shard_path.display()
                )));
            }
        }

        Ok(())
    }

    pub async fn shards_distribution(&self, local_peer_id: PeerId) -> Vec<(ShardId, PeerId)> {
        let shard_holder = self.shards_holder.read().await;
        shard_holder
            .get_shards()
            .map(|(shard_id, shard)| match shard {
                Shard::Local(_local_shard) => (*shard_id, local_peer_id),
                Shard::Proxy(_proxy_shard) => (*shard_id, local_peer_id),
                Shard::ForwardProxy(_proxy_shard) => (*shard_id, local_peer_id),
                Shard::Remote(remote_shard) => (*shard_id, remote_shard.peer_id),
            })
            .collect()
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
