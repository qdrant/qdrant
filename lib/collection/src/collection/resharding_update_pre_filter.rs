use std::collections::HashSet;

use segment::types::CustomIdCheckerCondition as _;

use crate::hash_ring;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::{CollectionResult, PointRequestInternal};
use crate::operations::CollectionUpdateOperations;
use crate::shards::resharding::ReshardStage;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::ShardHolder;
use crate::shards::shard_trait::ShardOperation;

#[derive(Clone, Debug)]
pub struct ReshardingUpdatePreFilter {
    /// Target shard of the resharding operation. This is the shard that:
    /// - *created* during resharding *up*
    /// - *deleted* during resharding *down*
    is_target_shard: bool,

    /// Shard that will be *receiving* migrated points during resharding:
    /// - *target* shard during resharding *up*
    /// - *non* target shards during resharding *down*
    is_receiver_shard: bool,

    /// Hashring filter, that selects points that are hashed into *target* shard
    filter: hash_ring::HashRingFilter,
}

impl ReshardingUpdatePreFilter {
    pub fn new(shard_holder: &ShardHolder, shard_id: ShardId) -> Option<Self> {
        let state = shard_holder.resharding_state()?;

        // Resharding *UP*
        // ┌────────────┐   ┌──────────┐
        // │            │   │          │
        // │ Shard 1    │   │ Shard 2  │
        // │ Non-Target ├──►│ Target   │
        // │ Sender     │   │ Receiver │
        // │            │   │          │
        // └────────────┘   └──────────┘
        //
        // Resharding *DOWN*
        // ┌────────────┐   ┌──────────┐
        // │            │   │          │
        // │ Shard 1    │   │ Shard 2  │
        // │ Non-Target │◄──┤ Target   │
        // │ Receiver   │   │ Sender   │
        // │            │   │          │
        // └────────────┘   └──────────┘

        // Target shard of the resharding operation. This is the shard that:
        // - *created* during resharding *up*
        // - *deleted* during resharding *down*
        let is_target_shard = shard_id == state.shard_id;

        // Shard that will be *receiving* migrated points during resharding:
        // - *target* shard during resharding *up*
        // - *non* target shards during resharding *down*
        let is_receiver_shard = match state.direction {
            ReshardingDirection::Up => is_target_shard,
            ReshardingDirection::Down => !is_target_shard,
        };

        // Shard that will be *sending* migrated points during resharding:
        // - *non* target shards during resharding *up*
        // - *target* shard during resharding *down*
        let is_sender_shard = !is_receiver_shard;

        // We apply resharding pre-filter:
        //
        // - on *receiver* shards during `MigratingPoints` stage
        // - and on *sender* shards during `ReadHashRingCommitted` stage when resharding *up*
        let should_filter = (is_receiver_shard && state.stage == ReshardStage::MigratingPoints)
            || (is_sender_shard
                && state.stage >= ReshardStage::ReadHashRingCommitted
                && state.direction == ReshardingDirection::Up);

        if !should_filter {
            return None;
        }

        let filter = shard_holder
            .resharding_filter()
            .expect("resharding filter is available when resharding is in progress");

        let pre_filter = Self {
            is_target_shard,
            is_receiver_shard,
            filter,
        };

        Some(pre_filter)
    }

    pub async fn apply(
        &self,
        operation: &mut CollectionUpdateOperations,
        local_shard: &(dyn ShardOperation + Sync),
        runtime: &tokio::runtime::Handle,
    ) -> CollectionResult<()> {
        // We must *not* pre-filter point upsert operations on *receiver* shards
        if self.is_receiver_shard && operation.is_upsert_points() {
            return Ok(());
        }

        // There's no need to pre-filter point delete operations
        if operation.is_delete_points() {
            return Ok(());
        }

        let points_to_filter = if self.is_target_shard {
            // When pre-filtering points on *target* shard, select *all* points for pre-filtering
            operation.point_ids()
        } else {
            // When pre-filtering points on *non* target shard, only select points
            // that are *hashed into target shard* for pre-filtering
            operation
                .point_ids()
                .into_iter()
                .filter(|&point_id| self.filter.check(point_id))
                .collect()
        };

        if points_to_filter.is_empty() {
            return Ok(());
        }

        let retrieve = PointRequestInternal {
            ids: points_to_filter.clone(),
            with_payload: None,
            with_vector: false.into(),
        };

        let retrieved_points = local_shard
            .retrieve(
                retrieve.into(),
                &false.into(),
                &false.into(),
                runtime,
                None, // TODO(resharding)?
            )
            .await?;

        let retrieved_points: HashSet<_> =
            retrieved_points.into_iter().map(|point| point.id).collect();

        let missing_points: HashSet<_> = points_to_filter
            .into_iter()
            .filter(|point_id| !retrieved_points.contains(point_id))
            .collect();

        operation.remove_point_ids(&missing_points);

        Ok(())
    }
}
