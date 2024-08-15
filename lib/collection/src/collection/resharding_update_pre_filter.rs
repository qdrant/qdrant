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

    /// Filter that selects point IDs that are moved, based on resharding hash rings.
    filter: hash_ring::HashRingMovedFilter,

    /// Resharding direction
    direction: ReshardingDirection,
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

        // After write hash ring commit, we stop filtering
        if state.stage >= ReshardStage::WriteHashRingCommitted {
            return None;
        }

        // TODO(timvisee): validate this logic, being alternative to above?
        // // We apply resharding pre-filter:
        // //
        // // - on *receiver* shards during `MigratingPoints` stage
        // // - and on *sender* shards during `ReadHashRingCommitted` stage when resharding *up*
        // let should_filter = (is_receiver_shard && state.stage == ReshardStage::MigratingPoints)
        //     || (!is_sender_shard
        //         && state.stage >= ReshardStage::ReadHashRingCommitted
        //         && state.direction == ReshardingDirection::Up);
        // if !should_filter {
        //     return None;
        // }

        let filter = shard_holder
            .resharding_moved_filter()
            .expect("resharding filter is available when resharding is in progress");

        let pre_filter = Self {
            is_target_shard,
            is_receiver_shard,
            filter,
            direction: state.direction,
        };

        Some(pre_filter)
    }

    pub async fn apply(
        &self,
        operation: &mut CollectionUpdateOperations,
        local_shard: &(dyn ShardOperation + Sync),
        runtime: &tokio::runtime::Handle,
    ) -> CollectionResult<()> {
        // Always apply on source shard if resharding down, we drop the shard at the end
        if !self.is_receiver_shard && matches!(self.direction, ReshardingDirection::Down) {
            return Ok(());
        }

        // Deletes: always apply
        if operation.is_delete_points() {
            return Ok(());
        }

        // Insertions: always apply in receiver shard
        if self.is_receiver_shard && operation.is_upsert_points() {
            return Ok(());
        }

        // Create list of point IDs to exclude, only exclude points being moved
        let mut exclude_points = operation.point_ids();
        exclude_points.retain(|&point_id| self.filter.check(point_id));

        // Only exclude points that are not in the shard yet
        let existing_points = local_shard
            .retrieve(
                PointRequestInternal {
                    ids: exclude_points.clone(),
                    with_payload: None,
                    with_vector: false.into(),
                }
                .into(),
                &false.into(),
                &false.into(),
                runtime,
                None, // TODO(resharding)?
            )
            .await?
            .into_iter()
            .map(|point| point.id)
            .collect::<HashSet<_>>();
        exclude_points.retain(|point_id| existing_points.contains(point_id));

        // Remove points to exclude from our operation
        if !exclude_points.is_empty() {
            let point_ids_to_remove = exclude_points.into_iter().collect();
            operation.remove_point_ids(&point_ids_to_remove);
        }

        Ok(())
    }
}
