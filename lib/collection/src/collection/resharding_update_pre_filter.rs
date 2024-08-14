use std::collections::HashSet;

use segment::types::CustomIdCheckerCondition as _;

use crate::hash_ring;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::types::{CollectionError, CollectionResult, PointRequestInternal};
use crate::operations::CollectionUpdateOperations;
use crate::shards::resharding::ReshardStage;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::ShardHolder;
use crate::shards::shard_trait::ShardOperation;

#[derive(Clone, Debug)]
pub struct ReshardingUpdatePreFilter {
    this_shard_id: ShardId,
    new_shard_id: ShardId,
    direction: ReshardingDirection,
    stage: ReshardStage,
    filter: Option<hash_ring::HashRingFilter>,
}

impl ReshardingUpdatePreFilter {
    pub fn new(shard_holder: &ShardHolder, shard_id: ShardId) -> Option<Self> {
        let state = shard_holder.resharding_state()?;

        let pre_filter = Self {
            this_shard_id: shard_id,
            new_shard_id: state.shard_id,
            direction: state.direction,
            stage: state.stage,
            filter: shard_holder.resharding_filter(),
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
        if self.is_receiver_shard() && operation.is_upsert_points() {
            return Ok(());
        }

        // There's no need to pre-filter point delete operations
        if operation.is_delete_points() {
            return Ok(());
        }

        // We apply filter on *receiver* shards during `MigratingPoints` stage
        let is_receiver_migrating_points =
            self.is_receiver_shard() && self.stage == ReshardStage::MigratingPoints;

        // And on *source* shards during `ReadHashRingCommitted` stage
        let is_old_source_read_hash_ring_committed = !self.is_new_shard()
            && !self.is_receiver_shard()
            && self.stage >= ReshardStage::ReadHashRingCommitted;

        let apply_filter = is_receiver_migrating_points || is_old_source_read_hash_ring_committed;

        if !apply_filter {
            return Ok(());
        }

        let points_to_filter = if self.is_new_shard() {
            // If we pre-filter points on *new* shard, we select *all* points
            operation.point_ids()
        } else {
            // If we pre-filter points on *old* shard, we select points that are hashed into *new* shard

            let filter = self
                .filter
                .as_ref()
                .ok_or_else(|| CollectionError::service_error("TODO"))?; // TODO(resharding)!

            operation
                .point_ids()
                .into_iter()
                .filter(|&point_id| filter.check(point_id))
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

    fn is_receiver_shard(&self) -> bool {
        match self.direction {
            ReshardingDirection::Up => self.is_new_shard(),
            ReshardingDirection::Down => !self.is_new_shard(),
        }
    }

    fn is_new_shard(&self) -> bool {
        self.this_shard_id == self.new_shard_id
    }
}
