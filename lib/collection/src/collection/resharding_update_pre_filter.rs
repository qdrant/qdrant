use std::collections::HashSet;

use segment::types::CustomIdCheckerCondition as _;

use crate::hash_ring;
use crate::operations::types::{CollectionResult, PointRequestInternal};
use crate::operations::CollectionUpdateOperations;
use crate::shards::resharding::ReshardStage;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::ShardHolder;
use crate::shards::shard_trait::ShardOperation;

#[derive(Clone, Debug)]
pub enum ReshardingUpdatePreFilter {
    NewShard,
    OldShard(hash_ring::HashRingFilter),
}

impl ReshardingUpdatePreFilter {
    pub fn new(shard_holder: &ShardHolder, shard_id: ShardId) -> Option<Self> {
        let state = shard_holder.resharding_state()?;

        if state.shard_id == shard_id && state.stage == ReshardStage::MigratingPoints {
            Some(Self::NewShard)
        } else if state.shard_id != shard_id && state.stage >= ReshardStage::ReadHashRingCommitted {
            shard_holder.resharding_filter().map(Self::OldShard)
        } else {
            None
        }
    }

    pub async fn apply(
        &self,
        operation: &mut CollectionUpdateOperations,
        local_shard: &(dyn ShardOperation + Sync),
        runtime: &tokio::runtime::Handle,
    ) -> CollectionResult<()> {
        // There's no need to pre-filter point delete operations
        if operation.is_delete_points() {
            return Ok(());
        }

        let points_to_filter = match self {
            Self::NewShard => {
                // We must *not* pre-filter point upsert operations
                if operation.is_upsert_points() {
                    return Ok(());
                }

                // Select *all* points
                operation.point_ids()
            }

            Self::OldShard(filter) => {
                // Select points that are hashed into *new* shard
                operation
                    .point_ids()
                    .into_iter()
                    .filter(|&point_id| filter.check(point_id))
                    .collect()
            }
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
