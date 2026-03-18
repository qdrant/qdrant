pub use shard::operations::payload_ops::*;

use super::{OperationToShard, SplitByShard, split_iter_by_shard};
use crate::hash_ring::HashRingRouter;

impl SplitByShard for PayloadOps {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            PayloadOps::SetPayload(operation) => {
                operation.split_by_shard(ring).map(PayloadOps::SetPayload)
            }
            PayloadOps::DeletePayload(operation) => operation
                .split_by_shard(ring)
                .map(PayloadOps::DeletePayload),
            PayloadOps::ClearPayload { points } => split_iter_by_shard(points, |id| *id, ring)
                .map(|points| PayloadOps::ClearPayload { points }),
            operation @ PayloadOps::ClearPayloadByFilter(_) => OperationToShard::to_all(operation),
            PayloadOps::OverwritePayload(operation) => operation
                .split_by_shard(ring)
                .map(PayloadOps::OverwritePayload),
        }
    }
}

impl SplitByShard for DeletePayloadOp {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match (&self.points, &self.filter) {
            (Some(_), _) => {
                split_iter_by_shard(self.points.unwrap(), |id| *id, ring).map(|points| {
                    DeletePayloadOp {
                        points: Some(points),
                        keys: self.keys.clone(),
                        filter: self.filter.clone(),
                    }
                })
            }
            (None, Some(_)) => OperationToShard::to_all(self),
            (None, None) => OperationToShard::to_none(),
        }
    }
}

impl SplitByShard for SetPayloadOp {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match (&self.points, &self.filter) {
            (Some(_), _) => {
                split_iter_by_shard(self.points.unwrap(), |id| *id, ring).map(|points| {
                    SetPayloadOp {
                        points: Some(points),
                        payload: self.payload.clone(),
                        filter: self.filter.clone(),
                        key: self.key.clone(),
                    }
                })
            }
            (None, Some(_)) => OperationToShard::to_all(self),
            (None, None) => OperationToShard::to_none(),
        }
    }
}
