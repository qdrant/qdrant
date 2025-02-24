use std::borrow::Cow;

use segment::types::{Filter, PointIdType};

use super::vector_ops;
use crate::operations::payload_ops::PayloadOps;
use crate::operations::{CollectionUpdateOperations, point_ops};

/// Structure to define what part of the shard are affected by the operation
pub enum OperationEffectArea<'a> {
    Empty,
    Points(Cow<'a, [PointIdType]>),
    Filter(&'a Filter),
}

/// Estimate how many points will be affected by the operation
pub enum PointsOperationEffect {
    /// No points affected
    Empty,
    /// Some points are affected
    Some(Vec<PointIdType>),
    /// Too many to enumerate, so we just say that it is a lot
    Many,
}

pub trait EstimateOperationEffectArea {
    fn estimate_effect_area(&self) -> OperationEffectArea;
}

impl EstimateOperationEffectArea for CollectionUpdateOperations {
    fn estimate_effect_area(&self) -> OperationEffectArea {
        match self {
            CollectionUpdateOperations::PointOperation(point_operation) => {
                point_operation.estimate_effect_area()
            }
            CollectionUpdateOperations::VectorOperation(vector_operation) => {
                vector_operation.estimate_effect_area()
            }
            CollectionUpdateOperations::PayloadOperation(payload_operation) => {
                payload_operation.estimate_effect_area()
            }
            CollectionUpdateOperations::FieldIndexOperation(_) => OperationEffectArea::Empty,
        }
    }
}

impl EstimateOperationEffectArea for point_ops::PointOperations {
    fn estimate_effect_area(&self) -> OperationEffectArea {
        match self {
            point_ops::PointOperations::UpsertPoints(insert_operations) => {
                insert_operations.estimate_effect_area()
            }
            point_ops::PointOperations::DeletePoints { ids } => {
                OperationEffectArea::Points(Cow::Borrowed(ids))
            }
            point_ops::PointOperations::DeletePointsByFilter(filter) => {
                OperationEffectArea::Filter(filter)
            }
            point_ops::PointOperations::SyncPoints(sync_op) => {
                debug_assert!(
                    false,
                    "SyncPoints operation should not be used during transfer"
                );
                OperationEffectArea::Points(Cow::Owned(
                    sync_op.points.iter().map(|x| x.id).collect(),
                ))
            }
        }
    }
}

impl EstimateOperationEffectArea for vector_ops::VectorOperations {
    fn estimate_effect_area(&self) -> OperationEffectArea {
        match self {
            vector_ops::VectorOperations::UpdateVectors(update_operation) => {
                let ids = update_operation.points.iter().map(|p| p.id).collect();
                OperationEffectArea::Points(Cow::Owned(ids))
            }
            vector_ops::VectorOperations::DeleteVectors(ids, _) => {
                OperationEffectArea::Points(Cow::Borrowed(&ids.points))
            }
            vector_ops::VectorOperations::DeleteVectorsByFilter(filter, _) => {
                OperationEffectArea::Filter(filter)
            }
        }
    }
}

impl EstimateOperationEffectArea for point_ops::PointInsertOperationsInternal {
    fn estimate_effect_area(&self) -> OperationEffectArea {
        match self {
            point_ops::PointInsertOperationsInternal::PointsBatch(batch) => {
                OperationEffectArea::Points(Cow::Borrowed(&batch.ids))
            }
            point_ops::PointInsertOperationsInternal::PointsList(list) => {
                OperationEffectArea::Points(Cow::Owned(list.iter().map(|x| x.id).collect()))
            }
        }
    }
}

impl EstimateOperationEffectArea for PayloadOps {
    fn estimate_effect_area(&self) -> OperationEffectArea {
        match self {
            PayloadOps::SetPayload(set_payload) => {
                if let Some(points) = &set_payload.points {
                    OperationEffectArea::Points(Cow::Borrowed(points))
                } else if let Some(filter) = &set_payload.filter {
                    OperationEffectArea::Filter(filter)
                } else {
                    OperationEffectArea::Empty
                }
            }
            PayloadOps::DeletePayload(delete_payload) => {
                if let Some(points) = &delete_payload.points {
                    OperationEffectArea::Points(Cow::Borrowed(points))
                } else if let Some(filter) = &delete_payload.filter {
                    OperationEffectArea::Filter(filter)
                } else {
                    OperationEffectArea::Empty
                }
            }
            PayloadOps::ClearPayload { points } => {
                OperationEffectArea::Points(Cow::Borrowed(points))
            }
            PayloadOps::ClearPayloadByFilter(filter) => OperationEffectArea::Filter(filter),
            PayloadOps::OverwritePayload(set_payload) => {
                if let Some(points) = &set_payload.points {
                    OperationEffectArea::Points(Cow::Borrowed(points))
                } else if let Some(filter) = &set_payload.filter {
                    OperationEffectArea::Filter(filter)
                } else {
                    OperationEffectArea::Empty
                }
            }
        }
    }
}
