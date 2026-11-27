use segment::types::PointIdType;

use super::payload_ops::PayloadOps;
use super::point_ops::PointOperations;
use super::vector_ops::VectorOperations;
use super::{CollectionUpdateOperations, FieldIndexOperations, VectorNameOperations};

/// Points an operation can change, as far as the operation itself tells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AffectedPoints {
    /// The operation changes at most these points
    Ids(Vec<PointIdType>),
    /// The operation changes no points
    Nothing,
    /// The operation can change points it doesn't list, so they depend on the shard's state
    Unknown,
}

impl CollectionUpdateOperations {
    /// Points this operation can change.
    pub fn affected_points(&self) -> AffectedPoints {
        match self {
            Self::PointOperation(op) => match op {
                PointOperations::UpsertPoints(op) => AffectedPoints::Ids(op.point_ids()),
                PointOperations::UpsertPointsConditional(op) => {
                    AffectedPoints::Ids(op.points_op.point_ids())
                }
                PointOperations::UpsertPointsRaw(points) => {
                    AffectedPoints::Ids(points.iter().map(|point| point.id).collect())
                }
                PointOperations::DeletePoints { ids } => AffectedPoints::Ids(ids.clone()),
                PointOperations::DeletePointsByFilter(_) => AffectedPoints::Unknown,
                // Also deletes the points in its ID range that it doesn't list
                PointOperations::SyncPoints(_) | PointOperations::SyncPointsRaw(_) => {
                    AffectedPoints::Unknown
                }
            },

            Self::VectorOperation(op) => match op {
                // The update filter only narrows down the listed points
                VectorOperations::UpdateVectors(op) => {
                    AffectedPoints::Ids(op.points.iter().map(|point| point.id).collect())
                }
                VectorOperations::DeleteVectors(points, _) => {
                    AffectedPoints::Ids(points.points.clone())
                }
                VectorOperations::DeleteVectorsByFilter(_, _) => AffectedPoints::Unknown,
            },

            // An explicit ID list takes precedence over the filter on apply
            Self::PayloadOperation(op) => match op {
                PayloadOps::SetPayload(op) | PayloadOps::OverwritePayload(op) => match &op.points {
                    Some(points) => AffectedPoints::Ids(points.clone()),
                    None => AffectedPoints::Unknown,
                },
                PayloadOps::DeletePayload(op) => match &op.points {
                    Some(points) => AffectedPoints::Ids(points.clone()),
                    None => AffectedPoints::Unknown,
                },
                PayloadOps::ClearPayload { points } => AffectedPoints::Ids(points.clone()),
                PayloadOps::ClearPayloadByFilter(_) => AffectedPoints::Unknown,
            },

            Self::FieldIndexOperation(op) => match op {
                FieldIndexOperations::CreateIndex(_) | FieldIndexOperations::DeleteIndex(_) => {
                    AffectedPoints::Nothing
                }
            },

            Self::VectorNameOperation(op) => match op {
                VectorNameOperations::CreateVectorName(_) => AffectedPoints::Nothing,
                // Removes the vector from every point
                VectorNameOperations::DeleteVectorName(_) => AffectedPoints::Unknown,
            },

            #[cfg(feature = "staging")]
            Self::StagingOperation(op) => match op {
                super::staging::StagingOperations::Delay(_) => AffectedPoints::Nothing,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use segment::types::Filter;

    use super::*;
    use crate::operations::payload_ops::{DeletePayloadOp, SetPayloadOp};

    #[test]
    fn test_affected_points() {
        let ids = vec![PointIdType::from(1), PointIdType::from(2)];

        let delete_points =
            CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
                ids: ids.clone(),
            });
        assert_eq!(
            delete_points.affected_points(),
            AffectedPoints::Ids(ids.clone()),
        );

        let delete_by_filter = CollectionUpdateOperations::PointOperation(
            PointOperations::DeletePointsByFilter(Filter::default()),
        );
        assert_eq!(delete_by_filter.affected_points(), AffectedPoints::Unknown);

        let set_payload_by_ids =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
                payload: Default::default(),
                points: Some(ids.clone()),
                filter: Some(Filter::default()),
                key: None,
            }));
        assert_eq!(
            set_payload_by_ids.affected_points(),
            AffectedPoints::Ids(ids.clone()),
        );

        let delete_payload_by_filter = CollectionUpdateOperations::PayloadOperation(
            PayloadOps::DeletePayload(DeletePayloadOp {
                keys: Vec::new(),
                points: None,
                filter: Some(Filter::default()),
            }),
        );
        assert_eq!(
            delete_payload_by_filter.affected_points(),
            AffectedPoints::Unknown,
        );

        let delete_index = CollectionUpdateOperations::FieldIndexOperation(
            FieldIndexOperations::DeleteIndex("field".parse().unwrap()),
        );
        assert_eq!(delete_index.affected_points(), AffectedPoints::Nothing);
    }
}
