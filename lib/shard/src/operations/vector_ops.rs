use segment::types::{Filter, PointIdType, VectorNameBuf};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};

use super::point_ops::{PointIdsList, VectorStructPersisted};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PointVectorsPersisted {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    pub vector: VectorStructPersisted,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct UpdateVectorsOp {
    /// Points with named vectors
    pub points: Vec<PointVectorsPersisted>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum VectorOperations {
    /// Update vectors
    UpdateVectors(UpdateVectorsOp),
    /// Delete vectors if exists
    DeleteVectors(PointIdsList, Vec<VectorNameBuf>),
    /// Delete vectors by given filter criteria
    DeleteVectorsByFilter(Filter, Vec<VectorNameBuf>),
}

impl VectorOperations {
    pub fn is_write_operation(&self) -> bool {
        match self {
            VectorOperations::UpdateVectors(_) => true,
            VectorOperations::DeleteVectors(..) => false,
            VectorOperations::DeleteVectorsByFilter(..) => false,
        }
    }

    pub fn point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::UpdateVectors(op) => Some(op.points.iter().map(|point| point.id).collect()),
            Self::DeleteVectors(points, _) => Some(points.points.clone()),
            Self::DeleteVectorsByFilter(_, _) => None,
        }
    }

    pub fn retain_point_ids<F>(&mut self, filter: F)
    where
        F: Fn(&PointIdType) -> bool,
    {
        match self {
            Self::UpdateVectors(op) => op.points.retain(|point| filter(&point.id)),
            Self::DeleteVectors(points, _) => points.points.retain(filter),
            Self::DeleteVectorsByFilter(_, _) => (),
        }
    }
}
