use blob_store::Blob;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimId64, DimWeight};

use crate::common::operation_error::OperationError;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct StoredSparseVector {
    /// Indices must be unique, explicitly use 64-bit integers for further interface extension
    pub indices: Vec<DimId64>,
    /// Values and indices must be the same length
    pub values: Vec<DimWeight>,
}

impl From<&SparseVector> for StoredSparseVector {
    fn from(vector: &SparseVector) -> Self {
        Self {
            indices: vector.indices.iter().copied().map(DimId64::from).collect(),
            values: vector.values.clone(),
        }
    }
}

impl TryFrom<StoredSparseVector> for SparseVector {
    type Error = OperationError;

    fn try_from(value: StoredSparseVector) -> Result<Self, Self::Error> {
        Ok(SparseVector {
            indices: value
                .indices
                .into_iter()
                .map(DimId::try_from)
                .collect::<Result<_, _>>()
                .map_err(|err| {
                    OperationError::service_error(format!("Failed to convert indices: {err}"))
                })?,
            values: value.values,
        })
    }
}

impl Blob for StoredSparseVector {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Sparse vector serialization should not fail")
    }

    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data).expect("Sparse vector deserialization should not fail")
    }
}
